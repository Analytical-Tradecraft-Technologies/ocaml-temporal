(** End-to-end OCaml/C/Rust regression for issue #771.

    Signal, query, and update inputs are encoded by the real OCaml client
    protocol and submitted to the real native bridge on a runtime that has no
    connected Temporal client. A request that Rust strictly decodes and
    validates reaches the lifecycle guard and returns [Invalid_state]; a
    request rejected by a size bound returns [Protocol]. Before the fix, every
    input whose base64 text exceeded 65,536 characters (more than 49,152 raw
    bytes) was rejected as [Protocol] even though OCaml had accepted it. *)

module Bridge = Temporal_core_bridge.Native_bridge
module Protocol = Temporal_protocol.Client_protocol
module Workflow = Temporal_protocol.Workflow_protocol

(** Raw payload length whose canonical base64 text is exactly the 65,536-byte
    protocol text limit that previously capped these operations. *)
let old_limit_raw_bytes = 65_536 / 4 * 3

(** Converts an OCaml protocol encoding error into a test failure. *)
let unwrap_encoding = function
  | Ok value -> value
  | Error error ->
      let view = Protocol.error_view error in
      failwith (Printf.sprintf "%s at %s: %s" view.code view.path view.message)

(** Builds a JSON-labelled payload of exactly [len] deterministic data bytes. *)
let payload len : Protocol.payload =
  {
    Workflow.metadata = [ ("encoding", Bytes.of_string "json/plain") ];
    data = Bytes.init len (fun index -> Char.chr (index mod 251));
  }

(** Exact run used by every request; the runtime never connects, so the
    identity only needs to satisfy validation. *)
let execution : Protocol.execution =
  { namespace = "default"; workflow_id = "workflow-1"; run_id = "run-1" }

(** One client operation under test: a label, the OCaml request encoder for a
    single payload, and the native entry point that receives the document. *)
type operation = {
  label : string;
  encode : Protocol.payload -> string;
  submit : Bridge.runtime -> bytes -> (bytes, Bridge.error) result;
}

(** Signal, query-with-input, and update admission, which share the payload
    bound with workflow start input. *)
let operations =
  [
    {
      label = "signal";
      encode =
        (fun input ->
          unwrap_encoding
            (Protocol.encode_signal_request
               {
                 execution;
                 signal_name = "add_document";
                 request_id = "signal-1";
                 input = [ input ];
               }));
      submit = Bridge.client_signal_workflow_json;
    };
    {
      label = "query";
      encode =
        (fun input ->
          unwrap_encoding
            (Protocol.encode_query_request
               { execution; query_type = "lookup"; input = [ input ] }));
      submit = Bridge.client_query_workflow_json;
    };
    {
      label = "update";
      encode =
        (fun input ->
          unwrap_encoding
            (Protocol.encode_update_request
               {
                 execution;
                 update_id = "update-1";
                 update_name = "set_state";
                 input = [ input ];
               }));
      submit = Bridge.client_update_workflow_json;
    };
  ]

(** Requires the bridge to accept [document] as valid and stop only because no
    client is connected. Any other failure, in particular [Protocol], means
    Rust rejected input that the OCaml encoder had already accepted. *)
let require_validated runtime operation ~raw_len document =
  match operation.submit runtime (Bytes.of_string document) with
  | Error { Bridge.status = Invalid_state; _ } -> ()
  | Error { Bridge.message; _ } ->
      failwith
        (Printf.sprintf "%s with a %d-byte payload was rejected: %s"
           operation.label raw_len message)
  | Ok _ ->
      failwith
        (Printf.sprintf "%s succeeded without a connected client" operation.label)

let () =
  let runtime =
    match Bridge.runtime_create () with
    | Ok runtime -> runtime
    | Error error -> failwith error.Bridge.message
  in
  List.iter
    (fun raw_len ->
      let input = payload raw_len in
      List.iter
        (fun operation ->
          require_validated runtime operation ~raw_len (operation.encode input))
        operations)
    [ old_limit_raw_bytes; old_limit_raw_bytes + 1; 60_000; 256 * 1024 ];
  (* Handler names keep the ordinary text bound: a hand-built document with a
     65,537-byte signal name bypasses the OCaml encoder and must still be
     rejected by Rust before the lifecycle guard. *)
  let oversized_name = String.make 65_537 'n' in
  let document =
    Printf.sprintf
      {|{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","signal_name":"%s","request_id":"signal-1","input":[]}|}
      oversized_name
  in
  (match
     Bridge.client_signal_workflow_json runtime
       (Bytes.of_string document)
   with
  | Error { Bridge.status = Protocol; _ } -> ()
  | _ -> failwith "an oversized signal name was not rejected as a protocol error");
  match Bridge.runtime_close runtime with
  | Ok () -> ()
  | Error error -> failwith error.Bridge.message
