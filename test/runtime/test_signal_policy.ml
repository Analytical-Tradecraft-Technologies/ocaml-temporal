(** The v1 fail-closed signal delivery policy (#811), exercised through the
    real native activation validator and the production public arity/codec
    boundary ([Temporal.Signal.Handler.dispatch_payloads]).

    A signal that the worker cannot apply (no registered handler, an
    undecodable payload, or more than one payload) must fail only the current
    workflow task: the completion carries a bounded task-failure diagnostic,
    no commands, and in particular no [Fail_workflow] command, so the run stays
    open for a compatible worker or an operator reset/termination. A
    deliberate [`Workflow] error returned by the callback is the contrasting
    case that does close the run. *)
module T = Temporal
module Execution = Temporal_runtime.Execution
module Native = Temporal_runtime.Native_execution
module Protocol = Temporal_protocol.Workflow_protocol

(** The name under which the scenario workflow registers its one handler. *)
let registered_name = "approve"

(** Turns an unexpected public error into a useful assertion failure. *)
let require = function
  | Ok value -> value
  | Error error -> failwith (T.Error.message error)

(** Copies a public payload into the runtime-owned representation. *)
let base_payload (payload : T.Payload.t) : Temporal_base.Payload.t =
  { metadata = payload.metadata; data = Bytes.copy payload.data }

(** Copies a runtime payload before public codec dispatch. *)
let public_payload (payload : Temporal_base.Payload.t) : T.Payload.t =
  { metadata = payload.metadata; data = Bytes.copy payload.data }

(** Preserves the category, retryability, and message of a public error across
    the private boundary, mirroring the production [Error_private.to_base]. *)
let base_error error =
  let view = T.Error.view error in
  Temporal_base.Error.make ~category:view.category ~message:view.message
    ~non_retryable:view.non_retryable
    ~details:(List.map base_payload view.details) ()

(** Encodes a typed value as a bridge protocol payload. *)
let payload codec value : Protocol.payload =
  let payload = require (T.Codec.encode codec value) in
  { metadata =
      List.map (fun (key, value) -> (key, Bytes.of_string value)) payload.metadata;
    data = Bytes.copy payload.data }

(** Initializes the scenario workflow with its unit input. *)
let initialize =
  Protocol.Initialize_workflow
    { workflow_id = "signal-policy-id"; workflow_type = "signal-policy";
      arguments = [ payload T.Codec.unit () ]; randomness_seed = "1";
      attempt = 1; context = None }

(** Builds one signal job with the given name and ordered payloads. *)
let signal ~name input =
  Protocol.Signal_workflow { signal_name = name; input; identity = "test"; headers = [] }

(** Runs one activation through native validation and translation. *)
let activate execution jobs =
  let activation : Protocol.activation =
    { run_id = "signal-policy-run";
      timestamp = Some { seconds = 1L; nanoseconds = 0 };
      is_replaying = false; history_length = 3L; jobs; metadata = None } in
  match Native.activate execution activation with
  | Ok completion -> completion
  | Error error -> failwith (Native.error_view error).message

(** Starts a workflow that waits on a timer and registers one string signal
    handler. The runtime handler forwards the complete repeated payload list
    to the production [dispatch_payloads] boundary, exactly as the native
    worker adapter does, so arity and decode classification are the real
    ones. [callback] decides the handler's own result. *)
let start ~callback =
  let definition =
    Temporal_base.Definition.make ~name:"signal-policy"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:
        (Some
           (fun () ->
             Result.map_error base_error
               (T.Workflow.sleep (T.Duration.of_ms 10L))))
  in
  let public_handler =
    T.Signal.Handler.make
      (T.Signal.define ~name:registered_name ~input:T.Codec.string)
      callback
  in
  let runtime_handler =
    Execution.make_signal_handler ~name:registered_name
      ~dispatch:(fun (signal : Execution.signal) ->
        List.map public_payload signal.input
        |> T.Signal.Handler.dispatch_payloads public_handler
        |> Result.map_error base_error)
  in
  let execution =
    Execution.start ~signal_handlers:[ runtime_handler ] definition ()
  in
  let initial = activate execution [ initialize ] in
  if Option.is_some initial.task_failure then
    failwith "initial activation unexpectedly failed its task";
  execution

(** Asserts the fail-closed outcome: the task failed with a diagnostic that
    satisfies [check] and is at most [max_bytes] long, no command (terminal or
    otherwise) was emitted, and the callback never ran. *)
let expect_task_failure ~label ~calls ?(max_bytes = 1_024) ~check
    (completion : Protocol.completion) =
  if completion.commands <> [] then
    failwith (label ^ ": a fail-closed signal emitted workflow commands");
  (match completion.task_failure with
  | Some { message; _ } ->
      if String.length message > max_bytes then
        failwith
          (Printf.sprintf "%s: diagnostic is unbounded (%d bytes)" label
             (String.length message));
      if not (check message) then
        failwith (label ^ ": unexpected task-failure diagnostic: " ^ message)
  | None -> failwith (label ^ ": the signal did not fail the workflow task"));
  if !calls <> 0 then failwith (label ^ ": the signal callback ran")

(** Returns whether [needle] occurs anywhere in [haystack]. *)
let contains ~needle haystack =
  let needle_length = String.length needle in
  let rec scan index =
    index + needle_length <= String.length haystack
    && (String.equal (String.sub haystack index needle_length) needle
       || scan (index + 1))
  in
  scan 0

(** Runs one scenario against a fresh execution and always releases it. *)
let with_execution ~callback scenario =
  let execution = start ~callback in
  Fun.protect
    ~finally:(fun () -> Execution.shutdown execution)
    (fun () -> scenario execution)

(** A counting callback that would accept any value. Fail-closed scenarios
    assert that it is never reached. *)
let counting calls _value =
  incr calls;
  Ok ()

(** A signal name with no registered handler (a near miss of [approve], as a
    typo would be) fails the task and names the signal and the recovery
    options in its diagnostic. *)
let test_unknown_signal () =
  let calls = ref 0 in
  with_execution ~callback:(counting calls) (fun execution ->
      activate execution [ signal ~name:"approved" [ payload T.Codec.string "yes" ] ]
      |> expect_task_failure ~label:"unknown signal" ~calls ~check:(fun message ->
             String.starts_with ~prefix:"unhandled workflow signal: approved " message
             && contains ~needle:"reset or terminated" message))

(** A maximal 65,536-byte signal name still produces a bounded diagnostic, and
    truncation keeps a recognizable prefix of the name. *)
let test_unknown_signal_diagnostic_is_bounded () =
  let calls = ref 0 in
  let name = String.make 65_536 's' in
  with_execution ~callback:(counting calls) (fun execution ->
      activate execution [ signal ~name [] ]
      |> expect_task_failure ~label:"overlong unknown signal" ~calls ~max_bytes:512
           ~check:(fun message ->
             String.starts_with
               ~prefix:("unhandled workflow signal: " ^ String.make 256 's' ^ "...")
               message))

(** A payload the registered codec cannot decode fails the task before the
    callback runs, rather than closing the run. *)
let test_undecodable_signal () =
  let calls = ref 0 in
  with_execution ~callback:(counting calls) (fun execution ->
      activate execution
        [ signal ~name:registered_name [ payload T.Codec.int 7 ] ]
      |> expect_task_failure ~label:"undecodable signal" ~calls
           ~check:(fun _ -> true))

(** More than one payload for the single-input handler is classified like a
    decode failure: it fails the task without invoking the callback and
    without dropping data or closing the run. *)
let test_wrong_arity_signal () =
  let calls = ref 0 in
  let value = payload T.Codec.string "yes" in
  with_execution ~callback:(counting calls) (fun execution ->
      activate execution [ signal ~name:registered_name [ value; value ] ]
      |> expect_task_failure ~label:"wrong-arity signal" ~calls
           ~check:(contains ~needle:"at most one payload"))

(** The public arity boundary reports repeated payloads in the [`Codec]
    category, which is what routes them to a task failure in the runtime. *)
let test_wrong_arity_category () =
  let handler =
    T.Signal.Handler.make
      (T.Signal.define ~name:registered_name ~input:T.Codec.string)
      (fun _ -> Ok ())
  in
  let value = require (T.Codec.encode T.Codec.string "yes") in
  match T.Signal.Handler.dispatch_payloads handler [ value; value ] with
  | Error error when (T.Error.view error).category = `Codec -> ()
  | Error error ->
      failwith ("wrong arity used category " ^ T.Error.kind error)
  | Ok () -> failwith "wrong arity was accepted"

(** Contrast: a deliberate [`Workflow] error returned by a correctly delivered
    signal's callback is an application decision and closes the run. *)
let test_handler_business_error_closes_run () =
  let callback _ =
    Error (T.Error.make ~category:`Workflow ~message:"rejected" ())
  in
  with_execution ~callback (fun execution ->
      let completion =
        activate execution
          [ signal ~name:registered_name [ payload T.Codec.string "no" ] ]
      in
      if Option.is_some completion.task_failure then
        failwith "a business error failed the task instead of the run";
      match completion.commands with
      | [ Protocol.Fail_workflow _ ] -> ()
      | _ -> failwith "a business error did not close the run")

let () =
  test_unknown_signal ();
  test_unknown_signal_diagnostic_is_bounded ();
  test_undecodable_signal ();
  test_wrong_arity_signal ();
  test_wrong_arity_category ();
  test_handler_business_error_closes_run ()
