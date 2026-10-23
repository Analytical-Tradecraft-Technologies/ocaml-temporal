(** Live interoperability regression for issue #517: an official Temporal SDK
    client (Python, pinned in [python/requirements.lock]) drives a workflow
    implemented in OCaml, and that workflow calls an activity hosted by the
    official SDK's worker.

    One OCaml process plays two roles. [worker] runs a public
    [Temporal.Worker] for the ["interop-order"] workflow on a unique task
    queue. [check] spawns and reaps that worker, publishes the queue names in
    [ready.json] inside the shared evidence directory, and then waits for the
    host controller to write [driver-exit] after the Python container has run
    [python/driver.py]. The Python driver performs the cross-SDK interactions
    and asserts their results and durable history; [check] afterwards opens
    the exact runs it reported through the OCaml client, so both SDKs must
    decode the same terminal results, failure types and failure details.

    Every payload uses the default [json/plain] converter on both sides.
    Prices are integer cents and the structured signal carries a 2{^53}+1
    integer, a fraction, non-ASCII text and [null], so a lossy number or string
    conversion in either direction changes a compared value. *)
open Temporal

(** Reports setup and client failures through their public diagnostic. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Sequences typed results inside workflow and codec code. *)
let ( let* ) = Result.bind

(** A typed decoding failure: workflow input that fails this check fails the
    workflow task, and update input that fails it rejects the update. *)
let shape message = Error (Error.codec ~message)

(** One order line. Prices are integer cents so neither SDK rounds them. *)
type item = { sku : string; qty : int; unit_cents : int }

(** The workflow input that the official SDK client sends as a JSON object.
    [note] is an explicit [null] in the driver's input and [coupon] is absent,
    so both spellings of an optional field are decoded. [pricing_task_queue]
    names the official SDK worker that hosts the pricing activity; carrying it
    in the input keeps the workflow deterministic. *)
type order = {
  customer : string;
  items : item list;
  note : string option;
  coupon : string option;
  rush : bool;
  pricing_task_queue : string;
}

(** Workflow-local state read by the [status] query and changed by the
    [add-item] update and [approve] signal. [approval] holds the signal's JSON
    document unchanged so the result can echo it back to the other SDK. *)
type state = { status : string; items : item list; approval : Yojson.Safe.t option }

(** Encodes one order line with the field names the Python driver uses. *)
let item_to_json { sku; qty; unit_cents } =
  `Assoc [ ("sku", `String sku); ("qty", `Int qty); ("unit_cents", `Int unit_cents) ]

(** Decodes one order line, rejecting missing fields and wrong JSON types. *)
let item_of_json = function
  | `Assoc fields -> (
      match
        (List.assoc_opt "sku" fields, List.assoc_opt "qty" fields,
         List.assoc_opt "unit_cents" fields)
      with
      | Some (`String sku), Some (`Int qty), Some (`Int unit_cents) ->
          Ok { sku; qty; unit_cents }
      | _ -> shape "order item requires a string sku and integer qty and unit_cents")
  | _ -> shape "order item must be a JSON object"

(** Encodes an optional string as either a JSON string or [null]. *)
let optional = function Some value -> `String value | None -> `Null

(** Decodes an optional string field; absent and [null] both mean [None]. *)
let optional_of_json name fields =
  match List.assoc_opt name fields with
  | None | Some `Null -> Ok None
  | Some (`String value) -> Ok (Some value)
  | Some _ -> shape (name ^ " must be a string or null")

(** Decodes every element or returns the first decoding error. *)
let decode_all decode values =
  List.fold_right (fun value acc ->
    let* rest = acc in
    let* value = decode value in
    Ok (value :: rest)) values (Ok [])

(** Encodes an order. Only the OCaml client's typed handle uses this direction;
    the official SDK client produces the workflow input. *)
let order_to_json order =
  `Assoc [
    ("customer", `String order.customer);
    ("items", `List (List.map item_to_json order.items));
    ("note", optional order.note);
    ("coupon", optional order.coupon);
    ("rush", `Bool order.rush);
    ("pricing_task_queue", `String order.pricing_task_queue) ]

(** Decodes the order object; any other JSON value is incompatible input. *)
let order_of_json = function
  | `Assoc fields -> (
      match
        (List.assoc_opt "customer" fields, List.assoc_opt "items" fields,
         List.assoc_opt "rush" fields, List.assoc_opt "pricing_task_queue" fields)
      with
      | Some (`String customer), Some (`List items), Some (`Bool rush),
        Some (`String pricing_task_queue) ->
          let* items = decode_all item_of_json items in
          let* note = optional_of_json "note" fields in
          let* coupon = optional_of_json "coupon" fields in
          Ok { customer; items; note; coupon; rush; pricing_task_queue }
      | _ -> shape "order requires customer, items, rush and pricing_task_queue")
  | _ -> shape "order must be a JSON object"

(** The [json/plain] codecs shared by the workflow input and update input. *)
let order_codec = Codec.json_conv ~to_json:order_to_json ~of_json:order_of_json
let item_codec = Codec.json_conv ~to_json:item_to_json ~of_json:item_of_json

(** The pricing activity is implemented only by the official SDK worker. Its
    input and output are untyped JSON documents so this side asserts nothing
    beyond what the Python activity returned. *)
let price = Activity.remote ~name:"interop-price" ~input:Codec.json ~output:Codec.json

(** Per-execution workflow state; see {!type-state}. *)
let state = Workflow_context.Local.create ()

(** Reads the state, reporting a query or update that arrives before the
    workflow body has stored it as the initial ["starting"] status. *)
let current () =
  let* value = Workflow_context.Local.get state in
  Ok (Option.value value ~default:{ status = "starting"; items = []; approval = None })

(** Replaces the state with [f] applied to its current value. *)
let modify f =
  let* value = current () in
  Workflow_context.Local.set state (f value)

(** True once the [approve] signal has stored its document. *)
let approved () =
  match Workflow_context.Local.get state with
  | Ok (Some { approval = Some _; _ }) -> true
  | Ok _ | Error _ -> false

(** Prices the order through the official SDK activity, then waits for the
    approval signal and returns a JSON document that echoes every value the
    other SDK sent, together with this run's exact identity. A pricing failure
    becomes a deliberate terminal workflow failure of type ["PricingFailed"]
    that forwards the activity's raw detail payloads, so the official SDK
    client must decode details produced by its own worker after they crossed
    the OCaml workflow in both directions. *)
let workflow = Workflow.define ~name:"interop-order" ~input:order_codec ~output:Codec.json
  (fun order ->
    let* () =
      Workflow_context.Local.set state
        { status = "pricing"; items = order.items; approval = None }
    in
    let* info = Workflow.info () in
    let request =
      `Assoc [
        ("currency", `String "EUR");
        ("rush", `Bool order.rush);
        ("items", `List (List.map item_to_json order.items)) ]
    in
    match
      Activity.execute ~task_queue:order.pricing_task_queue
        ~start_to_close_timeout:(Duration.of_ms 30_000L) price request
    with
    | Error error ->
        let activity_type = Option.value (Error.error_type error) ~default:"<none>" in
        Error (Error.make ~category:`Workflow ~non_retryable:true
          ~error_type:"PricingFailed" ~details:(Error.view error).details
          ~message:("pricing activity failed with " ^ activity_type) ())
    | Ok pricing ->
        let* () = modify (fun value -> { value with status = "awaiting-approval" }) in
        let* () = Condition.wait_until approved in
        let* () = modify (fun value -> { value with status = "approved" }) in
        let* final = current () in
        Ok (`Assoc [
          ("workflow_id", `String (Workflow.Info.workflow_id info));
          ("run_id", `String (Workflow.Info.run_id info));
          ("customer", `String order.customer);
          ("note", optional order.note);
          ("coupon", optional order.coupon);
          ("rush", `Bool order.rush);
          ("items", `List (List.map item_to_json final.items));
          ("pricing", pricing);
          ("approval", Option.value final.approval ~default:`Null);
          ("sdk", `String "temporal-ocaml") ]))

(** Read-only status view: the lifecycle step and the current line count. *)
let status_query = Query.define ~name:"status" ~output:Codec.json
let status () =
  let* value = current () in
  Ok (`Assoc [ ("status", `String value.status); ("items", `Int (List.length value.items)) ])

(** Stores the approval document exactly as decoded from the other SDK. *)
let approve = Signal.define ~name:"approve" ~input:Codec.json
let on_approve document = modify (fun value -> { value with approval = Some document })

(** Adds one order line and returns the new line count. *)
let add_item = Update.define ~name:"add-item" ~input:item_codec ~output:Codec.int

(** Rejects a non-positive quantity with a typed, non-retryable failure whose
    type and JSON detail the official SDK client must decode. *)
let validate_item item =
  if item.qty > 0 then Ok ()
  else
    Error (Error.make ~category:`Update ~non_retryable:true ~error_type:"InvalidQuantity"
      ~details:[get (Codec.encode Codec.json
        (`Assoc [ ("sku", `String item.sku); ("qty", `Int item.qty) ]))]
      ~message:"quantity must be positive" ())

(** Appends an accepted line; the line count is the update result. *)
let on_add_item item =
  let* () = modify (fun value -> { value with items = value.items @ [ item ] }) in
  let* value = current () in
  Ok (List.length value.items)

(** Runs the OCaml workflow worker on the fixture's unique task queue. *)
let worker address queue =
  let worker = get (Worker.create ~target_url:address ~namespace:"default"
    ~task_queue:queue ~activities:[] ~workflows:[Worker.workflow workflow
      ~queries:[Query.Handler.make status_query status]
      ~signals:[Signal.Handler.make approve on_approve]
      ~updates:[Update.Handler.make ~validator:validate_item add_item on_add_item]] ()) in
  get (Worker.run worker)

(** Writes [contents] to [path] through a rename, so the Python driver never
    reads a partially written readiness file. *)
let write_atomically path contents =
  let partial = path ^ ".partial" in
  Out_channel.with_open_text partial (fun channel -> output_string channel contents);
  Sys.rename partial path

(** Waits for the controller's [driver-exit] file and returns its trimmed
    status. The bound only prevents a lost driver from hanging this process
    until the controller's own outer timeout. *)
let await_driver_exit path =
  let deadline = Unix.gettimeofday () +. 600. in
  let rec loop () =
    if Sys.file_exists path then
      String.trim (In_channel.with_open_text path In_channel.input_all)
    else if Unix.gettimeofday () > deadline then
      failwith "the official SDK driver did not finish within 600 seconds"
    else (Unix.sleepf 0.2; loop ())
  in
  loop ()

(** Extracts a required field from the Python driver's JSON report. *)
let member name json =
  match Yojson.Safe.Util.member name json with
  | `Null -> failwith ("official SDK report is missing " ^ name)
  | value -> value

(** Extracts a required string field from the Python driver's JSON report. *)
let string_member name json =
  match member name json with
  | `String value -> value
  | _ -> failwith ("official SDK report field " ^ name ^ " is not a string")

(** Opens the exact run the driver reported, after checking that it belongs
    to this fixture's queue-derived workflow ID namespace. *)
let reported_handle client queue run =
  let id = string_member "workflow_id" run and run_id = string_member "run_id" run in
  if not (String.starts_with ~prefix:queue id) then
    failwith ("official SDK report names a foreign workflow ID: " ^ id);
  (get (Client.get_handle client ~run_id ~workflow ~id ()), id, run_id)

(** Reads the driver's report and requires the OCaml client to observe the
    same terminal outcome for every exact run the official SDK reported. *)
let verify_report address queue report =
  let python_sdk = string_member "python_sdk_version" report in
  let ocaml_sdk = string_member "ocaml_sdk_version" report in
  let server = string_member "server_version" report in
  let runs = member "runs" report in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  Fun.protect ~finally:(fun () -> ignore (Client.shutdown client)) (fun () ->
    let success = member "success" runs in
    let handle, id, run_id = reported_handle client queue success in
    (match get (Client.wait handle) with
    | Client.Completed { output; _ } ->
        let expected = member "result" success in
        if not (Yojson.Safe.equal (Yojson.Safe.sort output) (Yojson.Safe.sort expected)) then
          failwith (Printf.sprintf "OCaml client decoded a different result: %s <> %s"
            (Yojson.Safe.to_string output) (Yojson.Safe.to_string expected));
        if Yojson.Safe.Util.member "run_id" output <> `String run_id then
          failwith "workflow result names a different run"
    | _ -> failwith "OCaml client did not observe the completed interop run");
    Printf.printf "success run: %s/%s\n" id run_id;
    let failure = member "activity_failure" runs in
    let handle, id, run_id = reported_handle client queue failure in
    (match get (Client.wait handle) with
    | Client.Failed { error; _ } ->
        let view = Error.view error in
        if Error.error_type error <> Some "PricingFailed" || not view.non_retryable then
          failwith ("OCaml client decoded a different failure: " ^ Error.message error);
        let expected = `Assoc [ ("requested", `Int 3); ("sku", `String "OUT-OF-STOCK") ] in
        (match view.details with
        | [ detail ] when Yojson.Safe.equal
            (Yojson.Safe.sort (get (Codec.decode Codec.json detail))) expected -> ()
        | _ -> failwith "OCaml client decoded different failure details")
    | _ -> failwith "OCaml client did not observe the failed interop run");
    Printf.printf "activity failure run: %s/%s\n" id run_id;
    let incompatible = member "incompatible_input" runs in
    let handle, id, run_id = reported_handle client queue incompatible in
    (match get (Client.wait handle) with
    | Client.Terminated _ -> ()
    | _ -> failwith "incompatible-input run was not left open for termination");
    Printf.printf "incompatible input run: %s/%s\n" id run_id;
    Printf.printf
      "official SDK interoperability live regression: ok \
       (python temporalio %s, temporal-ocaml %s, server %s)\n%!"
      python_sdk ocaml_sdk server)

(** Hosts the OCaml worker while the official SDK driver runs, then checks the
    driver's report through the OCaml client. [evidence] is the directory the
    controller shares with the Python container. *)
let check address evidence =
  let queue = Printf.sprintf "sdk-interop-%d-%d" (Unix.getpid ())
    (Random.State.bits (Random.State.make_self_init ())) in
  let pid = Unix.create_process Sys.executable_name
    [| Sys.executable_name; "worker"; address; queue |]
    Unix.stdin Unix.stdout Unix.stderr in
  Fun.protect ~finally:(fun () -> Unix.kill pid Sys.sigterm; ignore (Unix.waitpid [] pid))
    (fun () ->
      write_atomically (Filename.concat evidence "ready.json")
        (Yojson.Safe.to_string (`Assoc [
          ("namespace", `String "default");
          ("workflow_task_queue", `String queue);
          ("pricing_task_queue", `String (queue ^ "-python")) ]));
      match await_driver_exit (Filename.concat evidence "driver-exit") with
      | "0" ->
          verify_report address queue
            (Yojson.Safe.from_file (Filename.concat evidence "python-report.json"))
      | status -> failwith ("the official SDK driver failed with status " ^ status))

(** [check] is the controller entry point; [worker] is its child process. *)
let () = match Array.to_list Sys.argv with
  | [ _; "worker"; address; queue ] -> worker address queue
  | [ _; "check"; address; evidence ] -> check address evidence
  | _ -> failwith "usage: regression check http://temporal:7233 /path/to/evidence"
