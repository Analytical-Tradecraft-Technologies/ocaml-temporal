(** Live interoperability regression for issue #517: an official Temporal SDK
    client (Python, pinned in [python/requirements.lock]) drives a workflow
    implemented in OCaml, and that workflow calls an activity hosted by the
    official SDK's worker.

    One OCaml process plays two roles. [worker] runs a public
    [Temporal.Worker] for the ["interop-order"] and ["interop-entity"]
    workflows on a unique task queue. [check] spawns and reaps that worker,
    publishes the queue names in [ready.json] inside the shared evidence
    directory, and then waits for the host controller to write [driver-exit]
    after the Python container has run [python/driver.py]. The Python driver
    performs the cross-SDK interactions and asserts their results and durable
    history; [check] afterwards opens the exact runs it reported through the
    OCaml client, so both SDKs must decode the same terminal results, failure
    types and failure details.

    Signal-with-start and update-with-start (#1151) are checked in both
    directions. The driver's client uses them against the OCaml entity, and
    the driver hosts a Python entity with the same protocol: once it
    publishes [python-ready], [check] drives that entity with
    [Client.signal_with_start] and [Client.update_with_start] while the
    driver runs its own cases, and publishes the runs in
    [ocaml-with-start.json], whose histories and results the driver then
    checks from its side.

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
    in the input keeps the workflow deterministic. [pricing_timeout_ms],
    absent except in the timeout case (#1143), sets the activity's
    start-to-close timeout in milliseconds and limits it to one attempt. *)
type order = {
  customer : string;
  items : item list;
  note : string option;
  coupon : string option;
  rush : bool;
  pricing_task_queue : string;
  pricing_timeout_ms : int option;
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
  `Assoc ([
    ("customer", `String order.customer);
    ("items", `List (List.map item_to_json order.items));
    ("note", optional order.note);
    ("coupon", optional order.coupon);
    ("rush", `Bool order.rush);
    ("pricing_task_queue", `String order.pricing_task_queue) ]
    @ (match order.pricing_timeout_ms with
      | Some ms -> [ ("pricing_timeout_ms", `Int ms) ]
      | None -> []))

(** Decodes the optional positive [pricing_timeout_ms] field. *)
let timeout_of_json fields =
  match List.assoc_opt "pricing_timeout_ms" fields with
  | None | Some `Null -> Ok None
  | Some (`Int ms) when ms > 0 -> Ok (Some ms)
  | Some _ -> shape "pricing_timeout_ms must be a positive integer or null"

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
          let* pricing_timeout_ms = timeout_of_json fields in
          Ok { customer; items; note; coupon; rush; pricing_task_queue; pricing_timeout_ms }
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
    the OCaml workflow in both directions. A pricing timeout (#1143) is
    recognized by [Error.timeout_type] and becomes a ["PricingTimedOut"]
    failure whose details are the timeout kind followed by the activity's
    last heartbeat details. *)
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
    let* retry_policy, start_to_close =
      match order.pricing_timeout_ms with
      | None -> Ok (None, Duration.of_ms 30_000L)
      | Some ms ->
          let* policy =
            Retry_policy.make ~initial_interval:(Duration.of_ms 1_000L)
              ~backoff_coefficient:2.0 ~maximum_interval:(Duration.of_ms 10_000L)
              ~maximum_attempts:1 ()
          in
          Ok (Some policy, Duration.of_ms (Int64.of_int ms))
    in
    match
      Activity.execute ~task_queue:order.pricing_task_queue ?retry_policy
        ~start_to_close_timeout:start_to_close price request
    with
    | Error error when Option.is_some (Error.timeout_type error) ->
        (* Branch on the typed timeout kind, never on the message (#1143),
           and return the heartbeat details apart from the failure details. *)
        let kind =
          match Error.timeout_type error with
          | Some `Start_to_close -> "start_to_close"
          | Some `Schedule_to_start -> "schedule_to_start"
          | Some `Schedule_to_close -> "schedule_to_close"
          | Some `Heartbeat -> "heartbeat"
          | Some `Unspecified | None -> "unspecified"
        in
        let* detail = Codec.encode Codec.json (`Assoc [ ("timeout", `String kind) ]) in
        Error (Error.make ~category:`Workflow ~non_retryable:true
          ~error_type:"PricingTimedOut"
          ~details:(detail :: Error.last_heartbeat_details error)
          ~message:("pricing activity timed out: " ^ kind) ())
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

(** {1 Signal-with-start and update-with-start (#1151)}

    Both SDKs implement the same small entity workflow: it records every
    [deliver] signal and accepted [apply] update in arrival order, waits for
    [finish], and completes with a JSON object naming its input label, the
    received items, its own run ID and the implementing SDK. The OCaml one is
    ["interop-entity"]; the Python one, ["interop-py-entity"], is hosted by
    the driver on the [python_workflow_task_queue] published in [ready.json].
    Each SDK's client uses signal-with-start and update-with-start against the
    other SDK's entity, so the operations are checked in both directions. *)

(** Items an entity run received from signals and accepted updates, newest
    first. *)
let entity_items = Workflow_context.Local.create ()

(** Set by [finish]; an unset flag is false. *)
let entity_finished = Workflow_context.Local.create ()

(** Reads the received items, newest first. *)
let entity_received () =
  let* items = Workflow_context.Local.get entity_items in
  Ok (Option.value items ~default:[])

(** True once [finish] has been delivered. *)
let entity_done () =
  match Workflow_context.Local.get entity_finished with
  | Ok (Some finished) -> finished
  | Ok None | Error _ -> false

(** Records one received item. *)
let entity_add item =
  let* items = entity_received () in
  Workflow_context.Local.set entity_items (item :: items)

(** Appends its string input to the run's items. *)
let deliver = Signal.define ~name:"deliver" ~input:Codec.string

(** Lets an entity run complete. [Codec.unit] sends no payload, matching the
    Python handler's empty parameter list. *)
let finish = Signal.define ~name:"finish" ~input:Codec.unit

(** Appends its input to the run's items and returns how many it now has. *)
let apply = Update.define ~name:"apply" ~input:Codec.string ~output:Codec.int

(** Rejects the input ["reject"] before the handler runs, with the typed,
    non-retryable failure the Python validator also raises: type
    ["Rejected"], message ["REJECTED"] and one JSON detail. *)
let validate_apply item =
  if not (String.equal item "reject") then Ok ()
  else
    let* detail = Codec.encode Codec.json (`Assoc [ ("item", `String item) ]) in
    Error (Error.make ~category:`Update ~non_retryable:true ~error_type:"Rejected"
      ~details:[ detail ] ~message:"REJECTED" ())

(** Records an accepted update and returns the new item count. *)
let on_apply item =
  let* () = entity_add item in
  let* items = entity_received () in
  Ok (List.length items)

(** The OCaml entity: waits for [finish] and reports what it received. *)
let entity = Workflow.define ~name:"interop-entity" ~input:Codec.string ~output:Codec.json
  (fun label ->
    let* info = Workflow.info () in
    let* () = Condition.wait_until entity_done in
    let* items = entity_received () in
    Ok (`Assoc [
      ("label", `String label);
      ("items", `List (List.rev_map (fun item -> `String item) items));
      ("run_id", `String (Workflow.Info.run_id info));
      ("sdk", `String "temporal-ocaml") ]))

(** Typed reference to the Python driver's entity; it has the same signals,
    update and output document as {!entity}. *)
let python_entity =
  Workflow.remote ~name:"interop-py-entity" ~input:Codec.string ~output:Codec.json

(** Runs the OCaml workflow worker on the fixture's unique task queue. *)
let worker address queue =
  let worker = get (Worker.create ~target_url:address ~namespace:"default"
    ~task_queue:queue ~activities:[] ~workflows:[
      Worker.workflow workflow
        ~queries:[Query.Handler.make status_query status]
        ~signals:[Signal.Handler.make approve on_approve]
        ~updates:[Update.Handler.make ~validator:validate_item add_item on_add_item];
      Worker.workflow entity
        ~signals:[
          Signal.Handler.make deliver entity_add;
          Signal.Handler.make finish (fun () ->
            Workflow_context.Local.set entity_finished true) ]
        ~updates:[Update.Handler.make ~validator:validate_apply apply on_apply]] ()) in
  get (Worker.run worker)

(** Writes [contents] to [path] through a rename, so the Python driver never
    reads a partially written readiness file. *)
let write_atomically path contents =
  let partial = path ^ ".partial" in
  Out_channel.with_open_text partial (fun channel -> output_string channel contents);
  Sys.rename partial path

(** Waits until one of [paths] exists and returns the first that does, in
    list order. The 600-second bound only prevents a lost driver from hanging
    this process until the controller's own outer timeout. *)
let await_any paths =
  let deadline = Unix.gettimeofday () +. 600. in
  let rec loop () =
    match List.find_opt Sys.file_exists paths with
    | Some path -> path
    | None when Unix.gettimeofday () > deadline ->
        failwith "the official SDK driver did not finish within 600 seconds"
    | None -> Unix.sleepf 0.2; loop ()
  in
  loop ()

(** Waits for the controller's [driver-exit] file and returns its trimmed
    status. *)
let await_driver_exit path =
  ignore (await_any [ path ]);
  String.trim (In_channel.with_open_text path In_channel.input_all)

(** Fails a with-start case against the Python entity with a labelled
    message. *)
let fail_with_start label message =
  failwith (Printf.sprintf "OCaml with-start against Python, %s: %s" label message)

(** Returns the exact run ID of a handle from a start operation, which always
    pins one. *)
let exact_run handle = Option.get (Client.run_id handle)

(** Requires [handle] to name exactly [run_id] with the given [started]. *)
let expect_run label ~run_id ~started handle =
  if Client.run_id handle <> Some run_id then
    fail_with_start label "another run was addressed";
  if Client.started handle <> started then
    fail_with_start label (Printf.sprintf "started was %b" (Client.started handle))

(** Finishes the Python entity run [handle] and requires it to complete with
    exactly [items], naming its own run and the Python SDK. Returns the
    decoded output for the driver to compare with what it decodes itself. *)
let finish_python_entity label handle items =
  get (Client.signal handle ~signal:finish ~input:());
  match Client.wait ~timeout:(Duration.of_seconds 30L) handle with
  | Ok (Client.Completed { output; _ }) ->
      let field name = Yojson.Safe.Util.member name output in
      if field "items" <> `List (List.map (fun item -> `String item) items) then
        fail_with_start label ("the run received " ^ Yojson.Safe.to_string (field "items"));
      if field "run_id" <> `String (exact_run handle) then
        fail_with_start label "the output names another run";
      if field "sdk" <> `String "temporalio-python" then
        fail_with_start label "the run was not completed by the Python entity";
      output
  | Ok _ -> fail_with_start label "the run did not complete"
  | Error error -> fail_with_start label ("waiting failed: " ^ Error.message error)

(** Requires an update result to be the Python validator's typed rejection:
    type ["Rejected"], non-retryable, with its JSON detail. *)
let expect_python_rejection label = function
  | Ok _ -> fail_with_start label "the rejected update was accepted"
  | Error error ->
      let view = Error.view error in
      if Error.error_type error <> Some "Rejected" || not view.non_retryable then
        fail_with_start label ("wrong update error: " ^ Error.message error);
      (match view.details with
      | [ detail ] when Yojson.Safe.equal (get (Codec.decode Codec.json detail))
          (`Assoc [ ("item", `String "reject") ]) -> ()
      | _ -> fail_with_start label "the rejection lost its detail")

(** The identity and decoded output of one run, as the report records it. *)
let run_report ?output handle =
  `Assoc ([
    ("workflow_id", `String (Client.workflow_id handle));
    ("run_id", `String (exact_run handle)) ]
    @ match output with Some output -> [ ("result", output) ] | None -> [])

(** Drives the Python entity on [task_queue] through the OCaml client's
    [signal_with_start] and [update_with_start] (#1151), asserting what the
    OCaml client observes: exact run IDs and [started] for an absent, open,
    replaced and closed workflow; accepted update results; a Python validator
    rejection reported beside the run, on an open run and on a run the call
    started; and the typed already-started error of a [`Fail] conflict.
    Returns the runs for the Python driver to check from its side. Every run
    is terminated if a check fails. *)
let with_start_against_python client queue task_queue =
  let tracked = ref [] in
  let track handle =
    tracked := handle :: !tracked;
    handle
  in
  let terminate_tracked () =
    List.iter (fun handle -> ignore (Client.terminate handle)) !tracked
  in
  let execution_timeout = Duration.of_minutes 5L in
  match
    let id = queue ^ "-ocaml-signal-with-start" in
    let signal_with_start ?id_conflict_policy item =
      track (get (Client.signal_with_start client ?id_conflict_policy ~execution_timeout
        ~workflow:python_entity ~task_queue ~id ~input:"ocaml" ~signal:deliver
        ~signal_input:item ()))
    in
    let first = signal_with_start "first" in
    expect_run "absent workflow" ~run_id:(exact_run first) ~started:true first;
    expect_run "open run" ~run_id:(exact_run first) ~started:false
      (signal_with_start "second");
    let replaced = signal_with_start ~id_conflict_policy:`Terminate_existing "third" in
    if String.equal (exact_run replaced) (exact_run first) then
      fail_with_start "terminate_existing" "the open run was kept";
    expect_run "terminate_existing" ~run_id:(exact_run replaced) ~started:true replaced;
    (match Client.wait ~timeout:(Duration.of_seconds 30L) first with
    | Ok (Client.Terminated _) -> ()
    | Ok _ | Error _ -> fail_with_start "terminate_existing" "the open run was not terminated");
    let replaced_output = finish_python_entity "replacement run" replaced [ "third" ] in
    let restarted = signal_with_start "fourth" in
    if String.equal (exact_run restarted) (exact_run replaced) then
      fail_with_start "closed workflow" "the closed run was signalled";
    expect_run "closed workflow" ~run_id:(exact_run restarted) ~started:true restarted;
    let restarted_output = finish_python_entity "restarted run" restarted [ "fourth" ] in
    let id = queue ^ "-ocaml-update-with-start" in
    let update_with_start ?(id = id) ~id_conflict_policy item =
      Client.update_with_start client ~id_conflict_policy ~execution_timeout
        ~workflow:python_entity ~task_queue ~id ~input:"ocaml" ~update:apply
        ~update_input:item ()
    in
    let updated, accepted = get (update_with_start ~id_conflict_policy:`Use_existing "one") in
    let updated = track updated in
    expect_run "update-with-start" ~run_id:(exact_run updated) ~started:true updated;
    if get (Client.wait_update (get accepted)) <> 1 then
      fail_with_start "update-with-start" "the first update was lost";
    let attached, accepted = get (update_with_start ~id_conflict_policy:`Use_existing "two") in
    expect_run "update on the open run" ~run_id:(exact_run updated) ~started:false attached;
    if get (Client.wait_update (get accepted)) <> 2 then
      fail_with_start "update on the open run" "the second update was lost";
    let rejected_on_open, rejection =
      get (update_with_start ~id_conflict_policy:`Use_existing "reject")
    in
    expect_run "rejection on the open run" ~run_id:(exact_run updated) ~started:false
      rejected_on_open;
    expect_python_rejection "rejection on the open run" rejection;
    (match update_with_start ~id_conflict_policy:`Fail "three" with
    | Ok _ -> fail_with_start "fail policy" "the open run was used"
    | Error error -> (
        match Client.already_started error with
        | Some { run_id; _ } when String.equal run_id (exact_run updated) -> ()
        | Some _ | None -> fail_with_start "fail policy" ("wrong error: " ^ Error.message error)));
    let updated_output = finish_python_entity "updated run" updated [ "one"; "two" ] in
    let fresh, rejection =
      get (update_with_start ~id:(id ^ "-rejected") ~id_conflict_policy:`Use_existing "reject")
    in
    let fresh = track fresh in
    expect_run "rejection on a new run" ~run_id:(exact_run fresh) ~started:true fresh;
    expect_python_rejection "rejection on a new run" rejection;
    let fresh_output = finish_python_entity "rejected update's run" fresh [] in
    `Assoc [
      ("signal_with_start_terminated", run_report first);
      ("signal_with_start_replacement", run_report ~output:replaced_output replaced);
      ("signal_with_start_restarted", run_report ~output:restarted_output restarted);
      ("update_with_start", run_report ~output:updated_output updated);
      ("update_with_start_rejected", run_report ~output:fresh_output fresh) ]
  with
  | runs -> runs
  | exception failure -> terminate_tracked (); raise failure

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
    let timeout = member "activity_timeout" runs in
    let handle, id, run_id = reported_handle client queue timeout in
    (match get (Client.wait handle) with
    | Client.Failed { error; _ } ->
        let view = Error.view error in
        if Error.error_type error <> Some "PricingTimedOut" || not view.non_retryable then
          failwith ("OCaml client decoded a different timeout failure: " ^ Error.message error);
        let decoded =
          List.map (fun detail -> Yojson.Safe.sort (get (Codec.decode Codec.json detail)))
            view.details
        in
        let expected =
          [ `Assoc [ ("timeout", `String "start_to_close") ]; `Assoc [ ("progress", `Int 42) ] ]
        in
        if not (List.equal Yojson.Safe.equal decoded expected) then
          failwith "OCaml client decoded different timeout failure details"
    | _ -> failwith "OCaml client did not observe the timed-out interop run");
    Printf.printf "activity timeout run: %s/%s\n" id run_id;
    let incompatible = member "incompatible_input" runs in
    let handle, id, run_id = reported_handle client queue incompatible in
    (match get (Client.wait handle) with
    | Client.Terminated _ -> ()
    | _ -> failwith "incompatible-input run was not left open for termination");
    Printf.printf "incompatible input run: %s/%s\n" id run_id;
    (* The OCaml entity runs the Python client signal-/update-with-started:
       one replaced under TERMINATE_EXISTING, the rest completed with the
       result Python decoded. *)
    let with_start = member "with_start" runs in
    let entity_run name =
      let run = member name with_start in
      let id = string_member "workflow_id" run and run_id = string_member "run_id" run in
      if not (String.starts_with ~prefix:queue id) then
        failwith ("official SDK report names a foreign workflow ID: " ^ id);
      (run, get (Client.get_handle client ~run_id ~workflow:entity ~id ()), id, run_id)
    in
    let _, handle, id, run_id = entity_run "signal_with_start_terminated" in
    (match get (Client.wait handle) with
    | Client.Terminated _ -> ()
    | _ -> failwith "the run replaced by Python's signal-with-start was not terminated");
    Printf.printf "Python signal-with-start replaced run: %s/%s\n" id run_id;
    List.iter (fun name ->
      let run, handle, id, run_id = entity_run name in
      (match get (Client.wait handle) with
      | Client.Completed { output; _ }
        when Yojson.Safe.equal (Yojson.Safe.sort output)
               (Yojson.Safe.sort (member "result" run)) -> ()
      | Client.Completed { output; _ } ->
          failwith (Printf.sprintf "OCaml client decoded a different %s result: %s"
            name (Yojson.Safe.to_string output))
      | _ -> failwith ("OCaml client did not observe the completed " ^ name ^ " run"));
      Printf.printf "Python %s run: %s/%s\n" name id run_id)
      [ "signal_with_start_replacement"; "signal_with_start_restarted";
        "update_with_start"; "update_with_start_rejected" ];
    Printf.printf
      "official SDK interoperability live regression: ok \
       (python temporalio %s, temporal-ocaml %s, server %s)\n%!"
      python_sdk ocaml_sdk server)

(** Once the driver publishes [python-ready] (its entity worker is polling),
    runs {!with_start_against_python} and publishes the outcome in
    [ocaml-with-start.json] for the driver to check: [{"ok": true, "runs":
    ...}] or [{"ok": false, "error": ...}], so a failure here never leaves the
    driver waiting. Returns the failure message, if any. If the driver exits
    first it never hosted the entity, and its own status reports why. *)
let run_with_start_cases address evidence queue entity_queue =
  let ready = Filename.concat evidence "python-ready" in
  if String.equal (await_any [ ready; Filename.concat evidence "driver-exit" ]) ready then begin
    let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
    let outcome =
      Fun.protect ~finally:(fun () -> ignore (Client.shutdown client)) (fun () ->
        match with_start_against_python client queue entity_queue with
        | runs -> Ok runs
        | exception Failure message -> Error message
        | exception failure -> Error (Printexc.to_string failure))
    in
    write_atomically (Filename.concat evidence "ocaml-with-start.json")
      (Yojson.Safe.to_string (match outcome with
        | Ok runs -> `Assoc [ ("ok", `Bool true); ("runs", runs) ]
        | Error message -> `Assoc [ ("ok", `Bool false); ("error", `String message) ]));
    match outcome with
    | Ok _ ->
        print_endline "OCaml signal-/update-with-start against the Python entity: ok";
        None
    | Error message -> Some message
  end
  else None

(** Hosts the OCaml worker while the official SDK driver runs, drives the
    driver's entity with the OCaml client's with-start operations, then checks
    the driver's report through the OCaml client. [evidence] is the directory
    the controller shares with the Python container. *)
let check address evidence =
  let queue = Printf.sprintf "sdk-interop-%d-%d" (Unix.getpid ())
    (Random.State.bits (Random.State.make_self_init ())) in
  let entity_queue = queue ^ "-python-entity" in
  let pid = Unix.create_process Sys.executable_name
    [| Sys.executable_name; "worker"; address; queue |]
    Unix.stdin Unix.stdout Unix.stderr in
  Fun.protect ~finally:(fun () -> Unix.kill pid Sys.sigterm; ignore (Unix.waitpid [] pid))
    (fun () ->
      write_atomically (Filename.concat evidence "ready.json")
        (Yojson.Safe.to_string (`Assoc [
          ("namespace", `String "default");
          ("workflow_task_queue", `String queue);
          ("pricing_task_queue", `String (queue ^ "-python"));
          ("python_workflow_task_queue", `String entity_queue) ]));
      let with_start_failure = run_with_start_cases address evidence queue entity_queue in
      match await_driver_exit (Filename.concat evidence "driver-exit"), with_start_failure with
      | "0", None ->
          verify_report address queue
            (Yojson.Safe.from_file (Filename.concat evidence "python-report.json"))
      | _, Some message -> failwith message
      | status, None -> failwith ("the official SDK driver failed with status " ^ status))

(** [check] is the controller entry point; [worker] is its child process. *)
let () = match Array.to_list Sys.argv with
  | [ _; "worker"; address; queue ] -> worker address queue
  | [ _; "check"; address; evidence ] -> check address evidence
  | _ -> failwith "usage: regression check http://temporal:7233 /path/to/evidence"
