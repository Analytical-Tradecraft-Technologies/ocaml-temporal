(** Live regression for issue #520: an update caller must never hang when the
    run closes before the update completes.

    The offline race matrix ([test/runtime/test_race_matrix.ml]) pins which
    commands the worker emits when an update handler is still suspended as
    its run completes, is cancelled, or is terminated. It cannot show what the
    {e caller} then receives, because that outcome is produced by the
    Temporal server when it closes the run. This fixture closes real runs
    against a server and asserts, for each way the run can close, the exact
    typed result of every update caller and that each one returns within a
    fixed bound.

    One process plays two roles. [worker] runs a public [Temporal.Worker] on
    a unique task queue. [check] owns the client, spawns and reaps the worker
    processes, and synchronizes only on observable server state: the update
    acceptance returned by the client, exact run histories, and the pending
    workflow task reported by [describe], both read through the official
    Temporal CLI. *)
open Temporal

(** Reports setup and client failures through their public diagnostic. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Opens when the [finish] signal arrives; the workflow body waits for it. *)
let finished = Workflow_context.Local.create ()

(** Condition predicate over the [finish] gate; an unset gate is closed. *)
let is_finished () = match Workflow_context.Local.get finished with
  | Ok (Some true) -> true
  | Ok _ | Error _ -> false

(** Parks until [finish], then completes with its input. The run therefore
    closes only through the driver: a signal, a cancellation request, or a
    termination. *)
let workflow = Workflow.define ~name:"update-run-close" ~input:Codec.int ~output:Codec.int
  (fun input -> Result.map (fun () -> input) (Condition.wait_until is_finished))

(** Lets the workflow body return. *)
let finish = Signal.define ~name:"finish" ~input:Codec.unit

(** An update whose handler is accepted and then never finishes. *)
let hold = Update.define ~name:"hold" ~input:Codec.int ~output:Codec.int

(** Suspends forever after acceptance, so the update can only end when the
    run closes. *)
let hold_handler (_ : int) = Result.map (fun () -> 0) (Condition.wait_until (fun () -> false))

(** Runs one worker until it is terminated. *)
let worker address queue =
  let worker = get (Worker.create ~target_url:address ~namespace:"default"
    ~task_queue:queue ~activities:[]
    ~workflows:[Worker.workflow workflow
      ~signals:[Signal.Handler.make finish (fun () -> Workflow_context.Local.set finished true)]
      ~updates:[Update.Handler.make hold hold_handler]] ()) in
  get (Worker.run worker)

(** Spawns a separate worker process, so one can be stopped without
    affecting the driver's client. *)
let spawn address queue = Unix.create_process Sys.executable_name
  [|Sys.executable_name; "worker"; address; queue|]
  Unix.stdin Unix.stdout Unix.stderr

(** Terminates and reaps one worker process; no fixture worker survives. *)
let stop pid = (try Unix.kill pid Sys.sigterm with Unix.Unix_error _ -> ());
  ignore (Unix.waitpid [] pid)

(** Seconds any one caller may wait for its outcome once the run is closed.
    Temporal reports these outcomes as soon as the close is persisted, so a
    caller still blocked after this bound would be the unexplained hang the
    issue forbids, not a slow server. *)
let caller_bound = 10.

(** Seconds allowed for one server-state observation to become true. *)
let observation_seconds = 30.

(** Polls [attempt] until it returns [Ok], and fails with [label] and the
    last reason when the deadline passes. *)
let await label attempt =
  let deadline = Unix.gettimeofday () +. observation_seconds in
  let rec poll () = match attempt () with
    | Ok value -> value
    | Error _ when Unix.gettimeofday () < deadline -> Unix.sleepf 0.05; poll ()
    | Error message -> failwith (Printf.sprintf "timed out waiting for %s: %s" label message) in
  poll ()

(** Deadline for one CLI invocation, matching the other history fixtures. *)
let cli_timeout = "30s"

(** Runs the official CLI against this fixture's exact run and parses its
    JSON output, without a shell. A failed read is an [Error] so callers can
    retry it. *)
let cli_json cli address handle command =
  let args = Array.of_list ([cli; "--address"; address; "--namespace"; "default";
    "--command-timeout"; cli_timeout; "workflow"] @ command
    @ ["--workflow-id"; Client.workflow_id handle; "--run-id"; Option.get (Client.run_id handle);
       "--output"; "json"]) in
  let input = Unix.open_process_args_in cli args in
  let document = match Yojson.Basic.from_channel input with
    | document -> Ok document
    | exception Yojson.Json_error message -> Error message
    | exception exn -> ignore (Unix.close_process_in input); raise exn in
  match Unix.close_process_in input, document with
  | Unix.WEXITED 0, Ok document -> Ok document
  | Unix.WEXITED 0, Error message -> Error ("unparsable CLI output: " ^ message)
  | Unix.WEXITED code, _ -> Error (Printf.sprintf "CLI exited with %d" code)
  | (Unix.WSIGNALED _ | Unix.WSTOPPED _), _ -> Error "CLI was killed"

(** Returns an event's type without its [EVENT_TYPE_] prefix. *)
let event_type event =
  let name = Yojson.Basic.Util.(event |> member "eventType" |> to_string) in
  let prefix = "EVENT_TYPE_" in
  if String.starts_with ~prefix name then
    String.sub name (String.length prefix) (String.length name - String.length prefix)
  else name

(** Reads one exact run's event types, retrying transient CLI failures. *)
let history cli address handle =
  await ("history of " ^ Client.workflow_id handle) (fun () ->
    Result.bind (cli_json cli address handle ["show"]) (fun document ->
      try Ok Yojson.Basic.Util.(document |> member "events" |> to_list |> List.map event_type)
      with Yojson.Basic.Util.Type_error (message, _) -> Error ("unexpected history: " ^ message)))

(** Reports whether [describe] shows a pending workflow task for the run. *)
let has_pending_task cli address handle =
  Result.map (fun document ->
    Yojson.Basic.Util.member "pendingWorkflowTask" document <> `Null)
    (cli_json cli address handle ["describe"])

(** The run's durable events without workflow-task bookkeeping, whose count
    depends on worker timing rather than on a decision. *)
let decisions events =
  List.filter (fun kind -> not (String.starts_with ~prefix:"WORKFLOW_TASK_" kind)) events

(** Requires an exact list of decision events and prints it as evidence. *)
let expect_history label expected events =
  let actual = decisions events in
  if actual <> expected then
    failwith (Printf.sprintf "%s: history\n    %s\n  expected\n    %s" label
      (String.concat "\n    " actual) (String.concat "\n    " expected));
  Printf.printf "%s history: %s\n%!" label (String.concat ", " actual)

(** Renders the fields of a caller's result that the contract pins: the
    category, retry flag, application failure type, RPC status, and
    message. A success is rendered too, so an unexpected success fails with
    its value. *)
let render = function
  | Ok value -> Printf.sprintf "ok %d" value
  | Error error ->
      let view = Error.view error in
      let status = match Client.rpc_status error with
        | None -> "none"
        | Some `Cancelled -> "cancelled" | Some `Unknown -> "unknown"
        | Some `Invalid_argument -> "invalid_argument"
        | Some `Deadline_exceeded -> "deadline_exceeded" | Some `Not_found -> "not_found"
        | Some `Already_exists -> "already_exists"
        | Some `Permission_denied -> "permission_denied"
        | Some `Resource_exhausted -> "resource_exhausted"
        | Some `Failed_precondition -> "failed_precondition" | Some `Aborted -> "aborted"
        | Some `Out_of_range -> "out_of_range" | Some `Unimplemented -> "unimplemented"
        | Some `Internal -> "internal" | Some `Unavailable -> "unavailable"
        | Some `Data_loss -> "data_loss" | Some `Unauthenticated -> "unauthenticated"
        | Some `Termination_outcome_uncertain -> "termination_outcome_uncertain" in
      Printf.sprintf "error kind=%s non_retryable=%b type=%s rpc=%s message=%S"
        (Error.kind error) view.non_retryable
        (Option.value ~default:"none" view.error_type) status view.message

(** Calls [operation] and fails unless it returns within {!caller_bound}
    seconds; returns the rendered result. A call that never returns is
    caught by the process alarm instead. *)
let bounded label operation =
  let started = Unix.gettimeofday () in
  let result = render (operation ()) in
  let elapsed = Unix.gettimeofday () -. started in
  if elapsed > caller_bound then
    failwith (Printf.sprintf "%s returned after %.1f s, more than %.0f s: %s"
      label elapsed caller_bound result);
  Printf.printf "%s (%.2f s): %s\n%!" label elapsed result;
  result

(** Requires a rendered value to equal [expected]. *)
let expect label expected actual =
  if actual <> expected then
    failwith (Printf.sprintf "%s: got\n    %s\n  expected\n    %s" label actual expected)

(** A pinned caller outcome: every structural field exactly, and the
    message's leading text. The public backend appends bounded source and
    type diagnostics to a Temporal failure message, so only the server's own
    sentence is pinned. *)
type outcome = { fields : string; message : string }

(** Requires a rendered caller result (see {!render}) to match [expected]. *)
let expect_outcome label expected actual =
  let prefix = Printf.sprintf "%s message=\"%s" expected.fields expected.message in
  if not (String.starts_with ~prefix actual) then
    failwith (Printf.sprintf "%s: got\n    %s\n  expected a result starting\n    %s"
      label actual prefix)

(** What every caller of an accepted, unfinished update receives once its run
    closes, however it closed. Temporal (1.32) fails the update itself with a
    non-retryable application failure of type
    [AcceptedUpdateCompletedWorkflow]. It is an update outcome, not an RPC
    error, so [rpc_status] is [None] and the category says that the update,
    not the transport, failed. *)
let accepted_outcome = {
  fields = "error kind=update non_retryable=true type=AcceptedUpdateCompletedWorkflow rpc=none";
  message = "Workflow Update failed because the Workflow completed before the Update completed.";
}

(** What the caller of an admitted, not yet accepted update receives when the
    run closes, and what a later caller of the same update ID receives.
    Temporal answers the in-flight request with [aborted] ("workflow update
    was aborted by closing workflow"). Temporal Core's client retries
    [aborted] as transient, and the retried request finds the run closed, so
    the caller sees the permanent [not_found] status. The update never ran,
    and no retry can change that. *)
let admitted_outcome = {
  fields = "error kind=bridge non_retryable=true type=NotFound rpc=not_found";
  message = "Temporal client RPC failed: not_found";
}

(** One call running on its own system thread, with the cell its rendered
    result lands in. A client call blocks only its own thread, with the
    OCaml runtime lock released (#807). *)
type background = { thread : Thread.t; result : string option ref; lock : Mutex.t }

(** Starts [operation] on a new thread, timing it as {!bounded} does. *)
let background label operation =
  let result = ref None and lock = Mutex.create () in
  let thread = Thread.create (fun () ->
    let rendered = try bounded label operation with Failure message -> "FAILED " ^ message in
    Mutex.protect lock (fun () -> result := Some rendered)) () in
  { thread; result; lock }

(** Joins a background call that must already be finishing, bounded by
    {!caller_bound}: a call still running then is reported as a hang rather
    than blocking the driver until the process alarm. *)
let join label call =
  let deadline = Unix.gettimeofday () +. caller_bound in
  let rec poll () =
    match Mutex.protect call.lock (fun () -> !(call.result)) with
    | Some result -> Thread.join call.thread; result
    | None when Unix.gettimeofday () < deadline -> Unix.sleepf 0.02; poll ()
    | None -> failwith (label ^ ": caller still blocked after the run closed") in
  poll ()

(** How a case closes its run: the body completes after a signal, the run
    is cancelled (the workflow cancels immediately, #514), or it is
    terminated. *)
type close = Complete | Cancel | Terminate

(** Names a close for labels. *)
let close_name = function
  | Complete -> "complete" | Cancel -> "cancel" | Terminate -> "terminate"

(** Closes [run] as [close] requests. *)
let close_run close run = match close with
  | Complete -> get (Client.signal ~request_id:"finish" run ~signal:finish ~input:())
  | Cancel -> get (Client.cancel run)
  | Terminate -> get (Client.terminate ~reason:"issue-520 fixture" run)

(** Renders the run's terminal result as the variant name. *)
let terminal run = match get (Client.wait run) with
  | Client.Completed { output; _ } -> Printf.sprintf "completed %d" output
  | Client.Failed _ -> "failed"
  | Client.Cancelled _ -> "cancelled"
  | Client.Terminated _ -> "terminated"
  | Client.Timed_out _ -> "timed_out"
  | Client.Continued_as_new _ -> "continued_as_new"

(** The terminal result {!Client.wait} must report for each close. *)
let expected_terminal = function
  | Complete -> "completed 7" | Cancel -> "cancelled" | Terminate -> "terminated"

(** The final durable event a closed run must end with. *)
let close_event = function
  | Complete -> "WORKFLOW_EXECUTION_COMPLETED"
  | Cancel -> "WORKFLOW_EXECUTION_CANCELED"
  | Terminate -> "WORKFLOW_EXECUTION_TERMINATED"

(** The events before the close: a cancellation request is itself durable. *)
let expected_history close =
  ["WORKFLOW_EXECUTION_STARTED"; "WORKFLOW_EXECUTION_UPDATE_ACCEPTED"]
  @ (match close with
     | Complete -> ["WORKFLOW_EXECUTION_SIGNALED"]
     | Cancel -> ["WORKFLOW_EXECUTION_CANCEL_REQUESTED"]
     | Terminate -> [])
  @ [close_event close]

(** Cases 1 to 3. The update is accepted and its handler suspended; one
    caller is already polling when the run closes, a second polls the same
    handle afterwards, and a third re-attaches by update ID afterwards. All
    three must return the same typed failure within the bound, and the
    history must show the acceptance with no completion. *)
let accepted_case ~cli ~address ~client ~queue ~track close =
  let label = "accepted+" ^ close_name close in
  let run = track (get (Client.start client ~workflow ~task_queue:queue
    ~id:(queue ^ "-" ^ close_name close) ~input:7 ())) in
  let pending = get (Client.start_update ~update_id:"held" run ~update:hold ~input:1 ()) in
  expect_history (label ^ " before close")
    ["WORKFLOW_EXECUTION_STARTED"; "WORKFLOW_EXECUTION_UPDATE_ACCEPTED"]
    (history cli address run);
  let in_flight = background (label ^ " in-flight wait_update")
    (fun () -> Client.wait_update pending) in
  close_run close run;
  expect (label ^ " terminal") (expected_terminal close) (terminal run);
  let during = join (label ^ " in-flight wait_update") in_flight in
  let after = bounded (label ^ " later wait_update") (fun () -> Client.wait_update pending) in
  let reattached = bounded (label ^ " re-attached start_update") (fun () ->
    Result.bind (Client.start_update ~update_id:"held" run ~update:hold ~input:1 ())
      Client.wait_update) in
  expect_outcome (label ^ " in-flight") accepted_outcome during;
  expect_outcome (label ^ " later") accepted_outcome after;
  expect_outcome (label ^ " re-attached") accepted_outcome reattached;
  expect_history label (expected_history close) (history cli address run)

(** Case 4. The update is admitted but not yet accepted when the run is
    terminated. Admission is made observable by stopping the run's only
    worker after its first workflow task: the run then has no pending
    workflow task, so the one [describe] reports afterwards is the
    speculative task Temporal schedules for the admitted update. *)
let admitted_case ~cli ~address ~target_url ~client ~queue ~track =
  let label = "admitted+terminate" in
  let worker_pid = spawn target_url queue in
  let run = Fun.protect ~finally:(fun () -> stop worker_pid) (fun () ->
    let run = track (get (Client.start client ~workflow ~task_queue:queue
      ~id:(queue ^ "-admitted") ~input:7 ())) in
    await "first workflow task" (fun () ->
      if List.mem "WORKFLOW_TASK_COMPLETED" (history cli address run) then Ok ()
      else Error "first workflow task not completed");
    run) in
  (match has_pending_task cli address run with
   | Ok false -> ()
   | Ok true -> failwith (label ^ ": a workflow task is pending before the update")
   | Error message -> failwith (label ^ ": describe failed: " ^ message));
  let caller = background (label ^ " start_update") (fun () ->
    Result.bind (Client.start_update ~update_id:"admitted" run ~update:hold ~input:1 ())
      Client.wait_update) in
  await "admitted update's workflow task" (fun () ->
    match has_pending_task cli address run with
    | Ok true -> Ok ()
    | Ok false -> Error "no pending workflow task"
    | Error message -> Error message);
  close_run Terminate run;
  expect (label ^ " terminal") "terminated" (terminal run);
  let during = join (label ^ " start_update") caller in
  let late = bounded (label ^ " late start_update") (fun () ->
    Result.bind (Client.start_update ~update_id:"admitted" run ~update:hold ~input:1 ())
      Client.wait_update) in
  expect_outcome label admitted_outcome during;
  expect_outcome (label ^ " late") admitted_outcome late;
  expect_history label
    ["WORKFLOW_EXECUTION_STARTED"; "WORKFLOW_EXECUTION_TERMINATED"]
    (history cli address run)

(** Runs every case with protected cleanup of workers and runs. *)
let check address cli =
  if not (String.starts_with ~prefix:"http://" address) then
    failwith "this fixture requires an HTTP development server";
  let cli_address = String.sub address 7 (String.length address - 7) in
  let tag = Printf.sprintf "update-run-close-%d-%d" (Unix.getpid ())
    (Random.State.bits (Random.State.make_self_init ())) in
  let queue = tag ^ "-accepted" in
  let worker_pid = spawn address queue in
  Fun.protect ~finally:(fun () -> stop worker_pid) (fun () ->
    let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
    Fun.protect ~finally:(fun () -> ignore (Client.shutdown client)) (fun () ->
      (* Every started run is terminated on exit; terminating a closed run
         is a harmless typed error. *)
      let cleanup = ref [] in
      let track run = cleanup := (fun () -> ignore (Client.terminate run)) :: !cleanup; run in
      Fun.protect ~finally:(fun () -> List.iter (fun terminate -> terminate ()) !cleanup)
        (fun () ->
          List.iter (accepted_case ~cli ~address:cli_address ~client ~queue ~track)
            [Complete; Cancel; Terminate];
          admitted_case ~cli ~address:cli_address ~target_url:address ~client
            ~queue:(tag ^ "-admitted") ~track;
          print_endline "update run close live regression: ok")))

(** An overall alarm bounds the driver, so a caller that never returns fails
    the suite with a named error; protected cleanup reaps the workers. The
    CLI path is explicit so the test cannot invoke an unrelated wrapper. *)
let () = match Array.to_list Sys.argv with
  | [_; "worker"; address; queue] -> worker address queue
  | [_; "check"; address; cli] ->
      Sys.set_signal Sys.sigalrm (Sys.Signal_handle (fun _ -> failwith "fixture timed out"));
      ignore (Unix.alarm 150);
      Fun.protect ~finally:(fun () -> ignore (Unix.alarm 0)) (fun () -> check address cli)
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"
