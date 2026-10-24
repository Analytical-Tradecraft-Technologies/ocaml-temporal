(** Self-checking cooperative cancellation example (#494).

    One process runs a worker for the activities and workflows in
    {!Cooperative_cleanup} and a client driver that starts four executions
    and checks their outcomes against the reservation ledger:

    + A workflow cancels a running synchronous activity. The activity must
      release its reservation and then acknowledge the cancellation, and the
      workflow must see the activity as cancelled.
    + The same with an asynchronous activity, whose completer thread learns
      of the cancellation from [Async_handle.heartbeat].
    + The worker is shut down while an activity runs. The activity must
      release its reservation and fail retryably; a replacement worker then
      runs the retry, which reacquires the same reservation key and commits
      it. The synchronous activity is used here; the asynchronous form
      observes the same shutdown through its heartbeat result (#1157), which
      the bounded-shutdown live regression verifies.
    + A new run reuses the closed run's workflow ID. Its activity must make
      and commit its own reservation, because the key includes the run ID.

    It prints one line per scenario and exits 0 only if all four behaved as
    described. The executions use the task queue [TEMPORAL_TASK_QUEUE] with
    a [-cancellation] suffix, so this example never shares a queue with the
    other example workers, and fresh workflow IDs, so it can be rerun in the
    same namespace. *)

(** Returns a defect describing a failed example check. *)
let check_failed message =
  Temporal.Error.defect ~message:("cancellation example: " ^ message)

(** A worker whose [Worker.run] loop runs on its own Domain. *)
type running_worker = {
  worker : Temporal.Worker.t;
  domain : (unit, Temporal.Error.t) result Domain.t;
  mutable stopped : bool;
}

(** Shuts [running] down and waits for its run loop, once. Shutdown waits
    for running activity callbacks for up to the worker's graceful shutdown
    period, so a cooperating activity can release its reservation and report
    its outcome first. *)
let stop running =
  if running.stopped then Ok ()
  else begin
    running.stopped <- true;
    let shutdown_result =
      Example_support.Lifecycle.protect_result "worker shutdown" (fun () ->
          Temporal.Worker.shutdown running.worker)
    in
    let run_result = Domain.join running.domain in
    Result.bind run_result (fun () -> shutdown_result)
  end

(** Creates a worker registering every activity and workflow of the example,
    runs it on a new Domain, calls [use] with it, and always stops it
    afterwards. [use] may stop the worker early with {!stop}. *)
let with_worker (config : Example_support.Config.t) ~task_queue ~identity use =
  let open Temporal.Result_syntax in
  let* worker =
    Temporal.Worker.create ~target_url:config.target_url
      ~namespace:config.namespace ~identity ~task_queue
      ~workflows:
        [
          Temporal.Worker.workflow
            Cooperative_cleanup.reserve_then_cancel_workflow;
          Temporal.Worker.workflow
            Cooperative_cleanup.reserve_and_commit_workflow;
        ]
      ~activities:
        [
          Temporal.Worker.activity Cooperative_cleanup.hold_reservation;
          Temporal.Worker.activity Cooperative_cleanup.hold_reservation_async;
        ]
      ()
  in
  let domain =
    Domain.spawn (fun () ->
        Example_support.Lifecycle.protect_result "worker run" (fun () ->
            Temporal.Worker.run worker))
  in
  let running = { worker; domain; stopped = false } in
  let result =
    Example_support.Lifecycle.protect_result "example scenario" (fun () ->
        use running)
  in
  let stop_result = stop running in
  let* value = result in
  let* () = stop_result in
  Ok value

(** Starts [workflow] with [input] under the workflow ID [id]. The execution
    timeout ends a run that a broken worker would otherwise leave open. *)
let start client ~task_queue ~workflow ~id input =
  Temporal.Client.start client
    ~execution_timeout:(Temporal.Duration.of_ms 120_000L) ~workflow ~task_queue
    ~id ~input ()

(** Waits for one exact run and returns its output, turning every other
    terminal state into an error. *)
let wait_completed handle =
  let open Temporal.Result_syntax in
  let* terminal = Temporal.Client.wait handle in
  match terminal with
  | Temporal.Client.Completed { output; _ } -> Ok output
  | Failed { error; _ } | Timed_out { error; _ } ->
      Error (check_failed ("workflow failed: " ^ Temporal.Error.message error))
  | Cancelled error | Terminated error ->
      Error (check_failed ("workflow ended: " ^ Temporal.Error.message error))
  | Continued_as_new _ -> Error (check_failed "workflow continued as new")

(** Returns the ledger key of the reservation activity in the exact run
    that [handle] addresses, matching [Cooperative_cleanup.reservation_key]. *)
let key_of handle =
  match Temporal.Client.run_id handle with
  | Some run_id ->
      Ok
        (String.concat "/"
           [
             Temporal.Client.workflow_id handle;
             run_id;
             Cooperative_cleanup.reservation_activity_id;
           ])
  | None -> Error (check_failed "a started workflow handle has no run ID")

(** Describes a reservation history for a failed check. *)
let describe_history history =
  let reason = function
    | Cooperative_cleanup.Ledger.Cancellation_requested -> "cancellation"
    | Worker_stopping -> "worker shutdown"
    | Operator_interrupted -> "pause or reset"
    | Attempt_lost -> "attempt lost"
  in
  let event = function
    | Cooperative_cleanup.Ledger.Acquired { attempt } ->
        Printf.sprintf "acquired by attempt %d" attempt
    | Released { attempt; reason = r } ->
        Printf.sprintf "released by attempt %d (%s)" attempt (reason r)
    | Committed { attempt } -> Printf.sprintf "committed by attempt %d" attempt
  in
  match history with
  | [] -> "no reservation was made"
  | events -> String.concat ", " (List.map event events)

(** Scenario 1 and 2: the workflow cancels a running activity ([mode] is
    ["sync"] or ["async"]). Checks that the workflow saw a cancelled
    activity and that the only attempt acquired the reservation and then
    released it because of the cancellation. *)
let cancellation_scenario client ~task_queue ~suffix mode =
  let open Temporal.Result_syntax in
  let id =
    Printf.sprintf "ocaml-temporal-example-cancel-%s-%s" mode suffix
  in
  let* handle =
    start client ~task_queue
      ~workflow:Cooperative_cleanup.reserve_then_cancel_workflow ~id mode
  in
  let* key = key_of handle in
  let* output = wait_completed handle in
  let* () =
    if String.equal output "cancelled" then Ok ()
    else Error (check_failed (mode ^ " activity was not cancelled: " ^ output))
  in
  match Cooperative_cleanup.Ledger.history key with
  | [ Acquired { attempt = 1 };
      Released { attempt = 1; reason = Cancellation_requested } ] ->
      Ok ()
  | history ->
      Error
        (check_failed
           (Printf.sprintf "%s activity did not clean up as expected: %s" mode
              (describe_history history)))

(** Polls until the reservation [key] is held, so the worker is shut down
    while the activity is running. Gives up after a minute. *)
let wait_until_held key =
  let deadline = Unix.gettimeofday () +. 60. in
  let rec poll () =
    match Cooperative_cleanup.Ledger.state key with
    | Some Held -> Ok ()
    | _ when Unix.gettimeofday () > deadline ->
        Error (check_failed "the reservation activity never started")
    | _ ->
        Unix.sleepf 0.05;
        poll ()
  in
  poll ()

(** Scenario 4: starts a new run under the closed run's workflow ID [id],
    which [Client.start]'s default reuse policy allows. The new run's
    activity has the same explicit activity ID, but its reservation key
    includes the new run ID, so it must make and commit a reservation of its
    own rather than find the old run's committed one and skip the work.
    [old_key] is the previous run's key, whose history must not change. *)
let reuse_scenario client ~task_queue ~id ~old_key =
  let open Temporal.Result_syntax in
  let old_history = Cooperative_cleanup.Ledger.history old_key in
  let* handle =
    start client ~task_queue
      ~workflow:Cooperative_cleanup.reserve_and_commit_workflow ~id 3
  in
  let* key = key_of handle in
  let* _output = wait_completed handle in
  let* () =
    if Cooperative_cleanup.Ledger.history old_key = old_history then Ok ()
    else Error (check_failed "the reused workflow ID changed the old reservation")
  in
  match Cooperative_cleanup.Ledger.history key with
  | [ Acquired { attempt = 1 }; Committed { attempt = 1 } ]
    when not (String.equal key old_key) ->
      Ok ()
  | history ->
      Error
        (check_failed
           ("the run with a reused workflow ID did not do its own work: "
           ^ describe_history history))

(** Scenario 3: shuts [first] down while its activity holds a reservation,
    then starts a replacement worker. Checks that the first attempt
    released the reservation because its worker was stopping, and that a
    later attempt on the replacement reacquired and committed it. Scenario 4
    ({!reuse_scenario}) then runs on the replacement worker. *)
let shutdown_scenario config client ~task_queue ~suffix first =
  let open Temporal.Result_syntax in
  let id = "ocaml-temporal-example-shutdown-" ^ suffix in
  let* handle =
    start client ~task_queue
      ~workflow:Cooperative_cleanup.reserve_and_commit_workflow ~id 30
  in
  let* key = key_of handle in
  let* () = wait_until_held key in
  let* () = stop first in
  with_worker config ~task_queue
    ~identity:"ocaml-temporal-example-cancellation-replacement" (fun _ ->
      let* _output = wait_completed handle in
      let* () =
        match Cooperative_cleanup.Ledger.history key with
        | [ Acquired { attempt = 1 };
            Released { attempt = 1; reason = Worker_stopping };
            Acquired { attempt = retry };
            Committed { attempt = committed } ]
          when retry > 1 && committed = retry ->
            Ok ()
        | history ->
            Error
              (check_failed
                 ("the reservation did not survive the worker shutdown: "
                 ^ describe_history history))
      in
      print_endline
        "Worker shutdown: the activity released its reservation, and a \
         replacement worker retried and committed it.";
      reuse_scenario client ~task_queue ~id ~old_key:key)

(** Runs the four scenarios in order and prints one line for each. *)
let run () =
  let open Temporal.Result_syntax in
  let* config = Example_support.Config.from_environment () in
  let task_queue = config.task_queue ^ "-cancellation" in
  let suffix = Printf.sprintf "%.0f-%d" (Unix.time ()) (Unix.getpid ()) in
  Example_support.Lifecycle.with_client config (fun client ->
      with_worker config ~task_queue
        ~identity:"ocaml-temporal-example-cancellation-worker" (fun worker ->
          let* () = cancellation_scenario client ~task_queue ~suffix "sync" in
          print_endline
            "Cancelled activity: released its reservation, then acknowledged \
             the cancellation.";
          let* () = cancellation_scenario client ~task_queue ~suffix "async" in
          print_endline
            "Cancelled asynchronous activity: released its reservation, then \
             acknowledged the cancellation.";
          let* () = shutdown_scenario config client ~task_queue ~suffix worker in
          print_endline
            "Reused workflow ID: the new run made and committed its own \
             reservation.";
          Ok ()))

(** Reports the outcome after every worker and the client have been shut
    down. *)
let () =
  match run () with
  | Ok () -> print_endline "Cooperative cancellation example passed."
  | Error error ->
      Example_support.report_error "cancellation example" error;
      exit 1
