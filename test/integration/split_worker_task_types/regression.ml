(** Live #805 regression for the split-worker deployment shape shipped
    in [examples/]. A workflow-only worker and an activity-only worker share
    one fresh task queue. Before #805 the workflow-only worker also polled
    activity tasks and failed them non-retryably as unregistered, so most
    executions failed. Every execution must now complete.

    The workflow-only worker also runs a workflow that schedules a local
    activity nobody registered (#1211). Local activities run on the worker
    that scheduled them, so the workflow-only worker must poll the task and
    reject it as unregistered, and the workflow sees that failure at once.
    Before the fix that worker ran no activity lane: the task was never
    polled, its workflow lane spun at full CPU, and the run never finished.

    Run against a disposable Temporal Server only:
    [dune exec test/integration/split_worker_task_types/regression.exe -- check
    http://127.0.0.1:7233]. [TEMPORAL_NAMESPACE] defaults to
    [temporal-sdk-test]. CI runs it through
    [make test-temporal-live-regressions]. *)
open Temporal

(** Converts the public result boundary into a short fixture failure. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

let namespace =
  Sys.getenv_opt "TEMPORAL_NAMESPACE"
  |> Option.value ~default:"temporal-sdk-test"

(** Number of executions started. One misrouted activity fails its workflow,
    so a handful of runs makes a regression effectively certain to surface. *)
let executions = 8

(** Executed only by the activity-only worker. *)
let render =
  Activity.define ~name:"split-worker.render" ~input:Codec.string
    ~output:Codec.string (fun input -> Ok ("rendered:" ^ input))

(** Executed only by the workflow-only worker; its activity is dispatched
    through the server to whichever worker polls activity tasks. *)
let compose =
  Workflow.define ~name:"split-worker.compose" ~input:Codec.string
    ~output:Codec.string (fun input -> Activity.execute render input)

(** Registered with no worker, so a local activity of this type can only be
    rejected as unregistered by the worker that schedules it. *)
let unregistered =
  Activity.define ~name:"split-worker.unregistered" ~input:Codec.string
    ~output:Codec.string (fun input -> Ok input)

(** Executed only by the workflow-only worker. It schedules [unregistered] as a
    single-attempt local activity and reports how it failed, so the run
    completes as soon as that worker rejects the task (#1211). A task nobody
    polls fails only when its start-to-close timeout expires, which is
    reported as ["timed out"], so that regression cannot pass. *)
let local_probe =
  Workflow.define ~name:"split-worker.unregistered-local" ~input:Codec.string
    ~output:Codec.string (fun input ->
      let retry_policy =
        get
          (Retry_policy.make ~initial_interval:(Duration.of_ms 1_000L)
             ~backoff_coefficient:2.0 ~maximum_interval:(Duration.of_ms 1_000L)
             ~maximum_attempts:1 ())
      in
      match
        Activity.execute_local ~start_to_close_timeout:(Duration.of_seconds 10L)
          ~retry_policy unregistered input
      with
      | Ok _ -> Ok "unexpectedly ran"
      | Error error when Option.is_some (Error.timeout_type error) ->
          Ok "timed out"
      | Error _ -> Ok "rejected")

(** Runs one worker on its own Domain so both share this process. *)
let start_worker worker = Domain.spawn (fun () -> Worker.run worker)

(** Starts every execution, waits for each exact run, and always shuts both
    workers down. A process-wide alarm bounds a hung server or worker. *)
let check address =
  ignore (Unix.alarm 120);
  let queue =
    Printf.sprintf "split-worker-%d-%d" (Unix.getpid ())
      (Random.State.bits (Random.State.make_self_init ()))
  in
  let workflow_worker =
    get
      (Worker.create ~target_url:address ~namespace ~task_queue:queue
         ~identity:"split-worker-workflows"
         ~workflows:[ Worker.workflow compose; Worker.workflow local_probe ]
         ~activities:[] ())
  in
  let activity_worker =
    get
      (Worker.create ~target_url:address ~namespace ~task_queue:queue
         ~identity:"split-worker-activities" ~workflows:[]
         ~activities:[ Worker.activity render ] ())
  in
  let running = [ start_worker workflow_worker; start_worker activity_worker ] in
  let client = get (Client.create ~target_url:address ~namespace ()) in
  let body =
    try
      let handles =
        List.init executions (fun index ->
            let input = string_of_int index in
            ( input,
              get
                (Client.start client ~workflow:compose ~task_queue:queue
                   ~id:(Printf.sprintf "%s-%d" queue index) ~input ()) ))
      in
      List.iter
        (fun (input, handle) ->
          match get (Client.wait handle) with
          | Client.Completed { output; _ } when output = "rendered:" ^ input -> ()
          | Client.Completed _ -> failwith "split worker returned a wrong result"
          | _ -> failwith "split worker execution did not complete")
        handles;
      let probe =
        get
          (Client.start client ~workflow:local_probe ~task_queue:queue
             ~id:(queue ^ "-unregistered-local") ~input:"probe" ())
      in
      (match get (Client.wait ~timeout:(Duration.of_seconds 30L) probe) with
      | Client.Completed { output = "rejected"; _ } -> ()
      | Client.Completed { output; _ } ->
          failwith ("unregistered local activity probe returned " ^ output)
      | _ -> failwith "unregistered local activity probe did not complete");
      Ok ()
    with exception_ -> Error exception_
  in
  ignore (Client.shutdown client);
  ignore (get (Worker.shutdown workflow_worker));
  ignore (get (Worker.shutdown activity_worker));
  List.iter (fun domain -> ignore (Domain.join domain)) running;
  match body with
  | Ok () ->
      Printf.printf
        "split worker task types live regression: %d executions completed, \
         unregistered local activity rejected\n%!"
        executions
  | Error exception_ -> raise exception_

let () =
  match Sys.argv with
  | [| _; "check"; address |] -> check address
  | _ ->
      prerr_endline "usage: regression.exe check TEMPORAL_ADDRESS";
      exit 2
