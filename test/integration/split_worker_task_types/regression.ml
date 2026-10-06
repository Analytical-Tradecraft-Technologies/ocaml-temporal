(** Live #805 regression for the split-worker deployment shape shipped
    in [examples/]. A workflow-only worker and an activity-only worker share
    one fresh task queue. Before #805 the workflow-only worker also polled
    activity tasks and failed them non-retryably as unregistered, so most
    executions failed. Every execution must now complete.

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
         ~workflows:[ Worker.workflow compose ] ~activities:[] ())
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
          | Client.Completed output when output = "rendered:" ^ input -> ()
          | Client.Completed _ -> failwith "split worker returned a wrong result"
          | _ -> failwith "split worker execution did not complete")
        handles;
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
        "split worker task types live regression: %d executions completed\n%!"
        executions
  | Error exception_ -> raise exception_

let () =
  match Sys.argv with
  | [| _; "check"; address |] -> check address
  | _ ->
      prerr_endline "usage: regression.exe check TEMPORAL_ADDRESS";
      exit 2
