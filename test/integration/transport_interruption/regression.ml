(** Live transport interruption regression (#504).

    A client and a worker reach Temporal through two separate in-process
    {!Fault_proxy} instances, while an independent direct client observes the
    durable server state. Each scenario injects one fault at a known request
    boundary (server refused, request applied but acknowledgement lost, or a
    delayed response) and then checks three things: the SDK reports the
    failure as a typed [Error.t] value, never an exception; an outcome the
    server may have applied is reported as uncertain or retryable rather than
    as a definite rejection; and the durable outcome reconciled through the
    direct client matches exactly one application of the operation.

    The fixture owns its worker Domain, uses a unique task queue, terminates
    every execution it creates, and requires every proxied socket to be closed
    after client and worker shutdown. Use only a disposable Temporal Server
    with the [default] namespace. The official Temporal CLI path is an
    explicit argument; it only reads this fixture's own activity history. *)
open Temporal

let namespace = "default"

(** Converts a setup failure at the public result boundary into a fixture
    failure with the SDK's own message. *)
let get label = function
  | Ok value -> value
  | Error error -> failwith (label ^ ": " ^ Error.message error)

(** A numeric wire codec shared by the workflow, signals, updates and query. *)
let number =
  Codec.make ~encoding:"json/plain"
    ~encode:(fun value -> Ok (Bytes.of_string (string_of_int value)))
    ~decode:(fun bytes ->
      match int_of_string_opt (Bytes.to_string bytes) with
      | Some value -> Ok value
      | None -> Error (Error.codec ~message:"invalid counter value"))

(** Workflow-local counter; each execution owns an independent value. *)
let state = Workflow_context.Local.create ()

(** Reads the counter without creating commands. *)
let read () = Result.map (Option.value ~default:0) (Workflow_context.Local.get state)

(** Adds a signal or update amount and returns the new counter. *)
let add amount = Result.bind (read ()) (fun value ->
  Result.bind (Workflow_context.Local.set state (value + amount)) read)

(** Completes with the counter once it reaches the input target. A target of
    zero completes on the first workflow task. Every signal and update must be
    applied exactly once for the counter to equal the expected sum. *)
let counter =
  Workflow.define ~name:"transport-interruption.counter" ~input:number ~output:number
    (fun target ->
      Result.bind (Workflow_context.Local.set state 0) (fun () ->
        Result.bind
          (Condition.wait_until (fun () ->
               match read () with Ok value -> value >= target | Error _ -> true))
          read))

(** Message definitions: a signal and an update each add their amount, and the
    query reads the counter that reconciliation compares against. *)
let signal = Signal.define ~name:"add" ~input:number
let query = Query.define ~name:"get" ~output:number
let update = Update.define ~name:"add" ~input:number ~output:number

(** Activity callback gate and invocation counter. They belong to this
    process and are never read by workflow code, so replay sees only the
    durable activity result. *)
let activity_entered = Atomic.make false
let release_activity = Atomic.make false
let activity_invocations = Atomic.make 0

(** Counts every callback dispatch, then holds the activity until the driver
    has switched the worker proxy to drop the completion acknowledgement. *)
let side_effect =
  Activity.define ~name:"transport-interruption.side-effect" ~input:number
    ~output:number (fun input ->
      Atomic.incr activity_invocations;
      Atomic.set activity_entered true;
      while not (Atomic.get release_activity) do Thread.delay 0.01 done;
      Ok (input + 1))

(** Runs one activity with a generous start-to-close timeout, so a lost
    completion is resolved by the SDK's retained retry rather than by the
    server redelivering the activity to a second callback. *)
let activity_workflow =
  Workflow.define ~name:"transport-interruption.activity" ~input:number ~output:number
    (fun input ->
      Activity.execute ~start_to_close_timeout:(Duration.of_ms 120_000L)
        side_effect input)

(** Splits ["http://host:port"] into the proxy's upstream address. *)
let upstream address =
  let prefix = "http://" in
  if not (String.starts_with ~prefix address) then
    failwith "transport interruption fixture requires an http:// server URL";
  let authority =
    String.sub address (String.length prefix) (String.length address - String.length prefix)
  in
  let authority =
    match String.index_opt authority '/' with
    | Some slash -> String.sub authority 0 slash
    | None -> authority
  in
  match String.rindex_opt authority ':' with
  | Some colon ->
      ( String.sub authority 0 colon,
        int_of_string (String.sub authority (colon + 1) (String.length authority - colon - 1)) )
  | None -> (authority, 7233)

(** Renders the public classification of one observed error for the log. *)
let describe error =
  let view = Error.view error in
  Printf.sprintf "kind=%s error_type=%s non_retryable=%b message=%S"
    (Error.kind error)
    (Option.value ~default:"-" view.error_type)
    view.non_retryable view.message

(** Runs [f], logs its duration, and fails when it took longer than [limit]
    seconds. Operations under a fault must end within their documented
    deadline; the outer controller timeout catches a call that never returns. *)
let timed label limit f =
  let started = Unix.gettimeofday () in
  let result = f () in
  let elapsed = Unix.gettimeofday () -. started in
  Fault_proxy.log "operation=%s elapsed=%.3fs" label elapsed;
  if elapsed > limit then
    failwith (Printf.sprintf "%s took %.1fs, beyond its %.0fs bound" label elapsed limit);
  result

(** Runs [f] on a helper Domain so the driver can inject a fault while the
    call is in flight. Exceptions are captured and re-raised by [join]. *)
let in_background f =
  Domain.spawn (fun () -> match f () with value -> Ok value | exception e -> Error e)

(** Joins an [in_background] call, re-raising a fixture exception. *)
let join domain = match Domain.join domain with Ok value -> value | Error e -> raise e

(** Polls [predicate] every 10 ms until it holds or [seconds] elapse. *)
let await label seconds predicate =
  let deadline = Unix.gettimeofday () +. seconds in
  while not (predicate ()) do
    if Unix.gettimeofday () >= deadline then failwith ("timed out waiting for " ^ label);
    Thread.delay 0.01
  done

(** Requires a typed [Error] and returns it; success under a fault that
    prevents any acknowledgement would mean the SDK invented an answer. *)
let expect_error label = function
  | Ok _ -> failwith (label ^ ": succeeded although its acknowledgement was lost")
  | Error error ->
      Fault_proxy.log "operation=%s outcome=error %s" label (describe error);
      error

(** Requires a transient RPC classification: the caller may retry the same
    logical request, so the error must not claim a permanent rejection. *)
let expect_retryable_rpc label error =
  match Client.rpc_status error with
  | Some (`Deadline_exceeded | `Unavailable | `Cancelled | `Unknown)
    when not (Error.view error).non_retryable -> ()
  | _ -> failwith (label ^ ": expected a retryable transport status, got " ^ describe error)

(** Scenario 1: a server that refuses connections makes [Client.create] return
    a typed error promptly, and the same target works once it returns. *)
let client_create_refused proxy =
  Fault_proxy.set_mode proxy Fault_proxy.Refuse;
  let result =
    timed "client_create_refused" 30. (fun () ->
      Client.create ~target_url:(Fault_proxy.url proxy) ~namespace ())
  in
  (match result with
  | Ok client ->
      ignore (Client.shutdown client);
      failwith "client_create_refused: connected through a refusing proxy"
  | Error error ->
      Fault_proxy.log "operation=client_create_refused outcome=error %s" (describe error);
      if (Error.view error).category <> `Bridge then
        failwith ("client_create_refused: expected a bridge error, got " ^ describe error));
  Fault_proxy.restore proxy;
  get "client create after restore"
    (Client.create ~target_url:(Fault_proxy.url proxy) ~namespace ())

(** Scenario 2: the start request reaches Temporal but its response is lost.
    The SDK must report an uncertain start naming the request ID, not a
    rejection. Reconciliation then proves the server did create the run: a
    start with another request ID is rejected as already started with that
    run, and a retry with the original request ID returns the same run. *)
let start_ack_lost ~proxy ~client ~direct ~queue ~track =
  let id = queue ^ "-start-ack-lost" in
  let request_id = queue ^ "-start-request" in
  Fault_proxy.set_mode proxy Fault_proxy.Drop_responses;
  let error =
    timed "start_ack_lost" 20. (fun () ->
      Client.start client ~request_id ~workflow:counter ~task_queue:queue ~id ~input:100 ())
    |> expect_error "start_ack_lost"
  in
  Fault_proxy.restore proxy;
  let view = Error.view error in
  if view.category <> `Bridge || not view.non_retryable
     || Client.rpc_status error <> None || Client.already_started error <> None
  then failwith ("start_ack_lost: not classified as an uncertain start: " ^ describe error);
  (* The uncertain error must carry the identity needed for reconciliation. *)
  let contains needle =
    let n = String.length needle and m = String.length view.message in
    let rec at i = i + n <= m && (String.sub view.message i n = needle || at (i + 1)) in
    at 0
  in
  if not (contains request_id && contains id) then
    failwith "start_ack_lost: uncertain start omitted its request or workflow ID";
  let accepted_run =
    match Client.start direct ~request_id:(queue ^ "-probe") ~workflow:counter
            ~task_queue:queue ~id ~input:100 () with
    | Error error -> (
        match Client.already_started error with
        | Some execution -> execution.Client.run_id
        | None -> failwith ("start_ack_lost: unexpected probe error " ^ describe error))
    | Ok handle ->
        ignore (track handle);
        failwith "start_ack_lost: the uncertain start had not been applied"
  in
  let handle =
    track (get "start retry with the same request ID"
      (Client.start client ~request_id ~workflow:counter ~task_queue:queue ~id ~input:100 ()))
  in
  if Client.run_id handle <> accepted_run then
    failwith "start_ack_lost: request-ID retry returned a different run";
  if not (Client.started handle) then
    failwith "start_ack_lost: request-ID retry was not reported as the creating start";
  Fault_proxy.log "scenario=start_ack_lost run_id=%s reconciled=accepted" accepted_run;
  handle

(** Scenario 3: a signal is forwarded but its acknowledgement is lost. The
    bounded control-plane deadline returns a retryable typed error; retrying
    with the same request ID succeeds and Temporal applies the signal once. *)
let signal_ack_lost ~proxy ~direct_handle ~handle =
  let request_id = Client.workflow_id handle ^ "-signal" in
  Fault_proxy.set_mode proxy Fault_proxy.Drop_responses;
  let error =
    timed "signal_ack_lost" 10. (fun () ->
      Client.signal ~request_id handle ~signal ~input:5)
    |> expect_error "signal_ack_lost"
  in
  Fault_proxy.restore proxy;
  expect_retryable_rpc "signal_ack_lost" error;
  get "signal retry" (Client.signal ~request_id handle ~signal ~input:5);
  let value = get "query after signal retry" (Client.query direct_handle ~query) in
  Fault_proxy.log "scenario=signal_ack_lost request_id=%s counter=%d" request_id value;
  if value <> 5 then
    failwith (Printf.sprintf "signal_ack_lost: expected one application (5), got %d" value)

(** Scenario 4: the update response is lost and delivered only after a
    connection reset. The fault is proven before it ends: the direct client
    observes the update applied (the worker's path is not faulted) while
    [start_update] is still blocked and the proxy has discarded response
    bytes. Core then re-sends the identical update ID on a new connection
    within the same acceptance budget, so the caller sees one accepted update
    and the workflow applies it once. *)
let update_response_delayed ~proxy ~direct_handle ~handle =
  let update_id = Client.workflow_id handle ^ "-update" in
  let dropped_before = Fault_proxy.dropped_bytes proxy in
  let returned = Atomic.make false in
  Fault_proxy.set_mode proxy Fault_proxy.Drop_responses;
  let pending =
    in_background (fun () ->
      let result =
        timed "update_response_delayed" 35. (fun () ->
          Client.start_update ~update_id handle ~update ~input:7 ())
      in
      Atomic.set returned true;
      result)
  in
  (* Bounded so a fault that never reaches the server fails here instead of
     passing after recovery without exercising the lost response. *)
  await "update applied while its response is dropped" 20. (fun () ->
    match Client.query direct_handle ~query with
    | Ok 12 -> true
    | Ok _ | Error _ -> false);
  let dropped = Fault_proxy.dropped_bytes proxy - dropped_before in
  if Atomic.get returned then
    failwith "update_response_delayed: start_update returned while responses were dropped";
  if dropped <= 0 then
    failwith "update_response_delayed: the proxy discarded no response bytes";
  Fault_proxy.log "scenario=update_response_delayed fault_exercised=true dropped_bytes=%d"
    dropped;
  Fault_proxy.restore proxy;
  let accepted = get "update after delayed response" (join pending) in
  let value = get "wait update" (Client.wait_update accepted) in
  let observed = get "query after update" (Client.query direct_handle ~query) in
  Fault_proxy.log "scenario=update_response_delayed update_id=%s result=%d counter=%d"
    update_id value observed;
  if value <> 12 || observed <> 12 then
    failwith "update_response_delayed: update was not applied exactly once"

(** Scenario 5: an exact-run wait survives a server outage. The wait is in
    flight when the proxy refuses all traffic; after the server returns, a
    direct signal completes the run and the original wait observes it. *)
let wait_across_outage ~proxy ~direct_handle ~handle =
  let waiting = in_background (fun () -> Client.wait handle) in
  Thread.delay 0.5;
  Fault_proxy.set_mode proxy Fault_proxy.Refuse;
  Thread.delay 3.;
  Fault_proxy.restore proxy;
  get "completing signal" (Client.signal direct_handle ~signal ~input:88);
  match timed "wait_across_outage" 60. (fun () -> join waiting) with
  | Ok (Client.Completed 100) ->
      Fault_proxy.log "scenario=wait_across_outage outcome=completed counter=100"
  | Ok _ -> failwith "wait_across_outage: unexpected terminal outcome"
  | Error error -> failwith ("wait_across_outage: wait failed: " ^ describe error)

(** Scenario 6: a terminate request is applied but its acknowledgement is
    lost. Terminate has no idempotency key, so the SDK must report the
    explicit uncertain outcome; the run's history then proves it was applied. *)
let terminate_ack_lost ~proxy ~client ~direct ~queue ~track =
  let direct_handle =
    track (get "terminate fixture start"
      (Client.start direct ~workflow:counter ~task_queue:queue
         ~id:(queue ^ "-terminate-ack-lost") ~input:100 ()))
  in
  let handle =
    get "terminate follow"
      (Client.follow client ~workflow:counter
         { Client.namespace; workflow_id = Client.workflow_id direct_handle;
           run_id = Client.run_id direct_handle })
  in
  (* The proxied connection must exist before responses are dropped. *)
  ignore (get "terminate warm-up query" (Client.query handle ~query));
  Fault_proxy.set_mode proxy Fault_proxy.Drop_responses;
  let error =
    timed "terminate_ack_lost" 10. (fun () -> Client.terminate ~reason:"transport fault" handle)
    |> expect_error "terminate_ack_lost"
  in
  Fault_proxy.restore proxy;
  if Client.rpc_status error <> Some `Termination_outcome_uncertain
     || not (Error.view error).non_retryable
  then failwith ("terminate_ack_lost: expected an uncertain outcome, got " ^ describe error);
  match timed "terminate reconciliation" 30. (fun () -> Client.wait direct_handle) with
  | Ok (Client.Terminated _) ->
      Fault_proxy.log "scenario=terminate_ack_lost run_id=%s reconciled=terminated"
        (Client.run_id direct_handle)
  | Ok _ -> failwith "terminate_ack_lost: uncertain terminate was not applied"
  | Error error -> failwith ("terminate_ack_lost: reconciliation failed: " ^ describe error)

(** Scenario 7: the worker's pollers lose the server while a workflow is
    started. The worker must keep running and complete the run after the
    server returns. *)
let worker_poll_outage ~proxy ~direct ~queue ~track ~worker_stopped =
  Fault_proxy.set_mode proxy Fault_proxy.Refuse;
  let handle =
    track (get "poll outage start"
      (timed "poll outage direct start" 15. (fun () ->
         Client.start direct ~workflow:counter ~task_queue:queue
           ~id:(queue ^ "-poll-outage") ~input:0 ())))
  in
  Thread.delay 3.;
  if Atomic.get worker_stopped then
    failwith "worker_poll_outage: Worker.run returned while the server was unavailable";
  Fault_proxy.restore proxy;
  match timed "worker_poll_outage" 60. (fun () -> Client.wait handle) with
  | Ok (Client.Completed 0) ->
      Fault_proxy.log "scenario=worker_poll_outage run_id=%s outcome=completed"
        (Client.run_id handle)
  | Ok _ -> failwith "worker_poll_outage: unexpected terminal outcome"
  | Error error -> failwith ("worker_poll_outage: wait failed: " ^ describe error)

(** Fetches the exact run's history event types through the official CLI,
    without a shell. A failed or unparsable read is [None] so the caller can
    poll again; the CLI's own diagnostic goes to the inherited stderr. *)
let history_event_types cli address handle =
  let address = String.sub address 7 (String.length address - 7) in
  let args = [| cli; "--address"; address; "--namespace"; namespace;
    "--command-timeout"; "30s"; "workflow"; "show";
    "--workflow-id"; Client.workflow_id handle; "--run-id"; Client.run_id handle;
    "--output"; "json" |] in
  let input = Unix.open_process_args_in cli args in
  let document =
    match Yojson.Basic.from_channel input with
    | document -> Some document
    | exception Yojson.Json_error _ -> None
  in
  match (Unix.close_process_in input, document) with
  | Unix.WEXITED 0, Some document -> (
      try
        Some Yojson.Basic.Util.(
          document |> member "events" |> to_list
          |> List.map (fun event -> event |> member "eventType" |> to_string))
      with Yojson.Basic.Util.Type_error _ -> None)
  | _ -> None

(** Scenario 8: the activity completion reaches Temporal but its
    acknowledgement is lost. Before the fault ends, the server history must
    already record [ActivityTaskCompleted] and the worker proxy must have
    discarded response bytes, so the scenario cannot pass by completing the
    activity only after recovery. The worker retains and re-sends the
    completion after reconnecting without dispatching the callback again, and
    the workflow reaches its result. *)
let activity_completion_ack_lost ~proxy ~direct ~queue ~track ~cli ~address =
  let handle =
    track (get "activity fixture start"
      (Client.start direct ~workflow:activity_workflow ~task_queue:queue
         ~id:(queue ^ "-activity-ack-lost") ~input:41 ()))
  in
  await "activity callback" 30. (fun () -> Atomic.get activity_entered);
  let dropped_before = Fault_proxy.dropped_bytes proxy in
  Fault_proxy.set_mode proxy Fault_proxy.Drop_responses;
  Atomic.set release_activity true;
  (* Bounded: a completion that never reaches the server while responses are
     dropped means the lost acknowledgement was not exercised. *)
  await "durable ActivityTaskCompleted while the acknowledgement is dropped" 30.
    (fun () ->
      match history_event_types cli address handle with
      | Some types -> List.mem "EVENT_TYPE_ACTIVITY_TASK_COMPLETED" types
      | None -> false);
  let dropped = Fault_proxy.dropped_bytes proxy - dropped_before in
  if dropped <= 0 then
    failwith "activity_completion_ack_lost: the proxy discarded no response bytes";
  Fault_proxy.log
    "scenario=activity_completion_ack_lost fault_exercised=true dropped_bytes=%d" dropped;
  Fault_proxy.restore proxy;
  (match timed "activity_completion_ack_lost" 60. (fun () -> Client.wait handle) with
  | Ok (Client.Completed 42) -> ()
  | Ok _ -> failwith "activity_completion_ack_lost: unexpected terminal outcome"
  | Error error -> failwith ("activity_completion_ack_lost: wait failed: " ^ describe error));
  let invocations = Atomic.get activity_invocations in
  Fault_proxy.log "scenario=activity_completion_ack_lost run_id=%s callback_invocations=%d"
    (Client.run_id handle) invocations;
  if invocations <> 1 then
    failwith (Printf.sprintf
      "activity_completion_ack_lost: callback ran %d times in one attempt" invocations)

(** Scenario 9: shutting a client down while its server is unreachable is
    bounded and returns [Ok]; nothing waits for the lost transport. *)
let shutdown_while_unavailable proxy =
  let client =
    get "shutdown fixture client" (Client.create ~target_url:(Fault_proxy.url proxy) ~namespace ())
  in
  Fault_proxy.set_mode proxy Fault_proxy.Refuse;
  let result = timed "shutdown_while_unavailable" 10. (fun () -> Client.shutdown client) in
  Fault_proxy.restore proxy;
  get "client shutdown while unavailable" result

(** Requires the SDK to have closed every proxied socket once its clients
    and worker are shut down: a remaining connection is a leaked handle. *)
let expect_no_connections proxy label =
  await (label ^ " proxied connections to close") 15. (fun () ->
    Fault_proxy.active_connections proxy = 0);
  Fault_proxy.log "proxy=%s leak_check=ok accepted=%d" label
    (Fault_proxy.accepted_connections proxy)

(** Runs every scenario in order against one worker and cleans up even when
    an assertion fails. *)
let check address cli =
  let host, port = upstream address in
  let queue =
    Printf.sprintf "transport-interruption-%d-%d" (Unix.getpid ())
      (Random.State.bits (Random.State.make_self_init ()))
  in
  Fault_proxy.log "fixture=begin task_queue=%s upstream=%s" queue address;
  let client_proxy = Fault_proxy.start ~name:"client" ~upstream_host:host ~upstream_port:port in
  let worker_proxy = Fault_proxy.start ~name:"worker" ~upstream_host:host ~upstream_port:port in
  let direct = get "direct client" (Client.create ~target_url:address ~namespace ()) in
  let worker =
    get "worker create"
      (Worker.create ~target_url:(Fault_proxy.url worker_proxy) ~namespace ~task_queue:queue
         ~workflows:
           [ Worker.workflow counter
               ~signals:[ Signal.Handler.make signal (fun amount -> Result.map ignore (add amount)) ]
               ~queries:[ Query.Handler.make query read ]
               ~updates:[ Update.Handler.make update add ];
             Worker.workflow activity_workflow ]
         ~activities:[ Worker.activity side_effect ] ())
  in
  let worker_stopped = Atomic.make false in
  let running =
    Domain.spawn (fun () ->
      let result = Worker.run worker in
      Atomic.set worker_stopped true;
      result)
  in
  let terminations = ref [] in
  let track handle =
    terminations := (fun () -> ignore (Client.terminate handle)) :: !terminations;
    handle
  in
  let proxied = ref [] in
  let body =
    try
      let client = client_create_refused client_proxy in
      proxied := [ client ];
      let handle = start_ack_lost ~proxy:client_proxy ~client ~direct ~queue ~track in
      let direct_handle =
        get "direct follow"
          (Client.follow direct ~workflow:counter
             { Client.namespace; workflow_id = Client.workflow_id handle;
               run_id = Client.run_id handle })
      in
      signal_ack_lost ~proxy:client_proxy ~direct_handle ~handle;
      update_response_delayed ~proxy:client_proxy ~direct_handle ~handle;
      wait_across_outage ~proxy:client_proxy ~direct_handle ~handle;
      terminate_ack_lost ~proxy:client_proxy ~client ~direct ~queue ~track;
      worker_poll_outage ~proxy:worker_proxy ~direct ~queue ~track ~worker_stopped;
      activity_completion_ack_lost ~proxy:worker_proxy ~direct ~queue ~track ~cli ~address;
      shutdown_while_unavailable client_proxy;
      Ok ()
    with exception_ -> Error exception_
  in
  (* Cleanup runs in a fixed order: release the callback, close executions,
     then the proxied client, then the worker, and only then check sockets. *)
  Atomic.set release_activity true;
  Fault_proxy.restore client_proxy;
  Fault_proxy.restore worker_proxy;
  List.iter (fun terminate -> try terminate () with _ -> ()) !terminations;
  let client_shutdown = List.map Client.shutdown !proxied in
  let worker_shutdown = Worker.shutdown worker in
  let run_result = Domain.join running in
  ignore (Client.shutdown direct);
  (match body with Ok () -> () | Error exception_ -> raise exception_);
  List.iter (get "proxied client shutdown") client_shutdown;
  get "worker shutdown" worker_shutdown;
  get "worker run" run_result;
  expect_no_connections client_proxy "client";
  expect_no_connections worker_proxy "worker";
  Fault_proxy.stop client_proxy;
  Fault_proxy.stop worker_proxy;
  print_endline "transport interruption live regression: ok"

(** The supplied URL must identify a disposable test server. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address; cli ] -> check address cli
  | _ -> failwith "usage: regression check http://temporal:7233 /path/to/temporal"
