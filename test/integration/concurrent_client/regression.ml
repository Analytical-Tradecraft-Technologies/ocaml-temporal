(** Live regression for concurrent calls on one client (#807).

    One [Client.t] used to serve every call on its supervisor Domain one at a
    time, and that Domain waited for the network inside each call: every
    pending [Client.wait] held it for 100 ms per turn, and a query on a
    workflow with no live worker held it until the query's deadline. With
    enough waiters, or one stuck query, every signal and start on the same
    client queued for seconds.

    Against a disposable Temporal server this check keeps 24 waits pending on
    open workflows from other threads and a query stuck on a workflow whose
    worker has been killed, and meanwhile requires every signal and start on
    the same client to finish within a latency bound. It then shows that
    terminating the workflows releases the waits promptly, that the stuck
    query ends at its own deadline, that shutting the client down with a
    wait in flight ends that wait with the closed-client error, and that a
    configured [max_pending_waits] (#796) is enforced as a capacity error,
    and that a bounded [Client.wait ?timeout] (#1136) expires with a
    retryable deadline error and releases its slot. The parent
    owns and reaps its worker, uses a unique task queue, and terminates every
    execution it starts. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the regression with a labelled message. *)
let fail label = failwith ("concurrent client: " ^ label)

(** Waits until terminated; the signal handler accepts and ignores input. *)
let blocked =
  Workflow.define ~name:"concurrent-client-blocked" ~input:Codec.string
    ~output:Codec.string (fun _ ->
      Result.map (fun () -> "unreachable") (Condition.wait_until (fun () -> false)))

(** The signal sent while the long calls are pending. *)
let poke = Signal.define ~name:"poke" ~input:Codec.string

(** A query that nobody answers once the workflow's worker is killed. *)
let state = Query.define ~name:"state" ~output:Codec.string

(** Open workflows that the pending waits observe. *)
let blocked_runs = 8

(** Concurrent waits per open workflow. *)
let waits_per_run = 3

(** Signals and starts measured while the long calls are pending. *)
let measured_calls = 10

(** Upper bound in seconds on one signal or start while the long calls are
    pending. Typical latency is tens of milliseconds; before #807 the same
    calls queued behind 24 waits of 100 ms each per turn (about 2.4 s) and
    behind the stuck query (up to its 8 s deadline). *)
let latency_bound = 1.5

(** Runs the fixture definitions on the test's unique queue. *)
let worker address queue =
  let worker =
    get
      (Worker.create ~target_url:address ~namespace:"default" ~task_queue:queue
         ~activities:[]
         ~workflows:
           [
             Worker.workflow blocked
               ~signals:[ Signal.Handler.make poke (fun _ -> Ok ()) ]
               ~queries:[ Query.Handler.make state (fun () -> Ok "blocked") ];
           ]
         ())
  in
  get (Worker.run worker)

(** Elapsed wall-clock seconds of [operation] and its result. *)
let timed operation =
  let started = Unix.gettimeofday () in
  let result = operation () in
  (Unix.gettimeofday () -. started, result)

(** One background call: its thread and the cell its result lands in. *)
type 'value background = { thread : Thread.t; result : 'value option ref; lock : Mutex.t }

(** Runs [operation] on a new system thread of this Domain. A client call
    blocks only the calling thread, with the OCaml runtime lock released. *)
let background operation =
  let result = ref None and lock = Mutex.create () in
  let thread =
    Thread.create
      (fun () ->
        let value = operation () in
        Mutex.lock lock;
        result := Some value;
        Mutex.unlock lock)
      ()
  in
  { thread; result; lock }

(** Reports whether a background call has finished. *)
let finished call =
  Mutex.lock call.lock;
  let finished = Option.is_some !(call.result) in
  Mutex.unlock call.lock;
  finished

(** Joins a background call and returns its result. *)
let join call =
  Thread.join call.thread;
  Option.get !(call.result)

(** Waits up to [seconds] for every call in [calls] to finish. *)
let await_all ~seconds label calls =
  let deadline = Unix.gettimeofday () +. seconds in
  while
    (not (List.for_all finished calls)) && Unix.gettimeofday () < deadline
  do
    Unix.sleepf 0.05
  done;
  if not (List.for_all finished calls) then
    fail (Printf.sprintf "%s did not finish within %.0f s" label seconds)

(** Starts one blocked workflow on [queue] and records it for cleanup. *)
let start_blocked client cleanups queue suffix =
  let handle =
    get
      (Client.start client ~workflow:blocked ~task_queue:queue
         ~id:(queue ^ "-" ^ suffix) ~input:"" ())
  in
  cleanups := (fun () -> ignore (Client.terminate handle)) :: !cleanups;
  handle

(** Spawns this executable as a worker process for [queue]. *)
let spawn_worker address queue =
  Unix.create_process Sys.executable_name
    [| Sys.executable_name; "worker"; address; queue |]
    Unix.stdin Unix.stdout Unix.stderr

(** Starts a blocked workflow on its own queue, proves its worker answers a
    query (so the workflow has run its first task), then kills that worker.
    Later queries reach no worker and stay pending until their deadline. *)
let orphaned_workflow client cleanups address queue =
  let orphan_queue = queue ^ "-orphan" in
  let pid = spawn_worker address orphan_queue in
  let killed = ref false in
  let kill () =
    if not !killed then (
      killed := true;
      (try Unix.kill pid Sys.sigkill with Unix.Unix_error _ -> ());
      ignore (Unix.waitpid [] pid))
  in
  Fun.protect ~finally:kill (fun () ->
      let orphan = start_blocked client cleanups orphan_queue "orphan" in
      let deadline = Unix.gettimeofday () +. 60.0 in
      let rec answered () =
        match Client.query ~rpc_timeout:(Duration.of_ms 5_000L) orphan ~query:state with
        | Ok "blocked" -> ()
        | Ok _ -> fail "orphan query returned an unexpected state"
        | Error _ when Unix.gettimeofday () < deadline ->
            Unix.sleepf 0.2;
            answered ()
        | Error error -> fail ("orphan worker never answered: " ^ Error.message error)
      in
      answered ();
      kill ();
      orphan)

(** The main check: pending waits and a stuck query do not delay signals and
    starts on the same client, and each long call still ends correctly. *)
let check_concurrency client cleanups address queue =
  let runs =
    List.init blocked_runs (fun index ->
        start_blocked client cleanups queue (Printf.sprintf "blocked-%d" index))
  in
  (* Its worker is gone, so this query stays pending until its own 8 s
     deadline. *)
  let orphan = orphaned_workflow client cleanups address queue in
  let waits =
    List.concat_map
      (fun handle -> List.init waits_per_run (fun _ -> background (fun () -> Client.wait handle)))
      runs
  in
  let query =
    background (fun () ->
        timed (fun () ->
            Client.query ~rpc_timeout:(Duration.of_ms 8_000L) orphan ~query:state))
  in
  (* Let every long call reach Temporal before measuring. *)
  Unix.sleepf 1.0;
  if List.exists finished waits || finished query then
    fail "a long call finished before its workflow closed";
  let latencies =
    List.init measured_calls (fun index ->
        let target = List.nth runs (index mod blocked_runs) in
        let signal_latency, signalled =
          timed (fun () -> Client.signal target ~signal:poke ~input:"poke")
        in
        (match signalled with
        | Ok () -> ()
        | Error error -> fail ("signal failed: " ^ Error.message error));
        let start_latency, _ =
          timed (fun () ->
              start_blocked client cleanups queue (Printf.sprintf "measured-%d" index))
        in
        max signal_latency start_latency)
  in
  let worst = List.fold_left max 0.0 latencies in
  Printf.printf "worst signal/start latency with %d pending waits and a stuck query: %.3f s\n%!"
    (List.length waits) worst;
  if worst > latency_bound then
    fail
      (Printf.sprintf "a signal or start took %.3f s (bound %.1f s) behind pending long calls"
         worst latency_bound);
  if List.exists finished waits then fail "a wait finished before its workflow closed";
  if finished query then fail "the stuck query ended before the measurements did";
  (* Terminating the workflows releases every wait promptly. *)
  List.iter (fun handle -> get (Client.terminate handle)) runs;
  await_all ~seconds:20.0 "waits after termination" waits;
  List.iter
    (fun call ->
      match join call with
      | Ok (Client.Terminated _) -> ()
      | Ok _ -> fail "a wait did not report Terminated"
      | Error error -> fail ("a wait failed: " ^ Error.message error))
    waits;
  (* The stuck query ends by itself (normally at its 8 s deadline),
     unaffected by the calls made meanwhile. It was still pending when the
     measurements finished, which is what the latency bound relies on. *)
  await_all ~seconds:30.0 "stuck query" [ query ];
  match join query with
  | elapsed, Error _ ->
      Printf.printf "stuck query ended after %.1f s\n%!" elapsed
  | _, Ok _ -> fail "a query against a workflow without a worker was answered"

(** Shutting a client down while a wait is in flight ends that wait with the
    closed-client error at once instead of leaving it blocked. *)
let check_shutdown_in_flight main cleanups address queue =
  let started = start_blocked main cleanups queue "shutdown" in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  let handle =
    get
      (Client.get_handle client ~workflow:blocked ?run_id:(Client.run_id started)
         ~id:(Client.workflow_id started) ())
  in
  let waiting = background (fun () -> Client.wait handle) in
  Unix.sleepf 1.0;
  if finished waiting then fail "the wait finished before shutdown";
  let elapsed, shutdown = timed (fun () -> Client.shutdown client) in
  (match shutdown with
  | Ok () -> ()
  | Error error -> fail ("shutdown failed: " ^ Error.message error));
  await_all ~seconds:10.0 "wait after shutdown" [ waiting ];
  if elapsed > 10.0 then fail "shutdown waited for the pending wait";
  match join waiting with
  | Error error when Error.message error = "client is shut down" -> ()
  | Error error -> fail ("in-flight wait ended with " ^ Error.message error)
  | Ok _ -> fail "an in-flight wait completed after shutdown"

(** A client created with [~max_pending_waits:2] (#796) refuses a wait on a
    third distinct run at once with the retryable capacity error, while its
    two admitted waits stay pending; shutting it down ends them. *)
let check_configured_wait_limit main cleanups address queue =
  let started =
    List.init 3 (fun index ->
        start_blocked main cleanups queue (Printf.sprintf "limited-%d" index))
  in
  let client =
    get (Client.create ~max_pending_waits:2 ~target_url:address ~namespace:"default" ())
  in
  Fun.protect
    ~finally:(fun () -> ignore (Client.shutdown client))
    (fun () ->
      let handles =
        List.map
          (fun handle ->
            get
              (Client.get_handle client ~workflow:blocked ?run_id:(Client.run_id handle)
                 ~id:(Client.workflow_id handle) ()))
          started
      in
      let admitted =
        List.map (fun handle -> background (fun () -> Client.wait handle))
          [ List.nth handles 0; List.nth handles 1 ]
      in
      Unix.sleepf 1.0;
      if List.exists finished admitted then fail "an admitted wait finished early";
      (match timed (fun () -> Client.wait (List.nth handles 2)) with
      | elapsed, Error error when Client.is_at_capacity error ->
          if elapsed > latency_bound then
            fail (Printf.sprintf "the capacity refusal took %.3f s" elapsed)
      | _, Error error -> fail ("third wait failed with " ^ Error.message error)
      | _, Ok _ -> fail "a wait beyond max_pending_waits was admitted");
      get (Client.shutdown client);
      await_all ~seconds:10.0 "limited waits after shutdown" admitted;
      List.iter
        (fun call ->
          match join call with
          | Error _ -> ()
          | Ok _ -> fail "a limited wait completed after shutdown")
        admitted)

(** [Client.wait ?timeout] on a live server (#1136), on a client admitting
    only one pending wait:
    - a bounded wait on an open run gives up after its bound with the
      retryable [Deadline_exceeded] RPC error, and the workflow is untouched;
    - the expired wait released its slot, so a wait on a second run is then
      admitted (not refused as at capacity) and stays pending;
    - once the first run is terminated, a later bounded wait on the same
      handle observes its terminal result;
    - shutting the client down during a bounded wait ends it promptly with
      the closed-client error rather than at its bound. *)
let check_bounded_wait main cleanups address queue =
  let first = start_blocked main cleanups queue "bounded-first" in
  let second = start_blocked main cleanups queue "bounded-second" in
  let client =
    get (Client.create ~max_pending_waits:1 ~target_url:address ~namespace:"default" ())
  in
  let shut_down = ref false in
  Fun.protect
    ~finally:(fun () -> if not !shut_down then ignore (Client.shutdown client))
    (fun () ->
      let rebind handle =
        get
          (Client.get_handle client ~workflow:blocked ?run_id:(Client.run_id handle)
             ~id:(Client.workflow_id handle) ())
      in
      let first_handle = rebind first and second_handle = rebind second in
      (match timed (fun () -> Client.wait ~timeout:(Duration.of_ms 1_000L) first_handle) with
      | elapsed, Error error when Client.rpc_status error = Some `Deadline_exceeded ->
          if (Error.view error).non_retryable then fail "the wait timeout was non-retryable";
          if elapsed < 0.9 || elapsed > 5.0 then
            fail (Printf.sprintf "the 1 s wait bound took %.3f s" elapsed)
      | _, Error error -> fail ("the bounded wait failed with " ^ Error.message error)
      | _, Ok _ -> fail "a bounded wait on an open run completed");
      let admitted = background (fun () -> Client.wait second_handle) in
      Unix.sleepf 1.0;
      if finished admitted then (
        match join admitted with
        | Error error when Client.is_at_capacity error ->
            fail "the expired wait did not release its slot"
        | _ -> fail "the wait on the second run finished early");
      get (Client.terminate second_handle);
      await_all ~seconds:10.0 "admitted wait after termination" [ admitted ];
      (match join admitted with
      | Ok (Client.Terminated _) -> ()
      | Ok _ -> fail "the admitted wait did not report termination"
      | Error error -> fail ("the admitted wait failed with " ^ Error.message error));
      get (Client.terminate first_handle);
      (match Client.wait ~timeout:(Duration.of_seconds 10L) first_handle with
      | Ok (Client.Terminated _) -> ()
      | Ok _ -> fail "a later wait on the timed-out handle did not see termination"
      | Error error -> fail ("a later bounded wait failed with " ^ Error.message error));
      let third = start_blocked main cleanups queue "bounded-shutdown" in
      let third_handle = rebind third in
      let waiting =
        background (fun () -> Client.wait ~timeout:(Duration.of_seconds 30L) third_handle)
      in
      Unix.sleepf 1.0;
      if finished waiting then fail "the bounded wait finished before shutdown";
      let elapsed, shutdown = timed (fun () -> Client.shutdown client) in
      shut_down := true;
      (match shutdown with
      | Ok () -> ()
      | Error error -> fail ("shutdown failed: " ^ Error.message error));
      await_all ~seconds:10.0 "bounded wait after shutdown" [ waiting ];
      if elapsed > 10.0 then fail "shutdown waited for the bounded wait";
      match join waiting with
      | Error error when Error.message error = "client is shut down" -> ()
      | Error error -> fail ("the bounded wait ended with " ^ Error.message error)
      | Ok _ -> fail "a bounded wait completed after shutdown")

(** Starts the worker process, runs every check, and terminates every
    fixture execution and the worker even when a check fails. *)
let check address =
  let queue = "concurrent-client-" ^ Temporal_base.Client_request_id.create () in
  let pid = spawn_worker address queue in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  let cleanups = ref [] in
  Fun.protect
    ~finally:(fun () ->
      List.iter (fun cleanup -> cleanup ()) !cleanups;
      ignore (Client.shutdown client);
      Unix.kill pid Sys.sigterm;
      ignore (Unix.waitpid [] pid))
    (fun () ->
      check_concurrency client cleanups address queue;
      check_shutdown_in_flight client cleanups address queue;
      check_configured_wait_limit client cleanups address queue;
      check_bounded_wait client cleanups address queue;
      print_endline "concurrent client live regression: ok")

(** The supplied URL must identify a disposable test namespace/server. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address ] -> check address
  | [ _; "worker"; address; queue ] -> worker address queue
  | _ -> failwith "usage: regression check http://localhost:7233"
