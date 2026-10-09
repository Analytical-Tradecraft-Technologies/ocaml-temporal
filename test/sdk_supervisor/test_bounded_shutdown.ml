(** Supervisor-level tests for bounded worker shutdown (#495).

    The production orchestration ([Native_worker_shutdown.run]) releases a
    real [Sdk_supervisor.Make] instance over a fake backend whose operations
    and teardown can block, the way a completion RPC or Core's worker
    deregistration blocks while the Temporal server is unreachable. Each
    case checks that the caller returns within its bound with a detached
    teardown, that the supervisor still releases the graph exactly once on
    its owner Domain afterwards, and that a late operation (an abandoned
    callback's completion) is rejected as [Closed] instead of reaching the
    released graph. *)

module Shutdown = Temporal_runtime.Native_worker_shutdown

(** Fails with [message] unless [condition] holds. *)
let require condition message = if not condition then failwith message

(** Waits for a cross-Domain observation, failing after five seconds. *)
let await label predicate =
  let deadline = Unix.gettimeofday () +. 5. in
  while not (predicate ()) do
    if Unix.gettimeofday () > deadline then failwith ("timed out: " ^ label);
    Thread.delay 0.005
  done

(** Fake backend state. [server_up] stands for the Temporal server's
    reachability: while it is [false], [Complete] and [shutdown] block. *)
type state = {
  server_up : bool Atomic.t;
  entered : bool Atomic.t;  (** Set when a [Complete] begins. *)
  closes : int Atomic.t;
  completions : int Atomic.t;
}

(** A fake backend whose completion and teardown need the server. *)
module Backend = struct
  type config = state
  type nonrec state = state
  type error = string

  type _ operation = Complete : unit operation

  (** The graph is the shared state record; creation cannot fail. *)
  let create state = Ok state

  (** Blocks until the server is reachable, like a completion RPC. *)
  let wait_for_server state =
    while not (Atomic.get state.server_up) do
      Thread.delay 0.005
    done

  (** Records one completion once the server is reachable. *)
  let perform : type value. state -> value operation -> (value, error) result =
   fun state Complete ->
    Atomic.set state.entered true;
    wait_for_server state;
    ignore (Atomic.fetch_and_add state.completions 1);
    Ok ()

  (** Worker deregistration also needs the server. *)
  let shutdown state =
    wait_for_server state;
    ignore (Atomic.fetch_and_add state.closes 1);
    Ok ()
end

(** The production supervisor over the fake backend. *)
module Supervisor = Sdk_supervisor.Make (Backend)

(** Fresh backend state with the server unreachable. *)
let state () =
  {
    server_up = Atomic.make false;
    entered = Atomic.make false;
    closes = Atomic.make 0;
    completions = Atomic.make 0;
  }

(** Operations for a worker with no run loop whose release is the
    supervisor's own terminal shutdown. *)
let operations supervisor =
  {
    Shutdown.try_acquire_lanes = (fun () -> true);
    release_lanes = ignore;
    activity_lane_detached = (fun () -> false);
    activity_callback_running = (fun () -> false);
    workflow_activation_in_flight = (fun () -> false);
    drain_workflow = (fun () -> Shutdown.Drained);
    drain_activity = (fun () -> Shutdown.Drained);
    outstanding_async_leases = (fun () -> 0);
    async_leases_error = string_of_int;
    release =
      (fun () ->
        match Supervisor.shutdown supervisor with
        | Ok () -> Shutdown.Released
        | Error _ -> Shutdown.Release_failed "supervisor shutdown failed");
    exception_error = Printexc.to_string;
  }

(** Runs a bounded shutdown with a zero grace period and [teardown]. *)
let bounded_shutdown ?(teardown = 0.3) supervisor =
  let started = Unix.gettimeofday () in
  let outcome =
    Shutdown.run ~lanes_deadline:started ~teardown_timeout_s:teardown
      (operations supervisor)
  in
  (outcome, Unix.gettimeofday () -. started)

(** Asserts that a shutdown returned a detached report within its bound. *)
let require_detached (outcome, elapsed) =
  (match outcome with
  | Shutdown.Shut_down { teardown = Shutdown.Detached; _ } -> ()
  | _ -> failwith "an unreachable server did not detach the teardown");
  require
    (elapsed < Shutdown.lanes_slack_s +. 0.3 +. 0.5)
    (Printf.sprintf "shutdown took %.3fs" elapsed)

(** The backend's own teardown blocks on the unreachable server. *)
let test_teardown_blocked () =
  let state = state () in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:4 state) in
  require_detached (bounded_shutdown supervisor);
  require (Atomic.get state.closes = 0) "teardown finished while blocked";
  (* A late completion from abandoned code must not reach the graph. *)
  require
    (Supervisor.perform supervisor Backend.Complete = Error Supervisor.Closed)
    "a late operation was admitted after shutdown began";
  Atomic.set state.server_up true;
  await "teardown after the server returned" (fun () ->
      Atomic.get state.closes = 1);
  require (Supervisor.shutdown supervisor = Ok ()) "cached shutdown result";
  require (Atomic.get state.closes = 1) "the graph was released twice"

(** A lane's completion RPC occupies the owner Domain, so the terminal
    request queues behind it; the caller still returns within its bound. *)
let test_operation_blocked () =
  let state = state () in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:4 state) in
  let lane =
    Domain.spawn (fun () -> Supervisor.perform supervisor Backend.Complete)
  in
  await "completion entered" (fun () -> Atomic.get state.entered);
  require_detached (bounded_shutdown supervisor);
  Atomic.set state.server_up true;
  require (Domain.join lane = Ok ()) "the in-flight completion was lost";
  await "teardown after the completion" (fun () -> Atomic.get state.closes = 1);
  require (Atomic.get state.completions = 1) "completion count"

(** With a reachable server the same path completes without detaching. *)
let test_reachable_server () =
  let state = state () in
  Atomic.set state.server_up true;
  let supervisor = Result.get_ok (Supervisor.create ~capacity:4 state) in
  match bounded_shutdown supervisor with
  | Shutdown.Shut_down { teardown = Shutdown.Completed; lanes_stopped = true; _ }, _
    ->
      require (Atomic.get state.closes = 1) "reachable server: close count"
  | _ -> failwith "a reachable server did not shut down cleanly"

let () =
  List.iter
    (fun (name, test) ->
      test ();
      Printf.printf "ok %s\n%!" name)
    [
      ("teardown blocked", test_teardown_blocked);
      ("operation blocked", test_operation_blocked);
      ("reachable server", test_reachable_server);
    ]
