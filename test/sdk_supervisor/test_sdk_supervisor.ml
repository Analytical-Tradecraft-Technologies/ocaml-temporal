(** A manually controlled synchronization gate for lifecycle tests. All fields
    are protected by [mutex], and waits recheck their predicate. *)
type gate = {
  mutex : Mutex.t;
  condition : Condition.t;
  mutable open_ : bool;
}

(** Creates a closed synchronization gate. *)
let create_gate () =
  { mutex = Mutex.create (); condition = Condition.create (); open_ = false }

(** Blocks the current ordinary Domain until [gate] is opened. *)
let await_gate gate =
  Mutex.lock gate.mutex;
  while not gate.open_ do
    Condition.wait gate.condition gate.mutex
  done;
  Mutex.unlock gate.mutex

(** Opens [gate] permanently and wakes every waiter. *)
let open_gate gate =
  Mutex.lock gate.mutex;
  gate.open_ <- true;
  Condition.broadcast gate.condition;
  Mutex.unlock gate.mutex

(** Fails with [label] when two structural values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Waits until a spawned Domain has reached a test milestone. *)
let await_atomic flag =
  while not (Atomic.get flag) do
    Domain.cpu_relax ()
  done

(** Test backend configuration. Atomics make counters safe to inspect while
    producer Domains are still active; [owner_ids] is inspected after
    supervisor shutdown establishes the owner-Domain happens-before edge. *)
type backend_config = {
  creates : int Atomic.t;
  closes : int Atomic.t;
  active : int Atomic.t;
  maximum_active : int Atomic.t;
  owner_ids : int list ref;
  create_error : string option;
  create_exception : string option;
  shutdown_error : string option;
  shutdown_exception : string option;
}

(** Allocates an independently observable backend configuration. *)
let backend_config ?create_error ?create_exception ?shutdown_error
    ?shutdown_exception () =
  {
    creates = Atomic.make 0;
    closes = Atomic.make 0;
    active = Atomic.make 0;
    maximum_active = Atomic.make 0;
    owner_ids = ref [];
    create_error;
    create_exception;
    shutdown_error;
    shutdown_exception;
  }

(** Records the current Domain and updates the maximum number of simultaneous
    backend calls. A correct supervisor keeps that maximum at one. *)
let enter_backend config =
  config.owner_ids := (Domain.self () :> int) :: !(config.owner_ids);
  let active = Atomic.fetch_and_add config.active 1 + 1 in
  let rec raise_maximum () =
    let current = Atomic.get config.maximum_active in
    if active > current
       && not (Atomic.compare_and_set config.maximum_active current active)
    then raise_maximum ()
  in
  raise_maximum ()

(** Leaves one instrumented backend invocation. *)
let leave_backend config = ignore (Atomic.fetch_and_add config.active (-1))

(** A typed operation language used to prove that callers never receive the
    backend state while operation result types remain precise. *)
module Backend = struct
  type config = backend_config
  type state = backend_config
  type error = string

  type _ operation =
    | Owner_id : int operation
    | Echo : 'value -> 'value operation
    | Expected_error : string -> int operation
    | Block : gate * gate -> unit operation
    | Crash : string -> int operation
    | Crash_after : gate * gate * string -> int operation

  (** Creates test state on the supervisor owner Domain. *)
  let create config =
    ignore (Atomic.fetch_and_add config.creates 1);
    config.owner_ids := [ (Domain.self () :> int) ];
    match config.create_exception with
    | Some message -> failwith message
    | None ->
        (match config.create_error with
        | None -> Ok config
        | Some error -> Error error)

  (** Executes one operation while recording serialization and ownership. *)
  let perform : type value. state -> value operation -> (value, error) result =
   fun config operation ->
    let execute : type output. output operation -> (output, error) result =
      function
      | Owner_id -> Ok (Domain.self () :> int)
      | Echo value -> Ok value
      | Expected_error error -> Error error
      | Block (entered, release) ->
          open_gate entered;
          await_gate release;
          Ok ()
      | Crash message -> failwith message
      | Crash_after (entered, release, message) ->
          open_gate entered;
          await_gate release;
          failwith message
    in
    enter_backend config;
    Fun.protect
      ~finally:(fun () -> leave_backend config)
      (fun () -> execute operation)

  (** Records deterministic release of the complete test resource graph. *)
  let shutdown config =
    config.owner_ids := (Domain.self () :> int) :: !(config.owner_ids);
    ignore (Atomic.fetch_and_add config.closes 1);
    match config.shutdown_exception with
    | Some message -> failwith message
    | None ->
        (match config.shutdown_error with
        | None -> Ok ()
        | Some error -> Error error)
end

module Supervisor = Sdk_supervisor.Make (Backend)

(** Closes a supervisor and requires an orderly result. *)
let expect_clean_shutdown supervisor =
  expect "first shutdown" (Ok ()) (Supervisor.shutdown supervisor);
  expect "idempotent shutdown" (Ok ()) (Supervisor.shutdown supervisor)

(** Proves creation, typed use, and cleanup all run on one owner Domain rather
    than any producer Domain. *)
let test_owner_domain_and_typed_operations () =
  let config = backend_config () in
  let caller_id = (Domain.self () :> int) in
  let supervisor =
    match Supervisor.create ~capacity:8 config with
    | Ok supervisor -> supervisor
    | Error _ -> failwith "supervisor creation failed"
  in
  let owner_id =
    match Supervisor.perform supervisor Owner_id with
    | Ok owner_id -> owner_id
    | Error _ -> failwith "owner identity operation failed"
  in
  if owner_id = caller_id then failwith "backend ran on the producer Domain";
  expect "integer typed operation" (Ok 42)
    (Supervisor.perform supervisor (Echo 42));
  expect "string typed operation" (Ok "typed")
    (Supervisor.perform supervisor (Echo "typed"));
  expect_clean_shutdown supervisor;
  expect "one create" 1 (Atomic.get config.creates);
  expect "one close" 1 (Atomic.get config.closes);
  if not (List.for_all (( = ) owner_id) !(config.owner_ids)) then
    failwith "backend lifecycle escaped its owner Domain"

(** Verifies that many producer Domains cannot overlap backend operations and
    every typed request completes exactly once. *)
let test_concurrent_producers_are_serialized () =
  let config = backend_config () in
  let supervisor =
    Result.get_ok (Supervisor.create ~capacity:5 config)
  in
  let count = 12 in
  let producers =
    Array.init count (fun value ->
        Domain.spawn (fun () ->
            Supervisor.perform supervisor (Echo value)))
  in
  let actual = Array.map Domain.join producers |> Array.to_list in
  let expected = List.init count (fun value -> Ok value) in
  expect "concurrent results" expected actual;
  expect "serialized maximum" 1 (Atomic.get config.maximum_active);
  expect_clean_shutdown supervisor

(** Expected backend errors remain typed and do not poison the owner. *)
let test_expected_error_keeps_supervisor_usable () =
  let config = backend_config () in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:2 config) in
  expect "expected operation error" (Error (Supervisor.Backend "expected"))
    (Supervisor.perform supervisor (Expected_error "expected"));
  expect "operation after expected error" (Ok 7)
    (Supervisor.perform supervisor (Echo 7));
  expect_clean_shutdown supervisor

(** A creation error is returned without publishing a supervisor or trying to
    close state which the backend never successfully created. *)
let test_creation_error () =
  let config = backend_config ~create_error:"cannot create" () in
  expect "creation error" (Error (Supervisor.Backend "cannot create"))
    (Supervisor.create ~capacity:2 config);
  expect "failed create count" 1 (Atomic.get config.creates);
  expect "failed create close count" 0 (Atomic.get config.closes)

(** An unexpected operation exception is propagated as a supervisor failure,
    closes the native graph exactly once, and rejects every later operation
    with the same terminal exception. *)
let test_unexpected_operation_failure_cleans_up () =
  let config = backend_config () in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:3 config) in
  let expect_crash label = function
    | Error (Supervisor.Supervisor_failed (Failure message))
      when String.equal message "boom" ->
        ()
    | _ -> failwith (label ^ " did not preserve the owner failure")
  in
  expect_crash "active crash" (Supervisor.perform supervisor (Crash "boom"));
  expect_crash "later call" (Supervisor.perform supervisor (Echo 1));
  expect_crash "shutdown after crash" (Supervisor.shutdown supervisor);
  expect_crash "repeated shutdown after crash" (Supervisor.shutdown supervisor);
  expect "exception cleanup count" 1 (Atomic.get config.closes)

(** Callers which begin contending while a defective operation is active all
    receive its contained terminal exception and none remains waiting. Mailbox
    admission order itself is proven by the dedicated mailbox tests. *)
let test_unexpected_failure_releases_contending_callers () =
  let config = backend_config () in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:8 config) in
  let entered = create_gate () in
  let release = create_gate () in
  let crashing =
    Domain.spawn (fun () ->
        Supervisor.perform supervisor
          (Crash_after (entered, release, "queued boom")))
  in
  await_gate entered;
  let waiter_started = Array.init 4 (fun _ -> Atomic.make false) in
  let waiters =
    Array.mapi
      (fun value started ->
        Domain.spawn (fun () ->
            Atomic.set started true;
            Supervisor.perform supervisor (Echo value)))
      waiter_started
  in
  Array.iter await_atomic waiter_started;
  for _ = 1 to 100_000 do
    Domain.cpu_relax ()
  done;
  open_gate release;
  let expect_contending_failure label = function
    | Error (Supervisor.Supervisor_failed (Failure message))
      when String.equal message "queued boom" ->
        ()
    | _ -> failwith (label ^ " did not receive the terminal owner failure")
  in
  expect_contending_failure "active failure" (Domain.join crashing);
  Array.iter
    (fun waiter ->
      expect_contending_failure "contending failure" (Domain.join waiter))
    waiters;
  expect_contending_failure "failed join result"
    (Supervisor.shutdown supervisor);
  expect "contending failure cleanup count" 1 (Atomic.get config.closes)

(** Shutdown waits behind admitted work, runs once on the owner, and rejects
    use after its result has been observed. *)
let test_shutdown_waits_and_is_idempotent () =
  let config = backend_config () in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:2 config) in
  let entered = create_gate () in
  let release = create_gate () in
  let operation =
    Domain.spawn (fun () -> Supervisor.perform supervisor (Block (entered, release)))
  in
  await_gate entered;
  let shutdown_started = Atomic.make false in
  let shutdown =
    Domain.spawn (fun () ->
        Atomic.set shutdown_started true;
        Supervisor.shutdown supervisor)
  in
  await_atomic shutdown_started;
  if Atomic.get config.closes <> 0 then
    failwith "shutdown overtook an admitted operation";
  open_gate release;
  expect "blocked operation" (Ok ()) (Domain.join operation);
  expect "concurrent shutdown" (Ok ()) (Domain.join shutdown);
  expect "cached shutdown" (Ok ()) (Supervisor.shutdown supervisor);
  expect "single shutdown call" 1 (Atomic.get config.closes);
  expect "use after shutdown" (Error Supervisor.Closed)
    (Supervisor.perform supervisor (Echo 1))

(** [initiate_shutdown] must return while a concurrent [shutdown] is still
    waiting on earlier admitted work. Holding the shutdown mutex across
    await/join would stall initiation for the whole graceful period. *)
let test_initiate_shutdown_does_not_wait_for_teardown () =
  let config = backend_config () in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:2 config) in
  let entered = create_gate () in
  let release = create_gate () in
  let active =
    Domain.spawn (fun () -> Supervisor.perform supervisor (Block (entered, release)))
  in
  await_gate entered;
  let shutdown_started = Atomic.make false in
  let shutdown =
    Domain.spawn (fun () ->
        Atomic.set shutdown_started true;
        Supervisor.shutdown supervisor)
  in
  await_atomic shutdown_started;
  let started = Unix.gettimeofday () in
  Supervisor.initiate_shutdown supervisor;
  let elapsed = Unix.gettimeofday () -. started in
  if elapsed > 0.5 then
    failwith
      (Printf.sprintf
         "initiate_shutdown blocked for %.3fs behind concurrent teardown"
         elapsed);
  open_gate release;
  expect "active work under concurrent initiate" (Ok ()) (Domain.join active);
  expect "concurrent teardown" (Ok ()) (Domain.join shutdown);
  expect "initiate while teardown single close" 1 (Atomic.get config.closes)

(** Initiating shutdown closes SDK operation admission synchronously even
    while earlier backend work is still blocked. Concurrent public shutdown
    callers then await the one cached terminal request and share its result. *)
let test_shutdown_initiation_closes_admission_and_shares_result () =
  let config = backend_config ~shutdown_error:"shared close failure" () in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:1 config) in
  let entered = create_gate () in
  let release = create_gate () in
  let active =
    Domain.spawn (fun () -> Supervisor.perform supervisor (Block (entered, release)))
  in
  await_gate entered;
  Supervisor.initiate_shutdown supervisor;
  expect "operation after shutdown initiation" (Error Supervisor.Closed)
    (Supervisor.perform supervisor (Echo 3));
  let late_operations =
    Array.init 16 (fun value ->
        Domain.spawn (fun () -> Supervisor.perform supervisor (Echo value)))
  in
  Array.iter
    (fun operation ->
      expect "concurrent operation after shutdown initiation"
        (Error Supervisor.Closed)
        (Domain.join operation))
    late_operations;
  expect "backend remains blocked after shutdown initiation" 0
    (Atomic.get config.closes);
  let callers =
    Array.init 16 (fun _ ->
        Domain.spawn (fun () -> Supervisor.shutdown supervisor))
  in
  open_gate release;
  expect "active work before initiated shutdown" (Ok ()) (Domain.join active);
  let expected = Error (Supervisor.Backend "shared close failure") in
  Array.iter
    (fun caller -> expect "initiated concurrent shutdown" expected (Domain.join caller))
    callers;
  expect "cached initiated shutdown" expected (Supervisor.shutdown supervisor);
  expect "one initiated backend shutdown" 1 (Atomic.get config.closes);
  expect "serialized initiated backend calls" 1
    (Atomic.get config.maximum_active)

(** A backend shutdown error is cached, while release is still attempted only
    once and later operations remain closed. *)
let test_shutdown_error_is_cached () =
  let config = backend_config ~shutdown_error:"close failed" () in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:2 config) in
  let expected = Error (Supervisor.Backend "close failed") in
  expect "shutdown error" expected (Supervisor.shutdown supervisor);
  expect "cached shutdown error" expected (Supervisor.shutdown supervisor);
  expect "failed shutdown call count" 1 (Atomic.get config.closes);
  expect "closed after shutdown error" (Error Supervisor.Closed)
    (Supervisor.perform supervisor (Echo 1))

(** Abandoning an instance schedules cleanup on a separate system thread so a
    forgotten explicit shutdown cannot strand its owner Domain or graph. *)
let test_abandoned_supervisor_is_cleaned_up () =
  let config = backend_config () in
  let weak = Weak.create 1 in
  let create_and_drop () =
    let supervisor = Result.get_ok (Supervisor.create ~capacity:2 config) in
    Weak.set weak 0 (Some supervisor)
  in
  create_and_drop ();
  let rec collect attempts =
    if Atomic.get config.closes = 1 then ()
    else if attempts = 0 then
      failwith "abandoned supervisor did not release its backend graph"
    else (
      Gc.full_major ();
      (* A yield need not let a newly created system thread run on Windows.
         This short blocking delay gives both the finalizer and its cleanup
         thread a scheduler opportunity while retaining a bounded failure. *)
      Thread.delay 0.001;
      collect (attempts - 1))
  in
  collect 5_000;
  expect "abandoned cleanup count" 1 (Atomic.get config.closes)

(** An owner that wraps a supervisor, like a native worker: its abandoned
    cleanup must drain its own state before the supervisor closes the graph. *)
type owner = { supervisor : Supervisor.t; drained : bool Atomic.t }

(** After [delegate_abandoned_cleanup], the owner's finalizer is the only
    cleanup path even though owner and supervisor become unreachable in the
    same collection, so [Gc.finalise] runs both finalizers (#1044). The owner's
    cleanup thread drains slowly; the backend must not be closed before the
    drain has finished, and it must still be closed exactly once. *)
let test_delegated_abandoned_cleanup_waits_for_owner () =
  let config = backend_config () in
  let create_and_drop () =
    let supervisor = Result.get_ok (Supervisor.create ~capacity:2 config) in
    Supervisor.delegate_abandoned_cleanup supervisor;
    let owner = { supervisor; drained = Atomic.make false } in
    Gc.finalise
      (fun owner ->
        ignore
          (Thread.create
             (fun owner ->
               Thread.delay 0.05;
               Atomic.set owner.drained true;
               ignore (Supervisor.shutdown owner.supervisor))
             owner))
      owner;
    (* Only the flag escapes, so owner and supervisor become unreachable
       together. *)
    owner.drained
  in
  let drained = create_and_drop () in
  let rec collect attempts =
    let closes = Atomic.get config.closes in
    if closes > 0 && not (Atomic.get drained) then
      failwith "supervisor finalizer closed the graph before the owner drained"
    else if closes = 1 then ()
    else if attempts = 0 then
      failwith "abandoned owner did not release its backend graph"
    else (
      Gc.full_major ();
      Thread.delay 0.001;
      collect (attempts - 1))
  in
  collect 5_000;
  (* Let any stray finalizer thread run before the exact-count check. *)
  Thread.delay 0.1;
  expect "delegated cleanup count" 1 (Atomic.get config.closes)

(** Concurrent shutdown callers all receive one cached expected backend error
    while the backend release operation executes exactly once. *)
let test_concurrent_shutdown_callers_share_result () =
  let config = backend_config ~shutdown_error:"shared close failure" () in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:8 config) in
  let callers =
    Array.init 16 (fun _ ->
        Domain.spawn (fun () -> Supervisor.shutdown supervisor))
  in
  let expected = Error (Supervisor.Backend "shared close failure") in
  Array.iter
    (fun caller -> expect "concurrent shutdown result" expected (Domain.join caller))
    callers;
  expect "concurrent shutdown backend count" 1 (Atomic.get config.closes)

(** A backend exception during creation is contained and returned to the
    producer instead of escaping or stranding the temporary owner Domain. *)
let test_creation_exception_is_contained () =
  let config = backend_config ~create_exception:"create exploded" () in
  let creator =
    Domain.spawn (fun () -> Supervisor.create ~capacity:2 config)
  in
  (match Domain.join creator with
  | Error (Supervisor.Supervisor_failed (Failure message))
    when String.equal message "create exploded" ->
      ()
  | _ -> failwith "creation exception was not contained");
  expect "exceptional create count" 1 (Atomic.get config.creates);
  expect "exceptional create close count" 0 (Atomic.get config.closes)

(** An unexpected shutdown exception is contained once and the exact terminal
    failure is shared by concurrent and later shutdown callers. *)
let test_shutdown_exception_is_contained_and_cached () =
  let config = backend_config ~shutdown_exception:"shutdown exploded" () in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:8 config) in
  let callers =
    Array.init 16 (fun _ ->
        Domain.spawn (fun () -> Supervisor.shutdown supervisor))
  in
  let expect_failure = function
    | Error (Supervisor.Supervisor_failed (Failure message))
      when String.equal message "shutdown exploded" ->
        ()
    | _ -> failwith "shutdown exception was not preserved"
  in
  Array.iter (fun caller -> expect_failure (Domain.join caller)) callers;
  expect_failure (Supervisor.shutdown supervisor);
  expect "exceptional shutdown backend count" 1 (Atomic.get config.closes)

(** #1042: a companion thread started with [spawn_owner_thread] runs on the
    owner Domain, not on the caller's, can use the instance through its
    ordinary entry points, and is waited for by [shutdown]'s owner join.
    After shutdown the request is refused as [Closed]. *)
let test_owner_thread_runs_on_owner_domain () =
  let config = backend_config () in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:2 config) in
  let owner_id =
    Result.get_ok (Supervisor.perform supervisor Owner_id)
  in
  let thread_domain = Atomic.make (-1) in
  let echoed = Atomic.make None in
  let release = Atomic.make false in
  let exited = Atomic.make false in
  let body () =
    Atomic.set thread_domain (Domain.self () :> int);
    Atomic.set echoed (Some (Supervisor.perform supervisor (Echo 7)));
    while not (Atomic.get release) do
      Thread.delay 0.005
    done;
    Atomic.set exited true
  in
  (match Supervisor.spawn_owner_thread supervisor body with
  | Ok (Ok ()) -> ()
  | Ok (Error _) -> failwith "owner thread could not be created"
  | Error _ -> failwith "owner refused the thread request");
  (* At most about five seconds before a lost thread fails the test. *)
  let rec await_echo remaining =
    if Atomic.get echoed = None then
      if remaining = 0 then failwith "owner thread did not run"
      else begin
        Thread.delay 0.001;
        await_echo (remaining - 1)
      end
  in
  await_echo 5_000;
  if Atomic.get thread_domain <> owner_id then
    failwith "companion thread did not run on the owner Domain";
  if Atomic.get thread_domain = (Domain.self () :> int) then
    failwith "companion thread ran on the caller's Domain";
  expect "companion thread operation" (Some (Ok 7)) (Atomic.get echoed);
  let shutdown = Domain.spawn (fun () -> Supervisor.shutdown supervisor) in
  (* The owner join waits for the companion thread, so release it only
     after shutdown started and check that shutdown returned after it. *)
  Thread.delay 0.05;
  Atomic.set release true;
  expect "shutdown with companion thread" (Ok ()) (Domain.join shutdown);
  if not (Atomic.get exited) then
    failwith "shutdown returned before the owner thread exited";
  match Supervisor.spawn_owner_thread supervisor ignore with
  | Error Supervisor.Closed -> ()
  | _ -> failwith "a closed supervisor accepted a thread request"

let () =
  test_owner_domain_and_typed_operations ();
  test_concurrent_producers_are_serialized ();
  test_expected_error_keeps_supervisor_usable ();
  test_creation_error ();
  test_unexpected_operation_failure_cleans_up ();
  test_unexpected_failure_releases_contending_callers ();
  test_shutdown_waits_and_is_idempotent ();
  test_initiate_shutdown_does_not_wait_for_teardown ();
  test_shutdown_initiation_closes_admission_and_shares_result ();
  test_shutdown_error_is_cached ();
  test_abandoned_supervisor_is_cleaned_up ();
  test_delegated_abandoned_cleanup_waits_for_owner ();
  test_concurrent_shutdown_callers_share_result ();
  test_creation_exception_is_contained ();
  test_shutdown_exception_is_contained_and_cached ();
  test_owner_thread_runs_on_owner_domain ()
