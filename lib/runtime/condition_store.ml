(** Implements deterministic, workflow-local condition waits.

    Temporal conditions are deliberately a language-runtime feature rather
    than a server operation.  A false predicate retains one scheduler-owned
    future and one predicate callback; the activation loop calls [notify] after
    it has run the current batch of workflow jobs.  This keeps the operation
    replay-safe and avoids inventing a timer, command, history event, thread,
    or native lock merely to wait for an OCaml value to change. *)

(** The callback used to decide whether one waiter can be released. *)
type predicate = unit -> (bool, Temporal_base.Error.t) result

(** One registered predicate and its one-shot scheduler signal.  [active] is
    checked by every transition so a stale callback cannot resolve a future
    after shutdown or after an earlier notification already settled it.
    [registration] is the waiter's node in the store's registry; it is set
    immediately after the record is linked and is used for O(1) removal. *)
type waiter = {
  predicate : predicate;
  resolve : (unit, Temporal_base.Error.t) Future_store.resolver;
  mutable active : bool;
  mutable registration : waiter Temporal_base.Ordered_registry.handle option;
}

(** All condition waiters for one workflow execution, in registration order.
    The registry makes registration and the removal of a released waiter
    constant-time, so releasing [n] waiters in one activation is O(n) list
    work rather than O(n{^ 2}); notification order is the explicit
    registration order, never a hash or address order. *)
type t = {
  scheduler : Scheduler.t;
  owner_id : int;
  waiters : waiter Temporal_base.Ordered_registry.t;
  mutable closed : bool;
}

(** Builds a new store whose waiters are resumed by [scheduler]. *)
let create scheduler =
  {
    scheduler;
    owner_id = Scheduler.id scheduler;
    waiters = Temporal_base.Ordered_registry.create ();
    closed = false;
  }

(** Builds a stable defect for calls that cannot safely park on this store. *)
let ownership_error () =
  Temporal_base.Error.defect
    ~message:"Temporal condition wait used outside its workflow scheduler"

(** Builds the lifecycle defect used after an execution has been torn down. *)
let closed_error () =
  Temporal_base.Error.defect
    ~message:"Temporal condition wait used after workflow execution ended"

(** Checks both scheduler ownership and liveness before mutable store state is
    read.  The thread-local owner check prevents a different workflow from
    awaiting a condition merely because its scheduler is also running. *)
let owns_scheduler store =
  Future_store.current_owner_matches store.owner_id
  && Scheduler.is_running store.scheduler
  && Scheduler.is_active store.scheduler

(** Turns a predicate exception into a typed defect.  Predicates are expected
    to be pure, but catching an accidental exception here keeps it from
    escaping through the activation loop or corrupting waiter bookkeeping. *)
let evaluate predicate =
  try predicate () with
  | exn ->
      Error
        (Temporal_base.Error.defect
           ~message:(
             "Temporal condition predicate raised: " ^ Printexc.to_string exn))

(** Unlinks [waiter] through its own registry handle in O(1).  Each
    registration owns a distinct handle, so this cannot remove another waiter
    with an equal predicate. *)
let remove waiter =
  Option.iter Temporal_base.Ordered_registry.remove waiter.registration;
  waiter.registration <- None

(** Marks one registration inactive and queues its result exactly once.  The
    removal happens before [resolve], because resolving queues a continuation
    and user code may run again during the same activation drain. *)
let settle waiter result =
  if waiter.active then (
    waiter.active <- false;
    remove waiter;
    waiter.resolve result)

(** Registers a false predicate after the initial evaluation.  Registration and
    suspension happen on the owning scheduler Domain, so no notification can
    interleave between these operations; the next post-drain notification
    performs the next predicate evaluation. Keeping registration single-shot is
    important for both deterministic replay and predicates that are costly to
    evaluate. *)
let register store predicate =
  let future, resolve =
    Scheduler.promise store.scheduler ~outside_error:ownership_error
  in
  let waiter =
    {
      predicate;
      resolve;
      active = true;
      registration = None;
    }
  in
  waiter.registration <-
    Some (Temporal_base.Ordered_registry.add store.waiters waiter);
  future

(** Evaluates the predicate immediately, then suspends through a scheduler-owned
    future only when it is false.  No waiter is registered for an already true
    predicate or for a predicate that reports an error. *)
let wait_until store ~predicate =
  if store.closed then Error (closed_error ())
  else if not (owns_scheduler store) then Error (ownership_error ())
  else
    match evaluate predicate with
    | Error error -> Error error
    | Ok true -> Ok ()
    | Ok false ->
        let future = register store predicate in
        Future_store.await future

(** Checks all waiters from a stable FIFO snapshot.  A predicate that becomes
    true or fails is removed before its signal is resolved; remaining waiters
    are still checked in their original order, so one bad predicate cannot
    starve later registrations. *)
let notify store =
  if store.closed then false
  else
    let queued = ref false in
    let snapshot = Temporal_base.Ordered_registry.to_list store.waiters in
    List.iter
      (fun waiter ->
        if waiter.active then
          match evaluate waiter.predicate with
          | Ok false -> ()
          | Ok true ->
              queued := true;
              settle waiter (Ok ())
          | Error error ->
              queued := true;
              settle waiter (Error error))
      snapshot;
    !queued

(** Closes the store and drops every predicate and resolver before scheduler
    teardown.  The scheduler-owned futures are then closed by [Scheduler.shutdown],
    which safely discontinues any workflow continuations parked on them. *)
let shutdown store =
  if not store.closed then (
    store.closed <- true;
    List.iter
      (fun waiter ->
        waiter.active <- false;
        waiter.registration <- None)
      (Temporal_base.Ordered_registry.take_all store.waiters))
