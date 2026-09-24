(** Contains the scheduler operations used by a future. Futures queue all
    resumptions through these functions, so workflow fibers still resume one at
    a time even when Rust reports several results concurrently. The callback
    liveness predicate also lets already-resolved outside-workflow futures
    deliver combinator callbacks inline without making that inert owner valid
    for workflow awaits. *)
type owner = {
  id : int;
  enqueue : (unit -> unit) -> unit;
  is_running : unit -> bool;
  callbacks_live : unit -> bool;
  on_create : unit -> unit;
  on_settled : unit -> unit;
  register_teardown : (unit -> unit) -> (unit -> unit);
}

(** Collects the scheduler callbacks stored in each future. *)
let make_owner ~id ~enqueue ~is_running ~callbacks_live ~on_create ~on_settled
    ~register_teardown =
  { id; enqueue; is_running; callbacks_live; on_create; on_settled;
    register_teardown }

(** Gives the standard library [result] type a local name distinct from [state]. *)
type ('value, 'error) result = ('value, 'error) Stdlib.result

(** A paused workflow fiber waiting for a result. OCaml continuations can be
    resumed only once, which matches a future's single-result rule. *)
type ('value, 'error) waiter =
  (('value, 'error) result, unit) Effect.Deep.continuation

(** Holds paused fibers and mapping callbacks while a result is unavailable.
    Lists are reversed so adding a new entry is constant time. *)
type ('value, 'error) pending = {
  mutable waiters : ('value, 'error) waiter list;
  mutable observers : (('value, 'error) result -> unit) option ref list;
}

(** [Closed] means the workflow ended before this future received a result and
    all paused fibers have been released. *)
type ('value, 'error) state =
  | Pending of ('value, 'error) pending
  | Ready of ('value, 'error) result
  | Closed

(** Stores the scheduler, the error to return when used from the wrong context,
    and the future's current state. *)
type ('value, 'error) t = {
  owner : owner;
  outside_error : unit -> 'error;
  mutable state : ('value, 'error) state;
}

type ('value, 'error) resolver = ('value, 'error) result -> unit

(** Identifies which input completed a heterogeneous two-way race. The public
    SDK re-exports this type while keeping the observer machinery private. *)
type ('left, 'right) race = Left of 'left | Right of 'right

type _ Effect.t +=
  | Await : ('value, 'error) t -> ('value, 'error) result Effect.t

(** Domain-local id of the scheduler fiber currently running on this Domain.
    [await] requires this to match the future's owner so a fiber cannot park on
    another workflow's pending future merely because that other scheduler is
    running somewhere else. *)
let current_owner_id_key = Domain.DLS.new_key (fun () -> (None : int option))

(** Publishes or clears the active owner id for the duration of one fiber. *)
let with_current_owner_id id action =
  let previous = Domain.DLS.get current_owner_id_key in
  Domain.DLS.set current_owner_id_key id;
  Fun.protect
    ~finally:(fun () -> Domain.DLS.set current_owner_id_key previous)
    action

(** True when this Domain is currently running the scheduler with [id]. *)
let current_owner_matches id =
  match Domain.DLS.get current_owner_id_key with
  | Some current -> current = id
  | None -> false

(** Internal exception used only to release a paused fiber during shutdown. It
    is caught inside this module and never becomes a workflow error. *)
exception Scheduler_shutdown

(** Closes a pending future during workflow completion, eviction, or shutdown.
    It updates the scheduler's pending count, clears callback references, and
    releases every paused fiber so the workflow's memory can be collected. *)
let teardown promise () =
  match promise.state with
  | Ready _ | Closed -> ()
  | Pending pending ->
      promise.state <- Closed;
      promise.owner.on_settled ();
      let waiters = List.rev pending.waiters in
      pending.waiters <- [];
      List.iter (fun observer -> observer := None) pending.observers;
      pending.observers <- [];
      List.iter
        (fun continuation ->
          try Effect.Deep.discontinue continuation Scheduler_shutdown
          with Scheduler_shutdown -> ())
        waiters

(** Creates a pending future and its resolver. The resolver stores the result
    before queueing paused fibers and callbacks, so any callback that inspects
    the future immediately sees it as ready. *)
let create ~owner ~outside_error =
  let promise =
    { owner; outside_error; state = Pending { waiters = []; observers = [] } }
  in
  owner.on_create ();
  (* Settled futures no longer need a shutdown callback. Removing the
     registration here is important: a long-lived scheduler must not retain
     every completed result through its teardown ledger. *)
  let unregister_teardown = owner.register_teardown (teardown promise) in
  let settle () =
    unregister_teardown ();
    owner.on_settled ()
  in
  let resolve result =
    match promise.state with
    | Ready _ | Closed -> invalid_arg "Temporal future resolved more than once"
    | Pending pending ->
        promise.state <- Ready result;
        settle ();
        (* Resume only while the owning scheduler is still live. After
           shutdown, the same queued thunk must discontinue the one-shot
           continuation so Fun.protect cleanups still run and the fiber is
           never silently dropped by Queue.clear. *)
        List.iter
          (fun continuation ->
            owner.enqueue (fun () ->
                if owner.callbacks_live () then
                  Effect.Deep.continue continuation result
                else
                  try Effect.Deep.discontinue continuation Scheduler_shutdown
                  with Scheduler_shutdown -> ()))
          (List.rev pending.waiters);
        List.iter
          (fun observer ->
            owner.enqueue (fun () ->
                let callback = !observer in
                observer := None;
                if owner.callbacks_live () then
                  Option.iter (fun callback -> callback result) callback))
          (List.rev pending.observers);
        pending.waiters <- [];
        pending.observers <- []
  in
  (promise, resolve)

(** Scheduler substitute for an already-known result created outside workflow
    execution. It runs callbacks immediately and never allows [await] to pause. *)
let inert_owner =
  make_owner ~id:(-1) ~enqueue:(fun thunk -> thunk ())
    ~is_running:(fun () -> false) ~callbacks_live:(fun () -> true)
    ~on_create:(fun () -> ())
    ~on_settled:(fun () -> ())
    ~register_teardown:(fun _ -> fun () -> ())

(** Creates an outside-workflow future and immediately gives it a result using
    the same resolver logic as ordinary futures. *)
let resolved ~outside_error result =
  let promise, resolve = create ~owner:inert_owner ~outside_error in
  resolve result;
  promise

let owner_id promise = promise.owner.id

(** Reports whether callbacks queued for [promise] may still run. The scheduler
    turns this false during shutdown, while the inert owner for resolved
    outside-workflow values keeps it true for immediate combinators. *)
let callbacks_live promise = promise.owner.callbacks_live ()

(** Queues one callback on the scheduler that owns [promise]. This is the
    low-level escape hatch used by public wrappers to preserve the same FIFO
    execution ordering as native future completions. *)
let enqueue promise thunk = promise.owner.enqueue thunk

(** Returns a ready result, or pauses only when called from the active fiber of
    the scheduler that owns this future. [is_running] alone is insufficient:
    another Domain may be running a different scheduler whose [is_running] is
    true while this Domain is not that owner. *)
let await promise =
  match promise.state with
  | Ready result -> result
  | Closed -> Error (promise.outside_error ())
  | Pending _ -> (
      match Domain.DLS.get current_owner_id_key with
      | Some id
        when id = promise.owner.id && promise.owner.is_running () ->
          Effect.perform (Await promise)
      | Some _ | None -> Error (promise.outside_error ()))

(** Saves a paused fiber on a pending future. If the result or shutdown happened
    first, queues the fiber immediately with the appropriate result. *)
let add_waiter promise continuation =
  match promise.state with
  | Pending pending -> pending.waiters <- continuation :: pending.waiters
  | Ready result ->
      promise.owner.enqueue (fun () ->
          if promise.owner.callbacks_live () then
            Effect.Deep.continue continuation result
          else
            try Effect.Deep.discontinue continuation Scheduler_shutdown
            with Scheduler_shutdown -> ())
  | Closed ->
      promise.owner.enqueue (fun () ->
          if promise.owner.callbacks_live () then
            Effect.Deep.continue continuation (Error (promise.outside_error ()))
          else
            try Effect.Deep.discontinue continuation Scheduler_shutdown
            with Scheduler_shutdown -> ())

(** Registers a callback used by [map], [map_error], and [both]. It runs
    through the workflow scheduler without pausing a fiber; callbacks queued
    when the owner shuts down become no-ops after runtime teardown. Public
    derived wrappers use the same liveness signal to avoid scheduling work
    from these skipped callbacks. The returned owner-only removal action also
    unlinks pending list storage and suppresses callbacks already queued. *)
let subscribe promise observer =
  let token = ref (Some observer) in
  let deliver result () =
    let callback = !token in
    token := None;
    if promise.owner.callbacks_live () then
      Option.iter (fun callback -> callback result) callback
  in
  (match promise.state with
  | Pending pending -> pending.observers <- token :: pending.observers
  | Ready result -> promise.owner.enqueue (deliver result)
  | Closed -> promise.owner.enqueue (deliver (Error (promise.outside_error ()))));
  fun () ->
    token := None;
    match promise.state with
    | Pending pending ->
        pending.observers <- List.filter (fun current -> current != token) pending.observers
    | Ready _ | Closed -> ()

(** Registers an observer whose lifetime ends at delivery or owner teardown. *)
let observe promise observer =
  let (_ : unit -> unit) = subscribe promise observer in
  ()

(** Waits for an arbitrary notification associated with [promise] without
    exposing an OCaml effect to higher layers. [register] receives a
    single-use signal callback; the callback must be invoked by the observer
    that makes the notification ready. The temporary gate is owned by the
    same scheduler, so the calling workflow fiber is suspended rather than a
    scheduler thread being blocked. *)
let await_gate promise register =
  let gate, resolve = create ~owner:promise.owner ~outside_error:(fun () -> ()) in
  let signaled = ref false in
  let signal () =
    if not !signaled then (
      signaled := true;
      resolve (Ok ()))
  in
  register signal;
  ignore (await gate)

(** Maps a successful result into a new future owned by the same scheduler. *)
let map mapper source =
  let mapped, resolve =
    create ~owner:source.owner ~outside_error:source.outside_error
  in
  observe source (fun result -> resolve (Result.map mapper result));
  mapped

(** Maps both a stored error and the error returned for use outside the owning
    workflow scheduler. *)
let map_error mapper source =
  let mapped, resolve =
    create ~owner:source.owner
      ~outside_error:(fun () -> mapper (source.outside_error ()))
  in
  observe source (fun result -> resolve (Result.map_error mapper result));
  mapped

(** Creates a ready failure owned by [source]. Aggregate ownership errors use
    this path so application mistakes remain typed values while subsequent
    combinators still belong to the original workflow scheduler. *)
let failed_from source error =
  let failed, resolve =
    create ~owner:source.owner ~outside_error:source.outside_error
  in
  resolve (Error error);
  failed

(** Reports whether every future belongs to the scheduler that owns [first]. *)
let same_owner first futures =
  List.for_all (fun future -> future.owner.id = first.owner.id) futures

(** Records both results before completing the combined future. One failure
    does not implicitly cancel or stop observing the other operation. A
    cross-scheduler pair produces [ownership_error] as a value. *)
let both ~ownership_error left right =
  if left.owner.id <> right.owner.id then failed_from left (ownership_error ())
  else
  let combined, resolve =
    create ~owner:left.owner ~outside_error:left.outside_error
  in
  let left_result = ref None in
  let right_result = ref None in
  let finish () =
    match (!left_result, !right_result) with
    | Some (Ok left), Some (Ok right) -> resolve (Ok (left, right))
    | Some (Error error), Some _ -> resolve (Error error)
    | Some _, Some (Error error) -> resolve (Error error)
    | _ -> ()
  in
  observe left (fun result ->
      left_result := Some result;
      finish ());
  observe right (fun result ->
      right_result := Some result;
      finish ());
  combined

(** Completes after every input and preserves input order. Errors are selected
    in input order only after all siblings settle, so aggregation never implies
    cancellation. *)
let all ~ownership_error futures =
  match futures with
  | [] -> resolved ~outside_error:ownership_error (Ok [])
  | first :: _ when not (same_owner first futures) ->
      failed_from first (ownership_error ())
  | first :: _ ->
      let combined, resolve =
        create ~owner:first.owner ~outside_error:first.outside_error
      in
      let remaining = ref (List.length futures) in
      let results = Array.make !remaining None in
      let finish_if_complete () =
        if !remaining = 0 then
          let ordered = Array.to_list results in
          match List.find_map (function Some (Error error) -> Some error | _ -> None) ordered with
          | Some error -> resolve (Error error)
          | None ->
              resolve
                (Ok
                   (List.map
                      (function
                        | Some (Ok value) -> value
                        | Some (Error _) | None ->
                            failwith "Temporal.Future.all result invariant violated")
                      ordered))
      in
      List.iteri
        (fun index future ->
          observe future (fun result ->
              results.(index) <- Some result;
              remaining := !remaining - 1;
              finish_if_complete ()))
        futures;
      combined

(** Settles with the first observed completion of two differently typed inputs.
    Observer registration order makes an already-ready left input win; pending
    inputs follow the scheduler's deterministic callback order. *)
let race ~ownership_error left right =
  if left.owner.id <> right.owner.id then failed_from left (ownership_error ())
  else
    let combined, resolve =
      create ~owner:left.owner ~outside_error:left.outside_error
    in
    let settled = ref false in
    let subscriptions = ref [] in
    (* Inert ready futures can deliver inline, before [subscribe] returns its
       remover. Detach that late token immediately if a winner already ran. *)
    let register subscribe =
      let remove = subscribe () in
      if !settled then remove () else subscriptions := remove :: !subscriptions
    in
    let resolution = ref (Some resolve) in
    let finish wrap result =
      if not !settled then (
        settled := true;
        let resolve = Option.get !resolution in
        resolution := None;
        let removals = !subscriptions in
        subscriptions := [];
        List.iter (fun remove -> remove ()) removals;
        resolve (Result.map wrap result))
    in
    register (fun () -> subscribe left (finish (fun value -> Left value)));
    register (fun () -> subscribe right (finish (fun value -> Right value)));
    combined

(** Settles with the first completion from a non-empty homogeneous collection.
    The mandatory [first] argument makes an empty selection unrepresentable. *)
let first ~ownership_error leading rest =
  if not (same_owner leading rest) then failed_from leading (ownership_error ())
  else
    let combined, resolve =
      create ~owner:leading.owner ~outside_error:leading.outside_error
    in
    let settled = ref false in
    let subscriptions = ref [] in
    (* Inert ready futures can deliver inline, before [subscribe] returns its
       remover. Detach that late token immediately if a winner already ran. *)
    let register subscribe =
      let remove = subscribe () in
      if !settled then remove () else subscriptions := remove :: !subscriptions
    in
    let resolution = ref (Some resolve) in
    let finish result =
      if not !settled then (
        settled := true;
        let resolve = Option.get !resolution in
        resolution := None;
        let removals = !subscriptions in
        subscriptions := [];
        List.iter (fun remove -> remove ()) removals;
        resolve result)
    in
    List.iter (fun future -> register (fun () -> subscribe future finish)) (leading :: rest);
    combined

(** Checks the state directly without pausing or scheduling work. *)
let is_ready promise = match promise.state with Ready _ -> true | _ -> false

(** Returns a ready result for inspection without removing it from the future. *)
let peek promise =
  match promise.state with Ready result -> Some result | Pending _ | Closed -> None
