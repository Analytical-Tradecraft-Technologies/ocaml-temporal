(** Pairs queued work with its enqueue sequence. These values live only until
    the work is drained; the scheduler retains no completed-work history. *)
type runnable = Runnable of int * (unit -> unit)

(** Private effect used by terminal workflow operations. The interface is
    declared in the signature so only package-internal modules can perform it. *)
type _ Effect.t += Abort_workflow : 'value Effect.t

(** Private control exception used only to settle an aborted fiber.
    It must not be converted into a workflow failure by user-code
    try/with wrappers; the deep handler treats it like shutdown. *)
exception Workflow_aborted

(** State for one workflow scheduler. [pending] counts futures without results,
    and [teardowns] stores one removable cleanup token for each pending future.
    Settling a future removes its token so completed values are not retained by
    a long-lived workflow scheduler. *)
type teardown_token = {
  mutable removed : bool;
  action : unit -> unit;
}

type t = {
  id : int;
  queue : runnable Queue.t;
  mutable next_sequence : int;
  mutable running : bool;
  mutable active : bool;
  mutable pending : int;
  mutable failures : exn list;
  mutable teardowns : teardown_token list;
  mutable abort_requested : bool;
}

type status = Complete | Failed of exn | Blocked

(** Supplies a different ID to every scheduler in the process, allowing
    [Future.both] to reject futures from different workflow executions. *)
let next_scheduler_id = Atomic.make 0

(** Initializes an active scheduler with no runnable or pending work. *)
let create () =
  {
    id = Atomic.fetch_and_add next_scheduler_id 1;
    queue = Queue.create ();
    next_sequence = 0;
    running = false;
    active = true;
    pending = 0;
    failures = [];
    teardowns = [];
    abort_requested = false;
  }

(** Returns the stable identity assigned to this workflow scheduler.  Runtime
    stores use it with the Domain-local owner marker to reject cross-workflow
    waits before they retain a continuation. *)
let id scheduler = scheduler.id

(** Reports whether a scheduler is currently draining queued workflow work. *)
let is_running scheduler = scheduler.running

(** Reports whether the scheduler still accepts workflow futures and queued
    callbacks.  Shutdown flips this flag before releasing continuations. *)
let is_active scheduler = scheduler.active

(** Assigns FIFO sequence numbers at enqueue time. Work submitted after shutdown
    is intentionally discarded only by internal callbacks already being torn down. *)
let enqueue scheduler thunk =
  if scheduler.active then (
    let sequence = scheduler.next_sequence in
    scheduler.next_sequence <- sequence + 1;
    Queue.push (Runnable (sequence, thunk)) scheduler.queue)

(** Gives a future access to this scheduler's queue and pending-future count. *)
let owner scheduler =
  Future_store.make_owner ~id:scheduler.id ~enqueue:(enqueue scheduler)
    ~is_running:(fun () -> scheduler.running && scheduler.active)
    ~callbacks_live:(fun () -> scheduler.running && scheduler.active)
    ~on_create:(fun () -> scheduler.pending <- scheduler.pending + 1)
    ~on_settled:(fun () -> scheduler.pending <- scheduler.pending - 1)
    ~register_teardown:(fun teardown ->
      let token = { removed = false; action = teardown } in
      scheduler.teardowns <- token :: scheduler.teardowns;
      (* Physical identity is safe here because every registration allocates a
         fresh token. The linear scan keeps shutdown order explicit while
         releasing the completed future's closure immediately. *)
      fun () ->
        if not token.removed then (
          token.removed <- true;
          scheduler.teardowns <-
            List.filter (fun current -> current != token) scheduler.teardowns))

(** Rejects new future allocation after shutdown, when no continuation could be
    safely resumed. *)
let promise scheduler ~outside_error =
  if not scheduler.active then invalid_arg "Temporal scheduler is shut down";
  Future_store.create ~owner:(owner scheduler) ~outside_error

(** Handles the private [Await] effect by saving the paused workflow fiber on
    its future. Effects not owned by this scheduler continue to an outer
    handler. The Domain-local owner id is published by [run] for the whole
    drain so resumed fibers still see the correct owner after an await. *)
let handle scheduler thunk =
  Effect.Deep.match_with thunk ()
    {
      retc = (fun () -> ());
      exnc =
        (fun exception_ ->
          (* Scheduler_shutdown only releases paused fibers during teardown; it
             is control flow, not a workflow defect. *)
          match exception_ with
          | Future_store.Scheduler_shutdown | Workflow_aborted -> ()
          | _ -> scheduler.failures <- exception_ :: scheduler.failures);
      effc =
        (fun (type result) (operation : result Effect.t) ->
          match operation with
          | Future_store.Await future ->
              Some
                (fun (continuation : (result, unit) Effect.Deep.continuation) ->
                  Future_store.add_waiter future continuation)
          | Abort_workflow ->
              Some
                (fun (continuation : (result, unit) Effect.Deep.continuation) ->
                  (* A terminal command has already been buffered. Stop sibling
                     fibers from appending further commands, then settle this
                     one-shot continuation so Fun.protect cleanups still run.
                     Use [Workflow_aborted] rather than [Scheduler_shutdown] so
                     workflow try/with wrappers re-raise it instead of turning
                     a deliberate abort into Fail_workflow. *)
                  scheduler.abort_requested <- true;
                  try Effect.Deep.discontinue continuation Workflow_aborted
                  with Workflow_aborted -> ())
          | _ -> None);
    }

(** Wraps root user code in the scheduler handler before enqueueing it. *)
let spawn scheduler thunk =
  if not scheduler.active then invalid_arg "Temporal scheduler is shut down";
  (* A root thunk may already be queued when another running fiber shuts the
     scheduler down. Keep the runnable in the queue until the owner drain
     reaches it, so native future continuations queued alongside it can still
     discontinue themselves and run their cleanup handlers. *)
  enqueue scheduler (fun () ->
      if scheduler.active then handle scheduler thunk else ())

(** Runs queued work until the queue is empty, including fibers added by future
    completions during the run. An uncaught exception is reported before
    [Blocked] or [Complete]. Publishes this scheduler as the Domain-local owner
    for the entire drain so [Future_store.await] accepts parking only on this
    execution's futures, including after a fiber resumes from a prior await. *)
let run scheduler =
  if not scheduler.active then invalid_arg "Temporal scheduler is shut down";
  if scheduler.running then invalid_arg "Temporal scheduler is already running";
  (* Abort is a per-drain control flag. Clearing it here lets a later
     activation of a still-live execution drain newly queued work; a terminal
     path seals the execution separately so this cannot revive a finished run. *)
  scheduler.abort_requested <- false;
  scheduler.running <- true;
  Fun.protect
    ~finally:(fun () -> scheduler.running <- false)
    (fun () ->
      Future_store.with_current_owner_id (Some scheduler.id) (fun () ->
          while
            (not (Queue.is_empty scheduler.queue))
            && not scheduler.abort_requested
          do
            let (Runnable (_, thunk)) = Queue.pop scheduler.queue in
            (* Once shutdown is requested, root thunks are inert but queued
               future callbacks must still run their owner-aware cleanup path. *)
            (try thunk ()
             with Future_store.Scheduler_shutdown | Workflow_aborted -> ()
             | exception_ ->
                 scheduler.failures <- exception_ :: scheduler.failures)
          done;
          match List.rev scheduler.failures with
          | failure :: _ -> Failed failure
          | [] when scheduler.pending > 0 -> Blocked
          | [] -> Complete))

(** Stable string adapter used by smoke tests and future diagnostics. *)
let run_label scheduler =
  match run scheduler with
  | Complete -> "complete"
  | Failed _ -> "failed"
  | Blocked -> "blocked"

(** Closes pending futures in creation order, then drains any already-queued
    resumptions. Waiters parked on futures that settled before shutdown were
    moved onto the queue as continue thunks; [Queue.clear] would drop those
    one-shot continuations without continue or discontinue. Drain them while
    [active] is false so the thunks discontinue instead, and skip the drain
    when the scheduler is mid-[run] so the existing loop owns the queue. *)
let shutdown scheduler =
  if scheduler.active then (
    scheduler.active <- false;
    List.iter
      (fun token ->
        if not token.removed then (
          token.removed <- true;
          token.action ()))
      (List.rev scheduler.teardowns);
    scheduler.teardowns <- [];
    if not scheduler.running then
      while not (Queue.is_empty scheduler.queue) do
        let (Runnable (_, thunk)) = Queue.pop scheduler.queue in
        try thunk () with
        | Future_store.Scheduler_shutdown | Workflow_aborted -> ()
        | exception_ -> scheduler.failures <- exception_ :: scheduler.failures
      done)

(** Performs the scheduler's private terminal control effect. *)
let abort_workflow () = Effect.perform Abort_workflow
