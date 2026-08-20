(** Implements workflow-local cancellation scopes without exposing the
    scheduler effect or the internal notification future. A scope's signal is
    an ordinary scheduler-owned future, so cancellation and waiter resumption
    use the same deterministic FIFO queue as every other workflow future. *)

type state = Active | Cancelled

(** The private state retained by one scope. The resolver and liveness callback
    are kept only to complete the scheduler-owned signal exactly once and to
    avoid touching a future already closed by workflow teardown. *)
type t = {
  cancellation : (unit, Error.t) Future.t;
  resolve : (unit, Temporal_base.Error.t) result -> unit;
  owner_id : int;
  callbacks_live : unit -> bool;
  mutable state : state;
  (* Hooks are kept by the owning scheduler and invoked in registration order
     when this scope is cancelled.  They are the bridge from cooperative OCaml
     observation to real Temporal cancellation commands. *)
  mutable cancel_hooks : (unit -> (unit, Error.t) result) list;
}

(** Constructs the stable public error returned when a scope has been
    cancelled. This is a normal operational result, not an exception. *)
let cancellation_error () =
  Error.make ~category:`Cancelled ~message:"workflow scope cancelled" ()

(** Constructs the defect used when a scope operation is attempted from an
    unrelated Domain or outside its workflow scheduler. *)
let ownership_error operation =
  Error.defect
    ~message:("Temporal.Scope." ^ operation ^ " used outside its owning workflow scheduler")

(** Creates a scheduler-owned scope signal only while a workflow context is
    active. The runtime context owns the underlying future and registers its
    teardown, while this public module retains only the typed façade and its
    one resolver. *)
let create () =
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None ->
      Error
        (Error.defect
           ~message:"Temporal.Scope.create used outside a workflow execution")
  | Some context ->
      let cancellation_base, resolve =
        Temporal_sdk_kernel.Workflow_context_store.create_signal context
      in
      let cancellation =
        Future_private.of_internal
          ~outside_error:cancellation_error cancellation_base
      in
      Ok
        {
          cancellation;
          resolve;
          owner_id =
            Temporal_sdk_kernel.Future_store.owner_id cancellation_base;
          callbacks_live =
            (fun () ->
              Temporal_sdk_kernel.Future_store.callbacks_live cancellation_base);
          state = Active;
          cancel_hooks = [];
        }

(** Checks whether the current scheduler is the one that owns [scope] and is
    still processing workflow callbacks. The liveness check makes a handle
    stale as soon as its scheduler has been shut down, including when teardown
    is requested from inside the scheduler's final drain. The short-circuit
    keeps the liveness callback from reading scheduler state on a foreign
    Domain. *)
let owns_scheduler scope =
  Temporal_sdk_kernel.Future_store.current_owner_matches scope.owner_id
  && scope.callbacks_live ()

(** Records cancellation and signals waiters through the owning scheduler.
    An active scope cannot be cancelled between scheduler runs or from another
    Domain: doing so would mutate state without a queue turn that can resume
    its waiters. The ownership check happens before reading [state], so even a
    repeated call cannot race an owner-domain cancellation. *)
let cancel scope =
  if not (owns_scheduler scope) then Error (ownership_error "cancel")
  else
    match scope.state with
    | Cancelled -> Ok ()
    | Active ->
        scope.state <- Cancelled;
        if scope.callbacks_live () then scope.resolve (Ok ());
        (* Seal the hook list before invoking user-owned closures.  If a hook
           re-enters [cancel], it therefore observes the already-cancelled
           state and cannot run this list twice. *)
        let hooks = List.rev scope.cancel_hooks in
        scope.cancel_hooks <- [];
        let first_error =
          List.fold_left
            (fun first_error hook ->
              let result =
                try hook () with
                | exn ->
                    Error
                      (Error.defect
                         ~message:
                           ("Temporal.Scope cancellation hook raised: "
                           ^ Printexc.to_string exn))
              in
              match (first_error, result) with
              | Some error, _ -> Some error
              | None, Error error -> Some error
              | None, Ok () -> None)
            None hooks
        in
        match first_error with None -> Ok () | Some error -> Error error

(** Registers a server-side cancellation action.  An active scope stores the
    hook until cancellation; registering after cancellation runs it
    immediately, closing the cancel-before-registration race without another
    lock or Domain. *)
let on_cancel scope hook =
  if not (owns_scheduler scope) then Error (ownership_error "on_cancel")
  else
    match scope.state with
    | Active ->
        scope.cancel_hooks <- hook :: scope.cancel_hooks;
        Ok ()
    | Cancelled ->
        (try hook () with
        | exn ->
            Error
              (Error.defect
                 ~message:
                   ("Temporal.Scope cancellation hook raised: "
                   ^ Printexc.to_string exn)))

(** Reports the scope state without scheduling work or touching the resolver.
    Status is an owner-domain operation just like [cancel]: returning a typed
    defect for a foreign or stale handle prevents a cross-Domain read of the
    mutable state and makes post-shutdown use explicit. *)
let is_cancelled scope =
  if not (owns_scheduler scope) then Error (ownership_error "is_cancelled")
  else Ok (match scope.state with Cancelled -> true | Active -> false)

(** Returns the scope's current cancellation result. The owner check is kept
    here rather than delegated to [is_cancelled] so the operation has one
    clear typed failure for a foreign or already-shutdown execution. *)
let check scope =
  if not (owns_scheduler scope) then Error (ownership_error "check")
  else
    match scope.state with
    | Cancelled -> Error (cancellation_error ())
    | Active -> Ok ()

(** Verifies that [scope] is being used by its owner scheduler before it can
    suspend a workflow fiber. Ready futures still obey this check: accepting a
    ready value from another execution would make ownership errors depend on
    timing rather than on the API contract. *)
let ensure_owner scope =
  if owns_scheduler scope then Ok () else Error (ownership_error "await")

(** Waits for either the operation or the scope signal. The operation is
    registered first, so if both inputs are already ready the deterministic
    Future race rule gives the operation precedence; a cancellation already
    recorded in [state] wins before registration. *)
let await scope future =
  match ensure_owner scope with
  | Error error -> Error error
  | Ok () -> (
      match check scope with
      | Error error -> Error error
      | Ok () ->
          match Future.await (Future.race future scope.cancellation) with
          | Error error -> Error error
          | Ok (Future.Left value) -> Ok value
          | Ok (Future.Right ()) -> Error (cancellation_error ()))

(** Runs a body inside a fresh scope and always requests cancellation during
    cleanup. A successful body returns a cleanup error so a failed durable
    cancellation request cannot be reported as overall success. The body's
    typed error or exception remains primary after cleanup is attempted. *)
let with_scope body =
  match create () with
  | Error _ as error -> error
  | Ok scope -> (
      match body scope with
      | exception exn ->
          ignore (cancel scope);
          raise exn
      | Error _ as error ->
          ignore (cancel scope);
          error
      | Ok value -> Result.map (fun () -> value) (cancel scope))
