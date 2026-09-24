(** Opaque value containing callbacks owned by one workflow scheduler.

    This package-private library is installed only below the
    [temporal-sdk/__private__] tree. Its callback contract is intentionally
    generic: it knows how to queue and observe a value owned by a scheduler,
    but it does not know about Temporal commands, Rust handles, or OCaml
    effects. *)
type ('value, 'error) t

(** Constructs a kernel future from callbacks owned by one scheduler.  The
    callback owner remains responsible for lifecycle cleanup and this
    constructor never starts a thread, allocates a queue, or accepts a native
    handle. *)
val make :
  await:(unit -> ('value, 'error) result) ->
  await_gate:((((unit -> unit) -> unit) -> unit)) ->
  observe:((('value, 'error) result -> unit) -> unit) ->
  is_ready:(unit -> bool) ->
  peek:(unit -> ('value, 'error) result option) ->
  owner_id:int ->
  outside_error:(unit -> 'error) ->
  callbacks_live:(unit -> bool) ->
  enqueue:((unit -> unit) -> unit) ->
  ('value, 'error) t

(** Invokes the scheduler-owned result callback. *)
val await : ('value, 'error) t -> ('value, 'error) result

(** Extracts the scheduler-owned suspension gate without retaining the future's
    result. *)
val await_gate : ('value, 'error) t -> (((unit -> unit) -> unit) -> unit)

(** Registers an observer for a scheduler-owned result notification. *)
val observe :
  ('value, 'error) t -> (('value, 'error) result -> unit) -> unit

(** Reports whether the scheduler-owned result is settled. *)
val is_ready : ('value, 'error) t -> bool

(** Reads a settled scheduler-owned result without consuming it. *)
val peek : ('value, 'error) t -> ('value, 'error) result option

(** Returns the workflow-execution identity that owns this value. *)
val owner_id : ('value, 'error) t -> int

(** Builds the error returned when the value is used outside its owner. *)
val outside_error : ('value, 'error) t -> unit -> 'error

(** Extracts the owner's liveness predicate without retaining this future's
    result. Adapters must pass this callback directly, without wrapping a call
    that captures the source future. *)
val callback_liveness : ('value, 'error) t -> (unit -> bool)

(** Reports whether queued callbacks may still run for this future's owner. *)
val callbacks_live : ('value, 'error) t -> bool

(** Extracts the owner queue callback without retaining this value. *)
val enqueue : ('value, 'error) t -> (unit -> unit) -> unit
