(** Private callback representation shared by the public future facade and its
    package-internal runtime adapters.  The public [Temporal.Future] module
    aliases this type but exposes no constructor or record fields. *)
type ('value, 'error) t = {
  (* Retrieves the result while the owning scheduler is active. *)
  await_impl : unit -> ('value, 'error) result;
  (* Registers a continuation with the scheduler's suspension gate. *)
  await_gate_impl : (((unit -> unit) -> unit) -> unit);
  (* Registers a callback for the next owner-scheduler notification. *)
  subscribe_impl : (('value, 'error) result -> unit) -> (unit -> unit);
  (* Reports whether this operation has already settled. *)
  is_ready_impl : unit -> bool;
  (* Reads a settled result without consuming it. *)
  peek_impl : unit -> ('value, 'error) result option;
  (* Identifies the workflow execution that owns this future. *)
  owner_id_impl : int;
  (* Builds the typed error returned when the future is used off-owner. *)
  outside_error_impl : unit -> 'error;
  (* Reports whether queued callbacks may still run for this owner. *)
  callbacks_live_impl : unit -> bool;
  (* Queues a callback on the owning scheduler. *)
  enqueue_impl : (unit -> unit) -> unit;
}

(** Constructs a kernel future from callbacks owned by one scheduler.  The
    callbacks remain the source of truth for lifecycle and cleanup; this
    record only groups those callbacks behind the private package boundary. *)
let make ~await ~await_gate ~subscribe ~is_ready ~peek ~owner_id ~outside_error
    ~callbacks_live ~enqueue =
  {
    await_impl = await;
    await_gate_impl = await_gate;
    subscribe_impl = subscribe;
    is_ready_impl = is_ready;
    peek_impl = peek;
    owner_id_impl = owner_id;
    outside_error_impl = outside_error;
    callbacks_live_impl = callbacks_live;
    enqueue_impl = enqueue;
  }

(** Invokes the scheduler-owned result callback. *)
let await future = future.await_impl ()

(** Extracts the stored gate without closing over the source future. *)
let await_gate future = future.await_gate_impl

(** Registers an observer for a scheduler-owned result notification. *)
let observe future callback =
  let (_ : unit -> unit) = future.subscribe_impl callback in
  ()

(** Subscribes until delivery or explicit removal on the owning scheduler. *)
let subscribe future callback = future.subscribe_impl callback

(** Reports whether the scheduler-owned result is settled. *)
let is_ready future = future.is_ready_impl ()

(** Reads a settled scheduler-owned result without consuming it. *)
let peek future = future.peek_impl ()

(** Returns the workflow-execution identity that owns this value. *)
let owner_id future = future.owner_id_impl

(** Builds the error returned when the value is used outside its owner. *)
let outside_error future = future.outside_error_impl

(** Extracts the stored owner predicate without closing over the future. *)
let callback_liveness future = future.callbacks_live_impl

(** Reports callback liveness without transferring the predicate. *)
let callbacks_live future = callback_liveness future ()

(** Extracts the stored queue function without closing over the future. *)
let enqueue future = future.enqueue_impl
