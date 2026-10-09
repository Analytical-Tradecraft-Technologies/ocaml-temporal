(** Exact protobuf timestamp copied from an activity task. *)
type timestamp = { seconds : int64; nanoseconds : int }

(** Immutable Core task metadata; see the interface for field semantics. *)
type info = {
  namespace : string;
  workflow_id : string;
  workflow_run_id : string;
  workflow_type : string;
  activity_id : string;
  activity_type : string;
  attempt : int;
  is_local : bool;
  scheduled_time : timestamp option;
  current_attempt_scheduled_time : timestamp option;
  started_time : timestamp option;
  schedule_to_close_timeout : Duration.t option;
  start_to_close_timeout : Duration.t option;
  task_heartbeat_timeout : Duration.t option;
}

(** Core's activity cancellation reasons; see the interface. *)
type cancel_reason =
  | Requested
  | Timed_out
  | Not_found
  | Worker_shutdown
  | Paused
  | Reset

(** One observed cancellation: the primary reason and every reported fact. *)
type cancellation = { reason : cancel_reason; reasons : cancel_reason list }

(** Single-assignment cell written by the attempt's adapter and read from any
    Domain. [Atomic.compare_and_set] makes the first publication win without a
    lock, so a reader on another Domain never blocks behind native work. *)
type cancellation_signal = cancellation option Atomic.t

(** Allocates the empty cell for one attempt. *)
let cancellation_signal () = Atomic.make None

(** Publishes only into an empty cell, keeping the first cancellation stable
    for the rest of the attempt. *)
let signal_cancellation signal cancellation =
  Atomic.compare_and_set signal None (Some cancellation)

(** Reads the published cancellation without blocking. *)
let observed_cancellation signal = Atomic.get signal

(** Stable labels shared with the native adapter's cancellation failures. *)
let cancel_reason_label = function
  | Requested -> "cancelled"
  | Timed_out -> "timed_out"
  | Not_found -> "not_found"
  | Worker_shutdown -> "worker_shutdown"
  | Paused -> "paused"
  | Reset -> "reset"

(** Builds the [`Cancelled] error a cooperative callback returns. It is
    non-retryable because a cancelled attempt must not be retried as if it
    had failed; the adapter turns it into a Temporal cancellation. *)
let cancelled_error { reason; _ } =
  Error.make ~non_retryable:true ~category:`Cancelled
    ~message:("activity cancellation requested: " ^ cancel_reason_label reason)
    ()

(** Lifetime-checked state shared by one activity implementation and its
    native heartbeat callback. The mutex is local to the context: the adapter's
    poll mutex serializes task execution, while this shorter lock protects a
    context retained accidentally by user code.

    [details] is the heartbeat detail list delivered with the task, i.e. the
    last details recorded by the previous attempt. It is immutable for the
    lifetime of the attempt: this attempt's own heartbeats are forwarded to
    Temporal for the {e next} attempt and never replace it, matching the other
    Temporal SDKs' "previous attempt's heartbeat details" contract. *)
type t = {
  mutex : Mutex.t;
  mutable active : bool;
  details : Payload.t list;
  heartbeat_timeout : Duration.t option;
  heartbeat_fn : Payload.t list -> (unit, Error.t) result;
  (* Task metadata, absent for synthetic contexts. Every field is immutable,
     so reads need no lock and stay valid after invalidation. *)
  info : info option;
  (* Cancellation cell owned by the adapter that runs this attempt. Synthetic
     contexts get a private cell nobody writes, so they are never cancelled. *)
  cancellation : cancellation_signal;
  (* Non-blocking, Domain-safe probe of the owning worker's stop flag. *)
  worker_shutting_down : unit -> bool;
}

(** Copies metadata and bytes so no callback can retain a buffer owned by the
    JSON decoder or another activity invocation. *)
let copy_payload ({ Payload.metadata; data } : Payload.t) : Payload.t =
  {
    Payload.metadata = List.map (fun (key, value) -> (key, value)) metadata;
    data = Bytes.copy data;
  }

(** Copies a heartbeat detail list without changing its order. *)
let copy_payloads values = List.map copy_payload values

(** Constructs an active context after the adapter has validated details and
    timeout values. [info] is shared by both public constructors. *)
let make ~cancellation ~worker_shutting_down ~info ~heartbeat ~details
    ~heartbeat_timeout =
  {
    mutex = Mutex.create ();
    active = true;
    details = copy_payloads details;
    heartbeat_timeout;
    heartbeat_fn = heartbeat;
    info;
    cancellation;
    worker_shutting_down;
  }

(** Probe used by contexts that do not belong to a native worker. *)
let never_shutting_down () = false

(** Constructs a context with no Core task metadata. *)
let create ~heartbeat ~details ~heartbeat_timeout =
  make ~cancellation:(cancellation_signal ())
    ~worker_shutting_down:never_shutting_down ~info:None ~heartbeat ~details
    ~heartbeat_timeout

(** Constructs a context that retains Core metadata but no stop signals. *)
let create_with_info ~info ~heartbeat ~details ~heartbeat_timeout =
  make ~cancellation:(cancellation_signal ())
    ~worker_shutting_down:never_shutting_down ~info:(Some info) ~heartbeat
    ~details ~heartbeat_timeout

(** Constructs a native-worker context wired to its adapter's stop signals. *)
let create_for_task ~cancellation ~worker_shutting_down ~info ~heartbeat
    ~details ~heartbeat_timeout =
  make ~cancellation ~worker_shutting_down ~info:(Some info) ~heartbeat
    ~details ~heartbeat_timeout

(** Supplies a typed bridge error for the deterministic mock backend, which
    has no native Core worker capable of recording a heartbeat. *)
let unavailable ~details ~heartbeat_timeout =
  create ~details ~heartbeat_timeout ~heartbeat:(fun _details ->
      Error
        (Error.make ~category:`Bridge
           ~message:"activity heartbeat is unavailable on this worker backend"
           ()))

(** Runs one callback only while the context is active. The callback and
    invalidation are serialized so a terminal completion cannot race a token
    submission. Unexpected callback exceptions become non-retryable typed
    defects. A successful heartbeat deliberately leaves [context.details]
    unchanged: those details describe the previous attempt and must stay stable
    for the whole attempt.

    The cancellation cell is read after the callback, because the adapter's
    callback is where a pending Core cancellation is delivered. An observed
    cancellation takes precedence over the callback's own result: the details
    were still submitted, but the attempt should now stop. *)
let heartbeat context details =
  Mutex.lock context.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock context.mutex)
    (fun () ->
      if not context.active then
        Error
          (Error.make ~category:`Bridge
             ~message:"activity context is no longer active" ())
      else
        let details = copy_payloads details in
        let submitted =
          try context.heartbeat_fn details with
          | exception_ ->
              Error
                (Error.make ~non_retryable:true ~category:`Defect
                   ~message:
                     (Printf.sprintf "activity heartbeat callback raised: %s"
                        (Printexc.to_string exception_))
                   ())
        in
        match Atomic.get context.cancellation with
        | Some cancellation -> Error (cancelled_error cancellation)
        | None -> submitted)

(** Reads the adapter-owned cell; see [cancellation_signal]. *)
let cancellation context = Atomic.get context.cancellation

(** Calls the worker's stop probe, which reads Domain-safe atomics. *)
let worker_shutting_down context = context.worker_shutting_down ()

(** Returns a private copy of the previous attempt's details so callers cannot
    mutate retained [bytes] values. The field is immutable and the copy only
    reads the context's own buffers, so no lock is needed; it remains readable
    after invalidation. *)
let details context = copy_payloads context.details

(** Timeout values are immutable and need no lock to read. *)
let heartbeat_timeout context = context.heartbeat_timeout

(** Task metadata is immutable strings, integers, and timestamps, so it is
    returned without copying or locking. *)
let info context = context.info

(** Ends the context lifetime after waiting for a callback already in flight.
    This is the use-after-completion guard for the opaque task token captured by
    [heartbeat_fn]. *)
let invalidate context =
  Mutex.lock context.mutex;
  context.active <- false;
  Mutex.unlock context.mutex
