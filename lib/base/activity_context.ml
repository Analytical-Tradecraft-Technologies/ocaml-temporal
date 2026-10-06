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
}

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
let make ~info ~heartbeat ~details ~heartbeat_timeout =
  {
    mutex = Mutex.create ();
    active = true;
    details = copy_payloads details;
    heartbeat_timeout;
    heartbeat_fn = heartbeat;
    info;
  }

(** Constructs a context with no Core task metadata. *)
let create ~heartbeat ~details ~heartbeat_timeout =
  make ~info:None ~heartbeat ~details ~heartbeat_timeout

(** Constructs a native-task context that also retains its Core metadata. *)
let create_with_info ~info ~heartbeat ~details ~heartbeat_timeout =
  make ~info:(Some info) ~heartbeat ~details ~heartbeat_timeout

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
    for the whole attempt. *)
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
        try context.heartbeat_fn details with
        | exception_ ->
            Error
              (Error.make ~non_retryable:true ~category:`Defect
                 ~message:
                   (Printf.sprintf "activity heartbeat callback raised: %s"
                      (Printexc.to_string exception_))
                 ()))

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
