(** An async-activity heartbeat that has already passed the canonical
    semantic encoder exactly once (#1025).

    It plays the role {!Encoded_activity_heartbeat} plays for a running
    activity, for a heartbeat sent through an asynchronous activity handle.
    The document also carries the attempt's heartbeat timeout, from which the
    bridge derives the interval at which it throttles the handle's
    heartbeats. A separate abstract type keeps the two documents apart: the
    submitted async heartbeat call accepts only this one, and the worker
    heartbeat only the other. *)

type t
(** Immutable canonical async-heartbeat JSON. Like
    {!Encoded_activity_heartbeat.t}, the value owns its string and retains
    no reference to the typed heartbeat or its mutable buffers. *)

val encode :
  Activity_protocol.async_heartbeat -> (t, Activity_protocol.error) result
(** Runs [Activity_protocol.encode_async_heartbeat] once and retains its
    output unchanged. *)

val to_bytes : t -> bytes
(** Returns a fresh mutable copy of the canonical document for a native
    call. Mutating it never affects [t]. *)

val to_string : t -> string
(** Returns the immutable canonical document without copying, for tests and
    diagnostics that compare or decode the exact submitted bytes. *)
