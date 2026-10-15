(** An activity heartbeat that has already passed the canonical semantic
    encoder exactly once.

    The native activity adapter validates every heartbeat before it may cross
    the supervisor mailbox, and the supervisor must hand Rust the canonical
    JSON bytes of that same heartbeat. Carrying the encoder's output from the
    first step to the second removes a redundant full encode (issue #1001),
    mirroring {!Encoded_activity_completion} (issue #995).

    The type is abstract so a value can only be produced by {!encode}. Holding
    one is therefore proof that the document was accepted by
    [Activity_protocol.encode_heartbeat], with every check that encoder
    applies; no caller can submit unchecked JSON through it. The same value
    serves both the worker heartbeat ([Record_activity_heartbeat]) and the
    synchronous namespace-bound asynchronous heartbeat
    ([Record_async_activity_heartbeat]); both native calls accept the same
    activity-heartbeat document. The worker's throttled async heartbeat call
    takes {!Encoded_async_activity_heartbeat} instead (#1025). *)

type t
(** Immutable canonical heartbeat JSON. The value owns its string; it holds
    no reference to the typed heartbeat or to any of its mutable buffers, so
    a later change to a token or detail buffer cannot alter what is sent. *)

val encode : Activity_protocol.heartbeat -> (t, Activity_protocol.error) result
(** Runs [Activity_protocol.encode_heartbeat] once and retains its output. The
    bytes are exactly those that encoder returns, so the wire format is
    unchanged. *)

val to_bytes : t -> bytes
(** Returns a fresh mutable copy of the canonical document for a native call.
    The caller owns the copy; mutating it never affects [t]. *)

val to_string : t -> string
(** Returns the immutable canonical document without copying, for test
    sources and diagnostics that must compare or decode the exact submitted
    bytes. *)
