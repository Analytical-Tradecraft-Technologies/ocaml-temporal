(** An activity completion that has already passed the canonical semantic
    encoder exactly once.

    The native activity adapter validates every completion before it may be
    retained as a pending lease or submitted, and the supervisor must hand Rust
    the canonical JSON bytes of that same completion. Carrying the encoder's
    output from the first step to the second removes a redundant full encode
    (issue #995), mirroring {!Encoded_workflow_completion} (issue #846). It also
    makes a retained completion a fixed snapshot: a retryable transport failure
    resubmits the identical string, and later mutation of a payload or token
    buffer cannot change what a retry sends.

    The type is abstract so a value can only be produced by {!encode}. Holding
    one is therefore proof that the document was accepted by
    [Activity_protocol.encode_completion], with every check that encoder
    applies; no caller can submit unchecked JSON through it. The same value
    serves both the worker completion ([Complete_activity]) and the
    namespace-bound asynchronous completion ([Complete_async_activity]); both
    native calls accept the same activity-completion document. *)

type t
(** Immutable canonical completion JSON. The value owns its string; it holds
    no reference to the typed completion or to any of its mutable buffers. *)

val encode :
  Activity_protocol.completion -> (t, Activity_protocol.error) result
(** Runs [Activity_protocol.encode_completion] once and retains its output.
    The bytes are exactly those that encoder returns, so the wire format is
    unchanged. *)

val to_bytes : t -> bytes
(** Returns a fresh mutable copy of the canonical document for a native call.
    The caller owns the copy; mutating it never affects [t], so a retained
    value can be resubmitted byte for byte. *)

val to_string : t -> string
(** Returns the immutable canonical document without copying, for test
    sources and diagnostics that must compare or decode the exact submitted
    bytes. *)
