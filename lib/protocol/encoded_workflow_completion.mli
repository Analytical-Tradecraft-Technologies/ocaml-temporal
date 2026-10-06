(** A workflow completion that has already passed the canonical semantic
    encoder exactly once.

    The native worker validates every completion before it may retire a
    leased activation, and the supervisor must hand Rust the canonical JSON
    bytes of that same completion. Carrying the encoder's output from the first
    step to the second removes a redundant full encode (issue #846) and also
    makes the submitted bytes a fixed snapshot: later mutation of a payload
    buffer owned by workflow code cannot change what a retried submission
    sends.

    The type is abstract so a value can only be produced by {!encode}. Holding
    one is therefore proof that the document was accepted by
    [Workflow_protocol.encode_completion], with every check that encoder
    applies; no caller can submit unchecked JSON through it. *)

type t
(** Immutable canonical completion JSON. The value owns its string; it holds
    no reference to the typed completion or to any of its mutable payload
    buffers. *)

val encode :
  Workflow_protocol.completion -> (t, Workflow_protocol.error) result
(** Runs [Workflow_protocol.encode_completion] once and retains its output.
    The bytes are exactly those that encoder returns, so the wire format is
    unchanged. *)

val to_bytes : t -> bytes
(** Returns a fresh mutable copy of the canonical document for a native call.
    The caller owns the copy; mutating it never affects [t], so a retained
    value can be resubmitted byte for byte. *)

val to_string : t -> string
(** Returns the immutable canonical document, for tests and diagnostics that
    must compare exact submitted bytes. *)
