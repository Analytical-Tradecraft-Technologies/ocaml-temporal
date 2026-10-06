(** Translation between the checked JSON workflow protocol and the private
    deterministic OCaml execution model.

    This module deliberately does not know about the supervisor or the Rust
    bridge. It is the narrow, pure-OCaml boundary that turns one validated
    [Workflow_protocol.activation] into [Activation.job] values and turns
    commands emitted by [Execution] back into protocol commands. Values that the
    current synthetic runtime cannot represent are rejected explicitly; they are
    never replaced with guessed Temporal defaults. *)

type error
(** A semantic translation failure. The representation stays private so a caller
    can log the stable view without depending on implementation fields. *)

type error_view = { code : string; path : string; message : string }
(** Safe diagnostics for a translation failure. Payload bytes are never copied
    into this record. *)

val error_view : error -> error_view
(** Returns the stable classification, path, and diagnostic for [error]. *)

type initialization = {
  workflow_id : string;
  workflow_type : string;
  arguments : Temporal_protocol.Workflow_protocol.payload list;
  randomness_seed : string;
  attempt : int;
  context : Temporal_protocol.Workflow_protocol.initialize_context option;
}
(** Initialization data retained alongside the runtime's [Start_workflow]
    marker. [Runtime.Activation.job] intentionally carries no initialization
    arguments, so retaining this record prevents the adapter from discarding
    Core's workflow identity, attempt, seed, and context. *)

type cache_removal = {
  message : string;
  reason : Temporal_protocol.Workflow_protocol.eviction_reason;
}
(** Cache-removal details retained because the runtime job is only a marker. *)

type translated_activation = private {
  run_id : string;
  timestamp : Temporal_protocol.Workflow_protocol.timestamp option;
  is_replaying : bool;
  history_length : int64;
  metadata : Temporal_protocol.Workflow_protocol.activation_metadata option;
  initialization : initialization option;
  cancellation_reason : string option;
  cache_removal : cache_removal option;
  jobs : Activation.job list;
  source : Temporal_protocol.Workflow_protocol.activation;
}
(** Activation after translation. [jobs] has exactly the source ordering, while
    the optional fields retain protocol facts that the small runtime job algebra
    cannot yet carry. [source] is the validated protocol activation, retained
    by reference. The record is [private] so only {!translate_activation} can
    build one: holding a value proves that its activation passed translation,
    which is what lets {!activate_translated} skip a second pass. *)

type encoded_completion = {
  completion : Temporal_protocol.Workflow_protocol.completion;
  encoded : Temporal_protocol.Encoded_workflow_completion.t;
}
(** A checked completion and the canonical bytes produced by its one encoder
    pass. [completion] may alias payload buffers owned by the execution;
    [encoded] is an immutable snapshot that a worker submits and retains
    instead of encoding [completion] again. *)

val translate_activation :
  Temporal_protocol.Workflow_protocol.activation ->
  (translated_activation, error) result
(** Translates and validates one activation. The protocol's own strict encoder
    is run first, so programmatically constructed values receive the same bounds
    and closed-object checks as JSON received from Rust. Callers that go on to
    execute the activation should pass the result to {!activate_translated}
    rather than translating it again. *)

val activation_jobs :
  Temporal_protocol.Workflow_protocol.activation ->
  (Activation.job list, error) result
(** Convenience projection used by a worker loop that only needs jobs. *)

val command_to_protocol :
  Activation.command ->
  (Temporal_protocol.Workflow_protocol.completion_command, error) result
(** Converts one runtime command when every field has an exact protocol
    representation. Activity options, including retry-policy invariants, are
    range-checked and payloads are copied; child-workflow commands and their
    two-stage start/terminal resolutions are translated without fabricating
    Core fields. *)

val completion_of_commands :
  run_id:string ->
  Activation.command list ->
  (Temporal_protocol.Workflow_protocol.completion, error) result
(** Converts an ordered command batch into a checked protocol completion. The
    canonical encoder runs once as validation and its output is dropped. *)

val validate_completion_for_activation :
  Temporal_protocol.Workflow_protocol.activation ->
  Temporal_protocol.Workflow_protocol.completion ->
  (unit, error) result
(** Verifies that query results are neither missing nor attached to the wrong
    activation before a completion reaches the native supervisor. *)

val activate_translated :
  ('input, 'output) Execution.t ->
  translated_activation ->
  (encoded_completion, error) result
(** Runs one translated activation through an existing deterministic execution
    and converts its commands into a protocol completion. The completion is
    encoded exactly once; its canonical bytes are returned so a worker can
    submit them without a second encode. Query-answer and cache-eviction
    checks run against [translated.source] after encoding, and an error from
    any check means no completion was produced. The activation is not
    translated or validated again. *)

val activate :
  ('input, 'output) Execution.t ->
  Temporal_protocol.Workflow_protocol.activation ->
  (Temporal_protocol.Workflow_protocol.completion, error) result
(** [translate_activation] followed by {!activate_translated}, returning only
    the typed completion. The execution's input and definition are supplied by
    the caller because the protocol's initialization arguments are
    intentionally retained in [translated_activation] rather than guessed into
    an existential value. *)
