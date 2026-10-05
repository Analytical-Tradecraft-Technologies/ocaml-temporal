(** Typed workflow update definitions.

    An update is a named, durable request that validates input, changes
    workflow state, and returns a typed result. Validation is run before the
    update callback and must not mutate state or emit commands. Native workers
    use the same dispatcher after the Rust/Core bridge decodes a [DoUpdate]
    activation. When invoked by a native workflow worker, a handler may suspend
    on a workflow [Future]; the runtime acknowledges it before parking and
    completes it when resumed. *)

(** A validated update name paired with input and output codecs. *)
type ('input, 'output) definition

(** Public name for an update definition. *)
type ('input, 'output) t = ('input, 'output) definition

(** Creates an update definition after validating its stable name. *)
val define :
  name:string -> input:'input Codec.t -> output:'output Codec.t ->
  ('input, 'output) t

(** Returns the stable update name. *)
val name : ('input, 'output) t -> string

(** Returns the codec used to decode update arguments. *)
val input : ('input, 'output) t -> 'input Codec.t

(** Returns the codec used to encode successful update results. *)
val output : ('input, 'output) t -> 'output Codec.t

(** An existentially packaged update validator and handler. *)
module Handler : sig
  (** A handler retains input/output codecs with both callbacks. *)
  type t

  (** Builds an update handler. [validator] runs first when supplied. A
      validator may reject with a typed [Error.t] of any category, including
      [Error.defect]; an exception it raises also rejects the update. In both
      cases [run] is not called, no update-side state change can occur, and
      the workflow task is unaffected. *)
  val make :
    ?validator:('input -> (unit, Error.t) result) ->
    ('input, 'output) definition ->
    ('input -> ('output, Error.t) result) ->
    t

  (** Convenience alias for [make]. *)
  val handle :
    ?validator:('input -> (unit, Error.t) result) ->
    ('input, 'output) definition ->
    ('input -> ('output, Error.t) result) ->
    t

  (** Returns the update name used to index an interaction registry. *)
  val name : t -> string

  (** Decodes input, optionally validates it, runs the update, and encodes the
      result. Native replay passes [~run_validator:false] because validation
      is a live-request check and must not run against historical input. When
      supplied, [on_validated] runs once after successful validation and before
      the handler, allowing the native runtime to acknowledge a handler that
      may suspend. Validators execute in a read-only mode: deterministic
      helpers such as [Temporal.Workflow.random_int] return a typed defect and
      cannot advance replay-sensitive workflow state. Application code normally
      leaves this optional hook unset. *)
  val dispatch :
    ?run_validator:bool ->
    ?on_validated:(unit -> unit) ->
    t -> Payload.t -> (Payload.t, Error.t) result

  (** Dispatches Temporal's repeated update input list. This is the worker
      adapter boundary. Zero payloads, which the Temporal CLI, Web UI, other
      SDKs, and this SDK's client send for a no-argument update, are decoded
      as the canonical [binary/null] unit payload; one payload is passed to
      [dispatch] with the same optional arguments; more than one is a
      non-retryable workflow error returned before decoding, validation, or
      [on_validated]. *)
  val dispatch_payloads :
    ?run_validator:bool ->
    ?on_validated:(unit -> unit) ->
    t -> Payload.t list -> (Payload.t, Error.t) result
end
