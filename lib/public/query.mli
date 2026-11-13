(** Typed workflow query definitions.

    A query reads workflow state and returns a value without changing durable
    history. Query handlers are required by contract to be read-only and
    non-suspending. A native worker invokes them outside an active workflow
    scheduler, in the read-only mode it also uses for update validators: an
    SDK operation that would change replay-sensitive state, emit a command,
    or suspend is rejected before it changes anything, with a typed defect
    where the operation returns a result or future and [Invalid_argument]
    where its contract raises for misuse (see {!Update.Handler.dispatch} for
    the list). A raised rejection fails only this query. The mode guards SDK
    state only: OCaml references, objects, I/O and foreign calls a handler
    reaches remain the application's responsibility. *)

(** A validated query name paired with the type of its result. This type keeps
    the original output-only API source-compatible. Use [typed] and
    [define_with_input] when the query should receive one typed argument. *)
type 'output definition

(** Public name for a query definition. *)
type 'output t = 'output definition

(** Creates a query definition after validating its stable name. *)
val define : name:string -> output:'output Codec.t -> 'output t

(** A query definition whose handler receives exactly one decoded input value.
    The native protocol supports ordered payload lists, while this public
    convenience deliberately fixes the arity at one so malformed requests are
    reported rather than silently ignored. *)
type ('input, 'output) typed

(** Creates a typed-input query definition. *)
val define_with_input :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) typed

(** Returns the stable name used to route a query request. *)
val name : 'output t -> string

(** Returns the stable name of a typed-input query. *)
val name_with_input : ('input, 'output) typed -> string

(** Returns the codec used to encode and decode query results. *)
val output : 'output t -> 'output Codec.t

(** Returns the input codec of a typed query. *)
val input : ('input, 'output) typed -> 'input Codec.t

(** Returns the result codec of a typed query. *)
val output_with_input : ('input, 'output) typed -> 'output Codec.t

(** A query handler closes over read-only workflow state and computes one typed
    result. The callback must not mutate workflow state, schedule commands, or
    wait for a future; the SDK rejects such operations, as described above. *)
module Handler : sig
  (** An existentially packaged query handler. *)
  type t

  (** Builds a read-only handler for [query]. *)
  val make : 'output definition -> (unit -> ('output, Error.t) result) -> t

  (** Convenience alias for [make]. *)
  val handle : 'output definition -> (unit -> ('output, Error.t) result) -> t

  (** Builds a handler for a typed-input query. *)
  val make_with_input :
    ('input, 'output) typed ->
    ('input -> ('output, Error.t) result) ->
    t

  (** Convenience alias for [make_with_input]. *)
  val handle_with_input :
    ('input, 'output) typed ->
    ('input -> ('output, Error.t) result) ->
    t

  (** Returns the query name used to index an interaction registry. *)
  val name : t -> string

  (** Invokes a query and encodes its typed result. *)
  val dispatch : t -> (Payload.t, Error.t) result

  (** Invokes a query with the already decoded payload arguments. This is the
      worker adapter boundary; callers should prefer [dispatch] or a typed
      handler constructor. *)
  val dispatch_payloads :
    t -> Payload.t list -> (Payload.t, Error.t) result
end
