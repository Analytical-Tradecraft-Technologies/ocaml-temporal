(** Deterministic local delivery of workflow interaction handlers.

    [Interaction] is the small, scheduler-independent part of the signals,
    queries, and updates API. It keeps typed handlers in immutable registries
    and routes encoded payloads by their Temporal names. The native activation
    protocol does not call this local dispatcher; the worker adapter owns native
    delivery and uses the same typed handler boundary. Keeping this module
    usable on its own gives tests and local tooling one precise ordering and
    validation contract. *)

(** An immutable set of registered interaction handlers. *)
type t

(** Builds a dispatcher from handler lists.

    Every name must be unique within its interaction kind. The lists are
    copied into persistent maps, so later changes to a caller's list cannot
    alter dispatch. Registration errors are returned before a dispatcher is
    allocated. *)
val create :
  ?signals:Signal.Handler.t list ->
  ?queries:Query.Handler.t list ->
  ?updates:Update.Handler.t list ->
  unit -> (t, Error.t) result

(** Encodes [input], routes it to the matching signal handler, and preserves
    the callback's typed result. The handler is invoked synchronously in the
    current domain; native worker delivery uses the corresponding handler
    through the worker's scheduler-owned adapter instead. *)
val signal : t -> 'input Signal.t -> 'input -> (unit, Error.t) result

(** Routes a query by name and decodes the handler's encoded result using the
    supplied definition. Query callbacks are expected to be read-only and
    non-suspending; this module does not silently make a blocking callback
    safe. The result is decoded only after the callback has run, so an
    output-encoding mismatch with the registered handler is reported as an
    [Error] after the callback executed. *)
val query : t -> 'output Query.t -> ('output, Error.t) result

(** Routes a one-input typed query locally and decodes its typed result. *)
val query_with_input :
  t -> ('input, 'output) Query.typed -> 'input -> ('output, Error.t) result

(** Encodes an update request, runs the registered validator before its
    implementation, and decodes the typed result. A rejected validator never
    invokes the implementation. An input-encoding mismatch is rejected before
    the validator runs, but the output is decoded only after the
    implementation has returned: an output-encoding mismatch yields an [Error]
    even though the implementation's state changes have already happened. *)
val update :
  t -> ('input, 'output) Update.t -> 'input -> ('output, Error.t) result
