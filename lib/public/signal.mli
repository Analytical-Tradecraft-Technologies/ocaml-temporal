(** Typed workflow signal definitions.

    A signal is a fire-and-forget message delivered to a running workflow. The
    definition owns the payload codec and the stable Temporal name; a handler
    can therefore decode the same bytes delivered by the local dispatcher or
    the native activation bridge. This module provides the typed definition
    and deterministic in-memory handler path. Native workflow signal delivery
    is available when the handler is attached with [Temporal.Worker.workflow].
    Queries and updates are separate message kinds with their own definitions
    and handlers in [Temporal.Query] and [Temporal.Update]. *)

(** A validated signal name paired with the type of its input value. *)
type 'input definition

(** Public name for a signal definition. The separate alias keeps the nested
    handler signature readable without exposing representation fields. *)
type 'input t = 'input definition

(** Creates a signal definition. [name] must be non-empty, valid UTF-8,
    NUL-free, and no longer than the bridge's 65,536-byte identifier limit.
    Invalid names are programmer configuration defects and raise
    [Invalid_argument], just like workflow and activity definitions. *)
val define : name:string -> input:'input Codec.t -> 'input t

(** Returns the stable name used when registering and sending the signal. *)
val name : 'input t -> string

(** Returns the codec used to encode signal arguments. *)
val input : 'input t -> 'input Codec.t

(** A signal handler closes over ordinary workflow state and applies one
    decoded signal value. The callback's result is typed so an expected
    application failure does not escape as an exception. *)
module Handler : sig
  (** An existentially packaged handler whose input type remains paired with
      its definition and callback. *)
  type t

  (** Builds a handler for [signal]. Signal callbacks use the same direct style
      as workflow functions. The local dispatcher invokes them synchronously;
      native worker delivery invokes them on the owning workflow scheduler. *)
  val make : 'input definition -> ('input -> (unit, Error.t) result) -> t

  (** Convenience alias for [make] that reads naturally at registration sites. *)
  val handle : 'input definition -> ('input -> (unit, Error.t) result) -> t

  (** Returns the name used to index this handler in an interaction registry. *)
  val name : t -> string

  (** Decodes one payload and invokes the callback. This is primarily used by
      the package's deterministic dispatcher; exposing only this typed
      boundary keeps codec and callback ownership inside the handler. *)
  val dispatch : t -> Payload.t -> (unit, Error.t) result

  (** Dispatches Temporal's repeated signal payload list. This is the worker
      adapter boundary. Zero payloads, which the Temporal CLI, Web UI, and
      other SDKs send for a no-argument signal, are decoded as the canonical
      [binary/null] unit payload; one payload is passed to [dispatch]; more
      than one is a non-retryable [`Codec] error and the callback is not
      invoked.

      Signal delivery is fail-closed in v1. When a native worker receives a
      signal it cannot apply (no handler registered under the signal's name,
      a payload the codec cannot decode, or more than one payload) it fails
      the current workflow task instead of closing the run. The signal stays
      in history, so every replay fails the same way; the run stays open and
      makes no progress until a worker with a matching handler and codec is
      deployed, or an operator resets or terminates it. A typed error that the
      callback itself returns keeps its own classification: a [`Workflow]
      error closes the run as Failed. *)
  val dispatch_payloads : t -> Payload.t list -> (unit, Error.t) result
end
