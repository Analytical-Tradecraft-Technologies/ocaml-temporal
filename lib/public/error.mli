(** Structured SDK and Temporal failures.

    Expected operational failures are returned as [(_, Error.t) result]
    values rather than raised. An error carries a broad {!type-category} for
    pattern matching, a diagnostic message, retry semantics, and optional raw
    Temporal details. *)

(** Identifies the part of the SDK or Temporal operation that failed. These
    broad categories are intended for pattern matching and metrics; [message]
    provides the more specific diagnostic. *)
type category =
  [ `Activity
  | `Bridge
  | `Cancelled
  | `Child_workflow
  | `Codec
  | `Defect
  | `Nexus
  | `Terminated
  | `Timeout
  | `Update
  | `Workflow ]

(** The information application code can inspect about an SDK error.
    [non_retryable] records whether Temporal should avoid retrying the failure.
    [details] contains any additional raw Temporal payloads supplied with it.
    [error_type] is the application-defined failure type, Temporal's
    [ApplicationFailureInfo.type]; see {!make} and {!val-error_type}. *)
type view = {
  category : category;
  message : string;
  non_retryable : bool;
  details : Payload.t list;
  error_type : string option;
}

(** Which Temporal timeout expired (#1143). [`Start_to_close],
    [`Schedule_to_start], [`Schedule_to_close] and [`Heartbeat] are the
    activity timeouts of the same names; a child workflow's run or execution
    timeout is reported by Temporal as [`Start_to_close]. [`Unspecified] is a
    timeout whose type Temporal did not record. *)
type timeout_type =
  [ `Start_to_close | `Schedule_to_start | `Schedule_to_close | `Heartbeat | `Unspecified ]

(** Where a failure received from Temporal originated (#1143), found by
    walking the failure chain from the outside inward past the wrapper layers
    that only say which operation failed (activity, child workflow and Nexus
    operation). The first other layer decides:

    - [`Application]: code raised it, in an activity, a child workflow or a
      workflow, in any SDK; {!val-error_type} names it.
    - [`Timeout t]: a Temporal timeout of type [t] expired. The activity or
      child [category] is kept, so code that matches [`Activity] still sees
      the failure, and [details] holds no heartbeat payloads; see
      {!last_heartbeat_details}.
    - [`Server]: the Temporal server failed the operation itself, for
      example because a result exceeded a namespace limit.
    - [`Cancelled] and [`Terminated]: the operation was cancelled or the
      run terminated.
    - [`Other]: any other layer, such as a workflow reset, a Nexus handler
      error, or a failure kind a newer server introduced.

    The origin is never transmitted: an error an activity or workflow returns
    is sent to Temporal with only its category, message, retryability,
    details and type. *)
type origin =
  [ `Application | `Timeout of timeout_type | `Server | `Cancelled | `Terminated | `Other ]

(** A failure returned by SDK operations through [result]. Expected failures,
    such as an activity error, cancellation, timeout, or invalid payload, are
    represented by this type rather than exceptions. *)
type t

(** Constructs a structured error at an application-facing boundary. Most
    callers should prefer the more specific [codec] or [defect] helpers, but
    this constructor is useful to adapters and custom activity implementations
    that need to preserve a Temporal category.

    [error_type] names the business failure, for example ["InvalidInput"]. It
    is sent as Temporal's [ApplicationFailureInfo.type] when an activity or
    workflow fails with this error, so it is what a retry policy's
    [non_retryable_error_types] list is matched against and what callers
    written in other Temporal SDKs see. When omitted, the lowercase category
    name ({!kind}) is sent instead. An empty string is treated as omitted.
    Raises [Invalid_argument] if [error_type] is not valid UTF-8, contains an
    ASCII control character, or is longer than 65536 characters once
    JSON-escaped (a double quote or backslash counts twice); such a value
    cannot be transmitted and indicates a programming error.

    [origin] and [last_heartbeat_details] (default [None] and empty) are what
    {!val-origin} and {!last_heartbeat_details} report. The SDK sets them on
    failures it receives from Temporal; application code may set them to
    exercise a branch on them in tests. They are never transmitted. *)
val make :
  ?non_retryable:bool ->
  ?error_type:string ->
  ?details:Payload.t list ->
  ?origin:origin ->
  ?last_heartbeat_details:Payload.t list ->
  category:category ->
  message:string ->
  unit ->
  t

(** Returns all publicly inspectable fields of an error. Detail payloads are
    copied, so mutating bytes in the returned view cannot change the error or a
    later view. *)
val view : t -> view

(** Returns the lowercase category name, such as ["activity"] or ["codec"]. *)
val kind : t -> string

(** Returns the human-readable explanation of the failure. *)
val message : t -> string

(** Returns the application failure type, if any. For an error received from
    Temporal (an activity, child workflow, or workflow result) this is the type
    of the first application failure found walking the failure chain from the
    outermost layer inward (so an application failure nested under an
    activity or child-workflow wrapper is found, while an application failure
    wrapping another application failure reports its own type), as set by the
    code that raised it in any SDK; an OCaml error created without
    [~error_type] arrives with its category name. [None] means the failure had
    no application layer or its type was empty. *)
val error_type : t -> string option

(** Returns where a failure received from Temporal originated, so workflow
    code can tell an activity or child timeout from a business failure or a
    server failure without reading {!val-message}:

    {[
      match Temporal.Activity.execute charge ~input with
      | Ok receipt -> Ok receipt
      | Error error -> (
          match Temporal.Error.origin error with
          | Some (`Timeout _) -> compensate ()
          | Some `Application | Some `Server | Some `Cancelled
          | Some `Terminated | Some `Other | None ->
              Error error)
    ]}

    [None] means the error was not received from Temporal: the SDK or
    application created it, unless it passed [~origin] to {!make} (useful
    for testing such branches). *)
val origin : t -> origin option

(** Returns [Some t] exactly when {!val-origin} is [Some (`Timeout t)]. *)
val timeout_type : t -> timeout_type option

(** Returns the heartbeat details Temporal recorded with a timeout or a
    workflow reset in the failure chain, in outer-to-inner order. They are
    the last details the activity heartbeated, kept apart from
    [(view error).details] so that decoding [details] with an activity's
    error-details codec never reads a heartbeat payload. Empty when there
    are none. Payloads are copied as for {!val-view}. *)
val last_heartbeat_details : t -> Payload.t list

(** Creates an error for a value that a custom codec could not encode or
    decode. *)
val codec : message:string -> t

(** Creates an error for an SDK bug or a violation of an API requirement.
    Its diagnostic [non_retryable] field is true. When propagated from workflow
    code, this fails the workflow task and leaves the execution open. Use
    [make ~category:`Workflow] for a deliberate terminal business failure. *)
val defect : message:string -> t
