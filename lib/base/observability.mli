(** Shared logging vocabulary for the OCaml SDK. This module never installs a
    reporter or changes source levels; those remain application policy. *)

(** Stable independently-filterable SDK log sources. *)
module Source : sig
  (** SDK/runtime initialization and shutdown lifecycle events. *)
  val lifecycle : Logs.src

  (** Calls through the private OCaml/C/Rust bridge. *)
  val bridge : Logs.src

  (** Deterministic workflow execution and activation processing. *)
  val workflow : Logs.src
end

(** Typed definitions for bounded structural metadata attached to SDK events.
    Reporters may render, index, or discard these tags. *)
module Tag : sig
  (** Stable operation identifier such as [runtime_create] or [activate]. *)
  val operation : string Logs.Tag.def

  (** Finite elapsed wall-clock milliseconds. Invalid values become zero. *)
  val duration_ms : float Logs.Tag.def

  (** Registered Temporal workflow type, truncated to a bounded tag length. *)
  val workflow_type : string Logs.Tag.def

  (** Non-negative number of jobs supplied in one activation. *)
  val job_count : int Logs.Tag.def

  (** Non-negative number of commands produced in one activation. *)
  val command_count : int Logs.Tag.def

  (** Stable lowercase bridge status name; never the raw bridge diagnostic. *)
  val bridge_status : string Logs.Tag.def

  (** Stable lowercase [Temporal.Error] category. *)
  val error_kind : string Logs.Tag.def
end

(** [tags ~operation ()] constructs metadata shared by current SDK boundaries.
    String values are bounded before they reach an application reporter without
    splitting valid UTF-8 characters. Negative counts and negative, NaN, or
    infinite durations become zero. *)
val tags :
  operation:string ->
  ?duration_ms:float ->
  ?workflow_type:string ->
  ?job_count:int ->
  ?command_count:int ->
  ?bridge_status:string ->
  ?error_kind:string ->
  unit ->
  Logs.Tag.set

(** [measure_ms action] returns [action ()] and its non-negative elapsed time
    in milliseconds. It is diagnostic only and must never affect workflow
    commands or other deterministic decisions. *)
val measure_ms : (unit -> 'value) -> 'value * float

(** [report ~src level ~tags message] submits one bounded constant SDK message.
    Any exception raised by the application-selected reporter or formatter is
    contained so observability cannot alter SDK result or exception semantics.
    Callers must not pass payload contents, credentials, or arbitrary remote
    diagnostics as [message]. *)
val report :
  src:Logs.src -> Logs.level -> tags:Logs.Tag.set -> string -> unit
