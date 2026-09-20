(** The type of an OCaml function that implements a workflow. It receives a
    decoded input and returns either the workflow output or a structured error.
    The function must obey Temporal's workflow determinism rules. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

(** The deployment identity Core selected for the workflow task currently
    running this workflow. It is task-local because a versioned worker may
    route later tasks of the same run to another build. *)
type deployment_version = { deployment_name : string; build_id : string }

(** A description of a workflow and the OCaml types it accepts and returns. A
    definition stores the Temporal workflow type name, its codecs, and
    optionally the OCaml function that implements it. *)
type ('input, 'output) t

(** Creates a workflow definition implemented by this OCaml worker. [name] is
    validated immediately; it must be non-empty, valid UTF-8, NUL-free, and no
    more than 65,536 bytes because it crosses the native protocol into
    Temporal history. Violations raise [Invalid_argument] as construction
    defects. The implementation must remain deterministic and return expected
    failures as [Error.t] values. *)
val define :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) implementation ->
  ('input, 'output) t

(** Creates a typed reference to a workflow implemented by another worker. [name]
    has the same non-empty, valid UTF-8, NUL-free, 65,536-byte contract as
    [define]. Use the reference with [Child_workflow.start] or
    [Child_workflow.execute] when invoking the workflow as a child. It has no
    local implementation and cannot be registered as executable worker code. *)
val remote :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) t

(** Returns the workflow type name used for registration and commands. *)
val name : ('input, 'output) t -> string

(** Returns the codec used to decode workflow inputs. This accessor is useful
    to worker adapters and generic workflow tooling; the definition itself
    remains opaque and cannot be constructed by record syntax. *)
val input : ('input, 'output) t -> 'input Codec.t

(** Returns the codec used to encode successful workflow outputs. *)
val output : ('input, 'output) t -> 'output Codec.t

(** Returns executable code for a local definition, or [None] for a remote
    reference. The callback still returns typed [Error.t] values rather than
    raising expected workflow failures. *)
val implementation :
  ('input, 'output) t -> ('input, 'output) implementation option

(** Starts a durable Temporal timer and returns immediately. Starting several
    timers before awaiting one emits them in call order. A zero duration returns
    a ready future without recording a timer. Outside workflow execution the
    returned future is ready with a typed defect rather than touching global
    time or creating an unowned timer. *)
val start_sleep : Duration.t -> (unit, Error.t) Future.t

(** Starts a durable Temporal timer and waits until it fires. This is equivalent
    to [Future.await (start_sleep duration)]. A zero duration returns
    immediately without recording a timer; outside workflow execution it
    returns a typed defect. *)
val sleep : Duration.t -> (unit, Error.t) result

(** Sends a typed signal to another workflow execution. The operation is
    represented as a future because Temporal acknowledges delivery in a later
    activation; [run_id] may be empty when the server should resolve the
    current run for the workflow ID. *)
val signal_external_workflow :
  workflow_id:string ->
  run_id:string ->
  signal:'input Signal.t ->
  input:'input ->
  (unit, Error.t) Future.t

(** Requests cancellation of another workflow execution and returns a future
    resolved by Temporal's acknowledgement or structured failure. *)
val cancel_external_workflow :
  workflow_id:string ->
  run_id:string ->
  reason:string ->
  (unit, Error.t) Future.t

(** Returns the exact timestamp attached to the activation currently executing
    this workflow. Temporal supplies the value for both live execution and
    replay, so the result is deterministic. Calling this outside workflow
    execution, or while processing an activation without a timestamp, returns
    a typed defect rather than reading the host wall clock. *)
val now : unit -> (Time.t, Error.t) result

(** Returns a deterministic pseudo-random integer in [0, bound).  The stream
    is seeded by Temporal for the workflow run and replayed from the same
    initialization metadata, so the result is stable for an identical call
    sequence.  [bound] must be positive; invalid bounds and calls outside a
    workflow return a typed defect. *)
val random_int : bound:int -> (int, Error.t) result

(** Returns the deployment/build identity attached to the current workflow
    task. [None] means that the worker is unversioned, the activation is
    synthetic, or the call is outside workflow execution. The value is
    Temporal metadata and never reads process or wall-clock state. *)
val current_deployment_version : unit -> deployment_version option

(** A snapshot of metadata recorded when this workflow run started. [None]
    means Core omitted that protobuf field; [Some []] preserves an explicitly
    empty map. Search attributes are their initial values, before any upsert.
    Expiration is a server-enforced deadline across the execution chain, not
    a timer the workflow runtime should schedule. *)
type start_metadata = {
  memo : (string * Payload.t) list option;
  search_attributes : (string * Payload.t) list option;
  execution_expiration_time : Time.t option;
}

(** Returns this run's deterministic start snapshot with independently owned
    payload bytes on every call. Replay reconstructs it from the same history;
    a continued run receives its own inherited metadata. Calling outside a
    workflow, or in a synthetic execution lacking metadata, returns a defect. *)
val start_metadata : unit -> (start_metadata, Error.t) result

(** Returns whether workflow code should take the new branch identified by
    [id]. On a new execution the first call returns [true] and records a patch
    marker; replay returns [true] only when Core reports that marker, otherwise
    it returns [false]. The decision is retained per workflow run and every
    call emits Core's idempotent marker command.

    Patch IDs must be non-empty, valid UTF-8, NUL-free, and at most 65,536
    bytes. Invalid IDs or calls outside workflow execution raise
    [Invalid_argument] as programmer misuse. IDs are durable history keys:
    never reuse an ID for a different behavioral change. *)
val patched : id:string -> bool

(** Records that the behavioral change identified by [id] is being phased out.
    A transition release replaces its [patched] call with [deprecate_patch] at
    the same logical point; it must not call both operations for one ID during a
    workflow execution. This function returns [unit] because deprecation is a
    durable lifecycle marker, not a branch decision.

    Patch IDs have the same validation and immutable-history requirements as
    [patched]. Invalid IDs, calls outside workflow execution, calls after that
    execution ends, or mixed patch modes raise [Invalid_argument] as programmer
    misuse. Replacing [patched] is safe only after marker-free executions that
    could take the old branch can no longer replay across that point. Removing
    the deprecation call is a later gate, safe only after incompatible
    non-deprecated-marker histories have drained or been otherwise accounted
    for. *)
val deprecate_patch : id:string -> unit

(** Merges encoded values into Temporal's indexed search attributes. The
    update is deterministic and becomes visible only after the workflow task
    is accepted. Duplicate, empty, malformed, or oversized keys raise
    [Invalid_argument] as programmer misuse; payload ownership is copied. *)
val upsert_search_attributes : (string * Payload.t) list -> unit

(** Ends the current run and starts a new run of [definition] with [input].
    This operation never returns to the calling workflow fiber. It is
    deterministic: the input is encoded through the definition's codec before
    the successor command is emitted. A codec failure fails the current run
    with a typed error; calling it outside workflow execution is programmer
    misuse and raises [Invalid_argument]. *)
val continue_as_new : ('input, 'output) t -> 'input -> 'value
