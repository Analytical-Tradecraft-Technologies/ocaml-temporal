(** The complete incoming event delivered to a registered workflow signal
    handler. Payload order, sender identity, and headers are retained exactly
    as validated by the native activation boundary. *)
type signal = {
  input : Temporal_base.Codec.payload list;
  identity : string;
  headers : (string * Temporal_base.Codec.payload) list;
}

(** An internal signal handler owned by one workflow execution. The callback is
    invoked only by that execution's scheduler; Rust never receives an OCaml
    closure or calls one from a native thread. *)
type signal_handler

(** Query input retained by the private dispatch layer. Arguments and headers
    remain available for future typed-input APIs even though the public API is
    currently output-only. *)
type query = {
  arguments : Temporal_base.Codec.payload list;
  headers : (string * Temporal_base.Codec.payload) list;
}

(** A synchronous, non-suspending query callback owned by one execution. *)
type query_handler

(** The complete request delivered to an update handler. The repeated input
    list and metadata are retained so unsupported public arity is rejected
    explicitly rather than silently truncating Core data. *)
type update = {
  id : string;
  protocol_instance_id : string;
  name : string;
  input : Temporal_base.Codec.payload list;
  headers : (string * Temporal_base.Codec.payload) list;
  identity : string;
  update_id : string;
}

(** A private update callback. [run_validator] is false during replay and tells
    the public adapter to skip its validator. [on_validated] is called before
    the callback so a suspended update can be acknowledged in its first
    activation. *)
type update_handler

(** Creates a handler for one validated signal name. The callback receives the
    complete runtime signal and returns a typed workflow error when delivery
    should fail the execution. *)
val make_signal_handler :
  name:string ->
  dispatch:(signal -> (unit, Temporal_base.Error.t) result) ->
  signal_handler

(** Returns the stable Temporal name used to look up a handler. *)
val signal_handler_name : signal_handler -> string

(** Builds a query handler invoked inline on the execution owner Domain. *)
val make_query_handler :
  name:string ->
  dispatch:(query -> (Temporal_base.Codec.payload, Temporal_base.Error.t) result) ->
  query_handler

(** Returns the stable query name used by registration validation. *)
val query_handler_name : query_handler -> string

(** Builds an update handler invoked on the execution owner Domain. *)
val make_update_handler :
  name:string ->
  dispatch:
    (run_validator:bool -> on_validated:(unit -> unit) -> update ->
     (Temporal_base.Codec.payload, Temporal_base.Error.t) result) ->
  update_handler

(** Returns the stable update name used by registration validation. *)
val update_handler_name : update_handler -> string

(** The in-memory state for one running workflow. It contains the workflow's
    scheduler and the activities and timers whose results are still pending. *)
type ('input, 'output) t

(** Creates the in-memory state but does not call the workflow function yet.
    The function starts only after [activate] receives [Start_workflow].
    [task_queue] is captured in the execution context and is used by activity
    commands when a workflow does not supply an explicit queue. *)
val start :
  ?task_queue:string ->
  ?randomness_seed:string ->
  ?signal_handlers:signal_handler list ->
  ?query_handlers:query_handler list ->
  ?update_handlers:update_handler list ->
  ( 'input,
    'output,
    'input -> ('output, Temporal_base.Error.t) result )
  Temporal_base.Definition.t ->
  'input ->
  ('input, 'output) t

(** Applies every job in list order, runs workflow fibers until none can make
    progress, and returns newly produced commands in their creation order. An
    execution removed from the cache ignores later calls. *)
val activate : ('input, 'output) t -> Activation.job list -> Activation.command list

(** Installs the run's historical start snapshot before its first activation. *)
val set_start_metadata :
  ('input, 'output) t -> Workflow_context_store.start_metadata option -> unit

(** Installs the timestamp carried by the activation currently being dispatched.
    The native protocol adapter calls this before [activate] so deterministic
    workflow clock reads observe the correct replay value. *)
val set_activation_timestamp :
  ('input, 'output) t -> Temporal_protocol.Workflow_protocol.timestamp option -> unit

(** Installs the deployment/build identity attached to the activation. *)
val set_activation_deployment_version :
  ('input, 'output) t -> (string * string) option -> unit

(** Installs Core's replay status for the activation currently being
    dispatched. The native adapter calls this before patch notifications and
    workflow fibers are processed. *)
val set_activation_is_replaying : ('input, 'output) t -> bool -> unit

(** Releases paused fibers and pending operation tables for this execution.
    Idempotent. Call when removing a run from a worker registry if a terminal
    or eviction path has not already shut the execution down. *)
val shutdown : ('input, 'output) t -> unit
