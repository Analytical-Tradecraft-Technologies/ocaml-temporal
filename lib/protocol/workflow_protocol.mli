(** Typed private JSON semantics for the first workflow activation and
    completion slice. Protobuf remains confined to the Rust bridge. *)

(** Binary Temporal payload with binary metadata preserved losslessly. *)
type payload = { metadata : (string * bytes) list; data : bytes }

(** Protobuf timestamp represented without floating-point conversion. *)
type timestamp = { seconds : int64; nanoseconds : int }

(** Nonnegative normalized protobuf duration. *)
type duration = { seconds : int64; nanoseconds : int }

(** Workflow/run identity for the root of a child-workflow tree. *)
type workflow_execution = { workflow_id : string; run_id : string }

(** Parent execution identity including its Temporal namespace. *)
type namespaced_workflow_execution = {
  namespace : string;
  workflow_id : string;
  run_id : string;
}

(** Exact workflow priority. [fairness_weight_bits] is the unsigned IEEE-754
    bit pattern, avoiding a lossy or non-integral JSON float representation. *)
type workflow_priority = {
  priority_key : int;
  fairness_key : string;
  fairness_weight_bits : int64;
}

(** Requester metadata attached to a workflow update. [update_id] is repeated
    in the enclosing request so the bridge can verify that Core's two identity
    fields have not diverged. *)
type update_meta = { identity : string; update_id : string }

(** Worker deployment identity attached to the current workflow task. *)
type worker_deployment_version = { deployment_name : string; build_id : string }

(** Service reason for suggesting continue-as-new. *)
type suggest_continue_as_new_reason =
  | Suggest_unspecified
  | History_size_too_large
  | Too_many_history_events
  | Too_many_updates

(** Activation metadata used by SDK language layers and normal Core traffic. *)
type activation_metadata = {
  available_internal_flags : int64 list;
  history_size_bytes : string;
  continue_as_new_suggested : bool;
  deployment_version_for_current_task : worker_deployment_version option;
  last_sdk_version : string;
  suggest_continue_as_new_reasons : suggest_continue_as_new_reason list;
  target_worker_deployment_version_changed : bool;
}

(** Retry state carried by an activity failure. *)
type retry_state =
  | Unspecified
  | In_progress
  | Non_retryable_failure
  | Timeout
  | Maximum_attempts_reached
  | Retry_policy_not_set
  | Internal_server_error
  | Cancel_requested

(** Timeout policy that Temporal reports as having elapsed.  This is distinct
    from [retry_state]'s [Timeout] wrapper state and preserves Core's
    TimeoutFailureInfo enum. *)
type timeout_type =
  | Timeout_unspecified
  | Timeout_start_to_close
  | Timeout_schedule_to_start
  | Timeout_schedule_to_close
  | Timeout_heartbeat

(** Stable JSON spelling of a timeout policy, used by diagnostics as well as
    the private protocol encoder. *)
val timeout_type_string : timeout_type -> string

(** Supported structured Temporal failure-info variants. *)
type failure_info =
  | Application of {
      type_name : string;
      non_retryable : bool;
      details : payload list;
    }
  | Canceled of { details : payload list; identity : string }
  | Activity of {
      scheduled_event_id : int64;
      started_event_id : int64;
      identity : string;
      activity_type : string;
      activity_id : string;
      retry_state : retry_state;
    }
  | Child_workflow of {
      namespace : string;
      workflow_id : string;
      run_id : string;
      workflow_type : string;
      initiated_event_id : int64;
      started_event_id : int64;
      retry_state : retry_state;
    }
  | Timeout_failure of {
      timeout_type : timeout_type;
      last_heartbeat_details : payload list;
    }

(** Recursive Temporal failure shared by activity resolution and workflow
    failure commands. *)
type failure = {
  message : string;
  source : string;
  stack_trace : string;
  encoded_attributes : payload option;
  cause : failure option;
  info : failure_info;
}

(** How Core created a successor workflow run. The value is retained even
    though the public workflow API does not currently expose continuation
    provenance. *)
type continue_as_new_initiator =
  | Continue_as_new_unspecified
  | Continue_as_new_workflow
  | Continue_as_new_retry
  | Continue_as_new_cron_schedule

(** A validated retry policy carried by a workflow execution. The backoff
    coefficient is kept as canonical unsigned IEEE-754 bits so JSON transport
    cannot round or otherwise reinterpret a floating-point value. *)
type retry_policy = {
  initial_interval : duration;
  backoff_coefficient_bits : string;
  maximum_interval : duration;
  maximum_attempts : int;
  non_retryable_error_types : string list;
}

(** Provenance and terminal data attached to a continuation initialization.
    [None] means Core supplied the ordinary zero/absent defaults. The nested
    fields remain optional because Core can carry a continuation failure or
    completion payload independently of the initiator. *)
type continuation = {
  continued_from_execution_run_id : string;
  initiator : continue_as_new_initiator;
  continued_failure : failure option;
  last_completion_result : payload list option;
}

(** Initialization fields delivered by Core, including optional continuation
    provenance for a successor run. *)
type initialize_context = {
  headers : (string * payload) list;
  identity : string;
  parent_workflow : namespaced_workflow_execution option;
  workflow_execution_timeout : duration option;
  workflow_run_timeout : duration option;
  workflow_task_timeout : duration option;
  first_execution_run_id : string;
  start_time : timestamp option;
  root_workflow : workflow_execution option;
  priority : workflow_priority option;
  retry_policy : retry_policy option;
  continuation : continuation option;
}

(** Reports whether a failure is non-retryable according to Temporal's
    wrapper semantics. Application failures provide the direct flag; activity
    and child-workflow wrappers use an explicit retry state when Core supplied
    one, and otherwise inherit the nested cause. The bounded walk protects
    callers that construct recursive protocol values without going through the
    JSON decoder's depth validation. *)
val failure_non_retryable : failure -> bool

(** Result variants Core may deliver for a remote activity. *)
type activity_resolution =
  | Completed of payload option
  | Failed of failure
  | Cancelled of failure
  (** Core requests a workflow timer before the next attempt of a local
      activity is scheduled. Remote activities never produce this result. *)
  | Backoff of {
      attempt : int64;
      backoff_duration : duration;
      original_schedule_time : timestamp option;
    }

(** The start acknowledgment is separate from terminal child completion. A
    successful start carries a run ID and leaves the child future pending. *)
type child_workflow_start_failure_cause =
  | Child_start_unspecified
  | Child_start_workflow_already_exists

type child_workflow_start_resolution =
  | Child_start_succeeded of string
  | Child_start_failed of {
      workflow_id : string;
      workflow_type : string;
      cause : child_workflow_start_failure_cause;
    }
  | Child_start_cancelled of failure

(** Terminal result delivered after a child start has succeeded. *)
type child_workflow_resolution =
  | Child_completed of payload option
  | Child_failed of failure
  | Child_cancelled of failure

(** Exact eviction reasons in the pinned Temporal Core revision. *)
type eviction_reason =
  | Eviction_unspecified
  | Cache_full
  | Cache_miss
  | Nondeterminism
  | Lang_fail
  | Lang_requested
  | Task_not_found
  | Unhandled_command
  | Fatal
  | Pagination_or_history_fetch
  | Workflow_execution_ending

(** Supported activation jobs in their Core-provided order. *)
type activation_job =
  | Initialize_workflow of {
      workflow_id : string;
      workflow_type : string;
      arguments : payload list;
      randomness_seed : string;
      attempt : int;
      context : initialize_context option;
    }
  | Resolve_activity of { seq : int64; result : activity_resolution }
  | Resolve_child_workflow_start of {
      seq : int64;
      result : child_workflow_start_resolution;
    }
  | Resolve_child_workflow of {
      seq : int64;
      result : child_workflow_resolution;
    }
  (** Resolves a signal sent to an external workflow. [Error] carries Core's
      structured failure when the target could not be signalled. *)
  | Resolve_signal_external_workflow of {
      seq : int64;
      result : (unit, failure) result;
    }
  (** Resolves a request to cancel an external workflow. *)
  | Resolve_request_cancel_external_workflow of {
      seq : int64;
      result : (unit, failure) result;
    }
  (** Delivers one synchronous query. Arguments and headers are retained even
      though the current public handler API is output-only; non-empty
      arguments are rejected by that adapter rather than silently discarded. *)
  | Query_workflow of {
      query_id : string;
      query_type : string;
      arguments : payload list;
      headers : (string * payload) list;
    }
  (** Delivers one workflow update request. Updates are identified by [id] in
      the semantic activation record. The Rust/Core adapter copies that
      authoritative value into [meta.update_id] because Core may strip the
      duplicate nested protobuf field; the strict JSON decoder receives only
      this canonical form and therefore requires the two semantic values to
      match. *)
  | Do_update of {
      id : string;
      protocol_instance_id : string;
      name : string;
      input : payload list;
      headers : (string * payload) list;
      meta : update_meta;
      run_validator : bool;
    }
  (** Delivers a signal's deterministic name, payloads, sender identity, and
      headers. Core does not assign a command sequence to an incoming signal. *)
  | Signal_workflow of {
      signal_name : string;
      input : payload list;
      identity : string;
      headers : (string * payload) list;
    }
  (** Reports a patch marker already present in this run's history. *)
  | Notify_has_patch of { patch_id : string }
  (** Replaces the workflow random stream using a canonical uint64 decimal seed. *)
  | Update_random_seed of { randomness_seed : string }
  | Fire_timer of { seq : int64 }
  | Cancel_workflow of { reason : string }
  | Remove_from_cache of { message : string; reason : eviction_reason }

(** One workflow activation. [jobs] ordering is deterministic and retained
    exactly. *)
type activation = {
  run_id : string;
  timestamp : timestamp option;
  is_replaying : bool;
  history_length : int64;
  jobs : activation_job list;
  metadata : activation_metadata option;
}
(** A complete activation. [timestamp] is absent only on Core's synthetic
    cache-eviction activation; ordinary activations require it. *)

(** How a scheduled activity responds to cancellation. *)
type activity_cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon

(** Controls how Core reports a child-workflow cancellation request to the
    parent.  The policy is part of the command so replay sees the same
    cancellation semantics on every activation. *)
type child_workflow_cancellation_type =
  | Child_try_cancel
  | Child_wait_cancellation_completed
  | Child_abandon
  | Child_wait_cancellation_requested

(** The successful or failed answer for one synchronous query. Query failures
    are returned to the caller and do not fail the workflow run. *)
type query_result =
  | Query_succeeded of payload
  | Query_failed of failure

(** The two-phase response sent for one workflow update. Core first accepts or
    rejects a request, then may receive its completed payload in a later
    command. *)
type update_response =
  | Update_accepted
  | Update_rejected of failure
  | Update_completed of payload

(** Supported workflow commands in scheduler emission order. *)
type completion_command =
  | Schedule_activity of {
      seq : int64;
      activity_id : string;
      activity_type : string;
      task_queue : string;
      arguments : payload list;
      schedule_to_close_timeout : duration option;
      schedule_to_start_timeout : duration option;
      start_to_close_timeout : duration option;
      heartbeat_timeout : duration option;
      retry_policy : retry_policy option;
      priority : workflow_priority option;
      cancellation_type : activity_cancellation_type;
      do_not_eagerly_execute : bool;
    }
  (** Schedules a local activity. Core executes it on the local activity lane
      and returns the result as a normal [Resolve_activity] activation job. *)
  | Schedule_local_activity of {
      seq : int64;
      activity_id : string;
      activity_type : string;
      attempt : int64;
      original_schedule_time : timestamp option;
      arguments : payload list;
      schedule_to_close_timeout : duration option;
      schedule_to_start_timeout : duration option;
      start_to_close_timeout : duration option;
      retry_policy : retry_policy option;
      local_retry_threshold : duration option;
      cancellation_type : activity_cancellation_type;
    }
  (** Starts a child workflow. The ordered payload list mirrors Temporal Core's
      repeated input field; the current activation runtime emits one payload.
      [retry_policy] is optional because Core's default policy remains valid
      when callers do not configure retries. *)
  | Start_child_workflow of {
      seq : int64;
      workflow_id : string;
      workflow_type : string;
      input : payload list;
      retry_policy : retry_policy option;
      cancellation_type : child_workflow_cancellation_type;
    }
  (** Requests cancellation of a previously started child workflow.  [reason]
      is retained in history and is therefore part of deterministic replay. *)
  | Cancel_child_workflow of { seq : int64; reason : string }
  (** Sends a typed signal to another workflow execution. The target identity,
      child-only safety flag, headers, and payloads are all retained so the
      native bridge can construct Core's official command without guessing. *)
  | Signal_external_workflow of {
      seq : int64;
      workflow_id : string;
      run_id : string;
      signal_name : string;
      input : payload list;
      child_workflow_only : bool;
      headers : (string * payload) list;
    }
  (** Requests cancellation of another workflow execution. Core resolves this
      asynchronous command through a matching activation job. *)
  | Request_cancel_external_workflow of {
      seq : int64;
      workflow_id : string;
      run_id : string;
      reason : string;
    }
  | Request_cancel_activity of { seq : int64 }
  (** Requests cancellation of a local activity attempt through Core's local
      activity manager. *)
  | Request_cancel_local_activity of { seq : int64 }
  | Start_timer of { seq : int64; start_to_fire_timeout : duration }
  | Cancel_timer of { seq : int64 }
  (** Answers a Core query request. The exact query ID is required for modern
      and legacy Core routing; it must not be generated or normalized. *)
  | Query_result of { query_id : string; result : query_result }
  (** Answers one update protocol instance. [Update_accepted] and
      [Update_rejected] are the first-phase decision; [Update_completed]
      carries the handler's final payload. *)
  | Update_response of {
      protocol_instance_id : string;
      response : update_response;
    }
  (** Records one active or deprecated patch lifecycle operation. A completion
      may repeat one mode for an ID, but validation rejects both modes for the
      same ID because Core durably retains the first such command. *)
  | Set_patch_marker of { patch_id : string; deprecated : bool }
  (** Merges payload values into Temporal's indexed search-attribute map. *)
  | Upsert_search_attributes of { search_attributes : (string * payload) list }
  | Complete_workflow of { result : payload option }
  | Fail_workflow of { failure : failure }
  (** Ends this run and starts a successor run of [workflow_type] with the
      supplied argument payloads. The current SDK emits one argument; the
      list mirrors Core's repeated payload field for forward compatibility. *)
  | Continue_as_new of { workflow_type : string; input : payload list }
  | Cancel_workflow_execution

(** Successful completion of one activation. At most one terminal workflow
    command may appear, and it must be last. Patch commands may repeat one mode
    per ID but may not mix active and deprecated modes for the same ID. *)
type completion = {
  run_id : string;
  commands : completion_command list;
  task_failure : failure option;
  (** [Some] fails the workflow task through Core, never the execution. Failed
      tasks carry no commands. [None] preserves the successful command batch. *)
}

type error
(** Opaque semantic or strict-JSON validation failure. *)

type error_view = { code : string; path : string; message : string }
(** Privacy-safe error details that never contain source payload bytes. *)

val error_view : error -> error_view
(** Copies the stable error classification, path, and diagnostic. *)

val decode_activation : string -> (activation, error) result
(** Strictly decodes and semantically validates one activation document. *)

val encode_activation : activation -> (string, error) result
(** Validates, normalizes, and semantically reparses an outgoing activation. *)

val decode_completion : string -> (completion, error) result
(** Strictly decodes and semantically validates one completion document. *)

val encode_completion : completion -> (string, error) result
(** Validates, normalizes, and semantically reparses an outgoing completion. *)

val validate_retry_policy : retry_policy -> (unit, error) result
(** Validates a programmatically constructed retry policy before a command
    translator exposes it as a semantic protocol value. *)

(** Private codec building blocks shared by the closed semantic protocol
    adapters. They are exposed only from the internal protocol library so each
    adapter applies identical payload, failure, numeric, and time rules. *)
module Internal : sig
  val invalid : string -> string -> error
  (** Constructs a privacy-safe semantic validation failure. *)

  val of_control_error : string -> Control_protocol.error -> error
  (** Converts a strict-JSON foundation failure without exposing input data. *)

  val exact_object :
    string -> string list -> Yojson.Safe.t -> ((string * Yojson.Safe.t) list, error) result
  (** Requires an object to contain exactly the named members. *)

  val field :
    string -> string -> (string * Yojson.Safe.t) list -> (Yojson.Safe.t, error) result
  (** Reads one required object member. *)

  val string : string -> Yojson.Safe.t -> (string, error) result
  (** Reads bounded UTF-8 text. *)

  val bool : string -> Yojson.Safe.t -> (bool, error) result
  (** Reads a JSON Boolean. *)

  val uint32 : string -> Yojson.Safe.t -> (int64, error) result
  (** Reads an unsigned protobuf 32-bit integer without platform narrowing. *)

  val int32 : string -> Yojson.Safe.t -> (int, error) result
  (** Reads a signed protobuf 32-bit integer. *)

  val identifier : string -> Yojson.Safe.t -> (string, error) result
  (** Reads a nonempty Temporal identifier within the protocol safety limit. *)

  val uint64_decimal : string -> Yojson.Safe.t -> (string, error) result
  (** Reads canonical unsigned 64-bit decimal text. *)

  val list :
    string ->
    (string -> Yojson.Safe.t -> ('a, error) result) ->
    Yojson.Safe.t ->
    ('a list, error) result
  (** Reads a list while retaining indexed error paths. *)

  val nullable :
    string ->
    (string -> Yojson.Safe.t -> ('a, error) result) ->
    Yojson.Safe.t ->
    ('a option, error) result
  (** Reads an explicitly present nullable member. *)

  val bytes_wrapper : string -> Yojson.Safe.t -> (bytes, error) result
  (** Decodes the canonical base64 object used by binary protocol fields. *)

  val bytes_wrapper_json : bytes -> (Yojson.Safe.t, error) result
  (** Encodes binary data into the canonical base64 object. *)

  val payload : string -> Yojson.Safe.t -> (payload, error) result
  (** Decodes a canonical Temporal payload. *)

  val payload_json : payload -> (Yojson.Safe.t, error) result
  (** Encodes and validates a canonical Temporal payload. *)

  val timestamp : string -> Yojson.Safe.t -> (timestamp, error) result
  (** Decodes exact protobuf timestamp components. *)

  val duration : string -> Yojson.Safe.t -> (duration, error) result
  (** Decodes a normalized nonnegative protobuf duration. *)

  val time_json : int64 -> int -> Yojson.Safe.t
  (** Encodes exact seconds and nanoseconds without floating point. *)

  val workflow_execution :
    string -> Yojson.Safe.t -> (workflow_execution, error) result
  (** Decodes a workflow/run identity pair. *)

  val workflow_priority :
    string -> Yojson.Safe.t -> (workflow_priority, error) result
  (** Decodes matching priority including exact floating-point weight bits. *)

  val failure : string -> Yojson.Safe.t -> (failure, error) result
  (** Decodes the supported recursive Temporal failure model. *)

  val failure_json : failure -> (Yojson.Safe.t, error) result
  (** Encodes the supported recursive Temporal failure model. *)
end
