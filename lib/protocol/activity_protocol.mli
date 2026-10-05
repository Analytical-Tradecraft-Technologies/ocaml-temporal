(** Typed private JSON semantics for remote activity tasks and completions.
    Official Temporal protobuf values remain confined to the Rust bridge. *)

(** Binary Temporal payload with binary metadata preserved losslessly. *)
type payload = Workflow_protocol.payload = {
  metadata : (string * bytes) list;
  data : bytes;
}

(** Exact protobuf timestamp components without floating-point conversion. *)
type timestamp = Workflow_protocol.timestamp = {
  seconds : int64;
  nanoseconds : int;
}

(** Normalized nonnegative protobuf duration. *)
type duration = Workflow_protocol.duration = {
  seconds : int64;
  nanoseconds : int;
}

(** Workflow and run identity that scheduled an activity. *)
type workflow_execution = Workflow_protocol.workflow_execution = {
  workflow_id : string;
  run_id : string;
}

(** Matching priority with the exact IEEE-754 fairness-weight bit pattern. *)
type workflow_priority = Workflow_protocol.workflow_priority = {
  priority_key : int;
  fairness_key : string;
  fairness_weight_bits : int64;
}

(** Retry state retained by a structured Temporal activity failure. *)
type retry_state = Workflow_protocol.retry_state =
  | Unspecified
  | In_progress
  | Non_retryable_failure
  | Timeout
  | Maximum_attempts_reached
  | Retry_policy_not_set
  | Internal_server_error
  | Cancel_requested

(** Timeout policy retained by Core's structured timeout failure info. *)
type timeout_type = Workflow_protocol.timeout_type =
  | Timeout_unspecified
  | Timeout_start_to_close
  | Timeout_schedule_to_start
  | Timeout_schedule_to_close
  | Timeout_heartbeat

(** Severity of an application failure in the pinned Core schema. *)
type application_failure_category = Workflow_protocol.application_failure_category =
  | Application_category_unspecified
  | Application_category_benign

(** Retry override attached to a Nexus handler error. *)
type nexus_handler_retry_behavior = Workflow_protocol.nexus_handler_retry_behavior =
  | Nexus_retry_unspecified
  | Nexus_retry_retryable
  | Nexus_retry_non_retryable

(** Supported closed set of structured Temporal failure details. *)
type failure_info = Workflow_protocol.failure_info =
  | Application of {
      type_name : string;
      non_retryable : bool;
      details : payload list;
      category : application_failure_category;
      next_retry_delay : duration option;
    }
  | Canceled of { details : payload list; identity : string }
  | Terminated of { identity : string }
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
  | Server of { non_retryable : bool }
  | Reset_workflow of { last_heartbeat_details : payload list }
  | Nexus_operation of {
      scheduled_event_id : int64;
      endpoint : string;
      service : string;
      operation : string;
      operation_id : string;
      operation_token : string;
    }
  | Nexus_handler of {
      type_name : string;
      retry_behavior : nexus_handler_retry_behavior;
    }
  | Absent

(** Recursive Temporal failure shared with workflow activation semantics. *)
type failure = Workflow_protocol.failure = {
  message : string;
  source : string;
  stack_trace : string;
  encoded_attributes : payload option;
  cause : failure option;
  info : failure_info;
}

(** Effective server-normalized retry policy for an activity attempt.
    [backoff_coefficient_bits] is canonical unsigned 64-bit decimal text so
    every possible IEEE-754 bit pattern survives OCaml's signed integers. *)
type retry_policy = {
  initial_interval : duration option;
  backoff_coefficient_bits : string;
  maximum_interval : duration option;
  maximum_attempts : int;
  non_retryable_error_types : string list;
}

(** Complete execution context needed to invoke one activity attempt.
    [is_local] identifies Core's local-activity lane, whose completions cannot
    hand off to the asynchronous client endpoint. Association lists are
    normalized by key when encoded. *)
type activity_start = {
  is_local : bool;
  workflow_namespace : string;
  workflow_type : string;
  workflow_execution : workflow_execution;
  activity_id : string;
  activity_type : string;
  header_fields : (string * payload) list;
  input : payload list;
  heartbeat_details : payload list;
  scheduled_time : timestamp option;
  current_attempt_scheduled_time : timestamp option;
  started_time : timestamp option;
  attempt : int64;
  schedule_to_close_timeout : duration option;
  start_to_close_timeout : duration option;
  heartbeat_timeout : duration option;
  retry_policy : retry_policy option;
  priority : workflow_priority option;
  standalone_run_id : string;
}

(** Stable semantic names for official Core cancellation reasons. *)
type cancel_reason =
  | Cancellation_not_found
  | Cancellation_requested
  | Cancellation_timed_out
  | Cancellation_worker_shutdown
  | Cancellation_paused
  | Cancellation_reset

(** Independent cancellation facts supplied by newer Core paths. *)
type cancellation_details = {
  is_not_found : bool;
  is_cancelled : bool;
  is_paused : bool;
  is_timed_out : bool;
  is_worker_shutdown : bool;
  is_reset : bool;
}

(** Cancellation context for an already outstanding activity attempt. *)
type activity_cancel = {
  reason : cancel_reason;
  details : cancellation_details option;
}

(** Closed set of activity-task variants delivered by the Rust bridge. *)
type task_variant = Start of activity_start | Cancel of activity_cancel

(** One leased activity task. [task_token] is opaque binary correlation data
    and must be copied unchanged into its completion. *)
type task = { task_token : bytes; variant : task_variant }

(** Terminal outcomes accepted by Temporal Core for an activity attempt. *)
type completion_result =
  | Completed of payload option
  | Failed of failure
  | Cancelled of failure
  | Will_complete_async

(** One terminal response to a leased activity task token. *)
type completion = { task_token : bytes; result : completion_result }

(** Progress details for one currently leased activity attempt. *)
type heartbeat = { task_token : bytes; details : payload list }

type error
(** Opaque semantic or strict-JSON validation failure. *)

type error_view = { code : string; path : string; message : string }
(** Privacy-safe diagnostics that never contain source payload bytes. *)

val error_view : error -> error_view
(** Copies the stable error classification, path, and diagnostic. *)

val decode_task : string -> (task, error) result
(** Strictly decodes and validates one activity-task document. *)

val encode_task : task -> (string, error) result
(** Validates, normalizes, and semantically reparses an outgoing task. *)

val decode_completion : string -> (completion, error) result
(** Strictly decodes and validates one activity-completion document. *)

val encode_completion : completion -> (string, error) result
(** Validates, normalizes, and semantically reparses an outgoing completion. *)

val decode_heartbeat : string -> (heartbeat, error) result
(** Strictly decodes and validates one activity heartbeat document. *)

val encode_heartbeat : heartbeat -> (string, error) result
(** Validates, normalizes, and semantically reparses an outgoing heartbeat. *)
