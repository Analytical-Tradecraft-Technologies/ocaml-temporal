(** One item of work delivered to a workflow execution. For example, a job can
    start the workflow, provide an activity result, or report that a timer has
    fired. [seq] identifies the earlier command that created that activity or
    timer. *)
type job =
  | Start_workflow
  | Resolve_activity of {
      seq : int64;
      result : (Temporal_base.Codec.payload, Temporal_base.Error.t) result;
    }
  (** Core requests a timer before a local activity retry. The activity future
      remains pending until the retried command resolves it. *)
  | Resolve_local_activity_backoff of {
      seq : int64;
      attempt : int64;
      backoff_milliseconds : int64;
      original_schedule_time : Temporal_protocol.Workflow_protocol.timestamp option;
    }
  (** Acknowledges that a child start was accepted. [Ok run_id] records the
      concrete execution while keeping the child result future pending. *)
  | Resolve_child_workflow_start of {
      seq : int64;
      result : (string, Temporal_base.Error.t) result;
    }
  | Resolve_child_workflow of {
      seq : int64;
      result : (Temporal_base.Codec.payload, Temporal_base.Error.t) result;
    }
  (** Completion of a signal sent to an external workflow. *)
  | Resolve_signal_external_workflow of {
      seq : int64;
      result : (unit, Temporal_base.Error.t) result;
    }
  (** Completion of a request to cancel an external workflow. *)
  | Resolve_request_cancel_external_workflow of {
      seq : int64;
      result : (unit, Temporal_base.Error.t) result;
    }
  (** A synchronous query request delivered outside workflow-future ordering. *)
  | Query_workflow of {
      query_id : string;
      query_type : string;
      arguments : Temporal_base.Codec.payload list;
      headers : (string * Temporal_base.Codec.payload) list;
    }
  (** A workflow update request. [protocol_instance_id] correlates the
      response with Core; [id] is the workflow-visible update identity. *)
  | Do_update of {
      id : string;
      protocol_instance_id : string;
      name : string;
      input : Temporal_base.Codec.payload list;
      headers : (string * Temporal_base.Codec.payload) list;
      identity : string;
      update_id : string;
      run_validator : bool;
    }
  (** A signal delivered by Core. It carries no command sequence because it is
      an incoming event rather than a completion of an earlier command. The
      execution runtime resolves its name against the workflow's private
      handler registry and preserves the full payload/metadata record. *)
  | Signal_workflow of {
      signal_name : string;
      input : Temporal_base.Codec.payload list;
      identity : string;
      headers : (string * Temporal_base.Codec.payload) list;
    }
  (** Reports a patch marker found in this execution's history. *)
  | Notify_has_patch of { patch_id : string }
  (** Replaces this execution's random seed at this position in the job list. *)
  | Update_random_seed of { randomness_seed : string }
  | Fire_timer of { seq : int64 }
  | Cancel_workflow
  | Remove_from_cache

(** How an activity reacts after workflow cancellation reaches its task. *)
type activity_cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon

(** Controls when Core resolves a parent future after a child cancellation
    request. *)
type child_workflow_cancellation_type =
  | Child_try_cancel
  | Child_wait_cancellation_completed
  | Child_abandon
  | Child_wait_cancellation_requested

(** A validated retry policy for an activity or child-workflow command kept in
    exact, bridge-ready form. The coefficient is an unsigned decimal
    rendering of its IEEE-754 bits; the other numeric fields retain their
    exact OCaml integer values. *)
type retry_policy = {
  initial_interval : int64;
  backoff_coefficient_bits : string;
  maximum_interval : int64;
  maximum_attempts : int;
  non_retryable_error_types : string list;
}

(** Scheduling metadata attached to an activity command.  The key and weight
    use the same exact wire representation as Temporal Core: zero requests the
    server default and the weight is an IEEE-754 single-precision bit pattern. *)
type priority = Temporal_base.Priority.t

(** An instruction produced by workflow code for Temporal Core. Commands are
    returned in the order they were created because Temporal records that order
    in workflow history and expects replay to reproduce it. Activity timeout
    values and retry intervals are exact non-negative milliseconds; [None]
    means that an optional timeout or retry policy is not supplied. *)
type command =
  | Schedule_activity of {
      seq : int64;
      activity_id : string;
      activity_type : string;
      task_queue : string;
      arguments : Temporal_base.Codec.payload list;
      schedule_to_close_timeout : int64 option;
      schedule_to_start_timeout : int64 option;
      start_to_close_timeout : int64 option;
      heartbeat_timeout : int64 option;
      retry_policy : retry_policy option;
      priority : priority option;
      cancellation_type : activity_cancellation_type;
      do_not_eagerly_execute : bool;
    }
  (** A local activity command executed by Core and resolved through the same
      activity sequence as a remote activity. *)
  | Schedule_local_activity of {
      seq : int64;
      activity_id : string;
      activity_type : string;
      attempt : int64;
      original_schedule_time : Temporal_protocol.Workflow_protocol.timestamp option;
      arguments : Temporal_base.Codec.payload list;
      schedule_to_close_timeout : int64 option;
      schedule_to_start_timeout : int64 option;
      start_to_close_timeout : int64 option;
      retry_policy : retry_policy option;
      local_retry_threshold : int64 option;
      cancellation_type : activity_cancellation_type;
    }
  | Start_child_workflow of {
      seq : int64;
      id : string;
      name : string;
      input : Temporal_base.Codec.payload;
      retry_policy : retry_policy option;
      cancellation_type : child_workflow_cancellation_type;
    }
  (** Requests cancellation of a pending child workflow. *)
  | Cancel_child_workflow of { seq : int64; reason : string }
  (** Sends one signal to a specific workflow execution. *)
  | Signal_external_workflow of {
      seq : int64;
      workflow_id : string;
      run_id : string;
      signal_name : string;
      input : Temporal_base.Codec.payload list;
      child_workflow_only : bool;
      headers : (string * Temporal_base.Codec.payload) list;
    }
  (** Requests cancellation of a specific workflow execution. *)
  | Request_cancel_external_workflow of {
      seq : int64;
      workflow_id : string;
      run_id : string;
      reason : string;
    }
  | Request_cancel_activity of { seq : int64 }
  (** Cancels a local activity through Core's local activity manager. *)
  | Request_cancel_local_activity of { seq : int64 }
  | Start_timer of { seq : int64; milliseconds : int64 }
  | Cancel_timer of { seq : int64 }
  (** A query answer. A failed result is returned to the query caller and does
      not fail the workflow execution. *)
  | Query_result of {
      query_id : string;
      result : (Temporal_base.Codec.payload, Temporal_base.Error.t) result;
    }
  (** One update protocol phase. An immediate update may emit accepted and
      completed commands with the same protocol instance in one activation. *)
  | Update_response of {
      protocol_instance_id : string;
      response :
        [ `Accepted
        | `Rejected of Temporal_base.Error.t
        | `Completed of Temporal_base.Codec.payload ];
    }
  (** Records one active or deprecated replay-safe patch lifecycle operation. *)
  | Set_patch_marker of { patch_id : string; deprecated : bool }
  (** Merges encoded values into the workflow's indexed search attributes.
      Core applies the map atomically and retains the update in history. *)
  | Upsert_search_attributes of {
      search_attributes : (string * Temporal_base.Codec.payload) list;
    }
  | Complete_workflow of Temporal_base.Codec.payload
  | Fail_workflow of Temporal_base.Error.t
  (** Replaces the current run with a new run of the same workflow type. The
      command is terminal for the current execution; [input] is the one
      encoded argument passed to the successor run. *)
  | Continue_as_new of { workflow_type : string; input : Temporal_base.Codec.payload }
  | Cancel_workflow_execution
