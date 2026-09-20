//! Closed semantic JSON protocol for workflow activations and completions.
//!
//! OCaml and Rust intentionally validate the same language-neutral document.
//! Protobuf values exist only in the conversion functions at the bottom of
//! this module and can therefore never escape into the OCaml API.

use std::collections::{BTreeMap, BTreeSet};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::protocol::{self, JsonValue, MAX_PAYLOAD_BYTES, MAX_STRING_BYTES, ProtocolError};

/// Decodes an explicitly present nullable field.
///
/// Serde otherwise maps both an omitted `Option` field and JSON `null` to
/// `None`. The private protocol distinguishes those cases: fields declared
/// required by the schema must appear, even when their value is null.
pub(crate) fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Opaque Temporal payload with binary-safe metadata and body bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Payload {
    /// Metadata values keyed by their Temporal metadata name.
    pub metadata: BTreeMap<String, Vec<u8>>,
    /// Opaque payload body.
    pub data: Vec<u8>,
}

/// Wire representation of one canonical base64 byte value.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EncodedBytes {
    encoding: String,
    data: String,
}

/// Wire representation used solely by the custom [`Payload`] serializer.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PayloadWire {
    metadata: BTreeMap<String, EncodedBytes>,
    data: EncodedBytes,
}

/// Converts bytes to the only encoding accepted by the protocol.
fn encode_bytes(value: &[u8]) -> Result<EncodedBytes, &'static str> {
    if value.len() > MAX_PAYLOAD_BYTES {
        return Err("payload exceeds the byte limit");
    }
    Ok(EncodedBytes {
        encoding: "base64".to_owned(),
        data: STANDARD.encode(value),
    })
}

/// Validates and decodes one canonical padded base64 wrapper.
fn decode_bytes(value: EncodedBytes) -> Result<Vec<u8>, &'static str> {
    if value.encoding != "base64" {
        return Err("unsupported payload encoding");
    }
    let decoded = STANDARD
        .decode(value.data.as_bytes())
        .map_err(|_| "payload is not canonical padded base64")?;
    if decoded.len() > MAX_PAYLOAD_BYTES || STANDARD.encode(&decoded) != value.data {
        return Err("payload is not canonical padded base64");
    }
    Ok(decoded)
}

impl Serialize for Payload {
    /// Emits the canonical binary-safe payload object.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let metadata = self
            .metadata
            .iter()
            .map(|(key, value)| {
                if key.is_empty() || key.len() > MAX_STRING_BYTES {
                    return Err(serde::ser::Error::custom(
                        "metadata key length is outside protocol limits",
                    ));
                }
                Ok((
                    key.clone(),
                    encode_bytes(value).map_err(serde::ser::Error::custom)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, S::Error>>()?;
        PayloadWire {
            metadata,
            data: encode_bytes(&self.data).map_err(serde::ser::Error::custom)?,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Payload {
    /// Accepts only closed canonical base64 payload objects.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = PayloadWire::deserialize(deserializer)?;
        let metadata = wire
            .metadata
            .into_iter()
            .map(|(key, value)| {
                if key.is_empty() || key.len() > MAX_STRING_BYTES {
                    return Err(de::Error::custom(
                        "metadata key length is outside protocol limits",
                    ));
                }
                Ok((key, decode_bytes(value).map_err(de::Error::custom)?))
            })
            .collect::<Result<BTreeMap<_, _>, D::Error>>()?;
        Ok(Self {
            metadata,
            data: decode_bytes(wire.data).map_err(de::Error::custom)?,
        })
    }
}

/// Exact protobuf timestamp components without floating-point conversion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Timestamp {
    /// Whole seconds from the Unix epoch.
    pub seconds: i64,
    /// Fractional nanoseconds in the inclusive range 0 through 999,999,999.
    pub nanoseconds: i32,
}

/// Exact normalized nonnegative protobuf duration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Duration {
    /// Whole nonnegative seconds.
    pub seconds: i64,
    /// Fractional nanoseconds in the inclusive range 0 through 999,999,999.
    pub nanoseconds: i32,
}

/// Workflow and run identifiers for the root execution of a child workflow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowExecution {
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Concrete workflow run identifier.
    pub run_id: String,
}

/// Parent execution identity, including its namespace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespacedWorkflowExecution {
    /// Namespace containing the parent execution.
    pub namespace: String,
    /// Stable parent workflow identifier.
    pub workflow_id: String,
    /// Concrete parent run identifier.
    pub run_id: String,
}

/// Exact workflow priority values delivered by Core.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowPriority {
    /// Lower positive values represent higher priority; zero requests default behavior.
    pub priority_key: i32,
    /// Fairness group key used by Temporal matching.
    pub fairness_key: String,
    /// Raw IEEE-754 bits preserve Core's `f32` without using JSON fractions.
    pub fairness_weight_bits: u32,
}

/// Metadata attached to a workflow update request.
///
/// Core removes the workflow-scoped update ID from this nested protobuf
/// message and exposes it on the update job. Keeping both values in the
/// semantic record lets the bilateral validators prove that the duplicated
/// identity remains consistent instead of silently dropping the metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateMeta {
    /// Identity supplied by the requester.  It is history-derived text and
    /// therefore must be copied before OCaml retains it.
    pub identity: String,
    /// Workflow-scoped identifier repeated for consistency checking.
    pub update_id: String,
}

/// Why Temporal Core created a successor workflow run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinueAsNewInitiator {
    /// Core supplied no continuation reason (the ordinary initial default).
    Unspecified,
    /// The workflow explicitly requested continue-as-new.
    Workflow,
    /// Core continued the workflow while applying a retry policy.
    Retry,
    /// Core continued the workflow because a cron schedule fired.
    CronSchedule,
}

/// Provenance and terminal data attached to a continuation initialization.
/// Optional failure and payload values are retained independently so a
/// successor activation cannot silently lose Core's completion metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Continuation {
    /// Run ID that continued into the current run.
    pub continued_from_execution_run_id: String,
    /// Core's reason for creating the successor run.
    pub initiator: ContinueAsNewInitiator,
    /// Failure recorded when the previous continuation did not complete.
    #[serde(deserialize_with = "required_nullable")]
    pub continued_failure: Option<Failure>,
    /// Payloads returned by the previous run when it completed.
    #[serde(deserialize_with = "required_nullable")]
    pub last_completion_result: Option<Vec<Payload>>,
}

/// Normal fields delivered with a workflow initialization, including
/// continuation provenance when Core starts a successor run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitializeContext {
    /// User headers delivered to workflow interceptors.
    pub headers: BTreeMap<String, Payload>,
    /// Start memo, preserving absent versus explicitly empty protobuf maps.
    #[serde(deserialize_with = "required_nullable")]
    pub memo: Option<BTreeMap<String, Payload>>,
    /// Indexed values recorded on this run's start event, including inherited values.
    #[serde(deserialize_with = "required_nullable")]
    pub search_attributes: Option<BTreeMap<String, Payload>>,
    /// Server-enforced execution-chain deadline; zero remains a present timestamp.
    #[serde(deserialize_with = "required_nullable")]
    pub workflow_execution_expiration_time: Option<Timestamp>,
    /// Server-applied delay before this run's first task, including continuation throttling.
    #[serde(deserialize_with = "required_nullable")]
    pub first_workflow_task_backoff: Option<Duration>,
    /// Identity of the client that started this execution.
    pub identity: String,
    /// Parent execution for a child workflow, otherwise absent.
    #[serde(deserialize_with = "required_nullable")]
    pub parent_workflow: Option<NamespacedWorkflowExecution>,
    /// Timeout across retries and continue-as-new runs.
    #[serde(deserialize_with = "required_nullable")]
    pub workflow_execution_timeout: Option<Duration>,
    /// Timeout for this individual workflow run.
    #[serde(deserialize_with = "required_nullable")]
    pub workflow_run_timeout: Option<Duration>,
    /// Timeout for one workflow task.
    #[serde(deserialize_with = "required_nullable")]
    pub workflow_task_timeout: Option<Duration>,
    /// First run in this execution chain.
    pub first_execution_run_id: String,
    /// Time at which the execution-start event was written.
    #[serde(deserialize_with = "required_nullable")]
    pub start_time: Option<Timestamp>,
    /// Root execution for a child workflow, otherwise absent.
    #[serde(deserialize_with = "required_nullable")]
    pub root_workflow: Option<WorkflowExecution>,
    /// Workflow scheduling priority, when supplied by Core.
    #[serde(deserialize_with = "required_nullable")]
    pub priority: Option<WorkflowPriority>,
    /// Effective workflow retry policy, when Core initialized a retrying
    /// execution. The workflow runtime does not act on this metadata, but it
    /// must cross the private bridge losslessly so a child workflow started
    /// with a retry policy can receive its first activation.
    #[serde(deserialize_with = "required_nullable")]
    pub retry_policy: Option<RetryPolicy>,
    /// Continuation metadata, absent only when all Core continuation fields
    /// carry their ordinary zero/absent defaults.
    #[serde(deserialize_with = "required_nullable")]
    pub continuation: Option<Continuation>,
}

/// Deployment version responsible for the current workflow task.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerDeploymentVersion {
    pub deployment_name: String,
    pub build_id: String,
}

/// Why the service suggests continuing the workflow as new.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuggestContinueAsNewReason {
    Unspecified,
    HistorySizeTooLarge,
    TooManyHistoryEvents,
    TooManyUpdates,
}

/// Activation-wide metadata consumed by a language SDK.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationMetadata {
    pub available_internal_flags: Vec<u32>,
    pub history_size_bytes: String,
    pub continue_as_new_suggested: bool,
    #[serde(deserialize_with = "required_nullable")]
    pub deployment_version_for_current_task: Option<WorkerDeploymentVersion>,
    pub last_sdk_version: String,
    pub suggest_continue_as_new_reasons: Vec<SuggestContinueAsNewReason>,
    pub target_worker_deployment_version_changed: bool,
}

/// Retry disposition attached to an activity failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryState {
    Unspecified,
    InProgress,
    NonRetryableFailure,
    Timeout,
    MaximumAttemptsReached,
    RetryPolicyNotSet,
    InternalServerError,
    CancelRequested,
}

/// The exact timeout reason reported by Temporal Core.
///
/// Keeping this separate from [`RetryState::Timeout`] matters: the retry state
/// describes an activity or child-workflow wrapper, while this enum records
/// which timeout policy actually elapsed and therefore carries Core's
/// `TimeoutFailureInfo` semantics losslessly across the JSON boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeoutType {
    /// Core did not identify a more specific timeout policy.
    Unspecified,
    /// The activity or workflow run exceeded its start-to-close timeout.
    StartToClose,
    /// The task remained in the queue longer than its schedule-to-start timeout.
    ScheduleToStart,
    /// The operation exceeded its schedule-to-close timeout.
    ScheduleToClose,
    /// A heartbeat was not received before the heartbeat timeout elapsed.
    Heartbeat,
}

/// Supported closed subset of Temporal failure information.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FailureInfo {
    Application {
        #[serde(rename = "type")]
        type_name: String,
        non_retryable: bool,
        details: Vec<Payload>,
    },
    Canceled {
        details: Vec<Payload>,
        identity: String,
    },
    Activity {
        scheduled_event_id: i64,
        started_event_id: i64,
        identity: String,
        activity_type: String,
        activity_id: String,
        retry_state: RetryState,
    },
    ChildWorkflow {
        namespace: String,
        workflow_id: String,
        run_id: String,
        workflow_type: String,
        initiated_event_id: i64,
        started_event_id: i64,
        retry_state: RetryState,
    },
    /// Timeout metadata, including the exact timeout policy and any heartbeat
    /// details Core retained from the timed-out activity.
    Timeout {
        timeout_type: TimeoutType,
        last_heartbeat_details: Vec<Payload>,
    },
}

/// Recursive Temporal failure with one of the explicitly supported info variants.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub message: String,
    pub source: String,
    pub stack_trace: String,
    #[serde(deserialize_with = "required_nullable")]
    pub encoded_attributes: Option<Payload>,
    #[serde(deserialize_with = "required_nullable")]
    pub cause: Option<Box<Failure>>,
    pub info: FailureInfo,
}

/// Result delivered for an activity sequence number.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActivityResolution {
    Completed {
        #[serde(deserialize_with = "required_nullable")]
        payload: Option<Payload>,
    },
    Failed {
        failure: Failure,
    },
    Cancelled {
        failure: Failure,
    },
    /// Core asks the language runtime to wait before issuing the next local
    /// activity attempt. This is deliberately not exposed for remote
    /// activities: only Core's local-activity state machine emits it.
    Backoff {
        attempt: u32,
        backoff_duration: Duration,
        #[serde(deserialize_with = "required_nullable")]
        original_schedule_time: Option<Timestamp>,
    },
}

/// Why Core could not start a child workflow execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildWorkflowStartFailureCause {
    Unspecified,
    WorkflowAlreadyExists,
}

/// Result of the initial child-workflow start command. A successful start only
/// records the run ID; the child future remains pending until the separate
/// terminal resolution arrives.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChildWorkflowStartResolution {
    Succeeded {
        run_id: String,
    },
    Failed {
        workflow_id: String,
        workflow_type: String,
        cause: ChildWorkflowStartFailureCause,
    },
    Cancelled {
        failure: Failure,
    },
}

/// Terminal result of a child workflow execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChildWorkflowResolution {
    Completed {
        #[serde(deserialize_with = "required_nullable")]
        payload: Option<Payload>,
    },
    Failed {
        failure: Failure,
    },
    Cancelled {
        failure: Failure,
    },
}

/// Result of an external signal or cancellation command. Core represents a
/// successful operation with an absent failure field and all rejected or
/// cancelled operations with one structured Temporal failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalWorkflowResolution {
    /// Temporal accepted the command for delivery.
    Succeeded,
    /// Temporal could not deliver or complete the command.
    Failed { failure: Box<Failure> },
}

/// Why Core requested removal of a workflow from the language cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvictionReason {
    Unspecified,
    CacheFull,
    CacheMiss,
    Nondeterminism,
    LangFail,
    LangRequested,
    TaskNotFound,
    UnhandledCommand,
    Fatal,
    PaginationOrHistoryFetch,
    WorkflowExecutionEnding,
}

/// Supported activation instruction sent from Core to OCaml.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActivationJob {
    InitializeWorkflow {
        workflow_id: String,
        workflow_type: String,
        arguments: Vec<Payload>,
        randomness_seed: String,
        attempt: i32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Boxed because initialization carries substantially more metadata
        /// than the other activation jobs; serde keeps the same JSON shape.
        context: Option<Box<InitializeContext>>,
    },
    ResolveActivity {
        seq: u32,
        result: ActivityResolution,
    },
    ResolveChildWorkflowStart {
        seq: u32,
        result: ChildWorkflowStartResolution,
    },
    ResolveChildWorkflow {
        seq: u32,
        result: ChildWorkflowResolution,
    },
    /// Reports completion of an external workflow signal command.
    ResolveSignalExternalWorkflow {
        seq: u32,
        result: ExternalWorkflowResolution,
    },
    /// Reports completion of an external workflow cancellation command.
    ResolveRequestCancelExternalWorkflow {
        seq: u32,
        result: ExternalWorkflowResolution,
    },
    /// Incoming signal data delivered by Core. Signals are activation events,
    /// not completions, so they intentionally do not carry a sequence number.
    SignalWorkflow {
        signal_name: String,
        input: Vec<Payload>,
        identity: String,
        headers: BTreeMap<String, Payload>,
    },
    /// Synchronously evaluates one read-only workflow query.
    ///
    /// Core gives query jobs their own activation. The bridge keeps the
    /// repeated arguments and headers lossless even though the first public
    /// OCaml query handler accepts no arguments; a non-empty argument list is
    /// rejected by that typed adapter rather than discarded.
    QueryWorkflow {
        query_id: String,
        query_type: String,
        arguments: Vec<Payload>,
        headers: BTreeMap<String, Payload>,
    },
    /// Requests validation and execution of one workflow update.
    ///
    /// Core may place update jobs beside ordinary workflow jobs.  The update
    /// response command is correlated by [protocol_instance_id], not by the
    /// workflow-visible [id], so both identifiers are retained exactly.
    DoUpdate {
        id: String,
        protocol_instance_id: String,
        name: String,
        input: Vec<Payload>,
        headers: BTreeMap<String, Payload>,
        meta: UpdateMeta,
        run_validator: bool,
    },
    /// Reports authoritative history evidence for one workflow patch.
    NotifyHasPatch {
        patch_id: String,
    },
    FireTimer {
        seq: u32,
    },
    CancelWorkflow {
        reason: String,
    },
    RemoveFromCache {
        message: String,
        reason: EvictionReason,
    },
}

/// Complete worker activation delivered to one deterministic workflow run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Activation {
    pub run_id: String,
    /// Core omits time only on its synthetic cache-eviction activation.
    #[serde(deserialize_with = "required_nullable")]
    pub timestamp: Option<Timestamp>,
    pub is_replaying: bool,
    pub history_length: u32,
    pub jobs: Vec<ActivationJob>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ActivationMetadata>,
}

/// How a workflow waits for cancellation of a scheduled activity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityCancellationType {
    TryCancel,
    WaitCancellationCompleted,
    Abandon,
}

/// Controls when Core resolves the parent after a child cancellation request.
/// The extra `WaitCancellationRequested` state is specific to child workflows
/// and is intentionally not merged with activity cancellation policies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildWorkflowCancellationType {
    TryCancel,
    WaitCancellationCompleted,
    Abandon,
    WaitCancellationRequested,
}

/// Retry policy supplied with one scheduled activity.
///
/// The coefficient remains an unsigned decimal rendering of its IEEE-754
/// bits. Keeping the bit pattern in the semantic protocol makes replay and
/// the OCaml/Rust boundary independent of a language's float formatter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    /// Positive delay before the first retry.
    pub initial_interval: Duration,
    /// Canonical unsigned decimal representation of an `f64` bit pattern.
    pub backoff_coefficient_bits: String,
    /// Positive upper bound for a retry delay.
    pub maximum_interval: Duration,
    /// Maximum attempts, where zero means unlimited.
    pub maximum_attempts: i32,
    /// Application failure type names that must not be retried.
    pub non_retryable_error_types: Vec<String>,
}

/// Supported deterministic command emitted by OCaml for Core.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompletionCommand {
    ScheduleActivity {
        seq: u32,
        activity_id: String,
        activity_type: String,
        task_queue: String,
        arguments: Vec<Payload>,
        #[serde(deserialize_with = "required_nullable")]
        schedule_to_close_timeout: Option<Duration>,
        #[serde(deserialize_with = "required_nullable")]
        schedule_to_start_timeout: Option<Duration>,
        #[serde(deserialize_with = "required_nullable")]
        start_to_close_timeout: Option<Duration>,
        #[serde(deserialize_with = "required_nullable")]
        heartbeat_timeout: Option<Duration>,
        #[serde(deserialize_with = "required_nullable")]
        retry_policy: Option<RetryPolicy>,
        /// Optional task priority and fairness metadata.  A present value
        /// retains zero/empty defaults explicitly so replay cannot infer a
        /// different server policy from an omitted JSON member.
        #[serde(deserialize_with = "required_nullable")]
        priority: Option<WorkflowPriority>,
        cancellation_type: ActivityCancellationType,
        do_not_eagerly_execute: bool,
    },
    /// Schedules a worker-local activity. Core executes this on its local
    /// activity lane and records the resolution in workflow history; the
    /// bridge still transports the command as strict semantic JSON so the
    /// OCaml scheduler can replay it without protobuf types.
    ScheduleLocalActivity {
        seq: u32,
        activity_id: String,
        activity_type: String,
        attempt: u32,
        #[serde(deserialize_with = "required_nullable")]
        original_schedule_time: Option<Timestamp>,
        arguments: Vec<Payload>,
        #[serde(deserialize_with = "required_nullable")]
        schedule_to_close_timeout: Option<Duration>,
        #[serde(deserialize_with = "required_nullable")]
        schedule_to_start_timeout: Option<Duration>,
        #[serde(deserialize_with = "required_nullable")]
        start_to_close_timeout: Option<Duration>,
        #[serde(deserialize_with = "required_nullable")]
        retry_policy: Option<RetryPolicy>,
        #[serde(deserialize_with = "required_nullable")]
        local_retry_threshold: Option<Duration>,
        cancellation_type: ActivityCancellationType,
    },
    /// Starts a child workflow and records the policy used for an explicit
    /// later cancellation command.  Core owns retries, so an optional policy
    /// is carried with the command rather than reimplemented in replayed OCaml
    /// code.
    StartChildWorkflow {
        seq: u32,
        workflow_id: String,
        workflow_type: String,
        input: Vec<Payload>,
        #[serde(deserialize_with = "required_nullable")]
        retry_policy: Option<RetryPolicy>,
        cancellation_type: ChildWorkflowCancellationType,
    },
    /// Requests cancellation of a previously started child workflow.
    CancelChildWorkflow {
        seq: u32,
        reason: String,
    },
    /// Sends a signal to a named external workflow execution.
    SignalExternalWorkflow {
        seq: u32,
        workflow_id: String,
        run_id: String,
        signal_name: String,
        input: Vec<Payload>,
        child_workflow_only: bool,
        headers: BTreeMap<String, Payload>,
    },
    /// Requests cancellation of a named external workflow execution.
    RequestCancelExternalWorkflow {
        seq: u32,
        workflow_id: String,
        run_id: String,
        reason: String,
    },
    RequestCancelActivity {
        seq: u32,
    },
    /// Requests cancellation of a local activity through Core's local lane.
    RequestCancelLocalActivity {
        seq: u32,
    },
    StartTimer {
        seq: u32,
        start_to_fire_timeout: Duration,
    },
    CancelTimer {
        seq: u32,
    },
    CompleteWorkflow {
        #[serde(deserialize_with = "required_nullable")]
        result: Option<Payload>,
    },
    FailWorkflow {
        failure: Failure,
    },
    /// Replaces the current run with a fresh run of the same workflow type.
    /// The language layer currently supplies only the workflow type and input;
    /// all other Core options remain explicit defaults until exposed safely.
    ContinueAsNew {
        workflow_type: String,
        input: Vec<Payload>,
    },
    CancelWorkflow,
    /// Returns one query result to Core without scheduling workflow work.
    ///
    /// The nested result mirrors Core's oneof: success carries an optional
    /// payload (the payload itself is required by the semantic validator),
    /// while failure carries the complete recursive Temporal failure.
    QueryResult {
        query_id: String,
        result: QueryResult,
    },
    /// Acknowledges validation or returns the terminal result of one update
    /// handler.  Core accepts the first response and later expects either a
    /// completed payload or a structured rejection for the same protocol ID.
    UpdateResponse {
        protocol_instance_id: String,
        response: UpdateResponseResult,
    },
    /// Records one active or deprecated patch lifecycle operation. Core owns
    /// same-mode history deduplication, so the language runtime intentionally
    /// sends this on every public patch call.
    SetPatchMarker {
        patch_id: String,
        deprecated: bool,
    },
    /// Merges encoded values into Core's indexed workflow search attributes.
    UpsertSearchAttributes {
        search_attributes: BTreeMap<String, Payload>,
    },
}

/// Outcome of one synchronous read-only workflow query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryResult {
    /// Query handler returned one payload.
    Succeeded { payload: Payload },
    /// Query handler could not produce a value; Core receives this failure
    /// without failing the workflow execution itself.
    Failed { failure: Box<Failure> },
}

/// The closed set of responses accepted by Core for one workflow update.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UpdateResponseResult {
    /// Validator passed, or Core requested replay without re-running it.
    Accepted,
    /// Validator or handler rejected the update.
    Rejected { failure: Box<Failure> },
    /// Handler completed successfully with one encoded payload.
    Completed { payload: Payload },
}

/// Successful activation completion for one workflow run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub run_id: String,
    pub commands: Vec<CompletionCommand>,
}

/// Converts the duplicate-aware foundation tree to Serde's owned value.
fn to_serde(value: JsonValue) -> serde_json::Value {
    match value {
        JsonValue::Null => serde_json::Value::Null,
        JsonValue::Bool(value) => serde_json::Value::Bool(value),
        JsonValue::Signed(value) => value.into(),
        JsonValue::Unsigned(value) => value.into(),
        JsonValue::String(value) => value.into(),
        JsonValue::Array(values) => values.into_iter().map(to_serde).collect(),
        JsonValue::Object(entries) => entries
            .into_iter()
            .map(|(key, value)| (key, to_serde(value)))
            .collect(),
    }
}

/// Converts Serde output back into the restricted integral JSON tree.
fn from_serde(value: serde_json::Value) -> Result<JsonValue, ProtocolError> {
    match value {
        serde_json::Value::Null => Ok(JsonValue::Null),
        serde_json::Value::Bool(value) => Ok(JsonValue::Bool(value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(JsonValue::Signed)
            .or_else(|| value.as_u64().map(JsonValue::Unsigned))
            .ok_or_else(|| ProtocolError::invalid("$", "non-integral number in semantic output")),
        serde_json::Value::String(value) => Ok(JsonValue::String(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(from_serde)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        serde_json::Value::Object(entries) => entries
            .into_iter()
            .map(|(key, value)| Ok((key, from_serde(value)?)))
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Object),
    }
}

/// Requires a bounded nonempty Temporal identifier that is safe for both
/// semantic JSON and the official Core string fields. Rust's `&str` invariant
/// already guarantees UTF-8, but the explicit check mirrors OCaml's byte-level
/// validator and documents the bilateral boundary for future refactors that
/// may construct values from raw bytes.
pub(crate) fn identifier(value: &str, path: &str) -> Result<(), ProtocolError> {
    if value.is_empty() || value.len() > MAX_STRING_BYTES {
        Err(ProtocolError::invalid(
            path,
            "identifier is empty or exceeds the protocol string safety limit",
        ))
    } else if value.as_bytes().contains(&0) {
        Err(ProtocolError::invalid(
            path,
            "identifier must not contain NUL",
        ))
    } else if std::str::from_utf8(value.as_bytes()).is_err() {
        Err(ProtocolError::invalid(
            path,
            "identifier must be valid UTF-8",
        ))
    } else {
        Ok(())
    }
}

/// Retains the ordinary text ceiling while the foundation parser temporarily
/// allows larger base64 strings at semantic payload paths.
fn bounded_text(value: &str, path: &str) -> Result<(), ProtocolError> {
    if value.len() > MAX_STRING_BYTES {
        Err(ProtocolError::invalid(
            path,
            "decoded JSON string limit exceeded",
        ))
    } else {
        Ok(())
    }
}

/// Validates a signal sender identity carried in Core's ordinary text field.
///
/// Unlike a Temporal identifier, an identity may be empty, but it still must
/// be safe to replay through the semantic JSON document.  Rust `&str` values
/// are UTF-8 by construction; retaining the explicit check beside the OCaml
/// validator makes that invariant visible at this bilateral boundary and keeps
/// the rule intact if this conversion is later changed to accept raw bytes.
fn signal_identity(value: &str, path: &str) -> Result<(), ProtocolError> {
    bounded_text(value, path)?;
    if value.as_bytes().contains(&0) {
        return Err(ProtocolError::invalid(
            path,
            "signal identity must not contain NUL",
        ));
    }
    if std::str::from_utf8(value.as_bytes()).is_err() {
        return Err(ProtocolError::invalid(
            path,
            "signal identity must be valid UTF-8",
        ));
    }
    Ok(())
}

/// Validates exact protobuf time component ranges.
pub(crate) fn validate_time(
    seconds: i64,
    nanos: i32,
    duration: bool,
    path: &str,
) -> Result<(), ProtocolError> {
    if !(0..=999_999_999).contains(&nanos) {
        return Err(ProtocolError::invalid(
            path,
            "nanoseconds are outside protobuf range",
        ));
    }
    if duration && seconds < 0 {
        return Err(ProtocolError::invalid(
            path,
            "duration must not be negative",
        ));
    }
    Ok(())
}

/// Parses the canonical unsigned decimal used for exact floating-point bits.
fn validate_uint64_decimal(value: &str, path: &str) -> Result<u64, ProtocolError> {
    if value.is_empty() || (value.len() > 1 && value.starts_with('0')) {
        return Err(ProtocolError::invalid(
            path,
            "value is not canonical unsigned 64-bit decimal",
        ));
    }
    let parsed = value.parse::<u64>().map_err(|_| {
        ProtocolError::invalid(path, "value is not canonical unsigned 64-bit decimal")
    })?;
    if parsed.to_string() != value {
        return Err(ProtocolError::invalid(
            path,
            "value is not canonical unsigned 64-bit decimal",
        ));
    }
    Ok(parsed)
}

/// Validates the retry policy constraints shared by the OCaml and Rust
/// semantic layers before a command can reach Temporal Core.
fn validate_retry_policy(value: &RetryPolicy, path: &str) -> Result<(), ProtocolError> {
    validate_time(
        value.initial_interval.seconds,
        value.initial_interval.nanoseconds,
        true,
        &format!("{path}.initial_interval"),
    )?;
    validate_time(
        value.maximum_interval.seconds,
        value.maximum_interval.nanoseconds,
        true,
        &format!("{path}.maximum_interval"),
    )?;
    let initial_is_zero =
        value.initial_interval.seconds == 0 && value.initial_interval.nanoseconds == 0;
    if initial_is_zero {
        return Err(ProtocolError::invalid(
            format!("{path}.initial_interval"),
            "duration must be positive",
        ));
    }
    if (
        value.maximum_interval.seconds,
        value.maximum_interval.nanoseconds,
    ) < (
        value.initial_interval.seconds,
        value.initial_interval.nanoseconds,
    ) {
        return Err(ProtocolError::invalid(
            format!("{path}.maximum_interval"),
            "maximum interval must be at least initial interval",
        ));
    }
    let bits = validate_uint64_decimal(
        &value.backoff_coefficient_bits,
        &format!("{path}.backoff_coefficient_bits"),
    )?;
    let coefficient = f64::from_bits(bits);
    if !coefficient.is_finite() {
        return Err(ProtocolError::invalid(
            format!("{path}.backoff_coefficient_bits"),
            "backoff coefficient must be finite",
        ));
    }
    if coefficient < 1.0 {
        return Err(ProtocolError::invalid(
            format!("{path}.backoff_coefficient_bits"),
            "backoff coefficient must be at least 1.0",
        ));
    }
    if value.maximum_attempts < 0 {
        return Err(ProtocolError::invalid(
            format!("{path}.maximum_attempts"),
            "maximum attempts must not be negative",
        ));
    }
    for (index, error_type) in value.non_retryable_error_types.iter().enumerate() {
        let item_path = format!("{path}.non_retryable_error_types[{index}]");
        bounded_text(error_type, &item_path)?;
        if error_type.is_empty() || error_type.as_bytes().contains(&0) {
            return Err(ProtocolError::invalid(
                &item_path,
                "error type must be non-empty and must not contain NUL",
            ));
        }
    }
    Ok(())
}

/// Validates failure detail payloads before JSON serialization allocates a
/// document. This gives timeout heartbeat details the same limits as
/// application and cancellation details.
fn validate_failure_payloads(values: &[Payload], path: &str) -> Result<(), ProtocolError> {
    for (index, payload) in values.iter().enumerate() {
        let payload_path = format!("{path}[{index}]");
        if payload.data.len() > MAX_PAYLOAD_BYTES {
            return Err(ProtocolError::invalid(
                &payload_path,
                "payload exceeds the byte limit",
            ));
        }
        for (key, value) in &payload.metadata {
            if key.is_empty() || key.len() > MAX_STRING_BYTES {
                return Err(ProtocolError::invalid(
                    &payload_path,
                    "payload metadata key is outside protocol limits",
                ));
            }
            if value.len() > MAX_PAYLOAD_BYTES {
                return Err(ProtocolError::invalid(
                    &payload_path,
                    "payload metadata value exceeds the byte limit",
                ));
            }
        }
    }
    Ok(())
}

/// Validates failure identifiers recursively.
pub(crate) fn validate_failure(value: &Failure, path: &str) -> Result<(), ProtocolError> {
    bounded_text(&value.message, path)?;
    bounded_text(&value.source, path)?;
    bounded_text(&value.stack_trace, path)?;
    match &value.info {
        FailureInfo::Application {
            type_name, details, ..
        } => {
            bounded_text(type_name, path)?;
            validate_failure_payloads(details, &format!("{path}.details"))?;
        }
        FailureInfo::Canceled { identity, details } => {
            bounded_text(identity, path)?;
            validate_failure_payloads(details, &format!("{path}.details"))?;
        }
        FailureInfo::Activity {
            scheduled_event_id,
            started_event_id,
            identity,
            activity_type,
            activity_id,
            retry_state: _,
        } => {
            if *scheduled_event_id < 0 || *started_event_id < 0 {
                return Err(ProtocolError::invalid(
                    path,
                    "activity failure event IDs must not be negative",
                ));
            }
            bounded_text(identity, path)?;
            identifier(activity_type, path)?;
            identifier(activity_id, path)?;
        }
        FailureInfo::ChildWorkflow {
            namespace,
            workflow_id,
            run_id,
            workflow_type,
            initiated_event_id,
            started_event_id,
            retry_state: _,
        } => {
            identifier(namespace, path)?;
            identifier(workflow_id, path)?;
            identifier(workflow_type, path)?;
            if *initiated_event_id < 0 || *started_event_id < 0 {
                return Err(ProtocolError::invalid(
                    path,
                    "child workflow failure event IDs must not be negative",
                ));
            }
            validate_child_failure_run_id(run_id, *started_event_id, path)?;
        }
        FailureInfo::Timeout {
            last_heartbeat_details,
            ..
        } => validate_failure_payloads(
            last_heartbeat_details,
            &format!("{path}.last_heartbeat_details"),
        )?,
    }
    if let Some(cause) = &value.cause {
        validate_failure(cause, path)?;
    }
    Ok(())
}

/// Validates the execution ID carried by a child-workflow failure.
///
/// Temporal Core emits a child failure while the child start is still in
/// flight when a parent cancels it before `ChildWorkflowExecutionStarted`.
/// In that narrow state Core has no concrete run ID yet and therefore emits
/// an empty string together with `started_event_id == 0`.  The empty value is
/// meaningful protocol state, not a missing field, so the bridge preserves it
/// while retaining the ordinary identifier checks once the child has started.
fn validate_child_failure_run_id(
    run_id: &str,
    started_event_id: i64,
    path: &str,
) -> Result<(), ProtocolError> {
    if run_id.is_empty() {
        if started_event_id == 0 {
            Ok(())
        } else {
            Err(ProtocolError::invalid(
                path,
                "child failure run_id may be empty only before the child starts",
            ))
        }
    } else {
        identifier(run_id, path)
    }
}

/// Applies invariants that derive-based closed-shape validation cannot express.
fn validate_activation(value: &Activation) -> Result<(), ProtocolError> {
    identifier(&value.run_id, "$.run_id")?;
    let eviction_count = value
        .jobs
        .iter()
        .filter(|job| matches!(job, ActivationJob::RemoveFromCache { .. }))
        .count();
    if eviction_count != 0 && (eviction_count != 1 || value.jobs.len() != 1) {
        return Err(ProtocolError::invalid(
            "$.jobs",
            "cache eviction must be the activation's only job",
        ));
    }
    match value.timestamp {
        Some(timestamp) => validate_time(
            timestamp.seconds,
            timestamp.nanoseconds,
            false,
            "$.timestamp",
        )?,
        None if eviction_count == 1 => {}
        None => {
            return Err(ProtocolError::invalid(
                "$.timestamp",
                "timestamp may be null only for cache eviction",
            ));
        }
    }
    let initialize_count = value
        .jobs
        .iter()
        .filter(|job| matches!(job, ActivationJob::InitializeWorkflow { .. }))
        .count();
    if initialize_count > 1
        || (initialize_count == 1
            && !matches!(
                value.jobs.first(),
                Some(ActivationJob::InitializeWorkflow { .. })
            ))
    {
        return Err(ProtocolError::invalid(
            "$.jobs",
            "initialize_workflow must occur at most once and first",
        ));
    }
    let query_count = value
        .jobs
        .iter()
        .filter(|job| matches!(job, ActivationJob::QueryWorkflow { .. }))
        .count();
    // Query activations are intentionally isolated by Core. Rejecting a
    // mixed activation here prevents a malformed bridge document from
    // running ordinary workflow work while a query is expected to be
    // answered synchronously.
    if query_count != 0 && query_count != value.jobs.len() {
        return Err(ProtocolError::invalid(
            "$.jobs",
            "query_workflow jobs must be the activation's only jobs",
        ));
    }
    let mut query_ids = BTreeSet::new();
    for job in &value.jobs {
        if let ActivationJob::QueryWorkflow { query_id, .. } = job
            && !query_ids.insert(query_id)
        {
            return Err(ProtocolError::invalid(
                "$.jobs.query_id",
                "query identifiers must be unique within one activation",
            ));
        }
    }
    let mut update_ids = BTreeSet::new();
    let mut update_protocol_ids = BTreeSet::new();
    for job in &value.jobs {
        match job {
            ActivationJob::InitializeWorkflow {
                workflow_id,
                workflow_type,
                randomness_seed,
                attempt,
                context,
                ..
            } => {
                identifier(workflow_id, "$.jobs.workflow_id")?;
                identifier(workflow_type, "$.jobs.workflow_type")?;
                if *attempt < 1 {
                    return Err(ProtocolError::invalid(
                        "$.jobs.attempt",
                        "attempt must be positive",
                    ));
                }
                let parsed = randomness_seed.parse::<u64>().map_err(|_| {
                    ProtocolError::invalid(
                        "$.jobs.randomness_seed",
                        "value is not canonical unsigned 64-bit decimal",
                    )
                })?;
                if parsed.to_string() != *randomness_seed {
                    return Err(ProtocolError::invalid(
                        "$.jobs.randomness_seed",
                        "value is not canonical unsigned 64-bit decimal",
                    ));
                }
                if let Some(context) = context {
                    for key in context.headers.keys() {
                        identifier(key, "$.jobs.context.headers")?;
                    }
                    for (name, fields) in [
                        ("memo", &context.memo),
                        ("search_attributes", &context.search_attributes),
                    ] {
                        if let Some(fields) = fields {
                            for key in fields.keys() {
                                identifier(key, &format!("$.jobs.context.{name}"))?;
                            }
                        }
                    }
                    if let Some(expiration) = context.workflow_execution_expiration_time {
                        validate_time(
                            expiration.seconds,
                            expiration.nanoseconds,
                            false,
                            "$.jobs.context.workflow_execution_expiration_time",
                        )?;
                    }
                    if let Some(backoff) = context.first_workflow_task_backoff {
                        validate_time(
                            backoff.seconds,
                            backoff.nanoseconds,
                            true,
                            "$.jobs.context.first_workflow_task_backoff",
                        )?;
                    }
                    bounded_text(&context.identity, "$.jobs.context.identity")?;
                    if let Some(parent) = &context.parent_workflow {
                        identifier(
                            &parent.namespace,
                            "$.jobs.context.parent_workflow.namespace",
                        )?;
                        identifier(
                            &parent.workflow_id,
                            "$.jobs.context.parent_workflow.workflow_id",
                        )?;
                        identifier(&parent.run_id, "$.jobs.context.parent_workflow.run_id")?;
                    }
                    identifier(
                        &context.first_execution_run_id,
                        "$.jobs.context.first_execution_run_id",
                    )?;
                    for duration in [
                        context.workflow_execution_timeout.as_ref(),
                        context.workflow_run_timeout.as_ref(),
                        context.workflow_task_timeout.as_ref(),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        validate_time(
                            duration.seconds,
                            duration.nanoseconds,
                            true,
                            "$.jobs.context.timeout",
                        )?;
                    }
                    if let Some(start_time) = context.start_time {
                        validate_time(
                            start_time.seconds,
                            start_time.nanoseconds,
                            false,
                            "$.jobs.context.start_time",
                        )?;
                    }
                    if let Some(root) = &context.root_workflow {
                        identifier(
                            &root.workflow_id,
                            "$.jobs.context.root_workflow.workflow_id",
                        )?;
                        identifier(&root.run_id, "$.jobs.context.root_workflow.run_id")?;
                    }
                    if let Some(priority) = &context.priority {
                        validate_priority(priority, "$.jobs.context.priority")?;
                    }
                    if let Some(retry_policy) = &context.retry_policy {
                        validate_retry_policy(retry_policy, "$.jobs.context.retry_policy")?;
                    }
                    if let Some(continuation) = &context.continuation {
                        identifier(
                            &continuation.continued_from_execution_run_id,
                            "$.jobs.context.continuation.continued_from_execution_run_id",
                        )?;
                        if let Some(failure) = &continuation.continued_failure {
                            validate_failure(
                                failure,
                                "$.jobs.context.continuation.continued_failure",
                            )?;
                        }
                    }
                }
            }
            ActivationJob::ResolveActivity { result, .. } => match result {
                ActivityResolution::Completed { .. } => {}
                ActivityResolution::Failed { failure }
                | ActivityResolution::Cancelled { failure } => {
                    validate_failure(failure, "$.jobs.result.failure")?
                }
                ActivityResolution::Backoff {
                    attempt,
                    backoff_duration,
                    original_schedule_time,
                } => {
                    if *attempt == 0 {
                        return Err(ProtocolError::invalid(
                            "$.jobs.result.attempt",
                            "local activity backoff attempt must be positive",
                        ));
                    }
                    validate_time(
                        backoff_duration.seconds,
                        backoff_duration.nanoseconds,
                        true,
                        "$.jobs.result.backoff_duration",
                    )?;
                    if let Some(timestamp) = original_schedule_time {
                        validate_time(
                            timestamp.seconds,
                            timestamp.nanoseconds,
                            false,
                            "$.jobs.result.original_schedule_time",
                        )?;
                    }
                }
            },
            ActivationJob::ResolveChildWorkflowStart { result, .. } => match result {
                ChildWorkflowStartResolution::Succeeded { run_id } => {
                    identifier(run_id, "$.jobs.result.run_id")?
                }
                ChildWorkflowStartResolution::Failed {
                    workflow_id,
                    workflow_type,
                    ..
                } => {
                    identifier(workflow_id, "$.jobs.result.workflow_id")?;
                    identifier(workflow_type, "$.jobs.result.workflow_type")?;
                }
                ChildWorkflowStartResolution::Cancelled { failure } => {
                    validate_failure(failure, "$.jobs.result.failure")?
                }
            },
            ActivationJob::ResolveChildWorkflow { result, .. } => match result {
                ChildWorkflowResolution::Completed { .. } => {}
                ChildWorkflowResolution::Failed { failure }
                | ChildWorkflowResolution::Cancelled { failure } => {
                    validate_failure(failure, "$.jobs.result.failure")?
                }
            },
            ActivationJob::ResolveSignalExternalWorkflow { result, .. }
            | ActivationJob::ResolveRequestCancelExternalWorkflow { result, .. } => {
                if let ExternalWorkflowResolution::Failed { failure } = result {
                    validate_failure(failure, "$.jobs.result.failure")?
                }
            }
            ActivationJob::SignalWorkflow {
                signal_name,
                identity,
                headers,
                ..
            } => {
                identifier(signal_name, "$.jobs.signal_name")?;
                signal_identity(identity, "$.jobs.identity")?;
                for key in headers.keys() {
                    identifier(key, "$.jobs.headers")?;
                }
            }
            ActivationJob::QueryWorkflow {
                query_id,
                query_type,
                headers,
                ..
            } => {
                identifier(query_id, "$.jobs.query_id")?;
                identifier(query_type, "$.jobs.query_type")?;
                for key in headers.keys() {
                    identifier(key, "$.jobs.headers")?;
                }
            }
            ActivationJob::DoUpdate {
                id,
                protocol_instance_id,
                name,
                headers,
                meta,
                ..
            } => {
                identifier(id, "$.jobs.id")?;
                identifier(protocol_instance_id, "$.jobs.protocol_instance_id")?;
                identifier(name, "$.jobs.name")?;
                if !update_ids.insert(id) {
                    return Err(ProtocolError::invalid(
                        "$.jobs.id",
                        "update identifiers must be unique within one activation",
                    ));
                }
                if !update_protocol_ids.insert(protocol_instance_id) {
                    return Err(ProtocolError::invalid(
                        "$.jobs.protocol_instance_id",
                        "update protocol identifiers must be unique within one activation",
                    ));
                }
                if meta.update_id.as_str() != id.as_str() {
                    return Err(ProtocolError::invalid(
                        "$.jobs.meta.update_id",
                        "update metadata ID must match the update ID",
                    ));
                }
                identifier(&meta.update_id, "$.jobs.meta.update_id")?;
                bounded_text(&meta.identity, "$.jobs.meta.identity")?;
                for key in headers.keys() {
                    identifier(key, "$.jobs.headers")?;
                }
            }
            ActivationJob::NotifyHasPatch { patch_id } => {
                identifier(patch_id, "$.jobs.patch_id")?;
            }
            ActivationJob::CancelWorkflow { reason } => bounded_text(reason, "$.jobs.reason")?,
            ActivationJob::RemoveFromCache { message, .. } => {
                bounded_text(message, "$.jobs.message")?
            }
            ActivationJob::FireTimer { .. } => {}
        }
    }
    if let Some(metadata) = &value.metadata {
        let parsed = metadata.history_size_bytes.parse::<u64>().map_err(|_| {
            ProtocolError::invalid(
                "$.metadata.history_size_bytes",
                "value is not canonical unsigned 64-bit decimal",
            )
        })?;
        if parsed.to_string() != metadata.history_size_bytes {
            return Err(ProtocolError::invalid(
                "$.metadata.history_size_bytes",
                "value is not canonical unsigned 64-bit decimal",
            ));
        }
        bounded_text(&metadata.last_sdk_version, "$.metadata.last_sdk_version")?;
        if let Some(deployment) = &metadata.deployment_version_for_current_task {
            bounded_text(&deployment.deployment_name, "$.metadata.deployment_name")?;
            identifier(&deployment.build_id, "$.metadata.build_id")?;
        }
    }
    Ok(())
}

/// Applies completion-wide ordering, identity, and cross-command invariants.
fn validate_completion(value: &Completion) -> Result<(), ProtocolError> {
    identifier(&value.run_id, "$.run_id")?;
    // Core retains the first patch-marker command for an ID. Tracking modes
    // while validating prevents a private caller from making durable
    // deprecation depend silently on command order. Same-mode repetitions are
    // intentionally preserved for Core to deduplicate.
    let mut patch_modes: BTreeMap<&str, bool> = BTreeMap::new();
    for (index, command) in value.commands.iter().enumerate() {
        let terminal = matches!(
            command,
            CompletionCommand::CompleteWorkflow { .. }
                | CompletionCommand::FailWorkflow { .. }
                | CompletionCommand::ContinueAsNew { .. }
                | CompletionCommand::CancelWorkflow
        );
        if terminal && index + 1 != value.commands.len() {
            return Err(ProtocolError::invalid(
                "$.commands",
                "terminal workflow command must be last",
            ));
        }
        match command {
            CompletionCommand::ScheduleActivity {
                activity_id,
                activity_type,
                task_queue,
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
                retry_policy,
                ..
            } => {
                identifier(activity_id, "$.commands.activity_id")?;
                identifier(activity_type, "$.commands.activity_type")?;
                identifier(task_queue, "$.commands.task_queue")?;
                if schedule_to_close_timeout.is_none() && start_to_close_timeout.is_none() {
                    return Err(ProtocolError::invalid(
                        "$.commands",
                        "activity requires schedule-to-close or start-to-close timeout",
                    ));
                }
                for duration in [
                    schedule_to_close_timeout,
                    schedule_to_start_timeout,
                    start_to_close_timeout,
                    heartbeat_timeout,
                ]
                .into_iter()
                .flatten()
                {
                    validate_time(
                        duration.seconds,
                        duration.nanoseconds,
                        true,
                        "$.commands.timeout",
                    )?;
                }
                if let Some(retry_policy) = retry_policy {
                    validate_retry_policy(retry_policy, "$.commands.retry_policy")?;
                }
                if let Some(priority) = command_priority(command) {
                    validate_priority(priority, "$.commands.priority")?;
                }
            }
            CompletionCommand::ScheduleLocalActivity {
                activity_id,
                activity_type,
                attempt,
                original_schedule_time,
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                retry_policy,
                local_retry_threshold,
                ..
            } => {
                identifier(activity_id, "$.commands.activity_id")?;
                identifier(activity_type, "$.commands.activity_type")?;
                if *attempt == 0 {
                    return Err(ProtocolError::invalid(
                        "$.commands.attempt",
                        "local activity attempt must be positive",
                    ));
                }
                if schedule_to_close_timeout.is_none() && start_to_close_timeout.is_none() {
                    return Err(ProtocolError::invalid(
                        "$.commands",
                        "local activity requires schedule-to-close or start-to-close timeout",
                    ));
                }
                for duration in [
                    schedule_to_close_timeout,
                    schedule_to_start_timeout,
                    start_to_close_timeout,
                    local_retry_threshold,
                ]
                .into_iter()
                .flatten()
                {
                    validate_time(
                        duration.seconds,
                        duration.nanoseconds,
                        true,
                        "$.commands.timeout",
                    )?;
                }
                if let Some(timestamp) = original_schedule_time {
                    validate_time(
                        timestamp.seconds,
                        timestamp.nanoseconds,
                        false,
                        "$.commands.original_schedule_time",
                    )?;
                }
                if let Some(retry_policy) = retry_policy {
                    validate_retry_policy(retry_policy, "$.commands.retry_policy")?;
                }
            }
            CompletionCommand::StartChildWorkflow {
                workflow_id,
                workflow_type,
                retry_policy,
                ..
            } => {
                identifier(workflow_id, "$.commands.workflow_id")?;
                identifier(workflow_type, "$.commands.workflow_type")?;
                if let Some(retry_policy) = retry_policy {
                    validate_retry_policy(retry_policy, "$.commands.retry_policy")?;
                }
            }
            CompletionCommand::SignalExternalWorkflow {
                workflow_id,
                run_id,
                signal_name,
                input,
                headers,
                ..
            } => {
                identifier(workflow_id, "$.commands.workflow_id")?;
                if !run_id.is_empty() {
                    identifier(run_id, "$.commands.run_id")?;
                }
                identifier(signal_name, "$.commands.signal_name")?;
                for payload in input {
                    validate_failure_payloads(std::slice::from_ref(payload), "$.commands.input")?;
                }
                for (key, payload) in headers {
                    identifier(key, "$.commands.headers")?;
                    validate_failure_payloads(std::slice::from_ref(payload), "$.commands.headers")?;
                }
            }
            CompletionCommand::RequestCancelExternalWorkflow {
                workflow_id,
                run_id,
                reason,
                ..
            } => {
                identifier(workflow_id, "$.commands.workflow_id")?;
                if !run_id.is_empty() {
                    identifier(run_id, "$.commands.run_id")?;
                }
                if reason.is_empty() {
                    return Err(ProtocolError::invalid(
                        "$.commands.reason",
                        "reason must not be empty",
                    ));
                }
                if reason.as_bytes().contains(&0) {
                    return Err(ProtocolError::invalid(
                        "$.commands.reason",
                        "reason must not contain NUL",
                    ));
                }
                bounded_text(reason, "$.commands.reason")?;
            }
            CompletionCommand::CancelChildWorkflow { reason, .. } => {
                if reason.is_empty() {
                    return Err(ProtocolError::invalid(
                        "$.commands.reason",
                        "reason must not be empty",
                    ));
                }
                if reason.as_bytes().contains(&0) {
                    return Err(ProtocolError::invalid(
                        "$.commands.reason",
                        "reason must not contain NUL",
                    ));
                }
                bounded_text(reason, "$.commands.reason")?;
                if std::str::from_utf8(reason.as_bytes()).is_err() {
                    return Err(ProtocolError::invalid(
                        "$.commands.reason",
                        "reason must be valid UTF-8",
                    ));
                }
            }
            CompletionCommand::RequestCancelActivity { .. }
            | CompletionCommand::RequestCancelLocalActivity { .. } => {}
            CompletionCommand::ContinueAsNew { workflow_type, .. } => {
                identifier(workflow_type, "$.commands.workflow_type")?;
            }
            CompletionCommand::StartTimer {
                start_to_fire_timeout,
                ..
            } => validate_time(
                start_to_fire_timeout.seconds,
                start_to_fire_timeout.nanoseconds,
                true,
                "$.commands.start_to_fire_timeout",
            )?,
            CompletionCommand::FailWorkflow { failure } => {
                validate_failure(failure, "$.commands.failure")?
            }
            CompletionCommand::QueryResult { query_id, result } => {
                identifier(query_id, "$.commands.query_id")?;
                match result {
                    QueryResult::Succeeded { payload } => {
                        // The semantic shape uses a required payload even
                        // though the protobuf field is optional. This avoids
                        // silently turning a successful query into null.
                        let _ = payload;
                    }
                    QueryResult::Failed { failure } => {
                        validate_failure(failure, "$.commands.result.failure")?
                    }
                }
            }
            CompletionCommand::UpdateResponse {
                protocol_instance_id,
                response,
            } => {
                identifier(protocol_instance_id, "$.commands.protocol_instance_id")?;
                match response {
                    UpdateResponseResult::Accepted => {}
                    UpdateResponseResult::Rejected { failure } => {
                        validate_failure(failure, "$.commands.response.failure")?
                    }
                    UpdateResponseResult::Completed { payload } => {
                        if payload.data.len() > MAX_PAYLOAD_BYTES {
                            return Err(ProtocolError::invalid(
                                "$.commands.response.payload",
                                "payload exceeds the byte limit",
                            ));
                        }
                    }
                }
            }
            CompletionCommand::SetPatchMarker {
                patch_id,
                deprecated,
            } => {
                identifier(patch_id, "$.commands.patch_id")?;
                if matches!(
                    patch_modes.insert(patch_id.as_str(), *deprecated),
                    Some(existing) if existing != *deprecated
                ) {
                    return Err(ProtocolError::invalid(
                        "$.commands.patch_id",
                        "one patch ID cannot use both active and deprecated marker modes",
                    ));
                }
            }
            CompletionCommand::UpsertSearchAttributes { search_attributes } => {
                for (key, payload) in search_attributes {
                    identifier(key, "$.commands.search_attributes")?;
                    if payload.data.len() > MAX_PAYLOAD_BYTES
                        || payload
                            .metadata
                            .values()
                            .any(|value| value.len() > MAX_PAYLOAD_BYTES)
                    {
                        return Err(ProtocolError::invalid(
                            "$.commands.search_attributes",
                            "payload exceeds the byte limit",
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    // An immediate update completion is represented by two commands with the
    // same protocol ID: acceptance followed by the final value. A later
    // activation may contain only the final command, after Core has recorded
    // acceptance in history. Reject duplicate phases while allowing that
    // documented two-phase shape.
    let mut update_phases: BTreeMap<&str, (bool, bool)> = BTreeMap::new();
    for command in &value.commands {
        let CompletionCommand::UpdateResponse {
            protocol_instance_id,
            response,
        } = command
        else {
            continue;
        };
        let phases = update_phases
            .entry(protocol_instance_id.as_str())
            .or_insert((false, false));
        match response {
            UpdateResponseResult::Accepted => {
                if phases.0 || phases.1 {
                    return Err(ProtocolError::invalid(
                        "$.commands.protocol_instance_id",
                        "update acceptance must be the first response and may appear once",
                    ));
                }
                phases.0 = true;
            }
            UpdateResponseResult::Rejected { .. } => {
                if phases.1 {
                    return Err(ProtocolError::invalid(
                        "$.commands.protocol_instance_id",
                        "update rejection may appear once and must be terminal",
                    ));
                }
                phases.1 = true;
            }
            UpdateResponseResult::Completed { .. } => {
                // A completed-only response is valid after a prior acceptance
                // was persisted by Core; an accepted+completed pair is also
                // valid in one activation. Track the terminal phase so a
                // second completion cannot be emitted accidentally.
                if phases.1 {
                    return Err(ProtocolError::invalid(
                        "$.commands.protocol_instance_id",
                        "update completion may appear once",
                    ));
                }
                phases.1 = true;
            }
        }
    }
    Ok(())
}

/// Returns the priority attached to a command without widening the command
/// match above.  Keeping this helper separate makes the validation rule easy
/// to audit when more priority-bearing command kinds are added.
fn command_priority(command: &CompletionCommand) -> Option<&WorkflowPriority> {
    match command {
        CompletionCommand::ScheduleActivity { priority, .. } => priority.as_ref(),
        _ => None,
    }
}

/// Validates the closed priority representation used by both OCaml and Rust.
fn validate_priority(value: &WorkflowPriority, path: &str) -> Result<(), ProtocolError> {
    if value.priority_key < 0 {
        return Err(ProtocolError::invalid(
            path,
            "priority key cannot be negative",
        ));
    }
    if value.fairness_key.len() > 64 {
        return Err(ProtocolError::invalid(
            path,
            "fairness key exceeds Core's 64-byte limit",
        ));
    }
    if value.fairness_key.as_bytes().contains(&0) {
        return Err(ProtocolError::invalid(
            path,
            "fairness key must not contain NUL",
        ));
    }
    let weight = f32::from_bits(value.fairness_weight_bits);
    if !weight.is_finite() || (weight != 0.0 && !(0.001..=1000.0).contains(&weight)) {
        return Err(ProtocolError::invalid(
            path,
            "fairness weight must be zero or finite in [0.001, 1000]",
        ));
    }
    Ok(())
}

/// Strictly decodes one workflow activation.
pub fn decode_activation(input: &str) -> Result<Activation, ProtocolError> {
    let value = protocol::decode_payload_object(input)?;
    let activation: Activation = serde_json::from_value(to_serde(value))
        .map_err(|_| ProtocolError::invalid("$", "invalid workflow activation semantics"))?;
    validate_activation(&activation)?;
    Ok(activation)
}

/// Validates, normalizes, and reparses one outgoing workflow activation.
pub fn encode_activation(value: &Activation) -> Result<String, ProtocolError> {
    validate_activation(value)?;
    let json = serde_json::to_value(value)
        .map_err(|_| ProtocolError::invalid("$", "could not encode workflow activation"))?;
    let output = protocol::encode_payload_object(&from_serde(json)?)?;
    decode_activation(&output)?;
    Ok(output)
}

/// Strictly decodes one successful workflow completion.
pub fn decode_completion(input: &str) -> Result<Completion, ProtocolError> {
    let value = protocol::decode_payload_object(input)?;
    let completion: Completion = serde_json::from_value(to_serde(value))
        .map_err(|_| ProtocolError::invalid("$", "invalid workflow completion semantics"))?;
    validate_completion(&completion)?;
    Ok(completion)
}

/// Validates, normalizes, and reparses one outgoing workflow completion.
pub fn encode_completion(value: &Completion) -> Result<String, ProtocolError> {
    validate_completion(value)?;
    let json = serde_json::to_value(value)
        .map_err(|_| ProtocolError::invalid("$", "could not encode workflow completion"))?;
    let output = protocol::encode_payload_object(&from_serde(json)?)?;
    decode_completion(&output)?;
    Ok(output)
}

/// Why an official Core protobuf could not be represented losslessly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreConversionErrorCode {
    /// The first protocol slice deliberately does not represent this variant or option.
    Unsupported,
    /// A required protobuf oneof or nested message was absent or malformed.
    InvalidCore,
}

/// Privacy-safe error returned at the Rust-only protobuf boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreConversionError {
    /// Stable category suitable for branching in bridge code.
    pub code: CoreConversionErrorCode,
    /// Static diagnostic that never includes user payload data.
    pub message: &'static str,
}

/// Constructs an unsupported-conversion error without copying source values.
pub(crate) fn unsupported(message: &'static str) -> CoreConversionError {
    CoreConversionError {
        code: CoreConversionErrorCode::Unsupported,
        message,
    }
}

/// Constructs a malformed-Core error without copying source values.
pub(crate) fn invalid_core(message: &'static str) -> CoreConversionError {
    CoreConversionError {
        code: CoreConversionErrorCode::InvalidCore,
        message,
    }
}

use temporalio_protos::{
    coresdk::{
        activity_result as core_activity, child_workflow as core_child_workflow,
        common as core_common, workflow_activation as core_activation,
        workflow_commands as core_commands, workflow_completion as core_completion,
    },
    temporal::api::{common::v1 as api_common, enums::v1 as api_enums, failure::v1 as api_failure},
};

/// Copies one Core payload while rejecting external references this JSON slice cannot preserve.
pub(crate) fn payload_from_core(
    value: &api_common::Payload,
) -> Result<Payload, CoreConversionError> {
    if !value.external_payloads.is_empty() {
        return Err(unsupported("external payload references are not supported"));
    }
    if value.data.len() > MAX_PAYLOAD_BYTES
        || value
            .metadata
            .values()
            .any(|bytes| bytes.len() > MAX_PAYLOAD_BYTES)
    {
        return Err(invalid_core("Core payload exceeds the protocol byte limit"));
    }
    if value
        .metadata
        .keys()
        .any(|key| key.is_empty() || key.len() > MAX_STRING_BYTES)
    {
        return Err(invalid_core(
            "Core payload metadata key is outside protocol limits",
        ));
    }
    Ok(Payload {
        metadata: value
            .metadata
            .iter()
            .map(|(key, bytes)| (key.clone(), bytes.clone()))
            .collect(),
        data: value.data.clone(),
    })
}

/// Builds a Core payload whose allocation ownership is transferred into protobuf values.
pub(crate) fn payload_to_core(value: &Payload) -> Result<api_common::Payload, CoreConversionError> {
    if value.data.len() > MAX_PAYLOAD_BYTES
        || value
            .metadata
            .values()
            .any(|bytes| bytes.len() > MAX_PAYLOAD_BYTES)
    {
        return Err(invalid_core(
            "semantic payload exceeds the protocol byte limit",
        ));
    }
    if value
        .metadata
        .keys()
        .any(|key| key.is_empty() || key.len() > MAX_STRING_BYTES)
    {
        return Err(invalid_core(
            "semantic payload metadata key is outside protocol limits",
        ));
    }
    Ok(api_common::Payload {
        metadata: value
            .metadata
            .iter()
            .map(|(key, bytes)| (key.clone(), bytes.clone()))
            .collect(),
        data: value.data.clone(),
        external_payloads: Vec::new(),
    })
}

/// Converts optional Core payload collections while preserving order.
fn payloads_from_core(
    value: Option<&api_common::Payloads>,
) -> Result<Vec<Payload>, CoreConversionError> {
    value
        .map(|payloads| payloads.payloads.iter().map(payload_from_core).collect())
        .unwrap_or_else(|| Ok(Vec::new()))
}

/// Converts an optional Core payload collection without collapsing an absent
/// value and an explicitly empty completion result.
fn payloads_option_from_core(
    value: Option<&api_common::Payloads>,
) -> Result<Option<Vec<Payload>>, CoreConversionError> {
    value
        .map(|payloads| {
            payloads
                .payloads
                .iter()
                .map(payload_from_core)
                .collect::<Result<Vec<_>, _>>()
                .map(Some)
        })
        .unwrap_or(Ok(None))
}

/// Converts semantic payload collections while preserving order.
fn payloads_to_core(values: &[Payload]) -> Result<api_common::Payloads, CoreConversionError> {
    Ok(api_common::Payloads {
        payloads: values
            .iter()
            .map(payload_to_core)
            .collect::<Result<_, _>>()?,
    })
}

/// Maps the official retry enum without accepting future unknown integer values.
fn retry_state_from_core(value: i32) -> Result<RetryState, CoreConversionError> {
    use api_enums::RetryState as Core;
    Ok(
        match Core::try_from(value).map_err(|_| invalid_core("unknown Core retry state"))? {
            Core::Unspecified => RetryState::Unspecified,
            Core::InProgress => RetryState::InProgress,
            Core::NonRetryableFailure => RetryState::NonRetryableFailure,
            Core::Timeout => RetryState::Timeout,
            Core::MaximumAttemptsReached => RetryState::MaximumAttemptsReached,
            Core::RetryPolicyNotSet => RetryState::RetryPolicyNotSet,
            Core::InternalServerError => RetryState::InternalServerError,
            Core::CancelRequested => RetryState::CancelRequested,
        },
    )
}

/// Maps a semantic retry state to the exact official protobuf number.
fn retry_state_to_core(value: RetryState) -> i32 {
    use api_enums::RetryState as Core;
    (match value {
        RetryState::Unspecified => Core::Unspecified,
        RetryState::InProgress => Core::InProgress,
        RetryState::NonRetryableFailure => Core::NonRetryableFailure,
        RetryState::Timeout => Core::Timeout,
        RetryState::MaximumAttemptsReached => Core::MaximumAttemptsReached,
        RetryState::RetryPolicyNotSet => Core::RetryPolicyNotSet,
        RetryState::InternalServerError => Core::InternalServerError,
        RetryState::CancelRequested => Core::CancelRequested,
    }) as i32
}

/// Maps Core's timeout enum without accepting unknown values from a newer
/// protobuf revision. Rejecting an unknown integer is safer than silently
/// changing timeout semantics during replay.
fn timeout_type_from_core(value: i32) -> Result<TimeoutType, CoreConversionError> {
    use api_enums::TimeoutType as Core;
    Ok(
        match Core::try_from(value).map_err(|_| invalid_core("unknown Core timeout type"))? {
            Core::Unspecified => TimeoutType::Unspecified,
            Core::StartToClose => TimeoutType::StartToClose,
            Core::ScheduleToStart => TimeoutType::ScheduleToStart,
            Core::ScheduleToClose => TimeoutType::ScheduleToClose,
            Core::Heartbeat => TimeoutType::Heartbeat,
        },
    )
}

/// Converts the semantic timeout enum to the official protobuf number.
fn timeout_type_to_core(value: TimeoutType) -> i32 {
    use api_enums::TimeoutType as Core;
    (match value {
        TimeoutType::Unspecified => Core::Unspecified,
        TimeoutType::StartToClose => Core::StartToClose,
        TimeoutType::ScheduleToStart => Core::ScheduleToStart,
        TimeoutType::ScheduleToClose => Core::ScheduleToClose,
        TimeoutType::Heartbeat => Core::Heartbeat,
    }) as i32
}

/// Converts the supported recursive official failure subset.
pub(crate) fn failure_from_core(
    value: &api_failure::Failure,
) -> Result<Failure, CoreConversionError> {
    use api_failure::failure::FailureInfo as Core;
    let info = match value
        .failure_info
        .as_ref()
        .ok_or_else(|| invalid_core("Core failure info is absent"))?
    {
        Core::ApplicationFailureInfo(info) => {
            if info.next_retry_delay.is_some() || info.category != 0 {
                return Err(unsupported("application failure options are not supported"));
            }
            FailureInfo::Application {
                type_name: info.r#type.clone(),
                non_retryable: info.non_retryable,
                details: payloads_from_core(info.details.as_ref())?,
            }
        }
        Core::CanceledFailureInfo(info) => FailureInfo::Canceled {
            details: payloads_from_core(info.details.as_ref())?,
            identity: info.identity.clone(),
        },
        Core::ActivityFailureInfo(info) => FailureInfo::Activity {
            scheduled_event_id: info.scheduled_event_id,
            started_event_id: info.started_event_id,
            identity: info.identity.clone(),
            activity_type: info
                .activity_type
                .as_ref()
                .ok_or_else(|| invalid_core("Core activity failure type is absent"))?
                .name
                .clone(),
            activity_id: info.activity_id.clone(),
            retry_state: retry_state_from_core(info.retry_state)?,
        },
        Core::ChildWorkflowExecutionFailureInfo(info) => {
            let execution = info
                .workflow_execution
                .as_ref()
                .ok_or_else(|| invalid_core("Core child failure execution is absent"))?;
            let workflow_type = info
                .workflow_type
                .as_ref()
                .ok_or_else(|| invalid_core("Core child failure workflow type is absent"))?;
            FailureInfo::ChildWorkflow {
                namespace: info.namespace.clone(),
                workflow_id: execution.workflow_id.clone(),
                run_id: execution.run_id.clone(),
                workflow_type: workflow_type.name.clone(),
                initiated_event_id: info.initiated_event_id,
                started_event_id: info.started_event_id,
                retry_state: retry_state_from_core(info.retry_state)?,
            }
        }
        Core::TimeoutFailureInfo(info) => FailureInfo::Timeout {
            timeout_type: timeout_type_from_core(info.timeout_type)?,
            last_heartbeat_details: payloads_from_core(info.last_heartbeat_details.as_ref())?,
        },
        _ => return Err(unsupported("Core failure info kind is not supported")),
    };
    Ok(Failure {
        message: value.message.clone(),
        source: value.source.clone(),
        stack_trace: value.stack_trace.clone(),
        encoded_attributes: value
            .encoded_attributes
            .as_ref()
            .map(payload_from_core)
            .transpose()?,
        cause: value
            .cause
            .as_deref()
            .map(failure_from_core)
            .transpose()?
            .map(Box::new),
        info,
    })
}

/// Builds an official failure using only options represented in semantic JSON.
pub(crate) fn failure_to_core(
    value: &Failure,
) -> Result<api_failure::Failure, CoreConversionError> {
    use api_failure::failure::FailureInfo as Core;
    let failure_info = Some(match &value.info {
        FailureInfo::Application {
            type_name,
            non_retryable,
            details,
        } => Core::ApplicationFailureInfo(api_failure::ApplicationFailureInfo {
            r#type: type_name.clone(),
            non_retryable: *non_retryable,
            details: Some(payloads_to_core(details)?),
            next_retry_delay: None,
            category: 0,
        }),
        FailureInfo::Canceled { details, identity } => {
            Core::CanceledFailureInfo(api_failure::CanceledFailureInfo {
                details: Some(payloads_to_core(details)?),
                identity: identity.clone(),
            })
        }
        FailureInfo::Activity {
            scheduled_event_id,
            started_event_id,
            identity,
            activity_type,
            activity_id,
            retry_state,
        } => Core::ActivityFailureInfo(api_failure::ActivityFailureInfo {
            scheduled_event_id: *scheduled_event_id,
            started_event_id: *started_event_id,
            identity: identity.clone(),
            activity_type: Some(api_common::ActivityType {
                name: activity_type.clone(),
            }),
            activity_id: activity_id.clone(),
            retry_state: retry_state_to_core(*retry_state),
        }),
        FailureInfo::ChildWorkflow {
            namespace,
            workflow_id,
            run_id,
            workflow_type,
            initiated_event_id,
            started_event_id,
            retry_state,
        } => Core::ChildWorkflowExecutionFailureInfo(
            api_failure::ChildWorkflowExecutionFailureInfo {
                namespace: namespace.clone(),
                workflow_execution: Some(api_common::WorkflowExecution {
                    workflow_id: workflow_id.clone(),
                    run_id: run_id.clone(),
                }),
                workflow_type: Some(api_common::WorkflowType {
                    name: workflow_type.clone(),
                }),
                initiated_event_id: *initiated_event_id,
                started_event_id: *started_event_id,
                retry_state: retry_state_to_core(*retry_state),
            },
        ),
        FailureInfo::Timeout {
            timeout_type,
            last_heartbeat_details,
        } => Core::TimeoutFailureInfo(api_failure::TimeoutFailureInfo {
            timeout_type: timeout_type_to_core(*timeout_type),
            // Core treats an absent or empty heartbeat-details collection as
            // equivalent. Normalize an empty semantic list to the absent
            // protobuf field, matching Application/Canceled's existing list
            // convention while avoiding an unnecessary empty message.
            last_heartbeat_details: (!last_heartbeat_details.is_empty())
                .then(|| payloads_to_core(last_heartbeat_details))
                .transpose()?,
        }),
    });
    Ok(api_failure::Failure {
        message: value.message.clone(),
        source: value.source.clone(),
        stack_trace: value.stack_trace.clone(),
        encoded_attributes: value
            .encoded_attributes
            .as_ref()
            .map(payload_to_core)
            .transpose()?,
        cause: value
            .cause
            .as_deref()
            .map(failure_to_core)
            .transpose()?
            .map(Box::new),
        failure_info,
    })
}

/// Maps Core's continuation initiator enum without accepting future values.
fn continuation_initiator_from_core(
    value: i32,
) -> Result<ContinueAsNewInitiator, CoreConversionError> {
    use api_enums::ContinueAsNewInitiator as Core;
    match Core::try_from(value)
        .map_err(|_| invalid_core("unknown Core continue-as-new initiator"))?
    {
        Core::Unspecified => Ok(ContinueAsNewInitiator::Unspecified),
        Core::Workflow => Ok(ContinueAsNewInitiator::Workflow),
        Core::Retry => Ok(ContinueAsNewInitiator::Retry),
        Core::CronSchedule => Ok(ContinueAsNewInitiator::CronSchedule),
    }
}

/// Converts continuation metadata from Core while preserving each optional
/// failure and completion payload field. Core's ordinary initialization uses
/// an empty run ID, initiator zero, and absent optional values; only that exact
/// default is represented as [None] in semantic JSON.
fn continuation_from_core(
    value: &core_activation::InitializeWorkflow,
) -> Result<Option<Continuation>, CoreConversionError> {
    if value.continued_from_execution_run_id.is_empty()
        && value.continued_initiator == 0
        && value.continued_failure.is_none()
        && value.last_completion_result.is_none()
    {
        return Ok(None);
    }
    if value.continued_from_execution_run_id.is_empty()
        || value.continued_from_execution_run_id.len() > MAX_STRING_BYTES
    {
        return Err(invalid_core(
            "Core continuation run ID is empty or exceeds the protocol limit",
        ));
    }
    Ok(Some(Continuation {
        continued_from_execution_run_id: value.continued_from_execution_run_id.clone(),
        initiator: continuation_initiator_from_core(value.continued_initiator)?,
        continued_failure: value
            .continued_failure
            .as_ref()
            .map(failure_from_core)
            .transpose()?,
        last_completion_result: payloads_option_from_core(value.last_completion_result.as_ref())?,
    }))
}

/// Converts one supported Core activity resolution.
fn activity_resolution_from_core(
    value: &core_activity::ActivityResolution,
    is_local: bool,
) -> Result<ActivityResolution, CoreConversionError> {
    use core_activity::activity_resolution::Status;
    match value
        .status
        .as_ref()
        .ok_or_else(|| invalid_core("Core activity resolution status is absent"))?
    {
        Status::Completed(value) => Ok(ActivityResolution::Completed {
            payload: value.result.as_ref().map(payload_from_core).transpose()?,
        }),
        Status::Failed(value) => Ok(ActivityResolution::Failed {
            failure: failure_from_core(
                value
                    .failure
                    .as_ref()
                    .ok_or_else(|| invalid_core("Core failed activity has no failure"))?,
            )?,
        }),
        Status::Cancelled(value) => Ok(ActivityResolution::Cancelled {
            failure: failure_from_core(
                value
                    .failure
                    .as_ref()
                    .ok_or_else(|| invalid_core("Core cancelled activity has no failure"))?,
            )?,
        }),
        Status::Backoff(value) => {
            if !is_local {
                return Err(unsupported("remote activity backoff is not supported"));
            }
            if value.attempt == 0 {
                return Err(invalid_core("Core local activity backoff attempt is zero"));
            }
            let backoff_duration = value
                .backoff_duration
                .as_ref()
                .ok_or_else(|| invalid_core("Core local activity backoff duration is absent"))
                .and_then(duration_from_core)?;
            let original_schedule_time = value
                .original_schedule_time
                .as_ref()
                .map(|timestamp| {
                    validate_time(timestamp.seconds, timestamp.nanos, false, "$").map_err(
                        |_| invalid_core("Core local activity schedule timestamp is invalid"),
                    )?;
                    Ok(Timestamp {
                        seconds: timestamp.seconds,
                        nanoseconds: timestamp.nanos,
                    })
                })
                .transpose()?;
            Ok(ActivityResolution::Backoff {
                attempt: value.attempt,
                backoff_duration,
                original_schedule_time,
            })
        }
    }
}

/// Maps Core's child-start failure enum while rejecting numbers unknown to the
/// pinned Core revision instead of silently changing the failure meaning.
fn child_start_failure_cause_from_core(
    value: i32,
) -> Result<ChildWorkflowStartFailureCause, CoreConversionError> {
    use core_child_workflow::StartChildWorkflowExecutionFailedCause as Core;
    Ok(
        match Core::try_from(value)
            .map_err(|_| invalid_core("unknown Core child start failure cause"))?
        {
            Core::Unspecified => ChildWorkflowStartFailureCause::Unspecified,
            Core::WorkflowAlreadyExists => ChildWorkflowStartFailureCause::WorkflowAlreadyExists,
        },
    )
}

/// Converts the activation sent when Core accepts or rejects the start command.
fn child_workflow_start_resolution_from_core(
    value: &core_activation::ResolveChildWorkflowExecutionStart,
) -> Result<ChildWorkflowStartResolution, CoreConversionError> {
    use core_activation::resolve_child_workflow_execution_start::Status;
    match value
        .status
        .as_ref()
        .ok_or_else(|| invalid_core("Core child start resolution status is absent"))?
    {
        Status::Succeeded(value) => Ok(ChildWorkflowStartResolution::Succeeded {
            run_id: value.run_id.clone(),
        }),
        Status::Failed(value) => Ok(ChildWorkflowStartResolution::Failed {
            workflow_id: value.workflow_id.clone(),
            workflow_type: value.workflow_type.clone(),
            cause: child_start_failure_cause_from_core(value.cause)?,
        }),
        Status::Cancelled(value) => Ok(ChildWorkflowStartResolution::Cancelled {
            failure: failure_from_core(
                value
                    .failure
                    .as_ref()
                    .ok_or_else(|| invalid_core("Core cancelled child start has no failure"))?,
            )?,
        }),
    }
}

/// Converts a terminal child-workflow result without dropping payloads or the
/// recursive failure diagnostics constructed by Core.
fn child_workflow_resolution_from_core(
    value: &core_child_workflow::ChildWorkflowResult,
) -> Result<ChildWorkflowResolution, CoreConversionError> {
    use core_child_workflow::child_workflow_result::Status;
    match value
        .status
        .as_ref()
        .ok_or_else(|| invalid_core("Core child workflow result status is absent"))?
    {
        Status::Completed(value) => Ok(ChildWorkflowResolution::Completed {
            payload: value.result.as_ref().map(payload_from_core).transpose()?,
        }),
        Status::Failed(value) => Ok(ChildWorkflowResolution::Failed {
            failure: failure_from_core(
                value
                    .failure
                    .as_ref()
                    .ok_or_else(|| invalid_core("Core failed child workflow has no failure"))?,
            )?,
        }),
        Status::Cancelled(value) => {
            Ok(ChildWorkflowResolution::Cancelled {
                failure: failure_from_core(value.failure.as_ref().ok_or_else(|| {
                    invalid_core("Core cancelled child workflow has no failure")
                })?)?,
            })
        }
    }
}

/// Converts Core's nullable failure field for an external workflow command.
fn external_workflow_resolution_from_core(
    failure: Option<&api_failure::Failure>,
) -> Result<ExternalWorkflowResolution, CoreConversionError> {
    match failure {
        None => Ok(ExternalWorkflowResolution::Succeeded),
        Some(failure) => Ok(ExternalWorkflowResolution::Failed {
            failure: Box::new(failure_from_core(failure)?),
        }),
    }
}

/// Maps every official cache-eviction enum in the pinned Core revision.
fn eviction_reason_from_core(value: i32) -> Result<EvictionReason, CoreConversionError> {
    use core_activation::remove_from_cache::EvictionReason as Core;
    Ok(
        match Core::try_from(value).map_err(|_| invalid_core("unknown Core eviction reason"))? {
            Core::Unspecified => EvictionReason::Unspecified,
            Core::CacheFull => EvictionReason::CacheFull,
            Core::CacheMiss => EvictionReason::CacheMiss,
            Core::Nondeterminism => EvictionReason::Nondeterminism,
            Core::LangFail => EvictionReason::LangFail,
            Core::LangRequested => EvictionReason::LangRequested,
            Core::TaskNotFound => EvictionReason::TaskNotFound,
            Core::UnhandledCommand => EvictionReason::UnhandledCommand,
            Core::Fatal => EvictionReason::Fatal,
            Core::PaginationOrHistoryFetch => EvictionReason::PaginationOrHistoryFetch,
            Core::WorkflowExecutionEnding => EvictionReason::WorkflowExecutionEnding,
        },
    )
}

/// Rejects cron and root start-delay semantics. The server also inserts a
/// first-task backoff for rapid continue-as-new and retry runs; that already
/// applied duration is validated and retained rather than mistaken for cron.
fn validate_initialize_subset(
    value: &core_activation::InitializeWorkflow,
    continuation: Option<&Continuation>,
) -> Result<(), CoreConversionError> {
    if !value.cron_schedule.is_empty()
        || value.continued_initiator == api_enums::ContinueAsNewInitiator::CronSchedule as i32
    {
        return Err(unsupported(
            "cron schedules are not supported by the OCaml worker",
        ));
    }
    let supported_continuation = continuation.is_some_and(|value| {
        matches!(
            value.initiator,
            ContinueAsNewInitiator::Workflow | ContinueAsNewInitiator::Retry
        )
    });
    if !supported_continuation
        && value
            .cron_schedule_to_schedule_interval
            .as_ref()
            .is_some_and(|duration| duration.seconds != 0 || duration.nanos != 0)
    {
        return Err(unsupported(
            "root start delay is not supported by the OCaml worker",
        ));
    }
    Ok(())
}

/// Maps one pinned Core suggestion reason and rejects future enum numbers.
fn suggestion_from_core(value: i32) -> Result<SuggestContinueAsNewReason, CoreConversionError> {
    use api_enums::SuggestContinueAsNewReason as Core;
    Ok(
        match Core::try_from(value)
            .map_err(|_| invalid_core("unknown Core continue-as-new suggestion reason"))?
        {
            Core::Unspecified => SuggestContinueAsNewReason::Unspecified,
            Core::HistorySizeTooLarge => SuggestContinueAsNewReason::HistorySizeTooLarge,
            Core::TooManyHistoryEvents => SuggestContinueAsNewReason::TooManyHistoryEvents,
            Core::TooManyUpdates => SuggestContinueAsNewReason::TooManyUpdates,
        },
    )
}

/// Converts an official pinned-Core activation without silently dropping data.
pub fn activation_from_core(
    value: &core_activation::WorkflowActivation,
) -> Result<Activation, CoreConversionError> {
    use core_activation::workflow_activation_job::Variant;
    let jobs = value
        .jobs
        .iter()
        .map(|job| {
            match job
                .variant
                .as_ref()
                .ok_or_else(|| invalid_core("Core activation job variant is absent"))?
            {
                Variant::InitializeWorkflow(value) => {
                    let continuation = continuation_from_core(value)?;
                    validate_initialize_subset(value, continuation.as_ref())?;
                    Ok(ActivationJob::InitializeWorkflow {
                        workflow_id: value.workflow_id.clone(),
                        workflow_type: value.workflow_type.clone(),
                        arguments: value
                            .arguments
                            .iter()
                            .map(payload_from_core)
                            .collect::<Result<_, _>>()?,
                        randomness_seed: value.randomness_seed.to_string(),
                        attempt: value.attempt,
                        context: Some(Box::new(InitializeContext {
                            headers: value
                                .headers
                                .iter()
                                .map(|(key, payload)| {
                                    Ok((key.clone(), payload_from_core(payload)?))
                                })
                                .collect::<Result<_, CoreConversionError>>()?,
                            memo: value
                                .memo
                                .as_ref()
                                .map(|memo| {
                                    memo.fields
                                        .iter()
                                        .map(|(key, payload)| {
                                            Ok((key.clone(), payload_from_core(payload)?))
                                        })
                                        .collect::<Result<_, CoreConversionError>>()
                                })
                                .transpose()?,
                            search_attributes: value
                                .search_attributes
                                .as_ref()
                                .map(|attributes| {
                                    attributes
                                        .indexed_fields
                                        .iter()
                                        .map(|(key, payload)| {
                                            Ok((key.clone(), payload_from_core(payload)?))
                                        })
                                        .collect::<Result<_, CoreConversionError>>()
                                })
                                .transpose()?,
                            workflow_execution_expiration_time: value
                                .workflow_execution_expiration_time
                                .as_ref()
                                .map(|time| Timestamp {
                                    seconds: time.seconds,
                                    nanoseconds: time.nanos,
                                }),
                            first_workflow_task_backoff: value
                                .cron_schedule_to_schedule_interval
                                .as_ref()
                                .map(|time| Duration {
                                    seconds: time.seconds,
                                    nanoseconds: time.nanos,
                                }),
                            identity: value.identity.clone(),
                            parent_workflow: value.parent_workflow_info.as_ref().map(|parent| {
                                NamespacedWorkflowExecution {
                                    namespace: parent.namespace.clone(),
                                    workflow_id: parent.workflow_id.clone(),
                                    run_id: parent.run_id.clone(),
                                }
                            }),
                            workflow_execution_timeout: value
                                .workflow_execution_timeout
                                .as_ref()
                                .map(duration_from_core)
                                .transpose()?,
                            workflow_run_timeout: value
                                .workflow_run_timeout
                                .as_ref()
                                .map(duration_from_core)
                                .transpose()?,
                            workflow_task_timeout: value
                                .workflow_task_timeout
                                .as_ref()
                                .map(duration_from_core)
                                .transpose()?,
                            first_execution_run_id: value.first_execution_run_id.clone(),
                            start_time: value.start_time.as_ref().map(|time| Timestamp {
                                seconds: time.seconds,
                                nanoseconds: time.nanos,
                            }),
                            root_workflow: value.root_workflow.as_ref().map(|root| {
                                WorkflowExecution {
                                    workflow_id: root.workflow_id.clone(),
                                    run_id: root.run_id.clone(),
                                }
                            }),
                            priority: value.priority.as_ref().map(|priority| WorkflowPriority {
                                priority_key: priority.priority_key,
                                fairness_key: priority.fairness_key.clone(),
                                fairness_weight_bits: priority.fairness_weight.to_bits(),
                            }),
                            retry_policy: value
                                .retry_policy
                                .as_ref()
                                .map(retry_policy_from_core)
                                .transpose()?,
                            continuation,
                        })),
                    })
                }
                Variant::ResolveActivity(value) => {
                    // Core explicitly documents `is_local` as internal information the
                    // language SDK need not preserve. The result semantics are
                    // otherwise identical, so this is the sole intentionally ignored
                    // activation field in this slice.
                    Ok(ActivationJob::ResolveActivity {
                        seq: value.seq,
                        result: activity_resolution_from_core(
                            value
                                .result
                                .as_ref()
                                .ok_or_else(|| invalid_core("Core activity result is absent"))?,
                            value.is_local,
                        )?,
                    })
                }
                Variant::ResolveChildWorkflowExecutionStart(value) => {
                    Ok(ActivationJob::ResolveChildWorkflowStart {
                        seq: value.seq,
                        result: child_workflow_start_resolution_from_core(value)?,
                    })
                }
                Variant::ResolveChildWorkflowExecution(value) => {
                    Ok(ActivationJob::ResolveChildWorkflow {
                        seq: value.seq,
                        result: child_workflow_resolution_from_core(
                            value.result.as_ref().ok_or_else(|| {
                                invalid_core("Core child workflow result is absent")
                            })?,
                        )?,
                    })
                }
                Variant::ResolveSignalExternalWorkflow(value) => {
                    Ok(ActivationJob::ResolveSignalExternalWorkflow {
                        seq: value.seq,
                        result: external_workflow_resolution_from_core(value.failure.as_ref())?,
                    })
                }
                Variant::ResolveRequestCancelExternalWorkflow(value) => {
                    Ok(ActivationJob::ResolveRequestCancelExternalWorkflow {
                        seq: value.seq,
                        result: external_workflow_resolution_from_core(value.failure.as_ref())?,
                    })
                }
                Variant::SignalWorkflow(value) => Ok(ActivationJob::SignalWorkflow {
                    signal_name: value.signal_name.clone(),
                    input: value
                        .input
                        .iter()
                        .map(payload_from_core)
                        .collect::<Result<_, _>>()?,
                    identity: value.identity.clone(),
                    headers: value
                        .headers
                        .iter()
                        .map(|(key, payload)| Ok((key.clone(), payload_from_core(payload)?)))
                        .collect::<Result<BTreeMap<_, _>, CoreConversionError>>()?,
                }),
                Variant::QueryWorkflow(value) => Ok(ActivationJob::QueryWorkflow {
                    // Preserve the exact Core identifier. In particular, the
                    // pinned Core revision uses `legacy_query` for the
                    // legacy PollWFTResp query; completion conversion below
                    // deliberately leaves that identifier intact so Core can
                    // route the answer through its legacy response path.
                    query_id: value.query_id.clone(),
                    query_type: value.query_type.clone(),
                    arguments: value
                        .arguments
                        .iter()
                        .map(payload_from_core)
                        .collect::<Result<_, _>>()?,
                    headers: value
                        .headers
                        .iter()
                        .map(|(key, payload)| Ok((key.clone(), payload_from_core(payload)?)))
                        .collect::<Result<BTreeMap<_, _>, CoreConversionError>>()?,
                }),
                Variant::DoUpdate(value) => {
                    let meta = value
                        .meta
                        .as_ref()
                        .ok_or_else(|| invalid_core("Core update metadata is absent"))?;
                    // Temporal Core stores the workflow-scoped update ID in
                    // the top-level [DoUpdate.id].  It may strip the duplicate
                    // nested field from [DoUpdate.meta] before handing the job
                    // to the language SDK, leaving that protobuf field at its
                    // default empty value.  Reject a non-empty conflicting
                    // copy, but always reconstruct the semantic metadata from
                    // the authoritative top-level ID so valid stripped jobs do
                    // not fail conversion.
                    if !meta.update_id.is_empty() && meta.update_id != value.id {
                        return Err(invalid_core(
                            "Core update metadata ID does not match update ID",
                        ));
                    }
                    Ok(ActivationJob::DoUpdate {
                        id: value.id.clone(),
                        protocol_instance_id: value.protocol_instance_id.clone(),
                        name: value.name.clone(),
                        input: value
                            .input
                            .iter()
                            .map(payload_from_core)
                            .collect::<Result<_, _>>()?,
                        headers: value
                            .headers
                            .iter()
                            .map(|(key, payload)| Ok((key.clone(), payload_from_core(payload)?)))
                            .collect::<Result<BTreeMap<_, _>, CoreConversionError>>()?,
                        meta: UpdateMeta {
                            identity: meta.identity.clone(),
                            update_id: value.id.clone(),
                        },
                        run_validator: value.run_validator,
                    })
                }
                Variant::NotifyHasPatch(value) => Ok(ActivationJob::NotifyHasPatch {
                    patch_id: value.patch_id.clone(),
                }),
                Variant::FireTimer(value) => Ok(ActivationJob::FireTimer { seq: value.seq }),
                Variant::CancelWorkflow(value) => Ok(ActivationJob::CancelWorkflow {
                    reason: value.reason.clone(),
                }),
                Variant::RemoveFromCache(value) => Ok(ActivationJob::RemoveFromCache {
                    message: value.message.clone(),
                    reason: eviction_reason_from_core(value.reason)?,
                }),
                _ => Err(unsupported("Core activation job kind is not supported")),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let activation = Activation {
        run_id: value.run_id.clone(),
        timestamp: value.timestamp.as_ref().map(|timestamp| Timestamp {
            seconds: timestamp.seconds,
            nanoseconds: timestamp.nanos,
        }),
        is_replaying: value.is_replaying,
        history_length: value.history_length,
        jobs,
        metadata: Some(ActivationMetadata {
            available_internal_flags: value.available_internal_flags.clone(),
            history_size_bytes: value.history_size_bytes.to_string(),
            continue_as_new_suggested: value.continue_as_new_suggested,
            deployment_version_for_current_task: value
                .deployment_version_for_current_task
                .as_ref()
                .map(|version| WorkerDeploymentVersion {
                    deployment_name: version.deployment_name.clone(),
                    build_id: version.build_id.clone(),
                }),
            last_sdk_version: value.last_sdk_version.clone(),
            suggest_continue_as_new_reasons: value
                .suggest_continue_as_new_reasons
                .iter()
                .copied()
                .map(suggestion_from_core)
                .collect::<Result<_, _>>()?,
            target_worker_deployment_version_changed: value
                .target_worker_deployment_version_changed,
        }),
    };
    validate_activation(&activation)
        .map_err(|_| invalid_core("Core activation violates semantic protocol invariants"))?;
    Ok(activation)
}

/// Converts one semantic duration to the protobuf representation.
pub(crate) fn duration_to_core(value: Duration) -> prost_wkt_types::Duration {
    prost_wkt_types::Duration {
        seconds: value.seconds,
        nanos: value.nanoseconds,
    }
}

/// Converts and validates one protobuf duration.
pub(crate) fn duration_from_core(
    value: &prost_wkt_types::Duration,
) -> Result<Duration, CoreConversionError> {
    let duration = Duration {
        seconds: value.seconds,
        nanoseconds: value.nanos,
    };
    validate_time(duration.seconds, duration.nanoseconds, true, "$")
        .map_err(|_| invalid_core("Core duration is not normalized"))?;
    Ok(duration)
}

/// Converts a validated semantic retry policy into Temporal Core's protobuf
/// policy without changing the coefficient's floating-point bits.
fn retry_policy_to_core(
    value: &RetryPolicy,
) -> Result<api_common::RetryPolicy, CoreConversionError> {
    validate_retry_policy(value, "$.commands.retry_policy")
        .map_err(|_| invalid_core("retry policy violates semantic invariants"))?;
    let coefficient_bits = value
        .backoff_coefficient_bits
        .parse::<u64>()
        .map_err(|_| invalid_core("retry policy coefficient bits are not canonical"))?;
    Ok(api_common::RetryPolicy {
        initial_interval: Some(duration_to_core(value.initial_interval)),
        backoff_coefficient: f64::from_bits(coefficient_bits),
        maximum_interval: Some(duration_to_core(value.maximum_interval)),
        maximum_attempts: value.maximum_attempts,
        non_retryable_error_types: value.non_retryable_error_types.clone(),
    })
}

/// Converts Core's effective retry policy back into the lossless semantic
/// representation, rejecting missing required durations instead of guessing
/// Core defaults on the OCaml side.
fn retry_policy_from_core(
    value: &api_common::RetryPolicy,
) -> Result<RetryPolicy, CoreConversionError> {
    let policy = RetryPolicy {
        initial_interval: duration_from_core(
            value
                .initial_interval
                .as_ref()
                .ok_or_else(|| invalid_core("Core retry policy initial interval is absent"))?,
        )?,
        backoff_coefficient_bits: value.backoff_coefficient.to_bits().to_string(),
        maximum_interval: duration_from_core(
            value
                .maximum_interval
                .as_ref()
                .ok_or_else(|| invalid_core("Core retry policy maximum interval is absent"))?,
        )?,
        maximum_attempts: value.maximum_attempts,
        non_retryable_error_types: value.non_retryable_error_types.clone(),
    };
    validate_retry_policy(&policy, "$.commands.retry_policy")
        .map_err(|_| invalid_core("Core retry policy violates semantic invariants"))?;
    Ok(policy)
}

/// Maps an OCaml-facing cancellation choice to the pinned Core enum.
fn cancellation_to_core(value: ActivityCancellationType) -> i32 {
    use core_commands::ActivityCancellationType as Core;
    (match value {
        ActivityCancellationType::TryCancel => Core::TryCancel,
        ActivityCancellationType::WaitCancellationCompleted => Core::WaitCancellationCompleted,
        ActivityCancellationType::Abandon => Core::Abandon,
    }) as i32
}

/// Converts a validated semantic priority to the protobuf value expected by
/// Temporal Core.  The zero/empty defaults are explicit so omitted optional
/// fields cannot accidentally inherit a stale value from a reused command.
fn priority_to_core(value: &WorkflowPriority) -> Result<api_common::Priority, CoreConversionError> {
    validate_priority(value, "$.commands.priority")
        .map_err(|_| invalid_core("workflow priority violates semantic invariants"))?;
    Ok(api_common::Priority {
        priority_key: value.priority_key,
        fairness_key: value.fairness_key.clone(),
        fairness_weight: f32::from_bits(value.fairness_weight_bits),
    })
}

/// Maps a pinned Core cancellation number without accepting unknown values.
fn cancellation_from_core(value: i32) -> Result<ActivityCancellationType, CoreConversionError> {
    use core_commands::ActivityCancellationType as Core;
    Ok(
        match Core::try_from(value).map_err(|_| invalid_core("unknown Core cancellation type"))? {
            Core::TryCancel => ActivityCancellationType::TryCancel,
            Core::WaitCancellationCompleted => ActivityCancellationType::WaitCancellationCompleted,
            Core::Abandon => ActivityCancellationType::Abandon,
        },
    )
}

/// Converts the semantic child policy to the pinned Core enum without relying
/// on numeric values at the OCaml/Rust boundary.
fn child_cancellation_to_core(value: ChildWorkflowCancellationType) -> i32 {
    use core_child_workflow::ChildWorkflowCancellationType as Core;
    (match value {
        ChildWorkflowCancellationType::TryCancel => Core::TryCancel,
        ChildWorkflowCancellationType::WaitCancellationCompleted => Core::WaitCancellationCompleted,
        ChildWorkflowCancellationType::Abandon => Core::Abandon,
        ChildWorkflowCancellationType::WaitCancellationRequested => Core::WaitCancellationRequested,
    }) as i32
}

/// Converts a Core child policy while rejecting enum values added by a newer
/// Core than this bridge was compiled against.
fn child_cancellation_from_core(
    value: i32,
) -> Result<ChildWorkflowCancellationType, CoreConversionError> {
    use core_child_workflow::ChildWorkflowCancellationType as Core;
    Ok(
        match Core::try_from(value)
            .map_err(|_| invalid_core("unknown Core child cancellation type"))?
        {
            Core::TryCancel => ChildWorkflowCancellationType::TryCancel,
            Core::WaitCancellationCompleted => {
                ChildWorkflowCancellationType::WaitCancellationCompleted
            }
            Core::Abandon => ChildWorkflowCancellationType::Abandon,
            Core::WaitCancellationRequested => {
                ChildWorkflowCancellationType::WaitCancellationRequested
            }
        },
    )
}

/// Builds one official Core command with unsupported optional fields defaulted explicitly.
fn command_to_core(
    value: &CompletionCommand,
    child_workflow_namespace: Option<&str>,
) -> Result<core_commands::WorkflowCommand, CoreConversionError> {
    use core_commands::workflow_command::Variant;
    let variant = match value {
        CompletionCommand::ScheduleActivity {
            seq,
            activity_id,
            activity_type,
            task_queue,
            arguments,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            retry_policy,
            priority,
            cancellation_type,
            do_not_eagerly_execute,
        } => Variant::ScheduleActivity(core_commands::ScheduleActivity {
            seq: *seq,
            activity_id: activity_id.clone(),
            activity_type: activity_type.clone(),
            task_queue: task_queue.clone(),
            headers: Default::default(),
            arguments: arguments
                .iter()
                .map(payload_to_core)
                .collect::<Result<_, _>>()?,
            schedule_to_close_timeout: schedule_to_close_timeout.map(duration_to_core),
            schedule_to_start_timeout: schedule_to_start_timeout.map(duration_to_core),
            start_to_close_timeout: start_to_close_timeout.map(duration_to_core),
            heartbeat_timeout: heartbeat_timeout.map(duration_to_core),
            retry_policy: retry_policy
                .as_ref()
                .map(retry_policy_to_core)
                .transpose()?,
            cancellation_type: cancellation_to_core(*cancellation_type),
            do_not_eagerly_execute: *do_not_eagerly_execute,
            versioning_intent: 0,
            priority: priority.as_ref().map(priority_to_core).transpose()?,
        }),
        CompletionCommand::ScheduleLocalActivity {
            seq,
            activity_id,
            activity_type,
            attempt,
            original_schedule_time,
            arguments,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            start_to_close_timeout,
            retry_policy,
            local_retry_threshold,
            cancellation_type,
        } => Variant::ScheduleLocalActivity(core_commands::ScheduleLocalActivity {
            seq: *seq,
            activity_id: activity_id.clone(),
            activity_type: activity_type.clone(),
            attempt: *attempt,
            original_schedule_time: original_schedule_time.map(|value| {
                prost_wkt_types::Timestamp {
                    seconds: value.seconds,
                    nanos: value.nanoseconds,
                }
            }),
            headers: Default::default(),
            arguments: arguments
                .iter()
                .map(payload_to_core)
                .collect::<Result<_, _>>()?,
            schedule_to_close_timeout: schedule_to_close_timeout.map(duration_to_core),
            schedule_to_start_timeout: schedule_to_start_timeout.map(duration_to_core),
            start_to_close_timeout: start_to_close_timeout.map(duration_to_core),
            retry_policy: retry_policy
                .as_ref()
                .map(retry_policy_to_core)
                .transpose()?,
            local_retry_threshold: local_retry_threshold.map(duration_to_core),
            cancellation_type: cancellation_to_core(*cancellation_type),
        }),
        CompletionCommand::RequestCancelActivity { seq } => {
            Variant::RequestCancelActivity(core_commands::RequestCancelActivity { seq: *seq })
        }
        CompletionCommand::RequestCancelLocalActivity { seq } => {
            Variant::RequestCancelLocalActivity(core_commands::RequestCancelLocalActivity {
                seq: *seq,
            })
        }
        CompletionCommand::StartChildWorkflow {
            seq,
            workflow_id,
            workflow_type,
            input,
            retry_policy,
            cancellation_type,
        } => Variant::StartChildWorkflowExecution(core_commands::StartChildWorkflowExecution {
            seq: *seq,
            workflow_id: workflow_id.clone(),
            workflow_type: workflow_type.clone(),
            input: input
                .iter()
                .map(payload_to_core)
                .collect::<Result<_, _>>()?,
            retry_policy: retry_policy
                .as_ref()
                .map(retry_policy_to_core)
                .transpose()?,
            cancellation_type: child_cancellation_to_core(*cancellation_type),
            // The worker namespace is injected by the namespace-aware
            // conversion used by live/replay workers. The legacy conversion
            // helper intentionally leaves it at Core's default for isolated
            // protocol round-trip tests; it is never submitted to a worker.
            namespace: child_workflow_namespace.unwrap_or_default().to_owned(),
            // Task queue, timeouts, and the other child options remain at Core
            // defaults until they have a stable public representation.
            ..Default::default()
        }),
        CompletionCommand::CancelChildWorkflow { seq, reason } => {
            Variant::CancelChildWorkflowExecution(core_commands::CancelChildWorkflowExecution {
                child_workflow_seq: *seq,
                reason: reason.clone(),
            })
        }
        CompletionCommand::SignalExternalWorkflow {
            seq,
            workflow_id,
            run_id,
            signal_name,
            input,
            child_workflow_only,
            headers,
        } => {
            let target = if *child_workflow_only {
                core_commands::signal_external_workflow_execution::Target::ChildWorkflowId(
                    workflow_id.clone(),
                )
            } else {
                core_commands::signal_external_workflow_execution::Target::WorkflowExecution(
                    core_common::NamespacedWorkflowExecution {
                        namespace: child_workflow_namespace.unwrap_or_default().to_owned(),
                        workflow_id: workflow_id.clone(),
                        run_id: run_id.clone(),
                    },
                )
            };
            Variant::SignalExternalWorkflowExecution(
                core_commands::SignalExternalWorkflowExecution {
                    seq: *seq,
                    target: Some(target),
                    signal_name: signal_name.clone(),
                    args: input
                        .iter()
                        .map(payload_to_core)
                        .collect::<Result<_, _>>()?,
                    headers: headers
                        .iter()
                        .map(|(key, payload)| Ok((key.clone(), payload_to_core(payload)?)))
                        .collect::<Result<_, CoreConversionError>>()?,
                },
            )
        }
        CompletionCommand::RequestCancelExternalWorkflow {
            seq,
            workflow_id,
            run_id,
            reason,
        } => Variant::RequestCancelExternalWorkflowExecution(
            core_commands::RequestCancelExternalWorkflowExecution {
                seq: *seq,
                workflow_execution: Some(core_common::NamespacedWorkflowExecution {
                    namespace: child_workflow_namespace.unwrap_or_default().to_owned(),
                    workflow_id: workflow_id.clone(),
                    run_id: run_id.clone(),
                }),
                reason: reason.clone(),
            },
        ),
        CompletionCommand::StartTimer {
            seq,
            start_to_fire_timeout,
        } => Variant::StartTimer(core_commands::StartTimer {
            seq: *seq,
            start_to_fire_timeout: Some(duration_to_core(*start_to_fire_timeout)),
        }),
        CompletionCommand::CancelTimer { seq } => {
            Variant::CancelTimer(core_commands::CancelTimer { seq: *seq })
        }
        CompletionCommand::CompleteWorkflow { result } => {
            Variant::CompleteWorkflowExecution(core_commands::CompleteWorkflowExecution {
                result: result.as_ref().map(payload_to_core).transpose()?,
            })
        }
        CompletionCommand::FailWorkflow { failure } => {
            Variant::FailWorkflowExecution(core_commands::FailWorkflowExecution {
                failure: Some(failure_to_core(failure)?),
            })
        }
        CompletionCommand::ContinueAsNew {
            workflow_type,
            input,
        } => {
            Variant::ContinueAsNewWorkflowExecution(core_commands::ContinueAsNewWorkflowExecution {
                workflow_type: workflow_type.clone(),
                arguments: input
                    .iter()
                    .map(payload_to_core)
                    .collect::<Result<_, _>>()?,
                // The semantic protocol intentionally exposes no task queue,
                // timeout, memo, header, retry, or versioning controls yet.
                ..Default::default()
            })
        }
        CompletionCommand::CancelWorkflow => {
            Variant::CancelWorkflowExecution(core_commands::CancelWorkflowExecution {})
        }
        CompletionCommand::QueryResult { query_id, result } => {
            let variant = match result {
                QueryResult::Succeeded { payload } => {
                    core_commands::query_result::Variant::Succeeded(core_commands::QuerySuccess {
                        response: Some(payload_to_core(payload)?),
                    })
                }
                QueryResult::Failed { failure } => {
                    core_commands::query_result::Variant::Failed(failure_to_core(failure)?)
                }
            };
            Variant::RespondToQuery(core_commands::QueryResult {
                query_id: query_id.clone(),
                variant: Some(variant),
            })
        }
        CompletionCommand::UpdateResponse {
            protocol_instance_id,
            response,
        } => {
            use core_commands::update_response::Response;
            let response = match response {
                UpdateResponseResult::Accepted => Response::Accepted(()),
                UpdateResponseResult::Rejected { failure } => {
                    Response::Rejected(failure_to_core(failure)?)
                }
                UpdateResponseResult::Completed { payload } => {
                    Response::Completed(payload_to_core(payload)?)
                }
            };
            Variant::UpdateResponse(core_commands::UpdateResponse {
                protocol_instance_id: protocol_instance_id.clone(),
                response: Some(response),
            })
        }
        CompletionCommand::SetPatchMarker {
            patch_id,
            deprecated,
        } => Variant::SetPatchMarker(core_commands::SetPatchMarker {
            patch_id: patch_id.clone(),
            deprecated: *deprecated,
        }),
        CompletionCommand::UpsertSearchAttributes { search_attributes } => {
            let indexed_fields = search_attributes
                .iter()
                .map(|(key, payload)| Ok((key.clone(), payload_to_core(payload)?)))
                .collect::<Result<_, CoreConversionError>>()?;
            Variant::UpsertWorkflowSearchAttributes(core_commands::UpsertWorkflowSearchAttributes {
                search_attributes: Some(api_common::SearchAttributes { indexed_fields }),
            })
        }
    };
    Ok(core_commands::WorkflowCommand {
        variant: Some(variant),
        user_metadata: None,
    })
}

/// Converts one supported Core command while rejecting every lossy option.
fn command_from_core(
    value: &core_commands::WorkflowCommand,
) -> Result<CompletionCommand, CoreConversionError> {
    use core_commands::workflow_command::Variant;
    if value.user_metadata.is_some() {
        return Err(unsupported("Core command user metadata is not supported"));
    }
    match value
        .variant
        .as_ref()
        .ok_or_else(|| invalid_core("Core command variant is absent"))?
    {
        Variant::ScheduleActivity(value) => {
            if !value.headers.is_empty() || value.versioning_intent != 0 {
                return Err(unsupported(
                    "Core schedule activity headers or versioning intent are not supported",
                ));
            }
            Ok(CompletionCommand::ScheduleActivity {
                seq: value.seq,
                activity_id: value.activity_id.clone(),
                activity_type: value.activity_type.clone(),
                task_queue: value.task_queue.clone(),
                arguments: value
                    .arguments
                    .iter()
                    .map(payload_from_core)
                    .collect::<Result<_, _>>()?,
                schedule_to_close_timeout: value
                    .schedule_to_close_timeout
                    .as_ref()
                    .map(duration_from_core)
                    .transpose()?,
                schedule_to_start_timeout: value
                    .schedule_to_start_timeout
                    .as_ref()
                    .map(duration_from_core)
                    .transpose()?,
                start_to_close_timeout: value
                    .start_to_close_timeout
                    .as_ref()
                    .map(duration_from_core)
                    .transpose()?,
                heartbeat_timeout: value
                    .heartbeat_timeout
                    .as_ref()
                    .map(duration_from_core)
                    .transpose()?,
                retry_policy: value
                    .retry_policy
                    .as_ref()
                    .map(retry_policy_from_core)
                    .transpose()?,
                priority: value.priority.as_ref().map(|priority| WorkflowPriority {
                    priority_key: priority.priority_key,
                    fairness_key: priority.fairness_key.clone(),
                    fairness_weight_bits: priority.fairness_weight.to_bits(),
                }),
                cancellation_type: cancellation_from_core(value.cancellation_type)?,
                do_not_eagerly_execute: value.do_not_eagerly_execute,
            })
        }
        Variant::ScheduleLocalActivity(value) => {
            if !value.headers.is_empty() {
                return Err(unsupported(
                    "Core local activity headers or user metadata are not represented by this protocol",
                ));
            }
            Ok(CompletionCommand::ScheduleLocalActivity {
                seq: value.seq,
                activity_id: value.activity_id.clone(),
                activity_type: value.activity_type.clone(),
                attempt: value.attempt,
                original_schedule_time: value.original_schedule_time.as_ref().map(|timestamp| {
                    Timestamp {
                        seconds: timestamp.seconds,
                        nanoseconds: timestamp.nanos,
                    }
                }),
                arguments: value
                    .arguments
                    .iter()
                    .map(payload_from_core)
                    .collect::<Result<_, _>>()?,
                schedule_to_close_timeout: value
                    .schedule_to_close_timeout
                    .as_ref()
                    .map(duration_from_core)
                    .transpose()?,
                schedule_to_start_timeout: value
                    .schedule_to_start_timeout
                    .as_ref()
                    .map(duration_from_core)
                    .transpose()?,
                start_to_close_timeout: value
                    .start_to_close_timeout
                    .as_ref()
                    .map(duration_from_core)
                    .transpose()?,
                retry_policy: value
                    .retry_policy
                    .as_ref()
                    .map(retry_policy_from_core)
                    .transpose()?,
                local_retry_threshold: value
                    .local_retry_threshold
                    .as_ref()
                    .map(duration_from_core)
                    .transpose()?,
                cancellation_type: cancellation_from_core(value.cancellation_type)?,
            })
        }
        Variant::StartChildWorkflowExecution(value) => {
            if !value.namespace.is_empty()
                || !value.task_queue.is_empty()
                || value.workflow_execution_timeout.is_some()
                || value.workflow_run_timeout.is_some()
                || value.workflow_task_timeout.is_some()
                || value.parent_close_policy != 0
                || value.workflow_id_reuse_policy != 0
                || !value.cron_schedule.is_empty()
                || !value.headers.is_empty()
                || !value.memo.is_empty()
                || value.search_attributes.is_some()
                || value.versioning_intent != 0
                || value.priority.is_some()
            {
                return Err(unsupported(
                    "Core child workflow options are not represented by this protocol",
                ));
            }
            Ok(CompletionCommand::StartChildWorkflow {
                seq: value.seq,
                workflow_id: value.workflow_id.clone(),
                workflow_type: value.workflow_type.clone(),
                input: value
                    .input
                    .iter()
                    .map(payload_from_core)
                    .collect::<Result<_, _>>()?,
                retry_policy: value
                    .retry_policy
                    .as_ref()
                    .map(retry_policy_from_core)
                    .transpose()?,
                cancellation_type: child_cancellation_from_core(value.cancellation_type)?,
            })
        }
        Variant::CancelChildWorkflowExecution(value) => {
            if value.reason.is_empty() || value.reason.as_bytes().contains(&0) {
                return Err(invalid_core("Core child cancellation reason is invalid"));
            }
            bounded_text(&value.reason, "$.commands.reason")
                .map_err(|_| invalid_core("Core child cancellation reason is too long"))?;
            Ok(CompletionCommand::CancelChildWorkflow {
                seq: value.child_workflow_seq,
                reason: value.reason.clone(),
            })
        }
        Variant::SignalExternalWorkflowExecution(value) => {
            let (workflow_id, run_id, child_workflow_only) = match value
                .target
                .as_ref()
                .ok_or_else(|| invalid_core("Core external signal target is absent"))?
            {
                core_commands::signal_external_workflow_execution::Target::WorkflowExecution(
                    execution,
                ) => (
                    execution.workflow_id.clone(),
                    execution.run_id.clone(),
                    false,
                ),
                core_commands::signal_external_workflow_execution::Target::ChildWorkflowId(id) => {
                    (id.clone(), String::new(), true)
                }
            };
            Ok(CompletionCommand::SignalExternalWorkflow {
                seq: value.seq,
                workflow_id,
                run_id,
                signal_name: value.signal_name.clone(),
                input: value
                    .args
                    .iter()
                    .map(payload_from_core)
                    .collect::<Result<_, _>>()?,
                child_workflow_only,
                headers: value
                    .headers
                    .iter()
                    .map(|(key, payload)| Ok((key.clone(), payload_from_core(payload)?)))
                    .collect::<Result<_, CoreConversionError>>()?,
            })
        }
        Variant::RequestCancelExternalWorkflowExecution(value) => {
            let execution = value
                .workflow_execution
                .as_ref()
                .ok_or_else(|| invalid_core("Core external cancellation target is absent"))?;
            Ok(CompletionCommand::RequestCancelExternalWorkflow {
                seq: value.seq,
                workflow_id: execution.workflow_id.clone(),
                run_id: execution.run_id.clone(),
                reason: value.reason.clone(),
            })
        }
        Variant::RequestCancelActivity(value) => {
            Ok(CompletionCommand::RequestCancelActivity { seq: value.seq })
        }
        Variant::RequestCancelLocalActivity(value) => {
            Ok(CompletionCommand::RequestCancelLocalActivity { seq: value.seq })
        }
        Variant::StartTimer(value) => Ok(CompletionCommand::StartTimer {
            seq: value.seq,
            start_to_fire_timeout: duration_from_core(
                value
                    .start_to_fire_timeout
                    .as_ref()
                    .ok_or_else(|| invalid_core("Core timer timeout is absent"))?,
            )?,
        }),
        Variant::CancelTimer(value) => Ok(CompletionCommand::CancelTimer { seq: value.seq }),
        Variant::CompleteWorkflowExecution(value) => Ok(CompletionCommand::CompleteWorkflow {
            result: value.result.as_ref().map(payload_from_core).transpose()?,
        }),
        Variant::FailWorkflowExecution(value) => Ok(CompletionCommand::FailWorkflow {
            failure: failure_from_core(
                value
                    .failure
                    .as_ref()
                    .ok_or_else(|| invalid_core("Core fail command has no failure"))?,
            )?,
        }),
        Variant::ContinueAsNewWorkflowExecution(value) => {
            if !value.task_queue.is_empty()
                || value.workflow_run_timeout.is_some()
                || value.workflow_task_timeout.is_some()
                || !value.memo.is_empty()
                || !value.headers.is_empty()
                || value.search_attributes.is_some()
                || value.retry_policy.is_some()
                || value.versioning_intent != 0
                || value.initial_versioning_behavior != 0
                || value.backoff_start_interval.is_some()
            {
                return Err(unsupported(
                    "Core continue-as-new options are not represented by this protocol",
                ));
            }
            identifier(&value.workflow_type, "$.commands.workflow_type")
                .map_err(|_| invalid_core("Core continue-as-new workflow type is invalid"))?;
            Ok(CompletionCommand::ContinueAsNew {
                workflow_type: value.workflow_type.clone(),
                input: value
                    .arguments
                    .iter()
                    .map(payload_from_core)
                    .collect::<Result<_, _>>()?,
            })
        }
        Variant::CancelWorkflowExecution(_) => Ok(CompletionCommand::CancelWorkflow),
        Variant::RespondToQuery(value) => {
            let result = match value
                .variant
                .as_ref()
                .ok_or_else(|| invalid_core("Core query result variant is absent"))?
            {
                core_commands::query_result::Variant::Succeeded(success) => {
                    QueryResult::Succeeded {
                        payload: payload_from_core(success.response.as_ref().ok_or_else(
                            || invalid_core("Core successful query result has no payload"),
                        )?)?,
                    }
                }
                core_commands::query_result::Variant::Failed(failure) => QueryResult::Failed {
                    failure: Box::new(failure_from_core(failure)?),
                },
            };
            Ok(CompletionCommand::QueryResult {
                query_id: value.query_id.clone(),
                result,
            })
        }
        Variant::UpdateResponse(value) => {
            use core_commands::update_response::Response;
            let response = match value
                .response
                .as_ref()
                .ok_or_else(|| invalid_core("Core update response variant is absent"))?
            {
                Response::Accepted(_) => UpdateResponseResult::Accepted,
                Response::Rejected(failure) => UpdateResponseResult::Rejected {
                    failure: Box::new(failure_from_core(failure)?),
                },
                Response::Completed(payload) => UpdateResponseResult::Completed {
                    payload: payload_from_core(payload)?,
                },
            };
            Ok(CompletionCommand::UpdateResponse {
                protocol_instance_id: value.protocol_instance_id.clone(),
                response,
            })
        }
        Variant::SetPatchMarker(value) => Ok(CompletionCommand::SetPatchMarker {
            patch_id: value.patch_id.clone(),
            deprecated: value.deprecated,
        }),
        Variant::UpsertWorkflowSearchAttributes(value) => {
            let search_attributes = value
                .search_attributes
                .as_ref()
                .ok_or_else(|| invalid_core("Core search-attribute command has no values"))?
                .indexed_fields
                .iter()
                .map(|(key, payload)| Ok((key.clone(), payload_from_core(payload)?)))
                .collect::<Result<_, CoreConversionError>>()?;
            Ok(CompletionCommand::UpsertSearchAttributes { search_attributes })
        }
        _ => Err(unsupported("Core workflow command kind is not supported")),
    }
}

/// Converts semantic commands to the official successful activation completion.
pub fn completion_to_core(
    value: &Completion,
) -> Result<core_completion::WorkflowActivationCompletion, CoreConversionError> {
    completion_to_core_with_child_namespace(value, None)
}

/// Shared conversion implementation used by the compatibility helper above
/// and by activation-aware worker completion. `None` preserves the historical
/// unit-test conversion for commands that are not submitted to a live worker;
/// live and replay runtimes always use
/// [`completion_to_core_for_activation_with_namespace`].
fn completion_to_core_with_child_namespace(
    value: &Completion,
    child_workflow_namespace: Option<&str>,
) -> Result<core_completion::WorkflowActivationCompletion, CoreConversionError> {
    validate_completion(value)
        .map_err(|_| invalid_core("semantic completion violates protocol invariants"))?;
    Ok(core_completion::WorkflowActivationCompletion {
        run_id: value.run_id.clone(),
        status: Some(
            core_completion::workflow_activation_completion::Status::Successful(
                core_completion::Success {
                    commands: value
                        .commands
                        .iter()
                        .map(|command| command_to_core(command, child_workflow_namespace))
                        .collect::<Result<_, _>>()?,
                    used_internal_flags: Vec::new(),
                    versioning_behavior: 0,
                },
            ),
        ),
    })
}

/// Converts a completion while enforcing activation-dependent invariants.
///
/// Core guarantees eviction activations contain only `RemoveFromCache`; the
/// language side must acknowledge those activations with no workflow commands.
/// Keeping this check beside conversion prevents accidental workflow execution
/// during cache teardown.
pub fn completion_to_core_for_activation(
    activation: &Activation,
    completion: &Completion,
) -> Result<core_completion::WorkflowActivationCompletion, CoreConversionError> {
    completion_to_core_for_activation_with_optional_namespace(activation, completion, None)
}

/// Converts one leased activation completion while injecting the worker
/// namespace into every child-workflow start command.
pub fn completion_to_core_for_activation_with_namespace(
    activation: &Activation,
    completion: &Completion,
    child_workflow_namespace: &str,
) -> Result<core_completion::WorkflowActivationCompletion, CoreConversionError> {
    identifier(child_workflow_namespace, "$.worker_namespace")
        .map_err(|_| invalid_core("worker namespace is not a valid identifier"))?;
    completion_to_core_for_activation_with_optional_namespace(
        activation,
        completion,
        Some(child_workflow_namespace),
    )
}

/// Applies activation-dependent invariants before converting the completion.
/// The optional namespace exists only for the legacy conversion helper; live
/// and replay workers use the validated namespace-bearing entry point above.
fn completion_to_core_for_activation_with_optional_namespace(
    activation: &Activation,
    completion: &Completion,
    child_workflow_namespace: Option<&str>,
) -> Result<core_completion::WorkflowActivationCompletion, CoreConversionError> {
    if activation.run_id != completion.run_id {
        return Err(invalid_core(
            "completion run id does not match its activation",
        ));
    }
    let eviction = matches!(
        activation.jobs.as_slice(),
        [ActivationJob::RemoveFromCache { .. }]
    );
    if eviction && !completion.commands.is_empty() {
        return Err(invalid_core(
            "cache eviction activation must have an empty completion",
        ));
    }
    let query_ids: BTreeSet<&str> = activation
        .jobs
        .iter()
        .filter_map(|job| match job {
            ActivationJob::QueryWorkflow { query_id, .. } => Some(query_id.as_str()),
            _ => None,
        })
        .collect();
    if query_ids.is_empty() {
        if completion
            .commands
            .iter()
            .any(|command| matches!(command, CompletionCommand::QueryResult { .. }))
        {
            return Err(invalid_core(
                "query result command requires a query activation",
            ));
        }
    } else {
        let mut result_ids = BTreeSet::new();
        for command in &completion.commands {
            let CompletionCommand::QueryResult { query_id, .. } = command else {
                return Err(invalid_core(
                    "query activation completion may contain only query results",
                ));
            };
            if !query_ids.contains(query_id.as_str()) || !result_ids.insert(query_id.as_str()) {
                return Err(invalid_core(
                    "query completion identifier does not match its activation",
                ));
            }
        }
        if result_ids != query_ids {
            return Err(invalid_core(
                "query activation must receive exactly one result per query",
            ));
        }
    }
    completion_to_core_with_child_namespace(completion, child_workflow_namespace)
}

/// Converts a successful official completion without silently dropping flags or status.
pub fn completion_from_core(
    value: &core_completion::WorkflowActivationCompletion,
) -> Result<Completion, CoreConversionError> {
    use core_completion::workflow_activation_completion::Status;
    let success = match value
        .status
        .as_ref()
        .ok_or_else(|| invalid_core("Core completion status is absent"))?
    {
        Status::Successful(success) => success,
        Status::Failed(_) => {
            return Err(unsupported(
                "failed activation completion is not represented by this command protocol",
            ));
        }
    };
    if !success.used_internal_flags.is_empty() || success.versioning_behavior != 0 {
        return Err(unsupported(
            "Core completion metadata is not represented by this protocol slice",
        ));
    }
    let completion = Completion {
        run_id: value.run_id.clone(),
        commands: success
            .commands
            .iter()
            .map(command_from_core)
            .collect::<Result<_, _>>()?,
    };
    validate_completion(&completion)
        .map_err(|_| invalid_core("Core completion violates semantic protocol invariants"))?;
    Ok(completion)
}
