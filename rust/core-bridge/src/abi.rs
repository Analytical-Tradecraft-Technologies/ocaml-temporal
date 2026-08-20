use crate::replay_bridge::{ReplayWorker, ReplayWorkerError};
use crate::worker_bridge::{
    PollLaneError, PollLanes, ReadinessWait, WorkerBridgeError, public_poll_lane_error_message,
    public_worker_error_message,
};
use crate::{activity_protocol, client_protocol, workflow_protocol};
use serde::Deserialize;
use std::collections::{HashMap, hash_map::Entry};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{
    Receiver, RecvTimeoutError, SyncSender, TryRecvError, channel, sync_channel,
};
use std::time::Duration;
use temporalio_client::{
    ActivityIdentifier, Client, ClientOptions, Connection, ConnectionOptions,
    errors::AsyncActivityError,
};
use temporalio_common::protos::coresdk::{
    ActivityHeartbeat as CoreActivityHeartbeat,
    activity_task::{self, ActivityTask as CoreActivityTask},
};
use temporalio_common::protos::{
    TaskToken,
    temporal::api::{common::v1 as api_common, enums::v1::VersioningBehavior},
};
use temporalio_sdk_core::{
    CoreRuntime, PollerBehavior, RuntimeOptions, TokioRuntimeBuilder, WorkerConfig,
    WorkerVersioningStrategy,
};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Version of the native ABI implemented by this crate.
pub const ABI_VERSION: u32 = 2;

/// Fixed-width status type shared with the C header.
pub type Status = i32;

/// Operation completed and `value` may own bytes.
pub const STATUS_OK: Status = 0;
/// A pointer, length, range, or other caller argument violated the ABI.
pub const STATUS_INVALID_ARGUMENT: Status = 1;
/// The caller requested an ABI version this bridge does not implement.
pub const STATUS_ABI_MISMATCH: Status = 2;
/// A Rust panic was contained before it crossed the C boundary.
pub const STATUS_PANIC: Status = 3;
/// Reserved non-panic bridge implementation failure.
pub const STATUS_INTERNAL: Status = 4;
/// An operation was requested in an incompatible graph lifecycle state.
pub const STATUS_INVALID_STATE: Status = 5;
/// A strict lifecycle configuration document was malformed or invalid.
pub const STATUS_CONFIGURATION: Status = 6;
/// Temporal client connection failed after configuration validation.
pub const STATUS_CONNECTION: Status = 7;
/// Official Core worker construction or namespace validation failed.
pub const STATUS_WORKER: Status = 8;
/// Worker shutdown is draining tasks that still require language completion.
pub const STATUS_OUTSTANDING_TASKS: Status = 9;
/// A bounded readiness operation has no result ready for handoff yet.
pub const STATUS_NOT_READY: Status = 10;
/// A semantic workflow or activity document failed strict validation.
pub const STATUS_PROTOCOL: Status = 11;
/// Temporal rejected a workflow start because the workflow ID is already in
/// use; the error buffer contains a closed client-error JSON document.
pub const STATUS_ALREADY_STARTED: Status = 12;
/// An explicitly transient activity-completion transport failure.
///
/// This status is reserved for a Core/client implementation that can prove
/// the completion was not consumed. The pinned Core revision currently
/// suppresses those network outcomes, so the production bridge fails closed
/// rather than emitting this status speculatively.
pub const STATUS_RETRYABLE: Status = 13;

/// Maximum accepted lifecycle configuration document size.
const MAX_LIFECYCLE_CONFIG_BYTES: usize = 64 * 1024;
/// Private transport-safety ceiling for one string crossing the bridge.
///
/// This is deliberately not a claim about Temporal Server identifier limits,
/// which can be namespace-configurable and differ by field. Core and Server
/// remain responsible for those semantic checks.
const MAX_TRANSPORT_STRING_BYTES: usize = 64 * 1024;
/// Prevents accidental allocation of unreasonable in-process worker state.
const MAX_WORKER_COUNT: u32 = 1_000_000;
/// Initial remote-activity concurrency until the private worker config grows a
/// separately tuned activity field in the end-to-end worker slice.
const DEFAULT_MAX_OUTSTANDING_ACTIVITIES: usize = 100;
/// Initial Core server-poll concurrency for remote activity tasks.
const DEFAULT_MAX_CONCURRENT_ACTIVITY_POLLS: usize = 5;
/// Temporal Core requires two workflow-task pollers whenever workflow caching
/// is enabled. This mirrors the OCaml sender-side validation.
const MIN_CACHED_WORKFLOW_POLLS: u32 = 2;
/// Prevents an unbounded graceful-shutdown duration from entering Core.
const MAX_GRACEFUL_SHUTDOWN_MS: u64 = 24 * 60 * 60 * 1_000;
/// Maximum time one exact-run client wait may occupy the supervisor owner.
///
/// The Temporal history request remains a close-event long poll, but the
/// outer ABI operation is deliberately bounded.  When the deadline elapses,
/// Tokio drops the in-flight request and the caller receives `NOT_READY` so
/// its mailbox can admit shutdown or another lifecycle operation before it
/// retries the wait.
const CLIENT_WAIT_TIMEOUT: Duration = Duration::from_millis(100);
/// Bounds the number of in-flight client starts retained by one supervisor.
///
/// Each entry owns one Tokio task, a response channel, and the validated
/// request payload until the RPC completes. A finite ceiling prevents a caller
/// that forgets tickets from turning the supervisor into an unbounded task
/// registry; the caller can submit more work after polling or closing
/// completed tickets.
const MAX_PENDING_STARTS: usize = 64;
/// Maximum time spent in one wait-ticket ABI call before the owner regains
/// control of its mailbox and can service lifecycle messages.
const START_WAIT_TIMEOUT: Duration = Duration::from_millis(100);

/// Converts an asynchronous activity client error into the closed status
/// vocabulary understood by the OCaml lease state machine.
///
/// `NotFound` is a terminal outcome for a retained task token: Temporal has
/// already completed, cancelled, timed out, or otherwise discarded that
/// activity, so retrying the same request could never make it valid. Other RPC
/// failures do not prove whether the server consumed the request. They remain
/// a generic connection failure and are therefore fail-closed rather than
/// guessed to be retryable. The remote diagnostic is intentionally discarded
/// at this boundary so server-controlled text cannot enter the stable ABI.
fn async_activity_failure(operation: &str, error: AsyncActivityError) -> Failure {
    match error {
        AsyncActivityError::NotFound(_) => Failure {
            status: STATUS_INVALID_STATE,
            message: format!("Temporal asynchronous activity {operation} is no longer active"),
        },
        AsyncActivityError::Rpc(_) => Failure {
            status: STATUS_CONNECTION,
            message: format!("Temporal asynchronous activity {operation} failed"),
        },
        _ => Failure {
            status: STATUS_CONNECTION,
            message: format!("Temporal asynchronous activity {operation} failed"),
        },
    }
}

const _: () = assert!(size_of::<Status>() == 4);

/// Monotonic test instrumentation for successfully exposed runtime owners.
static RUNTIMES_CREATED: AtomicU64 = AtomicU64::new(0);
/// Monotonic test instrumentation for Core instances whose destructor ran.
static RUNTIMES_CLEANED: AtomicU64 = AtomicU64::new(0);

/// Runs one exact-run history request for a bounded interval.
///
/// A Temporal history long poll can otherwise hold the single supervisor
/// owner Domain inside `Handle::block_on` until the workflow closes.  The
/// timeout is applied outside the Core request so its cancellation drops the
/// tonic future, rather than leaving a detached native operation alive.  A
/// timeout is an expected pending result (`Ok(None)`), while request errors
/// continue through the existing typed client-error conversion path.
async fn bounded_client_wait<F>(
    future: F,
) -> std::result::Result<
    Option<client_protocol::WaitWorkflowResponse>,
    client_protocol::ClientOperationError,
>
where
    F: Future<
        Output = std::result::Result<
            client_protocol::WaitWorkflowResponse,
            client_protocol::ClientOperationError,
        >,
    >,
{
    match tokio::time::timeout(CLIENT_WAIT_TIMEOUT, future).await {
        Ok(response) => response.map(Some),
        Err(_) => Ok(None),
    }
}

/// Byte allocation owned by the Rust bridge.
///
/// Callers must treat this as an opaque field of [`Result`] and release it
/// only through [`ocaml_temporal_core_v2_result_free`].
#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct Buffer {
    pub ptr: *mut u8,
    pub len: usize,
}

impl Default for Buffer {
    /// Returns the canonical empty buffer with no allocation ownership.
    fn default() -> Self {
        Self {
            ptr: ptr::null_mut(),
            len: 0,
        }
    }
}

impl Buffer {
    /// Transfers a vector allocation into the ABI, canonicalizing empty input.
    fn from_vec(value: Vec<u8>) -> Self {
        if value.is_empty() {
            return Self::default();
        }

        let len = value.len();
        let ptr = Box::into_raw(value.into_boxed_slice()).cast::<u8>();
        Self { ptr, len }
    }
}

/// Single result shape returned by every fallible ABI operation.
///
/// Exactly one of `value` or `error` can own bytes. A successful operation has
/// `status == STATUS_OK`; every other status may carry a UTF-8 diagnostic in
/// `error`.
#[repr(C)]
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Result {
    pub status: Status,
    pub value: Buffer,
    pub error: Buffer,
}

const _: () = {
    assert!(std::mem::offset_of!(Buffer, ptr) == 0);
    assert!(std::mem::offset_of!(Buffer, len) == size_of::<*mut u8>());
    assert!(std::mem::offset_of!(Result, status) == 0);
    assert!(std::mem::offset_of!(Result, value) == align_of::<Buffer>());
    assert!(
        std::mem::offset_of!(Result, error)
            == std::mem::offset_of!(Result, value) + size_of::<Buffer>()
    );
};

/// Internal structured failure converted into an owned ABI diagnostic.
#[derive(Debug)]
struct Failure {
    status: Status,
    message: String,
}

/// Owns the Tokio executor and shared Temporal Core runtime for one SDK instance.
///
/// The type is opaque to C. Higher-level client and worker handles will retain
/// the same runtime owner rather than creating independent executors.
pub struct Runtime {
    core: Option<CoreRuntime>,
    client: Option<Connection>,
    worker: Option<PollLanes>,
    /// Optional workflow-only replay graph. It is mutually exclusive with the
    /// live worker and remains owned by this runtime's supervisor Domain.
    replay_worker: Option<ReplayWorker>,
    /// Namespace passed to Core for child-workflow commands in the active
    /// worker graph. The semantic OCaml command intentionally omits this
    /// worker-scoped setting, so retaining it here prevents Core from using
    /// its empty default and later emitting child failures that cannot cross
    /// the strict semantic protocol.
    worker_namespace: Option<String>,
    workflow_activations: HashMap<String, workflow_protocol::Activation>,
    activity_tasks: HashMap<Vec<u8>, Vec<activity_protocol::ActivityTask>>,
    pending_starts: HashMap<String, PendingStart>,
    cleanup: std::sync::mpsc::Sender<RuntimeCleanup>,
}

/// One Rust-owned asynchronous start operation indexed by an opaque ticket.
///
/// The response channel is a one-shot handoff from a Tokio task to the sole
/// runtime owner. No Tokio task invokes OCaml or mutates [`Runtime`]; it only
/// sends the typed result and exits. The owner removes the entry exactly once
/// when a terminal result is observed, then joins the task before releasing
/// the connection clone it captured.
struct PendingStart {
    /// Complete validated request retained for exact retry matching and for
    /// the request/workflow identifiers used by an `Unknown` outcome.  Keeping
    /// the typed value (rather than only namespace and workflow ID) prevents a
    /// caller from accidentally reusing one request ID for a different task
    /// queue, workflow type, or payload and silently receiving the old ticket.
    request: Arc<client_protocol::StartWorkflowRequest>,
    /// One-shot result channel serviced only by the owner Domain.
    receiver: Receiver<
        std::result::Result<
            client_protocol::StartWorkflowResponse,
            client_protocol::ClientOperationError,
        >,
    >,
    /// Tokio task performing the network call through Core's runtime.
    task: JoinHandle<()>,
}

/// Result of one owner-side ticket-channel read.
enum StartRead {
    /// The Tokio task supplied a terminal typed result.
    Ready(
        std::result::Result<
            client_protocol::StartWorkflowResponse,
            client_protocol::ClientOperationError,
        >,
    ),
    /// No result is available in the selected poll interval.
    NotReady,
    /// The task exited without publishing a result, so acceptance is unknown.
    Disconnected,
}

/// Ownership transfer consumed by the runtime's dedicated cleanup thread.
struct RuntimeCleanup {
    core: CoreRuntime,
    client: Option<Connection>,
    worker: Option<PollLanes>,
    replay_worker: Option<ReplayWorker>,
    /// Aborted asynchronous-start tasks whose join handles must be awaited
    /// before the Core runtime and its connection clones are dropped.  The
    /// non-blocking OCaml finalizer transfers these handles here instead of
    /// dropping them on the caller thread, which would detach the tasks.
    pending_start_tasks: Vec<JoinHandle<()>>,
    completed: Option<SyncSender<Status>>,
}

/// Strict client connection document received from OCaml as UTF-8 JSON.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientConfig {
    target_url: String,
    identity: String,
}

/// Strict workflow-only Core worker document received from OCaml as JSON.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfigInput {
    namespace: String,
    task_queue: String,
    build_id: String,
    versioning: WorkerVersioningInput,
    max_cached_workflows: u32,
    max_outstanding_workflow_tasks: u32,
    max_concurrent_workflow_task_polls: u32,
    graceful_shutdown_timeout_ms: u64,
}

/// Explicit worker-routing mode carried across the OCaml/Rust JSON boundary.
/// The tag is intentionally closed so a typo cannot silently fall back to an
/// unversioned worker.  The top-level build ID is retained for both modes and
/// the legacy payload must repeat the same value; this bilateral consistency
/// check prevents routing metadata from diverging from worker identity.
#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum WorkerVersioningInput {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "legacy_build_id")]
    LegacyBuildId { build_id: String },
    #[serde(rename = "deployment_based")]
    DeploymentBased {
        deployment_name: String,
        build_id: String,
        use_worker_versioning: bool,
        default_versioning_behavior: Option<String>,
    },
}

impl Runtime {
    /// Starts the cleanup thread before exposing a handle, so every successful
    /// runtime allocation already has a non-blocking GC fallback path.
    fn new(core: CoreRuntime) -> std::result::Result<Self, Failure> {
        let (cleanup, receiver) = channel();
        std::thread::Builder::new()
            .name("ocaml-temporal-runtime-cleanup".to_owned())
            .spawn(move || run_runtime_cleanup(receiver))
            .map_err(|error| Failure {
                status: STATUS_INTERNAL,
                message: format!("could not start Temporal runtime cleanup thread: {error}"),
            })?;
        RUNTIMES_CREATED.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            core: Some(core),
            client: None,
            worker: None,
            replay_worker: None,
            worker_namespace: None,
            workflow_activations: HashMap::new(),
            activity_tasks: HashMap::new(),
            pending_starts: HashMap::new(),
            cleanup,
        })
    }

    /// Connects one official Core client without publishing partial state.
    fn connect_client(&mut self, config: ClientConfig) -> Operation {
        if self.client.is_some() {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal client is already connected".to_owned(),
            });
        }
        let core = self.core.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal runtime is already closed".to_owned(),
        })?;
        let target =
            temporalio_sdk_core::Url::parse(&config.target_url).map_err(|error| Failure {
                status: STATUS_CONFIGURATION,
                message: format!("client target_url is invalid: {error}"),
            })?;
        if !matches!(target.scheme(), "http" | "https") || target.host_str().is_none() {
            return Err(Failure {
                status: STATUS_CONFIGURATION,
                message: "client target_url must be an absolute http or https URL".to_owned(),
            });
        }
        validate_identifier("identity", &config.identity)?;

        let options = ConnectionOptions::new(target)
            .identity(config.identity)
            .maybe_metrics_meter(core.telemetry().get_temporal_metric_meter())
            .build();
        let connection = core
            .tokio_handle()
            .block_on(Connection::connect(options))
            .map_err(|_error| Failure {
                status: STATUS_CONNECTION,
                // Core's connection error can contain gRPC status details or
                // server-provided text.  Only the closed ABI category may
                // cross into OCaml; detailed diagnostics stay inside Rust.
                message: "Temporal client connection failed".to_owned(),
            })?;
        self.client = Some(connection);
        Ok(Vec::new())
    }

    /// Starts one dynamically named workflow through the connected Core
    /// client and returns a strict execution-reference JSON document.
    ///
    /// The request and response are intentionally separate from the public
    /// OCaml `Client` API.  This ABI slice is a low-level adapter that keeps
    /// dynamic workflow names and opaque payloads out of Rust's typed
    /// workflow-definition API until the native OCaml adapter is complete.
    fn start_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_start_request(text).map_err(protocol_failure)?;
        // The async begin path owns one Tokio task per request_id in
        // `pending_starts`. A concurrent sync start with the same logical ID
        // must not issue a second StartWorkflowExecution while that ticket is
        // live; callers must poll/wait the existing ticket instead.
        if let Some((_, pending)) = self
            .pending_starts
            .iter()
            .find(|(_, pending)| pending.request.request_id == request.request_id)
        {
            if !client_protocol::same_start_request(pending.request.as_ref(), &request) {
                return Err(Failure {
                    status: STATUS_PROTOCOL,
                    message: "start request_id is already pending for a different request"
                        .to_owned(),
                });
            }
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message:
                    "Temporal workflow start is already pending for this request_id; poll or wait the async ticket"
                        .to_owned(),
            });
        }
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        // Capture validated request identities before the request is moved into
        // the Core start call so an unencodable server response can still report
        // an Unknown outcome rather than dropping an accepted start.
        let request_id = request.request_id.clone();
        let workflow_id = request.workflow_id.clone();
        let response = handle
            .block_on(client_protocol::start_workflow(connection, request))
            .map_err(client_operation_failure)?;
        match client_protocol::encode_start_response(&response) {
            Ok(encoded) => Ok(encoded.into_bytes()),
            Err(_) => {
                let fallback = client_protocol::StartWorkflowOutcome::Unknown {
                    request_id,
                    workflow_id,
                };
                let encoded =
                    client_protocol::encode_start_outcome(&fallback).map_err(protocol_failure)?;
                Ok(encoded.into_bytes())
            }
        }
    }

    /// Requests cancellation of one exact workflow run through the official
    /// Temporal workflow service. The owner Domain performs the bounded RPC;
    /// the C stub releases the OCaml runtime lock while Tokio waits, and no
    /// Rust task retains an OCaml pointer after this method returns.
    fn cancel_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_cancel_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        let response = handle
            .block_on(client_protocol::cancel_workflow(connection, request))
            .map_err(client_operation_failure)?;
        client_protocol::encode_cancel_response(&response)
            .map(|encoded| encoded.into_bytes())
            .map_err(protocol_failure)
    }

    /// Terminates one exact workflow run through the official Temporal
    /// service. The bounded RPC runs on Tokio while the C shim releases the
    /// OCaml runtime lock, and only an owned acknowledgement crosses back.
    fn terminate_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_terminate_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        let response = handle
            .block_on(client_protocol::terminate_workflow(connection, request))
            .map_err(client_operation_failure)?;
        client_protocol::encode_terminate_response(&response)
            .map(|encoded| encoded.into_bytes())
            .map_err(protocol_failure)
    }

    /// Resets one exact workflow run through Temporal's official workflow
    /// service and returns the newly created run identity. The owner Domain
    /// performs the bounded RPC; no Tokio task or native callback retains an
    /// OCaml pointer after this method returns.
    fn reset_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_reset_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        let response = handle
            .block_on(client_protocol::reset_workflow(connection, request))
            .map_err(client_operation_failure)?;
        client_protocol::encode_reset_response(&response)
            .map(|encoded| encoded.into_bytes())
            .map_err(protocol_failure)
    }

    /// Sends one signal to one exact workflow run through the connected
    /// Temporal client. The request is validated before the connection lookup
    /// and the positive response is revalidated before crossing the ABI.
    fn signal_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_signal_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        let response = handle
            .block_on(client_protocol::signal_workflow(connection, request))
            .map_err(client_operation_failure)?;
        client_protocol::encode_signal_response(&response)
            .map(|encoded| encoded.into_bytes())
            .map_err(protocol_failure)
    }

    /// Executes one output-only query against one exact workflow run. Query
    /// rejection is converted to the same structured client-error JSON used
    /// by cancellation and signal operations; successful payloads are
    /// revalidated before crossing the ABI.
    fn query_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_query_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        let response = handle
            .block_on(client_protocol::query_workflow(connection, request))
            .map_err(client_operation_failure)?;
        client_protocol::encode_query_response(&response)
            .map(|encoded| encoded.into_bytes())
            .map_err(protocol_failure)
    }

    /// Starts one named workflow update and returns its durable update handle.
    /// Completion is observed through the separate bounded poll operation.
    fn update_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_update_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        let response = handle
            .block_on(client_protocol::update_workflow(connection, request))
            .map_err(client_operation_failure)?;
        client_protocol::encode_update_response(&response)
            .map(|encoded| encoded.into_bytes())
            .map_err(protocol_failure)
    }

    /// Polls one admitted update until completion or the bounded poll budget
    /// expires. A missing outcome is a normal pending result.
    fn poll_update_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request =
            client_protocol::decode_poll_update_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        let response = handle
            .block_on(client_protocol::poll_workflow_update(connection, request))
            .map_err(client_operation_failure)?;
        client_protocol::encode_poll_update_response(&response)
            .map(|encoded| encoded.into_bytes())
            .map_err(protocol_failure)
    }

    /// Lists one bounded visibility page through Temporal's official client.
    /// The Rust owner performs protobuf conversion and returns only the
    /// validated JSON page; no server-owned bytes or handles cross the ABI.
    fn list_visibility_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_visibility_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        let response = handle
            .block_on(client_protocol::list_visibility(connection, request))
            .map_err(client_operation_failure)?;
        client_protocol::encode_visibility_response(&response)
            .map(|encoded| encoded.into_bytes())
            .map_err(protocol_failure)
    }

    /// Begins one workflow start without waiting for the RPC response.
    ///
    /// The owner Domain performs only validation, ticket bookkeeping, and
    /// Tokio task admission here. The task owns a cloned Core connection and
    /// sends exactly one typed result over a standard-library channel; it
    /// never touches this runtime or calls into OCaml. A repeated request ID
    /// while an operation is pending returns the original ticket, preventing a
    /// caller retry from issuing a second Temporal start.
    fn begin_start_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_start_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();

        // A caller that retries a begin request with the same logical ID must
        // observe the existing ticket only when every validated request field
        // is identical. Reusing an ID for a changed workflow request is
        // rejected rather than silently aliasing the other operation.
        let request = Arc::new(request);
        if let Some((ticket, pending)) = self
            .pending_starts
            .iter()
            .find(|(_, pending)| pending.request.request_id == request.request_id)
        {
            if !client_protocol::same_start_request(pending.request.as_ref(), request.as_ref()) {
                return Err(Failure {
                    status: STATUS_PROTOCOL,
                    message: "start request_id is already pending for a different request"
                        .to_owned(),
                });
            }
            return Ok(client_protocol::encode_start_ticket(ticket)
                .map_err(protocol_failure)?
                .into_bytes());
        }
        if self.pending_starts.len() >= MAX_PENDING_STARTS {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "too many Temporal workflow starts are pending".to_owned(),
            });
        }

        // Reserve the registry slot before spawning Tokio work.  Once the
        // task owns a Core connection clone, every unwind path must find its
        // JoinHandle in `pending_starts` so shutdown can abort and join it.
        // `try_reserve` turns a recoverable allocation failure into a typed
        // operation error instead of leaving a detached task behind if the
        // later map insertion were to trigger allocation.
        reserve_pending_start_slots(&mut self.pending_starts, 1)?;

        let ticket = loop {
            let candidate = Uuid::new_v4().to_string();
            if !self.pending_starts.contains_key(&candidate) {
                break candidate;
            }
        };
        let encoded_ticket = client_protocol::encode_start_ticket(&ticket)
            .map_err(protocol_failure)?
            .into_bytes();
        let (sender, receiver) = channel();
        let task_request = Arc::clone(&request);
        let task = handle.spawn(async move {
            let result =
                client_protocol::start_workflow(connection, task_request.as_ref().clone()).await;
            // A closed receiver means the owner is shutting down or has
            // already consumed the terminal result. Dropping this send is
            // intentional; task ownership remains entirely on the Tokio side.
            let _ = sender.send(result);
        });

        // The ticket is moved rather than cloned at this boundary.  After the
        // task starts, no avoidable allocation may unwind before its handle is
        // registered for shutdown cleanup.
        self.pending_starts.insert(
            ticket,
            PendingStart {
                request,
                receiver,
                task,
            },
        );
        Ok(encoded_ticket)
    }

    /// Polls one start ticket without waiting for the RPC task.
    fn poll_start_workflow_json(&mut self, input: &[u8]) -> Operation {
        self.read_start_workflow_json(input, false)
    }

    /// Waits up to the bounded ticket interval, then returns control to the
    /// supervisor even when Temporal has not produced a response yet.
    fn wait_start_workflow_json(&mut self, input: &[u8]) -> Operation {
        self.read_start_workflow_json(input, true)
    }

    /// Shared ticket decoder and one-shot channel handoff for poll and wait.
    fn read_start_workflow_json(&mut self, input: &[u8], wait: bool) -> Operation {
        let text = decode_semantic_input(input)?;
        let ticket = client_protocol::decode_start_ticket(text).map_err(protocol_failure)?;
        // Ticket reads are meaningful only while the client connection is
        // live.  A ticket supplied before connection setup cannot have been
        // admitted by this runtime, but returning the lifecycle error is more
        // useful and consistent with begin/start/wait operations than exposing
        // the implementation detail that the pending-ticket map is empty.
        self.client.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let read = {
            let pending = self
                .pending_starts
                .get_mut(&ticket)
                .ok_or_else(|| Failure {
                    status: STATUS_PROTOCOL,
                    message: "unknown Temporal workflow start ticket".to_owned(),
                })?;
            if wait {
                match pending.receiver.recv_timeout(START_WAIT_TIMEOUT) {
                    Ok(result) => StartRead::Ready(result),
                    Err(RecvTimeoutError::Timeout) => StartRead::NotReady,
                    Err(RecvTimeoutError::Disconnected) => StartRead::Disconnected,
                }
            } else {
                match pending.receiver.try_recv() {
                    Ok(result) => StartRead::Ready(result),
                    Err(TryRecvError::Empty) => StartRead::NotReady,
                    Err(TryRecvError::Disconnected) => StartRead::Disconnected,
                }
            }
        };

        match read {
            StartRead::NotReady => Err(not_ready()),
            StartRead::Ready(result) => self.finish_start(ticket, Some(result)),
            StartRead::Disconnected => self.finish_start(ticket, None),
        }
    }

    /// Removes a terminal ticket, joins its Tokio task, and encodes the
    /// explicit accepted/rejected/unknown outcome. Joining after the channel
    /// handoff proves the task no longer holds a Core connection clone before
    /// the map entry is released.
    fn finish_start(
        &mut self,
        ticket: String,
        result: Option<
            std::result::Result<
                client_protocol::StartWorkflowResponse,
                client_protocol::ClientOperationError,
            >,
        >,
    ) -> Operation {
        let pending = self.pending_starts.remove(&ticket).ok_or_else(|| Failure {
            status: STATUS_PROTOCOL,
            message: "unknown Temporal workflow start ticket".to_owned(),
        })?;

        if let Some(core) = self.core.as_ref() {
            // The task has sent its one-shot result before this join. The
            // bounded join therefore only drains its final bookkeeping and
            // cannot wait on a second network operation.
            let _ = core.tokio_handle().block_on(pending.task);
        } else {
            // Runtime closure is serialized through the same owner, but keep a
            // defensive abort path if an internal caller ever reaches this
            // method after Core ownership was removed.
            pending.task.abort();
        }

        let request_id = pending.request.request_id.clone();
        let workflow_id = pending.request.workflow_id.clone();
        let outcome = match result {
            Some(Ok(response)) => client_protocol::StartWorkflowOutcome::Accepted(response),
            Some(Err(error)) if error.uncertain_start() => {
                client_protocol::StartWorkflowOutcome::Unknown {
                    request_id: request_id.clone(),
                    workflow_id: workflow_id.clone(),
                }
            }
            Some(Err(error)) => client_protocol::StartWorkflowOutcome::Rejected(error),
            None => client_protocol::StartWorkflowOutcome::Unknown {
                request_id: request_id.clone(),
                workflow_id: workflow_id.clone(),
            },
        };
        // The ticket is already retired. Prefer an always-encodable Unknown
        // outcome built from the validated request identities over returning a
        // protocol error that permanently loses an accepted or rejected start.
        match client_protocol::encode_start_outcome(&outcome) {
            Ok(encoded) => Ok(encoded.into_bytes()),
            Err(_) => {
                let fallback = client_protocol::StartWorkflowOutcome::Unknown {
                    request_id,
                    workflow_id,
                };
                client_protocol::encode_start_outcome(&fallback)
                    .map(|encoded| encoded.into_bytes())
                    .map_err(protocol_failure)
            }
        }
    }

    /// Aborts every in-flight start and returns handles that need cleanup.
    ///
    /// Explicit shutdown (`wait = true`) joins on the owner thread while the
    /// C stub has released the OCaml runtime lock.  GC fallback (`wait = false`)
    /// must not block the finalizer, so it returns the already-aborted handles
    /// for the dedicated cleanup thread to join before Core is released.  In
    /// either mode no task is detached while it still owns a Core connection.
    fn abort_pending_starts(&mut self, wait: bool) -> Vec<JoinHandle<()>> {
        let handle = self.core.as_ref().map(|core| core.tokio_handle());
        let tasks = self
            .pending_starts
            .drain()
            .map(|(_, pending)| pending.task)
            .collect::<Vec<_>>();
        for task in &tasks {
            task.abort();
        }
        if wait {
            if let Some(handle) = handle {
                handle.block_on(async {
                    for task in tasks {
                        let _ = task.await;
                    }
                });
            }
            Vec::new()
        } else {
            tasks
        }
    }

    /// Waits for one exact run with fixed `follow_runs = false` semantics.
    ///
    /// A continued-as-new close event is returned as a terminal outcome with
    /// successor metadata; the bridge never follows it implicitly.
    fn wait_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_wait_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        let response = handle
            .block_on(bounded_client_wait(client_protocol::wait_workflow(
                connection, request,
            )))
            .map_err(client_operation_failure)?;
        let response = response.ok_or_else(client_wait_not_ready)?;
        let encoded = client_protocol::encode_wait_response(&response).map_err(protocol_failure)?;
        Ok(encoded.into_bytes())
    }

    /// Constructs and validates one official workflow-only Core worker.
    ///
    /// Validation performs the namespace RPC before the worker enters the
    /// graph. Any construction or validation failure consumes the temporary
    /// worker and leaves the connected client available for a corrected retry.
    fn start_worker(&mut self, config: WorkerConfigInput) -> Operation {
        if self.worker.is_some() {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal workflow worker is already running".to_owned(),
            });
        }
        if self.replay_worker.is_some() {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal replay worker is already running".to_owned(),
            });
        }
        let client = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        // Keep the validated worker namespace beside the native graph. Child
        // workflow commands do not expose namespace in the language-level
        // command, but Core needs the worker namespace to populate failure
        // metadata consistently when a child is cancelled before it starts.
        let worker_namespace = config.namespace.clone();
        let worker_config = config.into_core()?;
        let core = self.core.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal runtime is already closed".to_owned(),
        })?;
        let handle = core.tokio_handle();
        let worker = {
            // Core's synchronous constructor creates poll buffers with
            // `tokio::spawn`. Entering the owned executor here is therefore a
            // construction precondition, even though only validation exposes
            // an async function in Core's public API.
            let _runtime_guard = handle.enter();
            temporalio_sdk_core::init_worker(core, worker_config, client)
        }
        .map_err(|_error| Failure {
            status: STATUS_WORKER,
            // Worker construction errors originate in Core and may include
            // endpoint or server details.  Do not serialize that text into
            // the public result buffer.
            message: "Temporal workflow worker construction failed".to_owned(),
        })?;

        if handle.block_on(worker.validate()).is_err() {
            {
                // Core's synchronous shutdown initiation spawns its server
                // deregistration task, so it needs the same runtime context
                // as synchronous worker construction.
                let _runtime_guard = handle.enter();
                worker.initiate_shutdown();
            }
            handle.block_on(worker.finalize_shutdown());
            return Err(Failure {
                status: STATUS_WORKER,
                // Validation performs Core/Server work, so its diagnostic is
                // subject to the same closed-category boundary as creation.
                message: "Temporal workflow worker validation failed".to_owned(),
            });
        }
        self.worker = Some(PollLanes::start(worker, &handle));
        self.worker_namespace = Some(worker_namespace);
        Ok(Vec::new())
    }

    /// Constructs the workflow-only Core replay worker without requiring a
    /// Temporal client connection. A runtime admits at most one worker graph:
    /// replay and live workers cannot overlap or compete for Core shutdown.
    fn start_replay_worker(&mut self, config: WorkerConfigInput) -> Operation {
        if self.worker.is_some() {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal workflow worker is already running".to_owned(),
            });
        }
        if self.replay_worker.is_some() {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal replay worker is already running".to_owned(),
            });
        }
        let worker_namespace = config.namespace.clone();
        let worker_config = config.into_core()?;
        let core = self.core.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal runtime is already closed".to_owned(),
        })?;
        let replay = ReplayWorker::start(core, worker_config).map_err(replay_worker_failure)?;
        self.replay_worker = Some(replay);
        self.worker_namespace = Some(worker_namespace);
        Ok(Vec::new())
    }

    /// Gracefully finalizes the child worker once; absence is already closed.
    fn shutdown_worker(&mut self) -> Operation {
        let Some(worker) = self.worker.as_mut() else {
            return Ok(Vec::new());
        };
        let handle = self
            .core
            .as_ref()
            .expect("a live worker always has its parent runtime")
            .tokio_handle();
        {
            // `initiate_shutdown` synchronously calls `tokio::spawn`; keep
            // the guard narrower than the subsequent blocking finalization.
            let _runtime_guard = handle.enter();
            worker.initiate_shutdown();
        }
        handle
            .block_on(worker.join_poll_lanes())
            .map_err(poll_lane_failure)?;
        if !worker.can_finalize() {
            return Err(Failure {
                status: STATUS_OUTSTANDING_TASKS,
                message: "Temporal worker is draining outstanding workflow or activity tasks"
                    .to_owned(),
            });
        }
        let worker = self
            .worker
            .take()
            .expect("checked worker remains owned until terminal finalization");
        match handle.block_on(worker.finalize()) {
            Ok(()) => {
                self.worker_namespace = None;
                // Semantic handoffs belong to the worker that created them.
                // Drop them with the worker so a later start on this runtime
                // cannot treat a recycled run ID or task token as a duplicate.
                self.workflow_activations.clear();
                self.activity_tasks.clear();
                Ok(Vec::new())
            }
            Err((worker, error)) => {
                // Put the worker back so the language side can finish outstanding
                // tasks and retry graceful shutdown instead of losing the graph.
                self.worker = Some(worker);
                Err(worker_bridge_failure(error))
            }
        }
    }

    /// Takes one workflow activation from the Rust lane without waiting.
    fn try_poll_workflow(&mut self) -> Operation {
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle()
            .clone();
        let worker = self.worker.as_mut().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let activation = worker
            .try_take_workflow(&handle)
            .ok_or_else(not_ready)?
            .map_err(poll_lane_failure)?;
        let semantic = match workflow_protocol::activation_from_core(&activation) {
            Ok(semantic) => semantic,
            Err(error) => {
                self.reject_workflow_delivery_with_reason(&activation.run_id, error.message)?;
                return Err(core_conversion_failure(error));
            }
        };
        let encoded = match workflow_protocol::encode_activation(&semantic) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.reject_workflow_delivery_with_reason(
                    &activation.run_id,
                    "semantic activation JSON encoding failed",
                )?;
                return Err(protocol_failure(error));
            }
        };
        // Retain must not leave a leased ledger entry without a handoff
        // document. On duplicate-lease retain failure, reject Core the same
        // way conversion failures do.
        if let Err(error) = retain_workflow_activation(&mut self.workflow_activations, semantic) {
            self.reject_workflow_delivery_with_reason(
                &activation.run_id,
                "workflow activation lease was duplicated",
            )?;
            return Err(error);
        }
        Ok(encoded.into_bytes())
    }

    /// Takes one replay activation, applies the same strict semantic
    /// conversion as the live worker, and retains its lease until completion.
    fn try_poll_replay_workflow(&mut self) -> Operation {
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle()
            .clone();
        let activation = {
            let worker = self.replay_worker.as_mut().ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal replay worker is not running".to_owned(),
            })?;
            worker
                .try_take_workflow(&handle)
                .ok_or_else(not_ready)?
                .map_err(poll_lane_failure)?
        };
        let semantic = match workflow_protocol::activation_from_core(&activation) {
            Ok(semantic) => semantic,
            Err(error) => {
                self.reject_replay_workflow_delivery(
                    &activation.run_id,
                    "semantic replay activation conversion failed",
                )?;
                return Err(core_conversion_failure(error));
            }
        };
        let encoded = match workflow_protocol::encode_activation(&semantic) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.reject_replay_workflow_delivery(
                    &activation.run_id,
                    "semantic replay activation JSON encoding failed",
                )?;
                return Err(protocol_failure(error));
            }
        };
        if let Err(error) = retain_workflow_activation(&mut self.workflow_activations, semantic) {
            self.reject_replay_workflow_delivery(
                &activation.run_id,
                "replay workflow activation lease was duplicated",
            )?;
            return Err(error);
        }
        Ok(encoded.into_bytes())
    }

    /// Waits for workflow-lane readiness without consuming the queued task.
    ///
    /// The native C stub invokes this while the OCaml runtime lock is released.
    /// A bounded timeout keeps a supervisor mailbox responsive to lifecycle
    /// commands when Core is quiet; callers retry after `STATUS_NOT_READY`.
    fn wait_workflow(&self) -> Operation {
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        match worker.wait_workflow() {
            ReadinessWait::Ready => Ok(Vec::new()),
            ReadinessWait::TimedOut => Err(Failure {
                status: STATUS_NOT_READY,
                message: "Temporal workflow readiness wait timed out; retry".to_owned(),
            }),
            ReadinessWait::Shutdown => Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal workflow readiness wait ended during worker shutdown".to_owned(),
            }),
            ReadinessWait::Error(error) => Err(poll_lane_failure(error)),
        }
    }

    /// Waits for replay activation readiness and records the terminal shutdown
    /// observation needed by the normal replay finalizer.
    fn wait_replay_workflow(&mut self) -> Operation {
        let worker = self.replay_worker.as_mut().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal replay worker is not running".to_owned(),
        })?;
        match worker.wait_workflow() {
            ReadinessWait::Ready => Ok(Vec::new()),
            ReadinessWait::TimedOut => Err(Failure {
                status: STATUS_NOT_READY,
                message: "Temporal replay readiness wait timed out; retry".to_owned(),
            }),
            ReadinessWait::Shutdown => Ok(Vec::new()),
            ReadinessWait::Error(error) => Err(poll_lane_failure(error)),
        }
    }

    /// Queues one strictly validated replay history in Core's bounded feeder.
    fn feed_replay_history_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle()
            .clone();
        let worker = self.replay_worker.as_mut().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal replay worker is not running".to_owned(),
        })?;
        worker
            .feed_json(&handle, text)
            .map_err(replay_worker_failure)
            .map(|()| Vec::new())
    }

    /// Closes the bounded replay input stream. Repetition is idempotent when a
    /// replay worker is still present; the absent graph is already closed.
    fn finish_replay_input(&mut self) -> Operation {
        if let Some(worker) = self.replay_worker.as_mut() {
            worker.finish_input();
        }
        Ok(Vec::new())
    }

    /// Completes one replay activation after strict semantic validation and
    /// removes its one-shot lease only after Core accepts the completion.
    fn complete_replay_workflow(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let semantic = workflow_protocol::decode_completion(text).map_err(protocol_failure)?;
        let activation = self
            .workflow_activations
            .get(&semantic.run_id)
            .ok_or_else(|| Failure {
                status: STATUS_PROTOCOL,
                message: "replay workflow completion does not match a leased activation".to_owned(),
            })?;
        let worker_namespace = self.worker_namespace.as_deref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal replay worker namespace is not available".to_owned(),
        })?;
        let completion = workflow_protocol::completion_to_core_for_activation_with_namespace(
            activation,
            &semantic,
            worker_namespace,
        )
        .map_err(core_conversion_failure)?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle()
            .clone();
        let worker = self.replay_worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal replay worker is not running".to_owned(),
        })?;
        handle
            .block_on(worker.complete_workflow(completion))
            .map_err(replay_worker_failure)?;
        self.workflow_activations.remove(&semantic.run_id);
        Ok(Vec::new())
    }

    /// Retires a replay activation that could not be decoded by OCaml while
    /// retaining the exact semantic lease check used by live workers.
    fn reject_replay_workflow(&mut self, input: &[u8]) -> Operation {
        let run_id = workflow_rejection_run_id(&self.workflow_activations, input)?;
        // Match live reject_polled_workflow: retire the semantic handoff even
        // when Core returns an error, so a failed reject cannot leave a stale
        // run_id that blocks later work on this runtime.
        let rejection = self.reject_replay_workflow_delivery(
            &run_id,
            "semantic replay activation conversion failed",
        );
        self.workflow_activations.remove(&run_id);
        rejection?;
        Ok(Vec::new())
    }

    /// Uses the replay worker's Rust-owned completion path for a failed
    /// semantic handoff; this never exposes a Core or Tokio handle to OCaml.
    fn reject_replay_workflow_delivery(
        &self,
        run_id: &str,
        reason: &'static str,
    ) -> std::result::Result<(), Failure> {
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle()
            .clone();
        let worker = self.replay_worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal replay worker is not running".to_owned(),
        })?;
        handle
            .block_on(worker.reject_workflow_delivery(run_id, reason))
            .map_err(replay_worker_failure)
    }

    /// Finalizes a naturally drained replay and retains it on every failure.
    fn finalize_replay(&mut self) -> Operation {
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle()
            .clone();
        let worker = self.replay_worker.take().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal replay worker is not running".to_owned(),
        })?;
        match handle.block_on(worker.finalize(&handle)) {
            Ok(()) => {
                self.worker_namespace = None;
                self.workflow_activations.clear();
                self.activity_tasks.clear();
                Ok(Vec::new())
            }
            Err((worker, error)) => {
                self.replay_worker = Some(worker);
                Err(replay_worker_failure(error))
            }
        }
    }

    /// Explicitly abandons a replay, force-completing its native debts and
    /// retaining the worker if Core cannot finalize it yet.
    fn dispose_replay(&mut self) -> Operation {
        if self.replay_worker.is_none() {
            return Ok(Vec::new());
        }
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle()
            .clone();
        let worker = self
            .replay_worker
            .take()
            .expect("replay worker was checked before disposal");
        match handle.block_on(worker.dispose(&handle)) {
            Ok(()) => {
                self.worker_namespace = None;
                self.workflow_activations.clear();
                self.activity_tasks.clear();
                Ok(Vec::new())
            }
            Err((worker, error)) => {
                self.replay_worker = Some(worker);
                Err(replay_worker_failure(error))
            }
        }
    }

    /// Strictly validates and submits one leased workflow completion.
    fn complete_workflow(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let semantic = workflow_protocol::decode_completion(text).map_err(protocol_failure)?;
        let activation = self
            .workflow_activations
            .get(&semantic.run_id)
            .ok_or_else(|| Failure {
                status: STATUS_PROTOCOL,
                message: "workflow completion does not match a leased activation".to_owned(),
            })?;
        let worker_namespace = self.worker_namespace.as_deref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal workflow worker namespace is not available".to_owned(),
        })?;
        let completion = workflow_protocol::completion_to_core_for_activation_with_namespace(
            activation,
            &semantic,
            worker_namespace,
        )
        .map_err(core_conversion_failure)?;
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .expect("worker retains parent runtime")
            .tokio_handle();
        handle
            .block_on(worker.complete_workflow(completion))
            .map_err(worker_bridge_failure)?;
        self.workflow_activations.remove(&semantic.run_id);
        Ok(Vec::new())
    }

    /// Revalidates and retires the exact workflow activation whose OCaml
    /// semantic decode failed after Rust handed off its lease.
    fn reject_polled_workflow(&mut self, input: &[u8]) -> Operation {
        let run_id = workflow_rejection_run_id(&self.workflow_activations, input)?;
        // reject_workflow_delivery retires the ledger even when Core rejects
        // the generated failure. Remove the semantic handoff on the same path
        // so a Core error cannot leave a stale run_id that blocks later work.
        let rejection = self.reject_workflow_delivery(&run_id);
        self.workflow_activations.remove(&run_id);
        rejection?;
        Ok(Vec::new())
    }

    /// Takes one remote activity task from its independent lane without waiting.
    fn try_poll_activity(&mut self) -> Operation {
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle()
            .clone();
        let worker = self.worker.as_mut().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let task = worker
            .try_take_activity(&handle)
            .ok_or_else(not_ready)?
            .map_err(poll_lane_failure)?;
        let semantic = match activity_protocol::task_from_core(&task) {
            Ok(semantic) => semantic,
            Err(error) => {
                self.reject_unrepresentable_activity(&task)?;
                return Err(core_conversion_failure(error));
            }
        };
        match activity_protocol::encode_task(&semantic) {
            Ok(encoded) => {
                retain_activity_task(&mut self.activity_tasks, task.task_token.clone(), semantic);
                Ok(encoded.into_bytes())
            }
            Err(error) => {
                self.reject_unrepresentable_activity(&task)?;
                Err(protocol_failure(error))
            }
        }
    }

    /// Rejects an activity task that cannot cross the semantic protocol.
    ///
    /// Core's Start variant owns the activity's single completion debt and
    /// must be failed so an unrepresentable task cannot block shutdown. A
    /// Cancel variant is only an update to that Start; it has no independent
    /// completion to fail, so dropping a malformed update preserves the
    /// in-flight Start lease for the activity implementation.
    fn reject_unrepresentable_activity(
        &self,
        task: &CoreActivityTask,
    ) -> std::result::Result<(), Failure> {
        if activity_task_owns_completion_debt(task) {
            self.reject_activity_delivery(&task.task_token)
        } else {
            Ok(())
        }
    }

    /// Waits for remote-activity-lane readiness without consuming its task.
    ///
    /// As with workflow readiness, this is a blocking native call with a
    /// bounded duration so the owner supervisor can process shutdown messages.
    fn wait_activity(&self) -> Operation {
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        match worker.wait_activity() {
            ReadinessWait::Ready => Ok(Vec::new()),
            ReadinessWait::TimedOut => Err(Failure {
                status: STATUS_NOT_READY,
                message: "Temporal activity readiness wait timed out; retry".to_owned(),
            }),
            ReadinessWait::Shutdown => Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal activity readiness wait ended during worker shutdown".to_owned(),
            }),
            ReadinessWait::Error(error) => Err(poll_lane_failure(error)),
        }
    }

    /// Applies the fixed native backoff used only after an explicitly
    /// retryable activity-completion outcome.
    ///
    /// The delay executes on the supervisor owner Domain and is called through
    /// a C stub that releases the OCaml runtime lock. It therefore cannot block
    /// a workflow effect scheduler, and its fixed duration keeps shutdown
    /// admission bounded.
    fn wait_activity_completion_retry_backoff(&self) -> Operation {
        let Some(worker) = self.worker.as_ref() else {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal worker is not running".to_owned(),
            });
        };
        worker.wait_activity_completion_retry_backoff();
        Ok(Vec::new())
    }

    /// Fails and retires a workflow activation that was never exposed to OCaml.
    fn reject_workflow_delivery(&self, run_id: &str) -> std::result::Result<(), Failure> {
        self.reject_workflow_delivery_with_reason(
            run_id,
            "semantic workflow activation conversion failed",
        )
    }

    /// Fails and retires a workflow activation with a static diagnostic
    /// describing the private conversion branch that rejected it.
    fn reject_workflow_delivery_with_reason(
        &self,
        run_id: &str,
        reason: &'static str,
    ) -> std::result::Result<(), Failure> {
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .expect("worker retains parent runtime")
            .tokio_handle();
        handle
            .block_on(worker.reject_workflow_delivery_with_reason(run_id, reason))
            .map_err(worker_bridge_failure)
    }

    /// Fails and retires an activity task that was never exposed to OCaml.
    fn reject_activity_delivery(&self, task_token: &[u8]) -> std::result::Result<(), Failure> {
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .expect("worker retains parent runtime")
            .tokio_handle();
        handle
            .block_on(worker.reject_activity_delivery(task_token))
            .map_err(worker_bridge_failure)
    }

    /// Strictly validates and submits one leased remote-activity completion.
    fn complete_activity(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let semantic = activity_protocol::decode_completion(text).map_err(protocol_failure)?;
        let task_token =
            activity_protocol::decode_token(&semantic.task_token).map_err(protocol_failure)?;
        let completion =
            activity_protocol::completion_to_core(&semantic).map_err(core_conversion_failure)?;
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .expect("worker retains parent runtime")
            .tokio_handle();
        handle
            .block_on(worker.complete_activity(completion))
            .map_err(worker_bridge_failure)?;
        retire_activity_semantics(&mut self.activity_tasks, &task_token);
        Ok(Vec::new())
    }

    /// Strictly validates and records progress for one leased remote activity.
    /// A heartbeat is not terminal, so the semantic task map and native ledger
    /// remain leased for the later completion or cancellation path. The pinned
    /// Core API is intentionally fire-and-forget: cancellation, pause, and
    /// reset flags from the server arrive later as an `ActivityTask::Cancel`,
    /// never as a fabricated synchronous result from this operation.
    fn record_activity_heartbeat(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let semantic = activity_protocol::decode_heartbeat(text).map_err(protocol_failure)?;
        let task_token =
            activity_protocol::decode_token(&semantic.task_token).map_err(protocol_failure)?;
        let details = semantic
            .details
            .iter()
            .map(workflow_protocol::payload_to_core)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(core_conversion_failure)?;
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let heartbeat = CoreActivityHeartbeat {
            task_token,
            details,
        };
        worker
            .record_activity_heartbeat(heartbeat)
            .map_err(worker_bridge_failure)?;
        Ok(Vec::new())
    }

    /// Creates a namespace-bound official client for an activity that was
    /// handed off with `WillCompleteAsync`.
    ///
    /// Async activity completion is deliberately separate from the worker
    /// ledger above. Core's client API identifies the task by its opaque token
    /// and sends the terminal RPC directly to Temporal. Reusing the worker
    /// completion path would make a retained async handle depend on a poll
    /// task that has already been retired.
    fn async_activity_client(&self) -> std::result::Result<Client, Failure> {
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let namespace = self.worker_namespace.clone().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker namespace is not available".to_owned(),
        })?;
        Client::new(connection, ClientOptions::new(namespace).build()).map_err(|_| Failure {
            status: STATUS_CONNECTION,
            message: "Temporal asynchronous activity client could not be created".to_owned(),
        })
    }

    /// Converts ordered semantic payloads into the optional protobuf collection
    /// accepted by Core's asynchronous activity handle.
    fn async_payloads(
        values: &[workflow_protocol::Payload],
    ) -> std::result::Result<Option<api_common::Payloads>, Failure> {
        if values.is_empty() {
            return Ok(None);
        }
        let payloads = values
            .iter()
            .map(workflow_protocol::payload_to_core)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(core_conversion_failure)?;
        Ok(Some(api_common::Payloads { payloads }))
    }

    /// Completes a previously accepted asynchronous activity through Core's
    /// namespace-bound client API.
    ///
    /// The semantic decoder is intentionally shared with worker completions,
    /// but this endpoint rejects `will_complete_async`: that marker is the
    /// worker-to-client handoff, not a terminal client operation. No entry in
    /// `self.activity_tasks` is read or retired here; the OCaml async lease
    /// owns the capability until Core accepts a terminal request.
    fn complete_async_activity(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let semantic = activity_protocol::decode_completion(text).map_err(protocol_failure)?;
        let task_token =
            activity_protocol::decode_token(&semantic.task_token).map_err(protocol_failure)?;
        let client = self.async_activity_client()?;
        let activity =
            client.get_async_activity_handle(ActivityIdentifier::TaskToken(TaskToken(task_token)));
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        let operation = match &semantic.result {
            activity_protocol::ActivityCompletionResult::Completed { result } => {
                let payloads = result
                    .as_ref()
                    .map(|payload| Self::async_payloads(std::slice::from_ref(payload)))
                    .transpose()?
                    .flatten();
                handle.block_on(activity.complete(payloads))
            }
            activity_protocol::ActivityCompletionResult::Failed { failure } => {
                let failure =
                    workflow_protocol::failure_to_core(failure).map_err(core_conversion_failure)?;
                handle.block_on(activity.fail(failure, None))
            }
            activity_protocol::ActivityCompletionResult::Cancelled { failure } => {
                let details = match &failure.info {
                    workflow_protocol::FailureInfo::Canceled { details, .. } => {
                        Self::async_payloads(details)?
                    }
                    _ => {
                        return Err(Failure {
                            status: STATUS_PROTOCOL,
                            message:
                                "asynchronous activity cancellation must use a canceled failure"
                                    .to_owned(),
                        });
                    }
                };
                handle.block_on(activity.report_cancelation(details))
            }
            activity_protocol::ActivityCompletionResult::WillCompleteAsync => {
                return Err(Failure {
                    status: STATUS_PROTOCOL,
                    message: "asynchronous activity completion cannot defer again".to_owned(),
                });
            }
        };
        operation.map_err(|error| async_activity_failure("completion", error))?;
        Ok(Vec::new())
    }

    /// Records a heartbeat for an asynchronously completed activity through
    /// the same namespace-bound client handle used for terminal operations.
    fn record_async_activity_heartbeat(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let semantic = activity_protocol::decode_heartbeat(text).map_err(protocol_failure)?;
        let task_token =
            activity_protocol::decode_token(&semantic.task_token).map_err(protocol_failure)?;
        let details = Self::async_payloads(&semantic.details)?;
        let client = self.async_activity_client()?;
        let activity =
            client.get_async_activity_handle(ActivityIdentifier::TaskToken(TaskToken(task_token)));
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        handle
            .block_on(activity.heartbeat(details))
            .map_err(|error| async_activity_failure("heartbeat", error))?;
        Ok(Vec::new())
    }

    /// Revalidates Rust-produced task JSON and removes only the rejected
    /// semantic handoff after OCaml cannot represent it.
    ///
    /// A start task owns the one Core completion debt for an activity token,
    /// so rejecting that task must generate the bridge failure and retire the
    /// native lease. A cancellation is only an update attached to the same
    /// token. If its JSON cannot be decoded, dropping that one update must not
    /// retire the start lease that another OCaml call still has to complete.
    fn reject_polled_activity(&mut self, input: &[u8]) -> Operation {
        let rejection = activity_rejection_task(&self.activity_tasks, input)?;
        let owns_completion_debt = matches!(
            &rejection.task.variant,
            activity_protocol::ActivityTaskVariant::Start(_)
        );
        // Only a Start represents an inaccessible leased Core completion. A
        // Cancel has no independent completion to reject; removing just its
        // retained document preserves the shared Start debt.
        let native_rejection = if owns_completion_debt {
            self.reject_activity_delivery(&rejection.task_token)
        } else {
            Ok(())
        };
        retire_activity_semantic(
            &mut self.activity_tasks,
            &rejection.task_token,
            &rejection.task,
        );
        native_rejection?;
        Ok(Vec::new())
    }

    /// Drops the client only after its worker child is absent.
    fn disconnect_client(&mut self) -> Operation {
        if self.worker.is_some() {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal client cannot disconnect while its worker is running".to_owned(),
            });
        }
        if !self.pending_starts.is_empty() {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal client cannot disconnect while workflow starts are pending"
                    .to_owned(),
            });
        }
        self.client.take();
        Ok(Vec::new())
    }

    /// Transfers Core to its cleanup thread and optionally waits for disposal.
    ///
    /// Explicit close waits while the OCaml lock is released. GC fallback does
    /// not wait on this caller, so a custom-block finalizer never stalls the
    /// collector; the dedicated cleanup thread joins any aborted start tasks.
    fn close(mut self, wait: bool) -> Status {
        let pending_start_tasks = self.abort_pending_starts(wait);
        let Some(core) = self.core.take() else {
            return STATUS_OK;
        };
        let (completed, receiver) = if wait {
            let (sender, receiver) = sync_channel(1);
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        let message = RuntimeCleanup {
            core,
            client: self.client.take(),
            worker: self.worker.take(),
            replay_worker: self.replay_worker.take(),
            pending_start_tasks,
            completed,
        };

        if let Err(error) = self.cleanup.send(message) {
            // The receiver only exits after a message, so this indicates a
            // defect in the cleanup thread itself. Reclaim on this thread to
            // preserve the no-leak guarantee even on that defensive path.
            let message = error.0;
            drop_runtime_graph(
                message.core,
                message.client,
                message.worker,
                message.replay_worker,
                message.pending_start_tasks,
            );
            RUNTIMES_CLEANED.fetch_add(1, Ordering::Release);
            return STATUS_INTERNAL;
        }

        match receiver {
            Some(receiver) => receiver.recv().unwrap_or(STATUS_INTERNAL),
            None => STATUS_OK,
        }
    }
}

/// Reports whether a Core activity task owns the single completion debt for
/// its opaque token. Only an explicit cancellation notification is excluded:
/// it shares the Start lease and must never be failed as a second completion.
/// Missing or future variants fail closed as owning the debt so an unexpected
/// Core shape cannot leave an outstanding task stranded during shutdown.
fn activity_task_owns_completion_debt(task: &CoreActivityTask) -> bool {
    !matches!(
        task.variant.as_ref(),
        Some(activity_task::activity_task::Variant::Cancel(_))
    )
}

/// Drops Core away from OCaml's collector and reports completion when asked.
fn run_runtime_cleanup(receiver: Receiver<RuntimeCleanup>) {
    let Ok(message) = receiver.recv() else {
        return;
    };
    let RuntimeCleanup {
        core,
        client,
        worker,
        replay_worker,
        pending_start_tasks,
        completed,
    } = message;
    let status = if catch_unwind(AssertUnwindSafe(|| {
        drop_runtime_graph(core, client, worker, replay_worker, pending_start_tasks)
    }))
    .is_ok()
    {
        STATUS_OK
    } else {
        STATUS_PANIC
    };
    // Release publishes completion after Core's destructor has returned. The
    // matching Acquire load is used only by the isolated ownership test.
    RUNTIMES_CLEANED.fetch_add(1, Ordering::Release);
    if let Some(completed) = completed {
        let _ = completed.send(status);
    }
}

/// Releases aborted start tasks, then worker, client, and Core on the runtime
/// cleanup thread.  Awaiting the transferred handles is essential: dropping a
/// Tokio [`JoinHandle`] after `abort` would detach its task, allowing the task
/// to retain a cloned `Connection` beyond the lifetime of the runtime graph.
fn drop_runtime_graph(
    core: CoreRuntime,
    client: Option<Connection>,
    worker: Option<PollLanes>,
    replay_worker: Option<ReplayWorker>,
    pending_start_tasks: Vec<JoinHandle<()>>,
) {
    let handle = core.tokio_handle();
    handle.block_on(async {
        for task in pending_start_tasks {
            let _ = task.await;
        }
    });

    if let Some(mut worker) = worker {
        {
            // Cleanup runs on a plain OS thread, so explicitly enter Core's
            // executor before its synchronous shutdown code spawns a task.
            let _runtime_guard = handle.enter();
            worker.initiate_shutdown();
        }
        // Core poll loops do not observe ShutDown while tasks remain
        // outstanding. Force-fail every still-owned debt before joining the
        // lanes so dispose cannot block forever waiting for OCaml.
        handle.block_on(worker.force_complete_outstanding_for_dispose());
        let _ = handle.block_on(worker.join_poll_lanes());
        // A poll that was already inside Core can return after the first drain
        // and publish a new ready task before its lane exits.  The joins above
        // establish that no producer remains; a final pass therefore closes
        // the only window in which a late task could otherwise be dropped with
        // an outstanding Core completion debt.
        handle.block_on(worker.force_complete_outstanding_for_dispose());
        match handle.block_on(worker.finalize()) {
            Ok(()) => {}
            Err((worker, _)) => {
                // Dispose cannot wait for OCaml. Dropping after force-complete
                // is the last-resort reclaim path.
                drop(worker);
            }
        }
    }
    if let Some(worker) = replay_worker {
        // GC fallback cannot report a typed replay error to OCaml. It still
        // performs the same bounded force-completion and join protocol as an
        // explicit dispose, then retries once before releasing the retained
        // owner as the last-resort cleanup path.
        let mut retained = Some(worker);
        for _ in 0..2 {
            let worker = retained
                .take()
                .expect("replay cleanup retains the worker after a failed attempt");
            match handle.block_on(worker.dispose(&handle)) {
                Ok(()) => break,
                Err((returned, _error)) => retained = Some(returned),
            }
        }
        drop(retained);
    }
    drop(client);
    drop(core);
}

/// Maps private worker state-machine errors to stable ABI failures.
fn worker_bridge_failure(error: WorkerBridgeError) -> Failure {
    let status = match &error {
        WorkerBridgeError::OutstandingTasks(_) => STATUS_OUTSTANDING_TASKS,
        WorkerBridgeError::RetryableActivityCompletion => STATUS_RETRYABLE,
        _ => STATUS_WORKER,
    };
    Failure {
        status,
        message: public_worker_error_message(&error).to_owned(),
    }
}

/// Maps replay lifecycle errors to the same closed ABI categories used by live
/// worker operations. Replay input and Core diagnostics are intentionally
/// reduced to stable messages before they cross the C/OCaml boundary.
fn replay_worker_failure(error: ReplayWorkerError) -> Failure {
    let (status, message) = match error {
        ReplayWorkerError::FeederClosed => (
            STATUS_INVALID_STATE,
            "Temporal replay history feeder is closed",
        ),
        ReplayWorkerError::ReplayNotDrained { .. } => (
            STATUS_OUTSTANDING_TASKS,
            "Temporal replay worker has not drained all input",
        ),
        ReplayWorkerError::InvalidHistory(_) => (
            STATUS_PROTOCOL,
            "Temporal replay history failed strict validation",
        ),
        ReplayWorkerError::CoreInitialization => {
            (STATUS_WORKER, "Temporal replay worker construction failed")
        }
        ReplayWorkerError::PollLane(_) => (STATUS_WORKER, "Temporal replay poll lane failed"),
        ReplayWorkerError::Finalization(_) => {
            (STATUS_WORKER, "Temporal replay worker finalization failed")
        }
    };
    Failure {
        status,
        message: message.to_owned(),
    }
}

/// Maps the low-level client adapter to a stable ABI status and a structured
/// privacy-safe error body.  In particular, Temporal's AlreadyStarted result
/// is machine-readable JSON rather than a server diagnostic string.
fn client_operation_failure(error: client_protocol::ClientOperationError) -> Failure {
    let status = match &error {
        client_protocol::ClientOperationError::AlreadyStarted { .. } => STATUS_ALREADY_STARTED,
        client_protocol::ClientOperationError::Rpc { .. } => STATUS_CONNECTION,
        client_protocol::ClientOperationError::Core(_) => STATUS_PROTOCOL,
    };
    Failure {
        status,
        message: error.to_json(),
    }
}

/// Maps a fatal background poll-lane error without exposing Core internals.
fn poll_lane_failure(error: PollLaneError) -> Failure {
    Failure {
        status: STATUS_WORKER,
        message: public_poll_lane_error_message(&error).to_owned(),
    }
}

/// Constructs the stable result used by empty non-blocking poll lanes.
fn not_ready() -> Failure {
    Failure {
        status: STATUS_NOT_READY,
        message: "no Temporal task is ready".to_owned(),
    }
}

/// Reports an exact-run wait that must be retried without exposing a fake
/// terminal workflow outcome to the OCaml supervisor.
fn client_wait_not_ready() -> Failure {
    Failure {
        status: STATUS_NOT_READY,
        message: "Temporal workflow has not reached a close event; retry".to_owned(),
    }
}

/// Converts strict semantic-protocol failures without reflecting user input.
fn protocol_failure(_error: crate::protocol::ProtocolError) -> Failure {
    Failure {
        status: STATUS_PROTOCOL,
        message: "Temporal semantic JSON failed validation".to_owned(),
    }
}

/// Converts protobuf-boundary failures to a privacy-safe protocol status.
fn core_conversion_failure(error: workflow_protocol::CoreConversionError) -> Failure {
    // `CoreConversionError::message` is an &'static str created by the
    // bilateral conversion table, never copied from a protobuf or server
    // value. It is therefore safe to retain as the closed protocol category.
    Failure {
        status: STATUS_PROTOCOL,
        message: format!(
            "Temporal Core task cannot be represented safely: {}",
            error.message
        ),
    }
}

/// Borrows one bounded UTF-8 semantic document for synchronous decoding.
fn decode_semantic_input(input: &[u8]) -> std::result::Result<&str, Failure> {
    if input.len() > crate::protocol::MAX_DOCUMENT_BYTES {
        return Err(Failure {
            status: STATUS_PROTOCOL,
            message: "Temporal semantic JSON exceeds the document limit".to_owned(),
        });
    }
    std::str::from_utf8(input).map_err(|_| Failure {
        status: STATUS_PROTOCOL,
        message: "Temporal semantic JSON is not UTF-8".to_owned(),
    })
}

/// Reserves registry capacity before a Tokio start task is admitted.
///
/// The runtime owner inserts the corresponding [`PendingStart`] only after
/// spawning the task, so this reservation is part of the ownership protocol:
/// a recoverable allocation failure must be reported before any task captures
/// a Core connection clone.  The caller should invoke this immediately before
/// task admission and insert no more than `additional` entries afterwards.
fn reserve_pending_start_slots(
    pending: &mut HashMap<String, PendingStart>,
    additional: usize,
) -> std::result::Result<(), Failure> {
    pending.try_reserve(additional).map_err(|error| Failure {
        status: STATUS_INTERNAL,
        message: format!("could not reserve Temporal workflow start slots: {error}"),
    })
}

/// Records one workflow activation without allowing a duplicate poll to
/// replace the semantic document already leased to the language runtime.
fn retain_workflow_activation(
    pending: &mut HashMap<String, workflow_protocol::Activation>,
    activation: workflow_protocol::Activation,
) -> std::result::Result<(), Failure> {
    match pending.entry(activation.run_id.clone()) {
        Entry::Vacant(entry) => {
            entry.insert(activation);
            Ok(())
        }
        Entry::Occupied(_) => Err(Failure {
            status: STATUS_INTERNAL,
            message: "workflow activation lease was duplicated".to_owned(),
        }),
    }
}

/// Extracts a workflow rejection identity only when the complete submitted
/// document equals the Rust semantic value retained at the original handoff.
fn workflow_rejection_run_id(
    pending: &HashMap<String, workflow_protocol::Activation>,
    input: &[u8],
) -> std::result::Result<String, Failure> {
    let text = decode_semantic_input(input)?;
    let semantic = workflow_protocol::decode_activation(text).map_err(protocol_failure)?;
    match pending.get(&semantic.run_id) {
        Some(leased) if leased == &semantic => Ok(semantic.run_id),
        Some(_) => Err(Failure {
            status: STATUS_PROTOCOL,
            message: "workflow rejection does not match the leased activation".to_owned(),
        }),
        None => Err(Failure {
            status: STATUS_PROTOCOL,
            message: "workflow rejection does not match a leased activation".to_owned(),
        }),
    }
}

/// Retains every semantic activity handoff under Core's opaque token. Multiple
/// cancellation updates may share that token and must not overwrite each other.
fn retain_activity_task(
    pending: &mut HashMap<Vec<u8>, Vec<activity_protocol::ActivityTask>>,
    task_token: Vec<u8>,
    task: activity_protocol::ActivityTask,
) {
    pending.entry(task_token).or_default().push(task);
}

/// Clears all handoff documents for one activity token when its single native
/// completion debt is retired. Cancellation updates add documents but, by
/// Temporal contract, never add another ledger obligation for the same token.
fn retire_activity_semantics(
    pending: &mut HashMap<Vec<u8>, Vec<activity_protocol::ActivityTask>>,
    task_token: &[u8],
) {
    pending.remove(task_token);
}

/// Removes one exact semantic task document while preserving any other
/// handoffs associated with the same opaque activity token.
///
/// A token can have one Start document and any number of Cancel updates. The
/// Start's completion debt is retired only after its own completion or
/// rejection; a malformed Cancel must not erase that document from the
/// ownership ledger.
fn retire_activity_semantic(
    pending: &mut HashMap<Vec<u8>, Vec<activity_protocol::ActivityTask>>,
    task_token: &[u8],
    task: &activity_protocol::ActivityTask,
) {
    let remove_token = if let Some(tasks) = pending.get_mut(task_token) {
        if let Some(index) = tasks.iter().position(|candidate| candidate == task) {
            tasks.remove(index);
        }
        tasks.is_empty()
    } else {
        false
    };
    if remove_token {
        pending.remove(task_token);
    }
}

/// Retained semantic task and its canonical opaque token for rejection.
///
/// Keeping both values avoids decoding the document twice and lets rejection
/// distinguish a Start from a Cancel before it mutates the ownership ledger.
struct ActivityRejection {
    /// Canonical token used by the native worker's completion ledger.
    task_token: Vec<u8>,
    /// Exact retained task document that matched the rejected bytes.
    task: activity_protocol::ActivityTask,
}

/// Extracts the retained semantic task only when the complete document
/// matches one successful Rust-to-OCaml handoff under its canonical token.
fn activity_rejection_task(
    pending: &HashMap<Vec<u8>, Vec<activity_protocol::ActivityTask>>,
    input: &[u8],
) -> std::result::Result<ActivityRejection, Failure> {
    let text = decode_semantic_input(input)?;
    let semantic = activity_protocol::decode_task(text).map_err(protocol_failure)?;
    let task_token =
        activity_protocol::decode_token(&semantic.task_token).map_err(protocol_failure)?;
    match pending.get(&task_token) {
        Some(tasks) if tasks.contains(&semantic) => Ok(ActivityRejection {
            task_token,
            task: semantic,
        }),
        Some(_) => Err(Failure {
            status: STATUS_PROTOCOL,
            message: "activity rejection does not match a leased task".to_owned(),
        }),
        None => Err(Failure {
            status: STATUS_PROTOCOL,
            message: "activity rejection does not match a leased task token".to_owned(),
        }),
    }
}

/// Extracts the canonical opaque activity token only when the complete task
/// document matches one retained at a successful Rust-to-OCaml handoff.
#[cfg(test)]
fn activity_rejection_token(
    pending: &HashMap<Vec<u8>, Vec<activity_protocol::ActivityTask>>,
    input: &[u8],
) -> std::result::Result<Vec<u8>, Failure> {
    activity_rejection_task(pending, input).map(|rejection| rejection.task_token)
}

impl WorkerConfigInput {
    /// Performs bridge-owned bounds checks and constructs official Core config.
    fn into_core(self) -> std::result::Result<WorkerConfig, Failure> {
        validate_identifier("namespace", &self.namespace)?;
        validate_identifier("task_queue", &self.task_queue)?;
        validate_identifier("build_id", &self.build_id)?;
        let versioning_strategy = match self.versioning {
            WorkerVersioningInput::None => WorkerVersioningStrategy::None {
                build_id: self.build_id,
            },
            WorkerVersioningInput::LegacyBuildId { build_id } => {
                validate_identifier("versioning.build_id", &build_id)?;
                if build_id != self.build_id {
                    return Err(Failure {
                        status: STATUS_CONFIGURATION,
                        message: "versioning.build_id must match build_id".to_owned(),
                    });
                }
                WorkerVersioningStrategy::LegacyBuildIdBased { build_id }
            }
            WorkerVersioningInput::DeploymentBased {
                deployment_name,
                build_id,
                use_worker_versioning,
                default_versioning_behavior,
            } => {
                validate_identifier("versioning.deployment_name", &deployment_name)?;
                validate_identifier("versioning.build_id", &build_id)?;
                if build_id != self.build_id {
                    return Err(Failure {
                        status: STATUS_CONFIGURATION,
                        message: "versioning.build_id must match build_id".to_owned(),
                    });
                }
                let default_versioning_behavior = match default_versioning_behavior.as_deref() {
                    None => None,
                    Some("auto_upgrade") => Some(VersioningBehavior::AutoUpgrade),
                    Some("pinned") => Some(VersioningBehavior::Pinned),
                    Some(value) => {
                        return Err(Failure {
                            status: STATUS_CONFIGURATION,
                            message: format!(
                                "versioning.default_versioning_behavior has unsupported value {value}"
                            ),
                        });
                    }
                };
                if !use_worker_versioning && default_versioning_behavior.is_some() {
                    return Err(Failure {
                        status: STATUS_CONFIGURATION,
                        message:
                            "versioning.default_versioning_behavior requires use_worker_versioning"
                                .to_owned(),
                    });
                }
                WorkerVersioningStrategy::WorkerDeploymentBased(
                    temporalio_common::worker::WorkerDeploymentOptions {
                        version: temporalio_common::worker::WorkerDeploymentVersion {
                            deployment_name,
                            build_id,
                        },
                        use_worker_versioning,
                        default_versioning_behavior,
                    },
                )
            }
        };
        validate_count("max_cached_workflows", self.max_cached_workflows, true)?;
        validate_count(
            "max_outstanding_workflow_tasks",
            self.max_outstanding_workflow_tasks,
            false,
        )?;
        validate_count(
            "max_concurrent_workflow_task_polls",
            self.max_concurrent_workflow_task_polls,
            false,
        )?;
        if self.max_cached_workflows > 0
            && self.max_concurrent_workflow_task_polls < MIN_CACHED_WORKFLOW_POLLS
        {
            return Err(Failure {
                status: STATUS_CONFIGURATION,
                message: "max_concurrent_workflow_task_polls must be at least 2 when max_cached_workflows is greater than zero".to_owned(),
            });
        }
        if self.graceful_shutdown_timeout_ms > MAX_GRACEFUL_SHUTDOWN_MS {
            return Err(Failure {
                status: STATUS_CONFIGURATION,
                message: format!("graceful_shutdown_timeout_ms exceeds {MAX_GRACEFUL_SHUTDOWN_MS}"),
            });
        }

        WorkerConfig::builder()
            .namespace(self.namespace)
            .task_queue(self.task_queue)
            .max_cached_workflows(self.max_cached_workflows as usize)
            .max_outstanding_workflow_tasks(self.max_outstanding_workflow_tasks as usize)
            .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(
                self.max_concurrent_workflow_task_polls as usize,
            ))
            .graceful_shutdown_period(Duration::from_millis(self.graceful_shutdown_timeout_ms))
            .versioning_strategy(versioning_strategy)
            .max_outstanding_activities(DEFAULT_MAX_OUTSTANDING_ACTIVITIES)
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(
                DEFAULT_MAX_CONCURRENT_ACTIVITY_POLLS,
            ))
            .task_types(crate::worker_bridge::bridge_task_types())
            .build()
            .map_err(|message| Failure {
                status: STATUS_CONFIGURATION,
                message: format!("Temporal workflow worker configuration is invalid: {message}"),
            })
    }
}

/// Validates only bridge-owned string invariants before Core sees the value.
fn validate_identifier(name: &str, value: &str) -> std::result::Result<(), Failure> {
    if value.is_empty() {
        return Err(Failure {
            status: STATUS_CONFIGURATION,
            message: format!("{name} must not be empty"),
        });
    }
    if value.len() > MAX_TRANSPORT_STRING_BYTES {
        return Err(Failure {
            status: STATUS_CONFIGURATION,
            message: format!("{name} exceeds {MAX_TRANSPORT_STRING_BYTES} UTF-8 bytes"),
        });
    }
    if value.as_bytes().contains(&0) {
        return Err(Failure {
            status: STATUS_CONFIGURATION,
            message: format!("{name} must not contain NUL"),
        });
    }
    Ok(())
}

/// Validates a bounded worker count, optionally allowing zero to disable it.
fn validate_count(name: &str, value: u32, allow_zero: bool) -> std::result::Result<(), Failure> {
    if (!allow_zero && value == 0) || value > MAX_WORKER_COUNT {
        return Err(Failure {
            status: STATUS_CONFIGURATION,
            message: format!(
                "{name} must be between {} and {MAX_WORKER_COUNT}",
                u8::from(!allow_zero)
            ),
        });
    }
    Ok(())
}

/// Byte-producing operation accepted by the shared panic/ownership wrapper.
type Operation = std::result::Result<Vec<u8>, Failure>;

/// Constructs the only valid successful result shape.
fn success(value: Vec<u8>) -> Result {
    Result {
        status: STATUS_OK,
        value: Buffer::from_vec(value),
        error: Buffer::default(),
    }
}

/// Constructs the only valid failed result shape with UTF-8 diagnostic bytes.
fn failure(status: Status, message: impl Into<String>) -> Result {
    Result {
        status,
        value: Buffer::default(),
        error: Buffer::from_vec(message.into().into_bytes()),
    }
}

/// Initializes output, contains panics, and commits one fully formed result.
///
/// Writing the empty result before executing user logic ensures every non-null
/// output is safe to free even when the operation panics.
unsafe fn invoke(output: *mut Result, operation: impl FnOnce() -> Operation) -> Status {
    if output.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    // SAFETY: The non-null output pointer is required by the ABI contract to
    // be valid and writable. `ptr::write` also permits uninitialized storage.
    unsafe { ptr::write(output, Result::default()) };

    let result = match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(value)) => success(value),
        Ok(Err(error)) => failure(error.status, error.message),
        Err(_) => failure(
            STATUS_PANIC,
            "Rust panic contained at the native ABI boundary",
        ),
    };
    let status = result.status;

    // SAFETY: The pointer was validated above and no reference to its previous
    // (empty) value is retained.
    unsafe { ptr::write(output, result) };
    status
}

/// Decodes one bounded strict UTF-8 JSON lifecycle configuration.
///
/// Parser details are deliberately reduced to one stable category. Serde's
/// diagnostics can include an input-controlled object member name (for
/// example, an unknown field), and the lifecycle configuration is an
/// application-controlled boundary rather than a place to reflect arbitrary
/// input back through the C/OCaml result buffer.
///
/// # Safety
///
/// A nonzero `input_len` requires `input` to identify that many readable bytes
/// for the duration of this synchronous parse.
unsafe fn decode_config<T: for<'de> Deserialize<'de>>(
    input: *const u8,
    input_len: usize,
) -> std::result::Result<T, Failure> {
    if input_len > MAX_LIFECYCLE_CONFIG_BYTES {
        return Err(Failure {
            status: STATUS_CONFIGURATION,
            message: format!("lifecycle configuration exceeds {MAX_LIFECYCLE_CONFIG_BYTES} bytes"),
        });
    }
    if input_len > 0 && input.is_null() {
        return Err(Failure {
            status: STATUS_INVALID_ARGUMENT,
            message: "configuration input is null but input_len is nonzero".to_owned(),
        });
    }
    let bytes = if input_len == 0 {
        &[]
    } else {
        // SAFETY: The caller promises the readable span documented above, and
        // the nonempty null case was rejected before constructing this slice.
        unsafe { std::slice::from_raw_parts(input, input_len) }
    };
    serde_json::from_slice(bytes).map_err(|_error| Failure {
        status: STATUS_CONFIGURATION,
        message: "invalid lifecycle configuration JSON".to_owned(),
    })
}

/// Borrows one native input span after enforcing a caller-selected ceiling.
///
/// # Safety
///
/// A nonzero length requires a readable input allocation for this call.
unsafe fn input_span<'a>(
    input: *const u8,
    input_len: usize,
    maximum: usize,
) -> std::result::Result<&'a [u8], Failure> {
    if input_len > maximum {
        return Err(Failure {
            status: STATUS_PROTOCOL,
            message: format!("native input exceeds {maximum} bytes"),
        });
    }
    if input_len != 0 && input.is_null() {
        return Err(Failure {
            status: STATUS_INVALID_ARGUMENT,
            message: "input is null but input_len is nonzero".to_owned(),
        });
    }
    if input_len == 0 {
        Ok(&[])
    } else {
        // SAFETY: The caller owns the readable span documented above.
        Ok(unsafe { std::slice::from_raw_parts(input, input_len) })
    }
}

/// Reclaims one bridge allocation and resets the buffer to canonical empty.
unsafe fn free_buffer(buffer: &mut Buffer) {
    if buffer.ptr.is_null() {
        buffer.len = 0;
        return;
    }

    let allocation = ptr::slice_from_raw_parts_mut(buffer.ptr, buffer.len);
    // SAFETY: Only buffers created by `Buffer::from_vec` may be passed to this
    // function. The exact pointer and length reconstruct its boxed slice.
    drop(unsafe { Box::from_raw(allocation) });
    *buffer = Buffer::default();
}

/// Negotiate ABI version 2.
///
/// # Safety
///
/// `output` must be null or point to writable storage for one [`Result`]. It
/// must not contain live bridge-owned allocations when this function starts.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_check_abi_version(
    requested_version: u32,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` validates null and otherwise relies on the documented
    // output-pointer contract of this exported function.
    unsafe {
        invoke(output, || {
            if requested_version == ABI_VERSION {
                Ok(Vec::new())
            } else {
                Err(Failure {
                    status: STATUS_ABI_MISMATCH,
                    message: format!(
                        "unsupported ABI version {requested_version}; expected {ABI_VERSION}"
                    ),
                })
            }
        })
    }
}

/// Copy bytes into a Rust-owned result buffer.
///
/// This operation exercises the ownership contract used later for encoded
/// workflow activations and completions.
///
/// # Safety
///
/// When `input_len` is nonzero, `input` must point to that many readable bytes.
/// `output` follows the same contract as
/// [`ocaml_temporal_core_v2_check_abi_version`] and must not overlap `input`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_echo(
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` validates the output pointer. Input is checked for null
    // before constructing the slice; remaining validity is the caller's ABI
    // obligation documented above.
    unsafe {
        invoke(output, || {
            if input_len == 0 {
                return Ok(Vec::new());
            }
            if input.is_null() {
                return Err(Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "input is null but input_len is nonzero".to_owned(),
                });
            }

            // SAFETY: The caller guarantees a readable input allocation of
            // `input_len` bytes and the null case was rejected above.
            Ok(std::slice::from_raw_parts(input, input_len).to_vec())
        })
    }
}

/// Wait on the native side for bridge conformance testing.
///
/// This bounded operation exists to prove that language bindings release
/// their runtime lock around blocking ABI calls. It is not a workflow timer.
///
/// # Safety
///
/// `output` follows the same contract as
/// [`ocaml_temporal_core_v2_check_abi_version`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_conformance_wait_ms(
    milliseconds: u32,
    output: *mut Result,
) -> Status {
    // SAFETY: The output-pointer contract is forwarded unchanged to `invoke`.
    unsafe {
        invoke(output, || {
            if milliseconds > 1_000 {
                return Err(Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "conformance wait cannot exceed 1000 ms".to_owned(),
                });
            }
            std::thread::sleep(Duration::from_millis(u64::from(milliseconds)));
            Ok(Vec::new())
        })
    }
}

/// Create the native runtime that will own later Core clients and workers.
///
/// On success, `runtime` receives one owned opaque handle. The caller must
/// eventually pass that same slot to [`ocaml_temporal_core_v2_runtime_free`].
///
/// # Safety
///
/// `runtime` must be null or point to writable storage for one runtime pointer.
/// `output` follows the result contract of
/// [`ocaml_temporal_core_v2_check_abi_version`]. A non-null runtime slot must
/// not already contain a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_runtime_new(
    runtime: *mut *mut Runtime,
    output: *mut Result,
) -> Status {
    // Canonicalize any writable handle slot even when the independent result
    // pointer is invalid. A caller can therefore inspect a known null value on
    // every failing return instead of retaining indeterminate ownership state.
    if !runtime.is_null() {
        // SAFETY: A non-null runtime argument promises writable pointer storage.
        unsafe { ptr::write(runtime, ptr::null_mut()) };
    }
    if output.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }
    if runtime.is_null() {
        // SAFETY: The result pointer was checked above and the closure does
        // not inspect the missing runtime slot.
        return unsafe {
            invoke(output, || {
                Err(Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime output pointer is null".to_owned(),
                })
            })
        };
    }

    // SAFETY: Both output locations were validated above.
    unsafe {
        invoke(output, || {
            let options = RuntimeOptions::builder()
                .build()
                .map_err(|_message| Failure {
                    status: STATUS_INTERNAL,
                    // Core runtime-option validation may change with the
                    // linked Core revision; expose only its stable category.
                    message: "could not configure Temporal Core runtime".to_owned(),
                })?;
            let core =
                CoreRuntime::new(options, TokioRuntimeBuilder::default()).map_err(|_error| {
                    Failure {
                        status: STATUS_INTERNAL,
                        // Runtime construction errors can contain Core or Tokio
                        // diagnostics. Keep the C result a closed category just
                        // like worker construction and poll failures.
                        message: "could not create Temporal Core runtime".to_owned(),
                    }
                })?;
            let owned = Box::into_raw(Box::new(Runtime::new(core)?));

            // SAFETY: The runtime slot remains exclusively owned by this call
            // until it returns and was validated before invoking the closure.
            ptr::write(runtime, owned);
            Ok(Vec::new())
        })
    }
}

/// Connect the runtime graph's single official Temporal client from strict
/// JSON. Successful return publishes the client; every failure leaves the
/// graph without a newly constructed child.
///
/// # Safety
///
/// `runtime` must be a live handle created by
/// [`ocaml_temporal_core_v2_runtime_new`] and exclusively owned for this call.
/// `input` follows [`decode_config`]'s byte-span contract, while `output`
/// follows [`ocaml_temporal_core_v2_check_abi_version`]'s result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_connect_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` validates output. The closure checks the opaque handle
    // before dereferencing and forwards the documented input span to decoding.
    unsafe {
        invoke(output, || {
            let runtime = runtime.as_mut().ok_or_else(|| Failure {
                status: STATUS_INVALID_ARGUMENT,
                message: "runtime pointer is null".to_owned(),
            })?;
            let config = decode_config::<ClientConfig>(input, input_len)?;
            runtime.connect_client(config)
        })
    }
}

/// Start one dynamically named workflow through the connected Core client.
///
/// The input is a strict client-start JSON document.  The successful value is
/// a strict execution-reference response.  Temporal AlreadyStarted failures
/// use `STATUS_ALREADY_STARTED` and place a closed JSON error body in `error`.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle.  The input
/// span is borrowed only for this synchronous call and `output` follows the
/// standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_start_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .start_workflow_json(input)
        })
    }
}

/// Request cancellation of one exact workflow run.
///
/// The successful value is a strict `{"acknowledged":true}` document. A
/// gRPC or protocol failure remains a typed native error and never becomes a
/// false acknowledgement.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle. The input span
/// is borrowed only for this synchronous call and `output` follows the normal
/// initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_cancel_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .cancel_workflow_json(input)
        })
    }
}

/// Terminate one exact workflow run. The successful value is a strict
/// `{"acknowledged":true}` document; no server-owned memory crosses the ABI.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle. `input` must
/// point to `input_len` readable bytes for the duration of this synchronous
/// call, and `output` must point to writable result storage. The C shim copies
/// the input before releasing the OCaml runtime lock and owns the returned
/// allocation through the normal result-release function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_terminate_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .terminate_workflow_json(input)
        })
    }
}

/// Reset one exact workflow run to a workflow-task event boundary.
///
/// The successful value is a strict execution identity for the new run. A
/// gRPC or protocol failure remains a typed native error and never becomes a
/// guessed run ID.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle. The input span
/// is borrowed only for this synchronous call and `output` follows the normal
/// initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_reset_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .reset_workflow_json(input)
        })
    }
}

/// Send one signal to one exact workflow run.
///
/// The successful value is a strict `{"acknowledged":true}` document. A
/// gRPC or protocol failure remains a typed native error and never becomes a
/// false acknowledgement.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle. The input span
/// is borrowed only for this synchronous call and `output` follows the normal
/// initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_signal_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .signal_workflow_json(input)
        })
    }
}

/// Execute one output-only query against one exact workflow run.
///
/// The successful value is a strict `{"result": [...]}` document. Query
/// rejection and gRPC failures remain structured client errors; no
/// server-controlled diagnostic text is exposed through the ABI.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle. The input span
/// is borrowed only for this synchronous call and `output` follows the normal
/// initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_query_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .query_workflow_json(input)
        })
    }
}

/// Starts one workflow update and returns a strict admitted-update response.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle. The input span
/// must remain readable for this synchronous call and `output` must point to
/// writable result storage owned by the caller.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_update_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .update_workflow_json(input)
        })
    }
}

/// Polls one admitted workflow update for a bounded interval.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle. The input span
/// must remain readable for this synchronous call and `output` must point to
/// writable result storage owned by the caller.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_poll_update_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .poll_update_workflow_json(input)
        })
    }
}

/// List one explicitly bounded visibility page. The JSON input is borrowed
/// only for this call and the output is an owned bridge result.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle. The input span
/// must point to `input_len` readable bytes for the duration of this call, and
/// `output` must point to writable storage for the initialized-result contract.
/// The caller owns the returned result and must release it with the matching
/// bridge free function, even when the status reports an error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_list_visibility_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .list_visibility_json(input)
        })
    }
}

/// Begin one workflow start and return an opaque asynchronous ticket.
///
/// The native call only validates and admits the request, then schedules the
/// Tokio operation. Poll or wait on the returned ticket to obtain a terminal
/// accepted/rejected/unknown outcome. A pending entry is owned by the runtime
/// supervisor and is never accessed concurrently from a Tokio task.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle. The input span
/// is borrowed only for this admission call and `output` follows the standard
/// initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_begin_start_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .begin_start_workflow_json(input)
        })
    }
}

/// Poll one asynchronous workflow-start ticket without waiting.
///
/// `STATUS_NOT_READY` is an expected result while the RPC remains in flight.
/// Once terminal, the successful value is a strict start-outcome document and
/// the ticket is retired, so a second poll reports an unknown-ticket protocol
/// error rather than duplicating or replaying the result.
///
/// # Safety
///
/// The runtime, input, and output contracts match
/// [`ocaml_temporal_core_v2_client_begin_start_workflow_json`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_poll_start_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .poll_start_workflow_json(input)
        })
    }
}

/// Wait for one asynchronous workflow-start ticket for a bounded interval.
///
/// The wait never blocks the OCaml runtime lock: the C stub releases that lock
/// around this ABI call, and the owner Domain regains control after at most
/// [`START_WAIT_TIMEOUT`]. A timeout is returned as `STATUS_NOT_READY`; the
/// caller can continue servicing its mailbox and wait again.
///
/// # Safety
///
/// The runtime, input, and output contracts match
/// [`ocaml_temporal_core_v2_client_begin_start_workflow_json`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_wait_start_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .wait_start_workflow_json(input)
        })
    }
}

/// Wait for one exact workflow run without following continued-as-new.
///
/// The successful value is a strict terminal-outcome document.  A
/// continued-as-new event is returned with its successor metadata instead of
/// being followed by the Rust bridge.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle.  The input
/// span is borrowed only for this synchronous call and `output` follows the
/// standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_wait_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .wait_workflow_json(input)
        })
    }
}

/// Start and validate one workflow-only official Core worker from strict JSON.
/// Success is not exposed until the namespace validation RPC completes.
///
/// # Safety
///
/// The runtime, input, and output contracts match
/// [`ocaml_temporal_core_v2_client_connect_json`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_worker_start_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` and `decode_config` enforce their respective pointer
    // preconditions before the runtime graph is mutated.
    unsafe {
        invoke(output, || {
            let runtime = runtime.as_mut().ok_or_else(|| Failure {
                status: STATUS_INVALID_ARGUMENT,
                message: "runtime pointer is null".to_owned(),
            })?;
            let config = decode_config::<WorkerConfigInput>(input, input_len)?;
            runtime.start_worker(config)
        })
    }
}

/// Start one workflow-only Core replay worker from strict worker settings.
/// Replay does not require a Temporal client because histories are supplied by
/// the caller through the bounded feeder.
///
/// # Safety
///
/// The runtime, input, and output contracts match
/// [`ocaml_temporal_core_v2_worker_start_json`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_replay_worker_start_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let runtime = runtime.as_mut().ok_or_else(|| Failure {
                status: STATUS_INVALID_ARGUMENT,
                message: "runtime pointer is null".to_owned(),
            })?;
            let config = decode_config::<WorkerConfigInput>(input, input_len)?;
            runtime.start_replay_worker(config)
        })
    }
}

/// Feed one strict replay-history JSON document to the bounded Core feeder.
///
/// # Safety
///
/// The runtime and output contracts match the workflow poll operation. The
/// input span is borrowed only for this synchronous call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_replay_worker_feed_history_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .feed_replay_history_json(input)
        })
    }
}

/// Close the replay history feeder. Repeating this operation is harmless.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output` must
/// satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_replay_worker_finish_input(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .finish_replay_input()
        })
    }
}

/// Non-blockingly take one validated replay activation JSON document.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output` must
/// satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_replay_worker_try_poll_workflow(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .try_poll_replay_workflow()
        })
    }
}

/// Wait for replay workflow readiness with the same bounded lock-release
/// contract as the live workflow wait.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output` must
/// satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_replay_worker_wait_workflow(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .wait_replay_workflow()
        })
    }
}

/// Validate and submit one replay workflow completion JSON document.
///
/// # Safety
///
/// The runtime, input, and output contracts match
/// [`ocaml_temporal_core_v2_worker_complete_workflow_json`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_replay_worker_complete_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .complete_replay_workflow(input)
        })
    }
}

/// Reject one replay activation that OCaml could not decode.
///
/// # Safety
///
/// The runtime, input, and output contracts match
/// [`ocaml_temporal_core_v2_worker_reject_workflow_json`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_replay_worker_reject_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .reject_replay_workflow(input)
        })
    }
}

/// Finalize a replay only after its feeder is closed and Core has naturally
/// drained all activations. A failure retains the worker for another attempt.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output` must
/// satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_replay_worker_finalize(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .finalize_replay()
        })
    }
}

/// Explicitly abandon replay with Core's shutdown-safe empty completions and
/// retain the worker if terminal finalization cannot yet succeed. Replay
/// activations are not live workflow tasks, so disposal never sends the
/// live-worker failure completion for an abandoned activation.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output` must
/// satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_replay_worker_dispose(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .dispose_replay()
        })
    }
}

/// Non-blockingly take one validated workflow activation JSON document.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output`
/// must satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_worker_try_poll_workflow(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .try_poll_workflow()
        })
    }
}

/// Wait for workflow-lane readiness without consuming the queued activation.
///
/// The caller must release the OCaml runtime lock around this operation. The
/// wait is bounded so a supervisor mailbox can regain control and process a
/// shutdown request even when Core has no task to deliver.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output` must
/// satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_worker_wait_workflow(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_ref()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .wait_workflow()
        })
    }
}

/// Validate and submit one workflow completion JSON document.
///
/// # Safety
///
/// The runtime and output contracts match the workflow poll operation. A
/// nonzero input length requires that many readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_worker_complete_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .complete_workflow(input)
        })
    }
}

/// Reject one Rust-produced workflow activation that OCaml could not decode.
///
/// Rust strictly reparses the original poll document and requires its complete
/// semantic activation to equal the one-shot value retained for that run.
///
/// # Safety
///
/// The runtime and output contracts match the workflow poll operation. A
/// nonzero input length requires that many readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_worker_reject_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .reject_polled_workflow(input)
        })
    }
}

/// Non-blockingly take one validated remote activity task JSON document.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output`
/// must satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_worker_try_poll_activity(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .try_poll_activity()
        })
    }
}

/// Wait for remote-activity-lane readiness without consuming the queued task.
///
/// The C binding releases the OCaml runtime lock while this bounded native wait
/// runs. A timeout is returned as `STATUS_NOT_READY` so the owner supervisor
/// can service other mailbox messages and retry.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output` must
/// satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_worker_wait_activity(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_ref()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .wait_activity()
        })
    }
}

/// Apply the bounded native delay used before retrying an explicitly
/// retryable activity completion.
///
/// The OCaml C stub invokes this operation with the runtime lock released. It
/// is a timer on the dedicated supervisor owner Domain, not a workflow timer
/// and not a readiness wait, so unrelated queued activity work cannot make a
/// retry spin.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output` must
/// satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_worker_wait_activity_completion_retry_backoff(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_ref()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .wait_activity_completion_retry_backoff()
        })
    }
}

/// Validate and submit one remote activity completion JSON document.
///
/// # Safety
///
/// The runtime and output contracts match the activity poll operation. A
/// nonzero input length requires that many readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_worker_complete_activity_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .complete_activity(input)
        })
    }
}

/// Validate and submit one remote activity heartbeat JSON document.
///
/// Success is an acknowledgement with an empty value. Temporal Core reports
/// any cancellation, pause, or reset discovered while processing this
/// heartbeat asynchronously through a later activity poll, whose
/// `ActivityTask::Cancel` document retains the independent detail flags.
///
/// # Safety
///
/// The runtime and output contracts match the activity poll operation. A
/// nonzero input length requires that many readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_worker_record_activity_heartbeat_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .record_activity_heartbeat(input)
        })
    }
}

/// Complete an activity that previously accepted asynchronous completion.
///
/// Unlike the worker completion endpoint, this operation uses the connected
/// namespace-bound Temporal client and does not consult the worker task ledger.
///
/// # Safety
///
/// The runtime and output contracts match the activity poll operation. A
/// nonzero input length requires that many readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_complete_async_activity_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .complete_async_activity(input)
        })
    }
}

/// Record a heartbeat for an activity that previously accepted asynchronous
/// completion.
///
/// # Safety
///
/// The runtime and output contracts match the activity poll operation. A
/// nonzero input length requires that many readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_record_async_activity_heartbeat_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .record_async_activity_heartbeat(input)
        })
    }
}

/// Reject one Rust-produced remote activity task that OCaml could not decode.
///
/// The closed decoder requires complete equality with a retained handoff task
/// before recovering its bounded opaque token. The native ledger makes
/// rejection one-shot and refuses unknown or repeated tokens.
///
/// # Safety
///
/// The runtime and output contracts match the activity poll operation. A
/// nonzero input length requires that many readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_worker_reject_activity_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .reject_polled_activity(input)
        })
    }
}

/// Gracefully stop and remove the runtime graph's workflow worker.
///
/// Repeating this operation after success is safe and returns success.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle. `output` follows
/// the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_worker_shutdown(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` validates the result before the closure checks and uses
    // the opaque runtime pointer.
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .shutdown_worker()
        })
    }
}

/// Remove the connected client after its worker child has stopped.
///
/// Repeating this operation when no client exists is safe and returns success.
///
/// # Safety
///
/// The pointer contracts match [`ocaml_temporal_core_v2_worker_shutdown`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_client_disconnect(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` validates output and the opaque handle is checked for
    // null before producing the unique mutable reference required by graph use.
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .disconnect_client()
        })
    }
}

/// Destroy one native runtime and clear the caller's slot.
///
/// Passing the same slot again after a successful call is safe because the
/// first call stores null before dropping the owner.
///
/// # Safety
///
/// `runtime` must be null or point to a slot initialized by
/// [`ocaml_temporal_core_v2_runtime_new`]. The slot must not be accessed
/// concurrently, and all future child handles must be closed first.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_runtime_free(runtime: *mut *mut Runtime) -> Status {
    if runtime.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // Clear first so even a defensive panic during destruction cannot
        // invite a second attempt to free the same allocation.
        // SAFETY: The caller guarantees exclusive writable access to the slot.
        let owned = unsafe { ptr::replace(runtime, ptr::null_mut()) };
        if owned.is_null() {
            STATUS_OK
        } else {
            // SAFETY: Non-null values in this slot originate from `Box::into_raw`
            // in `runtime_new` and have not been reclaimed previously.
            let runtime = unsafe { Box::from_raw(owned) };
            runtime.close(true)
        }
    }));

    match outcome {
        Ok(status) => status,
        Err(_) => STATUS_PANIC,
    }
}

/// Transfer a runtime to its cleanup thread without waiting for destruction.
///
/// This is reserved for the OCaml custom-block finalizer. Normal supervisor
/// shutdown uses [`ocaml_temporal_core_v2_runtime_free`] and waits while the
/// OCaml runtime lock is released.
///
/// # Safety
///
/// `runtime` has the same exclusive slot contract as
/// [`ocaml_temporal_core_v2_runtime_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_runtime_dispose(
    runtime: *mut *mut Runtime,
) -> Status {
    if runtime.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: The caller guarantees exclusive writable access to the slot.
        let owned = unsafe { ptr::replace(runtime, ptr::null_mut()) };
        if owned.is_null() {
            STATUS_OK
        } else {
            // SAFETY: The pointer was created by `runtime_new` and is consumed
            // exactly once by this pointer-to-pointer operation.
            let runtime = unsafe { Box::from_raw(owned) };
            runtime.close(false)
        }
    }));

    match outcome {
        Ok(status) => status,
        Err(_) => STATUS_PANIC,
    }
}

/// Release both owned buffers in a result and reset it to the empty state.
///
/// Repeated calls with the same result object are safe. Copying a live result
/// and freeing both copies is forbidden by the ownership contract.
///
/// # Safety
///
/// `result` must be null or point to a result initialized by this ABI. The
/// caller must not mutate its pointer or length fields.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v2_result_free(result: *mut Result) -> Status {
    if result.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: The caller promises this pointer refers to a bridge result
        // and has not altered either owned buffer.
        let result = unsafe { &mut *result };
        // SAFETY: Both buffers originate from this bridge or are empty.
        unsafe {
            free_buffer(&mut result.value);
            free_buffer(&mut result.error);
        }
        result.status = STATUS_OK;
    }));

    if outcome.is_ok() {
        STATUS_OK
    } else {
        STATUS_PANIC
    }
}

/// Rust-only probe proving that the shared ABI wrapper contains panics.
///
/// This symbol is not exported through the C header or assigned a stable ABI
/// name. It exists solely for the integration test crate.
///
/// # Safety
///
/// `output` follows the same contract as
/// [`ocaml_temporal_core_v2_check_abi_version`].
#[doc(hidden)]
pub unsafe fn test_invoke_panic(output: *mut Result) -> Status {
    // SAFETY: The pointer contract is forwarded unchanged to `invoke`.
    unsafe { invoke(output, || panic!("intentional ABI containment probe")) }
}

/// Returns process-local lifecycle counts for the isolated cleanup test.
///
/// This is intentionally not part of the C ABI. Keeping the counters monotonic
/// lets the test wait for asynchronous destruction without reaching into the
/// runtime owner or depending on timing alone.
#[doc(hidden)]
pub fn test_runtime_cleanup_counts() -> (u64, u64) {
    (
        RUNTIMES_CREATED.load(Ordering::Acquire),
        RUNTIMES_CLEANED.load(Ordering::Acquire),
    )
}

/// Returns the ABI category used for a worker error without exposing the
/// mapping function itself as part of the C ABI. Integration tests use this
/// helper to lock the bilateral retry classification to the explicit Rust
/// variant rather than to diagnostic text.
#[doc(hidden)]
pub fn test_worker_bridge_status(error: WorkerBridgeError) -> Status {
    worker_bridge_failure(error).status
}

#[cfg(test)]
#[path = "../tests/support/abi_rejection.rs"]
mod rejection_tests;

#[cfg(test)]
mod client_wait_tests {
    use super::bounded_client_wait;
    use crate::client_protocol::{ExecutionRef, WaitWorkflowResponse, WorkflowOutcome};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::task::{Context, Poll};

    /// Future used to prove a timed-out client request is dropped promptly.
    struct PendingWait {
        /// Set by [`Drop`] when timeout cancellation releases the request.
        dropped: Arc<AtomicBool>,
    }

    impl Future for PendingWait {
        type Output =
            std::result::Result<WaitWorkflowResponse, crate::client_protocol::ClientOperationError>;

        /// Remains pending forever, like an open Temporal history long poll.
        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingWait {
        /// Records that timeout cancellation reclaimed the in-flight request.
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    /// Builds a runtime with Tokio's timer driver for bounded-wait tests.
    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("client wait test runtime should build")
    }

    #[test]
    /// Returns `None` and drops a request that never produces a close event.
    fn timeout_cancels_pending_client_wait() {
        let runtime = test_runtime();
        let dropped = Arc::new(AtomicBool::new(false));
        let result = runtime.block_on(bounded_client_wait(PendingWait {
            dropped: Arc::clone(&dropped),
        }));

        assert_eq!(result, Ok(None));
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    /// Preserves a completed terminal response instead of turning it pending.
    fn completed_client_wait_passes_through() {
        let runtime = test_runtime();
        let response = WaitWorkflowResponse {
            execution: ExecutionRef {
                namespace: "default".to_owned(),
                workflow_id: "workflow-1".to_owned(),
                run_id: "run-1".to_owned(),
            },
            outcome: WorkflowOutcome::Completed {
                result: Vec::new(),
                successor: None,
            },
        };
        let result = runtime.block_on(bounded_client_wait(async { Ok(response.clone()) }));

        assert_eq!(result, Ok(Some(response)));
    }
}

#[cfg(test)]
mod async_activity_error_tests {
    use super::{STATUS_CONNECTION, STATUS_INVALID_STATE, async_activity_failure};
    use temporalio_client::{errors::AsyncActivityError, tonic::Status};

    /// A server-side not-found response means the retained task token is
    /// terminal, not that the OCaml supervisor should retry the same request.
    #[test]
    fn not_found_is_terminal_invalid_state() {
        let failure = async_activity_failure(
            "completion",
            AsyncActivityError::NotFound(Status::not_found("ignored")),
        );

        assert_eq!(failure.status, STATUS_INVALID_STATE);
        assert_eq!(
            failure.message,
            "Temporal asynchronous activity completion is no longer active"
        );
    }

    /// A generic RPC failure does not prove whether Temporal consumed the
    /// request, so the bridge must fail closed instead of inventing a retryable
    /// classification from the remote status text.
    #[test]
    fn rpc_failure_is_fail_closed_connection() {
        let failure = async_activity_failure(
            "heartbeat",
            AsyncActivityError::Rpc(Status::unavailable("ignored")),
        );

        assert_eq!(failure.status, STATUS_CONNECTION);
        assert_eq!(
            failure.message,
            "Temporal asynchronous activity heartbeat failed"
        );
    }
}

#[cfg(test)]
mod pending_start_reservation_tests {
    use super::{HashMap, PendingStart, STATUS_INTERNAL, reserve_pending_start_slots};

    /// A normal admission reservation creates room for the one entry that is
    /// inserted after the Tokio task is spawned.
    #[test]
    fn reserves_capacity_for_one_pending_start() {
        let mut pending: HashMap<String, PendingStart> = HashMap::new();
        reserve_pending_start_slots(&mut pending, 1)
            .expect("one pending-start slot should be reservable");
        assert!(pending.capacity() > pending.len());
    }

    /// Capacity overflow is converted into an internal operation error while
    /// the registry remains untouched, so task admission has not begun.
    #[test]
    fn rejects_unrepresentable_reservation_before_task_admission() {
        let mut pending: HashMap<String, PendingStart> = HashMap::new();
        let failure = reserve_pending_start_slots(&mut pending, usize::MAX)
            .expect_err("an impossible reservation must fail before spawning");
        assert_eq!(failure.status, STATUS_INTERNAL);
        assert!(pending.is_empty());
    }
}

#[cfg(test)]
mod worker_config_tests {
    use super::{
        MIN_CACHED_WORKFLOW_POLLS, STATUS_CONFIGURATION, VersioningBehavior, WorkerConfigInput,
    };

    /// Builds a complete worker document while varying only workflow poller
    /// concurrency, so the Core cache invariant is isolated from other fields.
    fn config(pollers: u32) -> WorkerConfigInput {
        WorkerConfigInput {
            namespace: "temporal-sdk-test".to_owned(),
            task_queue: "worker-config-test".to_owned(),
            build_id: "worker-config-test".to_owned(),
            versioning: super::WorkerVersioningInput::None,
            max_cached_workflows: 100,
            max_outstanding_workflow_tasks: 100,
            max_concurrent_workflow_task_polls: pollers,
            graceful_shutdown_timeout_ms: 1_000,
        }
    }

    /// Rejects the configuration that Core would otherwise report only after
    /// a live worker startup, keeping the public boundary deterministic.
    #[test]
    fn cached_workflows_require_two_workflow_pollers() {
        let failure = match config(MIN_CACHED_WORKFLOW_POLLS - 1).into_core() {
            Err(failure) => failure,
            Ok(_) => panic!("cached workflows must require two workflow pollers"),
        };
        assert_eq!(failure.status, STATUS_CONFIGURATION);
        assert!(
            failure
                .message
                .contains("max_concurrent_workflow_task_polls must be at least 2")
        );
    }

    /// Accepts the public default poller count before any runtime or network
    /// resource is allocated.
    #[test]
    fn cached_workflows_accept_two_workflow_pollers() {
        config(MIN_CACHED_WORKFLOW_POLLS)
            .into_core()
            .expect("two workflow pollers should satisfy Core's cache invariant");
    }

    /// Rejects a NUL before Core construction so the namespace retained for
    /// child-failure metadata can never become invalid after worker startup.
    #[test]
    fn worker_identifiers_reject_nul() {
        let mut worker = config(MIN_CACHED_WORKFLOW_POLLS);
        worker.namespace.push('\0');
        let failure = match worker.into_core() {
            Err(failure) => failure,
            Ok(_) => panic!("worker identifiers must reject NUL"),
        };
        assert_eq!(failure.status, STATUS_CONFIGURATION);
        assert!(failure.message.contains("namespace must not contain NUL"));
    }

    /// Maps the explicit legacy mode to Core's build-ID strategy instead of
    /// merely retaining the build ID as metadata.
    #[test]
    fn legacy_build_id_selects_core_versioning_strategy() {
        let mut worker = config(MIN_CACHED_WORKFLOW_POLLS);
        worker.versioning = super::WorkerVersioningInput::LegacyBuildId {
            build_id: worker.build_id.clone(),
        };
        let core = worker
            .into_core()
            .expect("matching legacy build ID should be accepted");
        assert!(matches!(
            core.versioning_strategy,
            temporalio_sdk_core::WorkerVersioningStrategy::LegacyBuildIdBased { .. }
        ));
    }

    /// Rejects a mismatched repeated build ID before Core construction so the
    /// server cannot receive contradictory worker routing metadata.
    #[test]
    fn legacy_build_id_must_match_top_level_build_id() {
        let mut worker = config(MIN_CACHED_WORKFLOW_POLLS);
        worker.versioning = super::WorkerVersioningInput::LegacyBuildId {
            build_id: "different-build".to_owned(),
        };
        let failure = match worker.into_core() {
            Err(failure) => failure,
            Ok(_) => panic!("mismatched legacy build IDs must fail closed"),
        };
        assert_eq!(failure.status, STATUS_CONFIGURATION);
        assert_eq!(failure.message, "versioning.build_id must match build_id");
    }

    /// Maps deployment-based routing to Core's modern worker deployment
    /// strategy and preserves the pinned default behavior used by workflows
    /// that do not specify an explicit versioning option.
    #[test]
    fn deployment_based_selects_core_versioning_strategy() {
        let mut worker = config(MIN_CACHED_WORKFLOW_POLLS);
        worker.versioning = super::WorkerVersioningInput::DeploymentBased {
            deployment_name: "agents".to_owned(),
            build_id: worker.build_id.clone(),
            use_worker_versioning: true,
            default_versioning_behavior: Some("pinned".to_owned()),
        };
        let core = worker
            .into_core()
            .expect("valid deployment routing should be accepted");
        match core.versioning_strategy {
            temporalio_sdk_core::WorkerVersioningStrategy::WorkerDeploymentBased(options) => {
                assert_eq!(options.version.deployment_name, "agents");
                assert_eq!(options.version.build_id, "worker-config-test");
                assert!(options.use_worker_versioning);
                assert_eq!(
                    options.default_versioning_behavior,
                    Some(VersioningBehavior::Pinned)
                );
            }
            other => panic!("unexpected Core versioning strategy: {other:?}"),
        }
    }

    /// Rejects a deployment behavior when worker versioning is disabled; this
    /// avoids silently carrying a policy that Core would ignore.
    #[test]
    fn deployment_behavior_requires_worker_versioning() {
        let mut worker = config(MIN_CACHED_WORKFLOW_POLLS);
        worker.versioning = super::WorkerVersioningInput::DeploymentBased {
            deployment_name: "agents".to_owned(),
            build_id: worker.build_id.clone(),
            use_worker_versioning: false,
            default_versioning_behavior: Some("auto_upgrade".to_owned()),
        };
        let failure = match worker.into_core() {
            Err(failure) => failure,
            Ok(_) => panic!("disabled deployment versioning must reject a default behavior"),
        };
        assert_eq!(failure.status, STATUS_CONFIGURATION);
        assert!(
            failure
                .message
                .contains("default_versioning_behavior requires use_worker_versioning")
        );
    }
}

#[cfg(test)]
#[path = "../tests/support/pending_start_cleanup.rs"]
mod pending_start_cleanup_tests;
