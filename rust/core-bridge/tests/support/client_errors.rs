//! Client RPC error classification regressions (issue #823). The pure mapping
//! tests cover every gRPC status the bridge can report and the bounded query
//! failure message; the transport tests drive real Core connections whose only
//! server is Core's in-memory callback transport, so a query handler failure
//! and permanent statuses arrive through the same tonic path as a live server.

use super::*;
use prost::Message;
use std::sync::Arc;
use temporalio_client::ConnectionOptions;
use temporalio_client::callback_based::CallbackBasedGrpcService;
use temporalio_client::tonic::Status as RpcStatus;
use temporalio_client::tonic::codegen::Bytes;
use temporalio_common::protos::google::rpc::Status as RpcStatusDetails;
use temporalio_common::protos::temporal::api::{
    errordetails::v1::QueryFailedFailure, failure::v1::Failure as CoreFailure,
};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, TokioRuntimeBuilder};

/// Encodes the `google.rpc.Status` detail bytes Temporal Server attaches to a
/// failed query: one `Any` naming `type_url` that wraps `detail`.
fn status_details(code: Code, message: &str, type_url: &str, detail: Vec<u8>) -> Bytes {
    let status = RpcStatusDetails {
        code: i32::from(code),
        message: message.to_owned(),
        details: vec![prost_wkt_types::Any {
            type_url: type_url.to_owned(),
            value: detail,
        }],
    };
    Bytes::from(status.encode_to_vec())
}

/// Builds the status Temporal Server returns when a query handler fails:
/// `InvalidArgument`, the handler message as status text, and a
/// `QueryFailedFailure` detail that optionally carries the full failure.
fn query_failed_status(status_message: &str, failure_message: Option<&str>) -> RpcStatus {
    let detail = QueryFailedFailure {
        failure: failure_message.map(|message| CoreFailure {
            message: message.to_owned(),
            ..Default::default()
        }),
    };
    RpcStatus::with_details(
        Code::InvalidArgument,
        status_message,
        status_details(
            Code::InvalidArgument,
            status_message,
            QUERY_FAILED_TYPE_URL,
            detail.encode_to_vec(),
        ),
    )
}

/// Every gRPC status keeps its stable snake_case code through the generic
/// mapping, and the server text never reaches the closed error document.
#[test]
fn every_status_code_maps_to_its_stable_rpc_code() {
    let cases = [
        (Code::Cancelled, "cancelled"),
        (Code::Unknown, "unknown"),
        (Code::InvalidArgument, "invalid_argument"),
        (Code::DeadlineExceeded, "deadline_exceeded"),
        (Code::NotFound, "not_found"),
        (Code::AlreadyExists, "already_exists"),
        (Code::PermissionDenied, "permission_denied"),
        (Code::ResourceExhausted, "resource_exhausted"),
        (Code::FailedPrecondition, "failed_precondition"),
        (Code::Aborted, "aborted"),
        (Code::OutOfRange, "out_of_range"),
        (Code::Unimplemented, "unimplemented"),
        (Code::Internal, "internal"),
        (Code::Unavailable, "unavailable"),
        (Code::DataLoss, "data_loss"),
        (Code::Unauthenticated, "unauthenticated"),
    ];
    for (code, expected) in cases {
        let error = map_query_status(RpcStatus::new(code, "secret server text"));
        assert_eq!(
            error,
            ClientOperationError::Rpc {
                code: expected.to_owned()
            }
        );
        let json = error.to_json();
        assert_eq!(json, format!(r#"{{"kind":"rpc","code":"{expected}"}}"#));
        assert!(!json.contains("secret"));
    }
}

/// The `QueryFailedFailure` detail turns `InvalidArgument` into the distinct
/// `query_failed` kind; the full failure's message is preferred over the
/// status text, which is only the server's copy of it.
#[test]
fn query_failed_detail_yields_query_failed_with_handler_message() {
    let error = map_query_status(query_failed_status("status copy", Some("handler said no")));
    assert_eq!(
        error,
        ClientOperationError::QueryFailed {
            message: "handler said no".to_owned()
        }
    );
    assert_eq!(
        error.to_json(),
        r#"{"kind":"query_failed","message":"handler said no"}"#
    );
    assert!(!error.uncertain_start());

    // An SDK that predates `QueryFailedFailure.failure` leaves it empty; the
    // status message is then the only copy of the handler's message.
    let legacy = map_query_status(query_failed_status("legacy handler message", None));
    assert_eq!(
        legacy,
        ClientOperationError::QueryFailed {
            message: "legacy handler message".to_owned()
        }
    );
}

/// Without the detail, or with a detail of another type, `InvalidArgument`
/// is an ordinary malformed-request RPC error. Core's generic detail decoder
/// ignores the type URL, so this guards the explicit check.
#[test]
fn invalid_argument_without_query_detail_stays_rpc() {
    let plain = map_query_status(RpcStatus::new(Code::InvalidArgument, "bad request"));
    assert_eq!(
        plain,
        ClientOperationError::Rpc {
            code: "invalid_argument".to_owned()
        }
    );

    let detail = QueryFailedFailure {
        failure: Some(CoreFailure {
            message: "not a query failure".to_owned(),
            ..Default::default()
        }),
    };
    let other_type = RpcStatus::with_details(
        Code::InvalidArgument,
        "bad request",
        status_details(
            Code::InvalidArgument,
            "bad request",
            "type.googleapis.com/google.rpc.BadRequest",
            detail.encode_to_vec(),
        ),
    );
    assert_eq!(
        map_query_status(other_type),
        ClientOperationError::Rpc {
            code: "invalid_argument".to_owned()
        }
    );

    let garbage = RpcStatus::with_details(
        Code::InvalidArgument,
        "bad request",
        Bytes::from_static(b"\xff\xff\xff"),
    );
    assert_eq!(
        map_query_status(garbage),
        ClientOperationError::Rpc {
            code: "invalid_argument".to_owned()
        }
    );
}

/// A query failure detail on any other code is not a query handler failure;
/// Temporal only reports handler failures as `InvalidArgument`.
#[test]
fn query_detail_on_other_code_stays_rpc() {
    let detail = QueryFailedFailure::default();
    let status = RpcStatus::with_details(
        Code::FailedPrecondition,
        "closed",
        status_details(
            Code::FailedPrecondition,
            "closed",
            QUERY_FAILED_TYPE_URL,
            detail.encode_to_vec(),
        ),
    );
    assert_eq!(
        map_query_status(status),
        ClientOperationError::Rpc {
            code: "failed_precondition".to_owned()
        }
    );
}

/// The handler message is truncated at a character boundary and NUL becomes
/// U+FFFD, so the closed document honors the bilateral string contract.
#[test]
fn query_failure_message_is_bounded_and_nul_free() {
    assert_eq!(bounded_query_failure_message("a\0b"), "a\u{fffd}b");
    assert_eq!(bounded_query_failure_message(""), "");

    let long = "x".repeat(MAX_QUERY_FAILURE_MESSAGE_BYTES + 10);
    assert_eq!(
        bounded_query_failure_message(&long).len(),
        MAX_QUERY_FAILURE_MESSAGE_BYTES
    );

    // A two-byte character straddling the limit is dropped whole.
    let straddling = format!("{}é", "x".repeat(MAX_QUERY_FAILURE_MESSAGE_BYTES - 1));
    let bounded = bounded_query_failure_message(&straddling);
    assert_eq!(bounded.len(), MAX_QUERY_FAILURE_MESSAGE_BYTES - 1);
    assert!(bounded.chars().all(|character| character == 'x'));

    let error = map_query_status(query_failed_status("copy", Some(&long)));
    let ClientOperationError::QueryFailed { message } = error else {
        panic!("expected a query failure");
    };
    assert_eq!(message.len(), MAX_QUERY_FAILURE_MESSAGE_BYTES);
}

/// A persisted start outcome can never be a query failure.
#[test]
fn start_outcome_rejects_query_failed_document() {
    let document = StartWorkflowOutcomeDocument::Rejected {
        error: ClientErrorDocument::QueryFailed {
            message: "no".to_owned(),
        },
    };
    assert!(validate_start_outcome_document(&document).is_err());
}

/// Creates a Core runtime and a connection whose server answers every
/// workflow-service RPC with `reply()`. The runtime must outlive every future
/// using the connection, so both are returned.
fn failing_connection(reply: Reply) -> (CoreRuntime, Connection) {
    let options = RuntimeOptions::builder().build().expect("runtime options");
    let core = CoreRuntime::new(options, TokioRuntimeBuilder::default()).expect("Core runtime");
    let service = CallbackBasedGrpcService {
        callback: Arc::new(move |request| {
            Box::pin(async move {
                if request.rpc == "GetSystemInfo" {
                    return Err(RpcStatus::unimplemented(
                        "Method temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo is unimplemented",
                    ));
                }
                Err(reply())
            })
        }),
    };
    let options =
        ConnectionOptions::new(temporalio_sdk_core::Url::parse("http://localhost:7233").unwrap())
            .service_override(service)
            .dns_load_balancing(None)
            .build();
    let connection = core
        .tokio_handle()
        .block_on(Connection::connect(options))
        .expect("callback connection");
    (core, connection)
}

/// Runs one query on the runtime that owns `connection`.
fn query(
    core: &CoreRuntime,
    connection: Connection,
) -> Result<QueryWorkflowResponse, ClientOperationError> {
    core.tokio_handle().block_on(query_workflow(
        connection,
        QueryWorkflowRequest {
            namespace: "default".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            run_id: "run-1".to_owned(),
            query_type: "state".to_owned(),
            input: Vec::new(),
        },
    ))
}

/// Runs one signal on the runtime that owns `connection`.
fn signal(
    core: &CoreRuntime,
    connection: Connection,
) -> Result<SignalWorkflowResponse, ClientOperationError> {
    core.tokio_handle().block_on(signal_workflow(
        connection,
        SignalWorkflowRequest {
            namespace: "default".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            run_id: "run-1".to_owned(),
            signal_name: "poke".to_owned(),
            request_id: "signal-1".to_owned(),
            input: Vec::new(),
        },
    ))
}

/// A server reporting a failed query handler reaches the caller as the
/// typed query failure with the handler's message, through a real transport.
#[test]
fn query_handler_failure_over_transport_is_query_failed() {
    let (core, connection) =
        failing_connection(|| query_failed_status("status copy", Some("unknown query state")));
    assert_eq!(
        query(&core, connection).unwrap_err(),
        ClientOperationError::QueryFailed {
            message: "unknown query state".to_owned()
        }
    );
}

/// Builds the status a scripted server replies with.
type Reply = fn() -> RpcStatus;

/// Permanent statuses on a query keep their code; only the detail makes a
/// query failure, so a malformed query is not mistaken for one.
#[test]
fn permanent_query_statuses_over_transport_keep_their_code() {
    let cases: [(Reply, &str); 3] = [
        (
            || RpcStatus::invalid_argument("malformed"),
            "invalid_argument",
        ),
        (|| RpcStatus::not_found("no such run"), "not_found"),
        (
            || RpcStatus::permission_denied("no access"),
            "permission_denied",
        ),
    ];
    for (reply, expected) in cases {
        let (core, connection) = failing_connection(reply);
        assert_eq!(
            query(&core, connection).unwrap_err(),
            ClientOperationError::Rpc {
                code: expected.to_owned()
            }
        );
    }
}

/// A query failure detail on a non-query RPC is not reinterpreted: only the
/// query operation recognizes `QueryFailedFailure`.
#[test]
fn signal_never_reports_query_failed() {
    let (core, connection) =
        failing_connection(|| query_failed_status("status copy", Some("handler")));
    assert_eq!(
        signal(&core, connection).unwrap_err(),
        ClientOperationError::Rpc {
            code: "invalid_argument".to_owned()
        }
    );
    let (core, connection) = failing_connection(|| RpcStatus::not_found("closed"));
    assert_eq!(
        signal(&core, connection).unwrap_err(),
        ClientOperationError::Rpc {
            code: "not_found".to_owned()
        }
    );
}
