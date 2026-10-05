//! Inbound Core activations that Temporal Server accepts but that exceed the
//! semantic protocol's safety limits (#802).
//!
//! The history event behind an activation is immutable, so a rejected
//! activation fails the same workflow task on every replay. These tests prove
//! that such activations instead degrade deterministically, that the degraded
//! document passes the strict semantic encoder and decoder, and that
//! SDK-produced (outbound) semantic documents remain strictly validated.

use ocaml_temporal_core_bridge::{
    protocol::MAX_STRING_BYTES,
    workflow_protocol::{
        self, ActivationJob, ActivityResolution, EvictionReason, Failure, FailureInfo,
    },
};
use temporalio_protos::{
    coresdk::{activity_result, workflow_activation as core_activation},
    temporal::api::{
        common::v1::{self as api_common, Payload as CorePayload},
        failure::v1 as api_failure,
        update::v1::Meta,
    },
};

/// Prefix of the marker [`workflow_protocol`] appends to truncated text.
const TRUNCATION_MARKER: &str = "\n[truncated by ocaml-temporal: original length ";

/// Wraps one job in an activation with the required deterministic timestamp.
fn activation(
    job: core_activation::workflow_activation_job::Variant,
) -> core_activation::WorkflowActivation {
    core_activation::WorkflowActivation {
        run_id: "run-802".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob { variant: Some(job) }],
        ..Default::default()
    }
}

/// Builds a `ResolveActivity` activation whose activity failed with `failure`.
fn failed_activity(failure: api_failure::Failure) -> core_activation::WorkflowActivation {
    activation(
        core_activation::workflow_activation_job::Variant::ResolveActivity(
            core_activation::ResolveActivity {
                seq: 1,
                result: Some(activity_result::ActivityResolution {
                    status: Some(activity_result::activity_resolution::Status::Failed(
                        activity_result::Failure {
                            failure: Some(failure),
                        },
                    )),
                }),
                is_local: false,
            },
        ),
    )
}

/// Builds an application failure with the given free text and cause.
fn application_failure(
    message: String,
    stack_trace: String,
    cause: Option<api_failure::Failure>,
) -> api_failure::Failure {
    api_failure::Failure {
        message,
        source: "JavaSDK".to_owned(),
        stack_trace,
        encoded_attributes: None,
        cause: cause.map(Box::new),
        failure_info: Some(api_failure::failure::FailureInfo::ApplicationFailureInfo(
            api_failure::ApplicationFailureInfo {
                r#type: "RemoteError".to_owned(),
                ..Default::default()
            },
        )),
    }
}

/// Converts, encodes, and strictly re-decodes one activation, proving the
/// result is representable on both sides of the bridge and that conversion
/// is a pure function of the Core value (the replay-stability requirement).
fn convert_and_round_trip(
    value: &core_activation::WorkflowActivation,
) -> workflow_protocol::Activation {
    let semantic = workflow_protocol::activation_from_core(value)
        .expect("server-valid activation must stay representable");
    assert_eq!(
        workflow_protocol::activation_from_core(value).unwrap(),
        semantic,
        "inbound degradation must be deterministic for replay"
    );
    let encoded = workflow_protocol::encode_activation(&semantic)
        .expect("degraded activation must pass the strict semantic encoder");
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );
    semantic
}

/// Returns the failure of the single failed `ResolveActivity` job.
fn resolved_failure(semantic: &workflow_protocol::Activation) -> &Failure {
    match semantic.jobs.as_slice() {
        [
            ActivationJob::ResolveActivity {
                result: ActivityResolution::Failed { failure },
                ..
            },
        ] => failure,
        jobs => panic!("unexpected activity resolution jobs: {jobs:?}"),
    }
}

/// Asserts `actual` is `original` truncated at a character boundary with the
/// marker naming the original byte length, and within the string limit.
fn assert_truncated(actual: &str, original: &str) {
    assert!(actual.len() <= MAX_STRING_BYTES);
    let marker = format!("{TRUNCATION_MARKER}{} bytes]", original.len());
    let kept = actual
        .strip_suffix(&marker)
        .expect("truncated text must end with the length marker");
    assert!(
        original.starts_with(kept),
        "kept text must be an exact prefix"
    );
    // The cut is at most three bytes (one partial UTF-8 sequence) short of
    // the space the marker leaves.
    assert!(kept.len() + marker.len() > MAX_STRING_BYTES - 4);
}

/// The issue's reproduction: a 65,537-byte failure message used to reject the
/// activation. Message, source, and stack trace are each truncated at a UTF-8
/// boundary in every layer, while text at exactly the limit is untouched.
#[test]
fn truncates_oversized_failure_text_at_utf8_boundary() {
    let message = "m".repeat(MAX_STRING_BYTES + 1);
    // Four-byte characters guarantee the naive cut falls mid-sequence for
    // at least one of the probed lengths.
    let stack_trace = "\u{1F600}".repeat(50_000);
    let exact = "x".repeat(MAX_STRING_BYTES);
    let cause = application_failure(exact.clone(), stack_trace.clone(), None);
    let mut top = application_failure(message.clone(), stack_trace.clone(), Some(cause));
    top.source = "s".repeat(3 * MAX_STRING_BYTES);
    let original_source = top.source.clone();

    let semantic = convert_and_round_trip(&failed_activity(top));
    let failure = resolved_failure(&semantic);
    assert_truncated(&failure.message, &message);
    assert_truncated(&failure.source, &original_source);
    assert_truncated(&failure.stack_trace, &stack_trace);
    let cause = failure.cause.as_deref().expect("cause must be retained");
    assert_eq!(cause.message, exact, "text at the limit must be lossless");
    assert_truncated(&cause.stack_trace, &stack_trace);
    assert!(matches!(
        &cause.info,
        FailureInfo::Application { type_name, .. } if type_name == "RemoteError"
    ));

    // A workflow that rethrows the received failure produces an outbound
    // completion; the truncated text must satisfy outbound validation too.
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-802".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::FailWorkflow {
            failure: failure.clone(),
        }],
    };
    workflow_protocol::encode_completion(&completion)
        .expect("rethrown truncated failure must remain a valid completion");
}

/// Failure-info identities and other free text are truncated, as are the
/// cancellation reason and Core-supplied metadata text.
#[test]
fn truncates_other_inbound_free_text() {
    let long = "i".repeat(MAX_STRING_BYTES + 10);
    let terminated = api_failure::Failure {
        message: "terminated".to_owned(),
        failure_info: Some(api_failure::failure::FailureInfo::TerminatedFailureInfo(
            api_failure::TerminatedFailureInfo {
                identity: long.clone(),
            },
        )),
        ..Default::default()
    };
    let semantic = convert_and_round_trip(&failed_activity(terminated));
    match &resolved_failure(&semantic).info {
        FailureInfo::Terminated { identity } => assert_truncated(identity, &long),
        info => panic!("unexpected failure info: {info:?}"),
    }

    let mut cancel = activation(
        core_activation::workflow_activation_job::Variant::CancelWorkflow(
            core_activation::CancelWorkflow {
                reason: long.clone(),
            },
        ),
    );
    cancel.last_sdk_version = long.clone();
    let semantic = convert_and_round_trip(&cancel);
    match semantic.jobs.as_slice() {
        [ActivationJob::CancelWorkflow { reason }] => assert_truncated(reason, &long),
        jobs => panic!("unexpected cancellation jobs: {jobs:?}"),
    }
    assert_truncated(&semantic.metadata.as_ref().unwrap().last_sdk_version, &long);
}

/// Core builds the eviction that follows a failed activation completion from a
/// Debug dump of the whole failure, rendering each payload byte as several
/// characters (#814). Such a message must truncate rather than reject the
/// eviction-only activation, keep Core's absent eviction timestamp, and leave
/// the activation recognizable as a pure eviction.
#[test]
fn truncates_oversized_eviction_message() {
    let details = vec![104u8; 20_000];
    let long = format!("Workflow activation completion failed: Failure {{ details: {details:?} }}");
    assert!(long.len() > MAX_STRING_BYTES);
    let eviction = core_activation::WorkflowActivation {
        run_id: "run-814".to_owned(),
        timestamp: None,
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(
                core_activation::workflow_activation_job::Variant::RemoveFromCache(
                    core_activation::RemoveFromCache {
                        message: long.clone(),
                        reason: core_activation::remove_from_cache::EvictionReason::LangFail as i32,
                    },
                ),
            ),
        }],
        ..Default::default()
    };
    assert!(eviction.is_only_eviction());
    let semantic = convert_and_round_trip(&eviction);
    assert_eq!(semantic.timestamp, None);
    match semantic.jobs.as_slice() {
        [ActivationJob::RemoveFromCache { message, reason }] => {
            assert_truncated(message, &long);
            assert_eq!(*reason, EvictionReason::LangFail);
        }
        jobs => panic!("unexpected eviction jobs: {jobs:?}"),
    }
}

/// Builds a linear application-failure chain with `layers` failures.
fn failure_chain(layers: usize) -> api_failure::Failure {
    (1..layers).fold(
        application_failure("layer".to_owned(), String::new(), None),
        |cause, _| application_failure("layer".to_owned(), String::new(), Some(cause)),
    )
}

/// Counts the failures in a semantic cause chain.
fn chain_layers(failure: &Failure) -> usize {
    std::iter::successors(Some(failure), |layer| layer.cause.as_deref()).count()
}

/// A chain within the layer cap converts unchanged; a longer chain keeps the
/// first layers exactly and replaces the remainder with one synthetic layer
/// that names how many causes were omitted, so the document stays inside the
/// 128-level JSON nesting limit.
#[test]
fn caps_deeply_nested_failure_causes() {
    let max = workflow_protocol::MAX_INBOUND_FAILURE_LAYERS;

    let semantic = convert_and_round_trip(&failed_activity(failure_chain(max)));
    let failure = resolved_failure(&semantic);
    assert_eq!(chain_layers(failure), max);
    assert!(
        std::iter::successors(Some(failure), |layer| layer.cause.as_deref())
            .all(|layer| layer.message == "layer")
    );

    let total = max + 50;
    let semantic = convert_and_round_trip(&failed_activity(failure_chain(total)));
    let failure = resolved_failure(&semantic);
    assert_eq!(chain_layers(failure), max);
    let layers: Vec<_> =
        std::iter::successors(Some(failure), |layer| layer.cause.as_deref()).collect();
    assert!(
        layers[..max - 1]
            .iter()
            .all(|layer| layer.message == "layer")
    );
    let synthetic = layers[max - 1];
    assert!(matches!(synthetic.info, FailureInfo::Absent {}));
    assert!(synthetic.cause.is_none());
    assert_eq!(
        synthetic.message,
        format!(
            "[truncated by ocaml-temporal: {} deeper failure causes omitted beyond {max} layers]",
            total - (max - 1)
        )
    );

    // Rethrowing the capped chain as the cause of a new workflow failure
    // still fits the outbound completion's nesting limit.
    let completion = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-802".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::FailWorkflow {
            failure: Failure {
                cause: Some(Box::new(failure.clone())),
                ..failure.clone()
            },
        }],
    };
    workflow_protocol::encode_completion(&completion)
        .expect("rethrown capped chain must remain a valid completion");
}

/// The synthetic layer must not change public retryability. A chain whose
/// retained prefix only defers (failures without failure info) and whose
/// omitted tail ends in a non-retryable application failure must remain
/// non-retryable, so the stand-in carries an authoritative server decision;
/// a retryable tail keeps the deferring `Absent` stand-in.
#[test]
fn capped_chain_preserves_omitted_tail_retryability() {
    let max = workflow_protocol::MAX_INBOUND_FAILURE_LAYERS;
    let deferring_chain = |non_retryable: bool| {
        let mut leaf = application_failure("leaf".to_owned(), String::new(), None);
        if let Some(api_failure::failure::FailureInfo::ApplicationFailureInfo(info)) =
            leaf.failure_info.as_mut()
        {
            info.non_retryable = non_retryable;
        }
        (1..max + 20).fold(leaf, |cause, _| api_failure::Failure {
            message: "wrapper".to_owned(),
            cause: Some(Box::new(cause)),
            ..Default::default()
        })
    };

    let semantic = convert_and_round_trip(&failed_activity(deferring_chain(true)));
    let failure = resolved_failure(&semantic);
    let last = std::iter::successors(Some(failure), |layer| layer.cause.as_deref())
        .last()
        .unwrap();
    assert_eq!(chain_layers(failure), max);
    assert!(matches!(
        last.info,
        FailureInfo::Server {
            non_retryable: true
        }
    ));

    let semantic = convert_and_round_trip(&failed_activity(deferring_chain(false)));
    let failure = resolved_failure(&semantic);
    let last = std::iter::successors(Some(failure), |layer| layer.cause.as_deref())
        .last()
        .unwrap();
    assert!(matches!(last.info, FailureInfo::Absent {}));
}

/// The deepest place a failure appears in an activation is a continuation's
/// `continued_failure`. A chain at the layer cap whose final layer carries a
/// detail payload with metadata (the deepest JSON beneath one layer) must
/// still fit the 128-level nesting limit there.
#[test]
fn capped_chain_fits_the_deepest_activation_location() {
    let max = workflow_protocol::MAX_INBOUND_FAILURE_LAYERS;
    let mut innermost = application_failure("leaf".to_owned(), String::new(), None);
    if let Some(api_failure::failure::FailureInfo::ApplicationFailureInfo(info)) =
        innermost.failure_info.as_mut()
    {
        info.details = Some(api_common::Payloads {
            payloads: vec![payload(b"detail")],
        });
    }
    let chain = (1..max).fold(innermost, |cause, _| {
        application_failure("layer".to_owned(), String::new(), Some(cause))
    });
    let initialize = core_activation::InitializeWorkflow {
        workflow_type: "workflow".to_owned(),
        workflow_id: "workflow-1".to_owned(),
        randomness_seed: 1,
        attempt: 1,
        first_execution_run_id: "first-run".to_owned(),
        continued_from_execution_run_id: "previous-run".to_owned(),
        continued_initiator: 1,
        continued_failure: Some(chain),
        ..Default::default()
    };
    let semantic = convert_and_round_trip(&activation(
        core_activation::workflow_activation_job::Variant::InitializeWorkflow(initialize),
    ));
    let [
        ActivationJob::InitializeWorkflow {
            context: Some(context),
            ..
        },
    ] = semantic.jobs.as_slice()
    else {
        panic!("unexpected initialize jobs: {:?}", semantic.jobs);
    };
    let failure = context
        .continuation
        .as_ref()
        .and_then(|continuation| continuation.continued_failure.as_ref())
        .expect("continued failure must be retained");
    assert_eq!(chain_layers(failure), max);
}

/// A test payload whose bytes must survive conversion exactly.
fn payload(data: &[u8]) -> CorePayload {
    CorePayload {
        metadata: [("encoding".to_owned(), b"binary/plain".to_vec())].into(),
        data: data.to_vec(),
        ..Default::default()
    }
}

/// The issue's signal reproduction: an empty header key and a NUL identity
/// used to reject the activation. Unaddressable keys are dropped, every other
/// header is preserved exactly, and NUL in the identity becomes U+FFFD.
#[test]
fn signal_with_unaddressable_header_keys_and_nul_identity_is_delivered() {
    let signal = activation(
        core_activation::workflow_activation_job::Variant::SignalWorkflow(
            core_activation::SignalWorkflow {
                signal_name: "order_updated".to_owned(),
                input: vec![payload(b"input")],
                identity: "client\0one".to_owned(),
                headers: [
                    (String::new(), payload(b"empty-key")),
                    ("nul\0key".to_owned(), payload(b"nul-key")),
                    ("k".repeat(MAX_STRING_BYTES + 1), payload(b"long-key")),
                    ("trace".to_owned(), payload(b"trace-value")),
                ]
                .into(),
            },
        ),
    );
    let semantic = convert_and_round_trip(&signal);
    match semantic.jobs.as_slice() {
        [
            ActivationJob::SignalWorkflow {
                identity,
                headers,
                input,
                ..
            },
        ] => {
            assert_eq!(identity, "client\u{FFFD}one");
            assert_eq!(headers.keys().collect::<Vec<_>>(), ["trace"]);
            assert_eq!(headers["trace"].data, b"trace-value");
            assert_eq!(input[0].data, b"input");
        }
        jobs => panic!("unexpected signal jobs: {jobs:?}"),
    }
}

/// Update identities follow the same NUL rule as signal identities, and query
/// and update headers follow the same key rule as signal headers.
#[test]
fn update_and_query_inbound_identity_and_header_rules() {
    let update = activation(core_activation::workflow_activation_job::Variant::DoUpdate(
        core_activation::DoUpdate {
            id: "update-1".to_owned(),
            protocol_instance_id: "protocol-1".to_owned(),
            name: "set".to_owned(),
            input: Vec::new(),
            headers: [
                (String::new(), payload(b"dropped")),
                ("kept".to_owned(), payload(b"kept")),
            ]
            .into(),
            meta: Some(Meta {
                update_id: "update-1".to_owned(),
                identity: "\0".to_owned(),
            }),
            run_validator: true,
        },
    ));
    let semantic = convert_and_round_trip(&update);
    match semantic.jobs.as_slice() {
        [ActivationJob::DoUpdate { headers, meta, .. }] => {
            assert_eq!(meta.identity, "\u{FFFD}");
            assert_eq!(headers.keys().collect::<Vec<_>>(), ["kept"]);
        }
        jobs => panic!("unexpected update jobs: {jobs:?}"),
    }

    let query = activation(
        core_activation::workflow_activation_job::Variant::QueryWorkflow(
            core_activation::QueryWorkflow {
                query_id: "q-1".to_owned(),
                query_type: "status".to_owned(),
                arguments: Vec::new(),
                headers: [("\0".to_owned(), payload(b"dropped"))].into(),
            },
        ),
    );
    let semantic = convert_and_round_trip(&query);
    match semantic.jobs.as_slice() {
        [ActivationJob::QueryWorkflow { headers, .. }] => assert!(headers.is_empty()),
        jobs => panic!("unexpected query jobs: {jobs:?}"),
    }
}

/// Workflow start headers and memo apply the same key rule; an explicitly
/// empty memo stays present rather than collapsing to absent.
#[test]
fn initialize_drops_unaddressable_header_and_memo_keys() {
    let initialize = core_activation::InitializeWorkflow {
        workflow_type: "start".to_owned(),
        workflow_id: "start-1".to_owned(),
        first_execution_run_id: "first-run".to_owned(),
        attempt: 1,
        identity: "starter\0".to_owned(),
        headers: [
            (String::new(), payload(b"dropped")),
            ("trace".to_owned(), payload(b"kept")),
        ]
        .into(),
        memo: Some(api_common::Memo {
            fields: [(String::new(), payload(b"dropped"))].into(),
        }),
        ..Default::default()
    };
    let semantic = convert_and_round_trip(&activation(
        core_activation::workflow_activation_job::Variant::InitializeWorkflow(initialize),
    ));
    match semantic.jobs.as_slice() {
        [
            ActivationJob::InitializeWorkflow {
                context: Some(context),
                ..
            },
        ] => {
            assert_eq!(context.headers.keys().collect::<Vec<_>>(), ["trace"]);
            assert_eq!(context.headers["trace"].data, b"kept");
            assert_eq!(context.memo.as_ref().map(|memo| memo.len()), Some(0));
            // The start identity has no NUL restriction in either decoder,
            // so it is preserved exactly.
            assert_eq!(context.identity, "starter\0");
        }
        jobs => panic!("unexpected initialize jobs: {jobs:?}"),
    }
}

/// Outbound (SDK-produced or semantic) documents keep the strict rules: the
/// relaxations above apply only to the Core-to-semantic conversion.
#[test]
fn semantic_documents_remain_strict() {
    let oversized = workflow_protocol::Completion {
        task_failure: None,
        run_id: "run-802".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::FailWorkflow {
            failure: Failure {
                message: "m".repeat(MAX_STRING_BYTES + 1),
                source: String::new(),
                stack_trace: String::new(),
                encoded_attributes: None,
                cause: None,
                info: FailureInfo::Absent {},
            },
        }],
    };
    assert!(workflow_protocol::encode_completion(&oversized).is_err());

    let semantic = workflow_protocol::Activation {
        run_id: "run-802".to_owned(),
        timestamp: Some(workflow_protocol::Timestamp {
            seconds: 0,
            nanoseconds: 0,
        }),
        is_replaying: false,
        history_length: 0,
        jobs: vec![ActivationJob::SignalWorkflow {
            signal_name: "order_updated".to_owned(),
            input: Vec::new(),
            identity: String::new(),
            headers: [(
                String::new(),
                workflow_protocol::Payload {
                    metadata: Default::default(),
                    data: Vec::new(),
                },
            )]
            .into(),
        }],
        metadata: None,
    };
    assert!(workflow_protocol::encode_activation(&semantic).is_err());
}
