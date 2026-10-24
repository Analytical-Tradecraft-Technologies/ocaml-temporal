//! Regression coverage for issue #1159: a workflow type's own versioning
//! behavior travels with a successful workflow completion and reaches Core
//! as `Success.versioning_behavior`.

use ocaml_temporal_core_bridge::workflow_protocol::{
    self, Completion, CompletionVersioningBehavior,
};
use temporalio_protos::coresdk::workflow_completion as core_completion;

/// A successful completion of one empty activation with `behavior`.
fn completion(behavior: Option<CompletionVersioningBehavior>) -> Completion {
    Completion {
        run_id: "versioned-run".to_owned(),
        commands: Vec::new(),
        task_failure: None,
        versioning_behavior: behavior,
    }
}

/// Core's `versioning_behavior` wire value for one converted completion.
fn core_behavior(value: &Completion) -> i32 {
    let core = workflow_protocol::completion_to_core(value).expect("valid completion");
    match core.status {
        Some(core_completion::workflow_activation_completion::Status::Successful(success)) => {
            success.versioning_behavior
        }
        other => panic!("expected a successful completion, got {other:?}"),
    }
}

/// Each behavior round-trips through the JSON document as `pinned` or
/// `auto_upgrade`, reaches Core as `PINNED` (1) or `AUTO_UPGRADE` (2), and
/// comes back from Core unchanged; an absent behavior is omitted from the
/// document and stays `UNSPECIFIED` (0).
#[test]
fn versioning_behavior_reaches_core() {
    for (behavior, wire, json) in [
        (None, 0, None),
        (
            Some(CompletionVersioningBehavior::Pinned),
            1,
            Some("pinned"),
        ),
        (
            Some(CompletionVersioningBehavior::AutoUpgrade),
            2,
            Some("auto_upgrade"),
        ),
    ] {
        let value = completion(behavior);
        let encoded = workflow_protocol::encode_completion(&value).expect("encodes");
        match json {
            Some(name) => assert!(
                encoded.contains(&format!(r#""versioning_behavior":"{name}""#)),
                "{encoded}"
            ),
            None => assert!(!encoded.contains("versioning_behavior"), "{encoded}"),
        }
        assert_eq!(
            workflow_protocol::decode_completion(&encoded).expect("decodes"),
            value
        );
        assert_eq!(core_behavior(&value), wire);
        let core = workflow_protocol::completion_to_core(&value).expect("converts");
        assert_eq!(
            workflow_protocol::completion_from_core(&core).expect("converts back"),
            value
        );
    }
}

/// An unknown behavior name and a behavior on a failed workflow task are
/// rejected by the strict decoder; `null` reads as absent, as for
/// `task_failure`.
#[test]
fn versioning_behavior_is_validated() {
    for document in [
        r#"{"run_id":"r","commands":[],"versioning_behavior":"sticky"}"#,
        r#"{"run_id":"r","commands":[],"versioning_behavior":"pinned","task_failure":{"message":"m","source":"s","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"absent"}}}"#,
    ] {
        assert!(
            workflow_protocol::decode_completion(document).is_err(),
            "{document}"
        );
    }
    assert_eq!(
        workflow_protocol::decode_completion(
            r#"{"run_id":"r","commands":[],"versioning_behavior":null}"#
        )
        .expect("null is absent")
        .versioning_behavior,
        None
    );
}
