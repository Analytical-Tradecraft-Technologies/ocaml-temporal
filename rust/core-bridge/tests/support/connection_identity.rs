//! SDK name and version reported on every Core client connection.
//!
//! Mounted from `abi.rs` with `#[path]` rather than built as an
//! integration test because [`super::connection_options`] and
//! [`super::SDK_VERSION`] are private to the ABI module.

/// Every connection reports this SDK's own name and package version
/// instead of Core's `temporal-rust` default (#816).
#[test]
fn connections_report_ocaml_sdk_identity() {
    let target = temporalio_sdk_core::Url::parse("http://localhost:7233").unwrap();
    let options = super::connection_options(target, "worker@host".to_owned(), None);
    assert_eq!(options.get_client_name(), "temporal-ocaml");
    assert_eq!(options.get_client_version(), super::SDK_VERSION);
    assert_eq!(
        super::SDK_VERSION,
        include_str!("../../../../.release-version")
            .trim()
            .replacen('~', "-", 1)
    );
}
