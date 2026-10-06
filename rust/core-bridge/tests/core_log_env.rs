//! Runtime construction honors `OCAML_TEMPORAL_CORE_LOG` (#833).
//!
//! This file deliberately contains a single test: it mutates the process
//! environment, which is only sound while no other thread in this test binary
//! reads or writes environment variables.

use std::ptr;

use ocaml_temporal_core_bridge::diagnostics::CORE_LOG_ENV;
use ocaml_temporal_core_bridge::{
    Result as AbiResult, STATUS_CONFIGURATION, STATUS_OK, ocaml_temporal_core_v3_result_free,
    ocaml_temporal_core_v3_runtime_free, ocaml_temporal_core_v3_runtime_new,
};

/// Attempts runtime construction, releases every resource it produced, and
/// returns the status together with the copied diagnostic text.
fn create_runtime() -> (i32, String) {
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    let status = unsafe { ocaml_temporal_core_v3_runtime_new(&mut runtime, &mut result) };
    let message = if result.error.ptr.is_null() {
        String::new()
    } else {
        // SAFETY: The bridge owns this readable allocation until it is freed.
        let bytes = unsafe { std::slice::from_raw_parts(result.error.ptr, result.error.len) };
        String::from_utf8(bytes.to_vec()).expect("diagnostic is UTF-8")
    };
    // SAFETY: The result is released exactly once, and a failed constructor
    // leaves a null runtime slot that free accepts as already released.
    unsafe {
        assert_eq!(ocaml_temporal_core_v3_result_free(&mut result), STATUS_OK);
        if status == STATUS_OK {
            assert_eq!(ocaml_temporal_core_v3_runtime_free(&mut runtime), STATUS_OK);
        } else {
            assert!(runtime.is_null());
        }
    }
    (status, message)
}

/// Sets the Core log variable for the following runtime construction.
fn set_level(value: &str) {
    // SAFETY: This binary has exactly one test, so no other thread reads or
    // writes the environment concurrently.
    unsafe { std::env::set_var(CORE_LOG_ENV, value) };
}

/// Every accepted spelling constructs a runtime; an invalid value is a typed
/// configuration failure that names the variable but not the rejected text,
/// and publishes no runtime handle.
#[test]
fn runtime_respects_core_log_environment() {
    for value in ["off", "error", "WARN", "info", "debug", "trace", ""] {
        set_level(value);
        let (status, message) = create_runtime();
        assert_eq!(status, STATUS_OK, "{value:?}: {message}");
    }

    set_level("loud-secret");
    let (status, message) = create_runtime();
    assert_eq!(status, STATUS_CONFIGURATION, "{message}");
    assert!(message.contains(CORE_LOG_ENV), "{message}");
    assert!(!message.contains("loud-secret"), "{message}");

    // SAFETY: See `set_level`; the binary has no concurrent environment users.
    unsafe { std::env::remove_var(CORE_LOG_ENV) };
    let (status, message) = create_runtime();
    assert_eq!(status, STATUS_OK, "{message}");
}
