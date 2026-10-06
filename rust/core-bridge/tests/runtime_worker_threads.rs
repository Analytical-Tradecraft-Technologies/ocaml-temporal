//! Runtime construction bounds its Tokio worker pool (#832).
//!
//! Tokio's default multi-thread runtime starts one worker per core, so every
//! client or worker used to cost dozens of idle threads on a large host. The
//! bridge now resolves an explicit count, or a small default, before building
//! Core. These tests read the pool size back from Tokio's own metrics rather
//! than counting OS threads, which would be platform-specific and racy.

use std::ptr;

use ocaml_temporal_core_bridge::{
    DEFAULT_RUNTIME_WORKER_THREADS_CAP, MAX_RUNTIME_WORKER_THREADS, Result as AbiResult, Runtime,
    STATUS_INVALID_ARGUMENT, STATUS_OK, ocaml_temporal_core_v2_result_free,
    ocaml_temporal_core_v2_runtime_free, ocaml_temporal_core_v2_runtime_new,
    ocaml_temporal_core_v2_runtime_new_with_worker_threads, test_runtime_worker_threads,
};

/// Copies the diagnostic text out of `result`, then releases it.
fn take_message(result: &mut AbiResult) -> String {
    let message = if result.error.ptr.is_null() {
        String::new()
    } else {
        // SAFETY: The bridge owns this readable allocation until it is freed.
        let bytes = unsafe { std::slice::from_raw_parts(result.error.ptr, result.error.len) };
        String::from_utf8(bytes.to_vec()).expect("diagnostic is UTF-8")
    };
    // SAFETY: The result was initialized by the bridge and is freed once.
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(result) },
        STATUS_OK
    );
    message
}

/// Creates a runtime with `worker_threads`, reports the Tokio pool size it
/// observed, and releases the handle. A failed construction returns its status
/// and diagnostic and must leave the runtime slot null.
fn worker_count(worker_threads: u32) -> std::result::Result<usize, (i32, String)> {
    let mut runtime: *mut Runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    let status = unsafe {
        ocaml_temporal_core_v2_runtime_new_with_worker_threads(
            worker_threads,
            &mut runtime,
            &mut result,
        )
    };
    let message = take_message(&mut result);
    if status != STATUS_OK {
        assert!(runtime.is_null(), "failed constructor published a handle");
        return Err((status, message));
    }
    // SAFETY: The handle is live and used only by this thread.
    let workers = unsafe { test_runtime_worker_threads(runtime) }.expect("live runtime");
    // SAFETY: The handle is released exactly once through its owning slot.
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
    Ok(workers)
}

/// The default when no count is supplied: the host's parallelism, capped.
fn expected_default() -> usize {
    let cap = usize::try_from(DEFAULT_RUNTIME_WORKER_THREADS_CAP).expect("cap fits usize");
    std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(cap)
}

/// An explicit count reaches Tokio unchanged, including both range ends.
#[test]
fn explicit_worker_thread_count_is_honored() {
    for requested in [1, 2, 3, MAX_RUNTIME_WORKER_THREADS] {
        let expected = usize::try_from(requested).expect("count fits usize");
        assert_eq!(
            worker_count(requested),
            Ok(expected),
            "requested {requested}"
        );
    }
}

/// Zero selects the capped default rather than Tokio's one-per-core default.
#[test]
fn zero_selects_capped_default() {
    let workers = worker_count(0).expect("default runtime");
    assert_eq!(workers, expected_default());
    assert!(workers >= 1);
    assert!(workers <= usize::try_from(DEFAULT_RUNTIME_WORKER_THREADS_CAP).unwrap());
}

/// The legacy constructor keeps working and uses the same capped default.
#[test]
fn legacy_constructor_uses_default() {
    let mut runtime: *mut Runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    let status = unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) };
    let message = take_message(&mut result);
    assert_eq!(status, STATUS_OK, "{message}");
    // SAFETY: The handle is live and used only by this thread.
    let workers = unsafe { test_runtime_worker_threads(runtime) };
    assert_eq!(workers, Some(expected_default()));
    // SAFETY: The handle is released exactly once through its owning slot.
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

/// A count above the bound is a typed invalid argument that names the range
/// and allocates no runtime.
#[test]
fn oversized_worker_thread_count_is_rejected() {
    for requested in [MAX_RUNTIME_WORKER_THREADS + 1, u32::MAX] {
        let (status, message) = worker_count(requested).expect_err("count must be rejected");
        assert_eq!(status, STATUS_INVALID_ARGUMENT, "requested {requested}");
        assert!(
            message.contains(&MAX_RUNTIME_WORKER_THREADS.to_string()),
            "diagnostic names the bound: {message}"
        );
    }
}

/// A null runtime slot is still rejected even when the count is valid, and a
/// null handle reports no worker count.
#[test]
fn null_slot_is_rejected() {
    let mut result = AbiResult::default();
    // SAFETY: The result location is writable; the null slot is the subject.
    let status = unsafe {
        ocaml_temporal_core_v2_runtime_new_with_worker_threads(2, ptr::null_mut(), &mut result)
    };
    take_message(&mut result);
    assert_eq!(status, STATUS_INVALID_ARGUMENT);
    // SAFETY: Null is explicitly accepted by the helper.
    assert_eq!(unsafe { test_runtime_worker_threads(ptr::null()) }, None);
}

/// The C header publishes the same upper bound the bridge enforces, so C and
/// OCaml callers cannot drift from Rust silently.
#[test]
fn header_bound_matches_bridge() {
    let header = include_str!("../include/ocaml_temporal_core.h");
    let definition = format!(
        "#define OCAML_TEMPORAL_CORE_MAX_RUNTIME_WORKER_THREADS UINT32_C({MAX_RUNTIME_WORKER_THREADS})"
    );
    assert!(header.contains(&definition), "missing {definition}");
}
