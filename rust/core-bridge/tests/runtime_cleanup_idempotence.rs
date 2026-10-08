use std::ptr;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    Result as AbiResult, STATUS_OK, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_dispose, ocaml_temporal_core_v4_runtime_new,
    test_runtime_cleanup_counts,
};

/// Proves that repeated disposal consumes one opaque runtime exactly once.
///
/// The first call atomically takes ownership of the native graph and clears
/// the caller's pointer. A later call sees a null pointer and is a no-op. The
/// isolated integration-test process makes the process-local cleanup counters
/// unambiguous, while the bounded wait observes the asynchronous destructor
/// completing without requiring the caller to sleep indefinitely.
#[test]
fn repeated_dispose_cleans_one_runtime_once() {
    let (created_before, cleaned_before) = test_runtime_cleanup_counts();
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();

    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_dispose(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());

    // A second disposal must not enqueue another cleanup job or dereference a
    // graph that the first call already transferred to the cleanup thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_dispose(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (created, cleaned) = test_runtime_cleanup_counts();
        if created == created_before + 1 && cleaned == cleaned_before + 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "repeated disposal did not complete exactly one cleanup: created {created_before}->{created}, cleaned {cleaned_before}->{cleaned}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
