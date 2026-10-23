//! A shared Core runtime is destroyed exactly once, by its last holder (#832).
//!
//! The shared handle and every attached graph each own one reference to Core.
//! This file holds a single test so no parallel test in the same process can
//! advance the process-local Core-drop counter and satisfy a check by
//! accident. Explicit release waits on the cleanup thread, so every count
//! below is deterministic except the GC-fallback case, which polls.

use std::ptr;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    Result as AbiResult, Runtime, STATUS_OK, SharedRuntime, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_free, ocaml_temporal_core_v4_runtime_new_attached,
    ocaml_temporal_core_v4_shared_runtime_dispose, ocaml_temporal_core_v4_shared_runtime_free,
    ocaml_temporal_core_v4_shared_runtime_new, test_core_runtime_counts,
};

/// Number of Core runtimes whose destructor has returned so far.
fn cores_dropped() -> u64 {
    test_core_runtime_counts().1
}

/// Creates a one-thread shared handle, panicking on failure.
fn new_shared() -> *mut SharedRuntime {
    let mut shared: *mut SharedRuntime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: Both output locations are writable and exclusively owned.
    let status = unsafe { ocaml_temporal_core_v4_shared_runtime_new(1, &mut shared, &mut result) };
    // SAFETY: The result was initialized by the bridge and is freed once.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(status, STATUS_OK);
    shared
}

/// Attaches one graph to a live shared handle, panicking on failure.
fn attach(shared: *const SharedRuntime) -> *mut Runtime {
    let mut runtime: *mut Runtime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: `shared` is live; both outputs are writable.
    let status =
        unsafe { ocaml_temporal_core_v4_runtime_new_attached(shared, &mut runtime, &mut result) };
    // SAFETY: As in `new_shared`.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(status, STATUS_OK);
    runtime
}

/// Releases a graph and waits for its cleanup thread.
fn free_runtime(runtime: &mut *mut Runtime) {
    // SAFETY: The slot holds a live handle used by one thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_runtime_free(runtime) },
        STATUS_OK
    );
}

/// Releases a shared handle and waits for its cleanup thread.
fn free_shared(shared: &mut *mut SharedRuntime) {
    // SAFETY: The slot holds a live handle used by one thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_shared_runtime_free(shared) },
        STATUS_OK
    );
}

/// Core is destroyed by whichever holder releases last, in every order, and
/// the non-blocking GC fallback eventually destroys it too.
#[test]
fn shared_core_is_destroyed_once_by_its_last_holder() {
    // Shared handle released first: the two graphs keep Core alive, and the
    // second graph's release destroys it.
    let before = cores_dropped();
    let mut shared = new_shared();
    let mut first = attach(shared);
    let mut second = attach(shared);
    free_shared(&mut shared);
    assert_eq!(cores_dropped(), before, "Core freed under attached graphs");
    free_runtime(&mut first);
    assert_eq!(
        cores_dropped(),
        before,
        "Core freed under an attached graph"
    );
    free_runtime(&mut second);
    assert_eq!(cores_dropped(), before + 1);

    // Graphs released first (the order OCaml's Runtime.shutdown enforces):
    // the shared handle's release destroys Core before it returns.
    let before = cores_dropped();
    let mut shared = new_shared();
    let mut runtime = attach(shared);
    free_runtime(&mut runtime);
    assert_eq!(cores_dropped(), before);
    free_shared(&mut shared);
    assert_eq!(cores_dropped(), before + 1);

    // The GC fallback hands the last reference to the cleanup thread without
    // waiting; Core is still destroyed exactly once.
    let before = cores_dropped();
    let mut shared = new_shared();
    // SAFETY: The slot holds a live handle used by one thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_shared_runtime_dispose(&mut shared) },
        STATUS_OK
    );
    assert!(shared.is_null());
    let deadline = Instant::now() + Duration::from_secs(5);
    while cores_dropped() != before + 1 {
        assert!(
            Instant::now() < deadline,
            "disposed shared Core was not destroyed"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(cores_dropped(), before + 1, "Core destroyed more than once");
}
