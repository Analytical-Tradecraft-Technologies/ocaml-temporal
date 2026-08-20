#include "ocaml_temporal_core.h"

#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>
#include <caml/threads.h>

#if defined(_WIN32)
#include <windows.h>
#else
#include <sched.h>
#endif

#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

/* OCaml custom block that is the sole owner of one initialized Rust result.
 * `live` makes explicit free and finalization idempotent with respect to the
 * same OCaml value. Copying this C structure remains forbidden. */
typedef struct owned_response {
  ocaml_temporal_core_result result;
  /* Atomic so explicit free and GC finalization cannot race a double free if
   * the same response value is observed from more than one Domain. */
  atomic_int live;
} owned_response;

/* Owner of the sole native runtime pointer for one SDK instance. Future
 * client and worker state remains subordinate to this owner.
 *
 * [active_calls] is a counted borrow of the pointer. A caller increments it
 * before loading the pointer and decrements it only after Rust has returned.
 * [closing] prevents a new caller from borrowing the pointer once close has
 * begun. The close path sets [closing], detaches the pointer, and waits for
 * the count to reach zero before handing the pointer to Rust for destruction.
 * This gate is needed even though the supervisor normally serializes calls:
 * finalizer and shutdown paths are defensive boundaries and must not turn an
 * accidental cross-Domain call into a use-after-free.
 *
 * This struct is deliberately allocated with `malloc`, never embedded
 * directly in the OCaml custom block. Every caller that borrows the runtime
 * (`invoke_runtime_json`, `invoke_runtime`, `ocaml_temporal_runtime_close`)
 * captures a `owned_runtime *` and keeps dereferencing it across a
 * `caml_enter_blocking_section` / `caml_leave_blocking_section` window during
 * which the OCaml runtime lock is released. A stop-the-world minor GC or
 * major-heap compaction on another Domain can run during that window and
 * relocate any OCaml-managed memory, including a small custom block's
 * payload. Keeping this struct outside the OCaml heap gives every borrower a
 * pointer value that stays valid for as long as the struct itself is alive,
 * regardless of what the GC does to the (separate, movable) custom block that
 * merely records where to find it. See [Runtime_val]. */
typedef struct owned_runtime {
  _Atomic(ocaml_temporal_core_runtime *) runtime;
  atomic_uint active_calls;
  atomic_int closing;
} owned_runtime;

/* Signature shared by lifecycle operations which borrow one strict JSON
 * document for the duration of a synchronous Rust call. */
typedef ocaml_temporal_core_status (*runtime_json_operation)(
    ocaml_temporal_core_runtime *, const uint8_t *, size_t,
    ocaml_temporal_core_result *);

/* Signature shared by lifecycle operations without an input document. */
typedef ocaml_temporal_core_status (*runtime_operation)(
    ocaml_temporal_core_runtime *, ocaml_temporal_core_result *);

/* Extract the custom-block payload; callers must separately require liveness
 * before reading Rust-owned pointer fields. */
static owned_response *Response_val(value response) {
  return (owned_response *)Data_custom_val(response);
}

/* Extract the stable, non-relocatable pointer to the runtime owner from its
 * OCaml custom block. The custom block payload holds only a plain pointer to
 * a `malloc`-allocated [owned_runtime]; that pointer value never changes
 * after [alloc_runtime] stores it, even though the block holding it may move.
 * It is therefore safe to read this once and keep dereferencing the result
 * across a released-lock window (see the [owned_runtime] comment) without
 * re-fetching from [runtime] afterward. A NULL result means allocation of the
 * owner struct failed after the custom block itself was created; callers that
 * can observe this (the finalizer) must treat it as a no-op. */
static owned_runtime *Runtime_val(value runtime) {
  return *(owned_runtime **)Data_custom_val(runtime);
}

/* Yield the OS thread while a close waits for an admitted Rust operation. Both
 * branches use facilities supplied by the platform C runtime: the Windows
 * native job links the system kernel library by default, while Unix-like
 * targets provide [sched_yield] through libc. No third-party synchronization
 * library or additional OPAM dependency is introduced. */
static void runtime_thread_yield(void) {
#if defined(_WIN32)
  (void)SwitchToThread();
#else
  (void)sched_yield();
#endif
}

/* Wait for every operation that was admitted before close to finish. The
 * function touches only C atomics and yields the OS thread; callers that can
 * block must invoke it only from a path which is already outside the OCaml
 * runtime lock. The custom-block finalizer is an exception to that general
 * rule: it is only a defensive ownership barrier, and it must never call an
 * OCaml runtime API. */
static void wait_for_runtime_calls(owned_runtime *owned) {
  while (atomic_load_explicit(&owned->active_calls, memory_order_seq_cst) != 0) {
    runtime_thread_yield();
  }
}

/* Mark the owner closed and remove its pointer from future operations. The
 * caller still owns the returned pointer until [wait_for_runtime_calls] has
 * observed that all earlier borrowers released it. */
static ocaml_temporal_core_runtime *detach_runtime(owned_runtime *owned) {
  atomic_store_explicit(&owned->closing, 1, memory_order_seq_cst);
  return atomic_exchange_explicit(&owned->runtime, NULL, memory_order_seq_cst);
}

/* Try to admit one Rust operation. The increment happens before either the
 * closing check or pointer load, which makes the close wait a complete
 * happens-before barrier for every pointer use. A closed/uninitialized owner
 * still returns a NULL pointer so Rust can produce its normal typed status. */
static int acquire_runtime(owned_runtime *owned,
                           ocaml_temporal_core_runtime **native_runtime) {
  atomic_fetch_add_explicit(&owned->active_calls, 1, memory_order_seq_cst);
  if (atomic_load_explicit(&owned->closing, memory_order_seq_cst) != 0) {
    atomic_fetch_sub_explicit(&owned->active_calls, 1, memory_order_seq_cst);
    *native_runtime = NULL;
    return 0;
  }

  *native_runtime =
      atomic_load_explicit(&owned->runtime, memory_order_seq_cst);
  if (*native_runtime == NULL) {
    atomic_fetch_sub_explicit(&owned->active_calls, 1, memory_order_seq_cst);
    return 0;
  }
  return 1;
}

/* Release an admitted borrow after the native call has completely stopped
 * touching the runtime pointer. The caller must not use [native_runtime]
 * after this decrement. */
static void release_runtime(owned_runtime *owned) {
  atomic_fetch_sub_explicit(&owned->active_calls, 1, memory_order_seq_cst);
}

/* Release Rust allocations exactly once and poison further field access. */
static void release_response(owned_response *response) {
  int expected = 1;
  if (atomic_compare_exchange_strong_explicit(
          &response->live, &expected, 0, memory_order_acq_rel,
          memory_order_acquire)) {
    (void)ocaml_temporal_core_v2_result_free(&response->result);
  }
}

/* GC fallback for responses not explicitly decoded because OCaml unwound. */
static void finalize_response(value response) {
  release_response(Response_val(response));
}

/* GC fallback for a runtime whose supervisor did not complete explicit
 * shutdown. Normal operation closes the owner deterministically first. This
 * is the one path that frees the malloc'd [owned_runtime]: the custom block
 * is collected at most once, so this runs at most once per runtime value. */
static void finalize_runtime(value runtime) {
  owned_runtime *owned = Runtime_val(runtime);
  ocaml_temporal_core_runtime *native_runtime;

  if (owned == NULL) {
    /* alloc_runtime's malloc failed after the custom block was already
     * registered for finalization; there is nothing to detach or free. */
    return;
  }

  native_runtime = detach_runtime(owned);
  /* Custom-block finalizers have a deliberately tiny contract: they may
   * inspect their C payload and release foreign resources, but they must not
   * call OCaml runtime operations such as [caml_enter_blocking_section]. The
   * runtime value is rooted by every admitted C primitive, so an ordinary
   * finalizer cannot overlap an active call; retain the atomic wait as a
   * defensive barrier for unusual shutdown/GC ordering without releasing or
   * reacquiring the OCaml lock here. */
  wait_for_runtime_calls(owned);
  if (native_runtime != NULL) {
    (void)ocaml_temporal_core_v2_runtime_dispose(&native_runtime);
  }
  free(owned);
}

/* Custom operations deliberately use identity/default behavior; responses are
 * resource owners, not serializable or semantically comparable values. */
static struct custom_operations response_operations = {
    .identifier = "org.ocaml-temporal.native-response.v1",
    .finalize = finalize_response,
    .compare = custom_compare_default,
    .hash = custom_hash_default,
    .serialize = custom_serialize_default,
    .deserialize = custom_deserialize_default,
    .compare_ext = custom_compare_ext_default,
    .fixed_length = NULL,
};

/* Runtime owners are identity resources and cannot be serialized or compared
 * by native contents. */
static struct custom_operations runtime_operations = {
    .identifier = "org.ocaml-temporal.native-runtime.v1",
    .finalize = finalize_runtime,
    .compare = custom_compare_default,
    .hash = custom_hash_default,
    .serialize = custom_serialize_default,
    .deserialize = custom_deserialize_default,
    .compare_ext = custom_compare_ext_default,
    .fixed_length = NULL,
};

/* Allocate and initialize the finalizable owner before any Rust call can write
 * allocations into it. Zeroing also satisfies the Rust result precondition. */
static value alloc_response(void) {
  CAMLparam0();
  CAMLlocal1(response);
  owned_response *owned;

  response = caml_alloc_custom(&response_operations, sizeof(owned_response), 0, 1);
  owned = Response_val(response);
  memset(owned, 0, sizeof(*owned));
  atomic_init(&owned->live, 1);
  CAMLreturn(response);
}

/* Allocate a null-initialized runtime owner before entering native code so a
 * later OCaml allocation failure cannot orphan a successfully created handle.
 *
 * The custom block payload stores only a pointer, immediately nulled, to a
 * separately `malloc`-allocated [owned_runtime] (see the [owned_runtime] and
 * [Runtime_val] comments for why this indirection exists). If that `malloc`
 * fails, the custom block is already registered for finalization with a NULL
 * inner pointer, and [finalize_runtime] treats NULL as a no-op. */
static value alloc_runtime(void) {
  CAMLparam0();
  CAMLlocal1(runtime);
  owned_runtime *owned;

  runtime = caml_alloc_custom(&runtime_operations, sizeof(owned_runtime *),
                              sizeof(owned_runtime), 1);
  *(owned_runtime **)Data_custom_val(runtime) = NULL;

  owned = malloc(sizeof(owned_runtime));
  if (owned == NULL) {
    caml_raise_out_of_memory();
  }
  atomic_init(&owned->runtime, NULL);
  atomic_init(&owned->active_calls, 0);
  atomic_init(&owned->closing, 0);
  *(owned_runtime **)Data_custom_val(runtime) = owned;

  CAMLreturn(runtime);
}

/* Invoke a blocking graph operation after copying mutable OCaml bytes. The
 * counted borrow keeps the native pointer alive until Rust returns, while the
 * owner gate makes a concurrent close produce a typed null-runtime error
 * instead of allowing a use-after-free. [owned] is fetched once, before the
 * blocking section: because it is the stable malloc'd pointer described on
 * [owned_runtime], it stays valid across [caml_enter_blocking_section] /
 * [caml_leave_blocking_section] with no re-fetch needed, unlike an interior
 * pointer into a movable OCaml block. */
static value invoke_runtime_json(value runtime, value input,
                                 runtime_json_operation operation) {
  CAMLparam2(runtime, input);
  CAMLlocal1(response);
  owned_runtime *owned = Runtime_val(runtime);
  ocaml_temporal_core_runtime *native_runtime = NULL;
  size_t input_length = caml_string_length(input);
  uint8_t *input_copy = NULL;
  ocaml_temporal_core_result native_result = {0};
  int admitted;

  response = alloc_response();
  if (input_length > 0) {
    input_copy = malloc(input_length);
    if (input_copy == NULL) {
      caml_raise_out_of_memory();
    }
    memcpy(input_copy, Bytes_val(input), input_length);
  }

  admitted = acquire_runtime(owned, &native_runtime);
  caml_enter_blocking_section();
  (void)operation(native_runtime, input_copy, input_length, &native_result);
  if (admitted) {
    /* Release the C borrow while the OCaml lock is still released. A
     * defensive finalizer can then observe the completed native call without
     * waiting for this thread to reacquire the lock merely to decrement an
     * atomic counter. */
    release_runtime(owned);
  }
  caml_leave_blocking_section();
  free(input_copy);
  Response_val(response)->result = native_result;
  CAMLreturn(response);
}

/* Invoke a blocking graph operation which needs no borrowed OCaml input. See
 * [invoke_runtime_json] for why [owned] can be fetched once, before the
 * blocking section, and safely reused after it. */
static value invoke_runtime(value runtime, runtime_operation operation) {
  CAMLparam1(runtime);
  CAMLlocal1(response);
  owned_runtime *owned = Runtime_val(runtime);
  ocaml_temporal_core_runtime *native_runtime = NULL;
  ocaml_temporal_core_result native_result = {0};
  int admitted;

  response = alloc_response();
  admitted = acquire_runtime(owned, &native_runtime);
  caml_enter_blocking_section();
  (void)operation(native_runtime, &native_result);
  if (admitted) {
    /* See [invoke_runtime_json]: the borrow is independent of the OCaml
     * runtime lock and is released before lock reacquisition. */
    release_runtime(owned);
  }
  caml_leave_blocking_section();
  Response_val(response)->result = native_result;
  CAMLreturn(response);
}

/* Reject use-after-free deterministically at the private binding boundary. */
static owned_response *require_live(value response) {
  owned_response *owned = Response_val(response);
  if (atomic_load_explicit(&owned->live, memory_order_acquire) == 0) {
    caml_invalid_argument("Temporal native response has already been freed");
  }
  return owned;
}

/* Copy one Rust-owned byte span without passing its canonical null pointer to
 * the OCaml initialized-string primitive.  The Rust ABI deliberately uses
 * `{ NULL, 0 }` for an empty allocation; allocating an empty OCaml string
 * directly avoids making a zero-length `memcpy` depend on whether a particular
 * OCaml runtime build tolerates a null source pointer.  A nonempty null span
 * is an ABI defect, so fail before dereferencing it rather than crashing. */
static value copy_owned_buffer(const ocaml_temporal_core_buffer *buffer) {
  if (buffer->len == 0) {
    return caml_alloc_string(0);
  }
  if (buffer->ptr == NULL) {
    caml_invalid_argument("Temporal native response has a null nonempty buffer");
  }
  return caml_alloc_initialized_string((mlsize_t)buffer->len,
                                        (const char *)buffer->ptr);
}

/* Negotiate ABI compatibility without blocking; the returned custom block owns
 * any diagnostic allocated by Rust. */
CAMLprim value ocaml_temporal_check_abi_version(value requested_version) {
  CAMLparam1(requested_version);
  CAMLlocal1(response);
  owned_response *owned;

  response = alloc_response();
  owned = Response_val(response);
  (void)ocaml_temporal_core_v2_check_abi_version(
      (uint32_t)Int32_val(requested_version), &owned->result);
  CAMLreturn(response);
}

/* Copy mutable OCaml bytes before releasing the runtime lock. Rust never holds
 * an OCaml heap pointer while another Domain or the GC can run. The response
 * custom block is young and movable: re-fetch [Response_val] only after the
 * blocking section, never store that interior pointer across it. */
CAMLprim value ocaml_temporal_echo(value input) {
  CAMLparam1(input);
  CAMLlocal1(response);
  size_t input_length = caml_string_length(input);
  uint8_t *input_copy = NULL;
  ocaml_temporal_core_result native_result = {0};

  /* Allocate the finalizable owner before acquiring any unmanaged resource. */
  response = alloc_response();

  if (input_length > 0) {
    input_copy = malloc(input_length);
    if (input_copy == NULL) {
      caml_raise_out_of_memory();
    }
    memcpy(input_copy, Bytes_val(input), input_length);
  }

  caml_enter_blocking_section();
  (void)ocaml_temporal_core_v2_echo(input_copy, input_length, &native_result);
  caml_leave_blocking_section();
  free(input_copy);
  /* Re-resolve after reacquiring the runtime lock: a concurrent minor GC may
   * have moved the custom block while the lock was released. */
  Response_val(response)->result = native_result;

  CAMLreturn(response);
}

/* Exercise a blocking Rust call with the OCaml runtime lock released. The
 * sentinel UINT32_MAX preserves invalid negative/overflow input for Rust-side
 * validation instead of truncating it into a valid duration. Same young-block
 * rule as [ocaml_temporal_echo]: no interior pointer across the lock release. */
CAMLprim value ocaml_temporal_conformance_wait_ms(value milliseconds) {
  CAMLparam1(milliseconds);
  CAMLlocal1(response);
  intnat requested = Long_val(milliseconds);
  uint32_t bounded_request;
  ocaml_temporal_core_result native_result = {0};

  bounded_request =
      requested < 0 || (uintnat)requested > UINT32_MAX ? UINT32_MAX
                                                       : (uint32_t)requested;

  response = alloc_response();
  caml_enter_blocking_section();
  (void)ocaml_temporal_core_v2_conformance_wait_ms(bounded_request,
                                                   &native_result);
  caml_leave_blocking_section();
  Response_val(response)->result = native_result;
  CAMLreturn(response);
}

/* Create Core/Tokio while the OCaml runtime lock is released. Rust writes only
 * C-stack storage during the blocking section; the opaque pointer and result
 * are copied into rooted custom blocks after reacquiring the lock. */
CAMLprim value ocaml_temporal_runtime_create(value unit) {
  CAMLparam1(unit);
  CAMLlocal3(runtime, response, pair);
  ocaml_temporal_core_runtime *native_runtime = NULL;
  ocaml_temporal_core_result native_result = {0};
  owned_runtime *runtime_owner;
  owned_response *response_owner;

  runtime = alloc_runtime();
  response = alloc_response();

  caml_enter_blocking_section();
  (void)ocaml_temporal_core_v2_runtime_new(&native_runtime, &native_result);
  caml_leave_blocking_section();

  runtime_owner = Runtime_val(runtime);
  atomic_store_explicit(&runtime_owner->runtime, native_runtime,
                        memory_order_release);
  response_owner = Response_val(response);
  response_owner->result = native_result;

  pair = caml_alloc_tuple(2);
  Store_field(pair, 0, runtime);
  Store_field(pair, 1, response);
  CAMLreturn(pair);
}

/* Connect the official client using a strict JSON configuration while the
 * OCaml runtime lock is available to other Domains. */
CAMLprim value ocaml_temporal_client_connect(value runtime, value input) {
  return invoke_runtime_json(runtime, input,
                             ocaml_temporal_core_v2_client_connect_json);
}

/* Start one dynamically named workflow through the connected native client.
 * The JSON input is copied before the runtime lock is released; Rust owns no
 * OCaml memory after this function returns. */
CAMLprim value ocaml_temporal_client_start_workflow_json(value runtime,
                                                         value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_start_workflow_json);
}

/* Request cancellation of one exact workflow run. The JSON input is copied
 * before the OCaml runtime lock is released; Rust owns no OCaml memory after
 * this function returns and the response custom block owns the native result
 * until the normal decoder frees it. */
CAMLprim value ocaml_temporal_client_cancel_workflow_json(value runtime,
                                                          value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_cancel_workflow_json);
}

/* Reset one exact workflow run. The JSON input is copied before the OCaml
 * runtime lock is released; Rust owns no OCaml memory after this call. */
CAMLprim value ocaml_temporal_client_reset_workflow_json(value runtime,
                                                          value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_reset_workflow_json);
}

/* Terminate one exact workflow run. The input is copied before releasing the
 * OCaml runtime lock and the acknowledgement is owned by the bridge result. */
CAMLprim value ocaml_temporal_client_terminate_workflow_json(value runtime,
                                                             value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_terminate_workflow_json);
}

/* Send one signal to one exact workflow run. The JSON input is copied before
 * the OCaml runtime lock is released; Rust owns no OCaml memory after this
 * call and the positive acknowledgement is decoded like cancellation. */
CAMLprim value ocaml_temporal_client_signal_workflow_json(value runtime,
                                                          value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_signal_workflow_json);
}

/* List one bounded visibility page. The input is copied before the OCaml
 * runtime lock is released and the returned JSON is copied back by invoke. */
CAMLprim value ocaml_temporal_client_list_visibility_json(value runtime,
                                                          value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_list_visibility_json);
}

/* Execute one output-only query against one exact workflow run. The JSON
 * input is copied before the OCaml runtime lock is released; Rust owns the
 * Temporal protobuf request and returns only an owned JSON response. */
CAMLprim value ocaml_temporal_client_query_workflow_json(value runtime,
                                                         value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_query_workflow_json);
}

/* Admit one workflow update. The shared invoke helper copies the JSON input,
 * releases the OCaml runtime lock while Rust performs the bounded RPC, and
 * returns an owned response buffer which the normal custom block finalizer
 * releases exactly once. */
CAMLprim value ocaml_temporal_client_update_workflow_json(value runtime,
                                                         value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_update_workflow_json);
}

/* Poll one admitted workflow update. Pending and terminal outcomes use the
 * same response ownership contract as every other client bridge operation. */
CAMLprim value ocaml_temporal_client_poll_update_workflow_json(value runtime,
                                                               value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_poll_update_workflow_json);
}

/* Admit one workflow start and return its opaque ticket. The input is copied
 * before releasing the OCaml runtime lock; Rust owns the pending Tokio task
 * and never retains OCaml memory from this call. */
CAMLprim value ocaml_temporal_client_begin_start_workflow_json(value runtime,
                                                               value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_begin_start_workflow_json);
}

/* Poll one asynchronous start ticket without blocking. STATUS_NOT_READY is
 * returned through the normal response value so the OCaml supervisor can
 * service other mailbox messages before polling again. */
CAMLprim value ocaml_temporal_client_poll_start_workflow_json(value runtime,
                                                              value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_poll_start_workflow_json);
}

/* Wait one bounded interval for an asynchronous start ticket. The shared
 * invoke helper releases the OCaml runtime lock around Rust's wait. */
CAMLprim value ocaml_temporal_client_wait_start_workflow_json(value runtime,
                                                              value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_wait_start_workflow_json);
}

/* Wait for one exact run while Rust performs the long poll outside the OCaml
 * runtime lock. The response custom block owns the result until [decode]. */
CAMLprim value ocaml_temporal_client_wait_workflow_json(value runtime,
                                                        value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_client_wait_workflow_json);
}

/* Complete an activity that has already returned WillCompleteAsync. Rust
 * routes this JSON through the namespace-bound client, not the worker ledger. */
CAMLprim value
ocaml_temporal_client_complete_async_activity_json(value runtime, value input) {
  return invoke_runtime_json(
      runtime, input,
      ocaml_temporal_core_v2_client_complete_async_activity_json);
}

/* Record progress for an admitted asynchronous activity through the same
 * namespace-bound client path. */
CAMLprim value ocaml_temporal_client_record_async_activity_heartbeat_json(
    value runtime, value input) {
  return invoke_runtime_json(
      runtime, input,
      ocaml_temporal_core_v2_client_record_async_activity_heartbeat_json);
}

/* Construct and validate the workflow-only Core worker. Network validation
 * and any failure cleanup happen with the OCaml runtime lock released. */
CAMLprim value ocaml_temporal_worker_start(value runtime, value input) {
  return invoke_runtime_json(runtime, input,
                             ocaml_temporal_core_v2_worker_start_json);
}

/* Construct the workflow-only replay worker from strict settings. Replay
 * history input is copied by the common helper before the OCaml runtime lock
 * is released; Rust owns the Core graph after this call returns. */
CAMLprim value ocaml_temporal_replay_worker_start(value runtime, value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_replay_worker_start_json);
}

/* Feed one replay-history document while Rust drives the bounded feeder on its
 * Core runtime. The helper releases the OCaml runtime lock for backpressure. */
CAMLprim value ocaml_temporal_replay_worker_feed_history(value runtime,
                                                         value input) {
  return invoke_runtime_json(
      runtime, input,
      ocaml_temporal_core_v2_replay_worker_feed_history_json);
}

/* Close replay input; Core will publish terminal shutdown after all admitted
 * histories and their workflow completions have been consumed. */
CAMLprim value ocaml_temporal_replay_worker_finish_input(value runtime) {
  return invoke_runtime(
      runtime, ocaml_temporal_core_v2_replay_worker_finish_input);
}

/* Drain one replay activation without waiting on Core. */
CAMLprim value ocaml_temporal_replay_worker_try_poll_workflow(value runtime) {
  return invoke_runtime(
      runtime, ocaml_temporal_core_v2_replay_worker_try_poll_workflow);
}

/* Wait for replay readiness with the OCaml runtime lock released. */
CAMLprim value ocaml_temporal_replay_worker_wait_workflow(value runtime) {
  return invoke_runtime(
      runtime, ocaml_temporal_core_v2_replay_worker_wait_workflow);
}

/* Submit one replay workflow completion after copying the JSON input. */
CAMLprim value ocaml_temporal_replay_worker_complete_workflow(value runtime,
                                                              value input) {
  return invoke_runtime_json(
      runtime, input,
      ocaml_temporal_core_v2_replay_worker_complete_workflow_json);
}

/* Retire one replay activation after OCaml semantic decode failure. */
CAMLprim value ocaml_temporal_replay_worker_reject_workflow(value runtime,
                                                            value input) {
  return invoke_runtime_json(
      runtime, input,
      ocaml_temporal_core_v2_replay_worker_reject_workflow_json);
}

/* Finalize a naturally drained replay while Rust retains ownership on error. */
CAMLprim value ocaml_temporal_replay_worker_finalize(value runtime) {
  return invoke_runtime(runtime, ocaml_temporal_core_v2_replay_worker_finalize);
}

/* Explicitly dispose replay and force-complete native debts. */
CAMLprim value ocaml_temporal_replay_worker_dispose(value runtime) {
  return invoke_runtime(runtime, ocaml_temporal_core_v2_replay_worker_dispose);
}

/* Drain one ready workflow activation without waiting on Core. The Rust
 * operation only touches the owner Domain's ready queue; it never invokes
 * OCaml from a Tokio thread. `NOT_READY` is carried in the normal response so
 * the OCaml scheduler can decide how to yield or wait for native readiness. */
CAMLprim value ocaml_temporal_worker_try_poll_workflow(value runtime) {
  return invoke_runtime(runtime,
                        ocaml_temporal_core_v2_worker_try_poll_workflow);
}

/* Wait for native workflow readiness with the OCaml runtime lock released. The
 * Rust wait is bounded so this call cannot strand the supervisor mailbox while
 * a separate shutdown request is waiting to be handled. A successful wait only
 * signals readiness; the owner must still call the non-blocking drain above. */
CAMLprim value ocaml_temporal_worker_wait_workflow(value runtime) {
  return invoke_runtime(runtime,
                        ocaml_temporal_core_v2_worker_wait_workflow);
}

/* Copy a semantic workflow completion before releasing the OCaml runtime lock.
 * Rust validates the document and checks its run-id lease; the bridge never
 * retains the temporary C copy after the call returns. */
CAMLprim value ocaml_temporal_worker_complete_workflow_json(value runtime,
                                                            value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_worker_complete_workflow_json);
}

/* Return the exact Rust-produced activation document when the OCaml decoder
 * cannot represent it. Rust reparses and matches the retained activation
 * before failing Core and retiring the one-shot lease. */
CAMLprim value ocaml_temporal_worker_reject_workflow_json(value runtime,
                                                          value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_worker_reject_workflow_json);
}

/* Drain one ready remote activity task without waiting. Workflow and activity
 * lanes are independent in Rust, so an empty activity lane does not delay a
 * ready workflow activation. */
CAMLprim value ocaml_temporal_worker_try_poll_activity(value runtime) {
  return invoke_runtime(runtime, ocaml_temporal_core_v2_worker_try_poll_activity);
}

/* Wait for native remote-activity readiness under the same released-lock and
 * bounded-time contract as workflow readiness. */
CAMLprim value ocaml_temporal_worker_wait_activity(value runtime) {
  return invoke_runtime(runtime,
                        ocaml_temporal_core_v2_worker_wait_activity);
}

/* Apply the fixed activity-completion retry delay on the Rust supervisor
 * Domain. The shared invoke helper releases the OCaml runtime lock while the
 * bounded timer runs, so this cannot block an OCaml workflow scheduler. */
CAMLprim value
ocaml_temporal_worker_wait_activity_completion_retry_backoff(value runtime) {
  return invoke_runtime(
      runtime,
      ocaml_temporal_core_v2_worker_wait_activity_completion_retry_backoff);
}

/* Copy a semantic activity completion before entering the blocking section.
 * The opaque task token remains Rust-owned state in the ledger; this input is
 * only a borrowed JSON document for one synchronous submission. */
CAMLprim value ocaml_temporal_worker_complete_activity_json(value runtime,
                                                            value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_worker_complete_activity_json);
}

/* Submit one heartbeat without exposing the native task token to OCaml. */
CAMLprim value
ocaml_temporal_worker_record_activity_heartbeat_json(value runtime,
                                                     value input) {
  return invoke_runtime_json(
      runtime, input,
      ocaml_temporal_core_v2_worker_record_activity_heartbeat_json);
}

/* Return the original Rust-produced activity document after OCaml decode
 * failure. Rust matches the retained task before the native ledger retires
 * its exact opaque token. */
CAMLprim value ocaml_temporal_worker_reject_activity_json(value runtime,
                                                          value input) {
  return invoke_runtime_json(
      runtime, input, ocaml_temporal_core_v2_worker_reject_activity_json);
}

/* Gracefully stop the worker; Rust treats an absent worker as already closed. */
CAMLprim value ocaml_temporal_worker_shutdown(value runtime) {
  return invoke_runtime(runtime, ocaml_temporal_core_v2_worker_shutdown);
}

/* Drop the client only after the worker child is absent. */
CAMLprim value ocaml_temporal_client_disconnect(value runtime) {
  return invoke_runtime(runtime, ocaml_temporal_core_v2_client_disconnect);
}

/* Close the owner in three ordered phases: reject new borrows, detach the
 * pointer, then wait outside the OCaml runtime lock for admitted operations to
 * finish before Rust frees Core/Tokio. A second close observes a null pointer
 * and remains idempotent.
 *
 * [wait_for_runtime_calls] polls [owned] for the entire duration of the
 * blocking section below, which can be arbitrarily long. This is exactly why
 * [owned_runtime] must be a stable, `malloc`-allocated struct rather than
 * living inside the movable custom block: a GC move on another Domain during
 * this wait must not invalidate the pointer this function is actively
 * dereferencing. */
CAMLprim value ocaml_temporal_runtime_close(value runtime) {
  CAMLparam1(runtime);
  owned_runtime *owned = Runtime_val(runtime);
  ocaml_temporal_core_runtime *native_runtime = detach_runtime(owned);
  ocaml_temporal_core_status status;

  caml_enter_blocking_section();
  wait_for_runtime_calls(owned);
  status = ocaml_temporal_core_v2_runtime_free(&native_runtime);
  caml_leave_blocking_section();
  CAMLreturn(Val_int(status));
}

/* Read the status while the custom block remains live. */
CAMLprim value ocaml_temporal_response_status(value response) {
  CAMLparam1(response);
  owned_response *owned = require_live(response);
  CAMLreturn(Val_int(owned->result.status));
}

/* Copy successful Rust bytes into GC-owned OCaml storage before cleanup. */
CAMLprim value ocaml_temporal_response_value(value response) {
  CAMLparam1(response);
  owned_response *owned = require_live(response);

  if (owned->result.status != OCAML_TEMPORAL_CORE_STATUS_OK) {
    caml_invalid_argument("Temporal native response does not contain a value");
  }
  CAMLreturn(copy_owned_buffer(&owned->result.value));
}

/* Copy failure diagnostics into GC-owned OCaml storage before cleanup. */
CAMLprim value ocaml_temporal_response_error(value response) {
  CAMLparam1(response);
  owned_response *owned = require_live(response);

  if (owned->result.status == OCAML_TEMPORAL_CORE_STATUS_OK) {
    caml_invalid_argument("Temporal native response does not contain an error");
  }
  CAMLreturn(copy_owned_buffer(&owned->result.error));
}

/* Explicit cleanup used by [Fun.protect]; finalization remains a fallback. */
CAMLprim value ocaml_temporal_response_free(value response) {
  CAMLparam1(response);
  release_response(Response_val(response));
  CAMLreturn(Val_unit);
}
