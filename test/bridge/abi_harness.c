#include "ocaml_temporal_core.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

static void assert_empty(ocaml_temporal_core_buffer buffer) {
  assert(buffer.ptr == NULL);
  assert(buffer.len == 0);
}

int main(void) {
  ocaml_temporal_core_result result = {0};
  ocaml_temporal_core_runtime *runtime = NULL;

  assert(ocaml_temporal_core_v2_check_abi_version(
             OCAML_TEMPORAL_CORE_ABI_VERSION, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(result.status == OCAML_TEMPORAL_CORE_STATUS_OK);
  assert_empty(result.value);
  assert_empty(result.error);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  /* Every operation negotiated as ABI v2 must use the v2 symbol namespace.
   * A null runtime exercises the visibility entry point without a server and
   * still proves that the canonical header and linked archive agree. */
  const uint8_t visibility_request[] =
      "{\"namespace\":\"default\",\"query\":\"\",\"page_size\":10,"
      "\"next_page_token\":null}";
  assert(ocaml_temporal_core_v2_client_list_visibility_json(
             NULL, visibility_request, sizeof(visibility_request) - 1,
             &result) == OCAML_TEMPORAL_CORE_STATUS_INVALID_ARGUMENT);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v2_check_abi_version(
             OCAML_TEMPORAL_CORE_ABI_VERSION - 1, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_ABI_MISMATCH);
  assert(result.status == OCAML_TEMPORAL_CORE_STATUS_ABI_MISMATCH);
  assert_empty(result.value);
  assert(result.error.ptr != NULL);
  assert(result.error.len > 0);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v2_check_abi_version(
             OCAML_TEMPORAL_CORE_ABI_VERSION + 1, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_ABI_MISMATCH);
  assert(result.status == OCAML_TEMPORAL_CORE_STATUS_ABI_MISMATCH);
  assert_empty(result.value);
  assert(result.error.ptr != NULL);
  assert(result.error.len > 0);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  const uint8_t input[] = {0x00, 0x7f, 0x80, 0xff};
  assert(ocaml_temporal_core_v2_echo(input, sizeof(input), &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(result.value.len == sizeof(input));
  assert(memcmp(result.value.ptr, input, sizeof(input)) == 0);
  assert_empty(result.error);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert_empty(result.value);
  assert_empty(result.error);

  assert(ocaml_temporal_core_v2_echo(NULL, 0, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert_empty(result.value);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v2_echo(NULL, 1, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_ARGUMENT);
  assert(result.error.ptr != NULL);
  assert(result.error.len > 0);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v2_conformance_wait_ms(0, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v2_runtime_new(&runtime, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(runtime != NULL);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  /* Poll, readiness, and completion symbols are safe to call before a worker
   * exists. The status is explicit and every result still follows the ordinary
   * ownership contract, which lets the OCaml wrapper exercise the same cleanup
   * path for expected lifecycle errors and successful JSON documents. */
  assert(ocaml_temporal_core_v2_worker_try_poll_workflow(runtime, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_STATE);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_worker_wait_workflow(runtime, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_STATE);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_worker_try_poll_activity(runtime, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_STATE);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_worker_wait_activity(runtime, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_STATE);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  const uint8_t malformed_completion[] = "{}";
  assert(ocaml_temporal_core_v2_worker_complete_workflow_json(
             runtime, malformed_completion, sizeof(malformed_completion) - 1,
             &result) == OCAML_TEMPORAL_CORE_STATUS_PROTOCOL);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_worker_complete_activity_json(
             runtime, malformed_completion, sizeof(malformed_completion) - 1,
             &result) == OCAML_TEMPORAL_CORE_STATUS_PROTOCOL);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_worker_reject_workflow_json(
             runtime, malformed_completion, sizeof(malformed_completion) - 1,
             &result) == OCAML_TEMPORAL_CORE_STATUS_PROTOCOL);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_worker_reject_activity_json(
             runtime, malformed_completion, sizeof(malformed_completion) - 1,
             &result) == OCAML_TEMPORAL_CORE_STATUS_PROTOCOL);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  /* Worker construction without its client parent is rejected without
   * mutating the runtime graph or retaining a partial worker. */
  const uint8_t worker_config[] =
      "{\"namespace\":\"temporal-sdk-test\",\"task_queue\":\"abi-test\","
      "\"build_id\":\"abi-test\",\"versioning\":{\"kind\":\"none\"},\"max_cached_workflows\":100,"
      "\"max_outstanding_workflow_tasks\":100,"
      "\"max_concurrent_workflow_task_polls\":5,"
      "\"graceful_shutdown_timeout_ms\":1000}";
  assert(ocaml_temporal_core_v2_worker_start_json(
             runtime, worker_config, sizeof(worker_config) - 1, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_STATE);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  /* Absent-child shutdown is deliberately idempotent. This property lets
   * explicit teardown and defensive parent cleanup share one safe contract. */
  assert(ocaml_temporal_core_v2_worker_shutdown(runtime, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_client_disconnect(runtime, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_runtime_free(&runtime) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v2_runtime_new(&runtime, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_runtime_dispose(&runtime) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(runtime == NULL);
  assert(runtime == NULL);
  assert(ocaml_temporal_core_v2_runtime_free(&runtime) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v2_check_abi_version(
             OCAML_TEMPORAL_CORE_ABI_VERSION, NULL) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_ARGUMENT);
  assert(ocaml_temporal_core_v2_result_free(NULL) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_ARGUMENT);
  assert(ocaml_temporal_core_v2_runtime_new(NULL, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_ARGUMENT);
  assert(ocaml_temporal_core_v2_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v2_runtime_free(NULL) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_ARGUMENT);

  return 0;
}
