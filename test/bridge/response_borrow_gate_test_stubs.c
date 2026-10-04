#include "response_borrow_gate.h"

#include <caml/memory.h>
#include <caml/mlvalues.h>
#include <caml/threads.h>

#if defined(_WIN32)
#include <windows.h>
#else
#include <sched.h>
#endif

/* This gate is private to the test executable. The flags coordinate OCaml
 * Domains around an admitted native read without adding a production hook. */
static response_borrow_gate gate;
static atomic_int reader_entered;
static atomic_int release_reader;
static atomic_int reader_done;
static atomic_int close_done;
static atomic_uint close_yields;

/* Yield only to the OS scheduler while the OCaml runtime lock is released. */
static void test_thread_yield(void) {
#if defined(_WIN32)
  (void)SwitchToThread();
#else
  (void)sched_yield();
#endif
}

/* Count close-loop iterations separately from the reader's wait loop. */
static void test_close_yield(void) {
  atomic_fetch_add_explicit(&close_yields, 1, memory_order_seq_cst);
  test_thread_yield();
}

/* Reset test state only when no reader or closer Domain is running. */
CAMLprim value ocaml_temporal_test_response_gate_reset(value unit) {
  CAMLparam1(unit);
  response_borrow_gate_init(&gate);
  atomic_store_explicit(&reader_entered, 0, memory_order_seq_cst);
  atomic_store_explicit(&release_reader, 0, memory_order_seq_cst);
  atomic_store_explicit(&reader_done, 0, memory_order_seq_cst);
  atomic_store_explicit(&close_done, 0, memory_order_seq_cst);
  atomic_store_explicit(&close_yields, 0, memory_order_seq_cst);
  CAMLreturn(Val_unit);
}

/* Hold an admitted native read until another Domain allows its release. */
CAMLprim value ocaml_temporal_test_response_gate_hold_read(value unit) {
  CAMLparam1(unit);
  if (!response_borrow_gate_acquire(&gate)) {
    atomic_store_explicit(&reader_done, 1, memory_order_seq_cst);
    CAMLreturn(Val_false);
  }
  atomic_store_explicit(&reader_entered, 1, memory_order_seq_cst);
  caml_enter_blocking_section();
  while (atomic_load_explicit(&release_reader, memory_order_seq_cst) == 0) {
    test_thread_yield();
  }
  response_borrow_gate_release(&gate);
  atomic_store_explicit(&reader_done, 1, memory_order_seq_cst);
  caml_leave_blocking_section();
  CAMLreturn(Val_true);
}

/* Close the same gate and report whether this caller owns the release. */
CAMLprim value ocaml_temporal_test_response_gate_close(value unit) {
  CAMLparam1(unit);
  caml_enter_blocking_section();
  int closed = response_borrow_gate_close_and_wait(&gate, test_close_yield);
  atomic_store_explicit(&close_done, 1, memory_order_seq_cst);
  caml_leave_blocking_section();
  CAMLreturn(Val_bool(closed));
}

/* Allow the held reader to finish without touching the borrowed gate. */
CAMLprim value ocaml_temporal_test_response_gate_release_read(value unit) {
  CAMLparam1(unit);
  atomic_store_explicit(&release_reader, 1, memory_order_seq_cst);
  CAMLreturn(Val_unit);
}

/* Return entered, closed, reader-done, close-done, and waiting bits. */
CAMLprim value ocaml_temporal_test_response_gate_state(value unit) {
  CAMLparam1(unit);
  int state = 0;
  if (atomic_load_explicit(&reader_entered, memory_order_seq_cst) != 0)
    state |= 1;
  if (atomic_load_explicit(&gate.live, memory_order_seq_cst) == 0)
    state |= 2;
  if (atomic_load_explicit(&reader_done, memory_order_seq_cst) != 0)
    state |= 4;
  if (atomic_load_explicit(&close_done, memory_order_seq_cst) != 0)
    state |= 8;
  if (atomic_load_explicit(&close_yields, memory_order_seq_cst) >= 2)
    state |= 16;
  CAMLreturn(Val_int(state));
}

/* A new read after close must fail; release any unexpected admission. */
CAMLprim value ocaml_temporal_test_response_gate_try_read(value unit) {
  CAMLparam1(unit);
  int admitted = response_borrow_gate_acquire(&gate);
  if (admitted)
    response_borrow_gate_release(&gate);
  CAMLreturn(Val_bool(admitted));
}
