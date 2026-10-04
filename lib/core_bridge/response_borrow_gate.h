#ifndef OCAML_TEMPORAL_RESPONSE_BORROW_GATE_H
#define OCAML_TEMPORAL_RESPONSE_BORROW_GATE_H

#include <stdatomic.h>

/* A short read borrows immutable Rust result bytes until its copy finishes.
 * Closing prevents new borrows and waits for admitted readers before the
 * caller releases the Rust allocation. The production gate lives in an OCaml
 * custom block, so its address must not survive an OCaml allocation. */
typedef struct response_borrow_gate {
  atomic_uint active_reads;
  atomic_int live;
} response_borrow_gate;

/* Initialize one gate before publishing its containing custom block. */
static inline void response_borrow_gate_init(response_borrow_gate *gate) {
  atomic_init(&gate->active_reads, 0);
  atomic_init(&gate->live, 1);
}

/* Admit an allocation-free read. Sequential consistency ensures a closer
 * that observes zero readers cannot free before a later reader observes the
 * closed flag; an admitted reader remains counted until it stops copying. */
static inline int response_borrow_gate_acquire(response_borrow_gate *gate) {
  atomic_fetch_add_explicit(&gate->active_reads, 1, memory_order_seq_cst);
  if (atomic_load_explicit(&gate->live, memory_order_seq_cst) == 0) {
    atomic_fetch_sub_explicit(&gate->active_reads, 1, memory_order_seq_cst);
    return 0;
  }
  return 1;
}

/* Release a read before entering any OCaml allocation or exception path. */
static inline void response_borrow_gate_release(response_borrow_gate *gate) {
  atomic_fetch_sub_explicit(&gate->active_reads, 1, memory_order_seq_cst);
}

/* Close once, wait for admitted readers, and report whether this caller owns
 * the subsequent Rust free. The yield callback must not call OCaml APIs. */
static inline int response_borrow_gate_close_and_wait(
    response_borrow_gate *gate, void (*yield_thread)(void)) {
  int expected = 1;
  if (!atomic_compare_exchange_strong_explicit(
          &gate->live, &expected, 0, memory_order_seq_cst,
          memory_order_seq_cst)) {
    return 0;
  }
  while (atomic_load_explicit(&gate->active_reads, memory_order_seq_cst) != 0) {
    yield_thread();
  }
  return 1;
}

#endif
