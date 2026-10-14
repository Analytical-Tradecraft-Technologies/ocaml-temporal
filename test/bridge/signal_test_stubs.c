#include <caml/mlvalues.h>

#if !defined(_WIN32)
#include <signal.h>
#endif

/* Deliver SIGUSR1 to the calling thread and return without running any OCaml
 * code (#1018). [raise] delivers a signal to its own thread before it returns,
 * so OCaml's C-level handler has recorded the signal as pending by then, but
 * unlike [Unix.kill] this stub never processes pending actions itself. The
 * OCaml handler therefore runs at the caller's next poll point, which lets the
 * test make a signal pending exactly as a bridge stub begins. The test is
 * skipped on Windows, which has no SIGUSR1; there the stub does nothing. It
 * neither allocates nor raises, so the OCaml declaration is [@@noalloc]. */
value ocaml_temporal_test_raise_sigusr1(value unit) {
  (void)unit;
#if !defined(_WIN32)
  (void)raise(SIGUSR1);
#endif
  return Val_unit;
}
