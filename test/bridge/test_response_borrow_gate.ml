(** A test-only C harness imports the same private gate used by native results.
    It holds a native read until OCaml releases it, so close/read overlap is
    guaranteed rather than dependent on a favorable race schedule. *)
external reset : unit -> unit = "ocaml_temporal_test_response_gate_reset"

external hold_read : unit -> bool
  = "ocaml_temporal_test_response_gate_hold_read"

external close : unit -> bool = "ocaml_temporal_test_response_gate_close"

external release_read : unit -> unit
  = "ocaml_temporal_test_response_gate_release_read"

external state : unit -> int = "ocaml_temporal_test_response_gate_state"

external try_read : unit -> bool
  = "ocaml_temporal_test_response_gate_try_read"

(** Bound every cross-Domain wait; a broken close cannot hang the test job. *)
let await label bit =
  let deadline = Unix.gettimeofday () +. 5.0 in
  while state () land bit = 0 do
    if Unix.gettimeofday () >= deadline then (
      prerr_endline ("response borrow gate timed out: " ^ label);
      Unix._exit 1);
    Domain.cpu_relax ()
  done

let () =
  reset ();
  let reader = Domain.spawn hold_read in
  await "reader admission" 1;
  let closer = Domain.spawn close in
  await "close gate" 2;
  await "close waiting for admitted read" 16;
  let closed_too_early = state () land 8 <> 0 in
  release_read ();
  await "reader release" 4;
  await "close completion" 8;
  assert (Domain.join reader);
  assert (Domain.join closer);
  assert (not closed_too_early);
  assert (not (try_read ()));
  assert (not (close ()))
