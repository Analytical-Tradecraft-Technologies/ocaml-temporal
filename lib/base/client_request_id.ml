(** Allocates identities for client operations, outside deterministic workflow
    execution. A fresh system-seeded state per call avoids shared PRNG state,
    counter resets, and inherited generator state across processes or Domains.
    These identifiers are deduplication keys, not authentication secrets. *)
let create () =
  let random = Random.State.make_self_init () in
  let high = Random.State.bits64 random in
  let low = Random.State.bits64 random in
  Printf.sprintf "ocaml-client-%016Lx%016Lx" high low
