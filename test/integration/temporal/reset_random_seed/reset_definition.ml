(** A bounded reset witness. Random values affect both a durable timer command
    and the result, so replay must reproduce the prefix and replacement stream. *)
let run _input =
  let open Temporal.Result_syntax in
  let* before = Temporal.Workflow.random_int ~bound:1_000_000 in
  let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 1_000L) in
  let* after = Temporal.Workflow.random_int ~bound:1_000_000 in
  let* () = Temporal.Workflow.sleep
      (Temporal.Duration.of_ms (Int64.of_int (1_000 + after mod 2_000))) in
  Ok (Printf.sprintf "%d:%d" before after)

(** Public definition used by the live worker and the private replay test. *)
let workflow = Temporal.Workflow.define ~name:"reset-random-seed"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string run
