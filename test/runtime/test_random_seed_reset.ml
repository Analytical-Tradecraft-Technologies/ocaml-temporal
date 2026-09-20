(** Reset regressions at the strict protocol/runtime boundary. The random draw
    before the timer belongs to the original seed; draws after the timer must
    belong to the last reset seed, in live execution and replay alike. *)
module Protocol = Temporal_protocol.Workflow_protocol
module Execution = Temporal_runtime.Execution
module Native_execution = Temporal_runtime.Native_execution
module Context = Temporal_runtime.Workflow_context_store

(** Reports a failed scenario without discarding its assertion label. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Unwraps a public workflow helper inside this test-only private definition. *)
let public_result = function
  | Ok value -> value
  | Error error -> failwith (Temporal.Error.message error)

(** Draws from the public workflow API so reset cannot pass by replacing a
    private context that workflow code does not actually use. *)
let draw () = public_result (Temporal.Workflow.random_int ~bound:1_000_000)

(** Computes an independent freshly seeded stream using the same public API. *)
let expected_stream seed =
  let scheduler = Temporal_runtime.Scheduler.create () in
  let context = Context.create ~randomness_seed:seed scheduler in
  Fun.protect ~finally:(fun () -> Context.shutdown context) (fun () ->
      Context.with_context context (fun () ->
          let first = draw () in
          let second = draw () in
          (first, second)))

(** Builds the envelope validated by the native adapter before state mutation. *)
let activation ~is_replaying jobs : Protocol.activation =
  { run_id = "reset-run"; timestamp = Some { seconds = 1L; nanoseconds = 0 };
    is_replaying; history_length = 12L; jobs; metadata = None }

(** Exercises two ordered replacements, full-width uint64 seeds, rejection
    before a timer is consumed, and unchanged earlier workflow observations. *)
let test_reset ~is_replaying seed =
  let before = ref None and after = ref None in
  let definition = Temporal_base.Definition.make ~name:"reset-random"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:(Some (fun () ->
        before := Some (draw ());
        public_result (Temporal.Workflow.sleep (Temporal.Duration.of_ms 1L));
        let first = draw () in
        let second = draw () in
        after := Some (first, second);
        Ok ())) in
  let execution = Execution.start ~randomness_seed:"42" definition () in
  let activate jobs = Native_execution.activate execution
      (activation ~is_replaying jobs) in
  Fun.protect ~finally:(fun () -> Execution.shutdown execution) (fun () ->
    let input = Result.get_ok (Temporal_base.Codec.encode Temporal_base.Codec.unit ()) in
    let payload : Protocol.payload =
      { metadata = List.map (fun (key, value) -> (key, Bytes.of_string value)) input.metadata;
        data = input.data } in
    let initial = Result.get_ok (activate [ Protocol.Initialize_workflow
      { workflow_id = "reset-workflow"; workflow_type = "reset-random";
        arguments = [ payload ]; randomness_seed = "42"; attempt = 1; context = None } ]) in
    let seq = match initial.commands with
      | [ Protocol.Start_timer { seq; _ } ] -> seq
      | _ -> failwith "initial random draw did not reach its timer" in
    expect "original seed" (Some (fst (expected_stream "42"))) !before;
    expect "suspended workflow" None !after;
    (match activate [ Protocol.Fire_timer { seq };
        Protocol.Update_random_seed { randomness_seed = "18446744073709551616" } ] with
     | Error _ -> ()
     | Ok _ -> failwith "invalid seed reached workflow state");
    expect "invalid activation kept timer suspended" None !after;
    let completed = Result.get_ok (activate [
      Protocol.Update_random_seed { randomness_seed = "7" };
      Protocol.Update_random_seed { randomness_seed = seed };
      Protocol.Fire_timer { seq } ]) in
    expect "last reset seed" (Some (expected_stream seed)) !after;
    expect "prefix retained" (Some (fst (expected_stream "42"))) !before;
    match completed.commands with
    | [ Protocol.Complete_workflow _ ] -> ()
    | _ -> failwith "reset workflow failed to complete")

(** Runs every edge seed through both live and replay activation metadata. *)
let () =
  List.iter (fun is_replaying ->
    List.iter (test_reset ~is_replaying)
      [ "0"; "1"; "9223372036854775808"; "18446744073709551615" ])
    [ false; true ]
