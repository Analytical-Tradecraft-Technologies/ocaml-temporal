(** Regression for #809: local-activity backoff commands must keep their order
    relative to commands from other fibers when Core merges jobs that were
    delivered in separate activations live into one activation on replay.

    The workflow waits on one local activity while a signal handler starts an
    unrelated timer. Each scenario applies the same ordered jobs once split
    into one activation per job, as a live worker may see them inside a
    heartbeating workflow task, and once merged, as Core may replay them. The
    concatenated command streams must be identical, otherwise Core reports
    nondeterminism for the recorded history. *)
module Activation = Temporal_runtime.Activation
module Context = Temporal_runtime.Workflow_context_store
module Execution = Temporal_runtime.Execution
module Future = Temporal_runtime.Future_store

(** An empty payload used for signal input and the activity argument. *)
let empty_payload = { Temporal_base.Payload.metadata = []; data = Bytes.empty }

(** Delay of the signal handler's timer, distinct from the backoff delay so the
    rendered commands show which fiber emitted each timer. *)
let signal_timer_milliseconds = 5_000L

(** Backoff delay Core requests once its local retry threshold is exceeded. *)
let backoff_milliseconds = 64_000L

(** The root schedules one local activity (sequence 1) and returns once Core
    resolves it. Retry delays are owned by the runtime, so the root never
    observes them. *)
let definition =
  Temporal_base.Definition.make ~name:"local-backoff-order"
    ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.string
    ~implementation:
      (Some
         (fun () ->
           let context = Option.get (Context.current ()) in
           let future, _cancel =
             Context.schedule_local_activity context ~name:"flaky"
               ~input:empty_payload ~decode:(fun payload -> Ok payload) ()
           in
           Result.map (fun _ -> "done") (Future.await future)))

(** Each delivered signal starts one workflow timer and returns without
    awaiting it, standing in for any handler that emits a command. *)
let signal_handler =
  Execution.make_signal_handler ~name:"poke" ~dispatch:(fun _signal ->
      let context = Option.get (Context.current ()) in
      ignore (Context.start_timer context signal_timer_milliseconds);
      Ok ())

(** Core's signal job for the handler above. *)
let signal =
  Activation.Signal_workflow
    { signal_name = "poke"; input = [ empty_payload ]; identity = "test";
      headers = [] }

(** Core's request to retry local activity 1 after a long backoff. *)
let backoff =
  Activation.Resolve_local_activity_backoff
    { seq = 1L; attempt = 2L; backoff_milliseconds;
      original_schedule_time = None }

(** Renders commands compactly so a mismatch names the reordered command. *)
let render = function
  | Activation.Schedule_local_activity { seq; attempt; _ } ->
      Printf.sprintf "local-activity(seq=%Ld,attempt=%Ld)" seq attempt
  | Activation.Start_timer { seq; milliseconds } ->
      Printf.sprintf "timer(seq=%Ld,ms=%Ld)" seq milliseconds
  | Activation.Complete_workflow _ -> "complete"
  | Activation.Fail_workflow error ->
      "fail:" ^ Temporal_base.Error.message error
  | _ -> "other"

(** Applies each activation in order to a fresh execution and returns every
    emitted command, rendered, in emission order. *)
let run activations =
  let execution =
    Execution.start ~signal_handlers:[ signal_handler ] definition ()
  in
  Fun.protect ~finally:(fun () -> Execution.shutdown execution) (fun () ->
      List.concat_map
        (fun jobs -> List.map render (Execution.activate execution jobs))
        activations)

(** Asserts an exact command stream, printing both on mismatch. *)
let expect label expected actual =
  if expected <> actual then
    failwith
      (Printf.sprintf "%s:\n  expected [%s]\n  actual   [%s]" label
         (String.concat "; " expected) (String.concat "; " actual))

(** Split activations stand in for the live run and merged activations for the
    replay. Both must equal the explicitly expected history order. *)
let check label ~live ~replay expected =
  expect (label ^ " live") expected (run live);
  expect (label ^ " replay") expected (run replay)

(** The issue's scenario: a signal and then Core's backoff job. The signal
    handler's timer was allocated first live, so the backoff timer must follow
    it even when both jobs arrive together. The same holds for the timer
    firing: the retry must follow the commands of an earlier signal. *)
let test_signal_before_backoff () =
  let resolve =
    Activation.Resolve_activity { seq = 1L; result = Ok empty_payload }
  in
  check "signal before backoff"
    ~live:
      [ [ Activation.Start_workflow ]; [ signal ]; [ backoff ]; [ signal ];
        [ Activation.Fire_timer { seq = 3L } ]; [ resolve ] ]
    ~replay:
      [ [ Activation.Start_workflow ]; [ signal; backoff ];
        [ signal; Activation.Fire_timer { seq = 3L } ]; [ resolve ] ]
    [ "local-activity(seq=1,attempt=1)"; "timer(seq=2,ms=5000)";
      "timer(seq=3,ms=64000)"; "timer(seq=4,ms=5000)";
      "local-activity(seq=1,attempt=2)"; "complete" ]

(** The opposite job order must still follow job order, not job kind: the
    backoff timer precedes a later signal's timer and the retry precedes a
    later signal's commands. *)
let test_backoff_before_signal () =
  check "backoff before signal"
    ~live:
      [ [ Activation.Start_workflow ]; [ backoff ]; [ signal ];
        [ Activation.Fire_timer { seq = 2L } ]; [ signal ] ]
    ~replay:
      [ [ Activation.Start_workflow ]; [ backoff; signal ];
        [ Activation.Fire_timer { seq = 2L }; signal ] ]
    [ "local-activity(seq=1,attempt=1)"; "timer(seq=2,ms=64000)";
      "timer(seq=3,ms=5000)"; "local-activity(seq=1,attempt=2)";
      "timer(seq=4,ms=5000)" ]

let () =
  test_signal_before_backoff ();
  test_backoff_before_signal ();
  print_endline "local activity backoff command order: ok"
