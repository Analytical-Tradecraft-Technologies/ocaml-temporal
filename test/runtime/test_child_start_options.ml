(** Tests for child workflow timeouts and ID reuse policy (#1135) at the public
    [Temporal.Child_workflow] boundary.

    The fixtures install a private workflow context on a deterministic
    scheduler and inspect the buffered runtime command, so they prove that
    valid options reach the [Start_child_workflow] command as exact
    milliseconds, that omitted options leave the command as before, and that
    every combination [Client.start] rejects is a typed defect returned
    before any command or sequence number exists. The protocol and Rust
    bridge re-checks are covered by the bridge tests. *)

module Activation = Temporal_runtime.Activation
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Fails with the scenario label when two values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** A command-only child reference; no child code runs in these tests. *)
let child =
  Temporal.Workflow.remote ~name:"child" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Shorthand for a whole-second public duration. *)
let seconds value = Temporal.Duration.of_seconds value

(** Starts one child with [start] inside a fresh workflow context, returning
    the ready result (if any) and every buffered command. *)
let with_start start =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let peeked = ref None in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          peeked := Temporal.Future.peek (start ())));
  ignore (Scheduler.run_label scheduler);
  let commands = Workflow_context_store.take_commands context in
  Workflow_context_store.shutdown context;
  (!peeked, commands)

(** Valid timeouts and every reuse policy are copied into the command as
    exact milliseconds and the matching runtime variant. *)
let test_options_reach_command () =
  List.iter
    (fun (policy, expected_policy) ->
      let _, commands =
        with_start (fun () ->
            Temporal.Child_workflow.start ~id:"child-1"
              ~execution_timeout:(seconds 3_600L) ~run_timeout:(seconds 600L)
              ~task_timeout:(Temporal.Duration.of_ms 10_500L)
              ~id_reuse_policy:policy child "input")
      in
      match commands with
      | [ Activation.Start_child_workflow
            { execution_timeout; run_timeout; task_timeout; id_reuse_policy; _ } ]
        ->
          expect "execution timeout" (Some 3_600_000L) execution_timeout;
          expect "run timeout" (Some 600_000L) run_timeout;
          expect "task timeout" (Some 10_500L) task_timeout;
          expect "reuse policy" (Some expected_policy) id_reuse_policy
      | _ -> failwith "expected exactly one child start command")
    [
      (`Allow_duplicate, Activation.Reuse_allow_duplicate);
      (`Allow_duplicate_failed_only, Activation.Reuse_allow_duplicate_failed_only);
      (`Reject_duplicate, Activation.Reuse_reject_duplicate);
    ]

(** Omitted options leave every new field unset, so the command (and the
    history it records) is unchanged from earlier releases. *)
let test_omitted_options_unchanged () =
  let _, commands =
    with_start (fun () -> Temporal.Child_workflow.start ~id:"child-1" child "input")
  in
  match commands with
  | [ Activation.Start_child_workflow
        { execution_timeout = None; run_timeout = None; task_timeout = None;
          id_reuse_policy = None; _ } ] -> ()
  | _ -> failwith "omitted child options changed the start command"

(** Every timeout combination [Client.start] rejects is a ready typed defect
    with the same message, and no command is emitted. The input codec is
    never reached, so a rejected start cannot run application code. *)
let test_invalid_timeouts_rejected () =
  List.iter
    (fun (label, start, message) ->
      let peeked, commands = with_start start in
      (match peeked with
      | Some (Error error) ->
          expect (label ^ " kind") "defect" (Temporal.Error.kind error);
          expect (label ^ " message") message (Temporal.Error.message error)
      | Some (Ok _) | None -> failwith (label ^ " was not a ready defect"));
      expect (label ^ " emitted nothing") [] commands)
    [
      ( "zero execution",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~execution_timeout:(Temporal.Duration.of_ms 0L) child "input"),
        "execution_timeout must be positive" );
      ( "task above 120 s",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~task_timeout:(seconds 121L) child "input"),
        "task_timeout (at most 120 seconds) exceeds its maximum" );
      ( "run above execution",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~execution_timeout:(seconds 60L) ~run_timeout:(seconds 61L) child
            "input"),
        "run_timeout exceeds execution_timeout" );
      ( "task above run",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~run_timeout:(seconds 30L) ~task_timeout:(seconds 31L) child "input"),
        "task_timeout exceeds the run (or execution) timeout" );
      ( "task above execution without run",
        (fun () ->
          Temporal.Child_workflow.start ~id:"child-1"
            ~execution_timeout:(seconds 30L) ~task_timeout:(seconds 31L) child
            "input"),
        "task_timeout exceeds the run (or execution) timeout" );
    ]

(** Runs the child start option contract as one dune test. *)
let () =
  test_options_reach_command ();
  test_omitted_options_unchanged ();
  test_invalid_timeouts_rejected ()
