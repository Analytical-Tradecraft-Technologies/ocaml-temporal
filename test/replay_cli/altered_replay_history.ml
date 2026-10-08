(** A copy of the example replay checker that links a deliberately changed
    version of the example workflow, standing in for an application whose
    next release is incompatible with recorded executions.

    The first argument selects the change, and the remaining arguments are
    passed to [Replay_command] unchanged:
    - [nondeterministic]: the workflow no longer starts its durable timer, so
      replaying a history that recorded [TimerStarted] is a nondeterminism.
    - [failing]: the workflow returns a defect, which fails the replayed
      workflow task.
    - [duplicate]: the same workflow is registered twice, an invalid
      registration that prevents the replay from running at all. *)

open Example_support.Definitions

(** The example workflow with its 250 ms timer removed: the same activities
    are scheduled in the same order, so only the missing timer command
    differs from the recorded history. *)
let without_timer name =
  let open Temporal.Result_syntax in
  let normalized_name = String.trim name in
  let* greeting_input = render_request "greeting" normalized_name in
  let* next_step_input = render_request "next-step" normalized_name in
  let greeting = Temporal.Activity.start remote_render_message greeting_input in
  let next_step =
    Temporal.Activity.start remote_render_message next_step_input
  in
  let* messages =
    Temporal.Future.await (Temporal.Future.all [ greeting; next_step ])
  in
  Ok (String.concat "\n" messages)

(** A workflow implementation that always reports a programming defect. A
    defect fails the workflow task rather than the workflow execution. *)
let always_defect _name =
  Error (Temporal.Error.defect ~message:"altered example workflow defect")

(** Defines a workflow under the example's type name and codecs, so the
    recorded history selects it during replay. *)
let as_example implementation =
  Temporal.Workflow.define ~name:compose_message_name
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string implementation

(** Selects the altered registrations named by [variant]. *)
let workflows = function
  | "nondeterministic" -> Some [ Temporal.Replay.workflow (as_example without_timer) ]
  | "failing" -> Some [ Temporal.Replay.workflow (as_example always_defect) ]
  | "duplicate" ->
      Some
        [
          Temporal.Replay.workflow local_compose_message;
          Temporal.Replay.workflow local_compose_message;
        ]
  | _ -> None

let () =
  match Array.to_list Sys.argv with
  | _ :: variant :: arguments -> (
      match workflows variant with
      | Some workflows ->
          exit
            (Replay_command.run_command ~program:"altered_replay_history"
               ~workflows arguments)
      | None ->
          prerr_endline ("unknown variant " ^ variant);
          exit 125)
  | _ ->
      prerr_endline "usage: altered_replay_history VARIANT ARGUMENTS...";
      exit 125
