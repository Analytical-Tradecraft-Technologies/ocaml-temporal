(** Pins the outbound argument-count rule for every workflow-side input
    (#1012). Whether an input is sent as zero or one Temporal payloads is
    decided by its codec, not by the encoded payload's shape: [Codec.unit] is
    a call with no arguments (#819), while [Codec.option]'s [None] -- which
    encodes to the same [binary/null] bytes -- is one [null] argument, as the
    Go, Java, and Python SDKs send for a nil value. A foreign
    handler with one optional parameter therefore receives its argument.

    Each case drives the public workflow API inside a private execution
    context and inspects the buffered runtime commands, so no Temporal server
    is required. *)

module Activation = Temporal_runtime.Activation
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** The exact payload every Temporal SDK uses for a null value. *)
let null_payload : Temporal_base.Codec.payload =
  { Temporal_base.Payload.metadata = [ ("encoding", "binary/null") ];
    data = Bytes.empty }

(** Runs [operation] as a workflow fiber in a fresh context and returns the
    commands it buffered. The fiber may end by aborting (continue-as-new), so
    the scheduler status is not asserted; only the emitted commands matter. *)
let commands_of operation =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          ignore (operation ())));
  ignore (Scheduler.run scheduler);
  let commands = Workflow_context_store.take_commands context in
  Workflow_context_store.shutdown context;
  commands

(** Extracts the argument list of the single input-carrying command. *)
let arguments_of label commands =
  match commands with
  | [ Activation.Schedule_activity { arguments; _ } ]
  | [ Activation.Schedule_local_activity { arguments; _ } ]
  | [ Activation.Start_child_workflow { input = arguments; _ } ]
  | [ Activation.Continue_as_new { input = arguments; _ } ]
  | [ Activation.Signal_external_workflow { input = arguments; _ } ] ->
      arguments
  | _ -> failwith (label ^ ": expected exactly one input-carrying command")

(** Requires [label]'s command to carry exactly [expected] arguments. *)
let expect label expected commands =
  if arguments_of label commands <> expected then
    failwith (label ^ ": unexpected argument payloads")

(** Exercises one input codec across every workflow-side command, checking
    that [value] is sent as [expected]. *)
let check_codec : type a. string -> a Temporal.Codec.t -> a ->
    Temporal_base.Codec.payload list -> unit =
 fun label codec value expected ->
  let activity =
    Temporal.Activity.remote ~name:"lookup" ~input:codec
      ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.remote ~name:"child" ~input:codec
      ~output:Temporal.Codec.unit
  in
  let signal = Temporal.Signal.define ~name:"notify" ~input:codec in
  expect (label ^ " activity") expected
    (commands_of (fun () -> Temporal.Activity.start activity value));
  expect (label ^ " local activity") expected
    (commands_of (fun () -> Temporal.Activity.start_local activity value));
  expect (label ^ " child workflow") expected
    (commands_of (fun () ->
         Temporal.Child_workflow.start ~id:"child-1" workflow value));
  expect (label ^ " continue-as-new") expected
    (commands_of (fun () -> Temporal.Workflow.continue_as_new workflow value));
  expect (label ^ " external signal") expected
    (commands_of (fun () ->
         Temporal.Workflow.signal_external_workflow ~workflow_id:"target"
           ~run_id:"" ~signal ~input:value))

let () =
  (* A unit input is a call with no arguments. *)
  check_codec "unit" Temporal.Codec.unit () [];
  (* [None] is one null argument even though its bytes equal [Codec.unit]. *)
  check_codec "option None" (Temporal.Codec.option Temporal.Codec.string) None
    [ null_payload ];
  (* An ordinary present value is one argument, unchanged. *)
  check_codec "option Some"
    (Temporal.Codec.option Temporal.Codec.string)
    (Some "customer-1")
    [
      { Temporal_base.Payload.metadata = [ ("encoding", "json/plain") ];
        data = Bytes.of_string "\"customer-1\"" };
    ];
  (* [Codec.is_unit] identifies only [Codec.unit], never a codec that merely
     writes [binary/null]. *)
  if not (Temporal.Codec.is_unit Temporal.Codec.unit) then
    failwith "Codec.unit is not reported as unit";
  if Temporal.Codec.is_unit (Temporal.Codec.option Temporal.Codec.unit) then
    failwith "an option codec was reported as unit";
  if
    Temporal.Codec.is_unit
      (Temporal.Codec.make ~encoding:"binary/null"
         ~encode:(fun () -> Ok Bytes.empty)
         ~decode:(fun _ -> Ok ()))
  then failwith "a user binary/null codec was reported as unit"
