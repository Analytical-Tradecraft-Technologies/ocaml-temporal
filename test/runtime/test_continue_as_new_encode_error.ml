(* Regression tests for #1034: a continue-as-new successor input encoder that
   returns an error of any category must fail only the current workflow task,
   never the workflow execution itself. *)

module Activation = Temporal_runtime.Activation
module Raw_execution = Temporal_runtime.Execution

(** Copies a public payload into the private representation used by the
    low-level execution state machine. *)
let base_payload (payload : Temporal.Payload.t) : Temporal_base.Payload.t =
  { Temporal_base.Payload.metadata = payload.metadata; data = Bytes.copy payload.data }

(** Copies a private payload back into the public representation for codec
    callbacks. *)
let public_payload (payload : Temporal_base.Payload.t) : Temporal.Payload.t =
  { Temporal.Payload.metadata = payload.metadata; data = Bytes.copy payload.data }

(** Converts a public error into the private error type accepted by the
    execution module, preserving its category, message and details. *)
let base_error (error : Temporal.Error.t) : Temporal_base.Error.t =
  let view = Temporal.Error.view error in
  Temporal_base.Error.make ~non_retryable:view.non_retryable
    ~details:(List.map base_payload view.details) ~category:view.category
    ~message:view.message ()

(** Adapts an opaque public codec to the base codec used by the private
    workflow definition. *)
let base_codec (codec : 'a Temporal.Codec.t) : 'a Temporal_base.Codec.t =
  Temporal_base.Codec.of_payload
    ~encode:(fun value ->
      match Temporal.Codec.encode codec value with
      | Ok payload -> Ok (base_payload payload)
      | Error error -> Error (base_error error))
    ~decode:(fun payload ->
      match Temporal.Codec.decode codec (public_payload payload) with
      | Ok value -> Ok value
      | Error error -> Error (base_error error))

(** Rebuilds a public workflow definition as the base definition accepted by
    [Raw_execution.start]. The installed package exposes only the opaque
    public definition, so this conversion is test-only. *)
let base_workflow (definition : ('input, 'output) Temporal.Workflow.t) =
  let implementation =
    Option.map
      (fun implementation input ->
        Result.map_error base_error (implementation input))
      (Temporal.Workflow.implementation definition)
  in
  Temporal_base.Definition.make ~name:(Temporal.Workflow.name definition)
    ~input:(base_codec (Temporal.Workflow.input definition))
    ~output:(base_codec (Temporal.Workflow.output definition)) ~implementation

(** Starts a workflow whose body continues as new into a successor whose input
    codec always fails with [error], then runs its first activation. Returns
    the emitted commands and the recorded task failure, if any. *)
let run_failing_continue_as_new ~name error =
  let bad_input =
    Temporal.Codec.make ~encoding:"json/plain"
      ~encode:(fun () -> Error error)
      ~decode:(fun _ -> Error (Temporal.Error.codec ~message:"unused"))
  in
  let successor =
    Temporal.Workflow.remote ~name:(name ^ "_target") ~input:bad_input
      ~output:Temporal.Codec.unit
  in
  let source =
    Temporal.Workflow.define ~name ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.continue_as_new successor ())
  in
  let execution = Raw_execution.start (base_workflow source) () in
  let commands = Raw_execution.activate execution [ Activation.Start_workflow ] in
  (commands, Raw_execution.task_failure execution)

(** A custom encoder returning a [`Workflow] error must not emit a
    [Fail_workflow] command. The task fails instead with a [Codec] error that
    still names the original category and message. *)
let test_workflow_category_encode_error_fails_task () =
  let error =
    Temporal.Error.make ~category:`Workflow ~error_type:"InvalidSuccessor"
      ~message:"successor input rejected" ()
  in
  match run_failing_continue_as_new ~name:"can_workflow_category" error with
  | [], Some failure ->
      let view = Temporal_base.Error.view failure in
      if view.category <> `Codec then
        failwith "continue-as-new encode error was not reported as a codec error";
      let contains needle =
        let haystack = view.message in
        let n = String.length needle and h = String.length haystack in
        let rec scan i =
          i + n <= h && (String.sub haystack i n = needle || scan (i + 1))
        in
        scan 0
      in
      if not (contains "successor input rejected" && contains "workflow") then
        failwith ("rewrapped error lost its original context: " ^ view.message)
  | _ :: _, _ ->
      failwith "a non-codec continue-as-new encode error emitted commands"
  | [], None ->
      failwith "a non-codec continue-as-new encode error did not fail the task"

(** Every non-task-failure category behaves the same way, and a [Codec] error
    keeps its original message unchanged. *)
let test_every_category_fails_task () =
  List.iter
    (fun category ->
      let error =
        Temporal.Error.make ~category ~message:"rejected" ()
      in
      match run_failing_continue_as_new ~name:"can_any_category" error with
      | [], Some failure ->
          let view = Temporal_base.Error.view failure in
          (match category with
          | `Bridge | `Defect ->
              if view.category <> category then
                failwith "task-failure category was not preserved"
          | `Codec ->
              if view.category <> `Codec || view.message <> "rejected" then
                failwith "codec encode error identity was not preserved"
          | _ ->
              if view.category <> `Codec then
                failwith "non-codec encode error was not rewrapped")
      | _ ->
          failwith
            ("continue-as-new encode error did not fail only the task: "
            ^ Temporal.Error.kind error))
    [ `Activity; `Bridge; `Cancelled; `Child_workflow; `Codec; `Defect;
      `Nexus; `Terminated; `Timeout; `Update; `Workflow ]

(** Runs every #1034 regression; any failure raises and fails the alias. *)
let () =
  test_workflow_category_encode_error_fails_task ();
  test_every_category_fails_task ()
