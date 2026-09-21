module Activation = Temporal_runtime.Activation
module Bridge = Temporal_core_bridge.Native_bridge
module Raw_execution = Temporal_runtime.Execution
module Observability = Temporal_base.Observability

(** Copies public payloads into the base representation consumed by the private
    execution fixture. This test observes logging around the low-level runtime,
    so it performs the boundary conversion explicitly instead of relying on a
    public/private record alias. *)
let base_payload (payload : Temporal.Payload.t) : Temporal_base.Payload.t =
  {
    Temporal_base.Payload.metadata = List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Converts public structured errors for the private execution definition. *)
let base_error (error : Temporal.Error.t) : Temporal_base.Error.t =
  let view = Temporal.Error.view error in
  Temporal_base.Error.make ~non_retryable:view.non_retryable
    ~details:(List.map base_payload view.details) ~category:view.category
    ~message:view.message ()

(** Installs public codec callbacks into the private codec representation while
    preserving each codec's own encoding metadata. *)
let base_codec (codec : 'a Temporal.Codec.t) : 'a Temporal_base.Codec.t =
  Temporal_base.Codec.of_payload
    ~encode:(fun value ->
      match Temporal.Codec.encode codec value with
      | Ok payload -> Ok (base_payload payload)
      | Error error -> Error (base_error error))
    ~decode:(fun payload ->
      let public_payload : Temporal.Payload.t =
        {
          Temporal.Payload.metadata =
            List.map (fun (key, value) -> (key, value)) payload.metadata;
          data = Bytes.copy payload.data;
        }
      in
      match Temporal.Codec.decode codec public_payload with
      | Ok value -> Ok value
      | Error error -> Error (base_error error))

(** Rebuilds an opaque public workflow as the private definition accepted by
    [Raw_execution]. Public errors cross this boundary only as copied values. *)
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

(** Keeps the logging fixture's execution calls concise while making the
    private-runtime conversion visible to readers. *)
module Execution = struct
  include Raw_execution

  let start ?task_queue definition input =
    Raw_execution.start ?task_queue (base_workflow definition) input
end

(** One event captured without retaining its formatting closure. Tests inspect
    stable source, level, and tag contracts rather than exact prose. *)
type event = {
  source : string;
  level : Logs.level;
  tags : Logs.Tag.set;
  message : string;
}

(** Captured events are process-local because each Dune test is a separate
    executable. *)
let events = ref []

(** Reporter that eagerly renders the bounded SDK message and preserves tags
    for structural assertions. *)
let capture_reporter =
  let report source level ~over continuation messagef =
    messagef (fun ?header:_ ?(tags = Logs.Tag.empty) format ->
        Format.kasprintf
          (fun message ->
            events :=
              {
                source = Logs.Src.name source;
                level;
                tags;
                message;
              }
              :: !events;
            over ();
            continuation ())
          format)
  in
  { Logs.report }

(** Reporter used to prove that an application reporter defect cannot alter an
    SDK operation's result or escape through workflow processing. *)
let raising_reporter =
  { Logs.report = (fun _ _ ~over:_ _ _ -> failwith "reporter defect") }

(** Runs an action with all SDK sources enabled and restores process-global
    Logs state even when an assertion fails. *)
let with_capture action =
  let reporter = Logs.reporter () in
  let level = Logs.level () in
  events := [];
  Logs.set_reporter capture_reporter;
  Logs.set_level (Some Logs.Debug);
  Fun.protect
    ~finally:(fun () ->
      Logs.set_reporter reporter;
      Logs.set_level level)
    action

(** Finds the newest event matching a stable source, level, and operation tag. *)
let find_event ~source ~level ~operation =
  List.find_opt
    (fun event ->
      event.source = source
      && event.level = level
      && Logs.Tag.find Observability.Tag.operation event.tags = Some operation)
    !events

(** Extracts an event or fails with a structural description. *)
let require_event ~source ~level ~operation =
  match find_event ~source ~level ~operation with
  | Some event -> event
  | None ->
      failwith
        (Printf.sprintf "missing %s event for %s" source operation)

(** Returns whether [needle] occurs in [haystack]. This local helper keeps the
    privacy assertion compatible with the project's OCaml 5.2 floor. *)
let contains ~haystack ~needle =
  let haystack_length = String.length haystack in
  let needle_length = String.length needle in
  let rec search offset =
    offset + needle_length <= haystack_length
    &&
    (String.sub haystack offset needle_length = needle || search (offset + 1))
  in
  needle_length = 0 || search 0

(** Renders tags only for privacy assertions. Production reporters receive the
    typed set directly; this rendering lets the test catch a secret that might
    be hidden in a structural tag even when the message prose is constant. *)
let rendered_tags event = Format.asprintf "%a" Logs.Tag.pp_set event.tags

(** Proves that a user-controlled value is absent from both portions of an
    emitted diagnostic record. The check intentionally covers every captured
    event, including lifecycle records emitted while a fixture is cleaned up. *)
let assert_not_in_events secret =
  List.iter
    (fun event ->
      assert (not (contains ~haystack:event.message ~needle:secret));
      assert (not (contains ~haystack:(rendered_tags event) ~needle:secret)))
    !events

(** Builds a string payload for the synthetic workflow fixture. *)
let payload value =
  match Temporal.Codec.encode Temporal.Codec.string value with
  | Ok payload -> base_payload payload
  | Error error -> failwith (Temporal.Error.message error)

(** Activity that keeps the workflow blocked after its first activation. *)
let waiting_activity =
  Temporal.Activity.remote ~name:"wait" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Records whether an application reporter can observe active workflow
    context, and attempts a re-entrant workflow API call if it can. *)
let reporter_observed_workflow_context = ref false

(** Reporter fixture that probes the determinism boundary before acknowledging
    each record. *)
let context_probing_reporter =
  let report _source _level ~over continuation messagef =
    if Temporal.Workflow_context.is_active () then (
      reporter_observed_workflow_context := true;
      ignore (Temporal.Activity.start waiting_activity "reporter-reentry"));
    messagef (fun ?header:_ ?tags:_ format ->
        Format.kasprintf
          (fun _message ->
            over ();
            continuation ())
          format)
  in
  { Logs.report }

(** Workflow fixture whose payload-like input must never enter log prose. *)
let waiting_workflow =
  Temporal.Workflow.define ~name:"logging_fixture"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      Temporal.Activity.execute waiting_activity input)

(** Workflow fixture that reaches the failure reporter from a running fiber. *)
let failing_workflow =
  Temporal.Workflow.define ~name:"failing_logging_fixture"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
      Error (Temporal.Error.make ~category:`Workflow ~message:"fixture failure" ()))

(** Stable source and tag names are the filtering contract applications use. *)
let test_source_and_tag_names () =
  assert (Logs.Src.name Observability.Source.lifecycle = "temporal.sdk.lifecycle");
  assert (Logs.Src.name Observability.Source.bridge = "temporal.sdk.bridge");
  assert (Logs.Src.name Observability.Source.workflow = "temporal.sdk.workflow");
  assert (Logs.Tag.name Observability.Tag.operation = "temporal.operation");
  assert (Logs.Tag.name Observability.Tag.duration_ms = "temporal.duration_ms");
  assert
    (Logs.Tag.name Observability.Tag.workflow_type = "temporal.workflow_type");
  assert (Logs.Tag.name Observability.Tag.job_count = "temporal.job_count");
  assert
    (Logs.Tag.name Observability.Tag.command_count = "temporal.command_count");
  assert
    (Logs.Tag.name Observability.Tag.bridge_status = "temporal.bridge_status");
  assert
    (Logs.Tag.name Observability.Tag.error_kind = "temporal.error_kind")

(** Bridge calls report status and non-negative millisecond latency without
    changing their typed result behavior. *)
let test_bridge_events () =
  with_capture (fun () ->
      (match Bridge.check_abi_version 1l with
      | Error { status = Abi_mismatch; _ } -> ()
      | _ -> failwith "ABI mismatch contract changed");
      let failure =
        require_event ~source:"temporal.sdk.bridge" ~level:Logs.Error
          ~operation:"check_abi_version"
      in
      assert
        (Logs.Tag.find Observability.Tag.bridge_status failure.tags
        = Some "abi_mismatch");
      let latency =
        require_event ~source:"temporal.sdk.bridge" ~level:Logs.Debug
          ~operation:"check_abi_version"
      in
      assert
        (match Logs.Tag.find Observability.Tag.duration_ms latency.tags with
        | Some duration -> duration >= 0. && Float.is_finite duration
        | None -> false);
      let runtime =
        match Bridge.runtime_create () with
        | Ok runtime -> runtime
        | Error error -> failwith error.message
      in
      assert (Bridge.runtime_close runtime = Ok ());
      ignore
        (require_event ~source:"temporal.sdk.lifecycle" ~level:Logs.Info
           ~operation:"runtime_create");
      ignore
        (require_event ~source:"temporal.sdk.lifecycle" ~level:Logs.Info
           ~operation:"runtime_close"))

(** A bridge JSON request and raw byte payload are both treated as opaque input:
    returned diagnostics and every emitted record contain only stable metadata,
    never request identifiers or payload contents. *)
let test_bridge_payload_privacy () =
  with_capture (fun () ->
      let secret = "bridge-json-and-payload-secret-7f2c" in
      let echoed = Bridge.echo (Bytes.of_string secret) in
      assert (echoed = Ok (Bytes.of_string secret));
      let runtime =
        match Bridge.runtime_create () with
        | Ok runtime -> runtime
        | Error error -> failwith error.message
      in
      let request =
        `Assoc
          [
            ("request_id", `String ("request-" ^ secret));
            ("namespace", `String "default");
            ("workflow_id", `String ("workflow-" ^ secret));
            ("workflow_type", `String "logging_fixture");
            ("task_queue", `String "default");
            ("input", `List []);
          ]
        |> Yojson.Safe.to_string |> Bytes.of_string
      in
      let error =
        match Bridge.client_start_workflow_json runtime request with
        | Error error -> error
        | Ok _ -> failwith "unconnected bridge unexpectedly started workflow"
      in
      assert (error.status = Bridge.Invalid_state);
      assert (not (contains ~haystack:error.message ~needle:secret));
      assert (Bridge.runtime_close runtime = Ok ());
      assert_not_in_events secret)

(** Workflow processing reports bounded structural metadata and never renders
    raw workflow input. *)
let test_workflow_events_and_privacy () =
  with_capture (fun () ->
      let secret = "payload-do-not-log-4ef315" in
      let execution = Execution.start waiting_workflow secret in
      let commands = Execution.activate execution [ Activation.Start_workflow ] in
      assert
        (commands
        = [
            Activation.Schedule_activity
              {
                seq = 1L;
                activity_id = "ocaml-activity-1";
                activity_type = "wait";
                task_queue = "default";
                arguments = [ payload secret ];
                schedule_to_close_timeout = None;
                schedule_to_start_timeout = None;
                start_to_close_timeout = Some 60_000L;
                heartbeat_timeout = None;
                retry_policy = None;
                priority = None;
                cancellation_type = Activation.Try_cancel;
                do_not_eagerly_execute = false;
              };
          ]);
      let activation =
        require_event ~source:"temporal.sdk.workflow" ~level:Logs.Debug
          ~operation:"activate"
      in
      assert
        (Logs.Tag.find Observability.Tag.workflow_type activation.tags
        = Some "logging_fixture");
      assert
        (Logs.Tag.find Observability.Tag.job_count activation.tags = Some 1);
      assert
        (Logs.Tag.find Observability.Tag.command_count activation.tags = Some 1);
      assert
        (match Logs.Tag.find Observability.Tag.duration_ms activation.tags with
        | Some duration -> duration >= 0. && Float.is_finite duration
        | None -> false);
      ignore
        (require_event ~source:"temporal.sdk.workflow" ~level:Logs.Info
           ~operation:"workflow_started");
      assert_not_in_events secret;
      ignore (Execution.activate execution [ Activation.Start_workflow ]);
      let failure =
        require_event ~source:"temporal.sdk.workflow" ~level:Logs.Error
          ~operation:"workflow_task_failed"
      in
      assert
        (Logs.Tag.find Observability.Tag.error_kind failure.tags = Some "bridge"))

(** Reporter exceptions are swallowed at the common wrapper, including both a
    direct bridge call and workflow activation. *)
let test_reporter_exceptions_are_contained () =
  let reporter = Logs.reporter () in
  let level = Logs.level () in
  Logs.set_reporter raising_reporter;
  Logs.set_level (Some Logs.Debug);
  Fun.protect
    ~finally:(fun () ->
      Logs.set_reporter reporter;
      Logs.set_level level)
    (fun () ->
      (match Bridge.check_abi_version 1l with
      | Error { status = Abi_mismatch; _ } -> ()
      | _ -> failwith "reporter changed bridge behavior");
      let execution = Execution.start waiting_workflow "private-input" in
      match Execution.activate execution [ Activation.Start_workflow ] with
      | [ Activation.Schedule_activity _ ] -> ()
      | _ -> failwith "reporter changed workflow behavior")

(** Application reporters run outside workflow context so re-entrant SDK calls
    cannot append commands or otherwise affect deterministic execution. *)
let test_reporters_cannot_reenter_workflow_context () =
  let reporter = Logs.reporter () in
  let level = Logs.level () in
  reporter_observed_workflow_context := false;
  Logs.set_reporter context_probing_reporter;
  Logs.set_level (Some Logs.Debug);
  Fun.protect
    ~finally:(fun () ->
      Logs.set_reporter reporter;
      Logs.set_level level)
    (fun () ->
      let execution = Execution.start failing_workflow () in
      match Execution.activate execution [ Activation.Start_workflow ] with
      | [ Activation.Fail_workflow _ ] ->
          assert (not !reporter_observed_workflow_context)
      | _ -> failwith "reporter re-entry changed workflow commands")

let () =
  test_source_and_tag_names ();
  test_bridge_events ();
  test_bridge_payload_privacy ();
  test_workflow_events_and_privacy ();
  test_reporter_exceptions_are_contained ();
  test_reporters_cannot_reenter_workflow_context ()
