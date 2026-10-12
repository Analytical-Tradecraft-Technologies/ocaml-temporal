module Activation = Temporal_runtime.Activation
module Execution = Temporal_runtime.Execution
module Context = Temporal_runtime.Workflow_context_store
module Protocol = Temporal_protocol.Workflow_protocol

(** Runtime-level retry policy record used to verify the activation command
    after the public constructor tests have checked the user-facing API. *)
let retry_policy : Activation.retry_policy =
  {
    initial_interval = 1_000L;
    backoff_coefficient_bits = "4609434218613702656";
    maximum_interval = 60_000L;
    maximum_attempts = 4;
    non_retryable_error_types = [ "InvalidInput" ];
  }

(** A policy with millisecond-only intervals verifies that the translator
    preserves nanoseconds instead of rounding retry values to whole seconds.
    The zero attempt count is Temporal's explicit unlimited-retry sentinel. *)
let boundary_retry_policy : Activation.retry_policy =
  {
    initial_interval = 1L;
    backoff_coefficient_bits = "4607182418800017408";
    maximum_interval = 1_001L;
    maximum_attempts = 0;
    non_retryable_error_types = [];
  }

(** Compares the runtime's millisecond retry policy with the protocol's
    protobuf-shaped duration records. These are intentionally different types:
    the runtime keeps exact milliseconds for replay, while the protocol stores
    normalized seconds and nanoseconds for Core. *)
let retry_policy_equal (protocol : Protocol.retry_policy)
    (activation : Activation.retry_policy) =
  let duration_of_milliseconds milliseconds : Protocol.duration =
    Protocol.
      {
        seconds = Int64.div milliseconds 1_000L;
        nanoseconds = Int64.to_int (Int64.rem milliseconds 1_000L) * 1_000_000;
      }
  in
  protocol.initial_interval = duration_of_milliseconds activation.initial_interval
  && String.equal protocol.backoff_coefficient_bits
       activation.backoff_coefficient_bits
  && protocol.maximum_interval = duration_of_milliseconds activation.maximum_interval
  && protocol.maximum_attempts = activation.maximum_attempts
  && protocol.non_retryable_error_types = activation.non_retryable_error_types

(** Encodes one string argument using the same base codec the public activity
    adapter supplies before it reaches the private workflow context. *)
let encode_input input =
  match Temporal_base.Codec.encode Temporal_base.Codec.string input with
  | Ok payload -> payload
  | Error error -> failwith (Temporal_base.Error.message error)

(** Defines a base workflow whose implementation schedules an activity directly
    through the private context; this isolates command translation from the
    public adapter tested by [test_activity_retry_policy]. *)
let make_workflow ?policy name =
  Temporal_base.Definition.make ~name ~input:Temporal_base.Codec.string
    ~output:Temporal_base.Codec.string
    ~implementation:
      (Some (fun input ->
           match Context.current () with
           | None ->
               Error
                 (Temporal_base.Error.defect
                    ~message:"retry-policy test ran outside workflow")
           | Some context ->
               let future, _cancel =
                 Context.schedule_activity context ~name:"retryable_activity"
                   ~input:[ encode_input input ] ?retry_policy:policy
                   ~decode:(Temporal_base.Codec.decode Temporal_base.Codec.string)
                   ()
               in
               Temporal_runtime.Future_store.await future))

(** Asserts the exact private representation emitted for an explicit policy. *)
let test_explicit_policy () =
  match
    Execution.activate
      (Execution.start (make_workflow ~policy:retry_policy "retry_policy") "input")
      [ Activation.Start_workflow ]
  with
  | [
      Activation.Schedule_activity
        {
          retry_policy = Some policy;
          _;
        };
    ] ->
      if policy <> retry_policy then failwith "retry policy was changed"
  | _ -> failwith "explicit retry policy was not emitted"

(** Asserts omitted policy remains an absent optional Core field. *)
let test_omitted_policy () =
  match
    Execution.activate
      (Execution.start (make_workflow "default_retry") "input")
      [ Activation.Start_workflow ]
  with
  | [ Activation.Schedule_activity { retry_policy = None; _ } ] -> ()
  | _ -> failwith "omitted retry policy was changed into a concrete policy"

(** Verifies that callers of the exported one-command translator cannot bypass
    retry-policy validation by avoiding [completion_of_commands]. *)
let test_direct_translation_validation () =
  (* Apply one malformed policy to the same command shape and require the
     translator to reject it before the command can reach JSON encoding. *)
  let expect_rejected label policy =
    let command =
      Activation.Schedule_activity
        {
          seq = 1L;
          activity_id = "activity-1";
          activity_type = "retryable_activity";
          task_queue = "default";
          arguments = [ encode_input "input" ];
          schedule_to_close_timeout = Some 60_000L;
          schedule_to_start_timeout = None;
          start_to_close_timeout = Some 30_000L;
          heartbeat_timeout = None;
          retry_policy = Some policy;
          priority = None;
          cancellation_type = Activation.Try_cancel;
          do_not_eagerly_execute = false;
        }
    in
    match Temporal_runtime.Native_execution.command_to_protocol command with
    | Error _ -> ()
    | Ok _ -> failwith ("direct command translation accepted " ^ label)
  in
  expect_rejected "a malformed coefficient"
    { retry_policy with backoff_coefficient_bits = "0" };
  expect_rejected "an invalid UTF-8 error type"
    {
      retry_policy with
      non_retryable_error_types = [ String.make 1 (Char.chr 0xff) ];
    }

(** Verifies the exact millisecond-to-protobuf conversion for both timeout
    fields and retry intervals.  This catches a boundary regression where
    sub-second values could be truncated while still producing valid JSON. *)
let test_subsecond_boundary_translation () =
  let command =
    Activation.Schedule_activity
      {
        seq = 1L;
        activity_id = "activity-1";
        activity_type = "retryable_activity";
        task_queue = "default";
        arguments = [ encode_input "input" ];
        schedule_to_close_timeout = Some 1_001L;
        schedule_to_start_timeout = Some 1L;
        start_to_close_timeout = Some 2L;
        heartbeat_timeout = Some 3L;
        retry_policy = Some boundary_retry_policy;
        priority = None;
        cancellation_type = Activation.Try_cancel;
        do_not_eagerly_execute = false;
      }
  in
  match Temporal_runtime.Native_execution.command_to_protocol command with
  | Ok
      (Protocol.Schedule_activity
        {
          schedule_to_close_timeout =
            Some { seconds = 1L; nanoseconds = 1_000_000 };
          schedule_to_start_timeout = Some { seconds = 0L; nanoseconds = 1_000_000 };
          start_to_close_timeout = Some { seconds = 0L; nanoseconds = 2_000_000 };
          heartbeat_timeout = Some { seconds = 0L; nanoseconds = 3_000_000 };
          retry_policy = Some policy;
          priority = None;
          _;
        })
    when retry_policy_equal policy boundary_retry_policy -> ()
  | _ -> failwith "sub-second timeout or retry policy precision was lost"

let () =
  test_explicit_policy ();
  test_omitted_policy ();
  test_direct_translation_validation ();
  test_subsecond_boundary_translation ()
