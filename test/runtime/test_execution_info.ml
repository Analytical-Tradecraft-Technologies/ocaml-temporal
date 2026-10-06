(** Tests for the read-only execution metadata exposed by
    [Temporal.Workflow.info], [Temporal.Workflow.is_replaying], and
    [Temporal.Activity.Context.info], and
    [Temporal.Activity.Async_context.info] (#792).

    Workflow scenarios drive real protocol activations through
    [Native_execution.activate], the same adapter the native worker uses, so
    they check that identity comes from Core's initialization job, that
    task-local history facts and the replay flag are replaced on every
    activation, and that live and replayed executions observe the same
    identity. Activity scenarios exercise the public projection of the private
    task metadata, including timestamp and timeout conversion and the
    synthetic-context defects. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Execution = Temporal_runtime.Execution
module Native_execution = Temporal_runtime.Native_execution

(** Fails with the stable translation diagnostic when a result was expected. *)
let unwrap label = function
  | Ok value -> value
  | Error error ->
      let view = Native_execution.error_view error in
      failwith
        (Printf.sprintf "%s: %s at %s (%s)" label view.message view.path
           view.code)

(** Fails with [label] when two values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Builds activation metadata carrying the history facts under test; every
    other field takes its neutral value. *)
let metadata ~history_size_bytes ~continue_as_new_suggested :
    Protocol.activation_metadata =
  {
    available_internal_flags = [];
    history_size_bytes;
    continue_as_new_suggested;
    deployment_version_for_current_task = None;
    last_sdk_version = "test";
    suggest_continue_as_new_reasons =
      (if continue_as_new_suggested then [ Protocol.Too_many_history_events ]
       else []);
    target_worker_deployment_version_changed = false;
  }

(** Wraps jobs in an ordinary activation for the fixed test run. *)
let activation ~is_replaying ~history_length ?metadata jobs : Protocol.activation =
  {
    run_id = "run-info";
    timestamp = Some { seconds = 1L; nanoseconds = 0 };
    is_replaying;
    history_length;
    jobs;
    metadata;
  }

(** The initialization job of a child run continued from an earlier run. *)
let initialize : Protocol.activation_job =
  let context : Protocol.initialize_context =
    {
      headers = [];
      memo = None;
      search_attributes = None;
      workflow_execution_expiration_time = None;
      first_workflow_task_backoff = None;
      identity = "client";
      parent_workflow =
        Some { namespace = "parents"; workflow_id = "parent-1"; run_id = "parent-run" };
      workflow_execution_timeout = None;
      workflow_run_timeout = None;
      workflow_task_timeout = None;
      first_execution_run_id = "first-run";
      start_time = Some { seconds = 1_700_000_000L; nanoseconds = 42 };
      root_workflow = None;
      priority = None;
      retry_policy = None;
      continuation = None;
    }
  in
  Protocol.Initialize_workflow
    {
      workflow_id = "info-1";
      workflow_type = "info_workflow";
      arguments = [];
      randomness_seed = "1";
      attempt = 3;
      context = Some context;
    }

(** One observation taken inside workflow code, kept as plain values so the
    assertions run after the workflow fiber has returned. *)
type observation = {
  identity : string * string * string option * string * string * int;
  namespace : string;
  parent : Temporal.Workflow.Info.parent option;
  start_time : (int64 * int) option;
  snapshot_replaying : bool;
  live_replaying : bool;
  history_length : int;
  history_size_bytes : int option;
  continue_as_new_suggested : bool;
}

(** Reads every public accessor for the current activation. *)
let observe () =
  match Temporal.Workflow.info () with
  | Error error -> failwith (Temporal.Error.message error)
  | Ok info ->
      let module Info = Temporal.Workflow.Info in
      {
        identity =
          ( Info.workflow_id info,
            Info.run_id info,
            Info.first_execution_run_id info,
            Info.workflow_type info,
            Info.task_queue info,
            Info.attempt info );
        namespace = Info.namespace info;
        parent = Info.parent info;
        start_time =
          Option.map
            (fun time -> (Temporal.Time.seconds time, Temporal.Time.nanoseconds time))
            (Info.start_time info);
        snapshot_replaying = Info.is_replaying info;
        live_replaying = Temporal.Workflow.is_replaying ();
        history_length = Info.history_length info;
        history_size_bytes = Info.history_size_bytes info;
        continue_as_new_suggested = Info.continue_as_new_suggested info;
      }

(** Runs one execution through a start activation and a timer activation and
    returns the observations taken before and after the timer. [replaying]
    selects the replay flag of the first activation; the second activation is
    always new progress without metadata, which must clear the first task's
    history facts rather than retain them. *)
let run ~replaying =
  let observations = ref [] in
  let definition =
    Temporal_base.Definition.make ~name:"info_workflow"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:
        (Some
           (fun () ->
             observations := observe () :: !observations;
             match Temporal.Workflow.sleep (Temporal.Duration.of_ms 1L) with
             | Error _ ->
                 Error
                   (Temporal_base.Error.make ~category:`Defect
                      ~message:"timer failed" ())
             | Ok () ->
                 observations := observe () :: !observations;
                 Ok ()))
  in
  let execution =
    Execution.start ~task_queue:"info-queue" ~namespace:"info-namespace"
      definition ()
  in
  let first =
    unwrap "start"
      (Native_execution.activate execution
         (activation ~is_replaying:replaying ~history_length:3L
            ~metadata:
              (metadata ~history_size_bytes:"2048" ~continue_as_new_suggested:true)
            [ initialize ]))
  in
  (match first.commands with
   | [ Protocol.Start_timer _ ] -> ()
   | _ -> failwith "workflow did not suspend on its timer");
  let second =
    unwrap "resume"
      (Native_execution.activate execution
         (activation ~is_replaying:false ~history_length:8L
            [ Protocol.Fire_timer { seq = 1L } ]))
  in
  (match second.commands with
   | [ Protocol.Complete_workflow _ ] -> ()
   | _ -> failwith "workflow did not complete");
  Execution.shutdown execution;
  match List.rev !observations with
  | [ before; after ] -> (before, after)
  | _ -> failwith "workflow did not record two observations"

(** Live and replayed executions see the same run identity, while the replay
    flag and history facts follow each activation. *)
let test_workflow_info () =
  let check ~replaying =
    let before, after = run ~replaying in
    let identity =
      ("info-1", "run-info", Some "first-run", "info_workflow", "info-queue", 3)
    in
    expect "identity" identity before.identity;
    expect "identity after timer" identity after.identity;
    expect "namespace" "info-namespace" before.namespace;
    expect "namespace after timer" "info-namespace" after.namespace;
    expect "parent"
      (Some
         { Temporal.Workflow.Info.namespace = "parents"; workflow_id = "parent-1";
           run_id = "parent-run" })
      before.parent;
    expect "start time" (Some (1_700_000_000L, 42)) before.start_time;
    expect "snapshot replay flag" replaying before.snapshot_replaying;
    expect "live replay flag" replaying before.live_replaying;
    expect "history length" 3 before.history_length;
    expect "history size" (Some 2048) before.history_size_bytes;
    expect "continue-as-new suggestion" true before.continue_as_new_suggested;
    expect "later replay flag" false after.snapshot_replaying;
    expect "later live replay flag" false after.live_replaying;
    expect "later history length" 8 after.history_length;
    expect "later history size" None after.history_size_bytes;
    expect "later continue-as-new suggestion" false
      after.continue_as_new_suggested
  in
  check ~replaying:false;
  check ~replaying:true

(** Detached code has no run: [info] fails closed and nothing is replaying. *)
let test_workflow_info_outside_workflow () =
  (match Temporal.Workflow.info () with
   | Error error -> expect "detached info kind" "defect" (Temporal.Error.kind error)
   | Ok _ -> failwith "detached info unexpectedly succeeded");
  expect "detached replay flag" false (Temporal.Workflow.is_replaying ())

(** A context without an initialization activation reports a defect instead
    of fabricating identity values. *)
let test_workflow_info_synthetic_context () =
  let scheduler = Temporal_runtime.Scheduler.create () in
  let context = Temporal_runtime.Workflow_context_store.create scheduler in
  let observed =
    Temporal_runtime.Workflow_context_store.with_context context Temporal.Workflow.info
  in
  Temporal_runtime.Workflow_context_store.shutdown context;
  match observed with
  | Error error ->
      expect "synthetic info message"
        "Temporal.Workflow.info is unavailable for this execution"
        (Temporal.Error.message error)
  | Ok _ -> failwith "synthetic info unexpectedly succeeded"

(** Activity metadata, including exact timestamps and the scheduling
    workflow, is projected unchanged and survives context invalidation. *)
let test_activity_info () =
  let base : Temporal_base.Activity_context.info =
    {
      namespace = "default";
      workflow_id = "wf";
      workflow_run_id = "run";
      workflow_type = "orders";
      activity_id = "charge";
      activity_type = "charge_card";
      attempt = 4;
      is_local = true;
      scheduled_time = Some { seconds = 10L; nanoseconds = 5 };
      current_attempt_scheduled_time = Some { seconds = 20L; nanoseconds = 0 };
      started_time = None;
      schedule_to_close_timeout = Some (Temporal_base.Duration.of_ms 90_000L);
      start_to_close_timeout = Some (Temporal_base.Duration.of_ms 30_001L);
      task_heartbeat_timeout = None;
    }
  in
  let make info =
    Temporal_base.Activity_context.create_with_info ~info
      ~heartbeat:(fun _ -> Ok ()) ~details:[] ~heartbeat_timeout:None
  in
  let context = make base in
  Temporal_base.Activity_context.invalidate context;
  let module Info = Temporal.Activity.Info in
  let seconds time = Option.map Temporal.Time.seconds time in
  (match Temporal.Activity.Context.info context with
   | Error error -> failwith (Temporal.Error.message error)
   | Ok info ->
       expect "activity namespace" "default" (Info.namespace info);
       expect "activity workflow"
         { Info.workflow_id = "wf"; run_id = "run"; workflow_type = "orders" }
         (Info.workflow info);
       expect "activity id" "charge" (Info.activity_id info);
       expect "activity type" "charge_card" (Info.activity_type info);
       expect "activity attempt" 4 (Info.attempt info);
       expect "activity local" true (Info.is_local info);
       expect "scheduled seconds" (Some 10L) (seconds (Info.scheduled_time info));
       expect "scheduled nanoseconds" (Some 5)
         (Option.map Temporal.Time.nanoseconds (Info.scheduled_time info));
       expect "attempt scheduled seconds" (Some 20L)
         (seconds (Info.current_attempt_scheduled_time info));
       expect "started time" None (seconds (Info.started_time info));
       let ms = Option.map Temporal.Duration.to_ms in
       expect "schedule-to-close timeout" (Some 90_000L)
         (ms (Info.schedule_to_close_timeout info));
       expect "start-to-close timeout" (Some 30_001L)
         (ms (Info.start_to_close_timeout info));
       expect "heartbeat timeout" None (ms (Info.heartbeat_timeout info)));
  match
    Temporal.Activity.Context.info
      (Temporal_base.Activity_context.unavailable ~details:[] ~heartbeat_timeout:None)
  with
  | Error error -> expect "synthetic activity kind" "defect" (Temporal.Error.kind error)
  | Ok _ -> failwith "synthetic activity info unexpectedly succeeded"

(** An asynchronous context carries the metadata it was built with, and one
    built without a Core task reports a defect rather than empty values. The
    native adapter path is covered by [test_native_async_activity]. *)
let test_async_context_info () =
  let handle () =
    Temporal_base.Async_activity.create
      ~submit:(fun _ -> Ok ())
      ~encode_output:(fun () -> Ok (Temporal_base.Payload.unit_null ()))
  in
  let info : Temporal_base.Activity_context.info =
    {
      namespace = "async-namespace";
      workflow_id = "wf";
      workflow_run_id = "run";
      workflow_type = "orders";
      activity_id = "ship";
      activity_type = "ship_order";
      attempt = 1;
      is_local = false;
      scheduled_time = None;
      current_attempt_scheduled_time = None;
      started_time = None;
      schedule_to_close_timeout = None;
      start_to_close_timeout = None;
      task_heartbeat_timeout = Some (Temporal_base.Duration.of_ms 2_000L);
    }
  in
  let module Info = Temporal.Activity.Info in
  (match
     Temporal.Activity.Async_context.info
       (Temporal_base.Async_activity.context ~info (handle ()))
   with
   | Error error -> failwith (Temporal.Error.message error)
   | Ok info ->
       expect "async namespace" "async-namespace" (Info.namespace info);
       expect "async activity id" "ship" (Info.activity_id info);
       expect "async heartbeat timeout" (Some 2_000L)
         (Option.map Temporal.Duration.to_ms (Info.heartbeat_timeout info)));
  match
    Temporal.Activity.Async_context.info
      (Temporal_base.Async_activity.context (handle ()))
  with
  | Error error -> expect "synthetic async kind" "defect" (Temporal.Error.kind error)
  | Ok _ -> failwith "synthetic async info unexpectedly succeeded"

(** Runs every execution-info scenario as one dune test executable. *)
let () =
  test_workflow_info ();
  test_workflow_info_outside_workflow ();
  test_workflow_info_synthetic_context ();
  test_activity_info ();
  test_async_context_info ()
