module Activation = Temporal_runtime.Activation
module Protocol = Temporal_protocol.Workflow_protocol
module Raw_execution = Temporal_runtime.Execution
module Future_store = Temporal_runtime.Future_store
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Copies a public payload into the private representation expected by the
    low-level execution fixture. The production adapter performs the same
    conversion before invoking Core; keeping it explicit here prevents this
    private-runtime test from relying on public type aliases. *)
let base_payload (payload : Temporal.Payload.t) : Temporal_base.Payload.t =
  {
    Temporal_base.Payload.metadata = List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Copies a private payload back into the public representation for codec
    callbacks used by the test adapter. *)
let public_payload (payload : Temporal_base.Payload.t) : Temporal.Payload.t =
  {
    Temporal.Payload.metadata = List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Converts a public structured error into the base error type used by the
    private execution module, including a fresh copy of every detail payload. *)
let base_error (error : Temporal.Error.t) : Temporal_base.Error.t =
  let view = Temporal.Error.view error in
  Temporal_base.Error.make ~non_retryable:view.non_retryable
    ~details:(List.map base_payload view.details) ~category:view.category
    ~message:view.message ()

(** Presents a private-runtime error through the public view helpers for
    assertions. Activation and context-store APIs intentionally expose only
    [Temporal_base.Error.t] to these tests; this copy keeps their checks from
    depending on a public/private type alias. *)
let public_error (error : Temporal_base.Error.t) : Temporal.Error.t =
  let view = Temporal_base.Error.view error in
  Temporal.Error.make ~non_retryable:view.non_retryable
    ~details:(List.map public_payload view.details) ~category:view.category
    ~message:view.message ()

(** Returns the stable public category label for a private-runtime error. *)
let public_error_kind error = Temporal.Error.kind (public_error error)

(** Returns the public diagnostic message for a private-runtime error. *)
let public_error_message error = Temporal.Error.message (public_error error)

(** Adapts an opaque public codec to a base codec for this low-level test. The
    payload-level constructor preserves value-dependent encoding metadata. *)
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

(** Rebuilds a public workflow as the base definition accepted by the private
    execution state machine. This is deliberately a test-only conversion: the
    installed package exposes only the opaque public definition. *)
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

(** Presents the private execution module with a test-friendly [start]
    wrapper that performs the explicit public-to-base conversion above. *)
module Execution = struct
  include Raw_execution

  let start ?task_queue definition input =
    Raw_execution.start ?task_queue (base_workflow definition) input
end

(** Builds a string payload through the public codec for activation fixtures. *)
let payload value =
  match Temporal.Codec.encode Temporal.Codec.string value with
  | Ok payload -> base_payload payload
  | Error error -> failwith (Temporal.Error.message error)

(** Simulates Core's successful child-start acknowledgment. The run ID is an
    internal lifecycle fact; tests use a stable value while the public future
    remains pending until the terminal child result arrives. *)
let child_started seq =
  Activation.Resolve_child_workflow_start { seq; result = Ok "child-run" }

(** Remote activity called by the greeting workflow fixture. *)
let greeting =
  Temporal.Activity.remote ~name:"greeting" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Workflow fixture that schedules an activity, waits, sleeps, and completes. *)
let workflow input =
  let open Temporal.Result_syntax in
  assert (Temporal.Workflow_context.is_active ());
  let pending = Temporal.Activity.start greeting input in
  let* greeting = Temporal.Future.await pending in
  let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 10L) in
  Ok (greeting ^ "!")

(** Typed definition for the main activation scenario. *)
let greeting_workflow =
  Temporal.Workflow.define ~name:"greeting_workflow"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string workflow

(** Remote child definition used to verify that a parent command carries the
    explicit durable child ID, workflow type, encoded input, and typed output
    codec without exposing runtime sequence numbers to application code. *)
let greeting_child =
  Temporal.Workflow.remote ~name:"greeting_child" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** A validated policy used by the child retry regression.  The policy is
    intentionally expressed through the public activity module because child
    and activity retries share Temporal Core's wire representation. *)
let child_retry_policy =
  match
    Temporal.Activity.Retry_policy.make
      ~initial_interval:(Temporal.Duration.of_ms 1_000L)
      ~backoff_coefficient:2.0
      ~maximum_interval:(Temporal.Duration.of_ms 5_000L)
      ~maximum_attempts:3 ~non_retryable_error_types:[ "InvalidInput" ] ()
  with
  | Ok policy -> policy
  | Error error -> failwith (Temporal.Error.message error)

(** Parent fixture that delegates retry scheduling to Core.  No OCaml retry
    loop is involved, which keeps the operation deterministic during replay. *)
let retrying_child_parent_workflow =
  Temporal.Workflow.define ~name:"retrying_child_parent"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
      Temporal.Child_workflow.execute ~retry_policy:child_retry_policy
        ~id:"retrying-child" greeting_child "Ada")

(** Parent fixture that exercises the direct-style child convenience function.
    The child call suspends this workflow until the matching resolution job is
    delivered by the synthetic Core boundary. *)
let child_parent_workflow =
  Temporal.Workflow.define ~name:"child_parent" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string (fun input ->
      Temporal.Child_workflow.execute ~id:"greeting/Ada" greeting_child input)

(** Compares expected activation values with a labelled failure message. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Parent fixture that retains a child handle, requests cancellation before
    the start acknowledgment, and then waits for Core's typed cancellation
    result. Calling [cancel] before and after resolution verifies that one
    handle emits only one durable command even when application code retries
    the request. *)
let cancelling_child_parent_workflow =
  Temporal.Workflow.define ~name:"cancelling_child_parent"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
      let handle =
        Temporal.Child_workflow.start_handle
          ~cancellation_type:
            Temporal.Child_workflow.Wait_cancellation_requested
          ~id:"cancel-me" greeting_child "Ada"
      in
      match
        ( Temporal.Child_workflow.cancel ~reason:"no longer needed" handle,
          Temporal.Child_workflow.cancel ~reason:"retry request" handle )
      with
      | Ok (), Ok () -> (
          match Temporal.Future.await (Temporal.Child_workflow.future handle) with
          | Ok _ ->
              Error
                (Temporal.Error.defect
                   ~message:"cancelled child unexpectedly completed")
          | Error error -> (
              match Temporal.Child_workflow.cancel handle with
              | Ok () -> Ok (Temporal.Error.kind error)
              | Error cancel_error -> Error cancel_error))
      | Error error, _ | _, Error error -> Error error)

(** Parent fixture that retains an activity handle, requests cancellation before
    the activity result arrives, and then observes the typed cancellation
    failure. Repeating [cancel] before and after resolution verifies that the
    handle emits one request for its private activity sequence. *)
let cancelling_activity_parent_workflow =
  Temporal.Workflow.define ~name:"cancelling_activity_parent"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
      let handle =
        Temporal.Activity.start_handle
          ~cancellation_type:Temporal.Activity.Wait_cancellation_completed
          greeting "Ada"
      in
      match (Temporal.Activity.cancel handle, Temporal.Activity.cancel handle) with
      | Ok (), Ok () -> (
          match Temporal.Future.await (Temporal.Activity.future handle) with
          | Ok _ ->
              Error
                (Temporal.Error.defect
                   ~message:"cancelled activity unexpectedly completed")
          | Error error -> (
              match Temporal.Activity.cancel handle with
              | Ok () -> Ok (Temporal.Error.kind error)
              | Error cancel_error -> Error cancel_error))
      | Error error, _ | _, Error error -> Error error)

(** Verifies explicit child policy, cancellation command ordering, idempotence,
    and typed cancellation resolution without relying on a live server. *)
let test_child_workflow_cancellation () =
  let execution = Execution.start cancelling_child_parent_workflow () in
  expect "child cancellation commands"
    [
      Activation.Start_child_workflow
        {
          seq = 1L;
          id = "cancel-me";
          name = "greeting_child";
          input = payload "Ada";
          retry_policy = None;
          cancellation_type = Activation.Child_wait_cancellation_requested;
        };
      Activation.Cancel_child_workflow { seq = 1L; reason = "no longer needed" };
    ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "cancelled child resolves parent"
    [ Activation.Complete_workflow (payload "cancelled") ]
    (Execution.activate execution
       [
         child_started 1L;
         Activation.Resolve_child_workflow
           {
             seq = 1L;
             result =
               Error
                 (Temporal_base.Error.make ~category:`Cancelled
                    ~message:"child cancellation acknowledged" ());
           };
       ])

(** Verifies the public activity handle emits one request-cancel command in
    source order and resumes its parent only after the typed terminal result.
    Core's activity cancellation command carries only the private sequence, so
    no user reason can be accidentally dropped at this boundary. *)
let test_activity_cancellation () =
  let execution = Execution.start cancelling_activity_parent_workflow () in
  expect "activity cancellation commands"
    [ Activation.Schedule_activity
        {
          seq = 1L;
          activity_id = "ocaml-activity-1";
          activity_type = "greeting";
          task_queue = "default";
          arguments = [ payload "Ada" ];
          schedule_to_close_timeout = None;
          schedule_to_start_timeout = None;
          start_to_close_timeout = Some 60_000L;
          heartbeat_timeout = None;
          retry_policy = None;
          priority = None;
          cancellation_type = Activation.Wait_cancellation_completed;
          do_not_eagerly_execute = false;
        };
      Activation.Request_cancel_activity { seq = 1L } ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "cancelled activity resolves parent"
    [ Activation.Complete_workflow (payload "cancelled") ]
    (Execution.activate execution
       [ Activation.Resolve_activity
           {
             seq = 1L;
             result =
               Error
                 (Temporal_base.Error.make ~category:`Cancelled
                    ~message:"activity cancellation acknowledged" ());
           } ])

(** Parent fixture that uses the public cancellation default rather than
    choosing a policy explicitly. The command assertion below is the
    regression guard for the public API: a handle that exposes [cancel] must
    request child cancellation by default, not silently abandon the child. *)
let default_cancelling_child_parent_workflow =
  Temporal.Workflow.define ~name:"default_cancelling_child_parent"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
      let handle =
        Temporal.Child_workflow.start_handle ~id:"default-cancel-me"
          greeting_child "Ada"
      in
      match Temporal.Child_workflow.cancel handle with
      | Error error -> Error error
      | Ok () -> (
          match Temporal.Future.await (Temporal.Child_workflow.future handle) with
          | Ok _ ->
              Error
                (Temporal.Error.defect
                   ~message:"default-cancelled child unexpectedly completed")
          | Error error -> Ok (Temporal.Error.kind error)))

(** Confirms that omitting [cancellation_type] still emits [Try_cancel] and
    that the public default reason crosses the deterministic command boundary
    unchanged. *)
let test_default_child_workflow_cancellation_policy () =
  let execution = Execution.start default_cancelling_child_parent_workflow () in
  expect "default child cancellation commands"
    [
      Activation.Start_child_workflow
        {
          seq = 1L;
          id = "default-cancel-me";
          name = "greeting_child";
          input = payload "Ada";
          retry_policy = None;
          cancellation_type = Activation.Child_try_cancel;
        };
      Activation.Cancel_child_workflow
        { seq = 1L; reason = "cancelled by workflow" };
    ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "default cancellation resolves parent"
    [ Activation.Complete_workflow (payload "cancelled") ]
    (Execution.activate execution
       [
         child_started 1L;
         Activation.Resolve_child_workflow
           {
             seq = 1L;
             result =
               Error
                 (Temporal_base.Error.make ~category:`Cancelled
                    ~message:"child cancellation acknowledged" ());
           };
       ])

(** Exercises the full synthetic start/activity/timer/completion sequence. *)
let test_commands_and_completion () =
  let execution = Execution.start greeting_workflow "Ada" in
  expect "activity command"
    [
      Activation.Schedule_activity
        {
          seq = 1L;
          activity_id = "ocaml-activity-1";
          activity_type = "greeting";
          task_queue = "default";
          arguments = [ payload "Ada" ];
          schedule_to_close_timeout = None;
          schedule_to_start_timeout = None;
          start_to_close_timeout = Some 60_000L;
          heartbeat_timeout = None;
          retry_policy = None;
          priority = None;
          cancellation_type = Activation.Try_cancel;
          do_not_eagerly_execute = false;
        };
    ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "timer command"
    [ Activation.Start_timer { seq = 2L; milliseconds = 10L } ]
    (Execution.activate execution
       [
         Activation.Resolve_activity
           { seq = 1L; result = Ok (payload "Hello Ada") };
       ]);
  expect "workflow complete"
    [ Activation.Complete_workflow (payload "Hello Ada!") ]
    (Execution.activate execution [ Activation.Fire_timer { seq = 2L } ]);
  expect "completion emitted once" [] (Execution.activate execution [])

(** Explicit activity labels survive command construction, while an execution's
    configured worker queue supplies the default for ordinary [Activity.start]
    calls. *)
let test_activity_options_and_queue () =
  let explicit_workflow =
    Temporal.Workflow.define ~name:"explicit_activity_options"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
        Temporal.Activity.start ~activity_id:"lookup-42" ~task_queue:"fast-lane"
          ~schedule_to_close_timeout:(Temporal.Duration.of_ms 5_000L)
          ~heartbeat_timeout:(Temporal.Duration.of_ms 1_000L)
          ~cancellation_type:Temporal.Activity.Wait_cancellation_completed
          ~do_not_eagerly_execute:true greeting input
        |> Temporal.Future.await)
  in
  let explicit = Execution.start ~task_queue:"worker-default" explicit_workflow "Ada" in
  begin match Execution.activate explicit [ Activation.Start_workflow ] with
  | [
      Activation.Schedule_activity
        {
          seq = 1L;
          activity_id = "lookup-42";
          activity_type = "greeting";
          task_queue = "fast-lane";
          arguments = [ argument ];
          schedule_to_close_timeout = Some 5_000L;
          schedule_to_start_timeout = None;
          start_to_close_timeout = None;
          heartbeat_timeout = Some 1_000L;
          retry_policy = None;
          priority = None;
          cancellation_type = Activation.Wait_cancellation_completed;
          do_not_eagerly_execute = true;
        };
    ] when argument = payload "Ada" -> ()
  | _ -> failwith "explicit activity options were not preserved"
  end;
  let defaulted = Execution.start ~task_queue:"worker-default" greeting_workflow "Ada" in
  begin match Execution.activate defaulted [ Activation.Start_workflow ] with
  | [ Activation.Schedule_activity { task_queue = "worker-default"; _ } ] -> ()
  | _ -> failwith "execution task queue was not used as activity default"
  end;
  let start_only_workflow =
    Temporal.Workflow.define ~name:"start_only_activity_options"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
        Temporal.Activity.start
          ~schedule_to_start_timeout:(Temporal.Duration.of_ms 2_000L)
          greeting input
        |> Temporal.Future.await)
  in
  let start_only = Execution.start start_only_workflow "Ada" in
  begin match Execution.activate start_only [ Activation.Start_workflow ] with
  | [
      Activation.Schedule_activity
        {
          schedule_to_start_timeout = Some 2_000L;
          start_to_close_timeout = Some 60_000L;
          _;
        };
    ] -> ()
  | _ -> failwith "activity default timeout was not applied with schedule-to-start"
  end

(** A defect must discard every workflow command and poison only the local
    cache generation. Returning its error keeps category assertions precise. *)
let require_task_failure execution commands =
  if commands <> [] then failwith "failed task retained workflow commands";
  match Raw_execution.task_failure execution with
  | Some error -> error
  | None -> failwith "expected a failed workflow task"

(** Invalid optional activity identity is returned through the workflow's
    typed failure path and does not emit a schedule command. *)
let test_invalid_activity_options_do_not_schedule () =
  let invalid_workflow =
    Temporal.Workflow.define ~name:"invalid_activity_options"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Activity.start ~task_queue:"" greeting "ignored"
        |> Temporal.Future.await
        |> Result.map (fun _ -> ()))
  in
  let execution = Execution.start invalid_workflow () in
  match Execution.activate execution [ Activation.Start_workflow ] with
  | [] ->
      let error = require_task_failure execution [] in
      expect "invalid activity option task failure" "defect" (public_error_kind error)
  | [ Activation.Schedule_activity _ ] ->
      failwith "invalid activity queue emitted a schedule command"
  | _ -> failwith "invalid activity options did not fail through the workflow"

(** Rejects malformed worker defaults while the execution context is created.
    An omitted activity queue must never defer this configuration error until a
    later command translation step, after a future and sequence were created. *)
let test_invalid_default_task_queue () =
  let expect_invalid label task_queue =
    try
      ignore (Execution.start ~task_queue greeting_workflow "Ada");
      failwith (label ^ " default task queue was accepted")
    with
    | Invalid_argument message ->
        if String.equal message "" then failwith (label ^ " had no diagnostic")
  in
  expect_invalid "empty" "";
  expect_invalid "NUL" "bad\000queue";
  expect_invalid "oversized" (String.make 65_537 'x');
  expect_invalid "UTF-8" (String.make 1 (Char.chr 0xff))

(** Runs the greeting fixture and returns all emitted command batches for replay
    comparison. *)
let run_greeting () =
  let execution = Execution.start greeting_workflow "Ada" in
  [
    Execution.activate execution [ Activation.Start_workflow ];
    Execution.activate execution
      [
        Activation.Resolve_activity
          { seq = 1L; result = Ok (payload "Hello Ada") };
      ];
    Execution.activate execution [ Activation.Fire_timer { seq = 2L } ];
  ]

(** Confirms identical inputs and activation jobs produce identical commands. *)
let test_replay_is_stable () =
  expect "replay command bytes" (run_greeting ()) (run_greeting ())

(** Creates one named activity used to observe result-delivery order. *)
let ordering_activity name =
  Temporal.Activity.remote ~name ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Workflow fixture whose output records the order in which two results resume
    their fibers. *)
let ordered_workflow =
  Temporal.Workflow.define ~name:"ordered" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      let observed = ref [] in
      let first = Temporal.Activity.start (ordering_activity "first") "first" in
      let second = Temporal.Activity.start (ordering_activity "second") "second" in
      let first =
        Temporal.Future.map
          (fun value ->
            observed := value :: !observed;
            value)
          first
      in
      let second =
        Temporal.Future.map
          (fun value ->
            observed := value :: !observed;
            value)
          second
      in
      match Temporal.Future.await (Temporal.Future.both first second) with
      | Error error -> Error error
      | Ok _ -> Ok (String.concat "," (List.rev !observed)))

(** Applies a chosen result-job order and checks the resulting workflow output. *)
let resolve_order jobs expected =
  let execution = Execution.start ordered_workflow () in
  ignore (Execution.activate execution [ Activation.Start_workflow ]);
  expect "explicit resolution order"
    [ Activation.Complete_workflow (payload expected) ]
    (Execution.activate execution jobs)

(** Confirms activation job order determines runnable-fiber order. *)
let test_resolution_job_order () =
  let first =
    Activation.Resolve_activity { seq = 1L; result = Ok (payload "first") }
  in
  let second =
    Activation.Resolve_activity { seq = 2L; result = Ok (payload "second") }
  in
  resolve_order [ second; first ] "second,first";
  resolve_order [ first; second ] "first,second"

(** Confirms duplicate starts and unknown activity/timer sequences fail as
    non-retryable bridge defects. *)
let test_bridge_defects () =
  let execution = Execution.start greeting_workflow "Ada" in
  ignore (Execution.activate execution [ Activation.Start_workflow ]);
  (match
     Execution.activate execution
       [ Activation.Fire_timer { seq = 999L } ]
   with
  | [] ->
      let error = require_task_failure execution [] in
      expect "bridge category" "bridge" (public_error_kind error)
  | _ -> failwith "unknown timer did not fail the workflow");
  let duplicate = Execution.start greeting_workflow "Ada" in
  ignore (Execution.activate duplicate [ Activation.Start_workflow ]);
  ignore
    (Execution.activate duplicate
       [
         Activation.Resolve_activity
           { seq = 1L; result = Ok (payload "Hello Ada") };
       ]);
  match
    Execution.activate duplicate
      [
        Activation.Resolve_activity
          { seq = 1L; result = Ok (payload "Hello again") };
      ]
  with
  | [] ->
      let error = require_task_failure duplicate [] in
      expect "duplicate category" "bridge" (public_error_kind error)
  | _ -> failwith "duplicate activity resolution was accepted"

(** Workflow fixture proving that a zero-duration sleep emits no timer. *)
let zero_sleep_workflow =
  Temporal.Workflow.define ~name:"zero_sleep" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.unit (fun () ->
      match Temporal.Workflow.sleep (Temporal.Duration.of_ms 0L) with
      | Ok () -> Ok ()
      | Error error -> Error error)

(** Workflow fixture that proves timers can be scheduled independently before
    the workflow chooses which one to await. *)
let concurrent_sleep_workflow =
  Temporal.Workflow.define ~name:"concurrent_sleep" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      let first =
        Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 5L)
      in
      let _second =
        Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 10L)
      in
      match Temporal.Future.await first with
      | Ok () -> Ok "first fired"
      | Error error -> Error error)

(** Workflow fixture that inspects the zero-duration future without waiting,
    proving it is immediately ready and history-neutral. *)
let zero_start_sleep_workflow =
  Temporal.Workflow.define ~name:"zero_start_sleep" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.unit (fun () ->
      let timer =
        Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 0L)
      in
      if Temporal.Future.is_ready timer then Temporal.Future.await timer
      else Error (Temporal.Error.defect ~message:"zero timer was pending"))

(** Workflow fixture combining the identity value [all []] with a normal timer.
    The empty aggregate must inherit this execution's scheduler ownership so it
    composes without a false cross-execution defect. *)
let empty_all_with_timer_workflow =
  Temporal.Workflow.define ~name:"empty_all_with_timer"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
      let empty = Temporal.Future.all [] in
      let timer =
        Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 1L)
      in
      match Temporal.Future.await (Temporal.Future.both empty timer) with
      | Ok ([], ()) -> Ok ()
      | Ok (_ :: _, ()) ->
          Error (Temporal.Error.defect ~message:"empty all returned a value")
      | Error error -> Error error)

(** Covers zero sleep and rejection of negative durations. *)
let test_zero_sleep_and_duration_validation () =
  let execution = Execution.start zero_sleep_workflow () in
  expect "zero sleep has no timer"
    [
      Activation.Complete_workflow
        (match Temporal.Codec.encode Temporal.Codec.unit () with
        | Ok payload -> base_payload payload
        | Error error -> failwith (Temporal.Error.message error));
    ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  match Temporal.Duration.of_ms (-1L) with
  | exception Invalid_argument _ -> ()
  | _ -> failwith "negative duration accepted"

(** Covers non-blocking timer creation, command order, selected waiting, and a
    ready zero-duration timer that emits no history command. *)
let test_start_sleep () =
  let concurrent = Execution.start concurrent_sleep_workflow () in
  expect "two timers start before waiting"
    [
      Activation.Start_timer { seq = 1L; milliseconds = 5L };
      Activation.Start_timer { seq = 2L; milliseconds = 10L };
    ]
    (Execution.activate concurrent [ Activation.Start_workflow ]);
  expect "awaited timer completes workflow"
    [ Activation.Complete_workflow (payload "first fired") ]
    (Execution.activate concurrent [ Activation.Fire_timer { seq = 1L } ]);
  let zero = Execution.start zero_start_sleep_workflow () in
  expect "zero start sleep has no command"
    [
      Activation.Complete_workflow
        (match Temporal.Codec.encode Temporal.Codec.unit () with
        | Ok payload -> base_payload payload
        | Error error -> failwith (Temporal.Error.message error));
    ]
    (Execution.activate zero [ Activation.Start_workflow ]);
  match
    Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 1L)
    |> Temporal.Future.peek
  with
  | Some (Error error) ->
      expect "detached start sleep" "defect" (Temporal.Error.kind error)
  | Some (Ok ()) | None -> failwith "detached start sleep did not fail immediately"

(** Proves an empty aggregate created inside a workflow is owned by that
    workflow and can be paired with an ordinary pending operation. *)
let test_empty_all_uses_current_workflow_owner () =
  let execution = Execution.start empty_all_with_timer_workflow () in
  expect "empty all composes with timer"
    [ Activation.Start_timer { seq = 1L; milliseconds = 1L } ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "empty all and timer complete"
    [
      Activation.Complete_workflow
        (match Temporal.Codec.encode Temporal.Codec.unit () with
        | Ok payload -> base_payload payload
        | Error error -> failwith (Temporal.Error.message error));
    ]
    (Execution.activate execution [ Activation.Fire_timer { seq = 1L } ])

(** Covers the child schedule/resolution path and proves the application-owned
    ID is emitted unchanged as durable command data. *)
let test_child_workflow_completion () =
  let execution = Execution.start child_parent_workflow "Ada" in
  expect "child schedule command"
    [
      Activation.Start_child_workflow
        {
          seq = 1L;
          id = "greeting/Ada";
          name = "greeting_child";
          input = payload "Ada";
          retry_policy = None;
          cancellation_type = Activation.Child_try_cancel;
        };
    ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "child result completes parent"
    [ Activation.Complete_workflow (payload "Hello Ada") ]
    (Execution.activate execution
       [
         child_started 1L;
         Activation.Resolve_child_workflow
           { seq = 1L; result = Ok (payload "Hello Ada") };
       ])

(** Verifies the public child retry policy is copied into the private command
    without exposing bridge-specific fields to workflow authors.  The Core
    worker, rather than this OCaml activation loop, owns subsequent retries. *)
let test_child_workflow_retry_policy () =
  let execution = Execution.start retrying_child_parent_workflow () in
  expect "child retry policy command"
    [
      Activation.Start_child_workflow
        {
          seq = 1L;
          id = "retrying-child";
          name = "greeting_child";
          input = payload "Ada";
          retry_policy =
            Some
              {
                Activation.initial_interval = 1_000L;
                backoff_coefficient_bits = "4611686018427387904";
                maximum_interval = 5_000L;
                maximum_attempts = 3;
                non_retryable_error_types = [ "InvalidInput" ];
              };
          cancellation_type = Activation.Child_try_cancel;
        };
    ]
    (Execution.activate execution [ Activation.Start_workflow ])

(** Parent fixture that starts a child and activity before awaiting either.
    Their shared sequence space and source order must be replay-stable. *)
let concurrent_child_parent =
  Temporal.Workflow.define ~name:"concurrent_child_parent"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
      let child =
        Temporal.Child_workflow.start ~id:"child-1" greeting_child "Ada"
      in
      let activity = Temporal.Activity.start greeting "Grace" in
      match Temporal.Future.await (Temporal.Future.both child activity) with
      | Ok (child, activity) -> Ok (child ^ "," ^ activity)
      | Error error -> Error error)

(** Verifies children and activities can be started concurrently and that
    child output decoding happens before the typed future is resolved. *)
let test_child_workflow_concurrency_and_decoding () =
  let execution = Execution.start concurrent_child_parent () in
  expect "child and activity command order"
    [
      Activation.Start_child_workflow
        {
          seq = 1L;
          id = "child-1";
          name = "greeting_child";
          input = payload "Ada";
          retry_policy = None;
          cancellation_type = Activation.Child_try_cancel;
        };
      Activation.Schedule_activity
        {
          seq = 2L;
          activity_id = "ocaml-activity-2";
          activity_type = "greeting";
          task_queue = "default";
          arguments = [ payload "Grace" ];
          schedule_to_close_timeout = None;
          schedule_to_start_timeout = None;
          start_to_close_timeout = Some 60_000L;
          heartbeat_timeout = None;
          retry_policy = None;
          priority = None;
          cancellation_type = Activation.Try_cancel;
          do_not_eagerly_execute = false;
        };
    ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "one concurrent result leaves parent pending" []
    (Execution.activate execution
       [
         child_started 1L;
         Activation.Resolve_child_workflow
           { seq = 1L; result = Ok (payload "Hello Ada") };
       ]);
  expect "both child and activity results complete parent"
    [ Activation.Complete_workflow (payload "Hello Ada,Hello Grace") ]
    (Execution.activate execution
       [
         Activation.Resolve_activity
           { seq = 2L; result = Ok (payload "Hello Grace") };
       ]);
  let invalid = Execution.start child_parent_workflow "Ada" in
  ignore (Execution.activate invalid [ Activation.Start_workflow ]);
  match
    Execution.activate invalid
      [
        child_started 1L;
        Activation.Resolve_child_workflow
          {
            seq = 1L;
            result =
              Ok
                {
                  Temporal_base.Payload.metadata = [ ("encoding", "binary/plain") ];
                  data = Bytes.of_string "wrong codec";
                };
          };
      ]
  with
  | [] ->
      let error = require_task_failure invalid [] in
      expect "child output codec failure" "codec" (public_error_kind error)
  | _ -> failwith "invalid child output did not fail the parent"

(** Codec fixture that rejects every input, used to prove encoding happens
    before command sequence allocation or mutation of workflow history. *)
let rejecting_child_input =
  Temporal.Codec.make ~encoding:"test/reject"
    ~encode:(fun _ -> Error (Temporal.Error.codec ~message:"input rejected"))
    ~decode:(fun _ -> Ok ())

(** Checks that validation returned a ready defect with the expected message,
    allowing the fixture to continue and prove a later valid child receives
    sequence one. *)
let require_ready_child_id_error expected future =
  match Temporal.Future.peek future with
  | Some (Error error) when String.equal (Temporal.Error.message error) expected ->
      Ok ()
  | Some (Error error) ->
      Error
        (Temporal.Error.defect
           ~message:
             (Printf.sprintf "unexpected child ID error: %s"
                (Temporal.Error.message error)))
  | Some (Ok _) ->
      Error (Temporal.Error.defect ~message:"invalid child ID succeeded")
  | None ->
      Error (Temporal.Error.defect ~message:"invalid child ID remained pending")

(** Rejects IDs that cannot cross the strict JSON boundary, then starts one
    valid child. This makes sequence allocation observable after both failures. *)
let child_id_boundary_workflow =
  Temporal.Workflow.define ~name:"child_id_boundary" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      let open Temporal.Result_syntax in
      let oversized = String.make 65_537 'x' in
      let* () =
        Temporal.Child_workflow.start ~id:oversized greeting_child "oversized"
        |> require_ready_child_id_error
             "child workflow id exceeds 65536 UTF-8 bytes"
      in
      let invalid_utf_8 = String.make 1 (Char.chr 0xff) in
      let* () =
        Temporal.Child_workflow.start ~id:invalid_utf_8 greeting_child "invalid"
        |> require_ready_child_id_error
             "child workflow id must be valid UTF-8"
      in
      Temporal.Child_workflow.execute ~id:"after-invalid" greeting_child "Ada")

(** Invalid IDs and codec inputs are expected failures. They produce no child
    command, and detached calls return a ready typed defect rather than raising
    or performing a private suspension effect. *)
let test_child_workflow_validation () =
  let empty_id =
    Temporal.Workflow.define ~name:"empty_child_id" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () ->
        let target =
          Temporal.Workflow.remote ~name:"child" ~input:Temporal.Codec.unit
            ~output:Temporal.Codec.unit
        in
        Temporal.Child_workflow.execute ~id:"" target ())
  in
  let empty_execution = Execution.start empty_id () in
  (match Execution.activate empty_execution [ Activation.Start_workflow ] with
  | [] ->
      let error = require_task_failure empty_execution [] in
      expect "empty child ID kind" "defect" (public_error_kind error);
      expect "empty child ID message" "child workflow id must not be empty"
        (public_error_message error)
  | _ -> failwith "empty child ID emitted a command or succeeded");
  let invalid_input =
    Temporal.Workflow.define ~name:"invalid_child_input"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let target =
          Temporal.Workflow.remote ~name:"rejecting_child"
            ~input:rejecting_child_input ~output:Temporal.Codec.unit
        in
        Temporal.Child_workflow.execute ~id:"valid-id" target ())
  in
  let invalid_execution = Execution.start invalid_input () in
  (match
     Execution.activate invalid_execution [ Activation.Start_workflow ]
   with
  | [] ->
      let error = require_task_failure invalid_execution [] in
      expect "child input codec failure" "input rejected"
        (public_error_message error)
  | _ -> failwith "invalid child input emitted a command or succeeded");
  match
    Temporal.Child_workflow.start ~id:"detached" greeting_child "Ada"
    |> Temporal.Future.peek
  with
  | Some (Error error) ->
      expect "detached child kind" "defect" (Temporal.Error.kind error)
  | Some (Ok _) | None -> failwith "detached child did not fail immediately"

(** Proves ID validation occurs before input encoding, sequence allocation, and
    command emission. The first valid child still receives sequence one. *)
let test_child_workflow_id_boundary () =
  let execution = Execution.start child_id_boundary_workflow () in
  expect "invalid child IDs consume no sequence"
    [
      Activation.Start_child_workflow
        {
          seq = 1L;
          id = "after-invalid";
          name = "greeting_child";
          input = payload "Ada";
          retry_policy = None;
          cancellation_type = Activation.Child_try_cancel;
        };
    ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "valid child after ID failures completes"
    [ Activation.Complete_workflow (payload "Hello Ada") ]
    (Execution.activate execution
       [
         child_started 1L;
         Activation.Resolve_child_workflow
           { seq = 1L; result = Ok (payload "Hello Ada") };
       ])

(** Remote child failures retain their typed error, while unknown or repeated
    sequence numbers become bridge defects and never resolve a future twice. *)
let test_child_workflow_failures_and_sequence_ownership () =
  let premature = Execution.start child_parent_workflow "Ada" in
  ignore (Execution.activate premature [ Activation.Start_workflow ]);
  (match
     Execution.activate premature
       [ Activation.Resolve_child_workflow
           { seq = 1L; result = Ok (payload "too early") } ]
   with
  | [] ->
      let error = require_task_failure premature [] in
      expect "child resolved before start" "bridge" (public_error_kind error)
  | _ -> failwith "child result was accepted before start acknowledgment");
  let failed_start = Execution.start child_parent_workflow "Ada" in
  ignore (Execution.activate failed_start [ Activation.Start_workflow ]);
  let start_error =
    Temporal_base.Error.make ~non_retryable:true ~category:`Child_workflow
      ~message:"child start rejected" ()
  in
  (match
     Execution.activate failed_start
       [
         Activation.Resolve_child_workflow_start
           { seq = 1L; result = Error start_error };
       ]
   with
  | [ Activation.Fail_workflow error ] ->
      expect "child start failure" "child start rejected"
        (public_error_message error)
  | _ -> failwith "child start failure did not retire the pending child");
  let remote_failure = Execution.start child_parent_workflow "Ada" in
  ignore (Execution.activate remote_failure [ Activation.Start_workflow ]);
  let child_error = base_error (Temporal.Error.make ~category:`Child_workflow ~message:"child failed" ()) in
  (match
     Execution.activate remote_failure
       [
         child_started 1L;
         Activation.Resolve_child_workflow
           { seq = 1L; result = Error child_error };
       ]
   with
  | [ Activation.Fail_workflow error ] ->
      expect "remote child error" "child failed" (public_error_message error)
  | _ -> failwith "remote child failure was not propagated");
  let unknown = Execution.start child_parent_workflow "Ada" in
  ignore (Execution.activate unknown [ Activation.Start_workflow ]);
  (match
     Execution.activate unknown
       [
         Activation.Resolve_child_workflow
           { seq = 999L; result = Ok (payload "unknown") };
       ]
   with
  | [] ->
      let error = require_task_failure unknown [] in
      expect "unknown child sequence" "bridge" (public_error_kind error)
  | _ -> failwith "unknown child sequence was accepted");
  let duplicate = Execution.start concurrent_child_parent () in
  ignore (Execution.activate duplicate [ Activation.Start_workflow ]);
  ignore
    (Execution.activate duplicate
       [
         child_started 1L;
         Activation.Resolve_child_workflow
           { seq = 1L; result = Ok (payload "Hello Ada") };
       ]);
  match
    Execution.activate duplicate
      [
        Activation.Resolve_child_workflow
          { seq = 1L; result = Ok (payload "Hello again") };
      ]
  with
  | [] ->
      let error = require_task_failure duplicate [] in
      expect "duplicate child sequence" "bridge" (public_error_kind error)
  | _ -> failwith "duplicate child resolution was accepted"

(** Ensures a conflicting second start result cannot consume the child resolver.
    The context must report a bridge error, leave the future pending, and still
    accept the legitimate terminal result that follows. *)
let test_child_start_conflicting_result_keeps_future_pending () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let future, _cancel =
    Workflow_context_store.start_child_workflow context ~id:"child-id"
      ~name:"child-workflow" ~input:(payload "Ada")
      ~decode:(fun value -> Ok value) ()
  in
  (match
     Workflow_context_store.resolve_child_workflow_start context ~seq:1L
       (Ok "child-run")
   with
  | Ok () -> ()
  | Error _ -> failwith "initial child start acknowledgment was rejected");
  let conflicting_error =
    Temporal_base.Error.make ~non_retryable:true ~category:`Child_workflow
      ~message:"conflicting child start" ()
  in
  (match
     Workflow_context_store.resolve_child_workflow_start context ~seq:1L
       (Error conflicting_error)
   with
  | Error error ->
      expect "conflicting child start kind" "bridge"
        (public_error_kind error)
  | Ok () -> failwith "conflicting child start was accepted");
  (match Future_store.peek future with
  | None -> ()
  | Some _ -> failwith "conflicting child start resolved the child future");
  (match
     Workflow_context_store.resolve_child_workflow context ~seq:1L
       (Ok (payload "Hello Ada"))
   with
  | Ok () -> ()
  | Error _ -> failwith "valid terminal child result was rejected after conflict");
  match Future_store.peek future with
  | Some (Ok result) when Bytes.equal result.data (payload "Hello Ada").data ->
      Workflow_context_store.shutdown context
  | Some (Error error) ->
      failwith ("terminal child result failed: " ^ public_error_message error)
  | Some (Ok _) -> failwith "terminal child result payload was not preserved"
  | None -> failwith "terminal child result did not resolve the child future"

(** A cancellation racing with an activity's natural result is a harmless
    retry. The runtime removes the resolver before invoking it, so the handle's
    terminal marker must make this late call a no-op rather than emit a command
    for an unknown sequence. *)
let test_activity_cancel_after_natural_completion_is_noop () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let future, cancel =
    Workflow_context_store.schedule_activity context ~name:"greeting"
      ~input:(payload "Ada") ~decode:(fun value -> Ok value) ()
  in
  expect "natural-completion activity schedule command"
    [ Activation.Schedule_activity
        {
          seq = 1L;
          activity_id = "ocaml-activity-1";
          activity_type = "greeting";
          task_queue = "default";
          arguments = [ payload "Ada" ];
          schedule_to_close_timeout = None;
          schedule_to_start_timeout = None;
          start_to_close_timeout = Some 60_000L;
          heartbeat_timeout = None;
          retry_policy = None;
          priority = None;
          cancellation_type = Activation.Try_cancel;
          do_not_eagerly_execute = false;
        } ]
    (Workflow_context_store.take_commands context);
  let result = payload "done" in
  (match Workflow_context_store.resolve_activity context ~seq:1L (Ok result) with
  | Ok () -> ()
  | Error error ->
      failwith
        ("natural activity result was rejected: " ^ public_error_message error));
  (match Future_store.peek future with
  | Some (Ok value) when Bytes.equal value.data result.data -> ()
  | Some (Error error) ->
      failwith ("natural activity completion failed: " ^ public_error_message error)
  | Some (Ok _) -> failwith "natural activity payload was changed"
  | None -> failwith "natural activity result did not resolve the future");
  (match
     Workflow_context_store.with_context context (fun () -> cancel ())
   with
  | Ok () -> ()
  | Error error ->
      failwith
        ("late activity cancellation failed: " ^ public_error_message error));
  expect "late activity cancellation emits no command" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** A Core backoff keeps the original local-activity future pending, creates a
    separate workflow timer, and re-emits the same activity sequence with the
    supplied retry attempt when that timer fires. *)
let test_local_activity_backoff_reschedules_without_resolving () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let future, _cancel =
    Workflow_context_store.schedule_local_activity context ~name:"local-greeting"
      ~input:(payload "Ada") ~decode:(fun value -> Ok value) ()
  in
  expect "initial local activity command"
    [ Activation.Schedule_local_activity
        {
          seq = 1L;
          activity_id = "ocaml-activity-1";
          activity_type = "local-greeting";
          attempt = 1L;
          original_schedule_time = None;
          arguments = [ payload "Ada" ];
          schedule_to_close_timeout = None;
          schedule_to_start_timeout = None;
          start_to_close_timeout = Some 60_000L;
          retry_policy = None;
          local_retry_threshold = None;
          cancellation_type = Activation.Wait_cancellation_completed;
        } ]
    (Workflow_context_store.take_commands context);
  let original_schedule_time : Protocol.timestamp =
    { Protocol.seconds = 100L; nanoseconds = 7 }
  in
  (match
     Workflow_context_store.resolve_local_activity_backoff context ~seq:1L
       ~attempt:2L ~backoff_milliseconds:2_500L ~original_schedule_time:(Some original_schedule_time)
   with
  | Ok () -> ()
  | Error error -> failwith ("local backoff was rejected: " ^ public_error_message error));
  expect "local backoff starts timer"
    [ Activation.Start_timer { seq = 2L; milliseconds = 2_500L } ]
    (Workflow_context_store.take_commands context);
  (match Future_store.peek future with
  | None -> ()
  | Some _ -> failwith "local backoff resolved the activity future");
  (match Workflow_context_store.fire_timer context ~seq:2L with
  | Ok () -> ()
  | Error error -> failwith ("local backoff timer failed: " ^ public_error_message error));
  expect "local backoff reschedules activity"
    [ Activation.Schedule_local_activity
        {
          seq = 1L;
          activity_id = "ocaml-activity-1";
          activity_type = "local-greeting";
          attempt = 2L;
          original_schedule_time = Some original_schedule_time;
          arguments = [ payload "Ada" ];
          schedule_to_close_timeout = None;
          schedule_to_start_timeout = None;
          start_to_close_timeout = Some 60_000L;
          retry_policy = None;
          local_retry_threshold = None;
          cancellation_type = Activation.Wait_cancellation_completed;
        } ]
    (Workflow_context_store.take_commands context);
  (match Workflow_context_store.resolve_activity context ~seq:1L (Ok (payload "done")) with
  | Ok () -> ()
  | Error error -> failwith ("rescheduled local activity failed: " ^ public_error_message error));
  Workflow_context_store.shutdown context

(** A terminal activity failure closes the handle lifecycle just like a
    successful result. A later cancellation retry must therefore be idempotent
    and must not allocate a second command. *)
let test_activity_cancel_after_failure_is_noop () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let future, cancel =
    Workflow_context_store.schedule_activity context ~name:"greeting"
      ~input:(payload "Ada") ~decode:(fun value -> Ok value) ()
  in
  ignore (Workflow_context_store.take_commands context);
  let failure =
    Temporal_base.Error.make ~non_retryable:true ~category:`Activity
      ~message:"activity rejected" ()
  in
  (match Workflow_context_store.resolve_activity context ~seq:1L (Error failure) with
  | Ok () -> ()
  | Error error ->
      failwith
        ("activity failure was rejected: " ^ public_error_message error));
  (match Future_store.peek future with
  | Some (Error error) when String.equal (public_error_message error) "activity rejected" ->
      ()
  | Some (Error error) ->
      failwith ("activity failure changed: " ^ public_error_message error)
  | Some (Ok _) -> failwith "failed activity unexpectedly succeeded"
  | None -> failwith "failed activity future remained pending");
  (match
     Workflow_context_store.with_context context (fun () -> cancel ())
   with
  | Ok () -> ()
  | Error error ->
      failwith
        ("cancellation after activity failure was not idempotent: "
        ^ public_error_message error));
  expect "failed activity retry emits no command" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** Cancellation is owned by the workflow context that created the handle. A
    detached call is rejected before the pending sequence can be mutated, while
    the owning context can still issue the one valid command afterward. *)
let test_activity_cancel_owner_check () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let _future, cancel =
    Workflow_context_store.schedule_activity context ~name:"greeting"
      ~input:(payload "Ada") ~decode:(fun value -> Ok value) ()
  in
  ignore (Workflow_context_store.take_commands context);
  (match cancel () with
  | Error error -> expect "detached activity cancellation" "defect" (public_error_kind error)
  | Ok () -> failwith "detached activity cancellation was accepted");
  expect "detached activity cancellation emits no command" []
    (Workflow_context_store.take_commands context);
  (match Workflow_context_store.with_context context (fun () -> cancel ()) with
  | Ok () -> ()
  | Error error ->
      failwith
        ("owning activity cancellation was rejected: " ^ public_error_message error));
  expect "owning activity cancellation command"
    [ Activation.Request_cancel_activity { seq = 1L } ]
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** A cancellation that races with a child's natural terminal result is a
    harmless retry, not a new bridge failure. This test drives the private
    context directly so it can assert that the handle emits no late cancel
    command after Core has removed the child entry and settled its future. *)
let test_child_cancel_after_natural_completion_is_noop () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let future, cancel =
    Workflow_context_store.start_child_workflow context ~id:"late-cancel-child"
      ~name:"child-workflow" ~input:(payload "Ada")
      ~decode:(fun value -> Ok value) ()
  in
  expect "natural-completion start command"
    [
      Activation.Start_child_workflow
        {
          seq = 1L;
          id = "late-cancel-child";
          name = "child-workflow";
          input = payload "Ada";
          retry_policy = None;
          cancellation_type = Activation.Child_try_cancel;
        };
    ]
    (Workflow_context_store.take_commands context);
  (match
     Workflow_context_store.resolve_child_workflow_start context ~seq:1L
       (Ok "child-run")
   with
  | Ok () -> ()
  | Error _ -> failwith "natural-completion start acknowledgment was rejected");
  let result = payload "done" in
  (match
     Workflow_context_store.resolve_child_workflow context ~seq:1L (Ok result)
   with
  | Ok () -> ()
  | Error _ -> failwith "natural-completion result was rejected");
  (match Future_store.peek future with
  | Some (Ok value) when Bytes.equal value.data result.data -> ()
  | Some (Error error) ->
      failwith ("natural completion failed: " ^ public_error_message error)
  | Some (Ok _) -> failwith "natural-completion payload was changed"
  | None -> failwith "natural-completion result did not resolve the future");
  let late_cancel =
    Workflow_context_store.with_context context (fun () ->
        cancel ~reason:"late retry after completion")
  in
  (match late_cancel with
  | Ok () -> ()
  | Error error ->
      failwith
        ("late cancel after natural completion failed: "
        ^ public_error_message error));
  expect "late cancel emits no command" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** A failed child start also closes the handle's lifecycle. Retrying
    cancellation after Core has rejected the start must therefore be a typed
    no-op, not a second cancel command for a sequence that no longer exists. *)
let test_child_cancel_after_start_failure_is_noop () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let future, cancel =
    Workflow_context_store.start_child_workflow context ~id:"failed-child"
      ~name:"child-workflow" ~input:(payload "Ada")
      ~decode:(fun value -> Ok value) ()
  in
  ignore (Workflow_context_store.take_commands context);
  let failure =
    Temporal_base.Error.make ~non_retryable:true ~category:`Child_workflow
      ~message:"child start rejected" ()
  in
  (match
     Workflow_context_store.resolve_child_workflow_start context ~seq:1L
       (Error failure)
   with
  | Ok () -> ()
  | Error error ->
      failwith
        ("child start failure was rejected: " ^ public_error_message error));
  (match Future_store.peek future with
  | Some (Error error) when String.equal (public_error_message error) "child start rejected" ->
      ()
  | Some (Error error) ->
      failwith
        ("child start failure changed: " ^ public_error_message error)
  | Some (Ok _) -> failwith "failed child unexpectedly succeeded"
  | None -> failwith "failed child future remained pending");
  let retry =
    Workflow_context_store.with_context context (fun () ->
        cancel ~reason:"retry after rejected start")
  in
  (match retry with
  | Ok () -> ()
  | Error error ->
      failwith
        ("cancel after failed child start was not idempotent: "
        ^ public_error_message error));
  expect "failed child retry emits no command" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** Proves lifecycle-order and duplicate-resolution defects are non-mutating.
    A terminal result received before the start acknowledgment leaves its
    resolver pending; a duplicate start cannot replace the first run ID; and a
    duplicate terminal result cannot overwrite the already-decoded payload. *)
let test_child_resolution_rejections_preserve_lifecycle_state () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let future, _cancel =
    Workflow_context_store.start_child_workflow context ~id:"ordered-child"
      ~name:"child-workflow" ~input:(payload "Ada")
      ~decode:(fun value -> Ok value) ()
  in
  (match
     Workflow_context_store.resolve_child_workflow context ~seq:1L
       (Ok (payload "too early"))
   with
  | Error error ->
      expect "terminal-before-start kind" "bridge" (public_error_kind error)
  | Ok () -> failwith "terminal-before-start was accepted");
  (match Future_store.peek future with
  | None -> ()
  | Some _ -> failwith "terminal-before-start changed the pending future");
  (match
     Workflow_context_store.resolve_child_workflow_start context ~seq:1L
       (Ok "child-run")
   with
  | Ok () -> ()
  | Error _ -> failwith "valid start acknowledgment was rejected");
  let conflicting_error =
    Temporal_base.Error.make ~non_retryable:true ~category:`Child_workflow
      ~message:"conflicting child start" ()
  in
  (match
     Workflow_context_store.resolve_child_workflow_start context ~seq:1L
       (Error conflicting_error)
   with
  | Error error ->
      expect "duplicate start kind" "bridge" (public_error_kind error)
  | Ok () -> failwith "duplicate start acknowledgment was accepted");
  (match Future_store.peek future with
  | None -> ()
  | Some _ -> failwith "duplicate start changed the pending future");
  let result = payload "Hello Ada" in
  (match
     Workflow_context_store.resolve_child_workflow context ~seq:1L
       (Ok result)
   with
  | Ok () -> ()
  | Error _ -> failwith "valid terminal result was rejected");
  (match Future_store.peek future with
  | Some (Ok value) when Bytes.equal value.data result.data -> ()
  | Some (Error error) ->
      failwith ("terminal result failed: " ^ public_error_message error)
  | Some (Ok _) -> failwith "terminal result was decoded differently"
  | None -> failwith "terminal result did not resolve the future");
  (match
     Workflow_context_store.resolve_child_workflow context ~seq:1L
       (Ok (payload "overwritten"))
   with
  | Error error ->
      expect "duplicate terminal kind" "bridge" (public_error_kind error)
  | Ok () -> failwith "duplicate terminal result was accepted");
  (match Future_store.peek future with
  | Some (Ok value) when Bytes.equal value.data result.data ->
      Workflow_context_store.shutdown context
  | Some (Error error) ->
      failwith
        ("duplicate terminal changed the result: " ^ public_error_message error)
  | Some (Ok _) -> failwith "duplicate terminal overwrote the result"
  | None -> failwith "duplicate terminal removed the resolved result")

(** Verifies cancellation emits once and cache removal releases blocked state
    without producing a workflow command. *)
let test_cancel_and_evict () =
  let cancelled = Execution.start greeting_workflow "Ada" in
  ignore (Execution.activate cancelled [ Activation.Start_workflow ]);
  expect "cancellation command" [ Activation.Cancel_workflow_execution ]
    (Execution.activate cancelled [ Activation.Cancel_workflow ]);
  let evicted = Execution.start greeting_workflow "Ada" in
  ignore (Execution.activate evicted [ Activation.Start_workflow ]);
  expect "eviction has no command" []
    (Execution.activate evicted [ Activation.Remove_from_cache ]);
  expect "evicted execution stays inert" []
    (Execution.activate evicted [ Activation.Start_workflow ])

(** Confirms the public continue-as-new operation buffers a terminal command,
    stops the current workflow fiber before it can schedule later work, and
    preserves the successor definition's encoded input through the private
    execution adapter. A later activation must remain inert because the
    current run has already been replaced by Temporal. *)
let test_continue_as_new_terminal () =
  let successor =
    Temporal.Workflow.remote ~name:"continuation_target"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let source =
    Temporal.Workflow.define ~name:"continuation_source"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        (* Bind the polymorphic terminal operation through a local value so
           the compiler does not reject the intentionally unreachable guard
           below as a non-returning statement. *)
        let continue : unit -> unit = Temporal.Workflow.continue_as_new successor in
        continue ();
        (* If the terminal effect ever resumed this fiber, this timer would
           append a second command after continue-as-new. The assertion below
           therefore covers both the scheduler abort and the execution
           terminal marker, rather than only checking the encoded command. *)
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 1L))
  in
  let execution = Execution.start source () in
  let expected_input =
    match Temporal.Codec.encode Temporal.Codec.unit () with
    | Ok value -> base_payload value
    | Error error -> failwith (Temporal.Error.message error)
  in
  expect "continue-as-new command"
    [ Activation.Continue_as_new
        { workflow_type = "continuation_target"; input = expected_input } ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "continue-as-new activation is terminal" []
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "continue-as-new ignores cancellation" []
    (Execution.activate execution [ Activation.Cancel_workflow ])


(** A continue-as-new whose successor input cannot be encoded must still seal
    the execution: the failure command is terminal, and a later cancellation
    must not emit a second terminal command. *)
let test_continue_as_new_encode_failure_is_terminal () =
  let bad_input =
    Temporal.Codec.make ~encoding:"json/plain"
      ~encode:(fun () ->
        Error (Temporal.Error.codec ~message:"successor input rejected"))
      ~decode:(fun _ -> Error (Temporal.Error.codec ~message:"unused"))
  in
  let successor =
    Temporal.Workflow.remote ~name:"continuation_target" ~input:bad_input
      ~output:Temporal.Codec.unit
  in
  let source =
    Temporal.Workflow.define ~name:"continuation_encode_fail"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let continue : unit -> (unit, Temporal.Error.t) result =
          Temporal.Workflow.continue_as_new successor
        in
        continue ())
  in
  let execution = Execution.start source () in
  (match Execution.activate execution [ Activation.Start_workflow ] with
  | [] -> ignore (require_task_failure execution [])
  | _ -> failwith "failed continue-as-new encoding retained commands");
  expect "encode-failed continue-as-new seals the execution" []
    (Execution.activate execution [ Activation.Cancel_workflow ])

(** A parked fiber discontinued by terminal shutdown must not append cancel
    commands after the terminal command in the same activation batch. *)
let test_terminal_finally_cannot_emit_cancel () =
  let activity =
    Temporal.Activity.remote ~name:"cleanup_target" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit
  in
  let source =
    Temporal.Workflow.define ~name:"terminal_cleanup_guard"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let handle = Temporal.Activity.start_handle activity () in
        Fun.protect
          ~finally:(fun () -> ignore (Temporal.Activity.cancel handle))
          (fun () ->
            match Temporal.Future.await (Temporal.Activity.future handle) with
            | Ok () | Error _ -> Ok ()))
  in
  let execution = Execution.start source () in
  (* First activation schedules the activity and blocks on await. *)
  let first = Execution.activate execution [ Activation.Start_workflow ] in
  (match first with
  | [ Activation.Schedule_activity _ ] -> ()
  | _ ->
      failwith
        ("expected only Schedule_activity, got "
        ^ string_of_int (List.length first)));
  (* Second activation cancels the workflow, discontinuing the await so the
     finally runs during terminal shutdown. *)
  let second = Execution.activate execution [ Activation.Cancel_workflow ] in
  (match second with
  | [ Activation.Cancel_workflow_execution ] -> ()
  | commands ->
      if
        List.exists
          (function Activation.Request_cancel_activity _ -> true | _ -> false)
          commands
      then
        failwith
          "Request_cancel_activity was emitted after Cancel_workflow_execution";
      failwith
        ("expected only Cancel_workflow_execution, got "
        ^ string_of_int (List.length commands)))

(** [continue_as_new] uses [terminate] rather than [emit_terminal], which
    historically left the context unsealed while the abort unwound the
    calling fiber. A [Fun.protect] cleanup that runs during that unwind (e.g.
    cancelling an activity started just before the continue-as-new) must not
    be able to append its command after the buffered [Continue_as_new].
    Regression test for the terminate/emit_terminal seal asymmetry. *)
let test_continue_as_new_finally_cannot_emit_cancel () =
  let activity =
    Temporal.Activity.remote ~name:"cleanup_target" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit
  in
  let successor =
    Temporal.Workflow.remote ~name:"continuation_target"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let source =
    Temporal.Workflow.define ~name:"continue_as_new_cleanup_guard"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let handle = Temporal.Activity.start_handle activity () in
        Fun.protect
          ~finally:(fun () -> ignore (Temporal.Activity.cancel handle))
          (fun () ->
            let continue : unit -> unit =
              Temporal.Workflow.continue_as_new successor
            in
            continue ());
        Ok ())
  in
  let execution = Execution.start source () in
  (* The activity schedules and the continue-as-new both happen synchronously
     within the first activation, with no await in between. *)
  let first = Execution.activate execution [ Activation.Start_workflow ] in
  (match first with
  | [ Activation.Schedule_activity _; Activation.Continue_as_new _ ] -> ()
  | commands ->
      if
        List.exists
          (function Activation.Request_cancel_activity _ -> true | _ -> false)
          commands
      then
        failwith "Request_cancel_activity was emitted after Continue_as_new"
      else
        failwith
          (Printf.sprintf
             "expected [Schedule_activity; Continue_as_new], got %d commands"
             (List.length commands)));
  expect "continue-as-new activation is terminal" []
    (Execution.activate execution [ Activation.Start_workflow ])

(** Exercises the complete per-execution patch decision rule. Live code takes
    the new branch, replay without a marker takes the old branch, and replay
    with Core's notification takes the new branch. Repeated calls deliberately
    emit repeated marker commands for Core to deduplicate. *)
let test_workflow_patch_decisions () =
  let run ~name ~is_replaying ~jobs implementation =
    let definition =
      Temporal.Workflow.define ~name ~input:Temporal.Codec.unit
        ~output:Temporal.Codec.unit implementation
    in
    let execution = Execution.start definition () in
    Execution.set_activation_is_replaying execution is_replaying;
    Execution.activate execution jobs
  in
  let live_decisions = ref [] in
  let live_commands =
    run ~name:"patch_live" ~is_replaying:false
      ~jobs:[ Activation.Start_workflow ] (fun () ->
        live_decisions :=
          [ Temporal.Workflow.patched ~id:"orders.v2";
            Temporal.Workflow.patched ~id:"orders.v2" ];
        Ok ())
  in
  expect "live patch decisions" [ true; true ] !live_decisions;
  (match live_commands with
  | [ Activation.Set_patch_marker
        { patch_id = "orders.v2"; deprecated = false };
      Activation.Set_patch_marker
        { patch_id = "orders.v2"; deprecated = false };
      Activation.Complete_workflow _ ] -> ()
  | _ -> failwith "repeated live patch calls emitted unexpected commands");
  let absent_decision = ref None in
  let absent_commands =
    run ~name:"patch_replay_absent" ~is_replaying:true
      ~jobs:[ Activation.Start_workflow ] (fun () ->
        absent_decision := Some (Temporal.Workflow.patched ~id:"orders.v2");
        Ok ())
  in
  expect "replay absent patch decision" (Some false) !absent_decision;
  (match absent_commands with
  | Activation.Set_patch_marker
      { patch_id = "orders.v2"; deprecated = false }
    :: Activation.Complete_workflow _ :: [] -> ()
  | _ -> failwith "replay without marker emitted unexpected commands");
  let present_decision = ref None in
  let present_commands =
    run ~name:"patch_replay_present" ~is_replaying:true
      ~jobs:
        [ Activation.Start_workflow;
          Activation.Notify_has_patch { patch_id = "orders.v2" } ]
      (fun () ->
        present_decision := Some (Temporal.Workflow.patched ~id:"orders.v2");
        Ok ())
  in
  expect "replay present patch decision" (Some true) !present_decision;
  (match present_commands with
  | Activation.Set_patch_marker
      { patch_id = "orders.v2"; deprecated = false }
    :: Activation.Complete_workflow _ :: [] -> ()
  | _ -> failwith "replay with marker emitted unexpected commands");
  let deprecated_commands =
    run ~name:"patch_deprecated" ~is_replaying:false
      ~jobs:[ Activation.Start_workflow ] (fun () ->
        Temporal.Workflow.deprecate_patch ~id:"orders.v2";
        Temporal.Workflow.deprecate_patch ~id:"orders.v2";
        Ok ())
  in
  (match deprecated_commands with
  | [ Activation.Set_patch_marker
        { patch_id = "orders.v2"; deprecated = true };
      Activation.Set_patch_marker
        { patch_id = "orders.v2"; deprecated = true };
      Activation.Complete_workflow _ ] -> ()
  | _ -> failwith "repeated patch deprecation emitted unexpected commands");
  let replay_deprecated_commands =
    run ~name:"patch_deprecated_replay_absent" ~is_replaying:true
      ~jobs:[ Activation.Start_workflow ] (fun () ->
        Temporal.Workflow.deprecate_patch ~id:"orders.v2";
        Ok ())
  in
  match replay_deprecated_commands with
  | [ Activation.Set_patch_marker
        { patch_id = "orders.v2"; deprecated = true };
      Activation.Complete_workflow _ ] -> ()
  | _ -> failwith "marker-free replay rejected patch deprecation"

(** One execution may repeat one marker mode but must never emit both active
    and deprecated commands for the same ID. Core retains the first command,
    so rejecting the second mode prevents source order from silently deciding
    durable history. History notification establishes only the replay decision
    and therefore remains compatible with deprecation-only replacement code. *)
let test_workflow_patch_mode_invariant () =
  let expect_invalid label action =
    match action () with
    | exception Invalid_argument message when not (String.equal message "") -> ()
    | exception _ -> failwith (label ^ " raised the wrong exception")
    | _ -> failwith (label ^ " unexpectedly succeeded")
  in
  let active_context = Workflow_context_store.create (Scheduler.create ()) in
  ignore (Workflow_context_store.patched active_context ~patch_id:"orders.v2");
  expect_invalid "active then deprecated" (fun () ->
      Workflow_context_store.deprecate_patch active_context
        ~patch_id:"orders.v2");
  (match Workflow_context_store.take_commands active_context with
  | [ Activation.Set_patch_marker
        { patch_id = "orders.v2"; deprecated = false } ] -> ()
  | _ -> failwith "active/deprecated conflict appended a second command");
  let drained_active_context =
    Workflow_context_store.create (Scheduler.create ())
  in
  ignore
    (Workflow_context_store.patched drained_active_context
       ~patch_id:"orders.v2");
  ignore (Workflow_context_store.take_commands drained_active_context);
  expect_invalid "active then deprecated after command drain" (fun () ->
      Workflow_context_store.deprecate_patch drained_active_context
        ~patch_id:"orders.v2");
  expect "active/deprecated conflict left drained buffer empty" []
    (Workflow_context_store.take_commands drained_active_context);
  let deprecated_context =
    Workflow_context_store.create (Scheduler.create ())
  in
  Workflow_context_store.deprecate_patch deprecated_context
    ~patch_id:"orders.v2";
  ignore (Workflow_context_store.take_commands deprecated_context);
  Workflow_context_store.deprecate_patch deprecated_context
    ~patch_id:"orders.v2";
  (match Workflow_context_store.take_commands deprecated_context with
  | [ Activation.Set_patch_marker
        { patch_id = "orders.v2"; deprecated = true } ] -> ()
  | _ -> failwith "deprecation mode did not survive command-buffer drain");
  expect_invalid "deprecated then active after command drain" (fun () ->
      ignore
        (Workflow_context_store.patched deprecated_context
           ~patch_id:"orders.v2"));
  expect "deprecated/active conflict left next buffer empty" []
    (Workflow_context_store.take_commands deprecated_context);
  let independent_context =
    Workflow_context_store.create (Scheduler.create ())
  in
  ignore
    (Workflow_context_store.patched independent_context
       ~patch_id:"orders.active");
  Workflow_context_store.deprecate_patch independent_context
    ~patch_id:"orders.deprecated";
  (match Workflow_context_store.take_commands independent_context with
  | [ Activation.Set_patch_marker
        { patch_id = "orders.active"; deprecated = false };
      Activation.Set_patch_marker
        { patch_id = "orders.deprecated"; deprecated = true } ] -> ()
  | _ -> failwith "independent patch IDs did not retain separate modes");
  let snapshot_context = Workflow_context_store.create (Scheduler.create ()) in
  let mutable_id = Bytes.of_string "orders.deprecated" in
  let aliased_id = Bytes.unsafe_to_string mutable_id in
  Workflow_context_store.deprecate_patch snapshot_context ~patch_id:aliased_id;
  Bytes.set mutable_id 0 'X';
  (match Workflow_context_store.take_commands snapshot_context with
  | [ Activation.Set_patch_marker
        { patch_id = "orders.deprecated"; deprecated = true } ] -> ()
  | _ -> failwith "mutable deprecated patch ID changed retained command");
  let notified_context = Workflow_context_store.create (Scheduler.create ()) in
  Workflow_context_store.set_activation_is_replaying notified_context true;
  Workflow_context_store.notify_has_patch notified_context
    ~patch_id:"orders.v2";
  Workflow_context_store.deprecate_patch notified_context
    ~patch_id:"orders.v2";
  (match Workflow_context_store.take_commands notified_context with
  | [ Activation.Set_patch_marker
        { patch_id = "orders.v2"; deprecated = true } ] -> ()
  | _ -> failwith "history notification incorrectly fixed local patch mode");
  Workflow_context_store.shutdown notified_context;
  expect_invalid "deprecate after execution shutdown" (fun () ->
      Workflow_context_store.deprecate_patch notified_context
        ~patch_id:"orders.v2")

(** Patch modes and copied identifiers belong to one workflow execution. Two
    runs may legitimately use the same durable ID at different migration
    phases, and mutation of a public-call alias after deprecation must not alter
    the command already retained by that run. *)
let test_workflow_patch_mode_isolation_and_public_deprecation_snapshot () =
  let active_definition =
    Temporal.Workflow.define ~name:"patch_mode_active"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        ignore (Temporal.Workflow.patched ~id:"shared.patch");
        Ok ())
  in
  let mutable_id = Bytes.of_string "shared.patch" in
  let aliased_id = Bytes.unsafe_to_string mutable_id in
  let deprecated_definition =
    Temporal.Workflow.define ~name:"patch_mode_deprecated"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.deprecate_patch ~id:aliased_id;
        Bytes.set mutable_id 0 'X';
        Ok ())
  in
  let active_commands =
    Execution.start active_definition ()
    |> fun execution -> Execution.activate execution [ Activation.Start_workflow ]
  in
  let deprecated_commands =
    Execution.start deprecated_definition ()
    |> fun execution -> Execution.activate execution [ Activation.Start_workflow ]
  in
  (match active_commands with
  | Activation.Set_patch_marker
      { patch_id = "shared.patch"; deprecated = false }
    :: _ -> ()
  | _ -> failwith "active execution did not retain its independent mode");
  match deprecated_commands with
  | Activation.Set_patch_marker
      { patch_id = "shared.patch"; deprecated = true }
    :: _ -> ()
  | _ -> failwith "public deprecation ID mutation changed retained command"

(** Patch state belongs to one workflow execution and patch IDs are copied at
    the public boundary. A notification in run A must not affect run B, and a
    caller mutating an unsafe string alias after [patched] returns must not
    change the emitted durable marker. *)
let test_workflow_patch_isolation_and_public_id_snapshot () =
  let decision_a = ref None in
  let definition_a =
    Temporal.Workflow.define ~name:"patch_run_a" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () ->
        decision_a := Some (Temporal.Workflow.patched ~id:"shared.patch");
        Ok ())
  in
  let decision_b = ref None in
  let mutable_id = Bytes.of_string "shared.patch" in
  let aliased_id = Bytes.unsafe_to_string mutable_id in
  let definition_b =
    Temporal.Workflow.define ~name:"patch_run_b" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () ->
        decision_b := Some (Temporal.Workflow.patched ~id:aliased_id);
        Bytes.set mutable_id 0 'X';
        Ok ())
  in
  let execution_a = Execution.start definition_a () in
  let execution_b = Execution.start definition_b () in
  Execution.set_activation_is_replaying execution_a true;
  Execution.set_activation_is_replaying execution_b true;
  let commands_a =
    Execution.activate execution_a
      [ Activation.Start_workflow;
        Activation.Notify_has_patch { patch_id = "shared.patch" } ]
  in
  let commands_b =
    Execution.activate execution_b [ Activation.Start_workflow ]
  in
  expect "patch run A decision" (Some true) !decision_a;
  expect "patch run B decision" (Some false) !decision_b;
  (match commands_a with
  | Activation.Set_patch_marker { patch_id = "shared.patch"; _ } :: _ -> ()
  | _ -> failwith "patch run A did not emit its marker");
  match commands_b with
  | Activation.Set_patch_marker { patch_id = "shared.patch"; _ } :: _ -> ()
  | _ -> failwith "patch ID mutation changed the emitted marker"

(** Proves the runtime snapshots a notification ID before retaining it as a
    per-execution hash-table key. Native/protocol adapters normally supply
    ordinary immutable strings, but a private caller can still pass an unsafe
    alias. Mutating that alias after notification must neither lose the
    recorded decision nor alter the marker emitted by a later [patched] call. *)
let test_incoming_patch_notification_snapshot () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  Workflow_context_store.set_activation_is_replaying context true;
  let mutable_id = Bytes.of_string "orders.v2" in
  let aliased_id = Bytes.unsafe_to_string mutable_id in
  Workflow_context_store.notify_has_patch context ~patch_id:aliased_id;
  Bytes.set mutable_id 0 'X';
  expect "snapshotted notification decision" true
    (Workflow_context_store.patched context ~patch_id:"orders.v2");
  match Workflow_context_store.take_commands context with
  | [ Activation.Set_patch_marker
        { patch_id = "orders.v2"; deprecated = false } ] -> ()
  | _ -> failwith "notification ID mutation changed retained patch state"

(** Confirms the workflow random stream is owned by the execution context and
    therefore repeats exactly for the same Core seed. The public function also
    rejects an invalid bound through the normal typed-defect path. *)
let test_workflow_random_int_is_replay_stable () =
  let draw seed =
    let scheduler = Scheduler.create () in
    let context =
      Workflow_context_store.create ~randomness_seed:seed scheduler
    in
    Workflow_context_store.with_context context (fun () ->
        List.init 8 (fun _ -> Temporal.Workflow.random_int ~bound:97))
  in
  let first = draw "18446744073709551615" in
  let replay = draw "18446744073709551615" in
  if first <> replay then failwith "workflow random stream changed during replay";
  List.iter
    (function
      | Ok value when value >= 0 && value < 97 -> ()
      | Ok _ -> failwith "workflow random value exceeded its bound"
      | Error _ -> failwith "workflow random stream unexpectedly failed")
    first;
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  (match
     Workflow_context_store.with_context context (fun () ->
         Temporal.Workflow.random_int ~bound:0)
   with
  | Error error when String.equal (Temporal.Error.kind error) "defect" -> ()
  | _ -> failwith "invalid workflow random bound was accepted")

(** Ensures a live update validator cannot consume the workflow random stream.
    Validators are not replayed, so allowing [Temporal.Workflow.random_int]
    there would make the next handler draw differ between live execution and
    replay. The guard must be removed before the update implementation runs,
    and the first post-validation draw must match a fresh execution. *)
let test_update_validator_cannot_consume_workflow_randomness () =
  let scheduler = Scheduler.create () in
  let context =
    Workflow_context_store.create ~randomness_seed:"314159" scheduler
  in
  let validator_error = ref None in
  let implementation_called = ref false in
  let definition =
    Temporal.Update.define ~name:"read_only_random_validator"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let handler =
    Temporal.Update.Handler.make
      ~validator:(fun () ->
        validator_error :=
          Some
            (match Temporal.Workflow.random_int ~bound:97 with
            | Ok _ -> "randomness unexpectedly allowed"
            | Error error -> Temporal.Error.message error);
        Ok ())
      definition (fun () ->
        implementation_called := true;
        Ok ())
  in
  let unit_payload =
    match Temporal.Codec.encode Temporal.Codec.unit () with
    | Ok payload -> payload
    | Error error -> failwith (Temporal.Error.message error)
  in
  let dispatch () = Temporal.Update.Handler.dispatch handler unit_payload in
  let result = Workflow_context_store.with_context context dispatch in
  (match result with
  | Ok _ -> ()
  | Error error ->
      failwith
        ("read-only validator unexpectedly rejected update: "
        ^ Temporal.Error.message error));
  if not !implementation_called then
    failwith "update implementation did not run after validator";
  (match !validator_error with
  | Some message
    when String.equal message
           "workflow randomness is unavailable in a read-only update validator" ->
      ()
  | Some message ->
      failwith ("unexpected validator randomness result: " ^ message)
  | None -> failwith "update validator did not run");
  let after_validation =
    Workflow_context_store.with_context context (fun () ->
        Temporal.Workflow.random_int ~bound:97)
  in
  let fresh_context =
    Workflow_context_store.create ~randomness_seed:"314159" (Scheduler.create ())
  in
  let fresh_first =
    Workflow_context_store.with_context fresh_context (fun () ->
        Temporal.Workflow.random_int ~bound:97)
  in
  if after_validation <> fresh_first then
    failwith "update validator changed the workflow random stream"

(** Verifies the public search-attribute helper emits one deterministic Core
    command with copied payload bytes and leaves normal workflow completion
    ordering intact. *)
let test_workflow_search_attribute_upsert () =
  let workflow =
    Temporal.Workflow.define ~name:"search_attribute_upsert"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let value =
          match Temporal.Codec.encode Temporal.Codec.string "ready" with
          | Ok value -> value
          | Error error -> failwith (Temporal.Error.message error)
        in
        Temporal.Workflow.upsert_search_attributes [ ("status", value) ];
        Ok ())
  in
  let execution = Execution.start workflow () in
  match Execution.activate execution [ Activation.Start_workflow ] with
  | [ Activation.Upsert_search_attributes { search_attributes = [ ("status", value) ] };
      Activation.Complete_workflow _ ] ->
      if Bytes.to_string value.data <> "\"ready\"" then
        failwith "search-attribute payload was not retained"
  | _ -> failwith "search-attribute upsert emitted an unexpected command sequence"

let () =
  test_commands_and_completion ();
  test_activity_options_and_queue ();
  test_invalid_activity_options_do_not_schedule ();
  test_invalid_default_task_queue ();
  test_replay_is_stable ();
  test_resolution_job_order ();
  test_bridge_defects ();
  test_zero_sleep_and_duration_validation ();
  test_start_sleep ();
  test_empty_all_uses_current_workflow_owner ();
  test_child_workflow_completion ();
  test_child_workflow_retry_policy ();
  test_child_workflow_cancellation ();
  test_activity_cancellation ();
  test_default_child_workflow_cancellation_policy ();
  test_child_workflow_concurrency_and_decoding ();
  test_child_workflow_validation ();
  test_child_workflow_id_boundary ();
  test_child_workflow_failures_and_sequence_ownership ();
  test_child_start_conflicting_result_keeps_future_pending ();
  test_activity_cancel_after_natural_completion_is_noop ();
  test_local_activity_backoff_reschedules_without_resolving ();
  test_activity_cancel_after_failure_is_noop ();
  test_activity_cancel_owner_check ();
  test_child_cancel_after_natural_completion_is_noop ();
  test_child_cancel_after_start_failure_is_noop ();
  test_child_resolution_rejections_preserve_lifecycle_state ();
  test_cancel_and_evict ();
  test_continue_as_new_terminal ();
  test_continue_as_new_encode_failure_is_terminal ();
  test_terminal_finally_cannot_emit_cancel ();
  test_continue_as_new_finally_cannot_emit_cancel ();
  test_workflow_patch_decisions ();
  test_workflow_patch_mode_invariant ();
  test_workflow_patch_mode_isolation_and_public_deprecation_snapshot ();
  test_workflow_patch_isolation_and_public_id_snapshot ();
  test_incoming_patch_notification_snapshot ();
  test_workflow_random_int_is_replay_stable ();
  test_update_validator_cannot_consume_workflow_randomness ();
  test_workflow_search_attribute_upsert ()
