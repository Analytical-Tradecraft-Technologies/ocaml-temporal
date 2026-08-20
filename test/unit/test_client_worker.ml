(** Exercises the public client and worker surface without a live Temporal
    server. The [mock://] endpoint remains a deterministic unit-test seam;
    HTTP(S) routing is checked at the native configuration boundary so these
    tests do not require a running Temporal service. *)

(** Converts an expected SDK failure into a readable unit-test diagnostic. *)
let unwrap = function
  | Ok value -> value
  | Error error ->
      failwith
        (Printf.sprintf "%s: %s" (Temporal.Error.kind error)
           (Temporal.Error.message error))

(** Requires a structured failure of the requested category. *)
let expect_error category = function
  | Error error when Temporal.Error.kind error = category -> ()
  | Error error ->
      failwith
        (Printf.sprintf "expected %s, got %s" category
           (Temporal.Error.kind error))
  | Ok _ -> failwith (Printf.sprintf "expected %s error" category)

(** Finds [needle] in [source] without relying on optional string extensions.
    The helper is intentionally small because it is only used for stable error
    fragments in this test executable. *)
let contains_substring source needle =
  let source_length = String.length source in
  let needle_length = String.length needle in
  (* The bounded scan avoids depending on a newer String helper while making
     the assertion stable across the supported OCaml versions. *)
  let rec search index =
    if index + needle_length > source_length then false
    else if String.sub source index needle_length = needle then true
    else search (index + 1)
  in
  if needle_length = 0 then true else search 0

(** Requires a structured failure whose message retains a stable diagnostic
    fragment without coupling the test to the complete native wording. *)
let expect_error_message_contains category fragment = function
  | Error error
    when Temporal.Error.kind error = category
         && contains_substring (Temporal.Error.message error) fragment ->
      ()
  | Error error ->
      failwith
        (Printf.sprintf "expected %s error containing %S, got %s: %s" category
           fragment (Temporal.Error.kind error) (Temporal.Error.message error))
  | Ok _ ->
      failwith
        (Printf.sprintf "expected %s error containing %S" category fragment)

(** A string workflow used to prove that a client handle retains its input and
    output codecs across start and exact-run wait. *)
let echo_workflow =
  Temporal.Workflow.define ~name:"unit.echo"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      Ok input)

(** A typed signal used by client-control tests. The mock transport only
    acknowledges delivery, but the definition still exercises the public
    codec/name boundary used by the native signal request. *)
let add_document_signal =
  Temporal.Signal.define ~name:"unit.add-document" ~input:Temporal.Codec.string

(** Unit definitions keep the worker dispatch test independent of payload
    implementation details while still requiring decoding and re-encoding. *)
let unit_workflow calls =
  Temporal.Workflow.define ~name:"unit.workflow"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
      Atomic.incr calls;
      Ok ())

(** A mock activity whose invocation is observable without process-global
    scheduler state or nondeterministic input. *)
let unit_activity calls =
  Temporal.Activity.define ~name:"unit.activity" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.unit (fun () ->
      Atomic.incr calls;
      Ok ())

(** An asynchronous activity definition used to prove that the deterministic
    mock reports its native-only mode as an activity failure instead of
    invoking a callback without a real completion lease. *)
let unsupported_async_activity =
  Temporal.Activity.define_async ~name:"unit.aaa-async-activity"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun _context () ->
      Temporal.Activity.Completed ())

(** Definitions that return an expected failure. A worker must acknowledge
    these failures to the backend and continue serving later tasks. *)
let failing_workflow =
  Temporal.Workflow.define ~name:"unit.failing-workflow"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
      Error (Temporal.Error.defect ~message:"synthetic workflow failure"))

(** Activity counterpart to [failing_workflow], used to check that an activity
    failure is acknowledged and does not stop later task dispatch. *)
let failing_activity =
  Temporal.Activity.define ~name:"unit.failing-activity"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
      Error (Temporal.Error.defect ~message:"synthetic activity failure"))

(** Duplicate workflow names are rejected before a worker backend is started. *)
let test_duplicate_workflows () =
  let calls = Atomic.make 0 in
  let definition = unit_workflow calls in
  expect_error "defect"
    (Temporal.Worker.create ~target_url:"mock://dispatch"
       ~namespace:"unit-test" ~task_queue:"unit-test"
       ~workflows:
         [ Temporal.Worker.workflow definition; Temporal.Worker.workflow definition ]
       ~activities:[] ())

(** Signal handlers are attached to one workflow registration and duplicate
    names are rejected before the mock or native backend is allocated. This
    test exercises the public ergonomics without needing a live signal task. *)
let test_workflow_signal_registration () =
  let signal =
    Temporal.Signal.define ~name:"unit.signal" ~input:Temporal.Codec.string
  in
  let handler = Temporal.Signal.Handler.make signal (fun _ -> Ok ()) in
  let calls = Atomic.make 0 in
  let workflow = unit_workflow calls in
  let worker =
    unwrap
      (Temporal.Worker.create ~target_url:"mock://dispatch"
         ~namespace:"unit-test" ~task_queue:"unit-test"
         ~workflows:[ Temporal.Worker.workflow ~signals:[ handler ] workflow ]
         ~activities:[] ())
  in
  unwrap (Temporal.Worker.shutdown worker);
  expect_error "defect"
    (Temporal.Worker.create ~target_url:"mock://dispatch"
       ~namespace:"unit-test" ~task_queue:"unit-test"
       ~workflows:
         [ Temporal.Worker.workflow ~signals:[ handler; handler ] workflow ]
       ~activities:[] ())

(** Query handlers use the same public registration ergonomics while retaining
    their output-only callback contract. The mock backend does not synthesize a
    native query activation, so this test focuses on registration ownership and
    duplicate-name rejection; the owner-Domain dispatch path is covered by the
    private native-worker runtime test. *)
let test_workflow_query_registration () =
  let query =
    Temporal.Query.define ~name:"unit.query" ~output:Temporal.Codec.string
  in
  let handler = Temporal.Query.Handler.make query (fun () -> Ok "ready") in
  let calls = Atomic.make 0 in
  let workflow = unit_workflow calls in
  let worker =
    unwrap
      (Temporal.Worker.create ~target_url:"mock://dispatch"
         ~namespace:"unit-test" ~task_queue:"unit-test"
         ~workflows:[ Temporal.Worker.workflow ~queries:[ handler ] workflow ]
         ~activities:[] ())
  in
  unwrap (Temporal.Worker.shutdown worker);
  expect_error "defect"
    (Temporal.Worker.create ~target_url:"mock://dispatch"
       ~namespace:"unit-test" ~task_queue:"unit-test"
       ~workflows:
         [ Temporal.Worker.workflow ~queries:[ handler; handler ] workflow ]
       ~activities:[] ())

(** Duplicate activity names use the same registration invariant as workflows. *)
let test_duplicate_activities () =
  let calls = Atomic.make 0 in
  let definition = unit_activity calls in
  expect_error "defect"
    (Temporal.Worker.create ~target_url:"mock://dispatch"
       ~namespace:"unit-test" ~task_queue:"unit-test" ~workflows:[]
       ~activities:
         [ Temporal.Worker.activity definition; Temporal.Worker.activity definition ]
       ())

(** A remote-only reference cannot be registered as executable worker code. *)
let test_remote_registration_is_rejected () =
  let remote =
    Temporal.Workflow.remote ~name:"unit.remote" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit
  in
  expect_error "defect"
    (Temporal.Worker.create ~target_url:"mock://dispatch"
       ~namespace:"unit-test" ~task_queue:"unit-test"
       ~workflows:[ Temporal.Worker.workflow remote ] ~activities:[] ())

(** The private fake poller supplies one workflow task and one activity task;
    [Worker.run] must decode, dispatch, encode, and acknowledge both before it
    observes the fake shutdown indication. *)
let test_worker_registration_and_dispatch () =
  let workflow_calls = Atomic.make 0 in
  let activity_calls = Atomic.make 0 in
  let workflow = unit_workflow workflow_calls in
  let activity = unit_activity activity_calls in
  let worker =
    unwrap
      (Temporal.Worker.create ~target_url:"mock://dispatch"
         ~namespace:"unit-test" ~task_queue:"unit-test"
         ~workflows:[ Temporal.Worker.workflow workflow ]
         ~activities:[ Temporal.Worker.activity activity ] ())
  in
  unwrap (Temporal.Worker.run worker);
  assert (Atomic.get workflow_calls = 1);
  assert (Atomic.get activity_calls = 1);
  unwrap (Temporal.Worker.shutdown worker);
  unwrap (Temporal.Worker.shutdown worker)

(** A task-level workflow or activity failure is not a worker-level failure:
    later successful tasks must still be dispatched after failure completion. *)
let test_worker_continues_after_task_failure () =
  let workflow_calls = Atomic.make 0 in
  let activity_calls = Atomic.make 0 in
  let worker =
    unwrap
      (Temporal.Worker.create ~target_url:"mock://dispatch"
         ~namespace:"unit-test" ~task_queue:"unit-test"
         ~workflows:
           [ Temporal.Worker.workflow failing_workflow;
             Temporal.Worker.workflow (unit_workflow workflow_calls) ]
         ~activities:
           [ Temporal.Worker.activity failing_activity;
             Temporal.Worker.activity (unit_activity activity_calls) ]
         ())
  in
  unwrap (Temporal.Worker.run worker);
  assert (Atomic.get workflow_calls = 1);
  assert (Atomic.get activity_calls = 1);
  unwrap (Temporal.Worker.shutdown worker)

(** The mock backend cannot provide the native lease required by an
    asynchronous activity, so it acknowledges that task as a typed activity
    failure and continues to the next ordinary activity task. *)
let test_mock_worker_rejects_async_activity_without_stopping () =
  let activity_calls = Atomic.make 0 in
  let worker =
    unwrap
      (Temporal.Worker.create ~target_url:"mock://dispatch"
         ~namespace:"unit-test" ~task_queue:"unit-test" ~workflows:[]
         ~activities:
           [ Temporal.Worker.activity unsupported_async_activity;
             Temporal.Worker.activity (unit_activity activity_calls) ] ())
  in
  unwrap (Temporal.Worker.run worker);
  assert (Atomic.get activity_calls = 1);
  unwrap (Temporal.Worker.shutdown worker)

(** The public run admission gate is shared by mock and native workers. After
    shutdown, attempting to run again must return a typed lifecycle error
    before it can poll a retired transport or silently succeed with no tasks. *)
let test_worker_run_after_shutdown_is_rejected () =
  let worker =
    unwrap
      (Temporal.Worker.create ~target_url:"mock://dispatch"
         ~namespace:"unit-test" ~task_queue:"unit-test" ~workflows:[]
         ~activities:[] ())
  in
  unwrap (Temporal.Worker.shutdown worker);
  expect_error_message_contains "bridge" "worker is shut down"
    (Temporal.Worker.run worker)

(** A client start returns the server-issued run id and [wait] decodes the
    terminal payload using the definition's output codec. *)
let test_typed_start_and_wait_handle () =
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let handle =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~request_id:"unit-start-1" ~task_queue:"unit-test" ~id:"unit-echo"
         ~input:"hello" ())
  in
  assert (Temporal.Client.workflow_id handle = "unit-echo");
  assert (String.length (Temporal.Client.run_id handle) > 0);
  (match Temporal.Client.wait handle with
  | Ok (Temporal.Client.Completed "hello") -> ()
  | Ok _ -> failwith "mock client returned an unexpected terminal result"
  | Error error -> failwith (Temporal.Error.message error));
  unwrap (Temporal.Client.shutdown client);
  unwrap (Temporal.Client.shutdown client);
  expect_error "bridge"
    (Temporal.Client.start client ~workflow:echo_workflow
       ~task_queue:"unit-test" ~id:"after-shutdown" ~input:"ignored" ())

(** The deterministic client seam exposes the same visibility row shape as the
    native adapter. Starting two workflows proves rows retain type, queue,
    exact run identity, and monotone running status before waits complete. *)
let test_client_visibility_listing () =
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  ignore
    (unwrap
       (Temporal.Client.start client ~workflow:echo_workflow
          ~task_queue:"unit-test" ~id:"visibility-a" ~input:"a" ()));
  ignore
    (unwrap
       (Temporal.Client.start client ~workflow:echo_workflow
          ~task_queue:"unit-test" ~id:"visibility-b" ~input:"b" ()));
  let page = unwrap (Temporal.Client.list_visibility client ~query:"" ()) in
  (match page.executions with
  | [ first; second ] ->
      assert (first.workflow_id = "visibility-a");
      assert (second.workflow_id = "visibility-b");
      assert (first.workflow_type = "unit.echo");
      assert (first.task_queue = "unit-test");
      assert (first.status = "running")
  | _ -> failwith "visibility listing returned an unexpected row count");
  expect_error "defect"
    (Temporal.Client.list_visibility client ~page_size:0 ~query:"" ());
  unwrap (Temporal.Client.shutdown client)

(** A continuation identity can be turned back into a typed exact-run handle
    without starting a second execution. Using the mock ledger's existing run
    proves the returned handle retained the workflow's output codec: [wait]
    decodes the payload through [echo_workflow] after [follow] rebuilt it. *)
let test_follow_continued_as_new_handle () =
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let started =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~task_queue:"unit-test" ~id:"unit-follow" ~input:"continued" ())
  in
  let continuation : string Temporal.Client.terminal_result =
    Temporal.Client.Continued_as_new
      {
        namespace = "unit-test";
        workflow_id = Temporal.Client.workflow_id started;
        run_id = Temporal.Client.run_id started;
      }
  in
  let execution =
    match continuation with
    | Temporal.Client.Continued_as_new execution -> execution
    | _ -> failwith "constructed continuation was not a continuation result"
  in
  let followed =
    unwrap (Temporal.Client.follow client ~workflow:echo_workflow execution)
  in
  assert (Temporal.Client.workflow_id followed = "unit-follow");
  assert (Temporal.Client.run_id followed = Temporal.Client.run_id started);
  (match Temporal.Client.wait followed with
  | Ok (Temporal.Client.Completed "continued") -> ()
  | Ok _ -> failwith "followed handle returned an unexpected terminal result"
  | Error error -> failwith (Temporal.Error.message error));
  unwrap (Temporal.Client.shutdown client)

(** A successor identity is validated before it can become a typed handle. The
    same checks cover empty, NUL-containing, and oversized values so malformed
    continuation metadata cannot reach a backend wait or cancellation call. *)
let test_follow_rejects_malformed_successor_identity () =
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let expect_defect execution =
    expect_error "defect"
      (Temporal.Client.follow client ~workflow:echo_workflow execution)
  in
  expect_defect { namespace = "unit-test"; workflow_id = ""; run_id = "run-1" };
  expect_defect { namespace = "unit-test"; workflow_id = "workflow-1"; run_id = "" };
  expect_defect
    { namespace = "unit-test"; workflow_id = "workflow\0001"; run_id = "run-1" };
  expect_defect
    { namespace = "unit-test"; workflow_id = "workflow-1"; run_id = "run\0001" };
  let oversized = String.make 65_537 'x' in
  expect_defect
    { namespace = "unit-test"; workflow_id = oversized; run_id = "run-1" };
  expect_defect
    { namespace = "unit-test"; workflow_id = "workflow-1"; run_id = oversized };
  unwrap (Temporal.Client.shutdown client);
  (* Handle construction observes the same closed-client admission rule as
     [start], [wait], and [cancel]; it must not retain a client graph after
     teardown has begun. *)
  expect_error "bridge"
    (Temporal.Client.follow client ~workflow:echo_workflow
       { namespace = "unit-test"; workflow_id = "workflow-1"; run_id = "run-1" })

(** A continuation identity from another namespace is rejected before a typed
    handle is constructed. This closes the namespace-confusion gap where a
    caller could combine a client for one Temporal namespace with a successor
    execution returned by another client. *)
let test_follow_rejects_cross_namespace_execution () =
  let client_a =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"namespace-a" ())
  in
  expect_error_message_contains "defect" "different namespace"
    (Temporal.Client.follow client_a ~workflow:echo_workflow
       {
         namespace = "namespace-b";
         workflow_id = "workflow-1";
         run_id = "run-1";
       });
  unwrap (Temporal.Client.shutdown client_a)

(** Cancellation is acknowledged separately from waiting. The mock transport
    records the exact run cancellation, makes repeated requests idempotent,
    and exposes the eventual [Cancelled] value through the ordinary wait API. *)
let test_exact_run_cancellation () =
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let handle =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~task_queue:"unit-test" ~id:"unit-cancel" ~input:"ignored" ())
  in
  unwrap
    (Temporal.Client.cancel ~request_id:"cancel-unit-1" ~reason:"unit test"
       handle);
  unwrap
    (Temporal.Client.cancel ~request_id:"cancel-unit-1" ~reason:"unit test"
       handle);
  (match Temporal.Client.wait handle with
  | Ok (Temporal.Client.Cancelled error) ->
      let view = Temporal.Error.view error in
      assert (Temporal.Error.kind error = "cancelled");
      assert (view.category = `Cancelled);
      assert (not view.non_retryable);
      assert (view.message = "workflow execution was cancelled")
  | Ok _ -> failwith "cancelled mock returned a non-cancelled terminal result"
  | Error error -> failwith (Temporal.Error.message error));
  unwrap (Temporal.Client.shutdown client)

(** Termination is distinct from cancellation: the mock records an immutable
    terminated terminal event and repeated requests remain harmless. *)
let test_exact_run_termination () =
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let handle =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~task_queue:"unit-test" ~id:"unit-terminate" ~input:"ignored" ())
  in
  unwrap (Temporal.Client.terminate ~reason:"operator test" handle);
  unwrap (Temporal.Client.terminate ~reason:"operator test" handle);
  (match Temporal.Client.wait handle with
  | Ok (Temporal.Client.Terminated error) ->
      let view = Temporal.Error.view error in
      assert (view.category = `Terminated);
      assert view.non_retryable
  | Ok _ -> failwith "terminated mock returned an unexpected terminal result"
  | Error error -> failwith (Temporal.Error.message error));
  unwrap (Temporal.Client.shutdown client)

(** Reset replaces the exact mock run with a fresh identity while preserving
    the original input. The old exact handle remains addressable and observes
    the terminal reset outcome; callers must rebuild the successor through
    [follow] rather than silently following it. *)
let test_exact_run_reset () =
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let handle =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~task_queue:"unit-test" ~id:"unit-reset" ~input:"reset-me" ())
  in
  let old_run = Temporal.Client.run_id handle in
  let execution =
    unwrap
      (Temporal.Client.reset ~request_id:"reset-unit-1"
         ~reason:"unit reset" ~workflow_task_finish_event_id:4L handle)
  in
  assert (execution.workflow_id = "unit-reset");
  assert (execution.run_id <> old_run);
  (match Temporal.Client.wait handle with
  | Ok (Temporal.Client.Terminated error) ->
      assert ((Temporal.Error.view error).category = `Terminated)
  | Ok _ -> failwith "old reset handle returned the wrong terminal result"
  | Error error -> failwith (Temporal.Error.message error));
  let retried =
    unwrap
      (Temporal.Client.reset ~request_id:"reset-unit-1"
         ~reason:"unit reset" ~workflow_task_finish_event_id:4L handle)
  in
  assert (retried.workflow_id = execution.workflow_id);
  assert (retried.run_id = execution.run_id);
  expect_error "workflow"
    (Temporal.Client.reset ~request_id:"reset-unit-1"
       ~reason:"different reset" ~workflow_task_finish_event_id:4L handle);
  let reset_handle =
    unwrap (Temporal.Client.follow client ~workflow:echo_workflow execution)
  in
  (match Temporal.Client.wait reset_handle with
  | Ok (Temporal.Client.Completed "reset-me") -> ()
  | Ok _ -> failwith "reset run returned an unexpected terminal result"
  | Error error -> failwith (Temporal.Error.message error));
  List.iter
    (fun result ->
      match result with
      | Error _ -> ()
      | Ok _ -> failwith "invalid reset metadata was accepted")
    [
      Temporal.Client.reset ~workflow_task_finish_event_id:(-1L) handle;
      Temporal.Client.reset ~reason:"contains\000nul"
        ~workflow_task_finish_event_id:4L handle;
    ];
  unwrap (Temporal.Client.shutdown client)

(** A late cancellation request cannot rewrite a terminal result that the mock
    has already exposed. This protects the deterministic seam from modelling
    mutable terminal history unlike a real Temporal execution. *)
let test_completed_mock_run_is_immutable () =
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let handle =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~task_queue:"unit-test" ~id:"unit-completed-cancel" ~input:"done"
         ())
  in
  (match Temporal.Client.wait handle with
  | Ok (Temporal.Client.Completed "done") -> ()
  | Ok _ -> failwith "mock completed run returned an unexpected first result"
  | Error error -> failwith (Temporal.Error.message error));
  unwrap
    (Temporal.Client.cancel ~request_id:"late-cancel" ~reason:"too late"
       handle);
  (match Temporal.Client.wait handle with
  | Ok (Temporal.Client.Completed "done") -> ()
  | Ok _ -> failwith "late cancellation rewrote a completed mock run"
  | Error error -> failwith (Temporal.Error.message error));
  unwrap (Temporal.Client.shutdown client)

(** A signal is sent to the exact handle run and acknowledged independently of
    waiting. Repeating an explicit request ID remains accepted by the
    deterministic mock, matching Temporal's retry-safe control operation shape.
    Reusing that ID for different signal data is rejected, so a caller cannot
    accidentally turn an idempotent retry key into an unrelated delivery. A
    signal to an already completed run is rejected rather than silently
    pretending that workflow code could still receive it. *)
let test_exact_run_signal () =
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let handle =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~task_queue:"unit-test" ~id:"unit-signal" ~input:"ignored" ())
  in
  unwrap
    (Temporal.Client.signal ~request_id:"signal-unit-1" handle
       ~signal:add_document_signal ~input:"document");
  expect_error_message_contains "workflow"
    "signal request ID was already used for different signal data"
    (Temporal.Client.signal ~request_id:"signal-unit-1" handle
       ~signal:add_document_signal ~input:"different-document");
  unwrap
    (Temporal.Client.signal ~request_id:"signal-unit-1" handle
       ~signal:add_document_signal ~input:"document");
  (match Temporal.Client.wait handle with
  | Ok (Temporal.Client.Completed "ignored") -> ()
  | Ok _ -> failwith "signal changed the mock terminal result"
  | Error error -> failwith (Temporal.Error.message error));
  (* [follow] only reconstructs a typed handle; the backend must still reject
     a fabricated run ID instead of delivering the signal to the workflow ID's
     current execution. *)
  let mismatched_handle =
    unwrap
      (Temporal.Client.follow client ~workflow:echo_workflow
         {
           namespace = "unit-test";
           workflow_id = "unit-signal";
           run_id = "not-the-started-run";
         })
  in
  expect_error_message_contains "bridge" "run id does not match"
    (Temporal.Client.signal ~request_id:"mismatched-run" mismatched_handle
       ~signal:add_document_signal ~input:"late");
  expect_error "workflow"
    (Temporal.Client.signal ~request_id:"late-signal" handle
       ~signal:add_document_signal ~input:"late");
  unwrap (Temporal.Client.shutdown client)

(** Default signal request IDs are shared across independent client handles.
    The two clients below connect to one deterministic endpoint and rebuild a
    typed handle for the exact execution started by the first client. The mock
    transport rejects reuse of one ID for different signal data, so the second
    call proves that the allocator is process-wide rather than scoped to each
    [Client.t]. *)
let test_default_signal_request_ids_are_process_wide () =
  let client_a =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let client_b =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let handle_a =
    unwrap
      (Temporal.Client.start client_a ~workflow:echo_workflow
         ~task_queue:"unit-test" ~id:"unit-global-signal" ~input:"ignored"
         ())
  in
  let handle_b =
    unwrap
      (Temporal.Client.follow client_b ~workflow:echo_workflow
         {
           namespace = "unit-test";
           workflow_id = Temporal.Client.workflow_id handle_a;
           run_id = Temporal.Client.run_id handle_a;
         })
  in
  unwrap
    (Temporal.Client.signal handle_a ~signal:add_document_signal
       ~input:"first");
  unwrap
    (Temporal.Client.signal handle_b ~signal:add_document_signal
       ~input:"second");
  unwrap (Temporal.Client.shutdown client_b);
  unwrap (Temporal.Client.shutdown client_a)

(** Invalid client settings are values rather than exceptions, and a malformed
    durable workflow id is rejected before the backend receives it. *)
let test_client_validation_errors () =
  expect_error "defect"
    (Temporal.Client.create ~target_url:"not-a-url" ~namespace:"unit-test" ());
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  expect_error "defect"
    (Temporal.Client.start client ~workflow:echo_workflow
       ~task_queue:"unit-test" ~id:"" ~input:"ignored" ());
  expect_error "defect"
    (Temporal.Client.start client ~workflow:echo_workflow ~request_id:""
       ~task_queue:"unit-test" ~id:"valid-id" ~input:"ignored" ());
  expect_error "defect"
    (Temporal.Client.start client ~workflow:echo_workflow
       ~request_id:"contains\000nul" ~task_queue:"unit-test" ~id:"valid-id"
       ~input:"ignored" ());
  let handle =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~task_queue:"unit-test" ~id:"cancel-validation" ~input:"ignored" ())
  in
  expect_error "defect"
    (Temporal.Client.cancel ~request_id:"" handle);
  expect_error "defect"
    (Temporal.Client.cancel ~reason:"contains\000nul" handle);
  expect_error "defect"
    (Temporal.Client.terminate ~reason:"contains\000nul" handle);
  expect_error "defect"
    (Temporal.Client.signal ~request_id:"" handle ~signal:add_document_signal
       ~input:"ignored");
  unwrap (Temporal.Client.shutdown client)

(** Identifier limits are enforced before transport selection. This keeps the
    deterministic mock honest about the native JSON bridge's 65,536-byte
    bound, so an oversized workflow ID or cancellation request cannot pass
    tests locally and fail only after switching to HTTP(S). Definition names
    use the same bound at construction time and are covered by
    [test_definition]. *)
let test_client_identifier_size_validation () =
  let oversized = String.make 65_537 'x' in
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  expect_error "defect"
    (Temporal.Client.start client ~workflow:echo_workflow
       ~task_queue:"unit-test" ~id:oversized ~input:"ignored" ());
  let handle =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~task_queue:"unit-test" ~id:"bounded-cancel" ~input:"ignored" ())
  in
  expect_error "defect"
    (Temporal.Client.cancel ~request_id:oversized handle);
  unwrap (Temporal.Client.shutdown client)

(** Malformed UTF-8 identifiers are rejected by the public client before
    transport selection. The mock backend must enforce the same string
    boundary as the native JSON protocol for both connection configuration and
    workflow operations. *)
let test_client_identifier_utf_8_validation () =
  let malformed_utf_8 = String.make 1 '\255' in
  expect_error_message_contains "defect" "valid UTF-8"
    (Temporal.Client.create ~target_url:"mock://client"
       ~namespace:malformed_utf_8 ());
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  expect_error_message_contains "defect" "valid UTF-8"
    (Temporal.Client.start client ~workflow:echo_workflow
       ~task_queue:"unit-test" ~id:malformed_utf_8 ~input:"ignored" ());
  unwrap (Temporal.Client.shutdown client)

(** An HTTP-shaped endpoint is deliberately handed to the native configuration
    validator rather than the deterministic mock. The malformed host fails
    before a runtime or network connection is allocated, proving the public
    routing decision without needing Temporal Server in this unit executable.
*)
let test_native_client_configuration_boundary () =
  expect_error_message_contains "bridge" "native client configuration failed"
    (Temporal.Client.create ~target_url:"http://" ~namespace:"unit-test" ())

(** The worker task queue is required configuration and is passed through the
    private backend boundary before any worker graph is allocated. *)
let test_worker_validation_errors () =
  expect_error "defect"
    (Temporal.Worker.create ~target_url:"mock://dispatch"
       ~namespace:"unit-test" ~task_queue:"" ~workflows:[] ~activities:[] ())

(** A non-mock endpoint must enter the native configuration boundary rather
    than silently falling back to the deterministic fake. An invalid absolute
    URL fails before a runtime or network resource is allocated, which keeps
    this assertion independent from Temporal Server availability. *)
let test_native_worker_configuration_boundary () =
  expect_error "bridge"
    (Temporal.Worker.create ~target_url:"http://"
       ~namespace:"unit-test" ~task_queue:"unit-test" ~workflows:[]
       ~activities:[] ())

(** Public worker options validate legacy routing metadata before any native
    graph is allocated, retain an immutable cache override, and expose the
    selected mode through an explicit variant. *)
let test_worker_options_versioning () =
  let options =
    match
      Temporal.Worker.Options.make
        ~versioning:(Temporal.Worker.Options.Legacy_build_id "build-v2")
        ~max_cached_workflows:0 ()
    with
    | Ok options -> options
    | Error _ -> failwith "valid legacy worker options were rejected"
  in
  (match Temporal.Worker.Options.versioning options with
  | Temporal.Worker.Options.Legacy_build_id build_id ->
      assert (build_id = "build-v2")
  | Temporal.Worker.Options.No_versioning ->
      failwith "legacy options were changed to unversioned mode"
  | Temporal.Worker.Options.Deployment_based _ ->
      failwith "legacy options were changed to deployment versioning");
  assert (Temporal.Worker.Options.max_cached_workflows options = Some 0);
  expect_error "defect"
    (Temporal.Worker.Options.make
       ~versioning:(Temporal.Worker.Options.Legacy_build_id "") ());
  expect_error "defect"
    (Temporal.Worker.Options.make ~max_cached_workflows:(-1) ())

(** Deployment-based routing keeps the deployment identity and default task
    behavior together, while rejecting a default behavior on an opted-out
    worker before the native bridge can allocate resources. *)
let test_worker_options_deployment_versioning () =
  let options =
    match
      Temporal.Worker.Options.make
        ~versioning:
          (Temporal.Worker.Options.Deployment_based
             {
               deployment_name = "agents";
               build_id = "agents-v3";
               use_worker_versioning = true;
               default_versioning_behavior = Some `Auto_upgrade;
             })
        ()
    with
    | Ok options -> options
    | Error _ -> failwith "valid deployment worker options were rejected"
  in
  (match Temporal.Worker.Options.versioning options with
  | Temporal.Worker.Options.Deployment_based
      {
        deployment_name;
        build_id;
        use_worker_versioning;
        default_versioning_behavior = Some `Auto_upgrade;
      } ->
      assert (deployment_name = "agents");
      assert (build_id = "agents-v3");
      assert use_worker_versioning
  | _ -> failwith "deployment worker options changed during construction");
  expect_error "defect"
    (Temporal.Worker.Options.make
       ~versioning:
         (Temporal.Worker.Options.Deployment_based
            {
              deployment_name = "agents";
              build_id = "agents-v3";
              use_worker_versioning = false;
              default_versioning_behavior = Some `Pinned;
            })
       ())

(** Runs all public worker and client regression assertions. *)
let () =
  test_duplicate_workflows ();
  test_workflow_signal_registration ();
  test_workflow_query_registration ();
  test_duplicate_activities ();
  test_remote_registration_is_rejected ();
  test_worker_registration_and_dispatch ();
  test_worker_continues_after_task_failure ();
  test_mock_worker_rejects_async_activity_without_stopping ();
  test_worker_run_after_shutdown_is_rejected ();
  test_typed_start_and_wait_handle ();
  test_client_visibility_listing ();
  test_follow_continued_as_new_handle ();
  test_follow_rejects_malformed_successor_identity ();
  test_follow_rejects_cross_namespace_execution ();
  test_exact_run_cancellation ();
  test_exact_run_reset ();
  test_exact_run_termination ();
  test_completed_mock_run_is_immutable ();
  test_exact_run_signal ();
  test_default_signal_request_ids_are_process_wide ();
  test_client_validation_errors ();
  test_client_identifier_size_validation ();
  test_client_identifier_utf_8_validation ();
  test_native_client_configuration_boundary ();
  test_worker_validation_errors ();
  test_native_worker_configuration_boundary ();
  test_worker_options_versioning ();
  test_worker_options_deployment_versioning ()
