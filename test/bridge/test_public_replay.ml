(** Offline replay through the public [Temporal.Replay] API only.

    The fixtures are the retained live task-failure histories under
    [test/integration/temporal/task_failure/histories]. No Temporal Server is
    involved: each call drives pinned Temporal Core's replay worker through
    the private native bridge and the production workflow adapter. *)

module R = Temporal.Replay

(** Reads one binary fixture declared as a Dune dependency of this test. *)
let read_fixture name =
  In_channel.with_open_bin
    (Filename.concat "../integration/temporal/task_failure/histories"
       (name ^ ".pb"))
    In_channel.input_all

(** Builds the history recorded for the live fixture case [name]. *)
let history name =
  match
    R.History.of_protobuf ~workflow_id:("task-failure-" ^ name)
      (read_fixture name)
  with
  | Ok history -> history
  | Error error -> failwith (Temporal.Error.message error)

(** The workflow type used by every task-failure fixture case. *)
let workflow_type name = "task-failure." ^ name

(** Defines a [unit -> string] workflow under a fixture's recorded type. *)
let define name implementation =
  Temporal.Workflow.define ~name:(workflow_type name)
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string implementation

(** The corrected source generation recorded in [body] and [encoder]: one
    100 ms timer, then a result. *)
let compatible name =
  define name (fun () ->
      let open Temporal.Result_syntax in
      let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 100L) in
      Ok "recovered")

(** An incompatible change: the recorded timer was removed, so the first
    workflow task completes the workflow instead of starting the timer. *)
let timer_removed name = define name (fun () -> Ok "recovered")

(** An incompatible change: an activity is scheduled where the history
    recorded a timer. *)
let activity_instead_of_timer name =
  let activity =
    Temporal.Activity.remote ~name:"replay-test.activity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
  in
  define name (fun () ->
      Temporal.Activity.execute
        ~start_to_close_timeout:(Temporal.Duration.of_ms 5_000L)
        activity ())

(** A deliberate business failure, matching the [business-*] fixtures. *)
let business name non_retryable =
  define name (fun () ->
      Error
        (Temporal.Error.make ~category:`Workflow ~non_retryable
           ~message:"intentional business failure" ()))

(** A workflow whose code fails its task with a defect on replay. *)
let defective name =
  define name (fun () ->
      Error (Temporal.Error.defect ~message:"replay-test defect"))

(** Fails the test with the public diagnostic of an unexpected result. *)
let expect_ok label = function
  | Ok () -> ()
  | Error failure ->
      failwith (label ^ ": expected Ok, got " ^ R.failure_message failure)

(** Describes a result in failure messages. *)
let describe = function
  | Ok () -> "Ok"
  | Error failure -> R.failure_message failure

(** Requires a [Nondeterminism] result with a non-empty Core description. *)
let expect_nondeterminism label result =
  match result with
  | Error (R.Nondeterminism { run_id; message }) ->
      if run_id = "" || message = "" then
        failwith (label ^ ": nondeterminism diagnostic is empty")
  | _ -> failwith (label ^ ": expected nondeterminism, got " ^ describe result)

(** Requires a [Workflow_task_failed] result. *)
let expect_task_failed label result =
  match result with
  | Error (R.Workflow_task_failed { message; _ }) ->
      if message = "" then failwith (label ^ ": task failure message is empty")
  | _ ->
      failwith (label ^ ": expected workflow task failure, got " ^ describe result)

(** Requires an [Invalid_history] result. *)
let expect_invalid label result =
  match result with
  | Error (R.Invalid_history _) -> ()
  | _ -> failwith (label ^ ": expected invalid history, got " ^ describe result)

(** Requires a [Replay_error] result. *)
let expect_replay_error label result =
  match result with
  | Error (R.Replay_error _) -> ()
  | _ -> failwith (label ^ ": expected replay error, got " ^ describe result)

(** Compatible code replays recorded histories, including histories with
    earlier failed workflow tasks and deliberately failed executions. *)
let test_compatible_histories () =
  List.iter
    (fun name ->
      expect_ok name
        (R.replay ~workflows:[ R.workflow (compatible name) ] (history name)))
    [ "body"; "encoder" ];
  expect_ok "missing"
    (R.replay
       ~workflows:[ R.workflow (timer_removed "missing") ]
       (history "missing"));
  expect_ok "business-retryable"
    (R.replay
       ~workflows:[ R.workflow (business "business-retryable" false) ]
       (history "business-retryable"));
  expect_ok "business-permanent"
    (R.replay
       ~workflows:[ R.workflow (business "business-permanent" true) ]
       (history "business-permanent"))

(** Changed command sequences are reported as nondeterminism. *)
let test_incompatible_changes () =
  expect_nondeterminism "timer removed"
    (R.replay ~workflows:[ R.workflow (timer_removed "body") ] (history "body"));
  expect_nondeterminism "activity instead of timer"
    (R.replay
       ~workflows:[ R.workflow (activity_instead_of_timer "body") ]
       (history "body"))

(** Failing workflow code and a missing registration are task failures,
    distinct from nondeterminism and from invalid input. *)
let test_task_failures () =
  expect_task_failed "defect"
    (R.replay ~workflows:[ R.workflow (defective "body") ] (history "body"));
  expect_task_failed "unregistered type"
    (R.replay
       ~workflows:[ R.workflow (compatible "encoder") ]
       (history "body"))

(** Malformed input is rejected before or by the bridge, never as a verdict
    about workflow code. *)
let test_invalid_input () =
  let of_protobuf ?(workflow_id = "invalid") bytes =
    R.History.of_protobuf ~workflow_id bytes
  in
  let rejected label = function
    | Error _ -> ()
    | Ok _ -> failwith (label ^ ": invalid history input was accepted")
  in
  rejected "empty history" (of_protobuf "");
  rejected "empty workflow ID" (of_protobuf ~workflow_id:"" "x");
  rejected "NUL workflow ID" (of_protobuf ~workflow_id:"a\000b" "x");
  rejected "non-UTF-8 workflow ID" (of_protobuf ~workflow_id:"\xff" "x");
  rejected "oversized workflow ID"
    (of_protobuf ~workflow_id:(String.make 65_537 'w') "x");
  let replay_bytes label bytes =
    match of_protobuf bytes with
    | Error error -> failwith (label ^ ": " ^ Temporal.Error.message error)
    | Ok history ->
        R.replay ~workflows:[ R.workflow (compatible "body") ] history
  in
  expect_invalid "not protobuf" (replay_bytes "not protobuf" "\xff\xff\xff\xff");
  let body = read_fixture "body" in
  expect_invalid "truncated"
    (replay_bytes "truncated" (String.sub body 0 (String.length body / 2)));
  (* A well-formed protobuf with no events fails Core's invariant checks. *)
  expect_invalid "no events" (replay_bytes "no events" "\x12\x00")

(** Registration problems are reported before native resources exist. *)
let test_registration_errors () =
  expect_replay_error "duplicate"
    (R.replay
       ~workflows:[ R.workflow (compatible "body"); R.workflow (compatible "body") ]
       (history "body"));
  expect_replay_error "remote"
    (R.replay
       ~workflows:
         [
           R.workflow
             (Temporal.Workflow.remote ~name:(workflow_type "body")
                ~input:Temporal.Codec.unit ~output:Temporal.Codec.string);
         ]
       (history "body"));
  expect_replay_error "invalid task queue"
    (R.replay ~task_queue:"" ~workflows:[ R.workflow (compatible "body") ]
       (history "body"))

(** [replay_all] keeps every history's own verdict in input order. *)
let test_replay_all () =
  let workflows = [ R.workflow (timer_removed "body"); R.workflow (compatible "encoder") ] in
  match R.replay_all ~workflows [ history "body"; history "encoder" ] with
  | [ (first, first_result); (second, second_result) ] ->
      if R.History.workflow_id first <> "task-failure-body" then
        failwith "replay_all reordered histories";
      if R.History.workflow_id second <> "task-failure-encoder" then
        failwith "replay_all reordered histories";
      expect_nondeterminism "replay_all body" first_result;
      expect_ok "replay_all encoder" second_result
  | _ -> failwith "replay_all returned the wrong number of results"

(** More sequential replays than OCaml permits simultaneous Domains (128).
    Each call spawns a supervisor Domain, so a leaked graph would make a
    later call fail to start; alternating verdicts covers both cleanup
    paths. *)
let test_repeated_cleanup () =
  let compatible_workflows = [ R.workflow (compatible "body") ] in
  let changed_workflows = [ R.workflow (timer_removed "body") ] in
  let body = history "body" in
  for iteration = 1 to 140 do
    if iteration mod 2 = 0 then
      expect_ok "repeated compatible" (R.replay ~workflows:compatible_workflows body)
    else
      expect_nondeterminism "repeated changed"
        (R.replay ~workflows:changed_workflows body)
  done

let () =
  test_compatible_histories ();
  test_incompatible_changes ();
  test_task_failures ();
  test_invalid_input ();
  test_registration_errors ();
  test_replay_all ();
  test_repeated_cleanup ()
