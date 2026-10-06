(** Behavioral coverage for [Temporal.Testing], the in-process time-skipping
    workflow test environment. Every test drives real workflow code through
    the public API only: timers must be skipped in virtual time, activities
    and child workflows must run (or be replaced by stubs), signals, queries,
    updates, cancellation, and continue-as-new must reach the workflow, and
    failures must surface as typed errors instead of hangs. *)

module T = Temporal
open T.Result_syntax

(** Fails the test with [label] when two values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Unwraps a successful result, failing the test with the error message. *)
let ok label = function
  | Ok value -> value
  | Error error -> failwith (label ^ " failed: " ^ T.Error.message error)

(** Asserts that a result failed with [category] and returns the error. *)
let expect_error label category = function
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")
  | Error error ->
      if (T.Error.view error).category <> category then
        failwith
          (Printf.sprintf "%s: unexpected %s error: %s" label
             (T.Error.kind error) (T.Error.message error));
      error

(** Asserts that [needle] occurs in [haystack], for diagnostics whose exact
    wording is not part of the contract. *)
let expect_contains label ~needle haystack =
  let needle_length = String.length needle in
  let rec search index =
    index + needle_length <= String.length haystack
    && (String.equal (String.sub haystack index needle_length) needle
       || search (index + 1))
  in
  if not (search 0) then
    failwith (Printf.sprintf "%s: %S does not contain %S" label haystack needle)

(** Creates an environment, runs [body], and always shuts it down. *)
let with_environment ?start_time ?max_activity_attempts ~workflows ~activities
    body =
  let environment =
    ok "create"
      (T.Testing.create ?start_time ?max_activity_attempts ~workflows
         ~activities ())
  in
  Fun.protect
    ~finally:(fun () -> T.Testing.shutdown environment)
    (fun () -> body environment)

(** Milliseconds since the Unix epoch of a public instant. *)
let epoch_ms time =
  Int64.add
    (Int64.mul (T.Time.seconds time) 1_000L)
    (Int64.of_int (T.Time.nanoseconds time / 1_000_000))

let one_day = T.Duration.of_ms 86_400_000L

(** Sleeps for a day and reports the workflow clock before and after, so the
    test can prove the timer fired in virtual rather than wall-clock time. *)
let sleepy_workflow =
  T.Workflow.define ~name:"sleepy" ~input:T.Codec.unit ~output:T.Codec.int64
    (fun () ->
      let* before = T.Workflow.now () in
      let* () = T.Workflow.sleep one_day in
      let* after = T.Workflow.now () in
      Ok (Int64.sub (epoch_ms after) (epoch_ms before)))

(** A day-long timer completes immediately and advances only virtual time. *)
let test_timer_skipping () =
  let start_time = ok "start time" (T.Time.of_unix ~seconds:1_000L ~nanoseconds:0) in
  with_environment ~start_time
    ~workflows:[ T.Testing.workflow sleepy_workflow ]
    ~activities:[]
    (fun environment ->
      let wall_start = Unix.gettimeofday () in
      let elapsed = ok "execute" (T.Testing.execute environment sleepy_workflow ()) in
      expect "workflow observed one virtual day" 86_400_000L elapsed;
      expect "environment clock advanced one day"
        (Int64.add 1_000_000L 86_400_000L)
        (epoch_ms (T.Testing.now environment));
      if Unix.gettimeofday () -. wall_start > 5.0 then
        failwith "timer skipping waited for wall-clock time")

(** The real implementation of the greeting activity. *)
let compose_greeting =
  T.Activity.define ~name:"compose_greeting" ~input:T.Codec.string
    ~output:T.Codec.string (fun name -> Ok ("Hello, " ^ name))

(** Calls [compose_greeting] once and returns its result. *)
let greeting_workflow =
  T.Workflow.define ~name:"greeting" ~input:T.Codec.string
    ~output:T.Codec.string (fun name ->
      T.Activity.execute compose_greeting name)

(** Registered activities run, and a mock replaces the real implementation
    without calling it. *)
let test_activity_and_override () =
  with_environment
    ~workflows:[ T.Testing.workflow greeting_workflow ]
    ~activities:[ T.Testing.activity compose_greeting ]
    (fun environment ->
      expect "real activity result" "Hello, OCaml"
        (ok "execute" (T.Testing.execute environment greeting_workflow "OCaml")));
  let calls = ref [] in
  with_environment
    ~workflows:[ T.Testing.workflow greeting_workflow ]
    ~activities:
      [
        T.Testing.activity compose_greeting;
        T.Testing.mock_activity compose_greeting (fun name ->
            calls := name :: !calls;
            Ok ("stubbed " ^ name));
      ]
    (fun environment ->
      expect "mocked activity result" "stubbed OCaml"
        (ok "execute" (T.Testing.execute environment greeting_workflow "OCaml"));
      expect "mock received the encoded input" [ "OCaml" ] !calls)

(** A remote activity reference only, implemented by another worker. *)
let remote_lookup =
  T.Activity.remote ~name:"remote_lookup" ~input:T.Codec.string
    ~output:T.Codec.int

(** Calls the remote activity and returns its result. *)
let lookup_workflow =
  T.Workflow.define ~name:"lookup" ~input:T.Codec.string ~output:T.Codec.int
    (fun key -> T.Activity.execute remote_lookup key)

(** A remote reference cannot run in-process: it fails with a reason naming
    the fix, and a mock makes it runnable. An unregistered activity fails
    instead of hanging. *)
let test_remote_and_missing_activities () =
  with_environment
    ~workflows:[ T.Testing.workflow lookup_workflow ]
    ~activities:[ T.Testing.activity remote_lookup ]
    (fun environment ->
      let error =
        expect_error "remote activity" `Activity
          (T.Testing.execute environment lookup_workflow "key")
      in
      expect_contains "remote activity reason" ~needle:"mock_activity"
        (T.Error.message error));
  with_environment
    ~workflows:[ T.Testing.workflow lookup_workflow ]
    ~activities:[]
    (fun environment ->
      let error =
        expect_error "missing activity" `Activity
          (T.Testing.execute environment lookup_workflow "key")
      in
      expect_contains "missing activity reason" ~needle:"not registered"
        (T.Error.message error));
  with_environment
    ~workflows:[ T.Testing.workflow lookup_workflow ]
    ~activities:[ T.Testing.mock_activity remote_lookup (fun key -> Ok (String.length key)) ]
    (fun environment ->
      expect "mocked remote activity" 3
        (ok "execute" (T.Testing.execute environment lookup_workflow "key")))

(** Fails retryably until its third attempt, recording each attempt number
    from the activity context. *)
let flaky_attempts = ref []

let flaky =
  T.Activity.define_with_context ~name:"flaky" ~input:T.Codec.unit
    ~output:T.Codec.int (fun context () ->
      let attempt =
        match T.Activity.Context.info context with
        | Ok info -> T.Activity.Info.attempt info
        | Error error -> failwith (T.Error.message error)
      in
      flaky_attempts := attempt :: !flaky_attempts;
      if attempt < 3 then
        Error (T.Error.make ~category:`Activity ~message:"transient" ())
      else Ok attempt)

(** A one-second, doubling retry policy with a bounded attempt budget. *)
let retry_policy ~maximum_attempts =
  ok "retry policy"
    (T.Activity.Retry_policy.make
       ~initial_interval:(T.Duration.of_ms 1_000L)
       ~backoff_coefficient:2.0
       ~maximum_interval:(T.Duration.of_ms 60_000L)
       ~maximum_attempts ())

(** Runs [flaky] with [maximum_attempts] and returns the attempt that
    succeeded. *)
let flaky_workflow =
  T.Workflow.define ~name:"flaky_workflow" ~input:T.Codec.int
    ~output:T.Codec.int (fun maximum_attempts ->
      T.Activity.execute ~retry_policy:(retry_policy ~maximum_attempts) flaky ())

(** Retries follow the policy in virtual time; an exhausted budget surfaces
    the last failure as an [Activity] error. *)
let test_activity_retries () =
  with_environment
    ~workflows:[ T.Testing.workflow flaky_workflow ]
    ~activities:[ T.Testing.activity flaky ]
    (fun environment ->
      flaky_attempts := [];
      let started = epoch_ms (T.Testing.now environment) in
      expect "succeeded on third attempt" 3
        (ok "execute" (T.Testing.execute environment flaky_workflow 5));
      expect "attempt numbers" [ 3; 2; 1 ] !flaky_attempts;
      expect "backoff of 1s then 2s in virtual time" 3_000L
        (Int64.sub (epoch_ms (T.Testing.now environment)) started);
      flaky_attempts := [];
      let error =
        expect_error "exhausted retries" `Activity
          (T.Testing.execute environment flaky_workflow 2)
      in
      expect "two attempts made" [ 2; 1 ] !flaky_attempts;
      expect_contains "final failure message" ~needle:"transient"
        (T.Error.message error))

(** Fails non-retryably with an application error type and a detail. *)
let rejecting =
  T.Activity.define ~name:"rejecting" ~input:T.Codec.unit ~output:T.Codec.unit
    (fun () ->
      Error
        (T.Error.make ~non_retryable:true ~error_type:"InvalidInput"
           ~category:`Activity ~message:"bad input" ()))

(** Propagates the activity failure as the workflow failure. *)
let rejecting_workflow =
  T.Workflow.define ~name:"rejecting_workflow" ~input:T.Codec.unit
    ~output:T.Codec.unit (fun () -> T.Activity.execute rejecting ())

(** Raises, which the workflow contract treats as a task failure. *)
let raising_workflow =
  T.Workflow.define ~name:"raising_workflow" ~input:T.Codec.unit
    ~output:T.Codec.unit (fun () -> failwith "workflow bug")

(** Activity failures keep their application type, and a workflow defect is
    reported as a defect instead of being retried forever. *)
let test_error_propagation () =
  with_environment
    ~workflows:
      [ T.Testing.workflow rejecting_workflow; T.Testing.workflow raising_workflow ]
    ~activities:[ T.Testing.activity rejecting ]
    (fun environment ->
      let error =
        expect_error "activity failure" `Activity
          (T.Testing.execute environment rejecting_workflow ())
      in
      expect "application type preserved" (Some "InvalidInput")
        (T.Error.error_type error);
      expect "non-retryable preserved" true (T.Error.view error).non_retryable;
      let error =
        expect_error "workflow defect" `Defect
          (T.Testing.execute environment raising_workflow ())
      in
      expect_contains "defect message" ~needle:"workflow bug"
        (T.Error.message error))

(** A child that sleeps before doubling its input. *)
let doubling_child =
  T.Workflow.define ~name:"doubling_child" ~input:T.Codec.int
    ~output:T.Codec.int (fun value ->
      let* () = T.Workflow.sleep (T.Duration.of_ms 60_000L) in
      Ok (value * 2))

(** A child implemented by another worker. *)
let remote_child =
  T.Workflow.remote ~name:"remote_child" ~input:T.Codec.int ~output:T.Codec.int

(** Runs both children concurrently and sums their results. *)
let parent_workflow =
  T.Workflow.define ~name:"parent" ~input:T.Codec.int ~output:T.Codec.int
    (fun value ->
      let local = T.Child_workflow.start ~id:"child-local" doubling_child value in
      let remote = T.Child_workflow.start ~id:"child-remote" remote_child value in
      let* doubled = T.Future.await local in
      let* stubbed = T.Future.await remote in
      Ok (doubled + stubbed))

(** Child workflows run in the environment, a stub replaces a remote child,
    and an unregistered child fails the parent's start. *)
let test_child_workflows () =
  with_environment
    ~workflows:
      [
        T.Testing.workflow parent_workflow;
        T.Testing.workflow doubling_child;
        T.Testing.mock_workflow remote_child (fun value -> Ok (value + 100));
      ]
    ~activities:[]
    (fun environment ->
      expect "child results combined" 130
        (ok "execute" (T.Testing.execute environment parent_workflow 10)));
  with_environment
    ~workflows:
      [ T.Testing.workflow parent_workflow; T.Testing.workflow doubling_child ]
    ~activities:[]
    (fun environment ->
      let error =
        expect_error "unregistered child" `Child_workflow
          (T.Testing.execute environment parent_workflow 10)
      in
      expect_contains "child start failure" ~needle:"remote_child"
        (T.Error.message error))

(** Cancels a running child through its scope and reports how the child's
    future settled. *)
let cancelling_parent =
  T.Workflow.define ~name:"cancelling_parent" ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      let* scope = T.Scope.create () in
      let child = T.Child_workflow.start ~scope ~id:"to-cancel" doubling_child 1 in
      let* () = T.Workflow.sleep (T.Duration.of_ms 1_000L) in
      let* () = T.Scope.cancel scope in
      match T.Future.await child with
      | Error error when (T.Error.view error).category = `Cancelled ->
          Ok "child cancelled"
      | Ok _ -> Ok "child completed"
      | Error error -> Error error)

(** A scope cancellation reaches the child before its timer fires. *)
let test_child_cancellation () =
  with_environment
    ~workflows:
      [ T.Testing.workflow cancelling_parent; T.Testing.workflow doubling_child ]
    ~activities:[]
    (fun environment ->
      expect "child cancelled" "child cancelled"
        (ok "execute" (T.Testing.execute environment cancelling_parent ()));
      expect "child timer never fired" 1_000L
        (Int64.sub (epoch_ms (T.Testing.now environment)) 1_704_067_200_000L))

(** Items appended by the [add_item] signal and update, per run. *)
let items : string list T.Workflow_context.Local.t =
  T.Workflow_context.Local.create ()

(** Reads the run's items, defaulting to none. *)
let current_items () =
  match T.Workflow_context.Local.get items with
  | Ok (Some value) -> value
  | Ok None -> []
  | Error error -> failwith (T.Error.message error)

let add_item_signal = T.Signal.define ~name:"add_item" ~input:T.Codec.string
let close_signal = T.Signal.define ~name:"close" ~input:T.Codec.unit
let items_query = T.Query.define ~name:"items" ~output:T.Codec.(option string)

let count_query =
  T.Query.define_with_input ~name:"count_with_prefix" ~input:T.Codec.string
    ~output:T.Codec.int

let add_item_update =
  T.Update.define ~name:"add_item_update" ~input:T.Codec.string
    ~output:T.Codec.int

(** Whether [close] has been received in this run. *)
let closed : bool T.Workflow_context.Local.t = T.Workflow_context.Local.create ()

(** Waits until it is closed, then returns the items joined in arrival order. *)
let cart_workflow =
  T.Workflow.define ~name:"cart" ~input:T.Codec.unit ~output:T.Codec.string
    (fun () ->
      let* () = T.Workflow_context.Local.set items [] in
      let* () =
        T.Condition.wait_until (fun () ->
            match T.Workflow_context.Local.get closed with
            | Ok (Some true) -> true
            | _ -> false)
      in
      Ok (String.concat "," (List.rev (current_items ()))))

(** Appends one item to the run's state. *)
let add_item item = T.Workflow_context.Local.set items (item :: current_items ())

(** The cart's registration, with every interaction handler. *)
let cart_registration =
  T.Testing.workflow cart_workflow
    ~signals:
      [
        T.Signal.Handler.make add_item_signal add_item;
        T.Signal.Handler.make close_signal (fun () ->
            T.Workflow_context.Local.set closed true);
      ]
    ~queries:
      [
        T.Query.Handler.make items_query (fun () ->
            Ok (match current_items () with [] -> None | last :: _ -> Some last));
        T.Query.Handler.make_with_input count_query (fun prefix ->
            Ok
              (List.length
                 (List.filter
                    (fun item -> String.starts_with ~prefix item)
                    (current_items ()))));
      ]
    ~updates:
      [
        T.Update.Handler.make
          ~validator:(fun item ->
            if String.equal item "" then
              Error (T.Error.make ~category:`Update ~message:"empty item" ())
            else Ok ())
          add_item_update
          (fun item ->
            let* () = add_item item in
            Ok (List.length (current_items ())));
      ]

(** Signals, typed queries, and validated updates reach the workflow, and
    a workflow waiting for a signal that never comes is reported as blocked. *)
let test_signals_queries_updates () =
  with_environment ~workflows:[ cart_registration ] ~activities:[]
    (fun environment ->
      let handle = ok "start" (T.Testing.start ~id:"cart-1" environment cart_workflow ()) in
      expect "workflow ID" "cart-1" (T.Testing.workflow_id handle);
      expect "empty query" None (ok "query" (T.Testing.query handle items_query));
      ok "signal" (T.Testing.signal handle add_item_signal "apple");
      expect "update result" 2
        (ok "update" (T.Testing.update handle add_item_update "avocado"));
      let error =
        expect_error "validator rejection" `Update
          (T.Testing.update handle add_item_update "")
      in
      expect_contains "rejection message" ~needle:"empty item"
        (T.Error.message error);
      ok "signal" (T.Testing.signal handle add_item_signal "banana");
      expect "latest item" (Some "banana")
        (ok "query" (T.Testing.query handle items_query));
      expect "query with input" 2
        (ok "query with input"
           (T.Testing.query_with_input handle count_query "a"));
      let error =
        expect_error "blocked workflow" `Defect (T.Testing.result handle)
      in
      expect_contains "blocked diagnostic" ~needle:"blocked"
        (T.Error.message error);
      ok "close" (T.Testing.signal handle close_signal ());
      expect "joined items" "apple,avocado,banana"
        (ok "result" (T.Testing.result handle));
      expect "closed workflow stays queryable" (Some "banana")
        (ok "query after close" (T.Testing.query handle items_query));
      ignore
        (expect_error "signal after close" `Defect
           (T.Testing.signal handle add_item_signal "late")))

(** Progress of [progress_workflow], readable by query. *)
let stage : string T.Workflow_context.Local.t = T.Workflow_context.Local.create ()

let stage_query = T.Query.define ~name:"stage" ~output:T.Codec.string

(** Moves through stages separated by ten-second timers. *)
let progress_workflow =
  T.Workflow.define ~name:"progress" ~input:T.Codec.unit ~output:T.Codec.string
    (fun () ->
      let* () = T.Workflow_context.Local.set stage "waiting" in
      let* () = T.Workflow.sleep (T.Duration.of_ms 10_000L) in
      let* () = T.Workflow_context.Local.set stage "halfway" in
      let* () = T.Workflow.sleep (T.Duration.of_ms 10_000L) in
      let* () = T.Workflow_context.Local.set stage "done" in
      Ok "done")

(** Reads the progress stage. *)
let read_stage () =
  match T.Workflow_context.Local.get stage with
  | Ok (Some value) -> Ok value
  | Ok None -> Ok "unset"
  | Error error -> Error error

(** [skip] exposes intermediate states; [timeout] bounds the virtual time a
    result may skip; cancellation is reported as [Cancelled]. *)
let test_skip_timeout_and_cancel () =
  let registration =
    T.Testing.workflow progress_workflow
      ~queries:[ T.Query.Handler.make stage_query read_stage ]
  in
  with_environment ~workflows:[ registration ] ~activities:[]
    (fun environment ->
      let handle = ok "start" (T.Testing.start environment progress_workflow ()) in
      expect "initial stage" "waiting" (ok "query" (T.Testing.query handle stage_query));
      ok "skip" (T.Testing.skip environment (T.Duration.of_ms 9_999L));
      expect "before the first timer" "waiting"
        (ok "query" (T.Testing.query handle stage_query));
      ok "skip" (T.Testing.skip environment (T.Duration.of_ms 1L));
      expect "after the first timer" "halfway"
        (ok "query" (T.Testing.query handle stage_query));
      ignore
        (expect_error "virtual timeout" `Timeout
           (T.Testing.result ~timeout:(T.Duration.of_ms 5_000L) handle));
      expect "result after timeout" "done" (ok "result" (T.Testing.result handle)));
  with_environment ~workflows:[ registration ] ~activities:[]
    (fun environment ->
      let handle = ok "start" (T.Testing.start environment progress_workflow ()) in
      ok "cancel" (T.Testing.cancel handle);
      ignore (expect_error "cancelled" `Cancelled (T.Testing.result handle)))

(** Counts down through continue-as-new, returning the final run's input. *)
let rec countdown_workflow =
  lazy
    (T.Workflow.define ~name:"countdown" ~input:T.Codec.int ~output:T.Codec.string
       (fun remaining ->
         if remaining = 0 then
           let* info = T.Workflow.info () in
           Ok
             (Printf.sprintf "%s/%s" (T.Workflow.Info.run_id info)
                (Option.value ~default:""
                   (T.Workflow.Info.first_execution_run_id info)))
         else T.Workflow.continue_as_new (Lazy.force countdown_workflow) (remaining - 1)))

(** Continue-as-new starts successor runs and the handle follows them. *)
let test_continue_as_new () =
  let countdown = Lazy.force countdown_workflow in
  with_environment ~workflows:[ T.Testing.workflow countdown ] ~activities:[]
    (fun environment ->
      let handle = ok "start" (T.Testing.start environment countdown 3) in
      (* The runs contain no timers, so [start] already ran every successor;
         the handle follows the chain to its fourth and final run, which
         still reports the chain's first run. *)
      expect "handle follows the latest run" "test-run-4" (T.Testing.run_id handle);
      expect "final run and first run" "test-run-4/test-run-1"
        (ok "result" (T.Testing.result handle)))

(** Combines virtual time, workflow identity, and the replay-safe random
    stream into one observable string. *)
let fingerprint_workflow =
  T.Workflow.define ~name:"fingerprint" ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      let* () = T.Workflow.sleep (T.Duration.of_ms 1_500L) in
      let* now = T.Workflow.now () in
      let* random = T.Workflow.random_int ~bound:1_000_000 in
      let* info = T.Workflow.info () in
      let* greeting = T.Activity.execute compose_greeting "again" in
      Ok
        (Printf.sprintf "%Ld %d %s %s %s %s %s" (epoch_ms now) random
           (T.Workflow.Info.workflow_id info)
           (T.Workflow.Info.run_id info)
           (T.Workflow.Info.namespace info)
           (T.Workflow.Info.task_queue info)
           greeting))

(** Two fresh environments produce identical observations, so tests built on
    the environment are reproducible. *)
let test_determinism () =
  let run () =
    with_environment
      ~workflows:[ T.Testing.workflow fingerprint_workflow ]
      ~activities:[ T.Testing.activity compose_greeting ]
      (fun environment -> ok "execute" (T.Testing.execute environment fingerprint_workflow ()))
  in
  let first = run () in
  expect "repeated run is identical" first (run ());
  expect_contains "default namespace and queue" ~needle:"default temporal-testing"
    first

(** Duplicate registrations are rejected unless they are mocks, and a
    workflow ID cannot be started twice while open. *)
let test_registration_rules () =
  (match
     T.Testing.create
       ~workflows:[ T.Testing.workflow greeting_workflow; T.Testing.workflow greeting_workflow ]
       ~activities:[] ()
   with
  | Ok environment ->
      T.Testing.shutdown environment;
      failwith "duplicate workflow registration was accepted"
  | Error error -> expect_contains "duplicate" ~needle:"duplicate" (T.Error.message error));
  with_environment ~workflows:[ cart_registration ] ~activities:[]
    (fun environment ->
      ignore (ok "start" (T.Testing.start ~id:"same" environment cart_workflow ()));
      ignore
        (expect_error "duplicate workflow ID" `Defect
           (T.Testing.start ~id:"same" environment cart_workflow ()));
      ignore
        (expect_error "unregistered workflow" `Defect
           (T.Testing.start environment greeting_workflow "x")));
  with_environment ~workflows:[ T.Testing.workflow remote_child ] ~activities:[]
    (fun environment ->
      let error =
        expect_error "remote workflow" `Defect
          (T.Testing.execute environment remote_child 1)
      in
      expect_contains "remote workflow reason" ~needle:"mock_workflow"
        (T.Error.message error));
  let environment =
    ok "create" (T.Testing.create ~workflows:[] ~activities:[] ())
  in
  T.Testing.shutdown environment;
  T.Testing.shutdown environment;
  ignore
    (expect_error "use after shutdown" `Defect
       (T.Testing.skip environment (T.Duration.of_ms 1L)))

(** Heartbeats its attempt number and fails until it sees the previous
    attempt's heartbeat, proving details are handed to the next attempt. *)
let resumable =
  T.Activity.define_with_context ~name:"resumable" ~input:T.Codec.unit
    ~output:T.Codec.string (fun context () ->
      match T.Activity.Context.details context with
      | [] ->
          let* () = T.Activity.Context.heartbeat context T.Codec.string "checkpoint" in
          Error (T.Error.make ~category:`Activity ~message:"interrupted" ())
      | payload :: _ -> T.Codec.decode T.Codec.string payload)

(** Runs [resumable] as a local activity, then [compose_greeting]. *)
let local_workflow =
  T.Workflow.define ~name:"local_workflow" ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      let* resumed =
        T.Activity.execute_local ~retry_policy:(retry_policy ~maximum_attempts:3)
          resumable ()
      in
      let* greeting = T.Activity.execute_local compose_greeting resumed in
      Ok greeting)

(** Local activities run in the environment, and heartbeat details recorded
    by one attempt are delivered to the next. *)
let test_local_activity_and_heartbeat_details () =
  with_environment
    ~workflows:[ T.Testing.workflow local_workflow ]
    ~activities:[ T.Testing.activity resumable; T.Testing.activity compose_greeting ]
    (fun environment ->
      expect "resumed from heartbeat" "Hello, checkpoint"
        (ok "execute" (T.Testing.execute environment local_workflow ())))

(** Signals the [cart-target] workflow and then closes it. *)
let sender_workflow =
  T.Workflow.define ~name:"sender" ~input:T.Codec.string ~output:T.Codec.unit
    (fun item ->
      let* () =
        T.Future.await
          (T.Workflow.signal_external_workflow ~workflow_id:"cart-target"
             ~run_id:"" ~signal:add_item_signal ~input:item)
      in
      T.Future.await
        (T.Workflow.signal_external_workflow ~workflow_id:"cart-target"
           ~run_id:"" ~signal:close_signal ~input:()))

(** One workflow signals another through the environment, and signalling a
    workflow that does not exist fails the sender's future. *)
let test_external_signal () =
  with_environment
    ~workflows:[ cart_registration; T.Testing.workflow sender_workflow ]
    ~activities:[]
    (fun environment ->
      let cart = ok "start cart" (T.Testing.start ~id:"cart-target" environment cart_workflow ()) in
      ok "sender" (T.Testing.execute environment sender_workflow "pear");
      expect "cart received the signals" "pear" (ok "cart result" (T.Testing.result cart));
      ignore
        (expect_error "missing target" `Workflow
           (T.Testing.execute environment sender_workflow "plum")))

let () =
  test_timer_skipping ();
  test_activity_and_override ();
  test_remote_and_missing_activities ();
  test_activity_retries ();
  test_error_propagation ();
  test_child_workflows ();
  test_child_cancellation ();
  test_signals_queries_updates ();
  test_skip_timeout_and_cancel ();
  test_continue_as_new ();
  test_determinism ();
  test_local_activity_and_heartbeat_details ();
  test_external_signal ();
  test_registration_rules ()
