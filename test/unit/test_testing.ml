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
    (T.Retry_policy.make
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
           let initiator =
             match T.Workflow.Info.continuation_initiator info with
             | Some `Continue_as_new -> "continue-as-new"
             | Some (`Retry | `Cron_schedule) | None -> "other"
           in
           Ok
             (Printf.sprintf "%s/%s<-%s:%s" (T.Workflow.Info.run_id info)
                (Option.value ~default:""
                   (T.Workflow.Info.first_execution_run_id info))
                (Option.value ~default:""
                   (T.Workflow.Info.continued_from_run_id info))
                initiator)
         else T.Workflow.continue_as_new (Lazy.force countdown_workflow) (remaining - 1)))

(** Continue-as-new starts successor runs and the handle follows them; each
    successor reports its predecessor and the continue-as-new initiator. *)
let test_continue_as_new () =
  let countdown = Lazy.force countdown_workflow in
  with_environment ~workflows:[ T.Testing.workflow countdown ] ~activities:[]
    (fun environment ->
      let handle = ok "start" (T.Testing.start environment countdown 3) in
      (* The runs contain no timers, so [start] already ran every successor;
         the handle follows the chain to its fourth and final run, which
         still reports the chain's first run and names its predecessor. *)
      expect "handle follows the latest run" "test-run-4" (T.Testing.run_id handle);
      expect "final run, first run, and predecessor"
        "test-run-4/test-run-1<-test-run-3:continue-as-new"
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
  (* Two handlers with one signal name are a typed defect from [create],
     never an exception from the first start. *)
  (match
     T.Testing.create
       ~workflows:
         [
           T.Testing.workflow cart_workflow
             ~signals:
               [
                 T.Signal.Handler.make add_item_signal add_item;
                 T.Signal.Handler.make add_item_signal add_item;
               ];
         ]
       ~activities:[] ()
   with
  | Ok environment ->
      T.Testing.shutdown environment;
      failwith "duplicate signal handler was accepted"
  | Error error ->
      if (T.Error.view error).category <> `Defect then
        failwith "duplicate signal handler was not a defect";
      expect_contains "duplicate handler" ~needle:"duplicate signal handler"
        (T.Error.message error));
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

(** Runs [resumable] as a remote activity, then [compose_greeting] as a local
    one. *)
let local_workflow =
  T.Workflow.define ~name:"local_workflow" ~input:T.Codec.unit
    ~output:T.Codec.string (fun () ->
      let* resumed =
        T.Activity.execute ~retry_policy:(retry_policy ~maximum_attempts:3)
          resumable ()
      in
      let* greeting = T.Activity.execute_local compose_greeting resumed in
      Ok greeting)

(** Details each [local_checkpoint] attempt saw, newest first. *)
let local_details_seen = ref []

(** Heartbeats and fails on every attempt, recording the details it sees. *)
let local_checkpoint =
  T.Activity.define_with_context ~name:"local_checkpoint" ~input:T.Codec.unit
    ~output:T.Codec.unit (fun context () ->
      local_details_seen :=
        List.length (T.Activity.Context.details context) :: !local_details_seen;
      let* () = T.Activity.Context.heartbeat context T.Codec.string "checkpoint" in
      Error (T.Error.make ~category:`Activity ~message:"interrupted" ()))

(** Runs [local_checkpoint] as a local activity with three attempts. *)
let local_checkpoint_workflow =
  T.Workflow.define ~name:"local_checkpoint_workflow" ~input:T.Codec.unit
    ~output:T.Codec.unit (fun () ->
      T.Activity.execute_local ~retry_policy:(retry_policy ~maximum_attempts:3)
        local_checkpoint ())

(** Local activities run in the environment. A remote activity's heartbeat
    details are delivered to its next attempt; a local activity's are not,
    as Core builds every local attempt with no details and drops its
    heartbeats (#1221). *)
let test_local_activity_and_heartbeat_details () =
  with_environment
    ~workflows:
      [ T.Testing.workflow local_workflow; T.Testing.workflow local_checkpoint_workflow ]
    ~activities:
      [
        T.Testing.activity resumable;
        T.Testing.activity compose_greeting;
        T.Testing.activity local_checkpoint;
      ]
    (fun environment ->
      expect "remote attempt resumed from heartbeat" "Hello, checkpoint"
        (ok "execute" (T.Testing.execute environment local_workflow ()));
      local_details_seen := [];
      ignore
        (expect_error "local attempts exhausted" `Activity
           (T.Testing.execute environment local_checkpoint_workflow ()));
      expect "every local attempt saw no heartbeat details" [ 0; 0; 0 ]
        !local_details_seen)

(** Signals the [cart-target] workflow and then closes it. *)
let sender_workflow =
  T.Workflow.define ~name:"sender" ~input:T.Codec.string ~output:T.Codec.unit
    (fun item ->
      let* () =
        T.Future.await
          (T.Workflow.signal_external_workflow ~workflow_id:"cart-target"
             ~signal:add_item_signal ~input:item ())
      in
      T.Future.await
        (T.Workflow.signal_external_workflow ~workflow_id:"cart-target"
           ~signal:close_signal ~input:() ()))

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

(** Every side effect a fire-and-forget operation performed, newest first. *)
let side_effects = ref []

(** Records that a server dispatched the receipt activity. *)
let send_receipt =
  T.Activity.define ~name:"send_receipt" ~input:T.Codec.string
    ~output:T.Codec.unit (fun id ->
      side_effects := ("activity " ^ id) :: !side_effects;
      Ok ())

(** Records that a server started the abandoned child. The write is
    test-only instrumentation; this workflow is never replayed. *)
let abandoned_child =
  T.Workflow.define ~name:"abandoned_child" ~input:T.Codec.string
    ~output:T.Codec.unit (fun id ->
      side_effects := ("child " ^ id) :: !side_effects;
      Ok ())

(** Starts an activity, an abandoned child, and an external signal without
    awaiting any of them, then completes in the same activation. A server
    never dispatches commands recorded in a run's closing workflow task. *)
let fire_and_forget_workflow =
  T.Workflow.define ~name:"fire_and_forget" ~input:T.Codec.string
    ~output:T.Codec.unit (fun id ->
      ignore (T.Activity.start send_receipt id);
      ignore
        (T.Child_workflow.start
           ~parent_close_policy:T.Child_workflow.Parent_close_policy.Abandon
           ~id:("abandoned-" ^ id) abandoned_child id);
      ignore
        (T.Workflow.signal_external_workflow ~workflow_id:"cart-final"
           ~signal:add_item_signal ~input:id ());
      Ok ())

(** Commands emitted in a run's final activation are not executed (#1035). *)
let test_final_activation_commands_dropped () =
  with_environment
    ~workflows:
      [
        cart_registration;
        T.Testing.workflow fire_and_forget_workflow;
        T.Testing.workflow abandoned_child;
      ]
    ~activities:[ T.Testing.activity send_receipt ]
    (fun environment ->
      side_effects := [];
      let cart =
        ok "start cart" (T.Testing.start ~id:"cart-final" environment cart_workflow ())
      in
      ok "fire and forget" (T.Testing.execute environment fire_and_forget_workflow "x");
      expect "no activity, child, or signal ran" [] !side_effects;
      ok "close cart" (T.Testing.signal cart close_signal ());
      expect "cart never received the signal" "" (ok "cart result" (T.Testing.result cart)))

let bump_signal = T.Signal.define ~name:"bump" ~input:T.Codec.unit

(** The number of [bump] signals the current run has handled. *)
let hits : int T.Workflow_context.Local.t = T.Workflow_context.Local.create ()

(** Reads the run's hit count, defaulting to zero. *)
let current_hits () =
  match T.Workflow_context.Local.get hits with
  | Ok (Some value) -> value
  | Ok None -> 0
  | Error error -> failwith (T.Error.message error)

(** Sleeps one second and returns how many bumps its handler counted. *)
let counter_workflow =
  T.Workflow.define ~name:"counter" ~input:T.Codec.unit ~output:T.Codec.int
    (fun () ->
      let* () = T.Workflow.sleep (T.Duration.of_ms 1_000L) in
      Ok (current_hits ()))

(** Sleeps one second, then bumps the workflow with ID [r]. *)
let bumper_workflow =
  T.Workflow.define ~name:"bumper" ~input:T.Codec.unit ~output:T.Codec.unit
    (fun () ->
      let* () = T.Workflow.sleep (T.Duration.of_ms 1_000L) in
      T.Future.await
        (T.Workflow.signal_external_workflow ~workflow_id:"r"
           ~signal:bump_signal ~input:() ()))

(** A signal and a timer that land in one activation are delivered in Core's
    order, signal first, so the acknowledged signal's handler runs before the
    root completes (#1036). *)
let test_core_job_order () =
  with_environment
    ~workflows:
      [
        T.Testing.workflow bumper_workflow;
        T.Testing.workflow counter_workflow
          ~signals:
            [
              T.Signal.Handler.make bump_signal (fun () ->
                  T.Workflow_context.Local.set hits (current_hits () + 1));
            ];
      ]
    ~activities:[]
    (fun environment ->
      let bumper = ok "start bumper" (T.Testing.start environment bumper_workflow ()) in
      let counter = ok "start counter" (T.Testing.start ~id:"r" environment counter_workflow ()) in
      expect "signal handled before completion" 1
        (ok "counter result" (T.Testing.result counter));
      ok "bumper result" (T.Testing.result bumper))

(** Fails retryably until its [succeed_at] attempt, then reports what
    [Workflow.info] says about the run. *)
let retrying_child =
  T.Workflow.define ~name:"retrying_child" ~input:T.Codec.int
    ~output:T.Codec.string (fun succeed_at ->
      let* info = T.Workflow.info () in
      let attempt = T.Workflow.Info.attempt info in
      if attempt < succeed_at then
        Error (T.Error.make ~category:`Workflow ~message:"transient child failure" ())
      else
        Ok
          (Printf.sprintf "attempt=%d initiator=%s policy=%b queue=%s" attempt
             (match T.Workflow.Info.continuation_initiator info with
             | Some `Retry -> "retry"
             | Some (`Continue_as_new | `Cron_schedule) | None -> "none")
             (Option.is_some (T.Workflow.Info.retry_policy info))
             (T.Workflow.Info.task_queue info)))

(** Runs [retrying_child] on another queue with a three-attempt policy. *)
let retrying_parent =
  T.Workflow.define ~name:"retrying_parent" ~input:T.Codec.int
    ~output:T.Codec.string (fun succeed_at ->
      T.Child_workflow.execute ~retry_policy:(retry_policy ~maximum_attempts:3)
        ~task_queue:"child-queue" ~id:"retrying-child" retrying_child succeed_at)

(** Runs [retrying_child] without a retry policy. *)
let single_attempt_parent =
  T.Workflow.define ~name:"single_attempt_parent" ~input:T.Codec.int
    ~output:T.Codec.string (fun succeed_at ->
      T.Child_workflow.execute ~id:"single-attempt-child" retrying_child succeed_at)

(** A child retry policy retries failed runs with backoff in virtual time,
    and the child reports its attempt, retry continuation, policy, and task
    queue (#1037). *)
let test_child_retry_policy () =
  with_environment
    ~workflows:
      [
        T.Testing.workflow retrying_parent;
        T.Testing.workflow single_attempt_parent;
        T.Testing.workflow retrying_child;
      ]
    ~activities:[]
    (fun environment ->
      let started = epoch_ms (T.Testing.now environment) in
      expect "third attempt succeeded"
        "attempt=3 initiator=retry policy=true queue=child-queue"
        (ok "retried child" (T.Testing.execute environment retrying_parent 3));
      expect "backoff of 1s then 2s in virtual time" 3_000L
        (Int64.sub (epoch_ms (T.Testing.now environment)) started);
      let error =
        expect_error "exhausted child retries" `Child_workflow
          (T.Testing.execute environment retrying_parent 4)
      in
      expect "exhausted child is non-retryable" true (T.Error.view error).non_retryable;
      expect "child failure type" (Some "workflow") (T.Error.error_type error);
      expect_contains "child failure cause" ~needle:"transient child failure"
        (T.Error.message error);
      let error =
        expect_error "child without policy" `Child_workflow
          (T.Testing.execute environment single_attempt_parent 2)
      in
      expect "child without policy inherits retryability" false
        (T.Error.view error).non_retryable)

(** Counts [cancelled_activity] attempts. *)
let cancelled_attempts = ref 0

(** Returns a retryable [Cancelled]-category error without any cancellation
    having been requested. *)
let cancelled_activity =
  T.Activity.define ~name:"cancelled_activity" ~input:T.Codec.unit
    ~output:T.Codec.unit (fun () ->
      incr cancelled_attempts;
      Error (T.Error.make ~category:`Cancelled ~message:"aborted" ()))

(** Runs [cancelled_activity] under a two-attempt policy. *)
let cancelled_activity_workflow =
  T.Workflow.define ~name:"cancelled_activity_workflow" ~input:T.Codec.unit
    ~output:T.Codec.unit (fun () ->
      T.Activity.execute ~retry_policy:(retry_policy ~maximum_attempts:2)
        cancelled_activity ())

(** Shares [flaky]'s name with an incompatible input codec, so the engine
    cannot decode the input the workflow encoded for [flaky]. *)
let flaky_with_int_input =
  T.Activity.define ~name:"flaky" ~input:T.Codec.int ~output:T.Codec.int
    (fun value -> Ok value)

(** Fails with a [Cancelled]-category error: a workflow failure, not an
    acknowledged cancellation. *)
let cancelled_failure_child =
  T.Workflow.define ~name:"cancelled_failure_child" ~input:T.Codec.unit
    ~output:T.Codec.unit (fun () ->
      Error (T.Error.make ~category:`Cancelled ~message:"gave up" ()))

(** Runs [cancelled_failure_child] and returns its failure. *)
let cancelled_failure_parent =
  T.Workflow.define ~name:"cancelled_failure_parent" ~input:T.Codec.unit
    ~output:T.Codec.unit (fun () ->
      T.Child_workflow.execute ~id:"cancelled-failure-child"
        cancelled_failure_child ())

(** Activity and child failures are classified and shaped as the native
    worker delivers them (#1038). *)
let test_native_error_shapes () =
  with_environment
    ~workflows:
      [
        T.Testing.workflow cancelled_activity_workflow;
        T.Testing.workflow flaky_workflow;
        T.Testing.workflow cancelled_failure_parent;
        T.Testing.workflow cancelled_failure_child;
      ]
    ~activities:[ T.Testing.activity cancelled_activity; T.Testing.activity flaky ]
    (fun environment ->
      cancelled_attempts := 0;
      let error =
        expect_error "cancelled-category activity error" `Activity
          (T.Testing.execute environment cancelled_activity_workflow ())
      in
      expect "cancelled-category error is retried" 2 !cancelled_attempts;
      expect "type is the category label" (Some "cancelled") (T.Error.error_type error);
      expect "exhausted activity is non-retryable" true (T.Error.view error).non_retryable;
      flaky_attempts := [];
      let error =
        expect_error "exhausted untyped activity" `Activity
          (T.Testing.execute environment flaky_workflow 2)
      in
      expect "untyped failure reports its category" (Some "activity")
        (T.Error.error_type error);
      expect "exhausted retries are non-retryable" true (T.Error.view error).non_retryable;
      let error =
        expect_error "child failing with a cancelled-category error" `Child_workflow
          (T.Testing.execute environment cancelled_failure_parent ())
      in
      expect "canceled cause carries no application type" None
        (T.Error.error_type error));
  with_environment
    ~workflows:[ T.Testing.workflow flaky_workflow ]
    ~activities:[ T.Testing.activity flaky_with_int_input ]
    (fun environment ->
      let error =
        expect_error "codec failure" `Activity
          (T.Testing.execute environment flaky_workflow 2)
      in
      expect "codec failure uses the native adapter type"
        (Some "ocaml_temporal_native_activity") (T.Error.error_type error))

(** Defined but never registered with the environment. *)
let unregistered_activity =
  T.Activity.define ~name:"unregistered" ~input:T.Codec.unit ~output:T.Codec.unit
    (fun () -> Ok ())

(** Schedules [unregistered_activity] remotely or locally, three attempts. *)
let unregistered_workflow =
  T.Workflow.define ~name:"unregistered_workflow" ~input:T.Codec.bool
    ~output:T.Codec.unit (fun local ->
      let retry_policy = retry_policy ~maximum_attempts:3 in
      if local then T.Activity.execute_local ~retry_policy unregistered_activity ()
      else T.Activity.execute ~retry_policy unregistered_activity ())

(** Runs of [cancelled_failure_child] under [retrying_cancelled_parent]. *)
let cancelled_child_runs = ref 0

(** Fails with a [Cancelled]-category error and counts its runs. *)
let counted_cancelled_child =
  T.Workflow.define ~name:"counted_cancelled_child" ~input:T.Codec.unit
    ~output:T.Codec.unit (fun () ->
      incr cancelled_child_runs;
      Error (T.Error.make ~category:`Cancelled ~message:"gave up" ()))

(** Runs [counted_cancelled_child] under a three-attempt workflow retry
    policy. *)
let retrying_cancelled_parent =
  T.Workflow.define ~name:"retrying_cancelled_parent" ~input:T.Codec.unit
    ~output:T.Codec.unit (fun () ->
      T.Child_workflow.execute ~retry_policy:(retry_policy ~maximum_attempts:3)
        ~id:"counted-cancelled-child" counted_cancelled_child ())

(** Testing matches the native worker in three more places (#1215):
    workflow-visible errors carry the native [Error.origin]; an unregistered
    activity type fails as ["NotFoundError"], retryably for a remote
    activity (so its policy runs every attempt) and at once for a local one
    (#1031); and a child failing with a [Cancelled] error is not retried,
    since the server never retries a canceled failure. *)
let test_native_origins_and_retryability () =
  with_environment
    ~workflows:
      [
        T.Testing.workflow unregistered_workflow;
        T.Testing.workflow cancelled_failure_parent;
        T.Testing.workflow cancelled_failure_child;
        T.Testing.workflow retrying_cancelled_parent;
        T.Testing.workflow counted_cancelled_child;
        T.Testing.workflow flaky_workflow;
      ]
    ~activities:[ T.Testing.activity flaky ]
    (fun environment ->
      let started = epoch_ms (T.Testing.now environment) in
      let error =
        expect_error "remote unregistered activity" `Activity
          (T.Testing.execute environment unregistered_workflow false)
      in
      expect "unregistered type" (Some "NotFoundError") (T.Error.error_type error);
      expect "remote unregistered type retried under the policy" 3_000L
        (Int64.sub (epoch_ms (T.Testing.now environment)) started);
      expect "activity failure origin" (Some `Application) (T.Error.origin error);
      let started = epoch_ms (T.Testing.now environment) in
      let error =
        expect_error "local unregistered activity" `Activity
          (T.Testing.execute environment unregistered_workflow true)
      in
      expect "local unregistered type" (Some "NotFoundError") (T.Error.error_type error);
      expect "local unregistered type not retried" 0L
        (Int64.sub (epoch_ms (T.Testing.now environment)) started);
      let error =
        expect_error "cancelled-category child" `Child_workflow
          (T.Testing.execute environment cancelled_failure_parent ())
      in
      expect "cancelled child origin" (Some `Cancelled) (T.Error.origin error);
      cancelled_child_runs := 0;
      ignore
        (expect_error "retrying parent of a cancelled child" `Child_workflow
           (T.Testing.execute environment retrying_cancelled_parent ()));
      expect "cancelled-category child failure is not retried" 1
        !cancelled_child_runs)

(** Answers a query without touching workflow state. *)
let ping_query = T.Query.define ~name:"ping" ~output:T.Codec.string

(** Sleeps, then reports [Workflow.Info.history_length]. *)
let history_workflow =
  T.Workflow.define ~name:"history" ~input:T.Codec.unit ~output:T.Codec.int
    (fun () ->
      let* () = T.Workflow.sleep (T.Duration.of_ms 1_000L) in
      let* info = T.Workflow.info () in
      Ok (T.Workflow.Info.history_length info))

(** Sleeps for a day forever; used to check that a finite timeout still
    bounds a workflow whose timers keep landing on the saturated clock. *)
let endless_sleep_workflow =
  T.Workflow.define ~name:"endless-sleep" ~input:T.Codec.unit
    ~output:T.Codec.unit (fun () ->
      let rec loop () =
        let* () = T.Workflow.sleep one_day in
        loop ()
      in
      loop ())

(** Virtual time saturates instead of overflowing, queries do not inflate
    the history length, and a mock replaces a registration regardless of
    list order (#1039). *)
let test_time_history_and_mock_order () =
  let near_end =
    ok "near end" (T.Time.of_unix ~seconds:(Int64.sub (Int64.div Int64.max_int 1_000L) 100L) ~nanoseconds:0)
  in
  with_environment ~start_time:near_end
    ~workflows:[ T.Testing.workflow sleepy_workflow ]
    ~activities:[]
    (fun environment ->
      let handle = ok "start sleepy" (T.Testing.start environment sleepy_workflow ()) in
      (* The one-day timer saturates at the last instant; the one-day
         timeout cannot be represented either, so it is no limit. *)
      ignore (ok "result with overflowing timeout" (T.Testing.result ~timeout:one_day handle));
      ok "overflowing skip" (T.Testing.skip environment one_day);
      if T.Time.compare (T.Testing.now environment) near_end < 0 then
        failwith "virtual time wrapped backwards");
  (* Once the clock saturates, a finite timeout must still end the wait
     rather than firing the workflow's timers at the last instant forever. *)
  with_environment ~start_time:near_end
    ~workflows:[ T.Testing.workflow endless_sleep_workflow ]
    ~activities:[]
    (fun environment ->
      let handle =
        ok "start endless" (T.Testing.start environment endless_sleep_workflow ())
      in
      ignore
        (expect_error "saturated finite timeout" `Timeout
           (T.Testing.result ~timeout:one_day handle)));
  let history_length ~queries =
    with_environment
      ~workflows:
        [
          T.Testing.workflow history_workflow
            ~queries:[ T.Query.Handler.make ping_query (fun () -> Ok "pong") ];
        ]
      ~activities:[]
      (fun environment ->
        let handle = ok "start history" (T.Testing.start environment history_workflow ()) in
        for _ = 1 to queries do
          expect "ping" "pong" (ok "query" (T.Testing.query handle ping_query))
        done;
        ok "history result" (T.Testing.result handle))
  in
  expect "queries do not grow history" (history_length ~queries:0)
    (history_length ~queries:5);
  with_environment
    ~workflows:[ T.Testing.workflow greeting_workflow ]
    ~activities:
      [
        T.Testing.mock_activity compose_greeting (fun name -> Ok ("mocked " ^ name));
        T.Testing.activity compose_greeting;
      ]
    (fun environment ->
      expect "prepended mock wins" "mocked OCaml"
        (ok "execute" (T.Testing.execute environment greeting_workflow "OCaml")));
  match
    T.Testing.create ~workflows:[]
      ~activities:
        [
          T.Testing.mock_activity compose_greeting (fun name -> Ok name);
          T.Testing.mock_activity compose_greeting (fun name -> Ok name);
        ]
      ()
  with
  | Ok environment ->
      T.Testing.shutdown environment;
      failwith "duplicate mock registration was accepted"
  | Error error -> (
      expect_contains "duplicate mock" ~needle:"duplicate mock" (T.Error.message error);
      (* Two ordinary registrations stay a defect even when a mock between
         them wins the name. *)
      match
        T.Testing.create ~workflows:[]
          ~activities:
            [
              T.Testing.activity compose_greeting;
              T.Testing.mock_activity compose_greeting (fun name -> Ok name);
              T.Testing.activity compose_greeting;
            ]
          ()
      with
      | Ok environment ->
          T.Testing.shutdown environment;
          failwith "duplicate ordinary registration around a mock was accepted"
      | Error error ->
          expect_contains "duplicate ordinary" ~needle:"duplicate activity"
            (T.Error.message error))

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
  test_registration_rules ();
  test_final_activation_commands_dropped ();
  test_core_job_order ();
  test_child_retry_policy ();
  test_native_error_shapes ();
  test_native_origins_and_retryability ();
  test_time_history_and_mock_order ()
