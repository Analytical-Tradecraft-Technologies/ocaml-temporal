(** Tests for public local-activity cancellation (#1156): [?scope] on
    [Activity.start_local]/[execute_local] and [Activity.start_local_handle].

    The fixtures install a private workflow context on a deterministic
    scheduler, start local activities through the public API, and play Core's
    part by resolving the activity or delivering a [DoBackoff] between
    scheduler runs. They assert the exact command buffer: cancelling a scoped
    or handled local activity emits one [Request_cancel_local_activity]
    carrying the recorded policy's sequence, a backoff in progress is also
    cancelled with its timer, and a completed activity or an already-cancelled
    scope emits nothing. Context-store ordering under every policy is covered
    by [test_local_activity_cancellation.ml]. *)

module Activation = Temporal_runtime.Activation
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Fails with the scenario label when two values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Checks that a peeked future holds a typed cancellation. *)
let expect_cancelled label = function
  | Some (Error error) ->
      expect (label ^ " kind") "cancelled" (Temporal.Error.kind error)
  | Some (Ok _) -> failwith (label ^ " succeeded")
  | None -> failwith (label ^ " is still pending")

(** A command-only local activity; its code never runs in these tests. *)
let local =
  Temporal.Activity.remote ~name:"local-step" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Runs [action] as a new workflow fiber with [context] installed and drives
    the scheduler until it is idle. *)
let run_in context scheduler action =
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context action);
  ignore (Scheduler.run_label scheduler)

(** Creates a scope inside the workflow, failing the test otherwise. *)
let create_scope () =
  match Temporal.Scope.create () with
  | Ok scope -> scope
  | Error error -> failwith (Temporal.Error.message error)

(** Extracts the sequence and policy of the one scheduled local activity. *)
let scheduled label = function
  | [ Activation.Schedule_local_activity { seq; cancellation_type; _ } ] ->
      (seq, cancellation_type)
  | _ -> failwith (label ^ ": expected one Schedule_local_activity")

(** A scope cancelled while the local activity runs emits one cancellation
    request; cancelling again emits nothing. The default policy,
    [Wait_cancellation_completed], is recorded on the schedule command, so
    the future stays pending until Core resolves it. *)
let test_scope_cancels_running_local () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let scope_ref = ref None in
  let future_ref = ref None in
  run_in context scheduler (fun () ->
      let scope = create_scope () in
      scope_ref := Some scope;
      future_ref := Some (Temporal.Activity.start_local ~scope local "input"));
  let seq, policy =
    scheduled "running" (Workflow_context_store.take_commands context)
  in
  expect "default local policy" Activation.Wait_cancellation_completed policy;
  let first = ref None and second = ref None in
  run_in context scheduler (fun () ->
      let scope = Option.get !scope_ref in
      first := Some (Temporal.Scope.cancel scope);
      second := Some (Temporal.Scope.cancel scope));
  expect "first cancel" (Some (Ok ())) !first;
  expect "repeated cancel" (Some (Ok ())) !second;
  expect "one cancellation request"
    [ Activation.Request_cancel_local_activity { seq } ]
    (Workflow_context_store.take_commands context);
  if Temporal.Future.is_ready (Option.get !future_ref) then
    failwith "Wait_cancellation_completed resolved before Core reported";
  Workflow_context_store.shutdown context

(** [start_local_handle] records an explicit policy and cancels exactly once;
    a second cancel after Core's resolution is an idempotent no-op. *)
let test_handle_cancel () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let handle_ref = ref None in
  run_in context scheduler (fun () ->
      handle_ref :=
        Some
          (Temporal.Activity.start_local_handle
             ~cancellation_type:Temporal.Activity.Try_cancel local "input"));
  let handle = Option.get !handle_ref in
  let seq, policy =
    scheduled "handle" (Workflow_context_store.take_commands context)
  in
  expect "explicit local policy" Activation.Try_cancel policy;
  let first = ref None in
  run_in context scheduler (fun () ->
      first := Some (Temporal.Activity.cancel handle));
  expect "handle cancel" (Some (Ok ())) !first;
  expect "handle cancellation request"
    [ Activation.Request_cancel_local_activity { seq } ]
    (Workflow_context_store.take_commands context);
  (* Core answers a Try_cancel request with a cancelled resolution. *)
  expect "cancelled resolution" (Ok ())
    (Workflow_context_store.resolve_activity context ~seq
       (Error
          (Temporal_base.Error.make ~category:`Cancelled
             ~message:"local activity cancelled" ())));
  ignore (Scheduler.run_label scheduler);
  let second = ref None in
  run_in context scheduler (fun () ->
      second := Some (Temporal.Activity.cancel handle));
  expect "late cancel" (Some (Ok ())) !second;
  expect "late cancel emitted nothing" []
    (Workflow_context_store.take_commands context);
  expect_cancelled "handle future"
    (Temporal.Future.peek (Temporal.Activity.future handle));
  Workflow_context_store.shutdown context

(** A local activity waiting in a language-owned retry backoff has no running
    attempt: scope cancellation requests cancellation, cancels the backoff
    timer, and settles the future immediately. *)
let test_scope_cancels_backoff () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let scope_ref = ref None in
  let future_ref = ref None in
  run_in context scheduler (fun () ->
      let scope = create_scope () in
      scope_ref := Some scope;
      future_ref := Some (Temporal.Activity.start_local ~scope local "input"));
  let seq, _ =
    scheduled "backoff" (Workflow_context_store.take_commands context)
  in
  expect "backoff job" (Ok ())
    (Workflow_context_store.resolve_local_activity_backoff context ~seq
       ~attempt:2L ~backoff_milliseconds:60_000L ~original_schedule_time:None);
  run_in context scheduler (fun () -> ());
  let timer_seq =
    match Workflow_context_store.take_commands context with
    | [ Activation.Start_timer { seq = timer_seq; milliseconds = 60_000L } ] ->
        timer_seq
    | _ -> failwith "backoff did not start its timer"
  in
  run_in context scheduler (fun () ->
      ignore (Temporal.Scope.cancel (Option.get !scope_ref)));
  expect "backoff cancellation"
    [ Activation.Request_cancel_local_activity { seq };
      Activation.Cancel_timer { seq = timer_seq } ]
    (Workflow_context_store.take_commands context);
  expect_cancelled "backoff future" (Temporal.Future.peek (Option.get !future_ref));
  Workflow_context_store.shutdown context

(** Once the local activity has completed, the scope no longer retains it and
    cancelling the scope emits nothing; an already-cancelled scope schedules
    nothing at all. *)
let test_no_command_without_pending_local () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let scope_ref = ref None in
  run_in context scheduler (fun () ->
      let scope = create_scope () in
      scope_ref := Some scope;
      ignore (Temporal.Activity.start_local ~scope local "input"));
  let seq, _ =
    scheduled "completed" (Workflow_context_store.take_commands context)
  in
  let payload =
    { Temporal_base.Payload.metadata = [ ("encoding", "json/plain") ];
      data = Bytes.of_string "\"done\"" }
  in
  expect "completion" (Ok ())
    (Workflow_context_store.resolve_activity context ~seq (Ok payload));
  ignore (Scheduler.run_label scheduler);
  let pre_cancelled = ref None in
  run_in context scheduler (fun () ->
      ignore (Temporal.Scope.cancel (Option.get !scope_ref));
      let cancelled = create_scope () in
      ignore (Temporal.Scope.cancel cancelled);
      pre_cancelled :=
        Temporal.Future.peek
          (Temporal.Activity.start_local ~scope:cancelled local "input"));
  expect "no command after completion or for a cancelled scope" []
    (Workflow_context_store.take_commands context);
  expect_cancelled "pre-cancelled start" !pre_cancelled;
  Workflow_context_store.shutdown context

(** Runs the local-activity cancellation contract as one dune test. *)
let () =
  test_scope_cancels_running_local ();
  test_handle_cancel ();
  test_scope_cancels_backoff ();
  test_no_command_without_pending_local ()
