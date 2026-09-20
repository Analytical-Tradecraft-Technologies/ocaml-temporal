(** Deterministic cancellation orderings for language-owned local retry timers. *)
module Activation = Temporal_runtime.Activation
module Context = Temporal_runtime.Workflow_context_store
module Future = Temporal_runtime.Future_store
module Scheduler = Temporal_runtime.Scheduler

(** Reports runtime failures without discarding their structured diagnostic. *)
let get = function
  | Ok value -> value
  | Error error -> failwith (Temporal_base.Error.message error)

(** Attaches a useful name to a command or lifecycle mismatch. *)
let expect label expected actual =
  if expected <> actual then failwith label

(** Creates one pending local operation without registering an activity worker. *)
let with_activity ~is_replaying policy action =
  let context = Context.create (Scheduler.create ()) in
  Context.set_activation_is_replaying context is_replaying;
  Fun.protect ~finally:(fun () -> Context.shutdown context) (fun () ->
      let future, cancel =
        Context.schedule_local_activity context ~name:"retrying-local"
          ~input:{ Temporal_base.Payload.metadata = []; data = Bytes.empty }
          ~cancellation_type:policy ~decode:(fun payload -> Ok payload) ()
      in
      ignore (Context.take_commands context);
      action context future (fun () ->
          Context.with_context context (fun () -> get (cancel ()))))

(** Models the completion that transfers a long retry delay from Core to OCaml. *)
let backoff context =
  get (Context.resolve_local_activity_backoff context ~seq:1L ~attempt:2L
         ~backoff_milliseconds:60_000L ~original_schedule_time:None)

(** Cancellation must resolve the original future, without another Core job. *)
let expect_cancelled future =
  match Future.peek future with
  | Some (Error error) ->
      expect "wrong terminal category" `Cancelled (Temporal_base.Error.view error).category
  | Some (Ok _) -> failwith "cancelled activity succeeded"
  | None -> failwith "cancelled local activity remains pending"

(** A cancel preceding the backoff job prevents even a retry timer being created. *)
let before_backoff ~is_replaying policy =
  with_activity ~is_replaying policy (fun context future cancel ->
      cancel ();
      expect "initial cancel command" [Activation.Request_cancel_local_activity { seq = 1L }]
        (Context.take_commands context);
      backoff context;
      expect_cancelled future;
      expect "cancelled backoff scheduled work" [] (Context.take_commands context);
      cancel ();
      expect "repeated cancellation emitted work" [] (Context.take_commands context))

(** A cancel during backoff removes the timer and resolves exactly once. *)
let during_backoff ~is_replaying policy =
  with_activity ~is_replaying policy (fun context future cancel ->
      backoff context;
      expect "backoff timer" [Activation.Start_timer { seq = 2L; milliseconds = 60_000L }]
        (Context.take_commands context);
      cancel ();
      expect_cancelled future;
      expect "cancellation commands"
        [Activation.Request_cancel_local_activity { seq = 1L };
         Activation.Cancel_timer { seq = 2L }]
        (Context.take_commands context);
      cancel ();
      expect "repeated cancellation emitted work" [] (Context.take_commands context);
      (* Core does not deliver a cancelled timer. A stale/duplicate job must
         still be rejected, and must never revive the cancelled operation. *)
      (match Context.fire_timer context ~seq:2L with
      | Error _ -> () | Ok () -> failwith "cancelled timer remained registered");
      expect "cancelled timer rescheduled activity" [] (Context.take_commands context))

(** A timer that fires first starts the next attempt; Core then owns its cancel. *)
let after_timer ~is_replaying policy =
  with_activity ~is_replaying policy (fun context future cancel ->
      backoff context;
      ignore (Context.take_commands context);
      get (Context.fire_timer context ~seq:2L);
      (match Context.take_commands context with
      | [Activation.Schedule_local_activity { seq = 1L; attempt = 2L; _ }] -> ()
      | _ -> failwith "timer did not schedule the next attempt");
      cancel ();
      expect "running retry cancellation" [Activation.Request_cancel_local_activity { seq = 1L }]
        (Context.take_commands context);
      expect "running retry resolved without Core" None (Future.peek future);
      get (Context.resolve_activity context ~seq:1L
             (Error (Temporal_base.Error.make ~category:`Cancelled ~message:"Core cancelled" ())));
      expect_cancelled future;
      cancel ();
      expect "late cancellation emitted work" [] (Context.take_commands context))

(** A later backoff after a retry's cancellation cannot start a third attempt. *)
let retry_backoff_after_cancel ~is_replaying policy =
  with_activity ~is_replaying policy (fun context future cancel ->
      backoff context;
      ignore (Context.take_commands context);
      get (Context.fire_timer context ~seq:2L);
      ignore (Context.take_commands context);
      cancel ();
      ignore (Context.take_commands context);
      get (Context.resolve_local_activity_backoff context ~seq:1L ~attempt:3L
             ~backoff_milliseconds:60_000L ~original_schedule_time:None);
      expect_cancelled future;
      expect "cancelled retry started another timer" [] (Context.take_commands context))

(** The same ordered jobs must produce the same commands and outcomes in live
    and replay contexts under all three Core cancellation policies. *)
let () =
  List.iter (fun is_replaying ->
    List.iter (fun policy ->
        before_backoff ~is_replaying policy;
        during_backoff ~is_replaying policy;
        after_timer ~is_replaying policy;
        retry_backoff_after_cancel ~is_replaying policy)
      [Activation.Try_cancel; Activation.Wait_cancellation_completed; Activation.Abandon])
    [false; true];
  print_endline "local activity backoff cancellation orderings: ok"
