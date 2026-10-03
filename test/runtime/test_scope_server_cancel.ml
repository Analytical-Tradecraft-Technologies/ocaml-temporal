(** Proves that a structured scope cancellation reaches Temporal Core's
    command buffer for an attached activity.  The fixture uses the real public
    activity API and the deterministic runtime context, so it catches the
    regression where [Scope.cancel] only woke OCaml waiters. *)

module Activation = Temporal_runtime.Activation
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Runs one operation while the workflow context is installed on its owner
    scheduler. *)
let with_context scheduler context action =
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context action)

(** A remote activity is sufficient: scheduling and cancellation are owned by
    Core before any activity implementation is invoked. *)
let activity =
  Temporal.Activity.remote ~name:"scope-cancel-target"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit

(** A remote child definition is enough to verify that Core receives the
    cancellation command; no child worker is needed for this command-buffer
    test. *)
let child =
  Temporal.Workflow.remote ~name:"scope-cancel-child"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit

(** Scope cancellation must emit exactly one activity-cancel command after the
    original schedule command, while repeated cancellation remains idempotent. *)
let test_attached_activity_cancellation () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let first_cancel = ref None in
  let second_cancel = ref None in
  with_context scheduler context (fun () ->
      match Temporal.Scope.create () with
      | Error error -> failwith (Temporal.Error.message error)
      | Ok scope ->
          ignore (Temporal.Activity.start_handle ~scope activity ());
          first_cancel := Some (Temporal.Scope.cancel scope);
          second_cancel := Some (Temporal.Scope.cancel scope));
  let run_label = Scheduler.run_label scheduler in
  if run_label <> "complete" && run_label <> "blocked" then
    failwith ("scope cancellation command test did not run: " ^ run_label);
  begin match (!first_cancel, !second_cancel) with
  | Some (Ok ()), Some (Ok ()) -> ()
  | _ -> failwith "scope cancellation did not remain idempotent"
  end;
  let commands = Workflow_context_store.take_commands context in
  begin match commands with
  | [ Activation.Schedule_activity _; Activation.Request_cancel_activity _ ] ->
      ()
  | _ ->
      failwith
        (Printf.sprintf "expected schedule plus cancellation, got %d commands"
           (List.length commands))
  end;
  Workflow_context_store.shutdown context

(** Scope cancellation also propagates to a child workflow handle, preserving
    the same exactly-once and idempotence guarantees as activities. *)
let test_attached_child_cancellation () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let first_cancel = ref None in
  with_context scheduler context (fun () ->
      match Temporal.Scope.create () with
      | Error error -> failwith (Temporal.Error.message error)
      | Ok scope ->
          ignore
            (Temporal.Child_workflow.start_handle ~scope ~id:"scope-child" child
               ());
          first_cancel := Some (Temporal.Scope.cancel scope));
  begin match Scheduler.run scheduler with
  | Scheduler.Failed exception_ ->
      failwith ("child scheduler failed: " ^ Printexc.to_string exception_)
  | Scheduler.Complete | Scheduler.Blocked -> ()
  end;
  begin match !first_cancel with
  | Some (Ok ()) -> ()
  | Some (Error error) ->
      failwith ("child scope cancellation failed: " ^ Temporal.Error.message error)
  | None -> failwith "child scope cancellation did not run"
  end;
  let commands = Workflow_context_store.take_commands context in
  begin match commands with
  | [ Activation.Start_child_workflow _; Activation.Cancel_child_workflow _ ] ->
      ()
  | _ ->
      failwith
        (Printf.sprintf "expected child start plus cancellation, got %d commands"
           (List.length commands))
  end;
  Workflow_context_store.shutdown context

(** Cancels the owning scope synchronously while the public input codec runs. *)
let cancelling_codec scope =
  Temporal.Codec.make ~encoding:"scope/cancel-during-encoding"
    ~encode:(fun () ->
      match Temporal.Scope.cancel scope with
      | Ok () -> Ok Bytes.empty
      | Error error -> Error error)
    ~decode:(fun _ -> Ok ())

(** Requires a ready cancellation result without waiting for Core. *)
let expect_cancelled future =
  match Temporal.Future.peek future with
  | Some (Error error) when Temporal.Error.kind error = "cancelled" -> ()
  | Some (Error error) ->
      failwith ("expected cancellation, got " ^ Temporal.Error.kind error)
  | Some (Ok ()) -> failwith "cancelled start unexpectedly succeeded"
  | None -> failwith "cancelled start remained pending"

(** A codec-cancelled scope must not schedule an activity or its cancellation. *)
let test_activity_cancelled_during_encoding () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  with_context scheduler context (fun () ->
      match Temporal.Scope.create () with
      | Error error -> failwith (Temporal.Error.message error)
      | Ok scope ->
          let definition =
            Temporal.Activity.remote ~name:"cancel-during-activity-encoding"
              ~input:(cancelling_codec scope) ~output:Temporal.Codec.unit
          in
          let handle = Temporal.Activity.start_handle ~scope definition () in
          expect_cancelled (Temporal.Activity.future handle));
  begin match Scheduler.run scheduler with
  | Scheduler.Failed exception_ ->
      failwith ("activity scheduler failed: " ^ Printexc.to_string exception_)
  | Scheduler.Complete | Scheduler.Blocked -> ()
  end;
  if Workflow_context_store.take_commands context <> [] then
    failwith "codec-cancelled activity emitted a Temporal command";
  Workflow_context_store.shutdown context

(** A codec-cancelled scope must not start a child or request its cancellation. *)
let test_child_cancelled_during_encoding () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  with_context scheduler context (fun () ->
      match Temporal.Scope.create () with
      | Error error -> failwith (Temporal.Error.message error)
      | Ok scope ->
          let definition =
            Temporal.Workflow.remote ~name:"cancel-during-child-encoding"
              ~input:(cancelling_codec scope) ~output:Temporal.Codec.unit
          in
          let handle =
            Temporal.Child_workflow.start_handle ~scope ~id:"cancel-during-child"
              definition ()
          in
          expect_cancelled (Temporal.Child_workflow.future handle));
  begin match Scheduler.run scheduler with
  | Scheduler.Failed exception_ ->
      failwith ("child scheduler failed: " ^ Printexc.to_string exception_)
  | Scheduler.Complete | Scheduler.Blocked -> ()
  end;
  if Workflow_context_store.take_commands context <> [] then
    failwith "codec-cancelled child emitted a Temporal command";
  Workflow_context_store.shutdown context

(** Runs the focused server-side cancellation assertion. *)
let () =
  test_attached_activity_cancellation ();
  test_attached_child_cancellation ();
  test_activity_cancelled_during_encoding ();
  test_child_cancelled_during_encoding ()
