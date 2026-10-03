(** Regression for #566: completed work must not accumulate registrations on
    a live scope or an indefinitely pending race input. All measurements use
    full major collections; the scheduler no longer retains a trace (#551). *)
module S = Temporal_runtime.Scheduler
module C = Temporal_runtime.Workflow_context_store
module A = Temporal_runtime.Activation
module F = Temporal_runtime.Future_store

(** Unwraps a public result without hiding fixture failures. *)
let public = function Ok value -> value | Error error -> failwith (Temporal.Error.message error)

(** Unwraps a runtime result without hiding fixture failures. *)
let internal = function
  | Ok value -> value
  | Error error -> failwith (Temporal_base.Error.view error).message

(** Runs a deterministic turn and reports scheduler defects. *)
let drain scheduler =
  match S.run scheduler with S.Failed error -> raise error | S.Complete | S.Blocked -> ()

(** Runs a body on its owning workflow scheduler. *)
let run scheduler context body =
  S.spawn scheduler (fun () -> C.with_context context body);
  drain scheduler

(** Adapts a controllable test future through the private kernel. *)
let facade future =
  Temporal_future_kernel.make
    ~await:(fun () -> F.await future) ~await_gate:(F.await_gate future)
    ~subscribe:(F.subscribe future) ~is_ready:(fun () -> F.is_ready future)
    ~peek:(fun () -> F.peek future) ~owner_id:(F.owner_id future)
    ~outside_error:(fun () -> Temporal.Error.defect ~message:"outside test scheduler")
    ~callbacks_live:(fun () -> F.callbacks_live future) ~enqueue:(F.enqueue future)

(** Measures retained OCaml words rather than RSS or cumulative allocation. *)
let live_words () =
  Gc.full_major ();
  Gc.full_major ();
  (Gc.stat ()).live_words

(** Permits fixed test/runtime overhead, but rejects even a single retained
    list cell per operation across a 10,000-operation batch. *)
let bounded label baseline =
  let growth = live_words () - baseline in
  Printf.printf "%s retained_words=%d\n%!" label growth;
  if growth > 4096 then failwith (label ^ " retained completed registrations")

(** Definitions only emit deterministic fixture commands; no server is used. *)
let activity = Temporal.Activity.remote ~name:"retention" ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit

(** A child definition exercises terminal completion and start rejection. *)
let child = Temporal.Workflow.remote ~name:"retention-child" ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit

(** Shared unit payload prevents allocation size from biasing the comparison. *)
let payload = internal (Temporal_base.Codec.encode Temporal_base.Codec.unit ())

(** Exercises success, failure, cancellation and child start rejection while
    keeping the same scope alive through three batches of completed work. *)
let test_scope_batches mode =
  let scheduler = S.create () in
  let context = C.create scheduler in
  let scope = ref None in
  run scheduler context (fun () -> scope := Some (public (Temporal.Scope.create ())));
  let scope = Option.get !scope in
  let baseline = live_words () in
  for batch = 1 to 3 do
    run scheduler context (fun () ->
        for index = 1 to 10_000 do
          let outcome =
            if index mod 3 = 0 then
              Error (Temporal_base.Error.make ~category:`Cancelled ~message:"cancelled" ())
            else if index mod 3 = 1 then Ok payload
            else Error (Temporal_base.Error.make ~category:`Activity ~message:"failed" ())
          in
          let future =
            if mode = "child" then (
              let handle = Temporal.Child_workflow.start_handle ~scope ~id:"retention-child" child () in
              let seq = match C.take_commands context with
                | [A.Start_child_workflow command] -> command.seq
                | _ -> failwith "expected child command" in
              if index mod 4 = 0 then
                internal (C.resolve_child_workflow_start context ~seq
                  (Error (Temporal_base.Error.make ~category:`Activity ~message:"start rejected" ())))
              else (
                internal (C.resolve_child_workflow_start context ~seq (Ok "run"));
                internal (C.resolve_child_workflow context ~seq outcome));
              Temporal.Child_workflow.future handle)
            else (
              let handle =
                if mode = "hooks" then Temporal.Activity.start_handle ~scope activity ()
                else Temporal.Activity.start_handle activity () in
              let seq = match C.take_commands context with
                | [A.Schedule_activity command] -> command.seq
                | _ -> failwith "expected activity command" in
              internal (C.resolve_activity context ~seq outcome);
              Temporal.Activity.future handle)
          in
          if mode = "await" then ignore (Temporal.Scope.await scope future)
          else ignore (Temporal.Future.await future)
        done);
    bounded (Printf.sprintf "%s batch=%d" mode batch) baseline;
    run scheduler context (fun () ->
        if public (Temporal.Scope.is_cancelled scope) then failwith "scope must stay active")
  done;
  run scheduler context (fun () -> public (Temporal.Scope.cancel scope));
  if C.take_commands context <> [] then failwith "completed operations emitted cancellation";
  C.shutdown context

(** Repeated selection must release registrations on runtime and derived
    pending inputs alike, including the homogeneous [first] implementation. *)
let test_selection_batches derived first runtime =
  let scheduler = S.create () in
  let outside_error () = Temporal.Error.defect ~message:"outside" in
  let loser, resolve_loser = S.promise scheduler ~outside_error in
  let public_loser = facade loser in
  let public_loser = if derived then Temporal.Future.map Fun.id public_loser else public_loser in
  let baseline = live_words () in
  for batch = 1 to 3 do
    for index = 1 to 10_000 do
      let winner, resolve_winner = S.promise scheduler ~outside_error in
      if runtime then (
        if first then ignore (F.first ~ownership_error:outside_error winner [loser; loser])
        else ignore (F.race ~ownership_error:outside_error winner loser))
      else (
        let winner = facade winner in
        if first then ignore (Temporal.Future.first winner [public_loser; public_loser])
        else ignore (Temporal.Future.race winner public_loser));
      resolve_winner (if index mod 2 = 0 then Ok () else Error (outside_error ()));
      drain scheduler
    done;
    bounded (Printf.sprintf "selection derived=%b first=%b runtime=%b batch=%d" derived first runtime batch) baseline
  done;
  if F.is_ready loser then failwith "selection cancelled its losing operation";
  resolve_loser (Ok ());
  drain scheduler;
  S.shutdown scheduler

(** Removing a pending or queued observer is idempotent and does not reorder
    the remaining callbacks. Tests both the runtime and derived registries. *)
let test_unsubscribe derived =
  let scheduler = S.create () in
  let source, resolve = S.promise scheduler ~outside_error:(fun () -> ()) in
  let future =
    (* This variant uses unit errors directly; the kernel adapter is generic. *)
    Temporal_future_kernel.make ~await:(fun () -> F.await source)
      ~await_gate:(F.await_gate source) ~subscribe:(F.subscribe source)
      ~is_ready:(fun () -> F.is_ready source) ~peek:(fun () -> F.peek source)
      ~owner_id:(F.owner_id source) ~outside_error:(fun () -> ())
      ~callbacks_live:(fun () -> F.callbacks_live source) ~enqueue:(F.enqueue source) in
  let future = if derived then Temporal.Future.map Fun.id future else future in
  let seen = ref [] in
  let subscribe index = Temporal_future_kernel.subscribe future (fun _ -> seen := index :: !seen) in
  let keep = subscribe 1 in
  let pending = subscribe 2 in
  pending (); pending ();
  let queued = subscribe 3 in
  let last = subscribe 4 in
  (* Queue removal after derived resolution but before its delivery thunks. *)
  Temporal_future_kernel.observe future (fun _ -> ());
  resolve (Ok ());
  S.spawn scheduler (fun () -> queued (); queued ());
  (* Runtime callbacks have already been enqueued, so remove that one now. *)
  if not derived then queued ();
  drain scheduler;
  if List.rev !seen <> [1; 4] then failwith "observer removal changed FIFO delivery";
  let ready = subscribe 5 in
  ready (); ready ();
  drain scheduler;
  if List.rev !seen <> [1; 4] then failwith "ready observer removal failed";
  keep (); last ();
  S.shutdown scheduler

(** Already-ready inert inputs deliver inline during subscription, including
    the second registration after the first has already won. *)
let test_inline_selection () =
  let left = Temporal.Future.all [] in
  let right = Temporal.Future.all [] in
  (match Temporal.Future.peek (Temporal.Future.race left right) with
   | Some (Ok (Temporal.Future.Left [])) -> () | _ -> failwith "inline race order");
  match Temporal.Future.peek (Temporal.Future.first left [right; left]) with
  | Some (Ok []) -> () | _ -> failwith "inline first order"

(** Completion before cancellation suppresses the hook even before its queued
    cleanup runs. Pending hooks still run in registration order, and a foreign
    terminal future is rejected before either scheduler is mutated. *)
let test_hook_lifetimes () =
  let scheduler = S.create () in
  let context = C.create scheduler in
  let foreign = S.create () in
  let outside_error () = Temporal.Error.defect ~message:"outside" in
  let other, _ = S.promise foreign ~outside_error in
  let seen = ref [] in
  run scheduler context (fun () ->
      let scope = public (Temporal.Scope.create ()) in
      let terminal, resolve = S.promise scheduler ~outside_error in
      let pending, _ = S.promise scheduler ~outside_error in
      let hook index () = seen := index :: !seen; Ok () in
      public (Temporal.Scope.on_cancel scope (hook 1));
      public (Temporal.Scope.on_cancel ~until:(facade terminal) scope (hook 2));
      public (Temporal.Scope.on_cancel ~until:(facade pending) scope (hook 3));
      resolve (Error (outside_error ()));
      public (Temporal.Scope.on_cancel ~until:(facade terminal) scope (hook 4));
      (match Temporal.Scope.on_cancel ~until:(facade other) scope (hook 5) with
       | Error _ -> () | Ok () -> failwith "accepted foreign terminal future");
      public (Temporal.Scope.cancel scope);
      public (Temporal.Scope.cancel scope);
      public (Temporal.Scope.on_cancel ~until:(facade terminal) scope (hook 6));
      public (Temporal.Scope.on_cancel ~until:(facade pending) scope (hook 7)));
  if List.rev !seen <> [1; 3; 7] then failwith "terminal hook ordering changed";
  C.shutdown context;
  S.shutdown foreign

(** Cancellation must remove its observer from a still-pending terminal future;
    keeping that future alive must not keep the cancelled scope alive too. *)
let test_cancel_detaches_terminal_observer () =
  let scheduler = S.create () in
  let context = C.create scheduler in
  let source, resolve = S.promise scheduler
      ~outside_error:(fun () -> Temporal.Error.defect ~message:"outside") in
  let weak = Weak.create 1 in
  run scheduler context (fun () ->
      let scope = public (Temporal.Scope.create ()) in
      Weak.set weak 0 (Some scope);
      public (Temporal.Scope.on_cancel ~until:(facade source) scope (fun () -> Ok ()));
      public (Temporal.Scope.cancel scope));
  ignore (live_words ());
  if Weak.check weak 0 then failwith "pending terminal future retained cancelled scope";
  resolve (Ok ());
  drain scheduler;
  C.shutdown context

(** Runs deterministic registration and bounded-retention regressions. *)
let () =
  List.iter test_scope_batches ["control"; "hooks"; "child"; "await"];
  List.iter (fun first ->
      test_selection_batches false first true;
      test_selection_batches false first false;
      test_selection_batches true first false) [false; true];
  test_unsubscribe false;
  test_unsubscribe true;
  test_inline_selection ();
  test_hook_lifetimes ();
  test_cancel_detaches_terminal_observer ()
