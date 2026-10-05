(** Regression for #847: settling one future must not cost O(pending futures).

    Each scenario builds a wide fan-out on one scheduler, settles it, and
    checks two things: the exact deterministic order in which continuations,
    hooks, or callbacks run (unchanged from the list-based implementation),
    and a CPU-time budget.  The budget is deliberately generous: at these
    sizes the previous filter-based removal took tens of seconds to minutes,
    whereas the linear implementation takes well under a second, so the bound
    separates the two complexity classes without behaving like a flaky
    micro-benchmark. *)

module S = Temporal_runtime.Scheduler
module C = Temporal_runtime.Workflow_context_store
module F = Temporal_runtime.Future_store

(** Number of concurrently pending registrations in every scenario. *)
let width = 50_000

(** CPU seconds allowed per scenario; roughly two orders of magnitude above
    the linear implementation and well below the quadratic one. *)
let budget_seconds = 10.0

(** Fails with the scenario label when [expected] and [actual] differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Runs [action] and fails if it used more than [budget_seconds] of CPU. *)
let within_budget label action =
  let started = Sys.time () in
  let value = action () in
  let elapsed = Sys.time () -. started in
  Printf.printf "%s cpu_seconds=%.3f\n%!" label elapsed;
  if elapsed > budget_seconds then
    failwith (Printf.sprintf "%s took %.1fs; settling is not linear" label elapsed);
  value

(** Structured defect used as every test future's outside-owner error. *)
let outside_error () = Temporal.Error.defect ~message:"outside test scheduler"

(** Drains one scheduler turn and surfaces any fiber defect. *)
let drain scheduler =
  match S.run scheduler with
  | S.Failed error -> raise error
  | S.Complete | S.Blocked -> ()

(** Runs [body] on its owning scheduler with [context] installed. *)
let run scheduler context body =
  S.spawn scheduler (fun () -> C.with_context context body);
  drain scheduler

(** Unwraps a public result without hiding fixture failures. *)
let public = function
  | Ok value -> value
  | Error error -> failwith (Temporal.Error.message error)

(** Adapts a controllable runtime future to the public facade. *)
let facade future =
  Temporal_future_kernel.make
    ~await:(fun () -> F.await future) ~await_gate:(F.await_gate future)
    ~subscribe:(F.subscribe future) ~is_ready:(fun () -> F.is_ready future)
    ~peek:(fun () -> F.peek future) ~owner_id:(F.owner_id future)
    ~outside_error ~callbacks_live:(fun () -> F.callbacks_live future)
    ~enqueue:(F.enqueue future)

(** [width] fibers each await their own future; settling every future in
    reverse creation order must resume fibers in resolution order and leave
    the scheduler complete.  This is the "many pending timers fired in one
    activation" shape from the issue. *)
let test_fan_out_settle () =
  let scheduler = S.create () in
  let resolvers = Array.make width (fun _ -> ()) in
  let seen = ref [] in
  for index = 0 to width - 1 do
    let future, resolve = S.promise scheduler ~outside_error in
    resolvers.(index) <- resolve;
    S.spawn scheduler (fun () ->
        match F.await future with
        | Ok value -> seen := value :: !seen
        | Error _ -> failwith "fan-out await failed")
  done;
  drain scheduler;
  within_budget "fan-out settle" (fun () ->
      for index = width - 1 downto 0 do
        resolvers.(index) (Ok index)
      done;
      expect "fan-out status" "complete" (S.run_label scheduler));
  (* [seen] is newest-first, so resolution order (descending) reads ascending. *)
  expect "fan-out resume order" (List.init width Fun.id) !seen;
  S.shutdown scheduler

(** Settling every other future and then shutting down must tear the rest
    down in creation order, discontinuing exactly the still-pending fibers. *)
let test_partial_settle_then_shutdown () =
  let scheduler = S.create () in
  let resolvers = Array.make width (fun _ -> ()) in
  let released = ref [] in
  for index = 0 to width - 1 do
    let future, resolve = S.promise scheduler ~outside_error in
    resolvers.(index) <- resolve;
    S.spawn scheduler (fun () ->
        Fun.protect
          ~finally:(fun () -> released := index :: !released)
          (fun () -> ignore (F.await future)))
  done;
  drain scheduler;
  within_budget "partial settle" (fun () ->
      for index = 0 to width - 1 do
        if index mod 2 = 0 then resolvers.(index) (Ok index)
      done;
      expect "partial status" "blocked" (S.run_label scheduler));
  released := [];
  S.shutdown scheduler;
  let expected = List.filter (fun index -> index mod 2 = 1) (List.init width Fun.id) in
  expect "shutdown teardown order" expected (List.rev !released)

(** [width] condition waiters released by one notification must resume in
    registration order. *)
let test_condition_release () =
  let scheduler = S.create () in
  let context = C.create scheduler in
  let ready = ref false in
  let seen = ref [] in
  for index = 0 to width - 1 do
    S.spawn scheduler (fun () ->
        C.with_context context (fun () ->
            match C.wait_until context ~predicate:(fun () -> Ok !ready) with
            | Ok () -> seen := index :: !seen
            | Error _ -> failwith "condition wait failed"))
  done;
  drain scheduler;
  ready := true;
  within_budget "condition release" (fun () ->
      expect "condition queued" true (C.notify_conditions context);
      expect "condition status" "complete" (S.run_label scheduler));
  expect "condition resume order" (List.init width Fun.id) (List.rev !seen);
  C.shutdown context;
  S.shutdown scheduler

(** [width] scope cancellation hooks bounded by [~until] futures; settling
    most of them unlinks their hooks, and cancellation then runs only the
    survivors, in registration order. *)
let test_scope_hooks () =
  let scheduler = S.create () in
  let context = C.create scheduler in
  let seen = ref [] in
  let scope = ref None in
  let resolvers = Array.make width (fun _ -> ()) in
  run scheduler context (fun () ->
      let created = public (Temporal.Scope.create ()) in
      scope := Some created;
      for index = 0 to width - 1 do
        let future, resolve = S.promise scheduler ~outside_error in
        resolvers.(index) <- resolve;
        public
          (Temporal.Scope.on_cancel ~until:(facade future) created (fun () ->
               seen := index :: !seen;
               Ok ()))
      done);
  let survives index = index mod 1000 = 999 in
  within_budget "scope hook settle" (fun () ->
      Array.iteri
        (fun index resolve -> if not (survives index) then resolve (Ok ()))
        resolvers;
      drain scheduler);
  run scheduler context (fun () ->
      match !scope with
      | Some scope -> public (Temporal.Scope.cancel scope)
      | None -> failwith "scope was not created");
  let expected = List.filter survives (List.init width Fun.id) in
  expect "surviving hook order" expected (List.rev !seen);
  C.shutdown context;
  S.shutdown scheduler

(** Many subscriptions on one long-lived pending future, each removed in
    FIFO order before the future settles (the losing side of a race against
    a shared deadline), then a few survivors delivered in registration
    order. *)
let test_observer_unsubscribe () =
  let scheduler = S.create () in
  let shared, resolve = S.promise scheduler ~outside_error in
  let seen = ref [] in
  let survivors = ref [] in
  within_budget "observer unsubscribe" (fun () ->
      let removals = Queue.create () in
      for index = 0 to width - 1 do
        let remove = F.subscribe shared (fun _ -> seen := index :: !seen) in
        if index mod 10_000 = 0 then survivors := index :: !survivors
        else Queue.push remove removals
      done;
      Queue.iter (fun remove -> remove ()) removals);
  resolve (Ok ());
  drain scheduler;
  expect "observer delivery order" (List.rev !survivors) (List.rev !seen);
  S.shutdown scheduler

(** Runs every scaling and ordering scenario. *)
let () =
  test_fan_out_settle ();
  test_partial_settle_then_shutdown ();
  test_condition_release ();
  test_scope_hooks ();
  test_observer_unsubscribe ()
