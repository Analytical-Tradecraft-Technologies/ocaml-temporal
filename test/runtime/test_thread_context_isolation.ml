(** Regression tests for #765: workflow context and scheduler ownership are
    bound per system thread, not per Domain.

    Two workers hosted on sibling system threads of one Domain may interleave
    their activations. These tests force that interleaving deterministically
    with a blocking rendezvous inside each workflow fiber, so both threads have
    an activation installed at the same time, and then check that each thread
    still observes only its own execution, records commands only into its own
    buffer, and leaves no binding behind once its extent ends. *)

module Scheduler = Temporal_runtime.Scheduler
module Context = Temporal_runtime.Workflow_context_store
module Future_store = Temporal_runtime.Future_store
module Activation = Temporal_runtime.Activation
module Thread_binding = Temporal_runtime.Thread_binding

(** Fails with a short scenario-specific message when a check is false. *)
let check label condition = if not condition then failwith label

(** A monotonically increasing stage shared by the threads of one scenario.
    Waiting blocks in [Condition.wait], which releases the OCaml runtime lock,
    so the sibling thread is guaranteed to run while a fiber is parked here. *)
type stage = { mutex : Mutex.t; changed : Condition.t; mutable value : int }

(** Creates a stage counter starting at zero. *)
let stage () = { mutex = Mutex.create (); changed = Condition.create (); value = 0 }

(** Advances the stage by one and wakes every waiter. *)
let advance stage =
  Mutex.protect stage.mutex (fun () ->
      stage.value <- stage.value + 1;
      Condition.broadcast stage.changed)

(** Blocks the calling thread until the stage reaches at least [target]. *)
let await_stage stage target =
  Mutex.protect stage.mutex (fun () ->
      while stage.value < target do
        Condition.wait stage.changed stage.mutex
      done)

(** Releases every waiter after a failed check, so a regression makes the
    test fail with its diagnostic instead of deadlocking at a rendezvous. *)
let abort stage =
  Mutex.protect stage.mutex (fun () ->
      stage.value <- max_int;
      Condition.broadcast stage.changed)

(** Unscheduled remote activity used only to buffer a durable command. *)
let activity =
  Temporal.Activity.remote ~name:"thread-isolation" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.unit

(** Checks that the calling thread sees exactly [context] as current and owns
    exactly [scheduler], and does not own [other_scheduler]. *)
let expect_owner label context scheduler other_scheduler =
  check (label ^ ": wrong current context")
    (match Context.current () with
    | Some current -> current == context
    | None -> false);
  check (label ^ ": not the scheduler owner")
    (Future_store.current_owner_matches (Scheduler.id scheduler));
  check (label ^ ": owns the sibling scheduler")
    (not (Future_store.current_owner_matches (Scheduler.id other_scheduler)))

(** Returns how many activity schedules [context] buffered. *)
let scheduled_activities context =
  List.length
    (List.filter
       (function Activation.Schedule_activity _ -> true | _ -> false)
       (Context.take_commands context))

(** Runs one fiber on [scheduler] the same way the activation adapter does: the
    whole drain executes inside [with_context]. *)
let drain scheduler context body =
  Scheduler.spawn scheduler body;
  match Context.with_context context (fun () -> Scheduler.run scheduler) with
  | Scheduler.Failed error -> raise error
  | Scheduler.Complete | Scheduler.Blocked -> ()

(** Reproduces the failure scenario from #765. Thread A installs its context,
    thread B installs its context while A is parked mid-activation, A then
    records a command and finishes while B is still mid-activation, and B
    records its command last. With a Domain-shared binding, A would see B's
    context, B would be left outside any workflow after A restored, and the
    Domain would keep A's stale context after B restored. *)
let test_interleaved_activations () =
  let stage = stage () in
  let scheduler_a = Scheduler.create () and scheduler_b = Scheduler.create () in
  let context_a = Context.create scheduler_a
  and context_b = Context.create scheduler_b in
  let failure = Atomic.make None in
  let guard body () =
    try body ()
    with error ->
      ignore (Atomic.compare_and_set failure None (Some error));
      abort stage
  in
  let worker_a =
    Thread.create
      (guard (fun () ->
           drain scheduler_a context_a (fun () ->
               expect_owner "A before B" context_a scheduler_a scheduler_b;
               advance stage;
               (* Stage 1: A installed. Park until B is installed too. *)
               await_stage stage 2;
               expect_owner "A while B installed" context_a scheduler_a
                 scheduler_b;
               ignore (Temporal.Activity.start_handle activity ());
               Thread.yield ();
               expect_owner "A after yield" context_a scheduler_a scheduler_b);
           check "A left its context installed" (Context.current () = None);
           (* Stage 3: A finished while B is still mid-activation. *)
           advance stage))
      ()
  in
  let worker_b =
    Thread.create
      (guard (fun () ->
           await_stage stage 1;
           drain scheduler_b context_b (fun () ->
               expect_owner "B while A installed" context_b scheduler_b
                 scheduler_a;
               advance stage;
               await_stage stage 3;
               expect_owner "B after A finished" context_b scheduler_b
                 scheduler_a;
               ignore (Temporal.Activity.start_handle activity ()));
           check "B left its context installed" (Context.current () = None)))
      ()
  in
  Thread.join worker_a;
  Thread.join worker_b;
  Option.iter raise (Atomic.get failure);
  check "A did not record exactly one command" (scheduled_activities context_a = 1);
  check "B did not record exactly one command" (scheduled_activities context_b = 1);
  check "stale context on the Domain" (Context.current () = None);
  check "context bindings leaked" (Context.bound_thread_count () = 0);
  check "owner bindings leaked" (Future_store.bound_owner_count () = 0);
  Context.shutdown context_a;
  Context.shutdown context_b

(** A thread spawned while its parent is inside a workflow extent starts with
    no workflow installed, so helper threads cannot mutate workflow state. *)
let test_spawned_thread_does_not_inherit () =
  let scheduler = Scheduler.create () in
  let context = Context.create scheduler in
  let observed = ref (Some context) in
  Context.with_context context (fun () ->
      Thread.join (Thread.create (fun () -> observed := Context.current ()) ()));
  check "spawned thread inherited the context" (!observed = None);
  check "context bindings leaked" (Context.bound_thread_count () = 0);
  Context.shutdown context

(** Every extent removes its entry even when it ends by an exception, and an
    explicit [without_context] mask restores the outer binding. *)
let test_cleanup_on_exception () =
  let scheduler = Scheduler.create () in
  let context = Context.create scheduler in
  let worker =
    Thread.create
      (fun () ->
        (try
           Context.with_context context (fun () ->
               Context.without_context (fun () ->
                   check "mask kept the context" (Context.current () = None));
               check "mask did not restore the context"
                 (match Context.current () with
                 | Some current -> current == context
                 | None -> false);
               ignore
                 (Context.with_read_only_query context (fun () ->
                      failwith "boom")))
         with Failure _ -> ());
        check "exception left a context" (Context.current () = None))
      ()
  in
  Thread.join worker;
  check "context bindings leaked" (Context.bound_thread_count () = 0);
  Context.shutdown context

(** Stresses the binding primitive directly: many threads of one Domain nest
    distinct values and yield between every step, so the runtime switches
    threads while sibling writes are in flight. Every read must return the
    calling thread's own innermost value and every entry must be removed. *)
let test_binding_stress () =
  let slot = Thread_binding.create () in
  let threads = 8 and rounds = 200 in
  let failure = Atomic.make None in
  let body index () =
    try
      for round = 1 to rounds do
        let outer = (index * 1_000_000) + round in
        Thread_binding.with_value slot (Some outer) (fun () ->
            Thread.yield ();
            check "outer value" (Thread_binding.get slot = Some outer);
            Thread_binding.with_value slot (Some (-outer)) (fun () ->
                Thread.yield ();
                check "inner value" (Thread_binding.get slot = Some (-outer)));
            Thread_binding.with_value slot None (fun () ->
                Thread.yield ();
                check "masked value" (Thread_binding.get slot = None));
            check "restored value" (Thread_binding.get slot = Some outer))
      done;
      check "thread kept a binding" (Thread_binding.get slot = None)
    with error -> Atomic.set failure (Some error)
  in
  List.init threads (fun index -> Thread.create (body index) ())
  |> List.iter Thread.join;
  Option.iter raise (Atomic.get failure);
  check "binding entries leaked" (Thread_binding.bound_count slot = 0)

let () =
  test_interleaved_activations ();
  test_spawned_thread_does_not_inherit ();
  test_cleanup_on_exception ();
  test_binding_stress ();
  print_endline "thread context isolation: ok"
