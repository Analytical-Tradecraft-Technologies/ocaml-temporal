(** Regression tests for the non-yielding workflow activation watchdog (#493).

    A workflow in these fixtures spins on an atomic release flag that the test
    controls, so it stops yielding for exactly as long as the test needs and
    can then be unstuck deterministically. The workflow lane runs [poll] on its
    own Domain, as the production worker loop does, while the test Domain plays
    the watchdog. The tests prove that the watchdog fails the stuck task
    exactly once, that the lane's late completion is dropped rather than
    submitted a second time for the same lease, that health stays sticky, and
    that ordinary activations are never affected. Query-only and
    eviction-only activations are stalled inside the activation observer,
    which runs inside the watchdog's window, to prove the report names the
    completion actually submitted rather than claiming a task failure. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Adapter = Temporal_runtime.Native_worker_execution
module Watchdog = Temporal_runtime.Native_worker_watchdog

(** A deterministic source error used by the fake supervisor. *)
type source_error = { code : string; message : string }

(** Fake supervisor state shared between the lane Domain and the watchdog
    (test) Domain. [mutex] makes cross-Domain reads of the ledgers well
    defined; the adapter itself never overlaps two calls for one lease. *)
type fake_supervisor = {
  (* Activations waiting to be leased by the adapter. *)
  queue : Protocol.activation Queue.t;
  (* Run IDs whose native lease has not yet been completed. *)
  leased : (string, unit) Hashtbl.t;
  (* Every accepted completion, newest first. *)
  completions : Protocol.completion list ref;
  (* Guards every field above. *)
  mutex : Mutex.t;
}

(** Allocates an empty queue and lease ledger. *)
let fake_supervisor () =
  {
    queue = Queue.create ();
    leased = Hashtbl.create 4;
    completions = ref [];
    mutex = Mutex.create ();
  }

(** Runs [body] with the fake's mutex held. *)
let locked supervisor body =
  Mutex.lock supervisor.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock supervisor.mutex) body

(** A source that rejects a completion for a lease it does not hold, so a
    second completion of one activation is observable as a stale-lease
    rejection rather than silently accepted. *)
module Fake_supervisor = struct
  type t = fake_supervisor
  type error = source_error

  (** Leases one queued activation by run ID. *)
  let try_poll_workflow supervisor =
    locked supervisor (fun () ->
        if Queue.is_empty supervisor.queue then Ok None
        else
          let activation = Queue.take supervisor.queue in
          Hashtbl.replace supervisor.leased activation.run_id ();
          Ok (Some activation))

  (** Retires the exact leased run ID and records the typed completion. *)
  let complete_workflow supervisor ~(completion : Protocol.completion) _encoded
      =
    locked supervisor (fun () ->
        if Hashtbl.mem supervisor.leased completion.run_id then begin
          Hashtbl.remove supervisor.leased completion.run_id;
          supervisor.completions := completion :: !(supervisor.completions);
          Ok ()
        end
        else Error { code = "stale_lease"; message = "run is not leased" })

  (** Exposes the source classification expected by the adapter signature. *)
  let error_code error = error.code

  (** Exposes the source diagnostic expected by the adapter signature. *)
  let error_message error = error.message

  (** No fake failure is retryable. *)
  let error_is_retryable _ = false

  (** A raised completion is fail-closed. *)
  let exception_is_retryable _ = false
end

module Worker = Adapter.Make (Fake_supervisor)

(** The canonical timestamp used by activation fixtures. *)
let timestamp : Protocol.timestamp = { seconds = 1L; nanoseconds = 0 }

(** Builds a start activation for [workflow_type]. *)
let start_activation ~run_id ~workflow_type : Protocol.activation =
  {
    run_id;
    timestamp = Some timestamp;
    is_replaying = false;
    history_length = 1L;
    jobs =
      [
        Protocol.Initialize_workflow
          {
            workflow_id = "workflow-" ^ run_id;
            workflow_type;
            arguments = [];
            randomness_seed = "1";
            attempt = 1;
            context = None;
          };
      ];
    metadata = None;
  }

(** Builds Core's synthetic eviction activation for [run_id]. *)
let eviction_activation ~run_id : Protocol.activation =
  {
    run_id;
    timestamp = None;
    is_replaying = false;
    history_length = 1L;
    jobs =
      [
        Protocol.Remove_from_cache
          { message = "task failed"; reason = Protocol.Lang_fail };
      ];
    metadata = None;
  }

(** Builds a query-only activation for [run_id] with one query per ID. *)
let query_activation ~run_id ~query_ids : Protocol.activation =
  {
    run_id;
    timestamp = Some timestamp;
    is_replaying = false;
    history_length = 1L;
    jobs =
      List.map
        (fun query_id ->
          Protocol.Query_workflow
            { query_id; query_type = "state"; arguments = []; headers = [] })
        query_ids;
    metadata = None;
  }

(** Adds one activation to the fake source queue. *)
let enqueue supervisor activation =
  locked supervisor (fun () -> Queue.add activation supervisor.queue)

(** Snapshot of the accepted completions, oldest first. *)
let completions supervisor =
  locked supervisor (fun () -> List.rev !(supervisor.completions))

(** A workflow that spins without yielding until [release] is set. [entered]
    proves the lane reached user code. *)
let spinning_workflow ~entered ~release =
  Temporal_base.Definition.make ~name:"spinning" ~input:Temporal_base.Codec.unit
    ~output:Temporal_base.Codec.unit
    ~implementation:
      (Some
         (fun () ->
           Atomic.set entered true;
           while not (Atomic.get release) do
             Domain.cpu_relax ()
           done;
           Ok ()))

(** A workflow that returns immediately. *)
let quick_workflow =
  Temporal_base.Definition.make ~name:"quick" ~input:Temporal_base.Codec.unit
    ~output:Temporal_base.Codec.unit
    ~implementation:(Some (fun () -> Ok ()))

(** Creates an adapter registering both fixtures. *)
let worker supervisor ~entered ~release =
  match
    Worker.create ~supervisor
      ~workflows:
        [
          Adapter.register (spinning_workflow ~entered ~release);
          Adapter.register quick_workflow;
        ]
      ()
  with
  | Ok worker -> worker
  | Error error -> failwith ("worker creation failed: " ^ error.message)

(** Creates an adapter whose activation observer spins until [release] is
    set. The observer runs after the activation is published to the watchdog
    and before any workflow or eviction handling, so it stalls activations
    that never enter a workflow function, such as query-only and eviction-only
    ones. [entered] proves the lane reached the observer. *)
let stalling_observer_worker supervisor ~entered ~release =
  let on_activation (_ : Adapter.activation_info) =
    Atomic.set entered true;
    while not (Atomic.get release) do
      Domain.cpu_relax ()
    done
  in
  match
    Worker.create ~on_activation ~supervisor
      ~workflows:[ Adapter.register quick_workflow ]
      ()
  with
  | Ok worker -> worker
  | Error error -> failwith ("worker creation failed: " ^ error.message)

(** Removes [run_id]'s lease from the fake so its next completion is rejected
    as stale, simulating a supervisor that cannot acknowledge the watchdog. *)
let revoke_lease supervisor ~run_id =
  locked supervisor (fun () -> Hashtbl.remove supervisor.leased run_id)

(** Polls [predicate] until it holds, failing after ten seconds. *)
let await ~what predicate =
  let rec loop remaining =
    if predicate () then ()
    else if remaining = 0 then failwith ("timed out waiting for " ^ what)
    else begin
      Thread.delay 0.001;
      loop (remaining - 1)
    end
  in
  loop 10_000

(** Requires one task-failure completion for [run_id]. *)
let expect_task_failure (completion : Protocol.completion) ~run_id =
  if not (String.equal completion.run_id run_id) then
    failwith "watchdog completed the wrong run";
  match completion.task_failure with
  | Some failure ->
      if completion.commands <> [] then
        failwith "watchdog failure carried workflow commands";
      if not (String.length failure.message > 0) then
        failwith "watchdog failure has no diagnostic"
  | None -> failwith "watchdog did not fail the workflow task"

(** The pure detection rule fires once per epoch, only after the deadline has
    been observed continuously, and restarts on a new epoch or idle lane. *)
let test_step_rule () =
  let step state running =
    Watchdog.step state ~running ~tick_ms:10 ~deadline_ms:30
  in
  let state, fired = step Watchdog.initial (Some 1) in
  if fired <> None then failwith "fired on first observation";
  let state, _ = step state (Some 1) in
  let state, _ = step state (Some 1) in
  let state, fired = step state (Some 1) in
  (match fired with
  | Some (1, 30) -> ()
  | _ -> failwith "did not fire at the deadline");
  let state, fired = step state (Some 1) in
  if fired <> None then failwith "fired twice for one epoch";
  (* A new epoch restarts the count. *)
  let state, fired = step state (Some 2) in
  if fired <> None then failwith "new epoch inherited elapsed time";
  let state, _ = step state (Some 2) in
  (* An idle sample resets, so quick successive activations never add up. *)
  let state, _ = step state None in
  let state, _ = step state (Some 2) in
  let _, fired = step state (Some 2) in
  if fired <> None then failwith "idle sample did not reset elapsed time";
  if Watchdog.tick_ms ~deadline_ms:2_000 <> 250 then
    failwith "unexpected tick for the default deadline";
  if Watchdog.tick_ms ~deadline_ms:8 <> 5 then
    failwith "tick lower bound not applied"

(** Starts [Worker.poll] on a separate Domain, as the production loop does. *)
let poll_on_lane worker = Domain.spawn (fun () -> Worker.poll worker)

(** The watchdog fails a stuck activation once; the lane's late completion is
    dropped; health is sticky; and the evicted run is acknowledged later. *)
let test_abandon_drops_late_completion () =
  let supervisor = fake_supervisor () in
  let entered = Atomic.make false and release = Atomic.make false in
  let worker = worker supervisor ~entered ~release in
  let run_id = "run-stuck" in
  enqueue supervisor (start_activation ~run_id ~workflow_type:"spinning");
  let lane = poll_on_lane worker in
  await ~what:"workflow code" (fun () -> Atomic.get entered);
  let epoch =
    match Worker.running_epoch worker with
    | Some epoch -> epoch
    | None -> failwith "stuck activation was not published"
  in
  (match Worker.abandon_activation worker ~epoch ~elapsed_ms:2_500 with
  | Some stuck ->
      if stuck.abandoned <> `Task_failed then
        failwith "task failure not reported as acknowledged";
      if stuck.workflow_type <> Some "spinning" then
        failwith "diagnostic lost the workflow type";
      if stuck.workflow_id <> Some ("workflow-" ^ run_id) then
        failwith "diagnostic lost the workflow ID";
      if stuck.elapsed_ms <> 2_500 then failwith "diagnostic lost elapsed time"
  | None -> failwith "watchdog did not abandon a stuck activation");
  (match completions supervisor with
  | [ completion ] -> expect_task_failure completion ~run_id
  | _ -> failwith "watchdog must submit exactly one completion");
  if Worker.abandon_activation worker ~epoch ~elapsed_ms:5_000 <> None then
    failwith "watchdog abandoned one activation twice";
  if Worker.running_epoch worker <> None then
    failwith "abandoned activation still reported as running";
  if Option.is_none (Worker.stuck worker) then
    failwith "health was not marked stuck";
  (* Unstick the code: its successful completion must not reach the source. *)
  Atomic.set release true;
  (match Domain.join lane with
  | Ok (Adapter.Rejected
          { error = { code = "activation_deadline_exceeded"; _ };
            lease_retired = true; _ }) -> ()
  | Ok _ -> failwith "late lane result was not reported as abandoned"
  | Error error -> failwith ("late lane returned an error: " ^ error.code));
  if List.length (completions supervisor) <> 1 then
    failwith "late completion was submitted after the watchdog";
  if Option.is_none (Worker.stuck worker) then
    failwith "health recovered without a restart";
  (* Core evicts a run whose task failed; the dropped run takes the empty
     acknowledgement path. *)
  enqueue supervisor (eviction_activation ~run_id);
  (match Worker.poll worker with
  | Ok (Adapter.Completed { command_count = 0; _ }) -> ()
  | Ok _ -> failwith "eviction of an abandoned run was not acknowledged"
  | Error error -> failwith ("eviction failed: " ^ error.code));
  (match List.rev (completions supervisor) with
  | latest :: _ ->
      if latest.task_failure <> None || latest.commands <> [] then
        failwith "eviction acknowledgement was not empty"
  | [] -> failwith "missing eviction acknowledgement");
  match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("adapter retained a pending completion: " ^ error.code)

(** Leases the single queued activation on a lane Domain, waits until the
    observer stalls it, abandons it as the watchdog would, and releases the
    lane. Returns the watchdog report and the lane's late result. [before]
    runs after the stall is observed and before the abandonment. *)
let abandon_stalled_activation ?(before = fun () -> ()) worker ~entered
    ~release =
  let lane = poll_on_lane worker in
  await ~what:"activation observer" (fun () -> Atomic.get entered);
  let epoch =
    match Worker.running_epoch worker with
    | Some epoch -> epoch
    | None -> failwith "stalled activation was not published"
  in
  before ();
  let stuck =
    match Worker.abandon_activation worker ~epoch ~elapsed_ms:2_500 with
    | Some stuck -> stuck
    | None -> failwith "watchdog did not abandon a stalled activation"
  in
  Atomic.set release true;
  (stuck, Domain.join lane)

(** A stuck query-only activation is released by failing its queries. The
    report says so and does not claim a workflow-task failure. *)
let test_stuck_query_reports_failed_queries () =
  let supervisor = fake_supervisor () in
  let entered = Atomic.make false and release = Atomic.make false in
  let worker = stalling_observer_worker supervisor ~entered ~release in
  let run_id = "run-query" in
  enqueue supervisor
    (query_activation ~run_id ~query_ids:[ "query-1"; "query-2" ]);
  let stuck, late = abandon_stalled_activation worker ~entered ~release in
  if stuck.abandoned <> `Queries_failed then
    failwith "stuck query activation not reported as failed queries";
  (match late with
  | Ok (Adapter.Rejected { lease_retired = true; _ }) -> ()
  | _ -> failwith "late query result was not dropped");
  (match completions supervisor with
  | [ { task_failure = None; commands; _ } ] ->
      let failed_ids =
        List.filter_map
          (function
            | Protocol.Query_result
                { query_id; result = Protocol.Query_failed _ } ->
                Some query_id
            | _ -> None)
          commands
      in
      if failed_ids <> [ "query-1"; "query-2" ] then
        failwith "watchdog did not fail every delivered query"
  | [ _ ] -> failwith "stuck query activation failed the workflow task"
  | _ -> failwith "watchdog must submit exactly one completion");
  match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("adapter retained a pending completion: " ^ error.code)

(** A stuck eviction-only activation is released with the empty eviction
    acknowledgement, and the report does not claim a workflow-task failure. *)
let test_stuck_eviction_reports_acknowledgement () =
  let supervisor = fake_supervisor () in
  let entered = Atomic.make false and release = Atomic.make false in
  let worker = stalling_observer_worker supervisor ~entered ~release in
  let run_id = "run-evicted" in
  enqueue supervisor (eviction_activation ~run_id);
  let stuck, late = abandon_stalled_activation worker ~entered ~release in
  if stuck.abandoned <> `Eviction_acknowledged then
    failwith "stuck eviction not reported as acknowledged";
  (match late with
  | Ok (Adapter.Rejected { lease_retired = true; _ }) -> ()
  | _ -> failwith "late eviction result was not dropped");
  (match completions supervisor with
  | [ { task_failure = None; commands = []; _ } ] -> ()
  | [ _ ] -> failwith "stuck eviction was not acknowledged with an empty completion"
  | _ -> failwith "watchdog must submit exactly one completion");
  match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("adapter retained a pending completion: " ^ error.code)

(** A watchdog completion the supervisor rejects is reported as
    unacknowledged, never as a task failure, and the lane's late result
    becomes an error because the lease state is unknown. *)
let test_unacknowledged_abandonment () =
  let supervisor = fake_supervisor () in
  let entered = Atomic.make false and release = Atomic.make false in
  let worker = stalling_observer_worker supervisor ~entered ~release in
  let run_id = "run-unacknowledged" in
  enqueue supervisor (start_activation ~run_id ~workflow_type:"quick");
  let stuck, late =
    abandon_stalled_activation worker ~entered ~release
      ~before:(fun () -> revoke_lease supervisor ~run_id)
  in
  if stuck.abandoned <> `Not_acknowledged then
    failwith "rejected watchdog completion not reported as unacknowledged";
  (match late with
  | Error _ -> ()
  | Ok _ -> failwith "late lane result hid an unacknowledged lease");
  if completions supervisor <> [] then
    failwith "fake accepted a completion for a revoked lease"

(** The real watchdog Domain detects a stuck activation within its declared
    bound and leaves quick activations alone. *)
let test_watchdog_domain_detects_once () =
  let supervisor = fake_supervisor () in
  let entered = Atomic.make false and release = Atomic.make false in
  let worker = worker supervisor ~entered ~release in
  let fired = Atomic.make 0 in
  let watchdog =
    match
      Watchdog.start ~deadline_ms:50
        ~running_epoch:(fun () -> Worker.running_epoch worker)
        ~abandon:(fun ~epoch ~elapsed_ms ->
          match Worker.abandon_activation worker ~epoch ~elapsed_ms with
          | Some _ -> Atomic.incr fired
          | None -> ())
    with
    | Ok watchdog -> watchdog
    | Error _ -> failwith "watchdog Domain did not start"
  in
  Fun.protect
    ~finally:(fun () ->
      Atomic.set release true;
      Watchdog.stop watchdog)
    (fun () ->
      (* Quick activations stay healthy while the watchdog samples. *)
      for index = 1 to 5 do
        let run_id = Printf.sprintf "run-quick-%d" index in
        enqueue supervisor (start_activation ~run_id ~workflow_type:"quick");
        match Worker.poll worker with
        | Ok (Adapter.Completed { terminal = true; _ }) -> ()
        | _ -> failwith "quick workflow did not complete normally"
      done;
      if Worker.stuck worker <> None then
        failwith "quick workflows were reported stuck";
      let run_id = "run-spinning" in
      enqueue supervisor (start_activation ~run_id ~workflow_type:"spinning");
      let lane = poll_on_lane worker in
      await ~what:"watchdog detection" (fun () ->
          Option.is_some (Worker.stuck worker));
      (match Worker.stuck worker with
      | Some stuck when stuck.elapsed_ms >= 50 -> ()
      | Some _ -> failwith "watchdog fired before the deadline"
      | None -> ());
      (* Give the watchdog several more ticks to prove it fires once. *)
      Thread.delay 0.1;
      if Atomic.get fired <> 1 then failwith "watchdog did not fire exactly once";
      Atomic.set release true;
      (match Domain.join lane with
      | Ok (Adapter.Rejected { lease_retired = true; _ }) -> ()
      | _ -> failwith "late completion was not dropped");
      let failures =
        List.filter
          (fun (completion : Protocol.completion) ->
            String.equal completion.run_id run_id)
          (completions supervisor)
      in
      match failures with
      | [ completion ] -> expect_task_failure completion ~run_id
      | _ -> failwith "stuck run was completed more than once")

(** A completed activation can no longer be claimed, so a watchdog that
    samples an epoch just before the lane finishes never submits a second
    completion. *)
let test_finished_epoch_is_not_abandoned () =
  let supervisor = fake_supervisor () in
  let entered = Atomic.make false and release = Atomic.make false in
  let worker = worker supervisor ~entered ~release in
  Atomic.set release true;
  enqueue supervisor (start_activation ~run_id:"run-done" ~workflow_type:"spinning");
  (match Worker.poll worker with
  | Ok (Adapter.Completed _) -> ()
  | _ -> failwith "released workflow did not complete");
  if Worker.running_epoch worker <> None then
    failwith "finished activation still published";
  if Worker.abandon_activation worker ~epoch:0 ~elapsed_ms:10_000 <> None then
    failwith "watchdog claimed a completed activation";
  if List.length (completions supervisor) <> 1 then
    failwith "finished activation was completed twice";
  if Worker.stuck worker <> None then failwith "finished activation marked stuck"

(** The public option validates the deadline and offers an explicit disable;
    the mock backend has no watchdog and always reports healthy. *)
let test_public_options () =
  let module W = Temporal.Worker in
  (match W.Options.workflow_activation_deadline W.Options.default with
  | `After duration when Temporal.Duration.to_ms duration = 2_000L -> ()
  | _ -> failwith "default activation deadline is not two seconds");
  (match W.Options.make ~workflow_activation_deadline:`Disabled () with
  | Ok options when W.Options.workflow_activation_deadline options = `Disabled
    -> ()
  | _ -> failwith "watchdog could not be disabled");
  (match
     W.Options.make
       ~workflow_activation_deadline:(`After (Temporal.Duration.of_ms 0L))
       ()
   with
  | Error _ -> ()
  | Ok _ -> failwith "zero activation deadline was accepted");
  (match
     W.Options.make
       ~workflow_activation_deadline:
         (`After (Temporal.Duration.of_ms 3_600_001L))
       ()
   with
  | Error _ -> ()
  | Ok _ -> failwith "over-long activation deadline was accepted");
  match
    W.create ~target_url:"mock://watchdog" ~namespace:"default"
      ~task_queue:"watchdog" ~workflows:[] ~activities:[] ()
  with
  | Ok worker -> (
      (match W.health worker with
      | W.Health.Healthy -> ()
      | W.Health.Stuck_workflow_activation _ ->
          failwith "mock worker reported stuck");
      match W.shutdown worker with
      | Ok () -> ()
      | Error _ -> failwith "mock worker shutdown failed")
  | Error _ -> failwith "mock worker creation failed"

(** Runs the watchdog regressions. *)
let () =
  test_step_rule ();
  test_abandon_drops_late_completion ();
  test_watchdog_domain_detects_once ();
  test_finished_epoch_is_not_abandoned ();
  test_stuck_query_reports_failed_queries ();
  test_stuck_eviction_reports_acknowledgement ();
  test_unacknowledged_abandonment ();
  test_public_options ()
