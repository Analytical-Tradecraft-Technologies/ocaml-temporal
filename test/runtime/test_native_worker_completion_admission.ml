(** Regression tests for workflow completions admitted without waiting for
    Temporal Core.

    The live workflow lane used to submit each completion synchronously, so a
    completion whose Core call (and server RPC) was slow kept every other run
    waiting. The adapter now hands the completion to the source, keeps the
    exact bytes owned, and settles its result when the source publishes an
    outcome. These tests drive that path with a deterministic fake source
    whose outcomes are held until the test releases them:

    - an admitted completion does not delay another run's activation;
    - a bounded window stops delivery while full and resumes once an outcome
      is collected, without losing the wake;
    - a run's next activation waits for its previous outcome, and a stale
      ticket cannot settle a newer activation;
    - failures before admission and unknown or rejected outcomes after it
      fail closed and are never resubmitted;
    - drain waits for admitted outcomes, reports completions still in flight
      as retryable, and forced discard releases them without a native call. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Adapter = Temporal_runtime.Native_worker_execution

(** A deterministic source error. *)
type source_error = { code : string; message : string }

(** One completion the fake has admitted and not yet published. *)
type admitted = {
  ticket : int;
  run_id : string;
  result : Protocol.completion_result;
}

(** Fake admitting source. [capacity] mirrors the bridge's completion window:
    a credit is held from delivery until the outcome is taken. *)
type fake = {
  queue : Protocol.activation Queue.t;
  (* Runs currently leased to the adapter (delivered, not yet admitted). *)
  leased : (string, unit) Hashtbl.t;
  (* Admitted completions whose outcome the test has not released. *)
  held : admitted list ref;
  (* Released outcomes waiting for [take_workflow_outcomes]. *)
  ready : Protocol.completion_outcome Queue.t;
  capacity : int;
  next_ticket : int ref;
  (* Every admission, as (run, ticket), to prove no lease is sent twice. *)
  submissions : (string * int) list ref;
  (* Ordered record of source calls and test steps. *)
  events : string list ref;
  (* Largest number of credits held at once. *)
  max_held : int ref;
  (* Result recorded for each run's next admission. *)
  results : (string, Protocol.completion_result) Hashtbl.t;
  (* Runs whose submission is rejected before admission. *)
  reject_before_admission : (string, unit) Hashtbl.t;
  (* Called by [wait_workflow_outcome] before it answers; the race test uses
     it to publish an outcome while the adapter waits. *)
  on_wait : (unit -> unit) ref;
}

(** Records one event. *)
let record fake event = fake.events := event :: !(fake.events)

(** The ordered event record. *)
let events fake = List.rev !(fake.events)

(** Credits held anywhere in the fake pipeline. *)
let credits fake =
  Hashtbl.length fake.leased + List.length !(fake.held) + Queue.length fake.ready

(** A fresh fake source with the given window. *)
let fake ?(capacity = 16) () =
  {
    queue = Queue.create ();
    leased = Hashtbl.create 4;
    held = ref [];
    ready = Queue.create ();
    capacity;
    next_ticket = ref 1;
    submissions = ref [];
    events = ref [];
    max_held = ref 0;
    results = Hashtbl.create 4;
    reject_before_admission = Hashtbl.create 4;
    on_wait = ref (fun () -> ());
  }

(** Moves the held completion of [run_id] to the ready queue, as Core
    answering it would. *)
let release fake run_id =
  match List.partition (fun (entry : admitted) -> String.equal entry.run_id run_id) !(fake.held) with
  | [ entry ], rest ->
      fake.held := rest;
      record fake ("released " ^ run_id);
      Queue.add
        Protocol.
          {
            ticket = entry.ticket;
            run_id = entry.run_id;
            result = entry.result;
            core_await_us = 1;
            queued_us = 0;
          }
        fake.ready
  | _ -> failwith ("no single held completion for " ^ run_id)

(** Publishes an outcome for a ticket nothing admitted (a stale outcome). *)
let publish_stale fake ~ticket ~run_id =
  Queue.add
    Protocol.
      {
        ticket;
        run_id;
        result = Completion_accepted;
        core_await_us = 1;
        queued_us = 0;
      }
    fake.ready

module Fake = struct
  type t = fake
  type error = source_error

  (** Delivers one activation while the window has a credit. *)
  let try_poll_workflow fake =
    if Queue.is_empty fake.queue then Ok None
    else if credits fake >= fake.capacity then begin
      record fake "window full";
      Ok None
    end
    else
      let activation = Queue.take fake.queue in
      if Hashtbl.mem fake.leased activation.run_id then
        failwith "fake delivered a second lease for one run";
      Hashtbl.replace fake.leased activation.run_id ();
      fake.max_held := max !(fake.max_held) (credits fake);
      record fake ("delivered " ^ activation.run_id);
      Ok (Some activation)

  (** The watchdog's synchronous path; unused by these tests. *)
  let complete_workflow _ ~completion:_ _ =
    failwith "the lane must not use the synchronous completion"

  (** Admits a leased run's completion, or rejects it before admission. *)
  let submit_workflow fake ~(completion : Protocol.completion) _encoded =
    let run_id = completion.run_id in
    if Hashtbl.mem fake.reject_before_admission run_id then begin
      record fake ("rejected before admission " ^ run_id);
      Error { code = "protocol"; message = "completion failed validation" }
    end
    else if not (Hashtbl.mem fake.leased run_id) then
      failwith ("completion submitted for an unleased run " ^ run_id)
    else begin
      Hashtbl.remove fake.leased run_id;
      let ticket = !(fake.next_ticket) in
      incr fake.next_ticket;
      let result =
        Option.value ~default:Protocol.Completion_accepted
          (Hashtbl.find_opt fake.results run_id)
      in
      fake.held := { ticket; run_id; result } :: !(fake.held);
      fake.submissions := (run_id, ticket) :: !(fake.submissions);
      record fake ("admitted " ^ run_id);
      Ok (Adapter.Admitted ticket)
    end

  (** Takes every released outcome. *)
  let take_workflow_outcomes fake =
    let outcomes = List.of_seq (Queue.to_seq fake.ready) in
    Queue.clear fake.ready;
    List.iter
      (fun (outcome : Protocol.completion_outcome) ->
        record fake ("collected " ^ outcome.run_id))
      outcomes;
    Ok outcomes

  (** Runs the test hook, then answers like the bridge's bounded wait. *)
  let wait_workflow_outcome fake =
    record fake "wait";
    !(fake.on_wait) ();
    if not (Queue.is_empty fake.ready) then Ok `Ready
    else if !(fake.held) = [] then Ok `Idle
    else Ok `Timed_out

  let error_code (error : error) = error.code
  let error_message (error : error) = error.message
  let error_is_retryable _ = false
  let exception_is_retryable _ = false
end

module Worker = Adapter.Make_admitting (Fake)

(** The canonical activation timestamp. *)
let timestamp : Protocol.timestamp = { seconds = 1L; nanoseconds = 0 }

(** First activation of a run of the unit workflow. *)
let start ~run_id : Protocol.activation =
  {
    run_id;
    timestamp = Some timestamp;
    is_replaying = false;
    history_length = 3L;
    jobs =
      [
        Protocol.Initialize_workflow
          {
            workflow_id = "workflow-" ^ run_id;
            workflow_type = "completion_admission";
            arguments = [];
            randomness_seed = "1";
            attempt = 1;
            context = None;
          };
      ];
    metadata = None;
  }

(** Core's cache eviction of a run. *)
let evict ~run_id : Protocol.activation =
  {
    run_id;
    timestamp = Some timestamp;
    is_replaying = false;
    history_length = 3L;
    jobs =
      [
        Protocol.Remove_from_cache
          { message = "completed"; reason = Protocol.Cache_full };
      ];
    metadata = None;
  }

(** A worker running a unit workflow that records each execution. *)
let worker fake =
  let workflow =
    Temporal_base.Definition.make ~name:"completion_admission"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:(Some (fun () -> Ok ()))
  in
  let on_activation (info : Adapter.activation_info) =
    record fake ("executed " ^ info.run_id)
  in
  match
    Worker.create ~on_activation ~supervisor:fake
      ~workflows:[ Adapter.register workflow ] ()
  with
  | Ok worker -> worker
  | Error error -> failwith ("worker creation failed: " ^ error.message)

(** Describes a poll result for failure messages. *)
let describe = function
  | Ok Adapter.Not_ready -> "Not_ready"
  | Ok (Adapter.Submitted { run_id; _ }) -> "Submitted " ^ run_id
  | Ok (Adapter.Completed { run_id; _ }) -> "Completed " ^ run_id
  | Ok (Adapter.Rejected { error; _ }) -> "Rejected " ^ error.code
  | Error (error : Adapter.error_view) -> "Error " ^ error.code

(** Requires [Submitted run_id]. *)
let expect_submitted run_id result =
  match result with
  | Ok (Adapter.Submitted { run_id = actual; _ }) when String.equal actual run_id -> ()
  | other -> failwith ("expected Submitted " ^ run_id ^ ", got " ^ describe other)

(** Requires [Completed run_id] with the given terminal flag. *)
let expect_completed ?terminal run_id result =
  match result with
  | Ok (Adapter.Completed { run_id = actual; terminal = actual_terminal; _ })
    when String.equal actual run_id
         && Option.fold ~none:true ~some:(Bool.equal actual_terminal) terminal -> ()
  | other -> failwith ("expected Completed " ^ run_id ^ ", got " ^ describe other)

(** Requires [Not_ready]. *)
let expect_not_ready result =
  match result with
  | Ok Adapter.Not_ready -> ()
  | other -> failwith ("expected Not_ready, got " ^ describe other)

(** Requires an error with [code] from [poll] or [drain]. *)
let expect_error (type value) code (result : (value, Adapter.error_view) result) =
  match result with
  | Error { code = actual; _ } when String.equal actual code -> ()
  | Error { code = actual; _ } ->
      failwith ("expected error " ^ code ^ ", got error " ^ actual)
  | Ok _ -> failwith ("expected error " ^ code ^ ", got Ok")

(** Requires the exact event sequence. *)
let expect_events fake expected =
  let actual = events fake in
  if actual <> expected then
    failwith
      (Printf.sprintf "event order:\n  expected %s\n  actual   %s"
         (String.concat "; " expected) (String.concat "; " actual))

(** Every lease reached the source once. *)
let expect_single_submissions fake =
  let runs = List.map fst !(fake.submissions) in
  let unique = List.sort_uniq String.compare runs in
  if List.length unique <> List.length runs then
    failwith "a completion was submitted to the source twice"

(** While run A's completion is admitted but unanswered, the same worker
    delivers, executes, and admits run B's, and settles B before A. *)
let test_admitted_completion_does_not_block_another_run () =
  let fake = fake () in
  Queue.add (start ~run_id:"a") fake.queue;
  Queue.add (start ~run_id:"b") fake.queue;
  let worker = worker fake in
  expect_submitted "a" (Worker.poll worker);
  expect_submitted "b" (Worker.poll worker);
  if Worker.admitted_completions worker <> 2 then
    failwith "both completions should be admitted";
  release fake "b";
  expect_completed ~terminal:true "b" (Worker.poll worker);
  if Worker.admitted_completions worker <> 1 then
    failwith "A must still be admitted while its outcome is held";
  expect_not_ready (Worker.poll worker);
  release fake "a";
  expect_completed ~terminal:true "a" (Worker.poll worker);
  expect_events fake
    [
      "delivered a"; "executed a"; "admitted a";
      "delivered b"; "executed b"; "admitted b";
      "released b"; "collected b";
      "released a"; "collected a";
    ];
  expect_single_submissions fake;
  match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("settled worker did not drain: " ^ error.code)

(** A full window stops delivery without dequeuing; collecting one outcome
    frees one credit and the next poll delivers the waiting activation. *)
let test_full_window_pauses_and_resumes_delivery () =
  let fake = fake ~capacity:2 () in
  List.iter (fun run_id -> Queue.add (start ~run_id) fake.queue) [ "a"; "b"; "c" ];
  let worker = worker fake in
  expect_submitted "a" (Worker.poll worker);
  expect_submitted "b" (Worker.poll worker);
  expect_not_ready (Worker.poll worker);
  if Queue.length fake.queue <> 1 then
    failwith "a full window must leave the activation queued";
  release fake "a";
  expect_completed "a" (Worker.poll worker);
  expect_submitted "c" (Worker.poll worker);
  if !(fake.max_held) > 2 then failwith "the window was exceeded";
  release fake "b";
  release fake "c";
  expect_completed "b" (Worker.poll worker);
  expect_completed "c" (Worker.poll worker);
  expect_single_submissions fake

(** Core may publish a run's next activation before the adapter collected
    the previous outcome. The eviction is not executed until that outcome is
    settled, which here happens while the adapter waits. *)
let test_next_activation_waits_for_previous_outcome () =
  let fake = fake () in
  Queue.add (start ~run_id:"a") fake.queue;
  let worker = worker fake in
  expect_submitted "a" (Worker.poll worker);
  Queue.add (evict ~run_id:"a") fake.queue;
  fake.on_wait := (fun () -> if !(fake.held) <> [] then release fake "a");
  expect_submitted "a" (Worker.poll worker);
  expect_events fake
    [
      "delivered a"; "executed a"; "admitted a";
      "delivered a"; "wait"; "released a"; "collected a";
      "executed a"; "admitted a";
    ];
  (* The first completion's result is reported, then the eviction's. *)
  expect_completed ~terminal:true "a" (Worker.poll worker);
  release fake "a";
  expect_completed ~terminal:false "a" (Worker.poll worker);
  match !(fake.submissions) with
  | [ ("a", second); ("a", first) ] when second <> first -> ()
  | _ -> failwith "each activation of the run must get its own ticket"

(** An outcome for a ticket the adapter does not hold is rejected without
    touching the admitted completion it might be mistaken for. *)
let test_stale_outcome_cannot_settle_newer_activation () =
  let fake = fake () in
  Queue.add (start ~run_id:"a") fake.queue;
  let worker = worker fake in
  expect_submitted "a" (Worker.poll worker);
  publish_stale fake ~ticket:999 ~run_id:"a";
  expect_error "unknown_completion_ticket" (Worker.poll worker);
  if Worker.admitted_completions worker <> 1 then
    failwith "a stale outcome settled the admitted completion";
  release fake "a";
  expect_completed "a" (Worker.poll worker)

(** A completion the source rejects before admission is never resubmitted:
    poll and drain return the recorded error without another source call. *)
let test_rejection_before_admission_fails_closed () =
  let fake = fake () in
  Hashtbl.replace fake.reject_before_admission "a" ();
  Queue.add (start ~run_id:"a") fake.queue;
  let worker = worker fake in
  expect_error "completion_failed" (Worker.poll worker);
  expect_error "completion_failed" (Worker.poll worker);
  expect_error "completion_failed" (Worker.drain worker);
  let rejections =
    List.filter (String.equal "rejected before admission a") (events fake)
  in
  if List.length rejections <> 1 then
    failwith "a rejected completion was submitted again";
  if Worker.admitted_completions worker <> 0 then
    failwith "a rejected completion was counted as admitted"

(** An outcome that is not an acknowledgement after admission (unknown or
    rejected by Core) stops the worker and is never resubmitted. *)
let test_failed_outcome_after_admission_fails_closed () =
  List.iter
    (fun (result, code) ->
      let fake = fake () in
      Hashtbl.replace fake.results "a" result;
      Queue.add (start ~run_id:"a") fake.queue;
      let worker = worker fake in
      expect_submitted "a" (Worker.poll worker);
      release fake "a";
      expect_error code (Worker.poll worker);
      expect_error code (Worker.poll worker);
      expect_error code (Worker.drain worker);
      if List.length !(fake.submissions) <> 1 then
        failwith "a completion with a failed outcome was resubmitted")
    [
      (Protocol.Completion_aborted, "completion_outcome_unknown");
      (Protocol.Completion_panicked, "completion_outcome_unknown");
      (Protocol.Completion_rejected, "completion_failed");
    ]

(** Drain reports a completion still awaiting Core as retryable and settles
    it once the outcome arrives, without resubmitting it. *)
let test_drain_waits_for_admitted_outcome () =
  let fake = fake () in
  Queue.add (start ~run_id:"a") fake.queue;
  let worker = worker fake in
  expect_submitted "a" (Worker.poll worker);
  expect_error Adapter.completion_in_flight_code (Worker.drain worker);
  release fake "a";
  (match Worker.drain worker with
   | Ok () -> ()
   | Error error -> failwith ("drain did not settle the outcome: " ^ error.code));
  if Worker.admitted_completions worker <> 0 then
    failwith "drain left an admitted completion";
  expect_single_submissions fake

(** Forced shutdown: the outcome never arrives, so drain keeps reporting the
    completion in flight; discard then releases the adapter's copy without
    any source call. *)
let test_forced_discard_releases_admitted_completion () =
  let fake = fake () in
  Queue.add (start ~run_id:"a") fake.queue;
  let worker = worker fake in
  expect_submitted "a" (Worker.poll worker);
  expect_error Adapter.completion_in_flight_code (Worker.drain worker);
  let before = List.length (events fake) in
  Worker.discard worker;
  if List.length (events fake) <> before then
    failwith "discard called the source";
  if Worker.admitted_completions worker <> 0 then
    failwith "discard kept an admitted completion";
  match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("discarded worker still drains: " ^ error.code)

(** A synchronous source keeps its behavior through [Make]: each completion
    is acknowledged before [poll] returns and never reported as submitted. *)
let test_synchronous_source_is_unchanged () =
  let fake = fake () in
  let module Sync = struct
    include Fake

    let complete_workflow fake ~(completion : Protocol.completion) _ =
      Hashtbl.remove fake.leased completion.run_id;
      record fake ("acknowledged " ^ completion.run_id);
      Ok ()
  end in
  let module Sync_worker = Adapter.Make (Sync) in
  Queue.add (start ~run_id:"a") fake.queue;
  let workflow =
    Temporal_base.Definition.make ~name:"completion_admission"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:(Some (fun () -> Ok ()))
  in
  let worker =
    Result.get_ok
      (Sync_worker.create ~supervisor:fake
         ~workflows:[ Adapter.register workflow ] ())
  in
  match Sync_worker.poll worker with
  | Ok (Adapter.Completed { run_id = "a"; _ }) ->
      if Sync_worker.admitted_completions worker <> 0 then
        failwith "a synchronous completion was counted as admitted"
  | other -> failwith ("synchronous source: " ^ describe other)

(** Runs every admission regression. *)
let () =
  test_admitted_completion_does_not_block_another_run ();
  test_full_window_pauses_and_resumes_delivery ();
  test_next_activation_waits_for_previous_outcome ();
  test_stale_outcome_cannot_settle_newer_activation ();
  test_rejection_before_admission_fails_closed ();
  test_failed_outcome_after_admission_fails_closed ();
  test_drain_waits_for_admitted_outcome ();
  test_forced_discard_releases_admitted_completion ();
  test_synchronous_source_is_unchanged ();
  print_endline "native worker completion admission: ok"
