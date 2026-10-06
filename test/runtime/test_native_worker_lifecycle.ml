(** Regression tests for retryable workflow-completion ownership.

    A workflow may finish executing before Core acknowledges its completion.
    The adapter must retain the encoded completion, avoid rerunning user code,
    and leave the run lease outstanding until a later drain succeeds. This
    focused fake rejects two consecutive completion attempts so the test covers
    both the poll retry path and a failed shutdown-style drain before the final
    successful drain. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Adapter = Temporal_runtime.Native_worker_execution

(** A deterministic source error used by the fake supervisor. *)
type source_error = { code : string; message : string }

(** Fake supervisor state. The adapter's mutex is the only permitted writer to
    the lease and completion ledgers; the test inspects them after each call. *)
type fake_supervisor = {
  (* Activations waiting to be leased by the adapter. *)
  queue : Protocol.activation Queue.t;
  (* Run IDs whose native lease has not yet been acknowledged. *)
  leased : (string, unit) Hashtbl.t;
  (* Accepted completions, retained for the final exactly-once assertion. *)
  completions : Protocol.completion list ref;
  (* Number of deliberately rejected completion attempts remaining. *)
  completion_rejections : int ref;
}

(** Decodes the canonical bytes a fake supervisor received, exactly as Rust
    would, so fixtures assert on what was actually submitted. The adapter
    only submits values built by the canonical encoder, so a decode failure
    is a defect in the code under test. *)
let submitted_completion encoded =
  match
    Protocol.decode_completion
      (Temporal_protocol.Encoded_workflow_completion.to_string encoded)
  with
  | Ok completion -> completion
  | Error error ->
      failwith
        ("submitted completion did not decode: "
        ^ (Protocol.error_view error).message)

(** Allocates an empty semantic queue and lease ledger. *)
let fake_supervisor () =
  {
    queue = Queue.create ();
    leased = Hashtbl.create 4;
    completions = ref [];
    completion_rejections = ref 0;
  }

(** Implements the typed supervisor contract while deliberately retaining a
    lease whenever completion transport is unavailable. *)
module Fake_supervisor = struct
  type t = fake_supervisor
  type error = source_error

  (** Leases one queued activation by run ID. *)
  let try_poll_workflow supervisor =
    if Queue.is_empty supervisor.queue then Ok None
    else
      let activation = Queue.take supervisor.queue in
      Hashtbl.replace supervisor.leased activation.run_id ();
      Ok (Some activation)

  (** Rejects the configured number of attempts before acknowledging the exact
      run ID. A rejection leaves the lease in place for adapter retry. *)
  let complete_workflow supervisor encoded =
    let completion = submitted_completion encoded in
    if !(supervisor.completion_rejections) > 0 then begin
      decr supervisor.completion_rejections;
      Error
        {
          code = "temporarily_unavailable";
          message = "completion transport unavailable";
        }
    end
    else if Hashtbl.mem supervisor.leased completion.run_id then begin
      Hashtbl.remove supervisor.leased completion.run_id;
      supervisor.completions := completion :: !(supervisor.completions);
      Ok ()
    end
    else Error { code = "stale_lease"; message = "run is not leased" }

  (** Exposes the source classification expected by the adapter signature. *)
  let error_code error = error.code

  (** Exposes the source diagnostic expected by the adapter signature. *)
  let error_message error = error.message

  (** The injected transport rejection leaves the fake lease in place, so the
      fixture explicitly classifies it as retryable; a stale lease is not. *)
  let error_is_retryable error = String.equal error.code "temporarily_unavailable"

  (** A raised completion is equally fail-closed. *)
  let exception_is_retryable _ = false
end

module Worker = Adapter.Make (Fake_supervisor)

(** The canonical timestamp used by the activation fixture. *)
let timestamp : Protocol.timestamp = { seconds = 1L; nanoseconds = 0 }

(** Builds the initialization job for a unit workflow. *)
let initialize ~run_id : Protocol.activation_job =
  Protocol.Initialize_workflow
    {
      workflow_id = "workflow-" ^ run_id;
      workflow_type = "native_worker_lifecycle";
      arguments = [];
      randomness_seed = "1";
      attempt = 1;
      context = None;
    }

(** Wraps initialization in a valid activation envelope. *)
let activation ~run_id : Protocol.activation =
  {
    run_id;
    timestamp = Some timestamp;
    is_replaying = true;
    history_length = 1L;
    jobs = [ initialize ~run_id ];
    metadata = None;
  }

(** Adds one activation to the fake source queue. *)
let enqueue supervisor activation = Queue.add activation supervisor.queue

(** Builds and registers the unit workflow used by the retry assertion. *)
let worker supervisor calls =
  let workflow =
    Temporal_base.Definition.make ~name:"native_worker_lifecycle"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:
        (Some (fun () ->
          incr calls;
          Ok ()))
  in
  match Worker.create ~supervisor ~workflows:[ Adapter.register workflow ] () with
  | Ok worker -> worker
  | Error error ->
      failwith
        (Printf.sprintf "worker creation failed: %s at %s (%s)" error.message
           error.path error.code)

(** Requires a typed completion error from the native adapter. *)
let expect_completion_error (type value)
    (result : (value, Adapter.error_view) result) =
  match result with
  | Error { code = "completion_failed"; _ } -> ()
  | Error error ->
      failwith ("unexpected worker error: " ^ error.code)
  | Ok _ -> failwith "completion transport rejection was not returned"

(** Proves that repeated drain failures retain one owned completion and never
    execute the workflow again. The second drain is the first acknowledged
    completion, so the fake lease is then retired exactly once. *)
let test_drain_retry_preserves_completion () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  enqueue supervisor (activation ~run_id:"run-lifecycle");
  (* One rejection belongs to [poll]; the second belongs to the first drain. *)
  supervisor.completion_rejections := 2;
  let worker = worker supervisor calls in
  expect_completion_error (Worker.poll worker);
  if !calls <> 1 then failwith "rejected completion reran the workflow";
  if Hashtbl.length supervisor.leased <> 1 then
    failwith "poll rejection retired the workflow lease prematurely";
  expect_completion_error (Worker.drain worker);
  if !calls <> 1 then failwith "failed drain reran the workflow";
  if Hashtbl.length supervisor.leased <> 1 then
    failwith "failed drain discarded the retained workflow lease";
  begin match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("successful retry did not drain: " ^ error.message)
  end;
  if !calls <> 1 then failwith "successful drain reran the workflow";
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "successful drain left a workflow lease outstanding";
  if List.length !(supervisor.completions) <> 1 then
    failwith "workflow completion was submitted more than once"

(** Proves terminal disposal drops the copied completion and execution state
    without making another supervisor completion attempt. The fake native lease
    remains outstanding here because only the real Rust runtime can force-retire
    it; [discard] is intentionally tested as the OCaml-side half of that
    ordered terminal path. *)
let test_discard_releases_pending_state () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  enqueue supervisor (activation ~run_id:"run-discard");
  supervisor.completion_rejections := 1;
  let worker = worker supervisor calls in
  expect_completion_error (Worker.poll worker);
  if Hashtbl.length supervisor.leased <> 1 then
    failwith "discard fixture did not retain a native lease";
  Worker.discard worker;
  begin match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("discard left a pending completion: " ^ error.message)
  end;
  if !calls <> 1 then failwith "discard reran the workflow";
  if List.length !(supervisor.completions) <> 0 then
    failwith "discard attempted a completion"

(** Runs the focused workflow lifecycle regression. *)
let () =
  test_drain_retry_preserves_completion ();
  test_discard_releases_pending_state ()
