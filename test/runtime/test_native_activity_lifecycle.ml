(** Regression tests for retryable activity-completion ownership.

    Activity implementations may have irreversible side effects before the
    native completion call returns. The adapter therefore retains an owned
    completion keyed by the opaque task token and retries that completion, not
    the implementation. This test rejects two attempts to cover a failed poll
    retry and a failed drain before the eventual acknowledgement. *)

module Protocol = Temporal_protocol.Activity_protocol
module Adapter = Temporal_runtime.Native_activity_execution

(** A deterministic source error used by the fake supervisor. *)
type source_error = { code : string; message : string; retryable : bool }

(** Fake supervisor state. Tokens are kept as bytes so the test exercises the
    same binary-safe lease key used by the native worker. *)
type fake_supervisor = {
  (* Tasks waiting to be leased by the adapter. *)
  queue : Protocol.task Queue.t;
  (* Binary tokens whose completions are still unacknowledged. *)
  leased : bytes list ref;
  (* Completions accepted by the fake source. *)
  completions : Protocol.completion list ref;
  (* Number of deliberate completion rejections remaining. *)
  completion_rejections : int ref;
}

(** Allocates an empty task queue and lease ledger. *)
let fake_supervisor () =
  {
    queue = Queue.create ();
    leased = ref [];
    completions = ref [];
    completion_rejections = ref 0;
  }

(** Removes one exact binary token from a lease list. *)
let remove_token token tokens =
  (* Remove one exact binary token while retaining the order of other leases. *)
  let rec loop reversed = function
    | [] -> (false, List.rev reversed)
    | current :: rest when Bytes.equal current token ->
        (true, List.rev_append reversed rest)
    | current :: rest -> loop (current :: reversed) rest
  in
  loop [] tokens

(** Implements the typed supervisor contract while deliberately retaining the
    lease whenever completion transport is unavailable. *)
module Fake_supervisor = struct
  type t = fake_supervisor
  type error = source_error

  (** Leases one queued task with an owned copy of its opaque token. *)
  let try_poll_activity supervisor =
    if Queue.is_empty supervisor.queue then Ok None
    else
      let task = Queue.take supervisor.queue in
      supervisor.leased := Bytes.copy task.task_token :: !(supervisor.leased);
      Ok (Some task)

  (** Rejects the configured number of attempts before acknowledging the exact
      binary token. A rejection leaves the lease in place for adapter retry. *)
  let complete_activity supervisor (completion : Protocol.completion) =
    if !(supervisor.completion_rejections) > 0 then begin
      decr supervisor.completion_rejections;
      Error
        {
          code = "temporarily_unavailable";
          message = "completion transport unavailable";
          retryable = true;
        }
    end
    else
      let found, remaining =
        remove_token completion.task_token !(supervisor.leased)
      in
      if not found then
        Error
          {
            code = "stale_lease";
            message = "activity token is not leased";
            retryable = false;
          }
      else begin
        supervisor.leased := remaining;
        supervisor.completions := completion :: !(supervisor.completions);
        Ok ()
      end

  (** Heartbeats are not part of this completion-ownership regression; the
      fake accepts them so the adapter's expanded supervisor contract remains
      explicit without hiding a native operation. *)
  let record_activity_heartbeat _supervisor (_heartbeat : Protocol.heartbeat) =
    Ok ()

  (** This lifecycle fake does not model deferred client completion; explicit
      rejection prevents an async test from accidentally bypassing its lease
      assertions. *)
  let complete_async_activity _supervisor (_completion : Protocol.completion) =
    Error
      {
        code = "unsupported";
        message = "async client operation is not configured in this fake";
        retryable = false;
      }

  let record_async_activity_heartbeat _supervisor
      (_heartbeat : Protocol.heartbeat) =
    Error
      {
        code = "unsupported";
        message = "async client operation is not configured in this fake";
        retryable = false;
      }

  (** Exposes the source classification expected by the adapter signature. *)
  let error_code error = error.code

  (** Exposes the source diagnostic expected by the adapter signature. *)
  let error_message error = error.message

  (** The lifecycle fake rejects only with the explicitly transient transport
      category; stale-token errors remain fatal protocol failures. *)
  let error_is_retryable error = error.retryable

  (** This lifecycle fake does not submit async heartbeats. *)
  let async_heartbeat_error_is_retryable error = error.retryable

  (** This fake does not inject completion exceptions. *)
  let exception_is_retryable _exception = false
end

module Worker = Adapter.Make (Fake_supervisor)

(** Creates the complete start-task envelope for a unit activity. *)
let start_task token : Protocol.task =
  let start : Protocol.activity_start =
    {
      is_local = false;
      workflow_namespace = "default";
      workflow_type = "lifecycle_workflow";
      workflow_execution = { Protocol.workflow_id = "workflow-1"; run_id = "run-1" };
      activity_id = "activity-1";
      activity_type = "native_activity_lifecycle";
      header_fields = [];
      input = [];
      heartbeat_details = [];
      scheduled_time = None;
      current_attempt_scheduled_time = None;
      started_time = None;
      attempt = 1L;
      schedule_to_close_timeout = None;
      start_to_close_timeout = None;
      heartbeat_timeout = None;
      retry_policy = None;
      priority = None;
      standalone_run_id = "";
    }
  in
  { Protocol.task_token = Bytes.copy token; variant = Start start }

(** Adds one task to the fake source queue. *)
let enqueue supervisor task = Queue.add task supervisor.queue

(** Builds and registers the unit activity used by the retry assertion. *)
let worker supervisor calls =
  let activity =
    Temporal_base.Definition.make ~name:"native_activity_lifecycle"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:
        (Some (fun _context () ->
          incr calls;
          Ok ()))
  in
  match Worker.create ~supervisor ~activities:[ Adapter.register activity ] with
  | Ok worker -> worker
  | Error error ->
      failwith
        (Printf.sprintf "worker creation failed: %s at %s (%s)" error.message
           error.path error.code)

(** Requires a typed completion error from the native activity adapter. *)
let expect_completion_error (type value)
    (result : (value, Adapter.error_view) result) =
  match result with
  | Error { code = "completion_failed"; _ } -> ()
  | Error error -> failwith ("unexpected activity error: " ^ error.code)
  | Ok _ -> failwith "completion transport rejection was not returned"

(** Proves that repeated drain failures retain one owned binary-token lease and
    never execute the activity again. *)
let test_drain_retry_preserves_completion () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let token = Bytes.of_string "\000lifecycle\255-token" in
  enqueue supervisor (start_task token);
  (* One rejection belongs to [poll]; the second belongs to the first drain. *)
  supervisor.completion_rejections := 2;
  let worker = worker supervisor calls in
  expect_completion_error (Worker.poll worker);
  if !calls <> 1 then failwith "rejected completion reran the activity";
  if List.length !(supervisor.leased) <> 1 then
    failwith "poll rejection retired the activity lease prematurely";
  expect_completion_error (Worker.drain worker);
  if !calls <> 1 then failwith "failed drain reran the activity";
  if List.length !(supervisor.leased) <> 1 then
    failwith "failed drain discarded the retained activity lease";
  begin match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("successful retry did not drain: " ^ error.message)
  end;
  if !calls <> 1 then failwith "successful drain reran the activity";
  if !(supervisor.leased) <> [] then
    failwith "successful drain left an activity lease outstanding";
  if List.length !(supervisor.completions) <> 1 then
    failwith "activity completion was submitted more than once"

(** Proves terminal disposal drops the copied activity token without attempting
    another completion. The fake native lease is deliberately left outstanding
    because force-retirement belongs to Rust; this test covers the adapter's
    ordered half of the terminal cleanup contract. *)
let test_discard_releases_pending_state () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let token = Bytes.of_string "discard-token" in
  enqueue supervisor (start_task token);
  supervisor.completion_rejections := 1;
  let worker = worker supervisor calls in
  expect_completion_error (Worker.poll worker);
  if List.length !(supervisor.leased) <> 1 then
    failwith "discard fixture did not retain an activity lease";
  Worker.discard worker;
  begin match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("discard left a pending lease: " ^ error.message)
  end;
  if !calls <> 1 then failwith "discard reran the activity";
  if !(supervisor.completions) <> [] then
    failwith "discard attempted an activity completion"

(** Runs the focused activity lifecycle regression. *)
let () =
  test_drain_retry_preserves_completion ();
  test_discard_releases_pending_state ()
