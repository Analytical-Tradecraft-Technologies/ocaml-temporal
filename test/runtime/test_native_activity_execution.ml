(** Unit tests for the private native activity execution adapter.

    The fake supervisor models only the typed semantic boundary: it leases one
    decoded task, accepts a completion for the exact opaque token, and retains
    the lease when completion transport is deliberately rejected. These tests
    therefore exercise codec ownership, typed dispatch, cancellation, failure
    completion, and retry without requiring Rust, C, a network, or Temporal
    Server. *)

module Protocol = Temporal_protocol.Activity_protocol
module Raw_adapter = Temporal_runtime.Native_activity_execution

(** Copies a public payload into the base payload representation expected by the
    private activity adapter. This test-only conversion keeps the installed
    public package's opaque payload type separate from runtime fixtures. *)
let base_payload (payload : Temporal.Payload.t) : Temporal_base.Payload.t =
  {
    Temporal_base.Payload.metadata = List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Converts a public structured error for a private adapter implementation. *)
let base_error (error : Temporal.Error.t) : Temporal_base.Error.t =
  let view = Temporal.Error.view error in
  Temporal_base.Error.make ~non_retryable:view.non_retryable
    ~details:(List.map base_payload view.details) ~category:view.category
    ~message:view.message ()

(** Installs public codec callbacks in the base codec representation without
    changing value-dependent encoding metadata. *)
let base_codec (codec : 'a Temporal.Codec.t) : 'a Temporal_base.Codec.t =
  Temporal_base.Codec.of_payload
    ~encode:(fun value ->
      match Temporal.Codec.encode codec value with
      | Ok payload -> Ok (base_payload payload)
      | Error error -> Error (base_error error))
    ~decode:(fun payload ->
      let public_payload : Temporal.Payload.t =
        {
          Temporal.Payload.metadata =
            List.map (fun (key, value) -> (key, value)) payload.metadata;
          data = Bytes.copy payload.data;
        }
      in
      match Temporal.Codec.decode codec public_payload with
      | Ok value -> Ok value
      | Error error -> Error (base_error error))

(** Rebuilds a public activity as the private base definition consumed by the
    native adapter. This is deliberately confined to this low-level test. *)
let base_activity (definition : ('input, 'output) Temporal.Activity.t) =
  let implementation =
    match Temporal.Activity.implementation_with_context definition with
    | Some implementation ->
        Some (fun context input ->
            Result.map_error base_error (implementation context input))
    | None ->
        Option.map
          (fun implementation _context input ->
            Result.map_error base_error (implementation input))
          (Temporal.Activity.implementation definition)
  in
  Temporal_base.Definition.make ~name:(Temporal.Activity.name definition)
    ~input:(base_codec (Temporal.Activity.input definition))
    ~output:(base_codec (Temporal.Activity.output definition)) ~implementation

(** Keeps the test-facing registration call ergonomic while making the
    public-to-base conversion explicit at the private runtime boundary. *)
module Adapter = struct
  include Raw_adapter

  let register definition = Raw_adapter.register (base_activity definition)
end

type source_error = { code : string; message : string; retryable : bool }
(** A deterministic source error used by the fake supervisor. *)

(** Marker raised only by the fake completion transport to model a transient
    exception at the native call boundary. *)
exception Transient_completion_failure

type fake_supervisor = {
  (* Tasks waiting to be leased in producer order. *)
  queue : Protocol.task Queue.t;
  (* Binary task tokens currently leased and requiring acknowledged completion. *)
  leased : bytes list ref;
  (* Completions accepted by the fake source, newest first for assertions. *)
  completions : Protocol.completion list ref;
  (* Validated submissions, including attempts whose acknowledgement fails. *)
  completion_attempts : Protocol.completion list ref;
  (* Heartbeats accepted while their corresponding token remains leased. *)
  heartbeats : Protocol.heartbeat list ref;
  (* One-shot transport rejection used to verify completion retry without a
     second activity invocation. *)
  reject_next_completion : bool ref;
  (* One-shot transient exception used to prove raised completion failures
     retain the same lease and are classified through the source boundary. *)
  raise_next_completion : bool ref;
  (* Optional source poll failure, modelling a lower-layer typed rejection. *)
  poll_error : source_error option ref;
}
(** Mutable fake-supervisor state. The adapter itself serializes access to all
    fields through its poll mutex; assertions inspect them only after a poll
    returns. *)

(** Allocates an empty semantic task queue and lease ledger. *)
let fake_supervisor () =
  {
    queue = Queue.create ();
    leased = ref [];
    completions = ref [];
    completion_attempts = ref [];
    heartbeats = ref [];
    reject_next_completion = ref false;
    raise_next_completion = ref false;
    poll_error = ref None;
  }

(** Copies a validated completion by traversing the same strict JSON semantic
    codec used at the Rust boundary. The fake therefore never retains a mutable
    token or payload buffer owned by the adapter. *)
let copy_completion completion =
  match Protocol.encode_completion completion with
  | Error _ -> failwith "adapter submitted an invalid completion to fake source"
  | Ok json -> (
      match Protocol.decode_completion json with
      | Ok value -> value
      | Error _ -> failwith "fake source could not reparse its own completion")

(** Copies a heartbeat through the strict protocol codec so the fake source
    cannot accidentally retain mutable buffers owned by an activity context. *)
let copy_heartbeat heartbeat =
  match Protocol.encode_heartbeat heartbeat with
  | Error _ -> failwith "adapter submitted an invalid heartbeat to fake source"
  | Ok json -> (
      match Protocol.decode_heartbeat json with
      | Ok value -> value
      | Error _ -> failwith "fake source could not reparse its own heartbeat")

(** Removes one exact opaque token from a list and reports whether it was found.
    [Bytes.equal] is intentional: tokens are binary correlation data, not text.
*)
let remove_token token tokens =
  (* Rebuild the list without the first exact match, preserving all later token
     order so the fake ledger models a set without changing diagnostics. *)
  let rec loop reversed = function
    | [] -> (false, List.rev reversed)
    | current :: rest when Bytes.equal current token ->
        (true, List.rev_append reversed rest)
    | current :: rest -> loop (current :: reversed) rest
  in
  loop [] tokens

(** Implements the typed supervisor contract over the deterministic queue. *)
module Fake_supervisor = struct
  type t = fake_supervisor
  type error = source_error

  (** Takes one queued task and records an owned copy of its token as leased. *)
  let try_poll_activity supervisor =
    match !(supervisor.poll_error) with
    | Some error -> Error error
    | None ->
        if Queue.is_empty supervisor.queue then Ok None
        else
          let task = Queue.take supervisor.queue in
          supervisor.leased :=
            Bytes.copy task.task_token :: !(supervisor.leased);
          Ok (Some task)

  (** Accepts a completion only for a currently leased exact token. An injected
      rejection leaves the native lease untouched so the adapter must retry the
      same completion without invoking the OCaml implementation again. *)
  let complete_activity supervisor (completion : Protocol.completion) =
    supervisor.completion_attempts :=
      copy_completion completion :: !(supervisor.completion_attempts);
    if !(supervisor.raise_next_completion) then begin
      supervisor.raise_next_completion := false;
      raise Transient_completion_failure
    end else if !(supervisor.reject_next_completion) then begin
      supervisor.reject_next_completion := false;
      Error
        {
          code = "temporarily_unavailable";
          message = "completion transport unavailable";
          retryable = true;
        }
    end
    else
      let found, remaining =
        remove_token completion.Protocol.task_token !(supervisor.leased)
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
        supervisor.completions :=
          copy_completion completion :: !(supervisor.completions);
        Ok ()
      end

  (** Accepts progress only while the activity token is leased. The focused
      dispatch tests do not retain heartbeat bodies; the lease check still
      verifies that the adapter never sends a heartbeat after completion. *)
  let record_activity_heartbeat supervisor (heartbeat : Protocol.heartbeat) =
    if
      List.exists
        (fun token -> Bytes.equal token heartbeat.Protocol.task_token)
        !(supervisor.leased)
    then begin
      supervisor.heartbeats :=
        copy_heartbeat heartbeat :: !(supervisor.heartbeats);
      Ok ()
    end
    else
      Error
        {
          code = "stale_lease";
          message = "activity token is not leased";
          retryable = false;
        }

  (** The legacy activity fake does not model a namespace-bound client; making
      this explicit keeps tests honest when the adapter contract grows. *)
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

  (** Exposes the bounded source classification expected by the adapter. *)
  let error_code error = error.code

  (** Exposes the bounded source message expected by the adapter. *)
  let error_message error = error.message

  (** Only the injected transport error is retryable; stale leases are
      protocol failures and must remain fatal. *)
  let error_is_retryable error = error.retryable

  (** The fake uses a private marker exception to model a transient owner-side
      completion raise without treating arbitrary implementation exceptions as
      retryable. *)
  let exception_is_retryable = function
    | Transient_completion_failure -> true
    | _ -> false
end

module Worker = Adapter.Make (Fake_supervisor)
(** The production functor is tested against the deterministic source, without
    coupling the tests to a concrete native handle or transport. *)

(** Converts an ordinary codec payload into the binary metadata representation
    consumed by [Activity_protocol]. *)
let protocol_payload (payload : Temporal.Payload.t) : Protocol.payload =
  {
    Protocol.metadata =
      List.map
        (fun (key, value) -> (key, Bytes.of_string value))
        payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Encodes one OCaml value with the requested codec and converts it into a task
    payload. Fixtures fail immediately if a public codec violates its own
    contract. *)
let encode_input codec value =
  match Temporal.Codec.encode codec value with
  | Ok payload -> protocol_payload payload
  | Error error -> failwith (Temporal.Error.message error)

(** Decodes an activity completion payload with a public codec after converting
    binary metadata back to the runtime string representation. *)
let decode_output codec (payload : Protocol.payload) =
  let runtime_payload : Temporal.Payload.t =
    {
      metadata =
        List.map
          (fun (key, value) -> (key, Bytes.to_string value))
          payload.metadata;
      data = Bytes.copy payload.data;
    }
  in
  match Temporal.Codec.decode codec runtime_payload with
  | Ok value -> value
  | Error error -> failwith (Temporal.Error.message error)

(** Constructs the complete start-task context required by the strict protocol.
    Optional timing/retry fields stay absent because dispatch tests focus on the
    activity type, token, input, and completion semantics. *)
let start_task_fields ~heartbeat_details ~heartbeat_timeout ~token ~activity_type
    ~input : Protocol.task =
  let start : Protocol.activity_start =
    {
      workflow_namespace = "default";
      workflow_type = "test_workflow";
      workflow_execution =
        { Protocol.workflow_id = "workflow-1"; run_id = "run-1" };
      activity_id = "activity-1";
      activity_type;
      header_fields = [];
      input;
      heartbeat_details;
      scheduled_time = None;
      current_attempt_scheduled_time = None;
      started_time = None;
      attempt = 1L;
      schedule_to_close_timeout = None;
      start_to_close_timeout = None;
      heartbeat_timeout;
      retry_policy = None;
      priority = None;
      standalone_run_id = "";
    }
  in
  { Protocol.task_token = Bytes.copy token; variant = Start start }

(** Builds the ordinary task shape used by tests that do not exercise
    heartbeat context state. *)
let start_task ~token ~activity_type ~input =
  start_task_fields ~heartbeat_details:[] ~heartbeat_timeout:None ~token
    ~activity_type ~input

(** Builds a task with server-supplied heartbeat state for contextual activity
    tests while keeping ordinary fixture call sites concise. *)
let start_task_with_heartbeat ~heartbeat_details ~heartbeat_timeout ~token
    ~activity_type ~input =
  start_task_fields ~heartbeat_details ~heartbeat_timeout ~token ~activity_type
    ~input

(** Constructs a cancellation task while retaining arbitrary binary token bytes,
    including values that are not valid UTF-8 text. *)
let cancel_task token : Protocol.task =
  {
    Protocol.task_token = Bytes.copy token;
    variant =
      Cancel
        {
          reason = Cancellation_requested;
          details =
            Some
              {
                is_not_found = false;
                is_cancelled = true;
                is_paused = false;
                is_timed_out = false;
                is_worker_shutdown = false;
                is_reset = false;
              };
        };
  }

(** Adds a task in producer order to the fake supervisor queue. *)
let enqueue supervisor task = Queue.add task supervisor.queue

(** Creates a worker and turns configuration failures into a test diagnostic. *)
let worker supervisor activities =
  match Worker.create ~supervisor ~activities with
  | Ok worker -> worker
  | Error (error : Adapter.error_view) ->
      failwith
        (Printf.sprintf "worker creation failed: %s at %s (%s)" error.message
           error.path error.code)

(** Returns the most recent completion or fails with a useful lease diagnostic.
*)
let latest_completion supervisor =
  match !(supervisor.completions) with
  | completion :: _ -> completion
  | [] -> failwith "expected the activity adapter to submit a completion"

(** Requires one successful outcome of the expected kind. *)
let expect_completed expected_kind = function
  | Ok (Adapter.Completed { kind; _ }) when kind = expected_kind -> ()
  | Ok (Adapter.Completed _) -> failwith "activity completion kind differed"
  | Ok Adapter.Not_ready ->
      failwith "activity poll unexpectedly reported Not_ready"
  | Ok (Adapter.Rejected { error; _ }) ->
      failwith ("activity task was rejected: " ^ error.message)
  | Error (error : Adapter.error_view) ->
      failwith
        (Printf.sprintf "activity poll failed: %s at %s (%s)" error.message
           error.path error.code)

(** A successful typed activity receives decoded input, returns encoded output,
    and retires the exact binary task token. *)
let test_successful_dispatch () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let activity =
    Temporal.Activity.define ~name:"native_activity_upper"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
        incr calls;
        Ok (String.uppercase_ascii input))
  in
  let token = Bytes.of_string "\000opaque\255-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"native_activity_upper"
       ~input:[ encode_input Temporal.Codec.string "hello" ]);
  let worker = worker supervisor [ Adapter.register activity ] in
  expect_completed Adapter.Succeeded (Worker.poll worker);
  if !calls <> 1 then
    failwith "activity implementation was not invoked exactly once";
  if !(supervisor.leased) <> [] then
    failwith "successful activity lease remained active";
  let completion = latest_completion supervisor in
  if not (Bytes.equal completion.Protocol.task_token token) then
    failwith "completion did not preserve the exact opaque task token";
  begin match completion.Protocol.result with
  | Protocol.Completed (Some payload) ->
      if decode_output Temporal.Codec.string payload <> "HELLO" then
        failwith "activity output was not decoded from the completion payload"
  | _ -> failwith "successful activity used a non-completed result variant"
  end;
  begin match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | _ -> failwith "empty activity queue did not report Not_ready"
  end

(** A typed [Error.t] from an activity becomes a structured application failure,
    preserves retryability and application detail payloads, and is acknowledged
    without raising an exception. The binary detail body verifies that failure
    diagnostics cross the native boundary without text conversion. *)
let test_typed_failure () =
  let supervisor = fake_supervisor () in
  let detail : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "binary/plain"); ("source", "test") ];
      data = Bytes.of_string "\000failure-detail\255";
    }
  in
  let activity =
    Temporal.Activity.define ~name:"native_activity_failure"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Error
          (Temporal.Error.make ~category:`Activity
             ~message:"deliberate activity failure" ~details:[ detail ] ()))
  in
  let token = Bytes.of_string "failure-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"native_activity_failure"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register activity ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ })
    when String.equal error.code "activity" ->
      ()
  | _ ->
      failwith "typed activity failure was not reported as a retired rejection"
  end;
  begin match (latest_completion supervisor).Protocol.result with
  | Protocol.Failed
      {
        info = Protocol.Application { non_retryable = false; details; _ };
        _;
      } ->
      begin match details with
      | [ detail ]
        when detail.Protocol.metadata
             = [ ("encoding", Bytes.of_string "binary/plain");
                 ("source", Bytes.of_string "test") ]
             && Bytes.equal detail.Protocol.data
                  (Bytes.of_string "\000failure-detail\255") ->
          ()
      | _ -> failwith "typed activity failure dropped or altered its details"
      end
  | _ ->
      failwith
        "typed activity failure did not preserve retryability or details"
  end

(** Malformed application details must fail only their own activity. Each case
    queues unrelated work behind the rejected task to detect a poisoned retry
    entry as well as checking the bounded failure and exact binary token. *)
let test_invalid_failure_details_allow_next_activity () =
  let cases =
    [ ("duplicate", [ ("encoding", "binary/plain"); ("encoding", "binary/plain") ]);
      ("oversized", [ (String.make 65_537 'k', "value") ]);
      ("empty", [ ("", "value") ]);
      ("nul", [ ("bad\000key", "value") ]);
      ("invalid-key", [ ("\255", "value") ]);
      ("invalid-value", [ ("encoding", "\255") ]) ]
  in
  List.iter
    (fun (label, metadata) ->
      let supervisor = fake_supervisor () in
      let detail : Temporal.Payload.t =
        { metadata; data = Bytes.of_string "private-invalid-detail" }
      in
      let bad =
        Temporal.Activity.define ~name:"bad-details" ~input:Temporal.Codec.unit
          ~output:Temporal.Codec.unit (fun () ->
            Error (Temporal.Error.make ~category:`Activity ~details:[ detail ]
              ~message:"application failure" ()))
      in
      let good_calls = ref 0 in
      let good =
        Temporal.Activity.define ~name:"after-bad-details" ~input:Temporal.Codec.unit
          ~output:Temporal.Codec.unit (fun () -> incr good_calls; Ok ())
      in
      let token = Bytes.of_string ("\000bad\255-" ^ label) in
      enqueue supervisor (start_task ~token ~activity_type:"bad-details" ~input:[]);
      enqueue supervisor (start_task ~token:(Bytes.of_string "next-token")
        ~activity_type:"after-bad-details" ~input:[]);
      let adapter = worker supervisor [ Adapter.register bad; Adapter.register good ] in
      (match Worker.poll adapter with
      | Ok (Adapter.Rejected { lease_retired = true;
          error = { code = "invalid_message"; retryable = false; _ }; _ }) -> ()
      | _ -> failwith (label ^ ": malformed details did not retire as a task failure"));
      let completion = latest_completion supervisor in
      if not (Bytes.equal completion.task_token token) then
        failwith (label ^ ": rejection changed the task token");
      (match completion.result with
      | Protocol.Failed { message; info = Protocol.Application
          { non_retryable = true; details = []; _ }; _ }
        when String.length message <= 1_024 -> ()
      | _ -> failwith (label ^ ": invalid details survived the bounded failure"));
      if !(supervisor.leased) <> [] then
        failwith (label ^ ": rejected task lease was not retired");
      expect_completed Adapter.Succeeded (Worker.poll adapter);
      if !good_calls <> 1 || List.length !(supervisor.completions) <> 2 then
        failwith (label ^ ": unrelated activity did not complete exactly once");
      match Worker.drain adapter with
      | Ok () when !(supervisor.leased) = [] -> ()
      | _ -> failwith (label ^ ": malformed completion poisoned the drain"))
    cases

(** Once a valid replacement failure has been submitted, a transport rejection
    must retain that exact completion, even if the original application payload
    changes. Retrying must neither redispatch the callback nor poll later work. *)
let test_invalid_failure_completion_retry () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let detail : Temporal.Payload.t =
    { metadata = [ ("duplicate", "one"); ("duplicate", "two") ];
      data = Bytes.of_string "private-detail" }
  in
  let activity =
    Temporal.Activity.define ~name:"bad-details-retry" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () ->
        incr calls;
        Error (Temporal.Error.make ~category:`Activity ~details:[ detail ]
          ~message:"application failure" ()))
  in
  let token = Bytes.of_string "\000retry-invalid\255-token" in
  enqueue supervisor (start_task ~token ~activity_type:"bad-details-retry" ~input:[]);
  enqueue supervisor (start_task ~token:(Bytes.of_string "later-task")
    ~activity_type:"bad-details-retry" ~input:[]);
  supervisor.reject_next_completion := true;
  let adapter = worker supervisor [ Adapter.register activity ] in
  (match Worker.poll adapter with
  | Error { code = "completion_failed"; retryable = true; _ } -> ()
  | _ -> failwith "replacement failure did not reach the transient transport boundary");
  if List.length !(supervisor.leased) <> 1 || !(supervisor.completions) <> [] then
    failwith "unacknowledged replacement failure retired its lease";
  Bytes.fill detail.data 0 (Bytes.length detail.data) 'x';
  (match Worker.poll adapter with
  | Ok (Adapter.Rejected { lease_retired = true; _ }) -> ()
  | _ -> failwith "replacement failure could not be retried");
  (match !(supervisor.completion_attempts) with
  | [ second; first ] when Protocol.encode_completion first = Protocol.encode_completion second
      && Bytes.equal second.task_token token -> ()
  | _ -> failwith "transport retry changed the replacement completion");
  if !calls <> 1 || !(supervisor.leased) <> [] || Queue.length supervisor.queue <> 1 then
    failwith "replacement retry reran the activity, retained its lease, or polled later work"

(** An unknown activity type is acknowledged with a typed non-retryable failure,
    preventing a leased task from being silently abandoned. *)
let test_unknown_activity_retires_lease () =
  let supervisor = fake_supervisor () in
  let token = Bytes.of_string "unknown-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"missing_activity" ~input:[]);
  let worker = worker supervisor [] in
  begin match Worker.poll worker with
  | Ok
      (Adapter.Rejected
         {
           activity_type = Some "missing_activity";
           lease_retired = true;
           error;
           _;
         })
    when String.equal error.code "unknown_activity_type" ->
      ()
  | _ -> failwith "unknown activity was not rejected with a retired lease"
  end;
  if !(supervisor.leased) <> [] then
    failwith "unknown activity lease remained active"

(** Cancellation is represented as a canceled Temporal failure and preserves the
    same opaque token even when no activity implementation is registered. *)
let test_cancellation () =
  let supervisor = fake_supervisor () in
  let token = Bytes.of_string "\000cancel\255" in
  enqueue supervisor (cancel_task token);
  let worker = worker supervisor [] in
  begin
    match Worker.poll worker with
    | Ok
        (Adapter.Completed
          {
            kind = Adapter.Cancelled;
            cancellation_details = Some details;
            _;
          }) ->
        if
          not
            (details.is_cancelled
            && not details.is_not_found
            && not details.is_paused
            && not details.is_timed_out
            && not details.is_worker_shutdown
            && not details.is_reset)
        then failwith "cancellation flags were not retained on the OCaml outcome"
    | Ok (Adapter.Completed _) ->
        failwith "cancellation outcome did not retain Core cancellation details"
    | Ok Adapter.Not_ready -> failwith "cancellation task was not polled"
    | Ok (Adapter.Rejected { error; _ }) ->
        failwith ("cancellation task was rejected: " ^ error.message)
    | Error error -> failwith ("cancellation poll failed: " ^ error.message)
  end;
  if !(supervisor.leased) <> [] then
    failwith "cancellation lease remained active";
  let completion = latest_completion supervisor in
  if not (Bytes.equal completion.Protocol.task_token token) then
    failwith "cancellation completion changed its opaque task token";
  begin match completion.Protocol.result with
  | Protocol.Cancelled { info = Protocol.Canceled { identity; _ }; _ }
    when String.equal identity "ocaml-temporal" ->
      ()
  | _ -> failwith "cancellation did not produce a canceled failure"
  end

(** A completion transport rejection leaves one exact pending completion. The
    next poll retries it and does not execute the implementation a second time.
*)
let test_completion_retry_does_not_redo_activity () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let activity =
    Temporal.Activity.define ~name:"native_activity_retry"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
        incr calls;
        Ok "once")
  in
  let token = Bytes.of_string "retry-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"native_activity_retry"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  supervisor.reject_next_completion := true;
  let worker = worker supervisor [ Adapter.register activity ] in
  begin match Worker.poll worker with
  | Error { code = "completion_failed"; _ } -> ()
  | _ -> failwith "completion transport rejection did not remain a typed error"
  end;
  if !calls <> 1 then failwith "failed completion unexpectedly reran activity";
  if !(supervisor.leased) = [] then
    failwith "fake source retired rejected lease";
  expect_completed Adapter.Succeeded (Worker.poll worker);
  if !calls <> 1 then failwith "pending completion retry reran activity";
  if List.length !(supervisor.completions) <> 1 then
    failwith "pending completion retry submitted more than one completion";
  if !(supervisor.leased) <> [] then
    failwith "retried completion left lease active"

(** A completion exception follows the same ownership rule as a typed
    rejection when the supervisor explicitly marks that exception transient.
    The second poll retries the copied completion and never re-enters the
    activity implementation. *)
let test_transient_completion_exception_does_not_redo_activity () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let activity =
    Temporal.Activity.define ~name:"native_activity_raise_retry"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
        incr calls;
        Ok "once")
  in
  let token = Bytes.of_string "raise-retry-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"native_activity_raise_retry"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  supervisor.raise_next_completion := true;
  let worker = worker supervisor [ Adapter.register activity ] in
  begin match Worker.poll worker with
  | Error { code = "completion_failed"; retryable = true; _ } -> ()
  | Error error ->
      failwith
        ("transient completion exception had the wrong classification: "
       ^ error.code)
  | Ok _ -> failwith "transient completion exception was silently accepted"
  end;
  if !calls <> 1 then failwith "raised completion reran the activity";
  if !(supervisor.leased) = [] then
    failwith "fake source retired raised completion lease";
  expect_completed Adapter.Succeeded (Worker.poll worker);
  if !calls <> 1 then failwith "raised completion retry reran activity";
  if List.length !(supervisor.completions) <> 1 then
    failwith "raised completion retry submitted more than one completion";
  if !(supervisor.leased) <> [] then
    failwith "raised completion retry left lease active"

(** A contextual activity receives the previous attempt's heartbeat details,
    reports a typed progress value through the supervisor, and cannot submit a
    second heartbeat after terminal completion invalidates its context. *)
let test_contextual_heartbeat_lifecycle () =
  let supervisor = fake_supervisor () in
  let retained_context = ref None in
  let activity =
    Temporal.Activity.define_with_context ~name:"native_activity_heartbeat"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
      (fun context () ->
        retained_context := Some context;
        let previous = Temporal.Activity.Context.details context in
        begin
          match previous with
          | [ payload ] when decode_output Temporal.Codec.string (protocol_payload payload) = "prior" ->
              ()
          | _ -> failwith "activity did not receive prior heartbeat details"
        end;
        begin
          match Temporal.Activity.Context.heartbeat_timeout context with
          | Some timeout when Temporal.Duration.to_ms timeout = 2_000L -> ()
          | _ -> failwith "activity heartbeat timeout was not converted exactly"
        end;
        match Temporal.Activity.Context.heartbeat context Temporal.Codec.string
                "progress" with
        | Ok () -> Ok "done"
        | Error error -> Error error)
  in
  (* The native adapter only receives the package-private base definition.
     This assertion protects the conversion that must retain a contextual
     callback as executable code instead of treating [define_with_context] as
     a remote-only reference. *)
  let converted = base_activity activity in
  begin
    match Temporal_base.Definition.implementation converted with
    | Some _ -> ()
    | None -> failwith "contextual activity conversion lost its implementation"
  end;
  let token = Bytes.of_string "heartbeat-token" in
  let prior = encode_input Temporal.Codec.string "prior" in
  enqueue supervisor
    (start_task_with_heartbeat ~heartbeat_details:[ prior ]
       ~heartbeat_timeout:(Some (Protocol.{ seconds = 2L; nanoseconds = 0 }))
       ~token ~activity_type:"native_activity_heartbeat"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register activity ] in
  expect_completed Adapter.Succeeded (Worker.poll worker);
  begin
    match !(supervisor.heartbeats) with
    | [ heartbeat ] ->
        if not (Bytes.equal heartbeat.Protocol.task_token token) then
          failwith "heartbeat changed the opaque task token";
        begin
          match heartbeat.Protocol.details with
          | [ payload ]
            when decode_output Temporal.Codec.string payload = "progress" ->
              ()
          | _ -> failwith "heartbeat detail payload was not preserved"
        end
    | _ -> failwith "contextual activity did not submit exactly one heartbeat"
  end;
  begin
    match !(retained_context) with
    | None -> failwith "contextual activity did not retain its test context"
    | Some context -> (
        match
          Temporal.Activity.Context.heartbeat context Temporal.Codec.string
            "after-completion"
        with
        | Error _ -> ()
        | Ok () -> failwith "heartbeat succeeded after activity completion")
  end

(** The public context view must never expose mutable payload storage owned by
    the adapter. This test mutates the source details, a getter result, the
    heartbeat argument, and the callback's retained view in turn; every later
    observation must still contain the original bytes and ordering. The
    timeout is checked through the public conversion as well, because a
    context's timing contract is part of the activity API rather than an
    implementation-only field. *)
let test_context_payloads_are_copied () =
  let source_data = Bytes.of_string "\000prior\255" in
  let source_payload : Temporal_base.Payload.t =
    {
      Temporal_base.Payload.metadata = [ ("encoding", "binary/plain") ];
      data = source_data;
    }
  in
  let callback_payloads = ref [] in
  let context =
    Temporal_base.Activity_context.create
      ~heartbeat:(fun payloads ->
        callback_payloads := payloads;
        Ok ())
      ~details:[ source_payload ]
      ~heartbeat_timeout:(Some (Temporal_base.Duration.of_ms 1_234L))
  in
  (* Creation copies the previous-attempt details before the source task can
     be reused or mutated by the caller. *)
  Bytes.set source_data 0 'X';
  let expected_source = Bytes.of_string "\000prior\255" in
  let expect_source (details : Temporal.Payload.t list) message =
    (* Compare both metadata and bytes so a shallow copy cannot pass this test
       merely by preserving the payload shape. *)
    match details with
    | [ payload ]
      when payload.metadata = [ ("encoding", "binary/plain") ]
           && Bytes.equal payload.data expected_source ->
        ()
    | _ -> failwith message
  in
  let first_details = Temporal.Activity.Context.details context in
  expect_source first_details
    "activity context changed copied previous heartbeat details";
  (* A getter returns another copy, so mutating it cannot corrupt the context's
     retained state. *)
  begin match first_details with
  | [ payload ] -> Bytes.set payload.data 0 'Y'
  | _ -> failwith "activity context returned an unexpected detail shape"
  end;
  expect_source (Temporal.Activity.Context.details context)
    "activity context leaked mutable detail bytes through its getter";
  begin match Temporal.Activity.Context.heartbeat_timeout context with
  | Some timeout when Temporal.Duration.to_ms timeout = 1_234L -> ()
  | _ -> failwith "activity context changed its heartbeat timeout"
  end;
  let heartbeat_data = Bytes.of_string "\000progress\255" in
  let heartbeat_payload : Temporal.Payload.t =
    {
      Temporal.Payload.metadata = [ ("encoding", "binary/plain") ];
      data = heartbeat_data;
    }
  in
  begin match
    Temporal.Activity.Context.heartbeat_payloads context [ heartbeat_payload ]
  with
  | Ok () -> ()
  | Error error ->
      failwith ("copied heartbeat payload was rejected: " ^ Temporal.Error.message error)
  end;
  let expected_heartbeat = Bytes.of_string "\000progress\255" in
  (* The callback receives an owned copy, not the public payload's bytes. *)
  Bytes.set heartbeat_data 0 'Z';
  begin match !callback_payloads with
  | [ payload ] when Bytes.equal payload.data expected_heartbeat -> ()
  | _ -> failwith "heartbeat callback retained caller-owned payload bytes"
  end;
  (* The context stores a separate copy after a successful callback, so even
     the callback's retained view cannot mutate the next-attempt details. *)
  begin match !callback_payloads with
  | [ payload ] -> Bytes.set payload.data 0 'Q'
  | _ -> failwith "heartbeat callback received an unexpected detail shape"
  end;
  begin match Temporal.Activity.Context.details context with
  | [ payload ] when Bytes.equal payload.data expected_heartbeat -> ()
  | _ -> failwith "activity context retained mutable callback payload bytes"
  end

(** Heartbeat callback failures and stale contexts are ordinary typed results.
    In particular, an invalidated context must reject before entering its
    callback, so a retained activity context cannot submit progress after the
    terminal completion has released its native lease. *)
let test_context_callback_exception_and_invalidation () =
  let callback_calls = ref 0 in
  let raising_context =
    Temporal_base.Activity_context.create
      ~heartbeat:(fun _payloads ->
        incr callback_calls;
        raise (Failure "heartbeat callback defect"))
      ~details:[] ~heartbeat_timeout:None
  in
  begin match
    Temporal.Activity.Context.heartbeat raising_context Temporal.Codec.string
      "progress"
  with
  | Error error ->
      let view = Temporal.Error.view error in
      if view.category <> `Defect || not view.non_retryable then
        failwith "heartbeat callback exception was not a non-retryable defect";
      if
        not
          (String.starts_with ~prefix:"activity heartbeat callback raised:"
             view.message)
      then failwith "heartbeat callback exception lost its typed diagnostic"
  | Ok () -> failwith "heartbeat callback exception escaped as success"
  end;
  if !callback_calls <> 1 then
    failwith "heartbeat callback exception did not invoke the callback once";
  let invalidated_calls = ref 0 in
  let invalidated_context =
    Temporal_base.Activity_context.create
      ~heartbeat:(fun _payloads ->
        incr invalidated_calls;
        Ok ())
      ~details:[] ~heartbeat_timeout:None
  in
  Temporal_base.Activity_context.invalidate invalidated_context;
  begin match
    Temporal.Activity.Context.heartbeat invalidated_context Temporal.Codec.string
      "late-progress"
  with
  | Error error ->
      let view = Temporal.Error.view error in
      if view.category <> `Bridge then
        failwith "invalidated activity context returned the wrong error category";
      if
        not
          (String.equal view.message "activity context is no longer active")
      then failwith "invalidated activity context returned the wrong message"
  | Ok () -> failwith "invalidated activity context accepted a heartbeat"
  end;
  if !invalidated_calls <> 0 then
    failwith "invalidated activity context entered its heartbeat callback"

(** More than one input payload is rejected explicitly rather than silently
    dropping later values. *)
let test_extra_input_is_rejected () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.define ~name:"native_activity_one_input"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
        Ok input)
  in
  enqueue supervisor
    (start_task
       ~token:(Bytes.of_string "extra-input")
       ~activity_type:"native_activity_one_input"
       ~input:
         [
           encode_input Temporal.Codec.string "first";
           encode_input Temporal.Codec.string "second";
         ]);
  let worker = worker supervisor [ Adapter.register activity ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ })
    when String.equal error.code "unsupported" ->
      ()
  | _ -> failwith "extra activity inputs were not rejected"
  end

(** Duplicate and remote registrations are rejected before any native task is
    polled, keeping the registry unambiguous. *)
let test_registration_validation () =
  let supervisor = fake_supervisor () in
  let definition () =
    Temporal.Activity.define ~name:"duplicate_activity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  begin match
    Worker.create ~supervisor
      ~activities:
        [ Adapter.register (definition ()); Adapter.register (definition ()) ]
  with
  | Error { code = "duplicate_activity"; _ } -> ()
  | _ -> failwith "duplicate activity registration was accepted"
  end;
  let remote =
    Temporal.Activity.remote ~name:"remote_activity" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit
  in
  begin match
    Worker.create ~supervisor ~activities:[ Adapter.register remote ]
  with
  | Error { code = "not_executable"; _ } -> ()
  | _ -> failwith "remote activity registration was accepted as executable"
  end

(** Exceptions from application activity code are converted into a typed,
    retired non-retryable failure instead of escaping the worker loop. *)
let test_implementation_exception_is_retired () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.define ~name:"native_activity_exception"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        raise (Failure "defect in activity"))
  in
  enqueue supervisor
    (start_task
       ~token:(Bytes.of_string "exception-token")
       ~activity_type:"native_activity_exception"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register activity ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ })
    when String.equal error.code "ocaml_exception" ->
      ()
  | _ -> failwith "activity exception did not retire its task lease"
  end

(** A source-side poll failure remains a typed adapter error because no task
    token was available to acknowledge. *)
let test_poll_error_is_typed () =
  let supervisor = fake_supervisor () in
  supervisor.poll_error :=
    Some
      {
        code = "poll_failed";
        message = "native activity poll failed";
        retryable = false;
      };
  let worker = worker supervisor [] in
  begin match Worker.poll worker with
  | Error (error : Adapter.error_view)
    when String.equal error.code "poll_failed"
         && String.equal error.path "$.poll" ->
      ()
  | Error _ -> failwith "poll error had the wrong typed diagnostic"
  | Ok _ -> failwith "poll error unexpectedly produced an activity outcome"
  end

(** Runs every adapter assertion with a stable test-process failure. *)
let () =
  test_successful_dispatch ();
  test_typed_failure ();
  test_invalid_failure_details_allow_next_activity ();
  test_invalid_failure_completion_retry ();
  test_unknown_activity_retires_lease ();
  test_cancellation ();
  test_completion_retry_does_not_redo_activity ();
  test_transient_completion_exception_does_not_redo_activity ();
  test_contextual_heartbeat_lifecycle ();
  test_context_payloads_are_copied ();
  test_context_callback_exception_and_invalidation ();
  test_extra_input_is_rejected ();
  test_registration_validation ();
  test_implementation_exception_is_retired ();
  test_poll_error_is_typed ()
