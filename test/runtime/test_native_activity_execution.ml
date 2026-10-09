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
    ?error_type:view.error_type
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

(** Converts asynchronous definitions so context rejection can be exercised
    through both dispatch paths. These tests never admit a deferred handle. *)
let base_async_activity (definition : ('input, 'output) Temporal.Activity.t) =
  let implementation =
    Option.map
      (fun implementation context input ->
        match implementation context input with
        | Temporal.Activity.Completed output ->
            Temporal_base.Async_activity.Completed output
        | Temporal.Activity.Failed error ->
            Temporal_base.Async_activity.Failed (base_error error)
        | Temporal.Activity.Will_complete_async handle ->
            Temporal_base.Async_activity.Will_complete_async handle)
      (Temporal.Activity.implementation_async definition)
  in
  Temporal_base.Definition.make ~name:(Temporal.Activity.name definition)
    ~input:(base_codec (Temporal.Activity.input definition))
    ~output:(base_codec (Temporal.Activity.output definition)) ~implementation

(** Keeps the test-facing registration call ergonomic while making the
    public-to-base conversion explicit at the private runtime boundary. *)
module Adapter = struct
  include Raw_adapter

  let register definition = Raw_adapter.register (base_activity definition)

  (** Registers the asynchronous callback through the same production adapter. *)
  let register_async definition =
    Raw_adapter.register_async (base_async_activity definition)
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
  (* One-shot non-retryable rejection. It models a failure after which Core
     may already have consumed the lease, so the adapter must never resubmit
     the retained completion (issue #843). *)
  reject_next_completion_permanently : bool ref;
  (* One-shot exception that is not classified retryable: an uncertain
     acknowledgement that must also fail closed. *)
  raise_next_completion_uncertain : bool ref;
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
    reject_next_completion_permanently = ref false;
    raise_next_completion_uncertain = ref false;
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
          (* Like the Rust ledger, a cancellation is an update to its start's
             single completion debt: it adds a lease only for a token that is
             not already leased (a bare cancellation in these fixtures). *)
          let already_leased =
            List.exists (Bytes.equal task.task_token) !(supervisor.leased)
          in
          (match task.variant with
          | Protocol.Cancel _ when already_leased -> ()
          | Protocol.Start _ | Protocol.Cancel _ ->
              supervisor.leased :=
                Bytes.copy task.task_token :: !(supervisor.leased));
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
    end else if !(supervisor.reject_next_completion_permanently) then begin
      supervisor.reject_next_completion_permanently := false;
      Error
        {
          code = "core_rejected";
          message = "completion lease may already be consumed";
          retryable = false;
        }
    end else if !(supervisor.raise_next_completion_uncertain) then begin
      supervisor.raise_next_completion_uncertain := false;
      failwith "injected uncertain completion exception"
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

  (** This fake does not submit async operations; preserve its explicit source
      classification to satisfy the operation-specific adapter contract. *)
  let async_operation_error_disposition error =
    if error.retryable then Temporal_runtime.Native_worker_policy.Retry_exact
    else Temporal_runtime.Native_worker_policy.Retired

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
      is_local = false;
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

(** Creates a worker whose shutdown probe reads [shutting_down], and turns
    configuration failures into a test diagnostic. *)
let worker ?(shutting_down = Atomic.make false) supervisor activities =
  match
    Worker.create ~supervisor ~activities
      ~worker_shutting_down:(fun () -> Atomic.get shutting_down)
  with
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
  | Protocol.Completed payload ->
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
  end;
  (* Without an explicit application type the category label remains the wire
     type, preserving the pre-existing behaviour for untyped errors. *)
  begin match (latest_completion supervisor).Protocol.result with
  | Protocol.Failed
      { info = Protocol.Application { type_name = "activity"; _ }; source; _ }
    when String.equal source "ocaml-temporal" ->
      ()
  | _ -> failwith "untyped activity failure changed its wire type or source"
  end

(** An explicit [~error_type] becomes [ApplicationFailureInfo.type] on the
    activity completion. This is the field Temporal Server and Core match
    against a retry policy's [non_retryable_error_types], so it must be the
    user's type rather than the category label. *)
let test_typed_failure_error_type () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.define ~name:"native_activity_typed_failure"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Error
          (Temporal.Error.make ~error_type:"InvalidInput" ~category:`Activity
             ~message:"rejected input" ()))
  in
  enqueue supervisor
    (start_task ~token:(Bytes.of_string "typed-failure-token")
       ~activity_type:"native_activity_typed_failure"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register activity ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; _ }) -> ()
  | _ -> failwith "typed activity failure was not retired"
  end;
  match (latest_completion supervisor).Protocol.result with
  | Protocol.Failed
      {
        info =
          Protocol.Application
            { type_name = "InvalidInput"; non_retryable = false; _ };
        _;
      } ->
      ()
  | _ -> failwith "activity failure did not carry its explicit error type"

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

(** Wire-valid context values which the runtime cannot represent must fail only
    their own task. Exercise both callback styles (a sub-millisecond heartbeat
    timeout is unrepresentable only for synchronous ones), immediate
    acknowledgement, and transport retry through poll and drain before running
    unrelated work. *)
let test_unrepresentable_context_retires_lease () =
  List.iter
    (fun async ->
      List.iter
        (fun binary_metadata ->
          List.iter
            (fun retry ->
              let supervisor = fake_supervisor () in
              let bad_calls = ref 0 in
              let good_calls = Atomic.make 0 in
              let bad =
                if async then
                  Adapter.register_async
                    (Temporal.Activity.define_async ~name:"bad-context"
                       ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
                       (fun _context () ->
                         incr bad_calls;
                         Temporal.Activity.Completed ()))
                else
                  Adapter.register
                    (Temporal.Activity.define ~name:"bad-context"
                       ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
                       (fun () -> incr bad_calls; Ok ()))
              in
              let good =
                Temporal.Activity.define ~name:"after-bad-context"
                  ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
                  (fun () ->
                    ignore (Atomic.fetch_and_add good_calls 1);
                    Ok ())
              in
              let heartbeat_details, heartbeat_timeout, path =
                if binary_metadata then
                  ([ Protocol.{ metadata = [ ("opaque", Bytes.of_string "\255") ];
                       data = Bytes.of_string "private-heartbeat" } ],
                   None, "$.variant.heartbeat_details[0].metadata.opaque")
                else
                  ([], Some Protocol.{ seconds = 1L; nanoseconds = 1 },
                   "$.variant.heartbeat_timeout.nanoseconds")
              in
              let token = Bytes.of_string "\000bad-context\255" in
              let task =
                start_task_with_heartbeat ~heartbeat_details ~heartbeat_timeout
                  ~token ~activity_type:"bad-context" ~input:[]
              in
              (* These are valid native tasks, not malformed wire envelopes. *)
              let task =
                match Result.bind (Protocol.encode_task task) Protocol.decode_task with
                | Ok task -> task
                | Error _ -> failwith "context fixture is not wire-valid"
              in
              enqueue supervisor task;
              enqueue supervisor
                (start_task ~token:(Bytes.of_string "next-context-token")
                   ~activity_type:"after-bad-context" ~input:[]);
              let adapter = worker supervisor [ bad; Adapter.register good ] in
              if retry then begin
                supervisor.reject_next_completion := true;
                (match Worker.poll adapter with
                | Error { code = "completion_failed"; retryable = true; _ } -> ()
                | _ -> failwith "context failure did not reach completion transport");
                supervisor.reject_next_completion := true;
                (match Worker.drain adapter with
                | Error { code = "completion_failed"; retryable = true; _ } -> ()
                | _ -> failwith "drain lost the rejected context completion");
                if !(supervisor.leased) <> [ token ]
                   || !(supervisor.completions) <> []
                   || Queue.length supervisor.queue <> 1 then
                  failwith "unacknowledged context failure lost its lease or polled ahead"
              end;
              (match Worker.poll adapter with
              | Ok (Adapter.Rejected { lease_retired = true;
                  error = { code = "unsupported"; path = actual; _ }; _ })
                when String.equal actual path -> ()
              | _ -> failwith "unsupported context escaped without retiring its task");
              let completion = latest_completion supervisor in
              (match completion.result with
              | Protocol.Failed { message; info = Protocol.Application
                  { non_retryable = true; details = []; _ }; _ }
                when String.length message <= 1_024 -> ()
              | _ -> failwith "context failure did not produce a bounded rejection");
              if not (Bytes.equal completion.task_token token)
                 || !(supervisor.leased) <> [] || !bad_calls <> 0 then
                failwith "context rejection changed the token or dispatched its callback";
              if List.length !(supervisor.completion_attempts) <> (if retry then 3 else 1)
                 || not (List.for_all
                   (fun attempt -> Protocol.encode_completion attempt
                     = Protocol.encode_completion completion)
                   !(supervisor.completion_attempts)) then
                failwith "context rejection changed across submission attempts";
              let module Loop = Temporal_runtime.Native_worker_loop in
              (* Use the production loop's progress contract to ensure later
                 work runs after rejection without a readiness wait. *)
              let poll_activity () =
                match Worker.poll adapter with
                | Ok (Adapter.Completed _) -> Ok Loop.Progress
                | Ok (Adapter.Rejected { lease_retired = true; _ }) -> Ok Loop.Progress
                | Error error -> Error error
                | _ -> failwith "queued activity did not make progress"
              in
              (match Loop.run ~closed:(fun () -> Atomic.get good_calls = 1)
                 ~poll_workflow:(fun () -> Ok Loop.Not_ready) ~poll_activity
                 ~wait_for_lane:(fun ~workflow_lane ~native_wait:_ ->
                   if not workflow_lane then failwith "unexpected activity wait";
                   (* The independent workflow lane may wait before the queued
                      activity has completed. Keep that fixture wait bounded. *)
                   let deadline = Unix.gettimeofday () +. 5. in
                   while Atomic.get good_calls = 0 do
                     if Unix.gettimeofday () >= deadline then
                       failwith "timed out waiting for queued activity";
                     Domain.cpu_relax ()
                   done;
                   Ok ())
                 ~retry_pending:(fun ~workflow_lane:_ -> failwith "unexpected retry wait")
               with
              | Ok () -> ()
              | Error _ -> failwith "context rejection stopped the worker loop");
              match Worker.drain adapter with
              | Ok () when !(supervisor.leased) = [] && Atomic.get good_calls = 1 -> ()
              | _ -> failwith "context rejection left unaccounted completion debt")
            [ false; true ])
        (* A sub-millisecond heartbeat timeout only fails synchronous
           definitions, whose context exposes the exact interval.
           Asynchronous callbacks accept it and report it rounded up through
           [Async_context.info]; test_native_async_activity covers that. *)
        (if async then [ true ] else [ true; false ]))
    [ false; true ]

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

(** A completion failure that is not explicitly retryable is fail-closed
    (issue #843): the retained completion is never submitted again, neither by
    a later poll nor by a shutdown drain, and it keeps blocking new tasks until
    terminal [discard]. Both a typed rejection and an unclassified exception
    must follow this rule. *)
let test_non_retryable_completion_is_never_resubmitted () =
  List.iter
    (fun (label, inject) ->
      let supervisor = fake_supervisor () in
      let calls = ref 0 in
      let name = "native_activity_fail_closed_" ^ label in
      let activity =
        Temporal.Activity.define ~name ~input:Temporal.Codec.unit
          ~output:Temporal.Codec.string (fun () ->
            incr calls;
            Ok "once")
      in
      let start token =
        start_task ~token:(Bytes.of_string token) ~activity_type:name
          ~input:[ encode_input Temporal.Codec.unit () ]
      in
      enqueue supervisor (start (label ^ "-token"));
      inject supervisor;
      let worker = worker supervisor [ Adapter.register activity ] in
      (* Matches the fail-closed error and returns it for later comparison. *)
      let expect_refusal context (result : (_, Adapter.error_view) result) =
        match result with
        | Error ({ code = "completion_failed"; retryable = false; _ } as error) ->
            error
        | Error error ->
            failwith
              (Printf.sprintf "%s %s returned the wrong error: %s" label context
                 error.code)
        | Ok _ ->
            failwith (Printf.sprintf "%s %s acknowledged a refused lease" label context)
      in
      let first = expect_refusal "poll" (Worker.poll worker) in
      (* A queued task must not be polled while the refused lease is retained. *)
      enqueue supervisor (start (label ^ "-next-token"));
      let again = expect_refusal "second poll" (Worker.poll worker) in
      let drained = expect_refusal "drain" (Worker.drain worker) in
      if again <> first || drained <> first then
        failwith (label ^ " refusal changed its recorded diagnostic");
      if List.length !(supervisor.completion_attempts) <> 1 then
        failwith (label ^ " retained completion was submitted more than once");
      if !calls <> 1 || Queue.length supervisor.queue <> 1 then
        failwith (label ^ " refused lease allowed further activity execution");
      Worker.discard worker;
      match Worker.drain worker with
      | Ok () -> ()
      | Error error ->
          failwith (label ^ " discard left a retained lease: " ^ error.code))
    [ ("rejected", fun supervisor ->
        supervisor.reject_next_completion_permanently := true);
      ("raised", fun supervisor ->
        supervisor.raise_next_completion_uncertain := true) ]

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
        (* #792: the native adapter must forward the task's identity so
           activities can build idempotency keys. *)
        begin
          match Temporal.Activity.Context.info context with
          | Error error -> failwith (Temporal.Error.message error)
          | Ok info ->
              let module Info = Temporal.Activity.Info in
              if Info.namespace info <> "default"
                 || Info.workflow info
                    <> { Info.workflow_id = "workflow-1"; run_id = "run-1";
                         workflow_type = "test_workflow" }
                 || Info.activity_id info <> "activity-1"
                 || Info.activity_type info <> "native_activity_heartbeat"
                 || Info.attempt info <> 1 || Info.is_local info
                 || Option.is_some (Info.scheduled_time info)
              then failwith "activity info did not mirror the start task"
        end;
        match Temporal.Activity.Context.heartbeat context Temporal.Codec.string
                "progress" with
        | Error error -> Error error
        | Ok () -> (
            (* Regression for #767: this attempt's own heartbeat is recorded
               for the next attempt and must not replace the previous
               attempt's details, which stay stable for the whole attempt. *)
            match Temporal.Activity.Context.details context with
            | [ payload ]
              when decode_output Temporal.Codec.string
                     (protocol_payload payload)
                   = "prior" ->
                Ok "done"
            | _ ->
                failwith
                  "heartbeat replaced the previous attempt's details"))
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
    observation must still contain the original bytes and ordering. A
    successful heartbeat must not replace the previous attempt's details. The
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
  (* Neither the successful heartbeat nor mutation of the callback's retained
     view may change the previous attempt's details (#767): they are fixed
     for the whole attempt. *)
  begin match !callback_payloads with
  | [ payload ] -> Bytes.set payload.data 0 'Q'
  | _ -> failwith "heartbeat callback received an unexpected detail shape"
  end;
  expect_source (Temporal.Activity.Context.details context)
    "successful heartbeat replaced the previous attempt's details"

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
      ~worker_shutting_down:(fun () -> false)
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
      ~worker_shutting_down:(fun () -> false)
  with
  | Error { code = "not_executable"; _ } -> ()
  | _ -> failwith "remote activity registration was accepted as executable"
  end

(** Exceptions from application activity code are converted into a typed,
    retired failure instead of escaping the worker loop. Exceptions are
    programmer defects, so the failure stays non-retryable, but it is
    identifiable as [ocaml_exception] and carries the backtrace when recording
    is enabled (#822). *)
let test_implementation_exception_is_retired () =
  let recording = Printexc.backtrace_status () in
  Printexc.record_backtrace true;
  Fun.protect ~finally:(fun () -> Printexc.record_backtrace recording)
  @@ fun () ->
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
  end;
  match (latest_completion supervisor).Protocol.result with
  | Protocol.Failed
      { message; stack_trace;
        info = Protocol.Application { type_name; non_retryable; _ }; _ } ->
      if not (String.equal type_name "ocaml_exception") then
        failwith "activity exception used the wrong failure type";
      if not non_retryable then
        failwith "activity exception would retry a programmer defect";
      if not (String.equal message "Failure(\"defect in activity\")") then
        failwith "activity exception lost its message";
      if String.equal stack_trace "" then
        failwith "activity exception lost its recorded backtrace"
  | _ -> failwith "activity exception did not submit an application failure"

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

(** The private context cell: the first published cancellation wins and stays
    fixed, a heartbeat after publication still submits its details but returns
    the [`Cancelled] error, and invalidation takes precedence over both while
    leaving the observed value readable. *)
let test_context_cancellation_signal () =
  let module Context = Temporal_base.Activity_context in
  let signal = Context.cancellation_signal () in
  let submitted = ref 0 in
  let info : Context.info =
    {
      namespace = "default"; workflow_id = "w"; workflow_run_id = "r";
      workflow_type = "t"; activity_id = "a"; activity_type = "x"; attempt = 1;
      is_local = false; scheduled_time = None;
      current_attempt_scheduled_time = None; started_time = None;
      schedule_to_close_timeout = None; start_to_close_timeout = None;
      task_heartbeat_timeout = None;
    }
  in
  let context =
    Context.create_for_task ~cancellation:signal
      ~worker_shutting_down:(fun () -> false) ~info
      ~heartbeat:(fun _ -> incr submitted; Ok ())
      ~details:[] ~heartbeat_timeout:None
  in
  if Context.heartbeat context [] <> Ok () then
    failwith "an uncancelled heartbeat failed";
  let first = { Context.reason = Context.Timed_out; reasons = [ Context.Timed_out ] } in
  if not (Context.signal_cancellation signal first) then
    failwith "the first cancellation was not published";
  if Context.signal_cancellation signal
       { reason = Context.Requested; reasons = [ Context.Requested ] }
  then failwith "a second cancellation replaced the first";
  if Context.cancellation context <> Some first then
    failwith "the context did not read its cell";
  begin match Context.heartbeat context [] with
  | Error error
    when (Temporal_base.Error.view error).category = `Cancelled
         && (Temporal_base.Error.view error).non_retryable ->
      ()
  | _ -> failwith "a heartbeat after cancellation did not report it"
  end;
  if !submitted <> 2 then failwith "a cancelled heartbeat was not submitted";
  Context.invalidate context;
  begin match Context.heartbeat context [] with
  | Error error when (Temporal_base.Error.view error).category = `Bridge -> ()
  | _ -> failwith "an invalidated context accepted a heartbeat"
  end;
  if !submitted <> 2 || Context.cancellation context <> Some first then
    failwith "invalidation changed the submitted or observed state"

(** Builds a cancellation task with an explicit Core reason and detail flags,
    for the cooperative-cancellation tests (#494). *)
let cancel_task_with ~reason ?details token : Protocol.task =
  {
    Protocol.task_token = Bytes.copy token;
    variant = Cancel { reason; details };
  }

(** A contextual activity that heartbeats [beats] times, recording what its
    context reports, and then hands its outcome to [finish]. Each heartbeat
    result is kept so tests can assert the exact cancellation boundary. *)
let heartbeating_activity ~name ~beats ~calls ~results ~finish =
  Temporal.Activity.define_with_context ~name ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun context () ->
      incr calls;
      if Option.is_some (Temporal.Activity.Context.cancellation context) then
        failwith "a fresh attempt context was already cancelled";
      for index = 1 to beats do
        results :=
          Temporal.Activity.Context.heartbeat context Temporal.Codec.int index
          :: !results
      done;
      finish context)

(** Asserts that a heartbeat result is the cooperative cancellation error. *)
let expect_cancelled_heartbeat = function
  | Error error when (Temporal.Error.view error).category = `Cancelled -> ()
  | Error error ->
      failwith ("heartbeat returned a non-cancellation error: " ^ Temporal.Error.message error)
  | Ok () -> failwith "heartbeat did not report the delivered cancellation"

(** A cancellation queued behind a running attempt is delivered to that
    attempt's context by its next heartbeat, with Core's reason and flags; the
    callback acknowledges it by returning the heartbeat error, and the adapter
    submits exactly one cancelled completion carrying the callback's details
    while keeping Core's details as outcome metadata. *)
let test_cancellation_delivered_through_heartbeat () =
  let supervisor = fake_supervisor () in
  let token = Bytes.of_string "\000running\255" in
  let calls = ref 0 in
  let results = ref [] in
  let observed = ref None in
  let detail : Temporal.Payload.t =
    { metadata = [ ("encoding", "binary/plain") ]; data = Bytes.of_string "cleaned up" }
  in
  let activity =
    heartbeating_activity ~name:"cooperative_cancel" ~beats:2 ~calls ~results
      ~finish:(fun context ->
        observed := Temporal.Activity.Context.cancellation context;
        match !results with
        | Error error :: _ ->
            let view = Temporal.Error.view error in
            Error
              (Temporal.Error.make ~non_retryable:true ~details:[ detail ]
                 ~category:view.category ~message:view.message ())
        | _ -> Ok "ignored")
  in
  enqueue supervisor
    (start_task ~token ~activity_type:"cooperative_cancel"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  enqueue supervisor
    (cancel_task_with ~reason:Protocol.Cancellation_requested
       ~details:
         {
           Protocol.is_not_found = false;
           is_cancelled = true;
           is_paused = true;
           is_timed_out = false;
           is_worker_shutdown = false;
           is_reset = false;
         }
       token);
  let worker = worker supervisor [ Adapter.register activity ] in
  begin match Worker.poll worker with
  | Ok
      (Adapter.Completed
         { kind = Adapter.Cancelled; cancellation_details = Some details; _ })
    when details.is_cancelled && details.is_paused ->
      ()
  | _ -> failwith "an acknowledged cancellation did not complete as cancelled"
  end;
  if !calls <> 1 then failwith "the cancelled callback did not run exactly once";
  (* The first heartbeat delivers the queued cancellation; both report it. *)
  List.iter expect_cancelled_heartbeat !results;
  begin match !observed with
  | Some cancellation ->
      let module C = Temporal.Activity.Cancellation in
      if C.reason cancellation <> C.Requested then
        failwith "the delivered cancellation lost its primary reason";
      if C.reasons cancellation <> [ C.Requested; C.Paused ] then
        failwith "the delivered cancellation lost its independent reasons"
  | None -> failwith "the context did not expose the delivered cancellation"
  end;
  if List.length !(supervisor.heartbeats) <> 2 then
    failwith "a cancelled heartbeat did not still record its details";
  if List.length !(supervisor.completions) <> 1 then
    failwith "cancellation produced more than one completion";
  begin match (latest_completion supervisor).Protocol.result with
  | Protocol.Cancelled { info = Protocol.Canceled { details = [ payload ]; _ }; _ }
    when Bytes.equal payload.data detail.data ->
      ()
  | _ -> failwith "the cancelled completion lost the callback's details"
  end;
  if not (Queue.is_empty supervisor.queue) then
    failwith "the cancellation task was left for a second completion";
  match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | _ -> failwith "a delivered cancellation was processed a second time"

(** A callback may ignore a delivered cancellation: returning [Ok] completes
    the attempt successfully, and a [`Cancelled] error without any delivered
    cancellation is an ordinary failure, never a fabricated cancellation. *)
let test_cancellation_outcome_precedence () =
  let supervisor = fake_supervisor () in
  let ignored = Bytes.of_string "ignored" in
  let calls = ref 0 in
  let results = ref [] in
  let ignoring =
    heartbeating_activity ~name:"ignores_cancel" ~beats:1 ~calls ~results
      ~finish:(fun _ -> Ok "finished anyway")
  in
  let claiming =
    Temporal.Activity.define_with_context ~name:"claims_cancel"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun _ () ->
        Error
          (Temporal.Error.make ~category:`Cancelled
             ~message:"not actually requested" ()))
  in
  enqueue supervisor
    (start_task ~token:ignored ~activity_type:"ignores_cancel"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  enqueue supervisor
    (cancel_task_with ~reason:Protocol.Cancellation_timed_out ignored);
  let worker =
    worker supervisor [ Adapter.register ignoring; Adapter.register claiming ]
  in
  expect_completed Adapter.Succeeded (Worker.poll worker);
  List.iter expect_cancelled_heartbeat !results;
  begin match (latest_completion supervisor).Protocol.result with
  | Protocol.Completed _ -> ()
  | _ -> failwith "an ignored cancellation changed a successful result"
  end;
  enqueue supervisor
    (start_task ~token:(Bytes.of_string "claims") ~activity_type:"claims_cancel"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; _ }) -> ()
  | _ -> failwith "an unrequested cancellation error was not a failure"
  end;
  match (latest_completion supervisor).Protocol.result with
  | Protocol.Failed { info = Protocol.Application { type_name = "cancelled"; _ }; _ } -> ()
  | _ -> failwith "an unrequested cancellation error became a cancellation"

(** Work that a heartbeat sweep takes but does not own is deferred in order: a
    start whose cancellation arrived before the start was admitted completes
    as cancelled without running its callback, and an uncancelled start runs
    normally afterwards. *)
let test_cancellation_before_admission () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let results = ref [] in
  let skipped_calls = ref 0 in
  let later_calls = ref 0 in
  let running =
    heartbeating_activity ~name:"sweeps" ~beats:1 ~calls ~results
      ~finish:(fun _ -> Ok "ran")
  in
  let skipped =
    Temporal.Activity.define ~name:"skipped" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () -> incr skipped_calls; Ok ())
  in
  let later =
    Temporal.Activity.define ~name:"later" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () -> incr later_calls; Ok ())
  in
  let unit_input = [ encode_input Temporal.Codec.unit () ] in
  let skipped_token = Bytes.of_string "skipped" in
  enqueue supervisor
    (start_task ~token:(Bytes.of_string "sweeps") ~activity_type:"sweeps"
       ~input:unit_input);
  enqueue supervisor
    (start_task ~token:skipped_token ~activity_type:"skipped" ~input:unit_input);
  enqueue supervisor
    (cancel_task_with ~reason:Protocol.Cancellation_requested skipped_token);
  enqueue supervisor
    (start_task ~token:(Bytes.of_string "later") ~activity_type:"later"
       ~input:unit_input);
  let worker =
    worker supervisor
      [
        Adapter.register running;
        Adapter.register skipped;
        Adapter.register later;
      ]
  in
  expect_completed Adapter.Succeeded (Worker.poll worker);
  (match !results with
  | [ Ok () ] -> ()
  | _ -> failwith "an unrelated cancellation reached the running attempt");
  if not (Queue.is_empty supervisor.queue) then
    failwith "the heartbeat sweep did not take every ready task";
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { kind = Adapter.Cancelled; activity_type; _ })
    when activity_type = Some "skipped" ->
      ()
  | _ -> failwith "a start cancelled before admission was not cancelled"
  end;
  if !skipped_calls <> 0 then
    failwith "a start cancelled before admission still ran its callback";
  expect_completed Adapter.Succeeded (Worker.poll worker);
  if !later_calls <> 1 then failwith "a deferred start did not run in order";
  begin match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | _ -> failwith "deferred work was processed twice"
  end;
  if !(supervisor.leased) <> [] then
    failwith "deferred work left an activity lease outstanding"

(** A context retained past its attempt neither sweeps nor reports a later
    cancellation: the attempt's outcome is already owned by its completion, so
    a cancellation that arrives afterwards stays with the source, which in
    production discards it as stale. *)
let test_cancellation_after_completion () =
  let supervisor = fake_supervisor () in
  let token = Bytes.of_string "finished" in
  let retained = ref None in
  let activity =
    Temporal.Activity.define_with_context ~name:"finishes"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun context () ->
        retained := Some context;
        Temporal.Activity.Context.heartbeat context Temporal.Codec.int 1)
  in
  enqueue supervisor
    (start_task ~token ~activity_type:"finishes"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register activity ] in
  expect_completed Adapter.Succeeded (Worker.poll worker);
  enqueue supervisor (cancel_task_with ~reason:Protocol.Cancellation_requested token);
  let context = Option.get !retained in
  begin match Temporal.Activity.Context.heartbeat context Temporal.Codec.int 2 with
  | Error error when (Temporal.Error.view error).category = `Bridge -> ()
  | _ -> failwith "a retained context accepted a heartbeat after completion"
  end;
  if Option.is_some (Temporal.Activity.Context.cancellation context) then
    failwith "a late cancellation reached a completed attempt";
  if Queue.length supervisor.queue <> 1 then
    failwith "a retained context swept the source after completion"

(** The shutdown probe supplied to the adapter is what the context reports,
    read live while the callback runs. *)
let test_worker_shutdown_signal () =
  let supervisor = fake_supervisor () in
  let shutting_down = Atomic.make false in
  let seen = ref [] in
  let activity =
    Temporal.Activity.define_with_context ~name:"watches_shutdown"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun context () ->
        seen := Temporal.Activity.Context.is_worker_shutting_down context :: !seen;
        Atomic.set shutting_down true;
        seen := Temporal.Activity.Context.is_worker_shutting_down context :: !seen;
        Ok ())
  in
  enqueue supervisor
    (start_task ~token:(Bytes.of_string "shutdown") ~activity_type:"watches_shutdown"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker ~shutting_down supervisor [ Adapter.register activity ] in
  expect_completed Adapter.Succeeded (Worker.poll worker);
  if !seen <> [ true; false ] then
    failwith "the context did not report the worker shutdown flag live";
  let synthetic =
    Temporal_base.Activity_context.unavailable ~details:[] ~heartbeat_timeout:None
  in
  if Temporal.Activity.Context.is_worker_shutting_down synthetic
     || Option.is_some (Temporal.Activity.Context.cancellation synthetic)
  then failwith "a synthetic context reported a stop signal"

(** Shutdown drain never runs deferred user code: a deferred start is failed
    retryably (or cancelled when its cancellation was deferred too), so no
    lease outlives the worker; a later poll then has nothing to dispatch. *)
let test_drain_retires_deferred_starts () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let results = ref [] in
  let deferred_calls = ref 0 in
  let running =
    heartbeating_activity ~name:"drain_sweeps" ~beats:1 ~calls ~results
      ~finish:(fun _ -> Ok "ran")
  in
  let deferred =
    Temporal.Activity.define ~name:"never_dispatched" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () -> incr deferred_calls; Ok ())
  in
  let unit_input = [ encode_input Temporal.Codec.unit () ] in
  let failed_token = Bytes.of_string "failed" in
  let cancelled_token = Bytes.of_string "cancelled" in
  enqueue supervisor
    (start_task ~token:(Bytes.of_string "drain") ~activity_type:"drain_sweeps"
       ~input:unit_input);
  enqueue supervisor
    (start_task ~token:failed_token ~activity_type:"never_dispatched"
       ~input:unit_input);
  enqueue supervisor
    (start_task ~token:cancelled_token ~activity_type:"never_dispatched"
       ~input:unit_input);
  enqueue supervisor
    (cancel_task_with ~reason:Protocol.Cancellation_worker_shutdown
       cancelled_token);
  let worker =
    worker supervisor [ Adapter.register running; Adapter.register deferred ]
  in
  expect_completed Adapter.Succeeded (Worker.poll worker);
  begin match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("drain failed: " ^ error.message)
  end;
  if !deferred_calls <> 0 then failwith "drain ran a deferred callback";
  if !(supervisor.leased) <> [] then
    failwith "drain left a deferred start leased";
  let result_for token =
    (List.find
       (fun (completion : Protocol.completion) ->
         Bytes.equal completion.task_token token)
       !(supervisor.completions))
      .result
  in
  begin match result_for failed_token with
  | Protocol.Failed
      {
        info =
          Protocol.Application
            { type_name = "ocaml_temporal_worker_shutdown"; non_retryable = false; _ };
        _;
      } ->
      ()
  | _ -> failwith "an undispatched start was not failed retryably"
  end;
  begin match result_for cancelled_token with
  | Protocol.Cancelled _ -> ()
  | _ -> failwith "a deferred start with a deferred cancellation was not cancelled"
  end;
  match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | _ -> failwith "drained deferred work was dispatched afterwards"

(** A poll error met by a heartbeat sweep is not lost: the heartbeat itself
    still succeeds, and the next poll reports the deferred error. *)
let test_sweep_poll_error_is_deferred () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.define_with_context ~name:"sweep_error"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun context () ->
        supervisor.poll_error :=
          Some { code = "poll_failed"; message = "sweep poll failed"; retryable = false };
        let result =
          Temporal.Activity.Context.heartbeat context Temporal.Codec.int 1
        in
        supervisor.poll_error := None;
        result)
  in
  enqueue supervisor
    (start_task ~token:(Bytes.of_string "sweep-error") ~activity_type:"sweep_error"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register activity ] in
  expect_completed Adapter.Succeeded (Worker.poll worker);
  match Worker.poll worker with
  | Error (error : Adapter.error_view) when error.code = "poll_failed" -> ()
  | _ -> failwith "a sweep poll error was not reported by the next poll"

(** Runs every adapter assertion with a stable test-process failure. *)
let () =
  test_successful_dispatch ();
  test_typed_failure ();
  test_typed_failure_error_type ();
  test_invalid_failure_details_allow_next_activity ();
  test_invalid_failure_completion_retry ();
  test_unrepresentable_context_retires_lease ();
  test_unknown_activity_retires_lease ();
  test_cancellation ();
  test_completion_retry_does_not_redo_activity ();
  test_transient_completion_exception_does_not_redo_activity ();
  test_non_retryable_completion_is_never_resubmitted ();
  test_contextual_heartbeat_lifecycle ();
  test_context_payloads_are_copied ();
  test_context_callback_exception_and_invalidation ();
  test_extra_input_is_rejected ();
  test_registration_validation ();
  test_implementation_exception_is_retired ();
  test_poll_error_is_typed ();
  test_context_cancellation_signal ();
  test_cancellation_delivered_through_heartbeat ();
  test_cancellation_outcome_precedence ();
  test_cancellation_before_admission ();
  test_cancellation_after_completion ();
  test_worker_shutdown_signal ();
  test_drain_retires_deferred_starts ();
  test_sweep_poll_error_is_deferred ()
