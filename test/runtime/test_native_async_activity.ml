(** Unit tests for deferred activity completion.

    The fake supervisor models the two distinct native obligations involved in
    [WillCompleteAsync]: the worker task completion that admits the handle, and
    the later namespace-bound client completion that retires the asynchronous
    lease. Keeping those ledgers separate makes it possible to catch the most
    dangerous integration mistake: sending a late completion through Core's
    worker task ledger after that ledger has already acknowledged the handoff.
*)

module Protocol = Temporal_protocol.Activity_protocol
module Encoded = Temporal_protocol.Encoded_activity_completion
module Encoded_heartbeat = Temporal_protocol.Encoded_activity_heartbeat
module Encoded_async_heartbeat = Temporal_protocol.Encoded_async_activity_heartbeat
module Raw_adapter = Temporal_runtime.Native_activity_execution
module Base_async = Temporal_base.Async_activity
module Bridge = Temporal_core_bridge.Native_bridge
module Worker_policy = Temporal_runtime.Native_worker_policy

(** Copies a public payload into the private representation used by the base
    adapter. The test intentionally performs the same ownership conversion as
    the production private module so mutable bytes cannot alias the fake. *)
let base_payload (payload : Temporal.Payload.t) : Temporal_base.Payload.t =
  {
    Temporal_base.Payload.metadata =
      List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Converts a public error without exposing the private error representation
    to the test's fake supervisor. *)
let base_error (error : Temporal.Error.t) : Temporal_base.Error.t =
  let view = Temporal.Error.view error in
  Temporal_base.Error.make ~non_retryable:view.non_retryable
    ~details:(List.map base_payload view.details) ~category:view.category
    ~message:view.message ()

(** Installs public codec callbacks in the base payload codec. This preserves
    value-dependent metadata such as [Codec.option] rather than reconstructing
    a codec from only its nominal encoding name. *)
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

(** Converts a public asynchronous activity definition for the private
    adapter. The opaque handle itself is already the base handle type; only the
    callback's result constructors and expected error type need translation. *)
let base_async_activity (definition : ('input, 'output) Temporal.Activity.t) =
  let implementation =
    Option.map
      (fun implementation context input ->
        match implementation context input with
        | Temporal.Activity.Completed output -> Base_async.Completed output
        | Temporal.Activity.Failed error -> Base_async.Failed (base_error error)
        | Temporal.Activity.Will_complete_async handle ->
            Base_async.Will_complete_async handle)
      (Temporal.Activity.implementation_async definition)
  in
  Temporal_base.Definition.make ~name:(Temporal.Activity.name definition)
    ~input:(base_codec (Temporal.Activity.input definition))
    ~output:(base_codec (Temporal.Activity.output definition)) ~implementation

(** Strictly decodes the adapter's single-encode completion bytes, as the Rust
    bridge does, and checks that they are the canonical encoding of the
    decoded value. The fake therefore owns independent token and payload
    storage exactly as a native implementation must, and never encodes a
    submission itself (issue #995). *)
let copy_completion encoded =
  let json = Encoded.to_string encoded in
  match Protocol.decode_completion json with
  | Error _ -> failwith "async fake received an invalid completion"
  | Ok value ->
      if Protocol.encode_completion value <> Ok json then
        failwith "async fake received non-canonical completion bytes";
      value

(** Strictly decodes the adapter's single-encode heartbeat bytes, as the
    Rust bridge does, and checks that they are canonical. Decoding gives the
    fake its own storage for late detail payloads, and the fake never encodes
    a submission itself (#1001). *)
let copy_heartbeat encoded =
  let json = Encoded_heartbeat.to_string encoded in
  match Protocol.decode_heartbeat json with
  | Error _ -> failwith "async fake received an invalid heartbeat"
  | Ok value ->
      if Protocol.encode_heartbeat value <> Ok json then
        failwith "async fake received non-canonical heartbeat bytes";
      value

(** Strictly decodes an async handle's single-encode heartbeat bytes, which
    also carry the attempt's heartbeat timeout (#1025), and checks that they
    are canonical, as [copy_heartbeat] does for a worker heartbeat. *)
let copy_async_heartbeat encoded =
  let json = Encoded_async_heartbeat.to_string encoded in
  match Protocol.decode_async_heartbeat json with
  | Error _ -> failwith "async fake received an invalid async heartbeat"
  | Ok value ->
      if Protocol.encode_async_heartbeat value <> Ok json then
        failwith "async fake received non-canonical async heartbeat bytes";
      value

type source_error = {
  code : string;
  message : string;
  retryable : bool;
  native_status : Bridge.status option;
}
(** Bounded source diagnostics returned by the deterministic supervisor. *)

type fake_supervisor = {
  queue : Protocol.task Queue.t;
  leased : bytes list ref;
  local_leased : bytes list ref;
  async_leased : bytes list ref;
  completions : Protocol.completion list ref;
  async_completions : Protocol.completion list ref;
  heartbeats : Protocol.heartbeat list ref;
  async_heartbeats : Protocol.heartbeat list ref;
  async_completion_calls : int ref;
  async_heartbeat_calls : int ref;
  async_completion_documents : string list ref;
  async_heartbeat_documents : string list ref;
  reject_next_worker_completion : bool ref;
  reject_next_async_completion : bool ref;
  reject_next_async_completion_terminal : bool ref;
  reject_next_async_completion_rejected : bool ref;
  reject_next_async_heartbeat : bool ref;
  reject_next_async_heartbeat_connection : bool ref;
  reject_next_async_heartbeat_rejected : bool ref;
  reject_next_async_heartbeat_terminal : bool ref;
  async_heartbeat_hook : (unit -> unit) ref;
      (** Runs first in every async heartbeat, standing for the time the RPC
          spends on the network; a test may block in it (#1025). *)
  async_heartbeat_timeouts : Protocol.duration option list ref;
      (** Heartbeat timeout carried by each accepted async heartbeat, newest
          first (#1025). *)
  async_heartbeat_response : Protocol.async_heartbeat_response ref;
      (** Activity state every accepted async heartbeat reports, standing
          for the flags of the token's latest send (#1025). *)
  free_client_call_slots : int ref;
      (** Free slots of the bridge's pending client-call registry, which
          every namespace-bound async call must occupy while it runs. At
          zero the registry is full and a call is refused with
          [Resource_exhausted] before any RPC exists (#1104). *)
  refused_client_calls : int ref;
      (** Async calls refused because the registry was full; they are not
          counted as submissions. *)
}
(** Fake native state. [leased] and [async_leased] intentionally model
    different Temporal APIs and are never allowed to substitute for one another.
    [async_completion_documents] and [async_heartbeat_documents] keep, newest
    first, the exact strings each namespace-bound call received, so a test can
    check physical identity of a retried submission (#1001).
*)

let fake_supervisor () =
  {
    queue = Queue.create ();
    leased = ref [];
    local_leased = ref [];
    async_leased = ref [];
    completions = ref [];
    async_completions = ref [];
    heartbeats = ref [];
    async_heartbeats = ref [];
    async_completion_calls = ref 0;
    async_heartbeat_calls = ref 0;
    async_completion_documents = ref [];
    async_heartbeat_documents = ref [];
    reject_next_worker_completion = ref false;
    reject_next_async_completion = ref false;
    reject_next_async_completion_terminal = ref false;
    reject_next_async_completion_rejected = ref false;
    reject_next_async_heartbeat = ref false;
    reject_next_async_heartbeat_connection = ref false;
    reject_next_async_heartbeat_rejected = ref false;
    reject_next_async_heartbeat_terminal = ref false;
    async_heartbeat_hook = ref ignore;
    async_heartbeat_timeouts = ref [];
    async_heartbeat_response = ref Protocol.Heartbeat_recorded;
    free_client_call_slots = ref max_int;
    refused_client_calls = ref 0;
  }

(** Removes the first byte-identical token from a lease ledger. Tokens are
    binary values, so text comparison would be both unsafe and incorrect. *)
let remove_token token tokens =
  let rec loop reversed = function
    | [] -> (false, List.rev reversed)
    | current :: rest when Bytes.equal current token ->
        (true, List.rev_append reversed rest)
    | current :: rest -> loop (current :: reversed) rest
  in
  loop [] tokens

(** Finds a token without exposing or decoding its bytes. *)
let has_token token tokens =
  List.exists (fun current -> Bytes.equal current token) tokens

(** Creates a stable fake error for stale or unavailable leases. *)
let source_error ?(retryable = false) ?native_status code message =
  { code; message; retryable; native_status }

(** Runs one namespace-bound async call while it occupies a slot of the
    fake client-call registry, as the bridge's submitted-call admission does
    (#1104). A full registry refuses the call with [Resource_exhausted]
    before [call] runs, so nothing is submitted or recorded; otherwise the
    slot is held for the duration of [call] and released afterwards. *)
let with_client_call_slot supervisor call =
  if !(supervisor.free_client_call_slots) <= 0 then begin
    incr supervisor.refused_client_calls;
    Error
      (source_error ~native_status:Bridge.Resource_exhausted
         "resource_exhausted"
         "too many Temporal client calls are pending (limit 1)")
  end
  else begin
    decr supervisor.free_client_call_slots;
    Fun.protect
      ~finally:(fun () -> incr supervisor.free_client_call_slots)
      call
  end

(** Implements the complete supervisor contract needed by the adapter. All
    methods copy values before retaining them, and late operations consult only
    the asynchronous ledger. *)
module Fake_supervisor = struct
  type t = fake_supervisor
  type error = source_error

  (** Leases one queued task in producer order. *)
  let try_poll_activity supervisor =
    if Queue.is_empty supervisor.queue then Ok None
    else
      let task = Queue.take supervisor.queue in
      supervisor.leased := Bytes.copy task.task_token :: !(supervisor.leased);
      (match task.variant with
      | Protocol.Start { is_local = true; _ } ->
          supervisor.local_leased :=
            Bytes.copy task.task_token :: !(supervisor.local_leased)
      | Protocol.Start _ | Protocol.Cancel _ -> ());
      Ok (Some task)

  (** Accepts a worker completion only for a leased token. Core rejects a
      deferred local result before retiring its token; the optional one-shot
      transport failure checks that an ordinary local failure is retried
      without rerunning the callback. *)
  let complete_activity supervisor encoded =
    let completion = copy_completion encoded in
    let token = completion.Protocol.task_token in
    if not (has_token token !(supervisor.leased)) then
      Error (source_error "stale_lease" "worker lease is not active")
    else if
      has_token token !(supervisor.local_leased)
      && completion.Protocol.result = Protocol.Will_complete_async
    then
      Error
        (source_error "malformed_activity_completion"
           "local activities cannot be completed asynchronously")
    else if !(supervisor.reject_next_worker_completion) then begin
      supervisor.reject_next_worker_completion := false;
      Error
        (source_error ~retryable:true "temporarily_unavailable"
           "worker completion transport unavailable")
    end
    else begin
      let _, remaining = remove_token token !(supervisor.leased) in
      supervisor.leased := remaining;
      let _, remaining_local = remove_token token !(supervisor.local_leased) in
      supervisor.local_leased := remaining_local;
      supervisor.completions := completion :: !(supervisor.completions);
      begin
        match completion.Protocol.result with
        | Protocol.Will_complete_async ->
            supervisor.async_leased :=
              Bytes.copy completion.Protocol.task_token
              :: !(supervisor.async_leased)
        | Protocol.Completed _ | Protocol.Failed _ | Protocol.Cancelled _ -> ()
      end;
      Ok ()
    end

  (** Accepts one late terminal completion only from the async ledger. The
      one-shot failures return the native statuses the Rust bridge actually
      emits for namespace-bound completions: [Invalid_state] for a server
      [NotFound], [Connection] for an ambiguous RPC outcome, and
      [Async_heartbeat_rejected] for a definitive rejection. A full fake
      client-call registry refuses the call first ([Resource_exhausted]). *)
  let complete_async_activity supervisor encoded =
    with_client_call_slot supervisor @@ fun () ->
    let completion = copy_completion encoded in
    incr supervisor.async_completion_calls;
    supervisor.async_completion_documents :=
      Encoded.to_string encoded :: !(supervisor.async_completion_documents);
    if !(supervisor.reject_next_async_completion_terminal) then begin
      supervisor.reject_next_async_completion_terminal := false;
      (* A terminal [NotFound] response means the native task token is no
         longer a completion capability. Retire the fake native ledger before
         returning the error so this test models that one-way server state
         transition rather than leaving an impossible lease behind. *)
      let _, remaining =
        remove_token completion.Protocol.task_token !(supervisor.async_leased)
      in
      supervisor.async_leased := remaining;
      Error
        (source_error ~native_status:Bridge.Invalid_state "not_found"
           "async activity no longer exists")
    end
    else if !(supervisor.reject_next_async_completion) then begin
      supervisor.reject_next_async_completion := false;
      Error
        (source_error ~native_status:Bridge.Connection "connection"
           "async completion transport unavailable")
    end
    else if !(supervisor.reject_next_async_completion_rejected) then begin
      supervisor.reject_next_async_completion_rejected := false;
      Error
        (source_error ~native_status:Bridge.Async_heartbeat_rejected
           "async_heartbeat_rejected" "async completion request rejected")
    end
    else
      let found, remaining =
        remove_token completion.Protocol.task_token !(supervisor.async_leased)
      in
      if not found then
        Error (source_error "stale_async_lease" "async lease is not active")
      else begin
        supervisor.async_leased := remaining;
        supervisor.async_completions :=
          completion :: !(supervisor.async_completions);
        Ok ()
      end

  (** Records ordinary activity heartbeats only while the worker lease remains
      active. Async callbacks use [record_async_activity_heartbeat] instead. *)
  let record_activity_heartbeat supervisor encoded =
    let heartbeat = copy_heartbeat encoded in
    if has_token heartbeat.Protocol.task_token !(supervisor.leased) then begin
      supervisor.heartbeats := heartbeat :: !(supervisor.heartbeats);
      Ok ()
    end
    else Error (source_error "stale_lease" "worker lease is not active")

  (** Records heartbeat details against the namespace-bound async lease. *)
  let record_async_activity_heartbeat supervisor encoded =
    with_client_call_slot supervisor @@ fun () ->
    !(supervisor.async_heartbeat_hook) ();
    let { Protocol.heartbeat; heartbeat_timeout } = copy_async_heartbeat encoded in
    incr supervisor.async_heartbeat_calls;
    supervisor.async_heartbeat_documents :=
      Encoded_async_heartbeat.to_string encoded
      :: !(supervisor.async_heartbeat_documents);
    if !(supervisor.reject_next_async_heartbeat_terminal) then begin
      supervisor.reject_next_async_heartbeat_terminal := false;
      let _, remaining =
        remove_token heartbeat.Protocol.task_token !(supervisor.async_leased)
      in
      supervisor.async_leased := remaining;
      Error
        (source_error ~native_status:Bridge.Invalid_state "not_found"
           "async activity no longer exists")
    end
    else if !(supervisor.reject_next_async_heartbeat_connection) then begin
      supervisor.reject_next_async_heartbeat_connection := false;
      Error
        (source_error ~native_status:Bridge.Connection "connection"
           "async heartbeat transport unavailable")
    end
    else if !(supervisor.reject_next_async_heartbeat_rejected) then begin
      supervisor.reject_next_async_heartbeat_rejected := false;
      Error
        (source_error ~native_status:Bridge.Async_heartbeat_rejected
           "async_heartbeat_rejected" "async heartbeat details rejected")
    end
    else if !(supervisor.reject_next_async_heartbeat) then begin
      supervisor.reject_next_async_heartbeat := false;
      Error
        (source_error ~retryable:true "temporarily_unavailable"
           "async heartbeat transport unavailable")
    end
    else if has_token heartbeat.Protocol.task_token !(supervisor.async_leased) then begin
      supervisor.async_heartbeats := heartbeat :: !(supervisor.async_heartbeats);
      supervisor.async_heartbeat_timeouts :=
        heartbeat_timeout :: !(supervisor.async_heartbeat_timeouts);
      Ok !(supervisor.async_heartbeat_response)
    end
    else Error (source_error "stale_async_lease" "async lease is not active")

  (** Exposes stable source classifications to the adapter. *)
  let error_code error = error.code
  let error_message error = error.message
  let error_is_retryable error =
    match error.native_status with
    | Some status -> Worker_policy.activity_completion_retryable status
    | None -> error.retryable

  (** Exercise the production async-operation classifier on statuses that the
      Rust async client actually returns, rather than on a premarked retryable
      fake. *)
  let async_operation_error_disposition error =
    match error.native_status with
    | Some status -> Worker_policy.async_operation_disposition status
    | None ->
        if error.retryable then Worker_policy.Retry_exact
        else Worker_policy.Retired

  let exception_is_retryable _ = false
end

module Adapter = struct
  include Raw_adapter

  (** Keeps the test call sites public-API shaped while the adapter receives a
      private base definition. *)
  let register_async definition =
    Raw_adapter.register_async (base_async_activity definition)
end

module Worker = Adapter.Make (Fake_supervisor)

(** Converts a public payload to the binary metadata shape used by the strict
    activity protocol fixture. *)
let protocol_payload (payload : Temporal.Payload.t) : Protocol.payload =
  {
    Protocol.metadata =
      List.map (fun (key, value) -> (key, Bytes.of_string value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Encodes one typed fixture value and fails loudly if its codec is invalid. *)
let encode_input codec value =
  match Temporal.Codec.encode codec value with
  | Ok payload -> protocol_payload payload
  | Error error -> failwith (Temporal.Error.message error)

(** Decodes one completion payload with the same public codec used by the
    registered activity. *)
let decode_output codec (payload : Protocol.payload) =
  let public_payload : Temporal.Payload.t =
    {
      metadata =
        List.map (fun (key, value) -> (key, Bytes.to_string value)) payload.metadata;
      data = Bytes.copy payload.data;
    }
  in
  match Temporal.Codec.decode codec public_payload with
  | Ok value -> value
  | Error error -> failwith (Temporal.Error.message error)

(** Builds a start task with explicit Core locality while leaving timing fields
    absent; async lifecycle tests focus on token ownership and payloads. *)
let start_task_with_locality ~is_local ~token ~activity_type ~input : Protocol.task =
  let start : Protocol.activity_start =
    {
      is_local;
      workflow_namespace = "default";
      workflow_type = "async_test_workflow";
      workflow_execution =
        { Protocol.workflow_id = "async-workflow-1"; run_id = "async-run-1" };
      activity_id = "async-activity-1";
      activity_type;
      header_fields = [];
      input;
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

(** Builds a remote task for the existing deferred-completion controls. *)
let start_task ~token ~activity_type ~input =
  start_task_with_locality ~is_local:false ~token ~activity_type ~input

(** Adds a task to the deterministic source queue. *)
let enqueue supervisor task = Queue.add task supervisor.queue

(** Creates an adapter worker and turns setup failures into readable test
    failures. *)
let worker supervisor activities =
  match
    Worker.create ~supervisor ~activities
      ~worker_shutting_down:(fun () -> false)
  with
  | Ok worker -> worker
  | Error (error : Raw_adapter.error_view) ->
      failwith
        (Printf.sprintf "async worker creation failed: %s at %s (%s)"
           error.message error.path error.code)

(** Asserts that a poll admitted a deferred completion. *)
let expect_deferred = function
  | Ok (Raw_adapter.Completed { kind = Raw_adapter.Deferred; _ }) -> ()
  | Ok (Raw_adapter.Completed _) -> failwith "async poll returned the wrong kind"
  | Ok Raw_adapter.Not_ready -> failwith "async poll unexpectedly reported Not_ready"
  | Ok (Raw_adapter.Rejected { error; _ }) ->
      failwith ("async activity was rejected: " ^ error.message)
  | Error (error : Raw_adapter.error_view) ->
      failwith
        (Printf.sprintf "async poll failed: %s at %s (%s)" error.message
           error.path error.code)

(** Asserts that a callback defect was converted into a retired, typed
    rejection rather than being mistaken for a deferred activity. *)
let expect_rejected code
    (result : (Raw_adapter.outcome, Raw_adapter.error_view) result) =
  match result with
  | Ok
      (Raw_adapter.Rejected
        { error = { code = actual; _ }; lease_retired = true; _ })
    when String.equal actual code -> ()
  | Ok (Raw_adapter.Rejected { error; lease_retired; _ }) ->
      failwith
        (Printf.sprintf
           "async rejection had unexpected diagnostic %s or lease state %b"
           error.code lease_retired)
  | Ok (Raw_adapter.Completed _) ->
      failwith "stale async handle was accepted as a deferred completion"
  | Ok Raw_adapter.Not_ready ->
      failwith "stale async handle unexpectedly produced Not_ready"
  | Error error ->
      failwith
        (Printf.sprintf "stale async handle escaped as adapter error: %s" error.code)

(** A local callback may choose the public deferred result, but Core cannot
    accept that handoff. The adapter submits one ordinary failure, keeps it on
    an uncertain first submission, and then processes the next local task
    without activating the retained handle. *)
let test_local_deferred_failure_keeps_worker_progress () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 and retained = ref None in
  let activity =
    Temporal.Activity.define_async ~name:"local_async_rejection"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
      (fun context () ->
        incr calls;
        if !calls = 1 then begin
          let handle = Temporal.Activity.Async_context.handle context in
          retained := Some handle;
          Temporal.Activity.Will_complete_async handle
        end
        else Temporal.Activity.Completed "next")
  in
  let first_token = Bytes.of_string "local-async-first" in
  let second_token = Bytes.of_string "local-async-second" in
  let input = [ encode_input Temporal.Codec.unit () ] in
  enqueue supervisor
    (start_task_with_locality ~is_local:true ~token:first_token
       ~activity_type:"local_async_rejection" ~input);
  enqueue supervisor
    (start_task_with_locality ~is_local:true ~token:second_token
       ~activity_type:"local_async_rejection" ~input);
  let worker = worker supervisor [ Adapter.register_async activity ] in
  supervisor.reject_next_worker_completion := true;
  begin
    match Worker.poll worker with
    | Error { retryable = true; code = "completion_failed"; _ } -> ()
    | _ -> failwith "uncertain local failure did not retain its completion"
  end;
  if !calls <> 1 || Queue.length supervisor.queue <> 1 then
    failwith "uncertain local completion reran the callback or consumed the next task";
  if not (has_token first_token !(supervisor.leased)) then
    failwith "uncertain local failure lost the worker lease";
  expect_rejected "local_async_unsupported" (Worker.poll worker);
  if !calls <> 1 then failwith "local failure retry reran the callback";
  begin
    match Temporal.Activity.Async_handle.complete (Option.get !retained) "late" with
    | Error error when (Temporal.Error.view error).non_retryable -> ()
    | Error _ -> failwith "rejected local handle remained retryable"
    | Ok () -> failwith "rejected local handle became active"
  end;
  begin
    match Worker.poll worker with
    | Ok (Raw_adapter.Completed { kind = Raw_adapter.Succeeded; _ }) -> ()
    | _ -> failwith "local deferred failure blocked the next activity"
  end;
  if !calls <> 2 || not (Queue.is_empty supervisor.queue) then
    failwith "next local callback did not run exactly once";
  if !(supervisor.leased) <> [] || !(supervisor.local_leased) <> []
     || !(supervisor.async_leased) <> [] then
    failwith "local completion left a worker or async lease outstanding";
  begin
    match !(supervisor.completions) with
    | [ { Protocol.task_token = second; result = Protocol.Completed _ };
        { Protocol.task_token = first;
          result = Protocol.Failed
            { info = Protocol.Application { non_retryable = true; _ }; _ } } ]
      when Bytes.equal second second_token && Bytes.equal first first_token -> ()
    | _ -> failwith "local failure and following success were not acknowledged"
  end;
  match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("local failure blocked drain: " ^ error.message)

(** Deferred completion admits a handle only after the worker-side handoff,
    then routes heartbeat and terminal operations through the async ledger. *)
let test_deferred_lifecycle () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let retained = ref None in
  let activity =
    Temporal.Activity.define_async ~name:"async_deferred_lifecycle"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
      (fun context () ->
        incr calls;
        let handle = Temporal.Activity.Async_context.handle context in
        (* The handle is deliberately dormant while the callback executes. *)
        begin
          match Temporal.Activity.Async_handle.complete handle "too-early" with
          | Error _ -> ()
          | Ok () -> failwith "dormant async handle submitted before handoff"
        end;
        retained := Some handle;
        Temporal.Activity.Will_complete_async handle)
  in
  let token = Bytes.of_string "async-lifecycle-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"async_deferred_lifecycle"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register_async activity ] in
  expect_deferred (Worker.poll worker);
  if !calls <> 1 then failwith "async callback ran more than once";
  if !(supervisor.leased) <> [] then
    failwith "worker ledger retained a lease after async handoff";
  if not (has_token token !(supervisor.async_leased)) then
    failwith "async handoff did not create a client-side lease";
  let handle =
    match !retained with
    | Some handle -> handle
    | None -> failwith "async callback did not retain its handle"
  in
  let detail : Temporal.Payload.t =
    { metadata = [ ("encoding", "binary/plain") ]; data = Bytes.of_string "tick" }
  in
  begin
    match Temporal.Activity.Async_handle.heartbeat handle [ detail ] with
    | Ok () -> ()
    | Error error ->
        failwith ("async heartbeat failed: " ^ Temporal.Error.message error)
  end;
  begin
    match !(supervisor.async_heartbeats) with
    | [ heartbeat ] when Bytes.equal heartbeat.Protocol.task_token token -> ()
    | _ -> failwith "async heartbeat was not recorded against the client lease"
  end;
  begin
    match Temporal.Activity.Async_handle.complete handle "finished" with
    | Ok () -> ()
    | Error error ->
        failwith ("async completion failed: " ^ Temporal.Error.message error)
  end;
  if !(supervisor.async_leased) <> [] then
    failwith "accepted async completion did not retire the client lease";
  begin
    match !(supervisor.async_completions) with
    | [ { Protocol.result = Protocol.Completed payload; task_token } ] ->
        if not (Bytes.equal task_token token) then
          failwith "async completion changed the opaque token";
        if decode_output Temporal.Codec.string payload <> "finished" then
          failwith "async completion output was not encoded by its definition"
    | _ -> failwith "expected one successful async completion"
  end;
  begin
    match Temporal.Activity.Async_handle.complete handle "again" with
    | Error _ -> ()
    | Ok () -> failwith "terminal async handle accepted a second completion"
  end;
  begin
    match Temporal.Activity.Async_handle.heartbeat handle [ detail ] with
    | Error _ -> ()
    | Ok () -> failwith "terminal async handle accepted a heartbeat"
  end

(** Admits a handle with a caller-supplied output codec so payload validation
    can be tested independently of callback dispatch and worker handoff. *)
let payload_validation_fixture output =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let retained = ref None in
  let activity =
    Temporal_base.Definition.make ~name:"async_payload_validation"
      ~input:Temporal_base.Codec.unit ~output
      ~implementation:(Some (fun context () ->
        incr calls;
        let handle = Base_async.handle context in
        retained := Some handle;
        Base_async.Will_complete_async handle))
  in
  let token = Bytes.of_string "\000async-validation\255" in
  enqueue supervisor
    (start_task ~token ~activity_type:"async_payload_validation"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Raw_adapter.register_async activity ] in
  expect_deferred (Worker.poll worker);
  (supervisor, worker, Option.get !retained, calls, token)

(** Local validation errors must preserve both lease registries and release
    the invalid request key. Exercise each handle method with a conversion
    error and two errors caught only by the complete protocol encoder. *)
let test_invalid_async_payload_preserves_handle () =
  let codec = Temporal_base.Codec.of_payload
      ~encode:(fun payload -> Ok (base_payload payload))
      ~decode:(fun _ -> Error (Temporal_base.Error.codec
        ~message:"output-only test codec")) in
  let cases =
    [ (* A binary metadata value and a NUL in a key are valid (#1048);
         a key that is not UTF-8 is not. *)
      ("non-utf8-key", [ ("bad\255key", "value") ]);
      ("duplicate", [ ("opaque", "one"); ("opaque", "two") ]);
      ("oversized", [ (String.make 65_537 'k', "value") ]) ]
  in
  List.iter
    (fun (label, metadata) ->
      List.iter
        (fun operation ->
          List.iter
            (fun correct_same_method ->
              let supervisor, worker, handle, calls, token =
                payload_validation_fixture codec
              in
              let invalid : Temporal.Payload.t =
                { metadata; data = Bytes.of_string "private-invalid-payload" }
              in
              let valid : Temporal.Payload.t =
                { metadata = [ ("encoding", "binary/plain") ];
                  data = Bytes.of_string "corrected" }
              in
              (* Complete obtains the invalid payload from its registered
                 codec; the other methods accept detail payloads directly. *)
              let submit payload =
                match operation with
                | `Complete -> Temporal.Activity.Async_handle.complete handle payload
                | `Fail -> Temporal.Activity.Async_handle.fail handle
                    (Temporal.Error.make ~category:`Activity ~message:"failure"
                       ~details:[ payload ] ())
                | `Cancel -> Temporal.Activity.Async_handle.cancel handle [ payload ]
                | `Heartbeat -> Temporal.Activity.Async_handle.heartbeat handle [ payload ]
              in
              (match submit invalid with
              | Error _ -> ()
              | Ok () -> failwith (label ^ ": invalid async payload was accepted"));
              if !(supervisor.async_completion_calls) <> 0
                 || !(supervisor.async_heartbeat_calls) <> 0 then
                failwith (label ^ ": local rejection reached the native supervisor");
              if !(supervisor.async_leased) <> [ token ] then
                failwith (label ^ ": local rejection retired the native lease");
              (match Worker.drain worker with
              | Error { code = "outstanding_async_leases"; retryable = true; _ } -> ()
              | _ -> failwith (label ^ ": local rejection lost the adapter lease"));
              let corrected =
                if correct_same_method then submit valid
                else Temporal.Activity.Async_handle.complete handle valid
              in
              (match corrected with
              | Ok () -> ()
              | Error error -> failwith (label ^ ": corrected request failed: "
                  ^ Temporal.Error.message error));
              if correct_same_method && operation = `Heartbeat then begin
                (match Worker.drain worker with
                | Error { code = "outstanding_async_leases"; _ } -> ()
                | _ -> failwith "corrected heartbeat retired the completion capability");
                match Temporal.Activity.Async_handle.complete handle valid with
                | Ok () -> ()
                | Error error -> failwith (Temporal.Error.message error)
              end;
              if !calls <> 1 || !(supervisor.async_completion_calls) <> 1
                 || List.length !(supervisor.async_completions) <> 1
                 || !(supervisor.async_heartbeat_calls)
                    <> (if correct_same_method && operation = `Heartbeat then 1 else 0)
                 || !(supervisor.async_leased) <> [] then
                failwith (label ^ ": corrected request did not retire exactly one lease");
              match Worker.drain worker with
              | Ok () -> ()
              | Error _ -> failwith "completed handle still blocked drain")
            [ true; false ])
        [ `Complete; `Fail; `Cancel; `Heartbeat ])
    cases

(** A codec's own typed rejection happens before an operation is reserved and
    must continue to allow a later, successfully encoded output. *)
let test_async_codec_error_preserves_handle () =
  let codec = Temporal.Codec.make ~encoding:"binary/plain"
      ~encode:(fun valid ->
        if valid then Ok Bytes.empty
        else Error (Temporal.Error.codec ~message:"invalid output"))
      ~decode:(fun _ -> Ok true) in
  let supervisor, worker, handle, _, _ = payload_validation_fixture (base_codec codec) in
  (match Temporal.Activity.Async_handle.complete handle false with
  | Error _ -> ()
  | Ok () -> failwith "codec rejected output was submitted");
  if !(supervisor.async_completion_calls) <> 0 then
    failwith "codec failure reached the supervisor";
  (match Temporal.Activity.Async_handle.complete handle true with
  | Ok () -> ()
  | Error error -> failwith (Temporal.Error.message error));
  match Worker.drain worker with
  | Ok () when !(supervisor.async_leased) = [] -> ()
  | _ -> failwith "codec recovery left completion debt"

(** Payload metadata values are arbitrary bytes in Temporal's protocol
    (#1048), so a public codec whose encoding marker is not UTF-8 produces a
    valid payload. The asynchronous completion must be submitted once with the
    marker unchanged instead of being rejected locally. *)
let test_async_binary_encoding_is_submitted () =
  let codec = Temporal.Codec.option
      (Temporal.Codec.make ~encoding:"\255"
        ~encode:(fun value -> Ok (Bytes.of_string value))
        ~decode:(fun bytes -> Ok (Bytes.to_string bytes))) in
  let supervisor, worker, handle, _, _ = payload_validation_fixture (base_codec codec) in
  (match Temporal.Activity.Async_handle.complete handle (Some "value") with
  | Ok () -> ()
  | Error error -> failwith (Temporal.Error.message error));
  if !(supervisor.async_completion_calls) <> 1 then
    failwith "binary encoding marker did not reach the supervisor exactly once";
  match Worker.drain worker with
  | Ok () when !(supervisor.async_leased) = [] -> ()
  | _ -> failwith "binary encoding completion left completion debt"

(** An ambiguous [Connection] failure of a late completion keeps the handle
    and native lease and retains the exact request key (#821). Retrying it
    succeeds without invoking the asynchronous implementation again. *)
let test_async_completion_retry () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let retained = ref None in
  let activity =
    Temporal.Activity.define_async ~name:"async_completion_retry"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
      (fun context () ->
        incr calls;
        let handle = Temporal.Activity.Async_context.handle context in
        retained := Some handle;
        Temporal.Activity.Will_complete_async handle)
  in
  let token = Bytes.of_string "async-retry-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"async_completion_retry"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register_async activity ] in
  expect_deferred (Worker.poll worker);
  supervisor.reject_next_async_completion := true;
  let handle = Option.get !retained in
  begin
    match Temporal.Activity.Async_handle.complete handle "once" with
    | Error error when (Temporal.Error.view error).non_retryable ->
        failwith "uncertain async completion was reported as non-retryable"
    | Error _ -> ()
    | Ok () -> failwith "rejected async completion was reported as accepted"
  end;
  if !calls <> 1 then failwith "async completion retry reran the callback";
  if not (has_token token !(supervisor.async_leased)) then
    failwith "async completion rejection retired the client lease";
  (match Worker.drain worker with
  | Error { code = "outstanding_async_leases"; retryable = true; _ } -> ()
  | _ -> failwith "uncertain completion dropped the adapter async lease");
  (match Temporal.Activity.Async_handle.complete handle "different" with
  | Error _ -> ()
  | Ok () -> failwith "uncertain completion allowed a different output");
  (match Temporal.Activity.Async_handle.heartbeat handle [] with
  | Error _ -> ()
  | Ok () -> failwith "uncertain completion allowed a different operation");
  if !(supervisor.async_completion_calls) <> 1
     || !(supervisor.async_heartbeat_calls) <> 0 then
    failwith "conflicting retry entered the native supervisor";
  begin
    match Temporal.Activity.Async_handle.complete handle "once" with
    | Ok () -> ()
    | Error error ->
        failwith ("retry of exact async completion failed: " ^ Temporal.Error.message error)
  end;
  if !calls <> 1 then failwith "accepted async retry reran the callback";
  if List.length !(supervisor.async_completions) <> 1 then
    failwith "async completion retry submitted more than one accepted result";
  if !(supervisor.async_completion_calls) <> 2 then
    failwith "async completion did not submit exactly one identical retry";
  (* The retry reuses the bytes validated and encoded for the first
     submission rather than preparing the completion again (#1001). *)
  match !(supervisor.async_completion_documents), !(supervisor.async_completions) with
  | [ retry; first ], [ accepted ] ->
      if not (retry == first) then
        failwith "async completion retry submitted a re-encoded document";
      if Protocol.encode_completion accepted <> Ok first then
        failwith "async completion bytes are not the canonical encoding"
  | _ -> failwith "async completion retry recorded unexpected submissions"

(** A terminal native rejection closes both sides of the retained capability.
    The adapter must drop its lease so worker drain cannot wait forever, while
    the base handle must reject later calls instead of retrying a task token
    that Temporal has already discarded. *)
let test_async_terminal_rejection_closes_lease () =
  let supervisor = fake_supervisor () in
  let retained = ref None in
  let activity =
    Temporal.Activity.define_async ~name:"async_terminal_rejection"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
      (fun context () ->
        let handle = Temporal.Activity.Async_context.handle context in
        retained := Some handle;
        Temporal.Activity.Will_complete_async handle)
  in
  let token = Bytes.of_string "async-terminal-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"async_terminal_rejection"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register_async activity ] in
  expect_deferred (Worker.poll worker);
  supervisor.reject_next_async_completion_terminal := true;
  let handle = Option.get !retained in
  begin
    match Temporal.Activity.Async_handle.complete handle "discarded" with
    | Error error ->
        let view = Temporal.Error.view error in
        if not view.non_retryable then
          failwith "terminal async rejection was not marked non-retryable"
    | Ok () -> failwith "terminal async rejection was reported as accepted"
  end;
  if !(supervisor.async_leased) <> [] then
    failwith "terminal async rejection left a native async lease outstanding";
  begin
    match Temporal.Activity.Async_handle.complete handle "retry" with
    | Error _ -> ()
    | Ok () -> failwith "closed async handle accepted a stale retry"
  end;
  begin
    match Worker.drain worker with
    | Ok () -> ()
    | Error error ->
        failwith ("terminal async rejection blocked drain: " ^ error.message)
  end

(** Admits one deferred activity for native-status heartbeat failure tests.
    The callback count proves a retry never re-executes user activity code. *)
let heartbeat_status_fixture ~name ~token =
  let supervisor = fake_supervisor () in
  let calls = ref 0 and retained = ref None in
  let activity =
    Temporal.Activity.define_async ~name ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.string
      (fun context () ->
        incr calls;
        let handle = Temporal.Activity.Async_context.handle context in
        retained := Some handle;
        Temporal.Activity.Will_complete_async handle)
  in
  enqueue supervisor
    (start_task ~token ~activity_type:name
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register_async activity ] in
  expect_deferred (Worker.poll worker);
  (supervisor, worker, Option.get !retained, calls)

(** An async heartbeat crosses the supervisor as the bytes of its single
    encoder pass (#1001). The fake decodes them strictly, as Rust does, and
    each accepted heartbeat yields exactly one document: the canonical
    encoding of what was recorded. The document is an immutable snapshot, so
    mutating the caller's detail buffer afterwards cannot change it, and a
    heartbeat the encoder rejects never reaches the supervisor. *)
let test_async_heartbeat_single_encode () =
  let token = Bytes.of_string "\000async-heartbeat-encode\255" in
  let supervisor, worker, handle, _ =
    heartbeat_status_fixture ~name:"async_heartbeat_single_encode" ~token
  in
  let detail : Temporal.Payload.t =
    { metadata = [ ("encoding", "binary/plain") ];
      data = Bytes.of_string "progress" }
  in
  (match Temporal.Activity.Async_handle.heartbeat handle [ detail ] with
  | Ok () -> ()
  | Error error ->
      failwith ("async heartbeat failed: " ^ Temporal.Error.message error));
  Bytes.fill detail.data 0 (Bytes.length detail.data) 'x';
  (match !(supervisor.async_heartbeat_documents), !(supervisor.async_heartbeats) with
  | [ document ], [ recorded ] ->
      if
        Protocol.encode_async_heartbeat
          { Protocol.heartbeat = recorded; heartbeat_timeout = None }
        <> Ok document
      then failwith "async heartbeat bytes are not the canonical encoding";
      if not (Bytes.equal recorded.Protocol.task_token token) then
        failwith "async heartbeat changed the opaque token";
      (match Protocol.decode_async_heartbeat document with
      | Ok { Protocol.heartbeat = { Protocol.details = [ payload ]; _ }; _ }
        when Bytes.equal payload.Protocol.data (Bytes.of_string "progress") -> ()
      | _ -> failwith "async heartbeat document observed a later buffer mutation")
  | _ -> failwith "async heartbeat was not submitted exactly once");
  let duplicate : Temporal.Payload.t =
    { metadata = [ ("opaque", "one"); ("opaque", "two") ];
      data = Bytes.of_string "invalid" }
  in
  (match Temporal.Activity.Async_handle.heartbeat handle [ duplicate ] with
  | Error _ -> ()
  | Ok () -> failwith "encoder-rejected async heartbeat was accepted");
  if !(supervisor.async_heartbeat_calls) <> 1 then
    failwith "encoder-rejected async heartbeat reached the supervisor";
  (match Temporal.Activity.Async_handle.complete handle "done" with
  | Ok () -> ()
  | Error error -> failwith (Temporal.Error.message error));
  match Worker.drain worker with
  | Ok () -> ()
  | Error _ -> failwith "completed async heartbeat handle still blocked drain"

(** The retry cache is keyed on the handle's request identity, not on
    structural equality of the operation (#1001). A [Fail] without an explicit
    [error_type] and one with [error_type = "activity"] are structurally
    different but send the same failure type for category [Activity], so the
    handle admits the second as the exact retry of the first; it must resend
    the first submission's string physically rather than re-encode it. *)
let test_async_retry_reuses_bytes_for_equal_request_key () =
  let token = Bytes.of_string "async-retry-key-token" in
  let supervisor, worker, handle, calls =
    heartbeat_status_fixture ~name:"async_retry_request_key" ~token
  in
  supervisor.reject_next_async_completion := true;
  let implicit =
    Temporal.Error.make ~category:`Activity ~message:"external work failed" ()
  in
  (match Temporal.Activity.Async_handle.fail handle implicit with
  | Error error when not (Temporal.Error.view error).non_retryable -> ()
  | Error _ -> failwith "uncertain async failure was reported as non-retryable"
  | Ok () -> failwith "rejected async failure was reported as accepted");
  let explicit =
    Temporal.Error.make ~error_type:"activity" ~category:`Activity
      ~message:"external work failed" ()
  in
  (match Temporal.Activity.Async_handle.fail handle explicit with
  | Ok () -> ()
  | Error error ->
      failwith ("equal-key async failure retry was refused: "
        ^ Temporal.Error.message error));
  if !calls <> 1 then failwith "async failure retry reran the callback";
  (match !(supervisor.async_completion_documents), !(supervisor.async_completions) with
  | [ retry; first ], [ accepted ] ->
      if not (retry == first) then
        failwith "equal-key async retry submitted a re-encoded document";
      if Protocol.encode_completion accepted <> Ok first then
        failwith "async failure bytes are not the canonical encoding"
  | _ -> failwith "equal-key async retry recorded unexpected submissions");
  match Worker.drain worker with
  | Ok () when !(supervisor.async_leased) = [] -> ()
  | _ -> failwith "accepted async failure retry left completion debt"

(** A native [Connection] heartbeat result is uncertain, not terminal: the
    client did not consume the worker completion lease, and the server activity
    can still be live. The handle and lease survive, and because a heartbeat is
    superseded by the next one, a newer heartbeat and the terminal completion
    are both allowed immediately (#836). *)
let test_async_connection_heartbeat_keeps_handle () =
  let token = Bytes.of_string "async-connection-token" in
  let supervisor, worker, handle, calls =
    heartbeat_status_fixture ~name:"async_connection_heartbeat" ~token
  in
  let detail : Temporal.Payload.t =
    { metadata = [ ("encoding", "binary/plain") ];
      data = Bytes.of_string "50" }
  in
  supervisor.reject_next_async_heartbeat_connection := true;
  (match Temporal.Activity.Async_handle.heartbeat handle [ detail ] with
  | Error error ->
      let view = Temporal.Error.view error in
      if view.category <> `Bridge || view.non_retryable then
        failwith "uncertain heartbeat was classified as terminal"
  | Ok () -> failwith "connection failure was reported as an accepted heartbeat");
  if !calls <> 1 || !(supervisor.async_leased) <> [ token ] then
    failwith "connection failure reran callback or retired server activity";
  (match Worker.drain worker with
  | Error { code = "outstanding_async_leases"; retryable = true; _ } -> ()
  | _ -> failwith "connection failure let worker drain an active async lease");
  let newer_detail : Temporal.Payload.t =
    { detail with data = Bytes.of_string "60" }
  in
  (match Temporal.Activity.Async_handle.heartbeat handle [ newer_detail ] with
  | Ok () -> ()
  | Error error ->
      failwith ("uncertain heartbeat blocked a newer heartbeat: "
        ^ Temporal.Error.message error));
  supervisor.reject_next_async_heartbeat_connection := true;
  (match Temporal.Activity.Async_handle.heartbeat handle [ newer_detail ] with
  | Error _ -> ()
  | Ok () -> failwith "second connection failure was reported as accepted");
  (match Temporal.Activity.Async_handle.complete handle "done" with
  | Ok () -> ()
  | Error error ->
      failwith ("uncertain heartbeat blocked completion: "
        ^ Temporal.Error.message error));
  if !calls <> 1 || !(supervisor.async_heartbeat_calls) <> 3
     || List.length !(supervisor.async_heartbeats) <> 1
     || !(supervisor.async_completion_calls) <> 1
     || List.length !(supervisor.async_completions) <> 1
     || !(supervisor.async_leased) <> [] then
    failwith "heartbeat recovery did not retire exactly one async lease";
  (match Worker.drain worker with
  | Ok () -> ()
  | Error _ -> failwith "completed async handle still blocked drain")

(** A definitive server rejection of a terminal request (for example cancel
    without a cancel request) leaves the activity live:
    the handle stays usable, the lease stays registered, and a different
    terminal operation can still finish the activity (#821). *)
let test_async_rejected_completion_keeps_live_handle () =
  let token = Bytes.of_string "async-rejected-cancel-token" in
  let supervisor, worker, handle, calls =
    heartbeat_status_fixture ~name:"async_rejected_cancel" ~token
  in
  supervisor.reject_next_async_completion_rejected := true;
  (match Temporal.Activity.Async_handle.cancel handle [] with
  | Error error when (Temporal.Error.view error).non_retryable -> ()
  | Error _ -> failwith "rejected cancellation was marked retryable"
  | Ok () -> failwith "rejected cancellation was reported as accepted");
  if !calls <> 1 || !(supervisor.async_leased) <> [ token ] then
    failwith "rejected cancellation retired the live async activity";
  (match Worker.drain worker with
  | Error { code = "outstanding_async_leases"; retryable = true; _ } -> ()
  | _ -> failwith "rejected cancellation dropped the adapter async lease");
  (match Temporal.Activity.Async_handle.heartbeat handle [] with
  | Ok () -> ()
  | Error error ->
      failwith ("rejected cancellation blocked a heartbeat: "
        ^ Temporal.Error.message error));
  let failure =
    Temporal.Error.make ~category:`Activity ~message:"external work failed" ()
  in
  (match Temporal.Activity.Async_handle.fail handle failure with
  | Ok () -> ()
  | Error error ->
      failwith ("fail after rejected cancellation was blocked: "
        ^ Temporal.Error.message error));
  (match !(supervisor.async_completions) with
  | [ { Protocol.result = Protocol.Failed _; _ } ] -> ()
  | _ -> failwith "fail after rejected cancellation was not submitted once");
  if !(supervisor.async_completion_calls) <> 2 || !(supervisor.async_leased) <> []
  then failwith "rejected cancellation recovery did not retire the lease";
  match Worker.drain worker with
  | Ok () -> ()
  | Error _ -> failwith "failed async handle still blocked drain"

(** A definitive server rejection discards only that heartbeat request. The
    activity token may still be live, so a corrected heartbeat and one terminal
    completion must remain possible without rerunning the callback. *)
let test_async_rejected_heartbeat_keeps_live_handle () =
  let token = Bytes.of_string "async-rejected-token" in
  let supervisor, worker, handle, calls =
    heartbeat_status_fixture ~name:"async_rejected_heartbeat" ~token
  in
  let detail text : Temporal.Payload.t =
    { metadata = []; data = Bytes.of_string text }
  in
  supervisor.reject_next_async_heartbeat_rejected := true;
  (match Temporal.Activity.Async_handle.heartbeat handle [ detail "invalid" ] with
  | Error error when (Temporal.Error.view error).non_retryable -> ()
  | Error _ -> failwith "rejected heartbeat was marked retryable"
  | Ok () -> failwith "rejected heartbeat was reported as accepted");
  if !calls <> 1 || !(supervisor.async_leased) <> [ token ] then
    failwith "rejected heartbeat retired the live async activity";
  (match Worker.drain worker with
  | Error { code = "outstanding_async_leases"; retryable = true; _ } -> ()
  | _ -> failwith "rejected heartbeat let worker drain a live async lease");
  (match Temporal.Activity.Async_handle.heartbeat handle [ detail "corrected" ] with
  | Ok () -> ()
  | Error error ->
      failwith ("corrected heartbeat was blocked: " ^ Temporal.Error.message error));
  (match Temporal.Activity.Async_handle.complete handle "done" with
  | Ok () -> ()
  | Error error ->
      failwith ("completion after rejected heartbeat failed: "
        ^ Temporal.Error.message error));
  if !calls <> 1 || !(supervisor.async_heartbeat_calls) <> 2
     || List.length !(supervisor.async_heartbeats) <> 1
     || !(supervisor.async_completion_calls) <> 1
     || !(supervisor.async_leased) <> [] then
    failwith "rejected heartbeat recovery did not complete the live activity";
  match Worker.drain worker with
  | Ok () -> ()
  | Error _ -> failwith "completed rejected-heartbeat handle blocked drain"

(** A native [Invalid_state] heartbeat means Temporal has discarded the token.
    The fake retires its server ledger before returning that terminal response,
    and the adapter must close the public handle and stop tracking the lease. *)
let test_async_not_found_heartbeat_closes_handle () =
  let token = Bytes.of_string "async-not-found-token" in
  let supervisor, worker, handle, _ =
    heartbeat_status_fixture ~name:"async_not_found_heartbeat" ~token
  in
  supervisor.reject_next_async_heartbeat_terminal := true;
  (match Temporal.Activity.Async_handle.heartbeat handle [] with
  | Error error when (Temporal.Error.view error).non_retryable -> ()
  | Error _ -> failwith "not-found heartbeat was treated as uncertain"
  | Ok () -> failwith "not-found heartbeat was reported as accepted");
  (match Temporal.Activity.Async_handle.heartbeat handle [] with
  | Error _ -> ()
  | Ok () -> failwith "terminal heartbeat left its handle active");
  (match Temporal.Activity.Async_handle.complete handle "late" with
  | Error _ -> ()
  | Ok () -> failwith "terminal heartbeat allowed a late completion");
  if !(supervisor.async_heartbeat_calls) <> 1
     || !(supervisor.async_completion_calls) <> 0
     || !(supervisor.async_leased) <> [] then
    failwith "terminal heartbeat did not retire its client capability";
  match Worker.drain worker with
  | Ok () -> ()
  | Error _ -> failwith "terminal heartbeat left an adapter lease outstanding"

(** Heartbeat retries retain the request while terminal cancellation preserves
    its detail payloads. This checks the non-terminal and terminal state paths
    independently on one admitted handle. *)
let test_async_heartbeat_and_cancel () =
  let supervisor = fake_supervisor () in
  let retained = ref None in
  let activity =
    Temporal.Activity.define_async ~name:"async_heartbeat_cancel"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
      (fun context () ->
        retained := Some (Temporal.Activity.Async_context.handle context);
        Temporal.Activity.Will_complete_async (Option.get !retained))
  in
  let token = Bytes.of_string "async-cancel-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"async_heartbeat_cancel"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register_async activity ] in
  expect_deferred (Worker.poll worker);
  let handle = Option.get !retained in
  let detail : Temporal.Payload.t =
    { metadata = [ ("encoding", "binary/plain") ]; data = Bytes.of_string "progress" }
  in
  supervisor.reject_next_async_heartbeat := true;
  begin
    match Temporal.Activity.Async_handle.heartbeat handle [ detail ] with
    | Error _ -> ()
    | Ok () -> failwith "rejected async heartbeat was reported as accepted"
  end;
  begin
    match Temporal.Activity.Async_handle.heartbeat handle [ detail ] with
    | Ok () -> ()
    | Error error ->
        failwith ("async heartbeat retry failed: " ^ Temporal.Error.message error)
  end;
  let cancel_detail : Temporal.Payload.t =
    { metadata = [ ("encoding", "binary/plain") ]; data = Bytes.of_string "reason" }
  in
  begin
    match Temporal.Activity.Async_handle.cancel handle [ cancel_detail ] with
    | Ok () -> ()
    | Error error ->
        failwith ("async cancellation failed: " ^ Temporal.Error.message error)
  end;
  if !(supervisor.async_leased) <> [] then
    failwith "async cancellation did not retire the client lease";
  begin
    match !(supervisor.async_completions) with
    | [ { Protocol.result = Protocol.Cancelled { info = Protocol.Canceled { details; _ }; _ }; _ } ] ->
        begin
          match details with
          | [ detail ] when Bytes.equal detail.Protocol.data (Bytes.of_string "reason") -> ()
          | _ -> failwith "async cancellation discarded detail payloads"
        end
    | _ -> failwith "async cancellation did not submit one canceled completion"
  end

(** Structured failure details remain attached to a late failure completion and
    are not converted into an exception or an untyped string. *)
let test_async_failure () =
  let supervisor = fake_supervisor () in
  let retained = ref None in
  let activity =
    Temporal.Activity.define_async ~name:"async_failure"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
      (fun context () ->
        let handle = Temporal.Activity.Async_context.handle context in
        retained := Some handle;
        Temporal.Activity.Will_complete_async handle)
  in
  let token = Bytes.of_string "async-failure-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"async_failure"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register_async activity ] in
  expect_deferred (Worker.poll worker);
  let detail : Temporal.Payload.t =
    { metadata = [ ("encoding", "binary/plain") ]; data = Bytes.of_string "failure" }
  in
  let failure =
    Temporal.Error.make ~category:`Activity ~details:[ detail ]
      ~message:"late failure" ()
  in
  begin
    match Temporal.Activity.Async_handle.fail (Option.get !retained) failure with
    | Ok () -> ()
    | Error error ->
        failwith ("async failure submission failed: " ^ Temporal.Error.message error)
  end;
  begin
    match !(supervisor.async_completions) with
    | [ { Protocol.result = Protocol.Failed { info = Protocol.Application { details; _ }; _ }; _ } ] ->
        begin
          match details with
          | [ detail ] when Bytes.equal detail.Protocol.data (Bytes.of_string "failure") -> ()
          | _ -> failwith "async failure dropped structured details"
        end
    | _ -> failwith "async failure did not submit one failed completion"
  end

(** An asynchronous callback reads the start task's metadata through
    [Async_context.info], including the effective timeouts. Sub-millisecond
    timeouts are rounded up instead of rejecting the task, so exposing them
    adds no new failure path (#792). This includes a 500 microsecond heartbeat
    timeout: only synchronous contexts need its exact value, so the async
    path must not apply that check. *)
let test_async_context_info () =
  let supervisor = fake_supervisor () in
  let observed = ref None in
  let activity =
    Temporal.Activity.define_async ~name:"async_info"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
      (fun context () ->
        observed := Some (Temporal.Activity.Async_context.info context);
        Temporal.Activity.Completed ())
  in
  let token = Bytes.of_string "async-info-token" in
  let task =
    start_task ~token ~activity_type:"async_info"
      ~input:[ encode_input Temporal.Codec.unit () ]
  in
  let task =
    match task.variant with
    | Protocol.Start start ->
        {
          task with
          variant =
            Protocol.Start
              {
                start with
                attempt = 2L;
                schedule_to_close_timeout = Some { seconds = 60L; nanoseconds = 1 };
                start_to_close_timeout = Some { seconds = 0L; nanoseconds = 500_000 };
                heartbeat_timeout = Some { seconds = 0L; nanoseconds = 500_000 };
              };
        }
    | _ -> failwith "fixture did not build a start task"
  in
  enqueue supervisor task;
  let worker = worker supervisor [ Adapter.register_async activity ] in
  begin
    match Worker.poll worker with
    | Ok (Raw_adapter.Completed { kind = Raw_adapter.Succeeded; _ }) -> ()
    | _ -> failwith "async info activity did not complete synchronously"
  end;
  let module Info = Temporal.Activity.Info in
  let ms = Option.map Temporal.Duration.to_ms in
  match !observed with
  | None -> failwith "async info callback did not run"
  | Some (Error error) ->
      failwith ("async context info failed: " ^ Temporal.Error.message error)
  | Some (Ok info) ->
      if Info.namespace info <> "default" then failwith "async info namespace";
      if Info.workflow info
         <> { Info.workflow_id = "async-workflow-1"; run_id = "async-run-1";
              workflow_type = "async_test_workflow" }
      then failwith "async info workflow";
      if Info.activity_id info <> "async-activity-1" then failwith "async info id";
      if Info.activity_type info <> "async_info" then failwith "async info type";
      if Info.attempt info <> 2 then failwith "async info attempt";
      if Info.is_local info then failwith "async info locality";
      if ms (Info.schedule_to_close_timeout info) <> Some 60_001L then
        failwith "schedule-to-close timeout was not rounded up";
      if ms (Info.start_to_close_timeout info) <> Some 1L then
        failwith "sub-millisecond start-to-close timeout was not rounded up";
      if ms (Info.heartbeat_timeout info) <> Some 1L then
        failwith "sub-millisecond heartbeat timeout was not rounded up"

(** The largest valid protobuf duration (315,576,000,000 s plus 999,999,999 ns)
    rounds up one millisecond past the public [Duration.t] maximum. Every
    timeout accessor must clamp it to 315,576,000,000,999 ms instead of
    raising [Invalid_argument] from [Duration.of_ms]. *)
let test_async_context_info_maximum_timeouts () =
  let supervisor = fake_supervisor () in
  let observed = ref None in
  let activity =
    Temporal.Activity.define_async ~name:"async_info_max"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
      (fun context () ->
        observed := Some (Temporal.Activity.Async_context.info context);
        Temporal.Activity.Completed ())
  in
  let token = Bytes.of_string "async-info-max-token" in
  let task =
    start_task ~token ~activity_type:"async_info_max"
      ~input:[ encode_input Temporal.Codec.unit () ]
  in
  let maximum : Protocol.duration =
    { seconds = 315_576_000_000L; nanoseconds = 999_999_999 }
  in
  let task =
    match task.variant with
    | Protocol.Start start ->
        {
          task with
          variant =
            Protocol.Start
              {
                start with
                schedule_to_close_timeout = Some maximum;
                start_to_close_timeout = Some maximum;
                heartbeat_timeout = Some maximum;
              };
        }
    | _ -> failwith "fixture did not build a start task"
  in
  enqueue supervisor task;
  let worker = worker supervisor [ Adapter.register_async activity ] in
  begin
    match Worker.poll worker with
    | Ok (Raw_adapter.Completed { kind = Raw_adapter.Succeeded; _ }) -> ()
    | _ -> failwith "maximum-timeout async activity did not complete"
  end;
  let module Info = Temporal.Activity.Info in
  let expected = Some 315_576_000_000_999L in
  let ms = Option.map Temporal.Duration.to_ms in
  match !observed with
  | None -> failwith "maximum-timeout async callback did not run"
  | Some (Error error) ->
      failwith ("async context info failed: " ^ Temporal.Error.message error)
  | Some (Ok info) ->
      if ms (Info.schedule_to_close_timeout info) <> expected then
        failwith "maximum schedule-to-close timeout was not clamped";
      if ms (Info.start_to_close_timeout info) <> expected then
        failwith "maximum start-to-close timeout was not clamped";
      if ms (Info.heartbeat_timeout info) <> expected then
        failwith "maximum heartbeat timeout was not clamped"

(** [drain] refuses to claim shutdown while an async capability remains, while
    [discard] closes the retained handle only after terminal native cleanup. *)
let test_async_drain_and_discard () =
  let supervisor = fake_supervisor () in
  let retained = ref None in
  let activity =
    Temporal.Activity.define_async ~name:"async_drain_discard"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
      (fun context () ->
        let handle = Temporal.Activity.Async_context.handle context in
        retained := Some handle;
        Temporal.Activity.Will_complete_async handle)
  in
  let token = Bytes.of_string "async-drain-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"async_drain_discard"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register_async activity ] in
  expect_deferred (Worker.poll worker);
  begin
    match Worker.drain worker with
    | Error { code = "outstanding_async_leases"; retryable = true; _ } -> ()
    | Error { code = "outstanding_async_leases"; retryable = false; _ } ->
        failwith "outstanding async lease was incorrectly marked terminal"
    | Error error ->
        failwith ("async drain used the wrong diagnostic: " ^ error.code)
    | Ok () -> failwith "async drain ignored an admitted completion handle"
  end;
  let handle = Option.get !retained in
  begin
    match Temporal.Activity.Async_handle.complete handle () with
    | Ok () -> ()
    | Error error ->
        failwith
          ("retryable async drain did not preserve the handle: "
          ^ Temporal.Error.message error)
  end;
  begin
    match Worker.drain worker with
    | Ok () -> ()
    | Error error ->
        failwith ("drain after async completion failed: " ^ error.message)
  end;

  (* A later admitted handle still exercises the terminal discard path. *)
  let second_token = Bytes.of_string "async-discard-token" in
  enqueue supervisor
    (start_task ~token:second_token ~activity_type:"async_drain_discard"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  expect_deferred (Worker.poll worker);
  let second_handle = Option.get !retained in
  Worker.discard worker;
  begin
    match Temporal.Activity.Async_handle.complete second_handle () with
    | Error _ -> ()
    | Ok () -> failwith "discarded async handle remained usable"
  end

(** Builds a remote start task carrying explicit server timeouts, given in
    whole seconds; [None] leaves a timeout unset. *)
let start_task_with_timeouts ?start_to_close ?schedule_to_close ?heartbeat
    ~token ~activity_type () : Protocol.task =
  let duration seconds = { Protocol.seconds = Int64.of_int seconds; nanoseconds = 0 } in
  match
    start_task ~token ~activity_type ~input:[ encode_input Temporal.Codec.unit () ]
  with
  | { variant = Protocol.Start start; _ } as task ->
      {
        task with
        variant =
          Protocol.Start
            {
              start with
              start_to_close_timeout = Option.map duration start_to_close;
              schedule_to_close_timeout = Option.map duration schedule_to_close;
              heartbeat_timeout = Option.map duration heartbeat;
            };
      }
  | _ -> assert false

(** An async activity whose callback hands its handle to [retained] and
    never finishes it, like the leaking completer described in #1043. *)
let leaked_async_activity retained =
  Temporal.Activity.define_async ~name:"leaked_async" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.unit (fun context () ->
      let handle = Temporal.Activity.Async_context.handle context in
      retained := Some handle;
      Temporal.Activity.Will_complete_async handle)

(** Requires [drain] to report the retryable outstanding-lease diagnostic. *)
let expect_outstanding label worker =
  match Worker.drain worker with
  | Error { code = "outstanding_async_leases"; retryable = true; _ } -> ()
  | Ok () -> failwith (label ^ ": a live async lease was expired early")
  | Error error -> failwith (label ^ ": unexpected drain error " ^ error.code)

(** An admitted async lease that application code never finishes expires
    once the server must have timed its attempt out (#1043): after the
    shorter close timeout, or twice the heartbeat timeout since the last
    async heartbeat (a throttled heartbeat's trailing send follows within
    one throttle interval, #1025), plus a few seconds of slack, all measured
    from admission on
    the worker's clock. An expired lease no longer holds [drain] (and so
    shutdown), is not counted as outstanding, closes its handle with a
    non-retryable error, and sends nothing to the server. A lease whose task
    reports no timeout never expires, and admitting a new lease retires
    expired ones so a leaking worker does not accumulate them. *)
let test_async_lease_expiry () =
  let clock = ref 1_000. in
  let make_worker supervisor activities =
    match
      Worker.create_with_clock
        ~now:(fun () -> !clock)
        ~supervisor ~activities
        ~worker_shutting_down:(fun () -> false)
    with
    | Ok worker -> worker
    | Error (error : Raw_adapter.error_view) -> failwith error.message
  in
  let expect_closed label handle =
    match Temporal.Activity.Async_handle.complete handle () with
    | Error error when (Temporal.Error.view error).non_retryable -> ()
    | Error _ -> failwith (label ^ ": an expired handle returned a retryable error")
    | Ok () -> failwith (label ^ ": an expired handle still completed")
  in
  (* The shorter close timeout bounds the attempt; slack is respected. *)
  List.iter
    (fun (label, task) ->
      clock := 1_000.;
      let supervisor = fake_supervisor () and retained = ref None in
      enqueue supervisor task;
      let worker =
        make_worker supervisor [ Adapter.register_async (leaked_async_activity retained) ]
      in
      expect_deferred (Worker.poll worker);
      clock := 1_000. +. 30. +. 4.;
      expect_outstanding label worker;
      if Worker.outstanding_async_leases worker <> Some 1 then
        failwith (label ^ ": the live lease was not counted");
      clock := 1_000. +. 30. +. 6.;
      if Worker.outstanding_async_leases worker <> Some 0 then
        failwith (label ^ ": an expired lease was still counted");
      (match Worker.drain worker with
      | Ok () -> ()
      | Error error -> failwith (label ^ ": an expired lease held drain: " ^ error.code));
      expect_closed label (Option.get !retained);
      if !(supervisor.async_completion_calls) <> 0 then
        failwith (label ^ ": expiry contacted the server"))
    [
      ( "start-to-close",
        start_task_with_timeouts ~start_to_close:30 ~schedule_to_close:600
          ~token:(Bytes.of_string "expiry-stc") ~activity_type:"leaked_async" () );
      ( "schedule-to-close",
        start_task_with_timeouts ~start_to_close:600 ~schedule_to_close:30
          ~token:(Bytes.of_string "expiry-s2c") ~activity_type:"leaked_async" () );
    ];
  (* A successful async heartbeat restarts the heartbeat window. *)
  clock := 0.;
  let supervisor = fake_supervisor () and retained = ref None in
  enqueue supervisor
    (start_task_with_timeouts ~start_to_close:3_600 ~heartbeat:10
       ~token:(Bytes.of_string "expiry-heartbeat") ~activity_type:"leaked_async" ());
  let worker =
    make_worker supervisor [ Adapter.register_async (leaked_async_activity retained) ]
  in
  expect_deferred (Worker.poll worker);
  let handle = Option.get !retained in
  clock := 8.;
  (match Temporal.Activity.Async_handle.heartbeat handle [] with
  | Ok () -> ()
  | Error error -> failwith ("async heartbeat failed: " ^ Temporal.Error.message error));
  (* The heartbeat at 8 s defers expiry to 8 + 2 * 10 + 5 s of slack. *)
  clock := 32.;
  expect_outstanding "heartbeat" worker;
  clock := 34.;
  (match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("a heartbeat-expired lease held drain: " ^ error.code));
  expect_closed "heartbeat" handle;
  (* Without any reported timeout the lease is kept indefinitely. *)
  clock := 0.;
  let supervisor = fake_supervisor () and retained = ref None in
  enqueue supervisor
    (start_task_with_timeouts ~token:(Bytes.of_string "expiry-none")
       ~activity_type:"leaked_async" ());
  let worker =
    make_worker supervisor [ Adapter.register_async (leaked_async_activity retained) ]
  in
  expect_deferred (Worker.poll worker);
  clock := 1e9;
  expect_outstanding "no timeout" worker;
  (match Temporal.Activity.Async_handle.complete (Option.get !retained) () with
  | Ok () -> ()
  | Error error -> failwith ("an unexpired handle failed: " ^ Temporal.Error.message error));
  (* Admission retires expired leases, bounding a leaking worker. *)
  clock := 0.;
  let supervisor = fake_supervisor () and retained = ref None in
  enqueue supervisor
    (start_task_with_timeouts ~start_to_close:10
       ~token:(Bytes.of_string "expiry-first") ~activity_type:"leaked_async" ());
  let worker =
    make_worker supervisor [ Adapter.register_async (leaked_async_activity retained) ]
  in
  expect_deferred (Worker.poll worker);
  let first = Option.get !retained in
  clock := 100.;
  enqueue supervisor
    (start_task_with_timeouts ~start_to_close:10
       ~token:(Bytes.of_string "expiry-second") ~activity_type:"leaked_async" ());
  expect_deferred (Worker.poll worker);
  expect_closed "admission" first;
  if Worker.outstanding_async_leases worker <> Some 1 then
    failwith "admitting a lease did not retire the expired one"

(** An async heartbeat carries its attempt's exact heartbeat timeout, from
    which the bridge derives its throttle interval (#1025). A cancellation,
    pause, or reset the bridge reports (from the token's latest send, which
    may precede a throttled call) is returned as a typed [`Cancelled] error,
    a pause or reset with the [error_type] a running attempt's failure uses;
    the heartbeat was recorded, so it still
    renews the lease's heartbeat window, and the handle stays live for the
    terminal operation that must follow. *)
let test_async_heartbeat_carries_timeout_and_activity_state () =
  let clock = ref 0. in
  let supervisor = fake_supervisor () and retained = ref None in
  enqueue supervisor
    (start_task_with_timeouts ~start_to_close:3_600 ~heartbeat:10
       ~token:(Bytes.of_string "heartbeat-state") ~activity_type:"leaked_async" ());
  let worker =
    match
      Worker.create_with_clock
        ~now:(fun () -> !clock)
        ~supervisor
        ~activities:[ Adapter.register_async (leaked_async_activity retained) ]
        ~worker_shutting_down:(fun () -> false)
    with
    | Ok worker -> worker
    | Error (error : Raw_adapter.error_view) -> failwith error.message
  in
  expect_deferred (Worker.poll worker);
  let handle = Option.get !retained in
  (match Temporal.Activity.Async_handle.heartbeat handle [] with
  | Ok () -> ()
  | Error error -> failwith ("async heartbeat failed: " ^ Temporal.Error.message error));
  (match !(supervisor.async_heartbeat_timeouts) with
  | [ Some { Protocol.seconds = 10L; nanoseconds = 0 } ] -> ()
  | _ -> failwith "async heartbeat did not carry the attempt's heartbeat timeout");
  List.iteri
    (fun index (label, response, error_type, non_retryable) ->
      supervisor.async_heartbeat_response := response;
      clock := float_of_int (20 * (index + 1));
      match Temporal.Activity.Async_handle.heartbeat handle [] with
      | Ok () -> failwith (label ^ ": the activity state was not reported")
      | Error error ->
          let view = Temporal.Error.view error in
          if view.category <> `Cancelled then
            failwith (label ^ ": wrong error category");
          if view.error_type <> error_type then
            failwith (label ^ ": wrong error type");
          if view.non_retryable <> non_retryable then
            failwith (label ^ ": wrong retryability"))
    [
      ("cancel", Protocol.Heartbeat_cancel_requested, None, true);
      ("pause", Protocol.Heartbeat_paused, Some "ActivityPause", false);
      ("reset", Protocol.Heartbeat_reset, Some "ActivityReset", false);
    ];
  (* The last heartbeat, at 60 s, renewed the window to 60 + 2 * 10 + 5 s. *)
  clock := 84.;
  expect_outstanding "activity state" worker;
  supervisor.async_heartbeat_response := Protocol.Heartbeat_recorded;
  (match Temporal.Activity.Async_handle.cancel handle [] with
  | Ok () -> ()
  | Error error ->
      failwith ("a handle that reported cancellation could not cancel: "
        ^ Temporal.Error.message error));
  match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("the cancelled lease held drain: " ^ error.code)

(** A handle retained from a synchronously completed attempt must not be
    attachable to the next attempt. Its submit callback still captures the old
    token, so accepting it would orphan the current asynchronous lease. *)
let test_stale_handle_rejected () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let stale = ref None in
  let activity =
    Temporal.Activity.define_async ~name:"async_stale_handle"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
      (fun context () ->
        incr calls;
        let current = Temporal.Activity.Async_context.handle context in
        match !stale with
        | None ->
            stale := Some current;
            Temporal.Activity.Completed "finished immediately"
        | Some old -> Temporal.Activity.Will_complete_async old)
  in
  let first_token = Bytes.of_string "async-stale-first" in
  let second_token = Bytes.of_string "async-stale-second" in
  enqueue supervisor
    (start_task ~token:first_token ~activity_type:"async_stale_handle"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  enqueue supervisor
    (start_task ~token:second_token ~activity_type:"async_stale_handle"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register_async activity ] in
  begin
    match Worker.poll worker with
    | Ok (Raw_adapter.Completed { kind = Raw_adapter.Succeeded; _ }) -> ()
    | _ -> failwith "first stale-handle fixture did not complete synchronously"
  end;
  expect_rejected "activity" (Worker.poll worker);
  if !calls <> 2 then failwith "stale-handle fixture reran an activity unexpectedly";
  if !(supervisor.leased) <> [] then
    failwith "stale-handle rejection left the current worker lease outstanding";
  if !(supervisor.async_leased) <> [] then
    failwith "stale handle was admitted as a current asynchronous lease";
  begin
    match Temporal.Activity.Async_handle.complete (Option.get !stale) "late" with
    | Error _ -> ()
    | Ok () -> failwith "stale dormant handle became usable after rejection"
  end

(** Once the owning worker is stopping (#1157), an accepted heartbeat is
    still submitted but returns the retryable ["WorkerShutdown"] error, a
    heartbeat whose submission reports an activity state keeps that error,
    the flag is readable through the handle, and the completer can still
    fail the handle with the shutdown error so Temporal retries the attempt.
    A handle created without a reader never reports shutdown. *)
let test_worker_shutdown_reaches_async_handle () =
  let stopping = ref false in
  let submitted = ref [] in
  let paused = ref false in
  let payload = Temporal_base.Payload.{ metadata = []; data = Bytes.empty } in
  let handle =
    Base_async.create ()
      ~worker_shutting_down:(fun () -> !stopping)
      ~submit:(fun operation ->
        submitted := operation :: !submitted;
        if !paused then
          Error
            (Base_async.Activity_state
               (Temporal_base.Error.make ~category:`Cancelled ~error_type:"ActivityPause"
                  ~message:"paused" ()))
        else Ok ())
      ~encode_output:(fun _ -> Ok payload)
  in
  (match Base_async.prepare_handoff ~expected:handle handle with
  | Ok () -> ()
  | Error _ -> failwith "shutdown fixture could not reserve its handoff");
  (match Base_async.activate handle with
  | Ok () -> ()
  | Error _ -> failwith "shutdown fixture could not activate");
  if Base_async.worker_shutting_down handle then
    failwith "a running worker was reported as shutting down";
  (match Base_async.heartbeat handle [] with
  | Ok () -> ()
  | Error _ -> failwith "a heartbeat before shutdown failed");
  stopping := true;
  if not (Base_async.worker_shutting_down handle) then
    failwith "a stopping worker was not reported through the handle";
  let shutdown_error =
    match Base_async.heartbeat handle [] with
    | Error error ->
        let view = Temporal_base.Error.view error in
        if view.non_retryable || view.category <> `Cancelled
           || view.error_type <> Some Base_async.worker_shutdown_error_type
        then failwith "the shutdown heartbeat error has the wrong shape";
        error
    | Ok () -> failwith "a heartbeat during shutdown returned Ok"
  in
  if List.length !submitted <> 2 then
    failwith "the heartbeat during shutdown was not submitted";
  paused := true;
  (match Base_async.heartbeat handle [] with
  | Error error when Temporal_base.Error.error_type error = Some "ActivityPause" -> ()
  | Error _ | Ok () -> failwith "an activity-state error did not take precedence");
  paused := false;
  (match Base_async.fail handle shutdown_error with
  | Ok () -> ()
  | Error _ -> failwith "the completer could not fail with the shutdown error");
  (match !submitted with
  | Base_async.Fail error :: _
    when Temporal_base.Error.error_type error = Some Base_async.worker_shutdown_error_type ->
      ()
  | _ -> failwith "the shutdown failure was not submitted");
  let unowned =
    Base_async.create () ~submit:(fun _ -> Ok ()) ~encode_output:(fun _ -> Ok payload)
  in
  if Base_async.worker_shutting_down unowned then
    failwith "a handle without a worker reported shutdown"

(** The base state machine rejects use before activation and keeps its terminal
    state after one accepted completion without requiring a native supervisor. *)
let test_base_state_machine () =
  let submitted = ref 0 in
  let payload = Temporal_base.Payload.{ metadata = []; data = Bytes.empty } in
  let handle =
    Base_async.create ()
      ~submit:(fun _operation -> incr submitted; Ok ())
      ~encode_output:(fun _ -> Ok payload)
  in
  begin
    match Base_async.complete handle () with
    | Error error when not (Temporal_base.Error.view error).non_retryable -> ()
    | Error _ -> failwith "dormant base async handle rejected permanently (#766)"
    | Ok () -> failwith "dormant base async handle submitted an operation"
  end;
  if !submitted <> 0 then failwith "dormant base handle entered its submit callback";
  begin
    match Base_async.prepare_handoff ~expected:handle handle with
    | Ok () -> ()
    | Error _ -> failwith "dormant base async handle could not reserve a handoff"
  end;
  (* A reserved handoff is not dormant, so the adapter's cleanup leaves it
     usable for activation. *)
  Base_async.close_if_dormant handle;
  begin
    match Base_async.complete handle () with
    | Error error when not (Temporal_base.Error.view error).non_retryable -> ()
    | Error _ -> failwith "handoff-pending base handle rejected permanently (#766)"
    | Ok () -> failwith "handoff-pending base handle submitted an operation"
  end;
  let abandoned =
    Base_async.create ()
      ~submit:(fun _operation -> incr submitted; Ok ())
      ~encode_output:(fun _ -> Ok payload)
  in
  Base_async.close_if_dormant abandoned;
  begin
    match Base_async.complete abandoned () with
    | Error error when (Temporal_base.Error.view error).non_retryable -> ()
    | Error _ -> failwith "closed dormant base handle remained retryable"
    | Ok () -> failwith "closed dormant base handle submitted an operation"
  end;
  if !submitted <> 0 then failwith "inactive base handle entered its submit callback";
  begin
    match Base_async.activate handle with
    | Ok () -> ()
    | Error _ -> failwith "base async handle could not activate"
  end;
  begin
    match Base_async.complete handle () with
    | Ok () -> ()
    | Error _ -> failwith "active base async handle could not complete"
  end;
  if !submitted <> 1 then failwith "base async handle submitted an unexpected count";
  begin
    match Base_async.complete handle () with
    | Error _ -> ()
    | Ok () -> failwith "terminal base async handle accepted another completion"
  end

(** List boundaries are part of the async idempotency key. Without an explicit
    payload count, three one-field payloads can encode to the same byte stream
    as one payload containing one metadata pair and a data field; the second
    request would then incorrectly retry the first operation. *)
let test_operation_key_boundaries () =
  let submitted = ref 0 in
  let failure =
    Temporal_base.Error.make ~category:`Bridge ~message:"transport unavailable" ()
  in
  let handle =
    Base_async.create ()
      ~submit:(fun _operation ->
        incr submitted;
        Error (Base_async.Retryable_submission failure))
      ~encode_output:(fun _ ->
        Ok Temporal_base.Payload.{ metadata = []; data = Bytes.empty })
  in
  let payload data = Temporal_base.Payload.{ metadata = []; data = Bytes.of_string data } in
  let three = [ payload "a"; payload "b"; payload "c" ] in
  let one =
    Temporal_base.Payload.
      { metadata = [ ("a", "b") ]; data = Bytes.of_string "c" }
  in
  ignore (Base_async.activate handle);
  (match Base_async.cancel handle three with
  | Error _ -> ()
  | Ok () -> failwith "fake transport unexpectedly accepted first cancellation");
  (match Base_async.cancel handle [ one ] with
  | Error _ -> ()
  | Ok () -> failwith "different cancellation operation was accepted");
  if !submitted <> 1 then
    failwith "different payload-list shape retried the retained operation"

(** A failed preflight on a later retry cannot erase an earlier uncertain
    submission. A different operation stays blocked until that request settles. *)
let test_local_rejection_preserves_earlier_uncertainty () =
  let attempts = ref 0 in
  let error = Temporal_base.Error.make ~category:`Bridge ~message:"unavailable" () in
  let handle = Base_async.create ()
      ~encode_output:(fun text -> Ok Temporal_base.Payload.
        { metadata = []; data = Bytes.of_string text })
      ~submit:(fun _ ->
        incr attempts;
        match !attempts with
        | 1 -> Error (Base_async.Retryable_submission error)
        | 2 -> Error (Base_async.Not_submitted error)
        | _ -> Ok ()) in
  ignore (Base_async.activate handle);
  (match Base_async.complete handle "original" with
  | Error _ -> ()
  | Ok () -> failwith "uncertain submission was accepted");
  (match Base_async.complete handle "original" with
  | Error _ -> ()
  | Ok () -> failwith "locally rejected retry was accepted");
  (match Base_async.complete handle "different" with
  | Error _ -> ()
  | Ok () -> failwith "local rejection erased earlier submission uncertainty");
  if !attempts <> 2 then failwith "conflicting request crossed the submit boundary";
  match Base_async.complete handle "original" with
  | Ok () when !attempts = 3 -> ()
  | _ -> failwith "original uncertain request could not be retried"

(** A definitive native rejection of the exact retry answers the earlier
    uncertain terminal request too, so it releases the key: the handle stays
    live and a different terminal operation may run (#821). Before that
    answer, a different operation stays blocked. *)
let test_native_rejection_releases_earlier_uncertainty () =
  let attempts = ref 0 in
  let error = Temporal_base.Error.make ~category:`Bridge ~message:"completion failed" () in
  let handle = Base_async.create ()
      ~encode_output:(fun text -> Ok Temporal_base.Payload.
        { metadata = []; data = Bytes.of_string text })
      ~submit:(fun _ ->
        incr attempts;
        match !attempts with
        | 1 -> Error (Base_async.Retryable_submission error)
        | 2 -> Error (Base_async.Rejected_submission error)
        | _ -> Ok ()) in
  ignore (Base_async.activate handle);
  (match Base_async.complete handle "rejected" with
  | Error _ -> ()
  | Ok () -> failwith "uncertain completion was accepted");
  (match Base_async.fail handle error with
  | Error _ -> ()
  | Ok () -> failwith "different operation ran before uncertainty settled");
  if !attempts <> 1 then failwith "conflicting request crossed the submit boundary";
  (match Base_async.complete handle "rejected" with
  | Error _ -> ()
  | Ok () -> failwith "rejected exact retry was accepted");
  (match Base_async.fail handle error with
  | Ok () when !attempts = 3 -> ()
  | _ -> failwith "definitive rejection left the handle blocked");
  match Base_async.complete handle "late" with
  | Error _ when !attempts = 3 -> ()
  | _ -> failwith "terminal handle accepted another operation"

(** An uncertain heartbeat is reported and forgotten (#836): a newer heartbeat
    and every terminal operation remain possible, and the same stale heartbeat
    is never required as a retry. *)
let test_uncertain_heartbeat_is_not_retained () =
  let submitted = ref [] in
  let error = Temporal_base.Error.make ~category:`Bridge ~message:"unavailable" () in
  let handle = Base_async.create ()
      ~encode_output:(fun text -> Ok Temporal_base.Payload.
        { metadata = []; data = Bytes.of_string text })
      ~submit:(fun operation ->
        submitted := operation :: !submitted;
        match operation with
        | Base_async.Heartbeat _ when List.length !submitted = 1 ->
            Error (Base_async.Retryable_submission error)
        | _ -> Ok ()) in
  let detail text =
    Temporal_base.Payload.{ metadata = []; data = Bytes.of_string text }
  in
  ignore (Base_async.activate handle);
  (match Base_async.heartbeat handle [ detail "50" ] with
  | Error error when not (Temporal_base.Error.view error).non_retryable -> ()
  | Error _ -> failwith "uncertain heartbeat was reported as non-retryable"
  | Ok () -> failwith "uncertain heartbeat was accepted");
  (match Base_async.heartbeat handle [ detail "60" ] with
  | Ok () -> ()
  | Error _ -> failwith "uncertain heartbeat blocked a newer heartbeat");
  (match Base_async.complete handle "done" with
  | Ok () -> ()
  | Error _ -> failwith "uncertain heartbeat blocked completion");
  match List.rev !submitted with
  | [ Base_async.Heartbeat _; Base_async.Heartbeat [ newer ]; Base_async.Complete _ ]
    when Bytes.equal newer.data (Bytes.of_string "60") -> ()
  | _ -> failwith "heartbeat recovery submitted an unexpected sequence"

(** A request that collides with another Domain's in-flight operation is a
    transient conflict, so it is reported as retryable rather than as a
    permanent rejection. The re-entrant submit callback models the second
    Domain deterministically: it runs while the first request is in flight. *)
let test_in_flight_conflict_is_retryable () =
  let handle_ref = ref None in
  let inner = ref None in
  let handle = Base_async.create ()
      ~encode_output:(fun text -> Ok Temporal_base.Payload.
        { metadata = []; data = Bytes.of_string text })
      ~submit:(fun operation ->
        (match operation, !inner with
        | Base_async.Heartbeat _, None ->
            inner := Some (Base_async.complete (Option.get !handle_ref) "done")
        | _ -> ());
        Ok ()) in
  handle_ref := Some handle;
  ignore (Base_async.activate handle);
  (match Base_async.heartbeat handle [] with
  | Ok () -> ()
  | Error _ -> failwith "in-flight heartbeat failed");
  (match !inner with
  | Some (Error error) when not (Temporal_base.Error.view error).non_retryable -> ()
  | Some (Error _) -> failwith "in-flight conflict was reported as permanent"
  | Some (Ok ()) -> failwith "concurrent operation bypassed the in-flight request"
  | None -> failwith "conflicting operation was not attempted");
  match Base_async.complete handle "done" with
  | Ok () -> ()
  | Error _ -> failwith "completion after the in-flight heartbeat failed"

(** Asserts that an async handle operation failed with a retryable error, the
    signal that a completer racing the handoff must retry rather than drop its
    result (#766). *)
let expect_retryable_not_active label = function
  | Error error when not (Temporal.Error.view error).non_retryable -> ()
  | Error error ->
      failwith
        (Printf.sprintf "%s: not-yet-active handle returned a permanent error: %s"
           label (Temporal.Error.message error))
  | Ok () -> failwith (label ^ ": inactive async handle submitted an operation")

(** Asserts that a handle which can never become active reports a
    non-retryable error, so a retry loop around it terminates. *)
let expect_permanently_closed label = function
  | Error error when (Temporal.Error.view error).non_retryable -> ()
  | Error _ -> failwith (label ^ ": unusable async handle remained retryable")
  | Ok () -> failwith (label ^ ": unusable async handle submitted an operation")

(** Regression for #766. A completer that runs before the callback returns, or
    while the worker handoff is still unacknowledged, receives a retryable
    error and no request reaches the client. Once the handoff is accepted the
    same call succeeds exactly once, without rerunning the callback. *)
let test_completion_racing_handoff_is_retryable () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 and retained = ref None and early = ref None in
  let activity =
    Temporal.Activity.define_async ~name:"async_handoff_race"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
      (fun context () ->
        incr calls;
        let handle = Temporal.Activity.Async_context.handle context in
        (* Models a completer on another Domain that finishes before this
           callback has returned [Will_complete_async]. *)
        early := Some (Temporal.Activity.Async_handle.complete handle "early");
        retained := Some handle;
        Temporal.Activity.Will_complete_async handle)
  in
  let token = Bytes.of_string "async-handoff-race" in
  enqueue supervisor
    (start_task ~token ~activity_type:"async_handoff_race"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register_async activity ] in
  supervisor.reject_next_worker_completion := true;
  begin
    match Worker.poll worker with
    | Error { retryable = true; _ } -> ()
    | _ -> failwith "uncertain handoff did not retain the worker lease"
  end;
  expect_retryable_not_active "dormant" (Option.get !early);
  let handle = Option.get !retained in
  (* The callback has returned, but Core has not accepted the handoff. *)
  expect_retryable_not_active "handoff pending"
    (Temporal.Activity.Async_handle.complete handle "finished");
  expect_retryable_not_active "handoff pending heartbeat"
    (Temporal.Activity.Async_handle.heartbeat handle []);
  if !(supervisor.async_completion_calls) <> 0
     || !(supervisor.async_heartbeat_calls) <> 0 then
    failwith "inactive async handle reached the client";
  expect_deferred (Worker.poll worker);
  if !calls <> 1 then failwith "handoff retry reran the async callback";
  begin
    match Temporal.Activity.Async_handle.complete handle "finished" with
    | Ok () -> ()
    | Error error ->
        failwith ("retried completion after handoff failed: "
                  ^ Temporal.Error.message error)
  end;
  if !(supervisor.async_completion_calls) <> 1 then
    failwith "retried completion was not submitted exactly once";
  match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("completed handoff blocked drain: " ^ error.message)

(** The documented pattern from #766 with a real second Domain: the callback
    hands its handle to a Domain that retries the retryable not-yet-active
    error. The first worker handoff is uncertain, so the Domain must keep
    retrying until the next poll accepts the handoff; its result is delivered
    exactly once instead of being lost. *)
let test_completer_domain_retries_until_handoff () =
  let supervisor = fake_supervisor () in
  let completer = ref None in
  let activity =
    Temporal.Activity.define_async ~name:"async_handoff_domain"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
      (fun context () ->
        let handle = Temporal.Activity.Async_context.handle context in
        let rec deliver attempts =
          match Temporal.Activity.Async_handle.complete handle "from-domain" with
          | Error error
            when (not (Temporal.Error.view error).non_retryable)
                 && attempts < 10_000_000 ->
              Domain.cpu_relax ();
              deliver (attempts + 1)
          | result -> result
        in
        completer := Some (Domain.spawn (fun () -> deliver 0));
        Temporal.Activity.Will_complete_async handle)
  in
  let token = Bytes.of_string "async-handoff-domain" in
  enqueue supervisor
    (start_task ~token ~activity_type:"async_handoff_domain"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register_async activity ] in
  supervisor.reject_next_worker_completion := true;
  begin
    match Worker.poll worker with
    | Error { retryable = true; _ } -> ()
    | _ -> failwith "uncertain handoff did not retain the worker lease"
  end;
  expect_deferred (Worker.poll worker);
  begin
    match Domain.join (Option.get !completer) with
    | Ok () -> ()
    | Error error ->
        failwith ("completer Domain lost its result: " ^ Temporal.Error.message error)
  end;
  begin
    match !(supervisor.async_completions) with
    | [ { Protocol.result = Protocol.Completed payload; task_token } ]
      when Bytes.equal task_token token
           && decode_output Temporal.Codec.string payload = "from-domain" -> ()
    | _ -> failwith "completer Domain did not complete exactly once"
  end;
  match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("completed handoff blocked drain: " ^ error.message)

(** A handle that will never be activated must stop being retryable: after a
    synchronous callback result, and after a reserved handoff is discarded by
    terminal native cleanup. Otherwise a completer following the retry
    contract from #766 would spin forever. *)
let test_unactivated_handles_close () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 and retained = ref [] in
  let activity =
    Temporal.Activity.define_async ~name:"async_never_active"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
      (fun context () ->
        incr calls;
        let handle = Temporal.Activity.Async_context.handle context in
        retained := handle :: !retained;
        match !calls with
        | 1 -> Temporal.Activity.Completed "sync"
        | 2 -> Temporal.Activity.Failed
                 (Temporal.Error.make ~category:`Activity ~message:"sync failure" ())
        | 3 -> raise Exit
        | _ -> Temporal.Activity.Will_complete_async handle)
  in
  let input = [ encode_input Temporal.Codec.unit () ] in
  List.iter
    (fun name ->
      enqueue supervisor
        (start_task ~token:(Bytes.of_string name)
           ~activity_type:"async_never_active" ~input))
    [ "never-1"; "never-2"; "never-3"; "never-4" ];
  let worker = worker supervisor [ Adapter.register_async activity ] in
  for _ = 1 to 3 do
    match Worker.poll worker with
    | Ok (Raw_adapter.Completed _ | Raw_adapter.Rejected _) -> ()
    | _ -> failwith "synchronous async outcome was not acknowledged"
  done;
  List.iter
    (fun handle ->
      expect_permanently_closed "synchronous outcome"
        (Temporal.Activity.Async_handle.complete handle "late"))
    !retained;
  supervisor.reject_next_worker_completion := true;
  begin
    match Worker.poll worker with
    | Error { retryable = true; _ } -> ()
    | _ -> failwith "uncertain handoff did not retain the worker lease"
  end;
  let pending = List.hd !retained in
  expect_retryable_not_active "reserved handoff"
    (Temporal.Activity.Async_handle.complete pending "late");
  Worker.discard worker;
  expect_permanently_closed "discarded handoff"
    (Temporal.Activity.Async_handle.complete pending "late");
  if !(supervisor.async_completion_calls) <> 0 then
    failwith "unactivated async handle reached the client"

(** #495 review: an admitted async lease must stay visible to bounded
    shutdown while an unrelated synchronous callback holds the adapter lock.
    [try_drain] reports the adapter busy without waiting, and
    [outstanding_async_leases] still counts the lease through the separate
    async lock, so shutdown cannot attribute it to the abandoned callback. *)
let test_async_lease_visible_while_callback_runs () =
  let supervisor = fake_supervisor () in
  let async_activity =
    Temporal.Activity.define_async ~name:"async_lease_visibility"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
      (fun context () ->
        Temporal.Activity.Will_complete_async
          (Temporal.Activity.Async_context.handle context))
  in
  let entered = Atomic.make false in
  let release = Atomic.make false in
  let blocking =
    Temporal_base.Definition.make ~name:"sync_blocking_callback"
      ~input:(base_codec Temporal.Codec.unit)
      ~output:(base_codec Temporal.Codec.string)
      ~implementation:
        (Some
           (fun _context () ->
             Atomic.set entered true;
             while not (Atomic.get release) do
               Thread.delay 0.005
             done;
             Ok "late"))
  in
  enqueue supervisor
    (start_task ~token:(Bytes.of_string "async-visible-token")
       ~activity_type:"async_lease_visibility"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker =
    worker supervisor
      [ Adapter.register_async async_activity; Raw_adapter.register blocking ]
  in
  expect_deferred (Worker.poll worker);
  if Worker.outstanding_async_leases worker <> Some 1 then
    failwith "the admitted async lease was not counted";
  enqueue supervisor
    (start_task ~token:(Bytes.of_string "sync-blocking-token")
       ~activity_type:"sync_blocking_callback"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let lane = Domain.spawn (fun () -> Worker.poll worker) in
  let deadline = Unix.gettimeofday () +. 5. in
  while not (Atomic.get entered) do
    if Unix.gettimeofday () > deadline then failwith "callback never entered";
    Thread.delay 0.005
  done;
  if not (Worker.callback_running worker) then
    failwith "the running callback was not reported";
  if Worker.try_drain worker <> None then
    failwith "try_drain did not report the held adapter as busy";
  if Worker.try_discard worker then
    failwith "try_discard discarded under a held adapter lock";
  if Worker.outstanding_async_leases worker <> Some 1 then
    failwith "the async lease was hidden by the busy adapter";
  Atomic.set release true;
  ignore (Domain.join lane);
  if Worker.callback_running worker then
    failwith "a returned callback was still reported running";
  if not (Worker.try_discard worker) then
    failwith "try_discard failed on an idle adapter";
  if Worker.outstanding_async_leases worker <> Some 0 then
    failwith "discard did not close the async lease"

(** Waits for a cross-Domain observation, failing after five seconds. *)
let await_flag label flag =
  let deadline = Unix.gettimeofday () +. 5. in
  while not (Atomic.get flag) do
    if Unix.gettimeofday () > deadline then failwith ("timed out: " ^ label);
    Thread.delay 0.005
  done

(** #1023: an asynchronous callback that is still running when the grace
    period ends is abandoned user code, exactly like a synchronous one, so
    the shutdown probe must report it. Only synchronous starts used to set
    the running state, so an abandoned [define_async] callback was reported
    as a native call. *)
let test_async_callback_reported_running () =
  let supervisor = fake_supervisor () in
  let entered = Atomic.make false and release = Atomic.make false in
  let activity =
    Temporal.Activity.define_async ~name:"async_callback_running"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
      (fun context () ->
        Atomic.set entered true;
        while not (Atomic.get release) do
          Thread.delay 0.005
        done;
        Temporal.Activity.Will_complete_async
          (Temporal.Activity.Async_context.handle context))
  in
  enqueue supervisor
    (start_task ~token:(Bytes.of_string "async-running-token")
       ~activity_type:"async_callback_running"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register_async activity ] in
  if Worker.callback_running worker then
    failwith "an idle adapter reported a running callback";
  let lane = Domain.spawn (fun () -> Worker.poll worker) in
  await_flag "async callback entered" entered;
  if not (Worker.callback_running worker) then
    failwith "a running asynchronous callback was not reported";
  Atomic.set release true;
  expect_deferred (Domain.join lane);
  (* After the handoff the work belongs to external code: it is an async
     lease, not a running callback. *)
  if Worker.callback_running worker then
    failwith "a returned asynchronous callback was still reported running";
  if Worker.outstanding_async_leases worker <> Some 1 then
    failwith "the handed-off lease was not counted"

(** #1025: the namespace-bound RPC of an async heartbeat runs without the
    adapter's async lock. While one handle's heartbeat waits for the server,
    the shutdown probe still reads the lease count and the activity lane
    still admits another handoff; before, both waited for the RPC. *)
let test_async_rpc_runs_without_async_lock () =
  let token = Bytes.of_string "async-unlocked-rpc-token" in
  let supervisor, worker, handle, _ =
    heartbeat_status_fixture ~name:"async_unlocked_rpc" ~token
  in
  let entered = Atomic.make false and release = Atomic.make false in
  supervisor.async_heartbeat_hook :=
    (fun () ->
      Atomic.set entered true;
      while not (Atomic.get release) do
        Thread.delay 0.005
      done);
  let heartbeat =
    Domain.spawn (fun () -> Temporal.Activity.Async_handle.heartbeat handle [])
  in
  await_flag "async heartbeat in flight" entered;
  if Worker.outstanding_async_leases worker <> Some 1 then
    failwith "the lease probe waited for, or was hidden by, an in-flight RPC";
  (* A second attempt of the same registered type is handed off meanwhile;
     its admission takes the same async lock. *)
  enqueue supervisor
    (start_task ~token:(Bytes.of_string "async-unlocked-second-token")
       ~activity_type:"async_unlocked_rpc"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let admitted = Atomic.make false in
  let lane =
    Domain.spawn (fun () ->
        let result = Worker.poll worker in
        Atomic.set admitted true;
        result)
  in
  await_flag "second handoff admitted during the RPC" admitted;
  expect_deferred (Domain.join lane);
  if Worker.outstanding_async_leases worker <> Some 2 then
    failwith "the second lease was not admitted";
  Atomic.set release true;
  (match Domain.join heartbeat with
  | Ok () -> ()
  | Error error ->
      failwith ("in-flight heartbeat failed: " ^ Temporal.Error.message error));
  if List.length !(supervisor.async_heartbeats) <> 1 then
    failwith "the in-flight heartbeat was not recorded"

(** Contains-substring check for diagnostics; the standard library has no
    substring search on every supported compiler. *)
let contains ~needle haystack =
  let n = String.length haystack and m = String.length needle in
  let rec loop i =
    i + m <= n && (String.sub haystack i m = needle || loop (i + 1))
  in
  loop 0

(** Requires a full client-call registry's refusal (#1104): a retryable
    [`Bridge] error naming [resource_exhausted]. *)
let expect_registry_refusal label = function
  | Error error ->
      let view = Temporal.Error.view error in
      if view.non_retryable then
        failwith (label ^ ": a full client-call registry was reported as terminal");
      if view.category <> `Bridge then
        failwith (label ^ ": a full client-call registry was not a bridge error");
      if not (contains ~needle:"resource_exhausted" view.message) then
        failwith (label ^ ": the refusal did not name resource_exhausted")
  | Ok () -> failwith (label ^ ": a refused call was reported as accepted")

(** A full client-call registry is transient back-pressure, not a terminal
    outcome (#1104). For complete, fail, and cancel alike the refused call
    returns a retryable error, sends nothing, and keeps both the handle and
    the adapter lease live; once capacity frees, the same call succeeds and
    retires the lease. *)
let test_async_full_registry_terminal_operations_keep_handle () =
  let failure =
    Temporal.Error.make ~category:`Activity ~message:"external work failed" ()
  in
  List.iter
    (fun (label, finish, expected) ->
      let token = Bytes.of_string ("registry-full-" ^ label) in
      let supervisor, worker, handle, calls =
        heartbeat_status_fixture ~name:("registry_full_" ^ label) ~token
      in
      supervisor.free_client_call_slots := 0;
      expect_registry_refusal label (finish handle);
      if !(supervisor.refused_client_calls) <> 1
         || !(supervisor.async_completion_calls) <> 0 then
        failwith (label ^ ": the refused call was not stopped at admission");
      if !calls <> 1 || !(supervisor.async_leased) <> [ token ] then
        failwith (label ^ ": the refusal retired the live async activity");
      if Worker.outstanding_async_leases worker <> Some 1 then
        failwith (label ^ ": the refusal dropped the adapter async lease");
      expect_outstanding label worker;
      (* A second refusal is still retryable and still keeps the handle. *)
      expect_registry_refusal label (finish handle);
      supervisor.free_client_call_slots := 1;
      (match finish handle with
      | Ok () -> ()
      | Error error ->
          failwith (label ^ ": the retry after capacity freed failed: "
            ^ Temporal.Error.message error));
      (match !(supervisor.async_completions) with
      | [ { Protocol.result; _ } ] when expected result -> ()
      | _ -> failwith (label ^ ": the retry did not submit one expected result"));
      if !calls <> 1 || !(supervisor.async_completion_calls) <> 1
         || !(supervisor.async_leased) <> [] then
        failwith (label ^ ": the retry did not retire exactly one lease");
      (match Worker.drain worker with
      | Ok () -> ()
      | Error error -> failwith (label ^ ": finished handle held drain: " ^ error.code));
      match finish handle with
      | Error _ -> ()
      | Ok () -> failwith (label ^ ": a finished handle accepted another call"))
    [
      ( "complete",
        (fun handle -> Temporal.Activity.Async_handle.complete handle "done"),
        function Protocol.Completed _ -> true | _ -> false );
      ( "fail",
        (fun handle -> Temporal.Activity.Async_handle.fail handle failure),
        function Protocol.Failed _ -> true | _ -> false );
      ( "cancel",
        (fun handle -> Temporal.Activity.Async_handle.cancel handle []),
        function Protocol.Cancelled _ -> true | _ -> false );
    ]

(** An async heartbeat refused by a full client-call registry (#1104)
    returns a retryable error, records nothing, and neither retains the
    heartbeat nor blocks the handle: a heartbeat and a completion succeed
    once capacity frees. *)
let test_async_full_registry_heartbeat_keeps_handle () =
  let token = Bytes.of_string "registry-full-heartbeat" in
  let supervisor, worker, handle, calls =
    heartbeat_status_fixture ~name:"registry_full_heartbeat" ~token
  in
  supervisor.free_client_call_slots := 0;
  expect_registry_refusal "heartbeat" (Temporal.Activity.Async_handle.heartbeat handle []);
  if !(supervisor.async_heartbeat_calls) <> 0
     || !(supervisor.refused_client_calls) <> 1
     || !(supervisor.async_leased) <> [ token ] then
    failwith "a refused heartbeat was submitted or retired the activity";
  expect_outstanding "heartbeat" worker;
  supervisor.free_client_call_slots := 1;
  (match Temporal.Activity.Async_handle.heartbeat handle [] with
  | Ok () -> ()
  | Error error ->
      failwith ("heartbeat after capacity freed failed: " ^ Temporal.Error.message error));
  (match Temporal.Activity.Async_handle.complete handle "done" with
  | Ok () -> ()
  | Error error ->
      failwith ("completion after a refused heartbeat failed: "
        ^ Temporal.Error.message error));
  if !calls <> 1 || List.length !(supervisor.async_heartbeats) <> 1
     || List.length !(supervisor.async_completions) <> 1
     || !(supervisor.async_leased) <> [] then
    failwith "heartbeat refusal recovery submitted an unexpected sequence"

(** A refused retry cannot erase an earlier uncertain terminal submission
    (#1104): the server may already have applied that request, so only the
    byte-identical retry may follow, and once capacity frees it resends the
    bytes retained for the first attempt. *)
let test_async_full_registry_preserves_uncertain_completion () =
  let token = Bytes.of_string "registry-full-uncertain" in
  let supervisor, worker, handle, _calls =
    heartbeat_status_fixture ~name:"registry_full_uncertain" ~token
  in
  supervisor.reject_next_async_completion := true;
  (match Temporal.Activity.Async_handle.complete handle "once" with
  | Error error when not (Temporal.Error.view error).non_retryable -> ()
  | _ -> failwith "uncertain completion was not reported as retryable");
  supervisor.free_client_call_slots := 0;
  expect_registry_refusal "uncertain retry"
    (Temporal.Activity.Async_handle.complete handle "once");
  (match Temporal.Activity.Async_handle.complete handle "different" with
  | Error _ -> ()
  | Ok () -> failwith "a refused retry released the uncertain completion");
  if !(supervisor.refused_client_calls) <> 1
     || !(supervisor.async_completion_calls) <> 1 then
    failwith "a conflicting completion reached the client-call registry";
  supervisor.free_client_call_slots := 1;
  (match Temporal.Activity.Async_handle.complete handle "once" with
  | Ok () -> ()
  | Error error ->
      failwith ("exact retry after capacity freed failed: "
        ^ Temporal.Error.message error));
  (match !(supervisor.async_completion_documents) with
  | [ retry; first ] when retry == first -> ()
  | _ -> failwith "the retry did not resend the retained completion bytes");
  match Worker.drain worker with
  | Ok () -> ()
  | Error error -> failwith ("completed handle held drain: " ^ error.code)

(** Retryable registry refusals do not keep a handle alive forever (#1043):
    a lease whose every call is refused still expires once the server must
    have timed its attempt out, after which the handle reports a
    non-retryable error and nothing is sent. *)
let test_async_full_registry_lease_still_expires () =
  let clock = ref 0. in
  let supervisor = fake_supervisor () and retained = ref None in
  enqueue supervisor
    (start_task_with_timeouts ~start_to_close:30
       ~token:(Bytes.of_string "registry-full-expiry") ~activity_type:"leaked_async" ());
  let worker =
    match
      Worker.create_with_clock ~now:(fun () -> !clock) ~supervisor
        ~activities:[ Adapter.register_async (leaked_async_activity retained) ]
        ~worker_shutting_down:(fun () -> false)
    with
    | Ok worker -> worker
    | Error (error : Raw_adapter.error_view) -> failwith error.message
  in
  expect_deferred (Worker.poll worker);
  let handle = Option.get !retained in
  supervisor.free_client_call_slots := 0;
  clock := 10.;
  expect_registry_refusal "expiry" (Temporal.Activity.Async_handle.complete handle ());
  expect_registry_refusal "expiry heartbeat"
    (Temporal.Activity.Async_handle.heartbeat handle []);
  clock := 30. +. 6.;
  if Worker.outstanding_async_leases worker <> Some 0 then
    failwith "a refused lease outlived its attempt timeout";
  supervisor.free_client_call_slots := 1;
  (match Temporal.Activity.Async_handle.complete handle () with
  | Error error when (Temporal.Error.view error).non_retryable -> ()
  | Error _ -> failwith "an expired handle still reported a retryable error"
  | Ok () -> failwith "an expired handle still completed");
  if !(supervisor.async_completion_calls) <> 0 then
    failwith "an expired handle contacted the server"

(** Runs the isolated async lifecycle assertions. *)
let () =
  test_base_state_machine ();
  test_worker_shutdown_reaches_async_handle ();
  test_operation_key_boundaries ();
  test_local_rejection_preserves_earlier_uncertainty ();
  test_native_rejection_releases_earlier_uncertainty ();
  test_uncertain_heartbeat_is_not_retained ();
  test_in_flight_conflict_is_retryable ();
  test_local_deferred_failure_keeps_worker_progress ();
  test_deferred_lifecycle ();
  test_invalid_async_payload_preserves_handle ();
  test_async_codec_error_preserves_handle ();
  test_async_binary_encoding_is_submitted ();
  test_async_completion_retry ();
  test_async_heartbeat_single_encode ();
  test_async_retry_reuses_bytes_for_equal_request_key ();
  test_async_terminal_rejection_closes_lease ();
  test_async_connection_heartbeat_keeps_handle ();
  test_async_rejected_completion_keeps_live_handle ();
  test_async_rejected_heartbeat_keeps_live_handle ();
  test_async_not_found_heartbeat_closes_handle ();
  test_async_heartbeat_and_cancel ();
  test_async_failure ();
  test_async_context_info ();
  test_async_context_info_maximum_timeouts ();
  test_async_drain_and_discard ();
  test_async_lease_expiry ();
  test_async_heartbeat_carries_timeout_and_activity_state ();
  test_stale_handle_rejected ();
  test_completion_racing_handoff_is_retryable ();
  test_completer_domain_retries_until_handoff ();
  test_unactivated_handles_close ();
  test_async_lease_visible_while_callback_runs ();
  test_async_callback_reported_running ();
  test_async_rpc_runs_without_async_lock ();
  test_async_full_registry_terminal_operations_keep_handle ();
  test_async_full_registry_heartbeat_keeps_handle ();
  test_async_full_registry_preserves_uncertain_completion ();
  test_async_full_registry_lease_still_expires ()
