(** Unit tests for deferred activity completion.

    The fake supervisor models the two distinct native obligations involved in
    [WillCompleteAsync]: the worker task completion that admits the handle, and
    the later namespace-bound client completion that retires the asynchronous
    lease. Keeping those ledgers separate makes it possible to catch the most
    dangerous integration mistake: sending a late completion through Core's
    worker task ledger after that ledger has already acknowledged the handoff.
*)

module Protocol = Temporal_protocol.Activity_protocol
module Raw_adapter = Temporal_runtime.Native_activity_execution
module Base_async = Temporal_base.Async_activity

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

(** Copies a protocol completion through the strict semantic codec. The fake
    therefore owns independent token and payload storage exactly as a native
    implementation must. *)
let copy_completion completion =
  match Protocol.encode_completion completion with
  | Error _ -> failwith "async fake received an invalid completion"
  | Ok json -> (
      match Protocol.decode_completion json with
      | Ok value -> value
      | Error _ -> failwith "async fake could not reparse its completion")

(** Copies a protocol heartbeat through the strict codec to test ownership of
    late detail payloads as well as their semantic shape. *)
let copy_heartbeat heartbeat =
  match Protocol.encode_heartbeat heartbeat with
  | Error _ -> failwith "async fake received an invalid heartbeat"
  | Ok json -> (
      match Protocol.decode_heartbeat json with
      | Ok value -> value
      | Error _ -> failwith "async fake could not reparse its heartbeat")

type source_error = { code : string; message : string; retryable : bool }
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
  reject_next_worker_completion : bool ref;
  reject_next_async_completion : bool ref;
  reject_next_async_completion_terminal : bool ref;
  reject_next_async_heartbeat : bool ref;
}
(** Fake native state. [leased] and [async_leased] intentionally model
    different Temporal APIs and are never allowed to substitute for one another.
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
    reject_next_worker_completion = ref false;
    reject_next_async_completion = ref false;
    reject_next_async_completion_terminal = ref false;
    reject_next_async_heartbeat = ref false;
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
let source_error ?(retryable = false) code message =
  { code; message; retryable }

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
  let complete_activity supervisor (completion : Protocol.completion) =
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
      supervisor.completions := copy_completion completion :: !(supervisor.completions);
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
      one-shot rejection proves that the OCaml handle retains and retries the
      exact request without rerunning user code. *)
  let complete_async_activity supervisor (completion : Protocol.completion) =
    incr supervisor.async_completion_calls;
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
        (source_error "not_found" "async activity no longer exists")
    end
    else if !(supervisor.reject_next_async_completion) then begin
      supervisor.reject_next_async_completion := false;
      Error
        (source_error ~retryable:true "temporarily_unavailable"
           "async completion transport unavailable")
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
          copy_completion completion :: !(supervisor.async_completions);
        Ok ()
      end

  (** Records ordinary activity heartbeats only while the worker lease remains
      active. Async callbacks use [record_async_activity_heartbeat] instead. *)
  let record_activity_heartbeat supervisor (heartbeat : Protocol.heartbeat) =
    if has_token heartbeat.Protocol.task_token !(supervisor.leased) then begin
      supervisor.heartbeats := copy_heartbeat heartbeat :: !(supervisor.heartbeats);
      Ok ()
    end
    else Error (source_error "stale_lease" "worker lease is not active")

  (** Records heartbeat details against the namespace-bound async lease. *)
  let record_async_activity_heartbeat supervisor (heartbeat : Protocol.heartbeat) =
    incr supervisor.async_heartbeat_calls;
    if !(supervisor.reject_next_async_heartbeat) then begin
      supervisor.reject_next_async_heartbeat := false;
      Error
        (source_error ~retryable:true "temporarily_unavailable"
           "async heartbeat transport unavailable")
    end
    else if has_token heartbeat.Protocol.task_token !(supervisor.async_leased) then begin
      supervisor.async_heartbeats :=
        copy_heartbeat heartbeat :: !(supervisor.async_heartbeats);
      Ok ()
    end
    else Error (source_error "stale_async_lease" "async lease is not active")

  (** Exposes stable source classifications to the adapter. *)
  let error_code error = error.code
  let error_message error = error.message
  let error_is_retryable error = error.retryable
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
  match Worker.create ~supervisor ~activities with
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
    | [ { Protocol.result = Protocol.Completed (Some payload); task_token } ] ->
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
    [ ("invalid-utf8", [ ("opaque", "\255") ]);
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

(** A public codec can emit an invalid encoding marker while successfully
    encoding its body. A corrected option value must still complete normally. *)
let test_async_invalid_encoding_preserves_handle () =
  let codec = Temporal.Codec.option
      (Temporal.Codec.make ~encoding:"\255"
        ~encode:(fun value -> Ok (Bytes.of_string value))
        ~decode:(fun bytes -> Ok (Bytes.to_string bytes))) in
  let supervisor, worker, handle, _, _ = payload_validation_fixture (base_codec codec) in
  (match Temporal.Activity.Async_handle.complete handle (Some "value") with
  | Error _ -> ()
  | Ok () -> failwith "invalid public codec encoding was accepted");
  if !(supervisor.async_completion_calls) <> 0 then
    failwith "invalid public codec encoding reached the supervisor";
  (match Temporal.Activity.Async_handle.complete handle None with
  | Ok () -> ()
  | Error error -> failwith (Temporal.Error.message error));
  match Worker.drain worker with
  | Ok () when !(supervisor.async_leased) = [] -> ()
  | _ -> failwith "corrected public codec output left completion debt"

(** A rejected late completion retains the exact request key. Retrying it
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
    | Error _ -> ()
    | Ok () -> failwith "rejected async completion was reported as accepted"
  end;
  if !calls <> 1 then failwith "async completion retry reran the callback";
  if not (has_token token !(supervisor.async_leased)) then
    failwith "async completion rejection retired the client lease";
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
    failwith "async completion did not submit exactly one identical retry"

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

(** The base state machine rejects use before activation and keeps its terminal
    state after one accepted completion without requiring a native supervisor. *)
let test_base_state_machine () =
  let submitted = ref 0 in
  let payload = Temporal_base.Payload.{ metadata = []; data = Bytes.empty } in
  let handle =
    Base_async.create
      ~submit:(fun _operation -> incr submitted; Ok ())
      ~encode_output:(fun _ -> Ok payload)
  in
  begin
    match Base_async.complete handle () with
    | Error _ -> ()
    | Ok () -> failwith "dormant base async handle submitted an operation"
  end;
  if !submitted <> 0 then failwith "dormant base handle entered its submit callback";
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
    Base_async.create
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
  let handle = Base_async.create
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

(** Runs the isolated async lifecycle assertions. *)
let () =
  test_base_state_machine ();
  test_operation_key_boundaries ();
  test_local_rejection_preserves_earlier_uncertainty ();
  test_local_deferred_failure_keeps_worker_progress ();
  test_deferred_lifecycle ();
  test_invalid_async_payload_preserves_handle ();
  test_async_codec_error_preserves_handle ();
  test_async_invalid_encoding_preserves_handle ();
  test_async_completion_retry ();
  test_async_terminal_rejection_closes_lease ();
  test_async_heartbeat_and_cancel ();
  test_async_failure ();
  test_async_drain_and_discard ();
  test_stale_handle_rejected ()
