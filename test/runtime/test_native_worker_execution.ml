(** Unit tests for the private native worker execution adapter.

    The fake supervisor below models the semantic contract exposed by the
    typed Native supervisor operations: polling leases one activation,
    completion retires exactly that run ID, and a protocol rejection is
    reported only after the lease has been retired. No Rust, C, network, or
    Temporal Server process is needed to exercise the OCaml registry. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Raw_adapter = Temporal_runtime.Native_worker_execution

(** Copies a public payload into the base representation consumed by the
    private worker execution adapter. Keeping this conversion in the fixture
    makes the public package boundary visible in a low-level runtime test. *)
let base_payload (payload : Temporal.Payload.t) : Temporal_base.Payload.t =
  {
    Temporal_base.Payload.metadata = List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Converts a base payload back into the public representation used by a
    typed update handler. The copy is intentional: native update input is
    owned by the execution activation, while a public codec is allowed to
    retain the payload during its callback. *)
let public_payload (payload : Temporal_base.Payload.t) : Temporal.Payload.t =
  {
    Temporal.Payload.metadata = List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Converts a public structured error to the base error representation used by
    the native execution registry. *)
let base_error (error : Temporal.Error.t) : Temporal_base.Error.t =
  let view = Temporal.Error.view error in
  Temporal_base.Error.make ~non_retryable:view.non_retryable
    ~details:(List.map base_payload view.details) ~category:view.category
    ~message:view.message ()

(** Installs public codec callbacks in a base codec without rewriting their
    encoding metadata. The adapter therefore preserves codecs such as option. *)
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

(** Converts an owned public payload to the binary-safe semantic protocol
    representation used by the fake supervisor. Metadata values are copied as
    bytes exactly as the Rust/JSON bridge does. *)
let protocol_payload (payload : Temporal.Payload.t) : Protocol.payload =
  {
    Protocol.metadata =
      List.map (fun (key, value) -> (key, Bytes.of_string value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Encodes one typed public value for a synthetic native activation. *)
let encoded_protocol codec value =
  match Temporal.Codec.encode codec value with
  | Ok payload -> protocol_payload payload
  | Error error ->
      failwith ("test payload encoding failed: " ^ Temporal.Error.message error)

(** Builds a protocol update job with the same duplicated identity fields that
    Temporal Core supplies. Keeping this fixture in one place prevents tests
    from accidentally bypassing the translator's correlation checks. *)
let update_job ~id ~protocol_instance_id ~name ~input ~run_validator :
    Protocol.activation_job =
  Protocol.Do_update
    {
      id;
      protocol_instance_id;
      name;
      input;
      headers = [];
      meta = { Protocol.identity = "update-client"; update_id = id };
      run_validator;
    }

(** Adapts the public typed update handler to the private runtime callback used
    by this test's fake native worker. The production [Native_worker] module
    performs this same one-payload conversion behind its private boundary; the
    fixture keeps that public contract visible while avoiding a real native
    Core worker. *)
let public_update_handler (handler : Temporal.Update.Handler.t) =
  let name = Temporal.Update.Handler.name handler in
  Raw_adapter.make_update_handler ~name
    ~dispatch:(fun ~run_validator ~on_validated update ->
      match Raw_adapter.update_input update with
      | [ payload ] ->
          Temporal.Update.Handler.dispatch ~run_validator ~on_validated handler
            (public_payload payload)
          |> Result.map base_payload |> Result.map_error base_error
      | _ ->
          Error
            (Temporal_base.Error.make ~non_retryable:true ~category:`Workflow
               ~message:
                 (Printf.sprintf
                    "update %s must contain exactly one payload for its registered OCaml handler"
                    name)
               ()))

(** Rebuilds a public workflow as the private base definition accepted by the
    native worker registry. Public implementation errors are converted only at
    this test boundary, matching the production adapter's ownership rule. *)
let base_workflow (definition : ('input, 'output) Temporal.Workflow.t) =
  let implementation =
    Option.map
      (fun implementation input ->
        Result.map_error base_error (implementation input))
      (Temporal.Workflow.implementation definition)
  in
  Temporal_base.Definition.make ~name:(Temporal.Workflow.name definition)
    ~input:(base_codec (Temporal.Workflow.input definition))
    ~output:(base_codec (Temporal.Workflow.output definition)) ~implementation

(** Keeps workflow fixture registration readable while making the public-to-base
    conversion explicit at the private adapter boundary. *)
module Adapter = struct
  include Raw_adapter

  (** Keeps both interaction kinds available to the fake native worker. The
      production public adapter performs the same conversion before calling
      this private registry; keeping the optional query list here lets this
      test exercise the owner-Domain query path without a native handle. *)
  let register ?(signal_handlers = []) ?(query_handlers = [])
      ?(update_handlers = []) definition =
    Raw_adapter.register ~signal_handlers ~query_handlers ~update_handlers
      (base_workflow definition)
end

(** Keeps workflow fixture sequencing on the same typed-result path as the
    production adapter. *)
let ( let* ) = Result.bind

(** A source-side error used by the deterministic semantic queue. *)
type source_error = { code : string; message : string }

(** One fake supervisor owns an activation queue and mutable lease ledger. All
    mutable fields are accessed by the adapter's serialized poll call in these
    tests. *)
type fake_supervisor = {
  (* Activations waiting to be leased in producer order. *)
  queue : Protocol.activation Queue.t;
  (* Run IDs currently leased by the fake source and therefore requiring one
     acknowledged completion. *)
  leased : (string, unit) Hashtbl.t;
  (* Completions accepted by the fake source, newest first for assertions. *)
  completions : Protocol.completion list ref;
  (* Every completion attempt, including rejected attempts, newest first. This
     lets retry tests compare the retained command value with the later
     acknowledgement without granting the fake source ownership of it. *)
  attempts : Protocol.completion list ref;
  (* Optional source error returned by the next poll, modelling a lower-layer
     semantic rejection whose lease has already been retired. *)
  poll_error : source_error option ref;
  (* Number of poll errors observed; tests use this to prove the source-side
     rejection path ran exactly once. *)
  rejected_poll_count : int ref;
  (* One-shot completion rejection used to verify retained-completion retry
     without rerunning workflow code. *)
  reject_next_completion : bool ref;
  (* One-shot completion exception used to verify the adapter's cleanup guard
     and explicit failure-completion fallback. *)
  raise_next_completion : bool ref;
}

(** Allocates an empty fake semantic queue. *)
let fake_supervisor () =
  {
    queue = Queue.create ();
    leased = Hashtbl.create 8;
    completions = ref [];
    attempts = ref [];
    poll_error = ref None;
    rejected_poll_count = ref 0;
    reject_next_completion = ref false;
    raise_next_completion = ref false;
  }

(** Implements the typed supervisor contract over the fake lease ledger. *)
module Fake_supervisor = struct
  type t = fake_supervisor
  type error = source_error

  (** Takes one activation and records its run ID as leased. An injected error
      represents a malformed activation rejected by the lower protocol layer;
      the counter proves that layer retired the lease before returning. *)
  let try_poll_workflow supervisor =
    match !(supervisor.poll_error) with
    | Some error ->
        incr supervisor.rejected_poll_count;
        Error error
    | None ->
        if Queue.is_empty supervisor.queue then Ok None
        else
          let activation = Queue.take supervisor.queue in
          Hashtbl.replace supervisor.leased activation.run_id ();
          Ok (Some activation)

  (** Accepts one completion only for an active run ID, then removes that lease
      and records the immutable semantic completion for assertions. *)
  let complete_workflow supervisor (completion : Protocol.completion) =
    supervisor.attempts := completion :: !(supervisor.attempts);
    if !(supervisor.raise_next_completion) then begin
      supervisor.raise_next_completion := false;
      raise (Failure "injected completion exception")
    end else if !(supervisor.reject_next_completion) then begin
      supervisor.reject_next_completion := false;
      Error { code = "temporarily_unavailable"; message = "completion transport unavailable" }
    end else if Hashtbl.mem supervisor.leased completion.run_id then begin
      Hashtbl.remove supervisor.leased completion.run_id;
      supervisor.completions := completion :: !(supervisor.completions);
      Ok ()
    end
    else Error { code = "stale_lease"; message = "run is not leased" }

  (** Exposes the stable source error code required by the adapter signature. *)
  let error_code error = error.code

  (** Exposes the stable source diagnostic required by the adapter signature. *)
  let error_message error = error.message
end

(** The test worker instantiates the production functor with the deterministic
    fake source, proving that no concrete native handle is required by the
    execution registry itself. *)
module Worker = Adapter.Make (Fake_supervisor)

(** The canonical timestamp used by every ordinary activation fixture. *)
let timestamp : Protocol.timestamp = { seconds = 1L; nanoseconds = 0 }

(** Builds a unit workflow start job with no arguments. The adapter fills in the
    canonical [binary/null] payload for the typed unit codec. *)
let initialize ~run_id ~workflow_type : Protocol.activation_job =
  Protocol.Initialize_workflow
    {
      workflow_id = "workflow-" ^ run_id;
      workflow_type;
      arguments = [];
      randomness_seed = "1";
      attempt = 1;
      context = None;
    }

(** Wraps jobs in the strict ordinary activation envelope accepted by the
    translation layer. *)
let activation ~run_id jobs : Protocol.activation =
  {
    run_id;
    timestamp = Some timestamp;
    is_replaying = true;
    history_length = 1L;
    jobs;
    metadata = None;
  }

(** Builds Core's synthetic cache-eviction envelope. Temporal deliberately omits
    the timestamp for this activation, so retaining that distinction verifies
    that the adapter does not normalize away a meaningful protocol invariant. *)
let eviction_activation ~run_id jobs : Protocol.activation =
  { (activation ~run_id jobs) with timestamp = None }

(** Adds an activation to the fake queue in producer order. *)
let enqueue supervisor activation = Queue.add activation supervisor.queue

(** Extracts the newest completion while failing with a useful test diagnostic
    when the adapter did not retire a lease. *)
let latest_completion supervisor =
  match !(supervisor.completions) with
  | completion :: _ -> completion
  | [] -> failwith "expected the adapter to submit a completion"

(** Extracts the newest native completion attempt, including one rejected by the
    fake source. The adapter must retain this exact semantic value for retry. *)
let latest_attempt supervisor =
  match !(supervisor.attempts) with
  | completion :: _ -> completion
  | [] -> failwith "expected the adapter to attempt a completion"

(** Creates a worker around a list of executable workflow definitions. *)
let worker supervisor workflows =
  match Worker.create ~supervisor ~workflows () with
  | Ok worker -> worker
  | Error error ->
      failwith
        (Printf.sprintf "worker creation failed: %s at %s (%s)" error.message
           error.path error.code)

(** Asserts a completed outcome and checks whether the workflow reached a
    terminal command. *)
let expect_completed ~terminal = function
  | Adapter.Completed { terminal = actual; _ } when Bool.equal actual terminal ->
      ()
  | Adapter.Completed { terminal = actual; run_id; command_count } ->
      failwith
        (Printf.sprintf "run %s emitted %d commands with terminal flag %b instead of %b"
           run_id command_count actual terminal)
  | Adapter.Not_ready -> failwith "poll unexpectedly reported Not_ready"
  | Adapter.Rejected { error; _ } ->
      failwith
        (Printf.sprintf "poll unexpectedly rejected activation: %s at %s (%s)"
           error.message error.path error.code)

(** A unit workflow completes in the first activation and is removed from the
    existential run registry only after the fake supervisor accepts its
    completion. *)
let test_terminal_workflow () =
  let supervisor = fake_supervisor () in
  let called = ref false in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_terminal"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        called := true;
        Ok ())
  in
  enqueue supervisor
    (activation ~run_id:"run-terminal"
       [ initialize ~run_id:"run-terminal" ~workflow_type:"native_worker_terminal" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  if not !called then failwith "workflow implementation was not invoked";
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Complete_workflow { result = None } ] -> ()
  | _ -> failwith "unit workflow did not complete with nullable result"
  end;
  begin match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | _ -> failwith "empty queue did not report Not_ready"
  end

(** The private replay observer receives only metadata after strict activation
    translation. This test proves that workflow identity, replay state, and the
    64-bit history length are delivered without exposing the activation payload
    or changing the ordinary completion path. *)
let test_activation_metadata_hook () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_activation_hook"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  enqueue supervisor
    (activation ~run_id:"run-hook"
       [ initialize ~run_id:"run-hook" ~workflow_type:"native_worker_activation_hook" ]);
  let seen = ref None in
  let worker =
    match
      Worker.create
        ~on_activation:(fun info -> seen := Some info)
        ~supervisor ~workflows:[ Adapter.register workflow ] ()
    with
    | Ok worker -> worker
    | Error error ->
        failwith
          (Printf.sprintf "activation hook worker creation failed: %s" error.message)
  in
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  match !seen with
  | Some
      {
        Raw_adapter.run_id = "run-hook";
        workflow_id = Some "workflow-run-hook";
        is_replaying = true;
        history_length = 1L;
        cache_removal_reason = None;
      } -> ()
  | None -> failwith "activation metadata hook was not called"
  | Some _ -> failwith "activation metadata hook received incorrect metadata"

(** The completion observer runs only after the fake supervisor has accepted the
    normal completion. This proves the live cache fixture's admission barrier
    cannot be satisfied by an activation callback that merely precedes lease
    retirement. *)
let test_completion_metadata_hook_runs_after_acknowledgement () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_completion_hook"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  enqueue supervisor
    (activation ~run_id:"run-completion-hook"
       [ initialize ~run_id:"run-completion-hook"
           ~workflow_type:"native_worker_completion_hook" ]);
  let seen = ref None in
  let lease_retired = ref false in
  let worker =
    match
      Worker.create
        ~on_completion:(fun info ->
          seen := Some info;
          lease_retired := not (Hashtbl.mem supervisor.leased info.run_id))
        ~supervisor ~workflows:[ Adapter.register workflow ] ()
    with
    | Ok worker -> worker
    | Error error ->
        failwith
          (Printf.sprintf "completion hook worker creation failed: %s" error.message)
  in
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  if not !lease_retired then
    failwith "completion metadata hook ran before the native lease was retired";
  match !seen with
  | Some
      {
        Raw_adapter.run_id = "run-completion-hook";
        workflow_id = Some "workflow-run-completion-hook";
        is_replaying = true;
        history_length = 1L;
        cache_removal_reason = None;
      } -> ()
  | None -> failwith "completion metadata hook was not called"
  | Some _ -> failwith "completion metadata hook received incorrect metadata"

(** A completion rejected by the native supervisor remains retained for a
    later poll. The observer must stay silent until that retry succeeds, then
    receive the original CacheFull metadata exactly once after lease retirement.
    This preserves the post-acknowledgement evidence that the live cache test
    uses even when the native completion call is transiently unavailable. *)
let test_completion_metadata_hook_survives_retry () =
  let run_id = "run-completion-hook-retry" in
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_completion_hook_retry"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id
           ~workflow_type:"native_worker_completion_hook_retry" ]);
  let seen = ref [] in
  let worker =
    match
      Worker.create
        ~on_completion:(fun info ->
          if Option.is_some info.cache_removal_reason then seen := info :: !seen)
        ~supervisor ~workflows:[ Adapter.register workflow ] ()
    with
    | Ok worker -> worker
    | Error error ->
        failwith
          (Printf.sprintf "completion retry hook creation failed: %s"
             error.message)
  in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  supervisor.reject_next_completion := true;
  enqueue supervisor
    (eviction_activation ~run_id
       [ Protocol.Remove_from_cache
           { message = "cache pressure"; reason = Protocol.Cache_full } ]);
  begin
    match Worker.poll worker with
    | Error { code = "completion_failed"; _ } -> ()
    | Ok _ -> failwith "rejected eviction acknowledgement unexpectedly succeeded"
    | Error error ->
        failwith
          (Printf.sprintf "eviction acknowledgement returned %s" error.code)
  end;
  if !seen <> [] then
    failwith "completion observer ran before the retained retry was accepted";
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin
    match !seen with
    | [
     {
       Raw_adapter.run_id = observed_run_id;
       cache_removal_reason = Some "cache_full";
       _;
     } ]
      when String.equal observed_run_id run_id ->
        ()
    | _ -> failwith "retained completion did not notify exactly once"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "retained eviction acknowledgement left a native lease outstanding"

(** A diagnostic callback is outside workflow code and must not be able to
    escape the worker poll with an unretired lease. The adapter converts its
    exception to a typed rejection and submits the normal failure completion. *)
let test_activation_metadata_hook_failure_is_typed () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_activation_hook_failure"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  enqueue supervisor
    (activation ~run_id:"run-hook-failure"
       [ initialize ~run_id:"run-hook-failure"
           ~workflow_type:"native_worker_activation_hook_failure" ]);
  let worker =
    match
      Worker.create
        ~on_activation:(fun _ -> failwith "diagnostic sink failed")
        ~supervisor ~workflows:[ Adapter.register workflow ] ()
    with
    | Ok worker -> worker
    | Error error ->
        failwith
          (Printf.sprintf "activation hook failure worker creation failed: %s"
             error.message)
  in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ }) ->
      if not (String.equal error.path "$.activation.replay_metadata") then
        failwith
          (Printf.sprintf
             "activation hook exception had the wrong diagnostic path: %s (%s)"
             error.path error.code)
  | Ok _ -> failwith "activation hook exception was not rejected"
  | Error _ -> failwith "activation hook exception escaped as a supervisor error"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "activation hook exception left a native lease outstanding"

(** A workflow that sleeps first remains in the run registry after its timer
    command, then completes when the matching timer job is delivered. *)
let test_timer_suspension_and_resume () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_timer"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  enqueue supervisor
    (activation ~run_id:"run-timer"
       [ initialize ~run_id:"run-timer" ~workflow_type:"native_worker_timer" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let timer_seq =
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_timer { seq; _ } ] -> seq
    | _ -> failwith "sleep did not emit exactly one timer command"
  in
  enqueue supervisor
    (activation ~run_id:"run-timer" [ Protocol.Fire_timer { seq = timer_seq } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Complete_workflow { result = None } ] -> ()
  | _ -> failwith "timer completion did not produce nullable unit result"
  end

(** A cancellation job resumes a suspended workflow with a terminal cancel
    command, and the adapter removes the run only after that completion is
    accepted by the supervisor. *)
let test_cancellation () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_cancel"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  enqueue supervisor
    (activation ~run_id:"run-cancel"
       [ initialize ~run_id:"run-cancel" ~workflow_type:"native_worker_cancel" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (activation ~run_id:"run-cancel"
       [ Protocol.Cancel_workflow { reason = "operator requested cancellation" } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Cancel_workflow_execution ] -> ()
  | _ -> failwith "cancellation did not produce a terminal cancel command"
  end

(** A native SignalWorkflow job is dispatched on the execution scheduler, not
    inline in the adapter's activation loop. The handler can therefore read
    deterministic workflow time, and the adapter preserves the signal's input,
    sender identity, header order, and payload ownership. *)
let test_signal_handler_runs_on_scheduler () =
  let supervisor = fake_supervisor () in
  let seen = ref None in
  let handler =
    Raw_adapter.make_signal_handler ~name:"order_updated" ~dispatch:(fun signal ->
        match Raw_adapter.signal_input signal with
        | [ payload ] -> (
            match
              Temporal_base.Codec.decode (base_codec Temporal.Codec.string) payload
            with
            | Error error -> Error error
            | Ok value ->
                seen :=
                  Some
                    ( value,
                      Raw_adapter.signal_identity signal,
                      List.map fst (Raw_adapter.signal_headers signal),
                      Temporal.Workflow.now () );
                Ok ())
        | _ ->
            Error
              (Temporal_base.Error.make ~non_retryable:true ~category:`Workflow
                 ~message:"test signal handler received an unexpected arity" ()))
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_signal"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  let run_id = "run-signal" in
  enqueue supervisor
    (activation ~run_id [ initialize ~run_id ~workflow_type:"native_worker_signal" ]);
  let worker = worker supervisor [ Adapter.register ~signal_handlers:[ handler ] workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let timer_seq =
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_timer { seq; _ } ] -> seq
    | _ -> failwith "signal workflow did not emit its initial timer"
  in
  let input = encoded_protocol Temporal.Codec.string "ready" in
  let header = encoded_protocol Temporal.Codec.string "trace-value" in
  enqueue supervisor
    (activation ~run_id
       [ Protocol.Signal_workflow
           {
             signal_name = "order_updated";
             input = [ input ];
             identity = "sender";
             headers = [ ("trace", header) ];
           } ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin
    match !seen with
    | Some ("ready", "sender", [ "trace" ], Ok instant)
      when Int64.equal (Temporal.Time.seconds instant) 1L
           && Int.equal (Temporal.Time.nanoseconds instant) 0 ->
        ()
    | Some _ -> failwith "signal handler lost deterministic signal metadata"
    | None -> failwith "signal activation did not run its registered handler"
  end;
  enqueue supervisor
    (activation ~run_id [ Protocol.Fire_timer { seq = timer_seq } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker))

(** A public output-only query handler is converted into the private native
    registration without exposing its existential type. Query delivery runs
    synchronously on the execution owner Domain, emits one result with the
    original query ID, and never resumes the suspended workflow scheduler.
    The second query also proves that the current public API rejects arguments
    as a typed query failure instead of silently ignoring them. *)
let test_public_query_handler_registration () =
  let supervisor = fake_supervisor () in
  (* This value is allocated beside the workflow definition, then populated by
     the scheduler-owned workflow body.  The query handler may only read it;
     successful delivery proves the inline query dispatch temporarily installs
     the same execution context without resuming the parked scheduler. *)
  let status_local = Temporal.Workflow_context.Local.create () in
  let query =
    Temporal.Query.define ~name:"current-status" ~output:Temporal.Codec.string
  in
  let public_handler =
    Temporal.Query.Handler.make query (fun () ->
        match Temporal.Workflow_context.Local.get status_local with
        | Ok (Some status) -> Ok status
        | Ok None ->
            Error
              (Temporal.Error.defect
                 ~message:"query observed no workflow-local status")
        | Error error -> Error error)
  in
  let query_handler =
    Raw_adapter.make_query_handler ~name:(Temporal.Query.name query)
      ~dispatch:(fun native_query ->
        match Raw_adapter.query_arguments native_query with
        | [] ->
            Temporal.Query.Handler.dispatch public_handler
            |> Result.map base_payload |> Result.map_error base_error
        | _ ->
            Error
              (Temporal_base.Error.make ~non_retryable:true ~category:`Workflow
                 ~message:"query arguments are not supported by this handler" ()))
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_query"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        match Temporal.Workflow_context.Local.set status_local "ready" with
        | Error error -> Error error
        | Ok () -> Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  let run_id = "run-public-query" in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id ~workflow_type:"native_worker_query" ]);
  let worker =
    worker supervisor
      [ Adapter.register ~query_handlers:[ query_handler ] workflow ]
  in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let timer_seq =
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_timer { seq; _ } ] -> seq
    | _ -> failwith "query workflow did not emit its initial timer"
  in
  let query_activation query_id arguments =
    activation ~run_id
      [ Protocol.Query_workflow
          {
            query_id;
            query_type = "current-status";
            arguments;
            headers = [ ("trace", encoded_protocol Temporal.Codec.string "q") ];
          } ]
  in
  enqueue supervisor (query_activation "query-1" []);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Query_result
          { query_id = "query-1"; result = Query_succeeded payload } ] ->
        let expected = encoded_protocol Temporal.Codec.string "ready" in
        if payload <> expected then
          failwith "public query handler returned an unexpected payload"
    | _ -> failwith "public query handler did not return a successful result"
  end;
  enqueue supervisor
    (query_activation "query-2"
       [ encoded_protocol Temporal.Codec.string "unexpected" ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Query_result
          { query_id = "query-2"; result = Query_failed failure } ]
      when Protocol.failure_non_retryable failure ->
        ()
    | _ -> failwith "query arguments were not rejected as a typed failure"
  end;
  enqueue supervisor
    (activation ~run_id [ Protocol.Fire_timer { seq = timer_seq } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker))

(** Public worker registration retains update handlers next to the workflow
    definition. The deterministic mock backend is sufficient here because the
    native activation path is covered below; this test proves that public
    construction accepts one handler and rejects duplicate names before any
    backend resource is allocated. *)
let test_public_update_handler_registration () =
  let update =
    Temporal.Update.define ~name:"public-update-registration"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string
  in
  let handler =
    Temporal.Update.Handler.make update (fun value -> Ok (String.uppercase_ascii value))
  in
  let workflow =
    Temporal.Workflow.define ~name:"public-update-workflow"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  let create updates =
    Temporal.Worker.create ~target_url:"mock://public-update-registration"
      ~namespace:"unit-test" ~task_queue:"unit-test"
      ~workflows:[ Temporal.Worker.workflow ~updates workflow ] ~activities:[] ()
  in
  begin
    match create [ handler ] with
    | Ok public_worker -> (
        match Temporal.Worker.shutdown public_worker with
        | Ok () -> ()
        | Error error ->
            failwith
              ("public update worker shutdown failed: " ^ Temporal.Error.message error))
    | Error error ->
        failwith
          ("public update handler registration failed: "
          ^ Temporal.Error.message error)
  end;
  begin
    match create [ handler; handler ] with
    | Error error when String.equal (Temporal.Error.kind error) "defect" -> ()
    | Error error ->
        failwith
          ("duplicate public update handler returned "
          ^ Temporal.Error.kind error)
    | Ok public_worker ->
        ignore (Temporal.Worker.shutdown public_worker);
        failwith "duplicate public update handler was accepted"
  end;
  let private_supervisor = fake_supervisor () in
  let private_handler = public_update_handler handler in
  begin
    match
      Worker.create ~supervisor:private_supervisor
        ~workflows:
          [ Adapter.register
              ~update_handlers:[ private_handler; private_handler ] workflow ]
        ()
    with
    | Error { code = "duplicate_update_handler"; _ } -> ()
    | Error error ->
        failwith ("private update registration returned " ^ error.code)
    | Ok private_worker ->
        Worker.discard private_worker;
        failwith "duplicate private update handler was accepted"
  end

(** A DoUpdate for a name that is absent from the workflow registration is a
    protocol-level rejection, not a workflow-task failure. The adapter must
    emit one non-retryable UpdateRejected response, keep the sleeping workflow
    alive, and later allow its timer to complete normally. *)
let test_unknown_update_handler_rejected () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_unknown_update"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  let run_id = "run-unknown-update" in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id ~workflow_type:"native_worker_unknown_update" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let timer_seq =
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_timer { seq; _ } ] -> seq
    | _ -> failwith "unknown-update workflow did not emit its initial timer"
  in
  enqueue supervisor
    (activation ~run_id
       [ update_job ~id:"unknown-update" ~protocol_instance_id:"protocol-unknown"
           ~name:"not-registered" ~input:[] ~run_validator:true ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Update_response
          {
            protocol_instance_id = "protocol-unknown";
            response = Protocol.Update_rejected failure;
          } ] ->
        if not (Protocol.failure_non_retryable failure) then
          failwith "unknown update was not rejected non-retryably";
        if
          not
            (String.equal failure.message
               "unhandled workflow update: not-registered")
        then failwith "unknown update rejection lost its handler name"
    | _ -> failwith "unknown update did not emit one rejected response"
  end;
  enqueue supervisor
    (activation ~run_id [ Protocol.Fire_timer { seq = timer_seq } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "unknown update workflow left a native lease outstanding"

(** The public update boundary accepts exactly one payload. Both zero and
    repeated payloads must be rejected explicitly, preserving Core input rather
    than silently selecting one element. The workflow remains suspended after
    both protocol responses, which proves these are update failures rather than
    terminal workflow failures. *)
let test_update_input_arity_rejected () =
  let supervisor = fake_supervisor () in
  let update =
    Temporal.Update.define ~name:"arity-checked-update"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string
  in
  let calls = ref 0 in
  let public_handler =
    Temporal.Update.Handler.make update (fun value ->
        incr calls;
        Ok (String.uppercase_ascii value))
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_update_arity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  let run_id = "run-update-arity" in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id ~workflow_type:"native_worker_update_arity" ]);
  let worker =
    worker supervisor
      [ Adapter.register
          ~update_handlers:[ public_update_handler public_handler ] workflow ]
  in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let first = encoded_protocol Temporal.Codec.string "first" in
  let second = encoded_protocol Temporal.Codec.string "second" in
  enqueue supervisor
    (activation ~run_id
       [ update_job ~id:"arity-empty" ~protocol_instance_id:"protocol-empty"
           ~name:"arity-checked-update" ~input:[] ~run_validator:true;
         update_job ~id:"arity-many" ~protocol_instance_id:"protocol-many"
           ~name:"arity-checked-update" ~input:[ first; second ]
           ~run_validator:true ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Update_response
          {
            protocol_instance_id = "protocol-empty";
            response = Protocol.Update_rejected empty_failure;
          };
        Protocol.Update_response
          {
            protocol_instance_id = "protocol-many";
            response = Protocol.Update_rejected many_failure;
          } ] ->
        if not (Protocol.failure_non_retryable empty_failure) then
          failwith "empty update input was not rejected non-retryably";
        if not (Protocol.failure_non_retryable many_failure) then
          failwith "multi-payload update input was not rejected non-retryably";
        if !calls <> 0 then
          failwith "arity rejection invoked the typed update callback"
    | _ -> failwith "update arity failures did not preserve response order"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "update arity activation left a native lease outstanding"

(** A valid public update is executed once even when the native completion is
    transiently rejected. The exact accepted/completed command list remains in
    the adapter's pending map; the next poll retries it without entering the
    workflow scheduler or calling the typed handler again. *)
let test_update_completion_retry () =
  let supervisor = fake_supervisor () in
  let update =
    Temporal.Update.define ~name:"retryable-update"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string
  in
  let calls = ref 0 in
  let handler =
    Temporal.Update.Handler.make update (fun value ->
        incr calls;
        Ok (String.uppercase_ascii value))
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_update_retry"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  let run_id = "run-update-retry" in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id ~workflow_type:"native_worker_update_retry" ]);
  let worker =
    worker supervisor
      [ Adapter.register ~update_handlers:[ public_update_handler handler ] workflow ]
  in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  supervisor.reject_next_completion := true;
  enqueue supervisor
    (activation ~run_id
       [ update_job ~id:"retry-update" ~protocol_instance_id:"protocol-retry"
           ~name:"retryable-update"
           ~input:[ encoded_protocol Temporal.Codec.string "ready" ]
           ~run_validator:true ]);
  begin
    match Worker.poll worker with
    | Error { code = "completion_failed"; _ } -> ()
    | Error error ->
        failwith
          ("update completion returned the wrong error: " ^ error.message)
    | Ok _ -> failwith "rejected update completion was acknowledged immediately"
  end;
  if !calls <> 1 then failwith "rejected update completion reran the handler";
  if Hashtbl.length supervisor.leased <> 1 then
    failwith "rejected update completion retired its lease too early";
  let retained = latest_attempt supervisor in
  begin
    match retained.commands with
    | [ Protocol.Update_response
          { protocol_instance_id = "protocol-retry"; response = Protocol.Update_accepted };
        Protocol.Update_response
          {
            protocol_instance_id = "protocol-retry";
            response = Protocol.Update_completed payload;
          } ]
      when payload = encoded_protocol Temporal.Codec.string "READY" ->
        ()
    | _ -> failwith "retained update completion did not contain both phases"
  end;
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  if !calls <> 1 then failwith "retrying an update completion reran the handler";
  if latest_completion supervisor <> retained then
    failwith "update completion retry changed the retained command bytes";
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "retried update completion left a native lease outstanding"

(** A run that has processed an update must still be removed after its terminal
    workflow completion. Core may then send a cache-eviction activation; the
    adapter acknowledges it with an empty completion and rejects any later job
    for the retired run without invoking the update handler again. *)
let test_update_terminal_and_eviction_cleanup () =
  let supervisor = fake_supervisor () in
  let update =
    Temporal.Update.define ~name:"cleanup-update"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string
  in
  let calls = ref 0 in
  let handler =
    Temporal.Update.Handler.make update (fun value ->
        incr calls;
        Ok value)
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_update_cleanup"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  let run_id = "run-update-cleanup" in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id ~workflow_type:"native_worker_update_cleanup" ]);
  let worker =
    worker supervisor
      [ Adapter.register ~update_handlers:[ public_update_handler handler ] workflow ]
  in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let timer_seq =
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_timer { seq; _ } ] -> seq
    | _ -> failwith "update cleanup workflow did not emit its initial timer"
  in
  enqueue supervisor
    (activation ~run_id
       [ update_job ~id:"cleanup-update" ~protocol_instance_id:"protocol-cleanup"
           ~name:"cleanup-update"
           ~input:[ encoded_protocol Temporal.Codec.string "retained" ]
           ~run_validator:true ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  if !calls <> 1 then failwith "cleanup update handler did not run exactly once";
  enqueue supervisor
    (activation ~run_id [ Protocol.Fire_timer { seq = timer_seq } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (eviction_activation ~run_id
       [ Protocol.Remove_from_cache
           { message = "update run eviction"; reason = Protocol.Cache_full } ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin match (latest_completion supervisor).commands with
  | [] -> ()
  | _ -> failwith "terminal update eviction emitted a workflow command"
  end;
  enqueue supervisor
    (activation ~run_id
       [ update_job ~id:"stale-update" ~protocol_instance_id:"protocol-stale"
           ~name:"cleanup-update"
           ~input:[ encoded_protocol Temporal.Codec.string "stale" ]
           ~run_validator:true ]);
  begin
    match Worker.poll worker with
    | Ok (Adapter.Rejected { error; lease_retired = true; _ })
      when String.equal error.code "unknown_run_id" -> ()
    | Ok _ -> failwith "retired update run accepted a stale update"
    | Error error ->
        failwith ("stale update cleanup failed: " ^ error.message)
  end;
  if !calls <> 1 then failwith "stale update invoked a retired handler";
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "terminal update eviction left a native lease outstanding"

(** Adapter-level failures happen before the normal execution path can
    validate a completion. A query lease still needs one result per query ID;
    sending [Fail_workflow] here would be rejected by Core and leave the lease
    pending forever. This covers both an unknown run and a translation error,
    the two early-rejection paths that previously shared that invalid command. *)
let test_query_adapter_failure_uses_query_results () =
  let query_job ~query_id ~query_type : Protocol.activation_job =
    Protocol.Query_workflow
      { query_id; query_type; arguments = []; headers = [] }
  in
  let query_activation ~run_id ~query_id ~query_type =
    activation ~run_id [ query_job ~query_id ~query_type ]
  in
  let expect_query_failure ~label ~expected_code supervisor worker activation
      expected_query_ids =
    enqueue supervisor activation;
    begin
      match Worker.poll worker with
      | Ok (Adapter.Rejected { lease_retired = true; error; _ }) ->
          if not (String.equal error.code expected_code) then
            failwith
              (label ^ " returned the wrong rejection code: " ^ error.code)
      | Ok _ -> failwith (label ^ " was not rejected")
      | Error error -> failwith (label ^ " failed to retire its lease: " ^ error.message)
    end;
    begin
      match (latest_completion supervisor).commands with
      | commands ->
          let actual_query_ids =
            List.map
              (function
                | Protocol.Query_result
                    { query_id; result = Protocol.Query_failed _ } ->
                    query_id
                | Protocol.Fail_workflow _ ->
                    failwith (label ^ " emitted an invalid workflow-failure command")
                | _ -> failwith (label ^ " emitted a non-query failure command"))
              commands
          in
          if
            List.sort String.compare actual_query_ids
            <> List.sort String.compare expected_query_ids
          then failwith (label ^ " did not emit one failed query result per query ID")
    end;
    if Hashtbl.length supervisor.leased <> 0 then
      failwith (label ^ " left a native query lease outstanding")
  in
  let unknown_supervisor = fake_supervisor () in
  let unknown_worker = worker unknown_supervisor [] in
  expect_query_failure ~label:"unknown query run" ~expected_code:"unknown_run_id"
    unknown_supervisor unknown_worker
    (activation ~run_id:"run-query-unknown"
       [ query_job ~query_id:"query-unknown-a" ~query_type:"current-status";
         query_job ~query_id:"query-unknown-b" ~query_type:"current-status" ])
    [ "query-unknown-a"; "query-unknown-b" ];
  let translation_supervisor = fake_supervisor () in
  let translation_worker = worker translation_supervisor [] in
  expect_query_failure ~label:"query translation failure"
    ~expected_code:"invalid_message" translation_supervisor translation_worker
    (query_activation ~run_id:"run-query-translation"
       ~query_id:"query-translation" ~query_type:"")
    [ "query-translation" ]


(** Query-only activations remain read-only even when runtime command validation
    has to synthesize an adapter-level failed query result. The invalid payload
    handler forces [Native_execution.activate] to fail after finding an existing
    run; the following timer activation proves the registry entry was not
    dropped while retiring the query lease. *)
let test_query_validation_failure_preserves_run () =
  let supervisor = fake_supervisor () in
  let invalid_query =
    Raw_adapter.make_query_handler ~name:"invalid-payload"
      ~dispatch:(fun _query ->
        Ok { Temporal_base.Payload.metadata = [ ("", "invalid") ]; data = Bytes.empty })
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_query_validation_failure"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  let run_id = "run-query-validation-failure" in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id
           ~workflow_type:"native_worker_query_validation_failure" ]);
  let worker =
    worker supervisor [ Adapter.register ~query_handlers:[ invalid_query ] workflow ]
  in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let timer_seq =
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_timer { seq; _ } ] -> seq
    | _ -> failwith "query validation fixture did not emit its initial timer"
  in
  enqueue supervisor
    (activation ~run_id
       [ Protocol.Query_workflow
           {
             query_id = "query-invalid-payload";
             query_type = "invalid-payload";
             arguments = [];
             headers = [];
           } ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ })
    when String.equal error.code "invalid_message" -> ()
  | Ok _ -> failwith "invalid query payload was not retired as a rejection"
  | Error error ->
      failwith ("invalid query payload failed to retire: " ^ error.message)
  end;
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Query_result
        { query_id = "query-invalid-payload"; result = Query_failed _ } ] -> ()
  | _ -> failwith "invalid query payload did not emit a failed query result"
  end;
  enqueue supervisor
    (activation ~run_id [ Protocol.Fire_timer { seq = timer_seq } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker))

(** Overlong unhandled query names are bounded before protocol encoding. The
    query type is individually valid at the bridge limit, but the constructed
    diagnostic would exceed that limit without truncation. *)
let test_unhandled_query_failure_message_is_bounded () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_long_unhandled_query"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  let run_id = "run-long-unhandled-query" in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id ~workflow_type:"native_worker_long_unhandled_query" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let query_type = String.make 65_536 'q' in
  enqueue supervisor
    (activation ~run_id
       [ Protocol.Query_workflow
           { query_id = "query-long"; query_type; arguments = []; headers = [] } ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Query_result
        { query_id = "query-long"; result = Query_failed { message; _ } } ]
    when String.length message <= 65_536 -> ()
  | [ Protocol.Query_result { result = Query_failed { message; _ }; _ } ] ->
      failwith
        (Printf.sprintf "unhandled query message remained overlong: %d"
           (String.length message))
  | _ -> failwith "long unhandled query did not emit a failed query result"
  end

(** A SignalWorkflow with no matching registration fails the workflow
    non-retryably. Silently acknowledging an unknown signal would make replay
    diverge from the worker's observable state, so the run is removed only
    after Core acknowledges the explicit failure command. *)
let test_unhandled_signal_fails_closed () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_unhandled_signal"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  let run_id = "run-unhandled-signal" in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id ~workflow_type:"native_worker_unhandled_signal" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (activation ~run_id
       [ Protocol.Signal_workflow
           {
             signal_name = "missing";
             input = [];
             identity = "sender";
             headers = [];
           } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Fail_workflow
          { failure = { message; info = Protocol.Application { non_retryable; _ }; _ } } ]
      when non_retryable
           && String.equal message "unhandled workflow signal: missing" ->
        ()
    | _ -> failwith "unhandled signal did not produce a non-retryable failure"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "unhandled signal left a native lease outstanding";
  match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | Ok _ -> failwith "unhandled signal retained stale execution state"
  | Error error -> failwith ("unhandled signal cleanup failed: " ^ error.message)

(** A cache eviction retires the run without a command. A later activation for
    that run is rejected, proving that eviction removed the OCaml execution
    state only after the empty completion was acknowledged. The callback
    assertion also proves that the adapter preserves Core's typed eviction
    reason as metadata rather than confusing it with replay. *)
let test_eviction () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_eviction"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  enqueue supervisor
    (activation ~run_id:"run-eviction"
       [ initialize ~run_id:"run-eviction"
           ~workflow_type:"native_worker_eviction" ]);
  let seen = ref [] in
  let completed = ref [] in
  let eviction_completion_lease_retired = ref false in
  let worker =
    match
      Worker.create
        ~on_activation:(fun info -> seen := info :: !seen)
        ~on_completion:(fun info ->
          completed := info :: !completed;
          if Option.is_some info.cache_removal_reason then
            eviction_completion_lease_retired :=
              not (Hashtbl.mem supervisor.leased info.run_id))
        ~supervisor ~workflows:[ Adapter.register workflow ] ()
    with
    | Ok worker -> worker
    | Error error ->
        failwith
          (Printf.sprintf "eviction metadata worker creation failed: %s"
             error.message)
  in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (eviction_activation ~run_id:"run-eviction"
       [ Protocol.Remove_from_cache
           { message = "test eviction"; reason = Protocol.Cache_full } ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin
    match !seen with
    | eviction :: _
      when String.equal eviction.run_id "run-eviction"
           && eviction.cache_removal_reason = Some "cache_full" ->
        ()
    | _ -> failwith "cache eviction metadata did not preserve Core's reason"
  end;
  begin match (latest_completion supervisor).commands with
  | [] -> ()
  | _ -> failwith "cache eviction unexpectedly emitted a workflow command"
  end;
  if not !eviction_completion_lease_retired then
    failwith "cache eviction completion hook ran before the native lease retired";
  begin
    match !completed with
    | eviction :: _
      when String.equal eviction.run_id "run-eviction"
           && eviction.cache_removal_reason = Some "cache_full" ->
        ()
    | _ -> failwith "cache eviction completion metadata was not delivered"
  end;
  enqueue supervisor
    (activation ~run_id:"run-eviction" [ Protocol.Fire_timer { seq = 1L } ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { error; lease_retired = true; _ })
    when String.equal error.code "unknown_run_id" -> ()
  | _ -> failwith "evicted run remained in the execution registry"
  end

(** An eviction removes only the cached in-memory execution. If Temporal later
    replays a start activation for the same run ID, the adapter must create a
    new scheduler and invoke the registered workflow again instead of reviving
    the shut-down continuation from the evicted generation. The second
    generation also proves that its timer sequence starts from a fresh
    execution-local counter and can complete normally. *)
let test_eviction_allows_fresh_replay_execution () =
  let supervisor = fake_supervisor () in
  let invocations = ref 0 in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_eviction_replay"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        incr invocations;
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  let run_id = "run-eviction-replay" in
  let workflow_type = "native_worker_eviction_replay" in
  enqueue supervisor (activation ~run_id [ initialize ~run_id ~workflow_type ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  if !invocations <> 1 then
    failwith "initial cached execution did not invoke the workflow exactly once";
  let first_timer_seq =
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_timer { seq; _ } ] -> seq
    | _ -> failwith "initial cached execution did not schedule its timer"
  in
  enqueue supervisor
    (eviction_activation ~run_id
       [ Protocol.Remove_from_cache
           { message = "replay generation eviction"; reason = Protocol.Cache_full } ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin match (latest_completion supervisor).commands with
  | [] -> ()
  | _ -> failwith "eviction acknowledgement emitted a workflow command"
  end;
  if !invocations <> 1 then
    failwith "cache eviction unexpectedly re-invoked the workflow";
  (* A later start activation represents the replayed second generation. It
     deliberately reuses the run ID but advances history metadata so the
     fixture cannot accidentally be mistaken for the original activation. *)
  let replay_start =
    { (activation ~run_id [ initialize ~run_id ~workflow_type ]) with
      history_length = 2L }
  in
  enqueue supervisor replay_start;
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  if !invocations <> 2 then
    failwith "replayed start did not create a fresh workflow execution";
  let replay_timer_seq =
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_timer { seq; _ } ] -> seq
    | _ -> failwith "replayed execution did not schedule its timer"
  in
  if replay_timer_seq <> first_timer_seq then
    failwith
      "replayed execution reused the wrong scheduler sequence instead of a fresh one";
  enqueue supervisor (activation ~run_id [ Protocol.Fire_timer { seq = replay_timer_seq } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "fresh replay execution left a native lease outstanding"

(** A second initialization for a run that is still suspended is a terminal
    adapter defect. Once the corresponding failure completion is acknowledged,
    the original execution must be shut down and removed: its paused fiber must
    run cleanup exactly once, and later jobs for the same run ID must be rejected
    instead of resuming workflow code that Core has already been told failed. *)
let test_duplicate_initialization_retires_existing_run () =
  let supervisor = fake_supervisor () in
  let cleanups = ref 0 in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_duplicate_initialization"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Fun.protect
          ~finally:(fun () -> incr cleanups)
          (fun () -> Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L)))
  in
  let run_id = "run-duplicate-initialization" in
  let workflow_type = "native_worker_duplicate_initialization" in
  enqueue supervisor (activation ~run_id [ initialize ~run_id ~workflow_type ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let timer_seq =
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_timer { seq; _ } ] -> seq
    | _ -> failwith "duplicate-initialization fixture did not suspend on a timer"
  in
  enqueue supervisor (activation ~run_id [ initialize ~run_id ~workflow_type ]);
  begin
    match Worker.poll worker with
    | Ok (Adapter.Rejected { error; lease_retired = true; _ })
      when String.equal error.code "duplicate_run_id" ->
        ()
    | Ok _ -> failwith "duplicate initialization did not retire its failure lease"
    | Error error ->
        failwith
          ("duplicate initialization failure was not acknowledged: " ^ error.message)
  end;
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Fail_workflow _ ] -> ()
    | _ -> failwith "duplicate initialization did not submit a failure completion"
  end;
  if !cleanups <> 1 then
    failwith "duplicate initialization did not tear down the existing workflow fiber";
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "duplicate initialization left a native lease outstanding";
  enqueue supervisor (activation ~run_id [ Protocol.Fire_timer { seq = timer_seq } ]);
  match Worker.poll worker with
  | Ok (Adapter.Rejected { error; lease_retired = true; _ })
    when String.equal error.code "unknown_run_id" ->
      ()
  | Ok _ -> failwith "duplicate initialization retained stale execution state"
  | Error error ->
      failwith ("duplicate initialization cleanup poll failed: " ^ error.message)

(** A parent that is evicted while a child is pending must not retain the old
    child future or sequence allocator. Temporal can later deliver a replayed
    start for the same run ID; the new execution must issue the same child
    start command from a fresh scheduler, then accept the two-stage child
    resolution normally. This is the local recovery invariant behind the live
    parent/child replacement gate: eviction retires one generation, while
    replay reconstructs workflow state from history rather than reviving an
    OCaml continuation that no longer belongs to Core. *)
let test_child_replay_after_eviction_restarts_pending_child () =
  let supervisor = fake_supervisor () in
  let invocations = ref 0 in
  let child =
    Temporal.Workflow.remote ~name:"native_worker_replay_child"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_child_replay_parent"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        incr invocations;
        let pending =
          Temporal.Child_workflow.start ~id:"replayed-child" child ()
        in
        Temporal.Future.await pending)
  in
  let run_id = "run-child-replay-eviction" in
  let workflow_type = "native_worker_child_replay_parent" in
  let initial =
    { (activation ~run_id
        [ initialize ~run_id ~workflow_type ]) with
      is_replaying = false }
  in
  enqueue supervisor initial;
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  if !invocations <> 1 then
    failwith "initial parent execution was not invoked exactly once";
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_child_workflow { seq = 1L; workflow_id; _ } ]
      when String.equal workflow_id "replayed-child" ->
        ()
    | _ -> failwith "initial parent did not issue the expected child start"
  end;
  enqueue supervisor
    (eviction_activation ~run_id
       [ Protocol.Remove_from_cache
           {
             message = "child replay recovery";
             reason = Protocol.Cache_full;
           } ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  if !invocations <> 1 then
    failwith "eviction re-invoked the parent before replay";
  begin
    match (latest_completion supervisor).commands with
    | [] -> ()
    | _ -> failwith "child replay eviction emitted a workflow command"
  end;
  (* A replayed start has the same run ID but a later history position. The
     replay flag is part of the activation contract and ensures the workflow
     sees the generation as reconstructed state, not a second live start. *)
  let replay =
    { (activation ~run_id
        [ initialize ~run_id ~workflow_type ]) with
      history_length = 2L;
      is_replaying = true }
  in
  enqueue supervisor replay;
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  if !invocations <> 2 then
    failwith "replayed parent did not create a fresh execution";
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_child_workflow { seq = 1L; workflow_id; _ } ]
      when String.equal workflow_id "replayed-child" ->
        ()
    | _ ->
        failwith
          "replayed parent reused stale child state or allocated a wrong sequence"
  end;
  enqueue supervisor
    (activation ~run_id
       [ Protocol.Resolve_child_workflow_start
           {
             seq = 1L;
             result = Protocol.Child_start_succeeded "child-replay-run";
           } ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin
    match (latest_completion supervisor).commands with
    | [] -> ()
    | _ -> failwith "child start acknowledgement emitted an unexpected command"
  end;
  enqueue supervisor
    (activation ~run_id
       [ Protocol.Resolve_child_workflow
           { seq = 1L; result = Protocol.Child_completed None } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Complete_workflow { result = None } ] -> ()
    | _ -> failwith "replayed child completion did not complete the parent"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "replayed child parent left a native lease outstanding"

(** A normal terminal completion removes its run before Core sends the later
    cache-eviction activation. The adapter must still acknowledge that leased
    eviction with Core's exact successful empty completion instead of trying
    to report an invalid workflow failure. *)
let test_eviction_after_terminal_completion () =
  let run_id = "run-terminal-eviction" in
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_terminal_eviction"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id
           ~workflow_type:"native_worker_terminal_eviction" ]);
  let eviction_notifications = ref [] in
  let worker =
    match
      Worker.create
        ~on_completion:(fun info ->
          if Option.is_some info.cache_removal_reason then
            eviction_notifications := info :: !eviction_notifications)
        ~supervisor ~workflows:[ Adapter.register workflow ] ()
    with
    | Ok worker -> worker
    | Error error ->
        failwith
          (Printf.sprintf "terminal eviction worker creation failed: %s"
             error.message)
  in
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (eviction_activation ~run_id
       [ Protocol.Remove_from_cache
           {
             message = "terminal run eviction";
             reason = Protocol.Cache_full;
           } ]);
  (* A raised native completion must preserve the empty acknowledgement rather
     than entering the ordinary failure-completion fallback. The next poll
     therefore retries the same leased eviction and has no workflow command. *)
  supervisor.raise_next_completion := true;
  begin
    match Worker.poll worker with
    | Error error when String.equal error.code "completion_failed" -> ()
    | Error error ->
        failwith
          (Printf.sprintf "terminal eviction raised the wrong error: %s" error.code)
    | Ok _ -> failwith "terminal eviction exception was unexpectedly acknowledged"
  end;
  if Hashtbl.length supervisor.leased <> 1 then
    failwith "raised terminal eviction did not retain its native lease";
  if !eviction_notifications <> [] then
    failwith "terminal eviction observer ran before acknowledgement retry";
  begin
    match Worker.poll worker with
    | Ok
        (Adapter.Completed
          { run_id = "run-terminal-eviction"; command_count = 0; terminal = false }) ->
        ()
    | Ok _ -> failwith "terminal eviction did not return an empty completion"
    | Error error ->
        failwith
          (Printf.sprintf "terminal eviction was rejected: %s" error.message)
  end;
  begin
    match (latest_completion supervisor).commands with
    | [] -> ()
    | _ -> failwith "terminal eviction emitted a workflow command"
  end;
  begin
    match !eviction_notifications with
    | [ { Raw_adapter.run_id = observed_run_id;
          cache_removal_reason = Some "cache_full";
          _ } ]
      when String.equal observed_run_id run_id ->
        ()
    | _ ->
        failwith
          "terminal eviction retry did not notify exactly once after acknowledgement"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "terminal eviction left a native lease outstanding"

(** An exception from an ordinary completion is caught at the transaction
    boundary. The adapter makes one explicit failure-completion attempt, so the
    lease is retired rather than escaping with an unacknowledged task. *)
let test_unexpected_completion_exception_is_retried () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_completion_exception"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  enqueue supervisor
    (activation ~run_id:"run-completion-exception"
       [ initialize ~run_id:"run-completion-exception"
           ~workflow_type:"native_worker_completion_exception" ]);
  supervisor.raise_next_completion := true;
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ })
    when String.equal error.code "ocaml_exception" -> ()
  | _ -> failwith "completion exception did not trigger a typed failure retry"
  end;
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Fail_workflow _ ] -> ()
  | _ -> failwith "completion exception retry did not submit a failure"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "completion exception retry left a native lease outstanding"

(** A workflow completion that is rejected after execution is retained exactly
    as produced. Draining the adapter acknowledges it without invoking the
    workflow implementation again, which is the shutdown safety property the
    native worker relies on. *)
let test_completion_rejection_is_drained_without_redo () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_completion_retry"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        incr calls;
        Ok ())
  in
  enqueue supervisor
    (activation ~run_id:"run-completion-retry"
       [ initialize ~run_id:"run-completion-retry"
           ~workflow_type:"native_worker_completion_retry" ]);
  supervisor.reject_next_completion := true;
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Error { code = "completion_failed"; _ } -> ()
  | _ -> failwith "completion rejection did not remain a typed error"
  end;
  if !calls <> 1 then failwith "rejected completion reran the workflow";
  if Hashtbl.length supervisor.leased <> 1 then
    failwith "rejected completion unexpectedly retired the native lease";
  begin match Worker.drain worker with
  | Ok () -> ()
  | Error error ->
      failwith
        (Printf.sprintf "pending workflow completion was not drained: %s" error.message)
  end;
  if !calls <> 1 then failwith "draining the completion reran the workflow";
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "drained workflow completion left a native lease outstanding";
  if List.length !(supervisor.completions) <> 1 then
    failwith "draining submitted more than one workflow completion"

(** If an ordinary completion raises, the adapter converts the exception into a
    typed rejected outcome and submits one explicit failure completion. *)
let test_failure_completion_exception_is_typed () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.remote ~name:"native_worker_completion_failure"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_failure_exception"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Activity.execute activity ())
  in
  enqueue supervisor
    (activation ~run_id:"run-failure-exception"
       [ initialize ~run_id:"run-failure-exception"
           ~workflow_type:"native_worker_failure_exception" ]);
  supervisor.raise_next_completion := true;
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; _ }) -> ()
  | _ -> failwith "failure completion exception was not typed"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "typed failure completion left a native lease outstanding"

(** A later activation can fail after a run has already suspended. Once that
    failure is acknowledged, the stale execution must be removed just like a
    failure during initialization; otherwise a subsequent activation could
    resume an execution that Temporal has already retired. *)
let test_resumed_failure_removes_run () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.remote ~name:"native_worker_resumed_activity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_resumed_failure"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L) in
        Temporal.Activity.execute activity ())
  in
  enqueue supervisor
    (activation ~run_id:"run-resumed-failure"
       [ initialize ~run_id:"run-resumed-failure"
           ~workflow_type:"native_worker_resumed_failure" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let timer_seq =
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_timer { seq; _ } ] -> seq
    | _ -> failwith "resumed failure workflow did not emit a timer"
  in
  enqueue supervisor
    (activation ~run_id:"run-resumed-failure"
       [ Protocol.Fire_timer { seq = timer_seq } ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Schedule_activity _ ] -> ()
  | _ -> failwith "timer resolution did not schedule the resumed activity"
  end;
  enqueue supervisor
    (activation ~run_id:"run-resumed-failure"
       [ Protocol.Fire_timer { seq = timer_seq } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Fail_workflow _ ] -> ()
  | _ -> failwith "invalid timer resolution did not fail the workflow"
  end;
  enqueue supervisor
    (activation ~run_id:"run-resumed-failure"
       [ Protocol.Fire_timer { seq = timer_seq } ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ })
    when String.equal error.code "unknown_run_id" -> ()
  | _ -> failwith "resumed failed run remained in the execution registry"
  end

(** A native activity command is submitted with its complete identifier, queue,
    argument, and timeout fields; the run remains suspended awaiting the result. *)
let test_activity_command_retires_lease () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.remote ~name:"native_worker_activity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_unsupported"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Activity.execute activity ())
  in
  enqueue supervisor
    (activation ~run_id:"run-unsupported"
       [ initialize ~run_id:"run-unsupported"
           ~workflow_type:"native_worker_unsupported" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { terminal = false; _ }) -> ()
  | Ok _ -> failwith "activity command unexpectedly completed the workflow"
  | Error error -> failwith ("activity command failed to retire: " ^ error.message)
  end;
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Schedule_activity { activity_type = "native_worker_activity"; _ } ] -> ()
  | _ -> failwith "activity command did not submit its complete protocol shape"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "activity command left a native lease outstanding"

(** Proves that [discard] disposes an execution that is blocked awaiting an
    activity result, and therefore lives only in the run registry
    ([adapter.runs]), never in the pending-completion table. Native worker
    shutdown's happy path must call [discard] after every activation, even one
    whose most recent poll left it suspended rather than pending, or the
    blocked run's scheduler and one-shot continuation would leak until a
    later GC cycle instead of being torn down deterministically at shutdown.
    A subsequent job for the same run ID is accepted as [unknown_run_id] only
    if the run was actually removed from the registry, so this proves
    disposal rather than merely inferring it from an internal accessor. *)
let test_discard_shuts_down_blocked_execution () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.remote ~name:"native_worker_discard_activity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_discard_blocked"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Activity.execute activity ())
  in
  enqueue supervisor
    (activation ~run_id:"run-discard-blocked"
       [ initialize ~run_id:"run-discard-blocked"
           ~workflow_type:"native_worker_discard_blocked" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { terminal = false; _ }) -> ()
  | Ok _ ->
      failwith "blocked activity fixture unexpectedly completed the workflow"
  | Error error ->
      failwith ("blocked activity fixture failed to poll: " ^ error.message)
  end;
  (* The workflow task lease is retired as soon as its [Schedule_activity]
     command is submitted; the run itself stays alive in [adapter.runs],
     suspended, awaiting a future activation with the activity's result. *)
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "blocked run's workflow task lease was not retired by its poll";
  Worker.discard worker;
  enqueue supervisor
    (activation ~run_id:"run-discard-blocked"
       [ Protocol.Cancel_workflow { reason = "post-discard probe" } ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ }) ->
      if not (String.equal error.code "unknown_run_id") then
        failwith
          ("discard left the blocked run reachable under a different code: "
          ^ error.code)
  | Ok _ ->
      failwith "discard did not remove the blocked run from the execution registry"
  | Error error -> failwith ("post-discard probe failed to poll: " ^ error.message)
  end

(** Child starts and their two-stage Core resolutions share one worker lease.
    The first completion records the start command, a successful start
    acknowledgment leaves the workflow pending, and the terminal child result
    finally retires the run with the parent output. *)
let test_child_command_and_resolution_lifecycle () =
  let supervisor = fake_supervisor () in
  let child =
    Temporal.Workflow.remote ~name:"native_worker_child"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_child_lifecycle"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let pending = Temporal.Child_workflow.start ~id:"child-1" child () in
        Temporal.Future.await pending)
  in
  enqueue supervisor
    (activation ~run_id:"run-child-gate"
       [ initialize ~run_id:"run-child-gate"
           ~workflow_type:"native_worker_child_lifecycle" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { run_id = "run-child-gate"; terminal = false; command_count = 1 }) ->
      ()
  | Ok _ -> failwith "child command was not submitted as a pending completion"
  | Error error ->
      failwith ("child command lifecycle returned an adapter error: " ^ error.message)
  end;
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Start_child_workflow { workflow_id = "child-1"; _ } ] -> ()
  | _ -> failwith "child command did not submit its protocol completion"
  end;
  enqueue supervisor
    (activation ~run_id:"run-child-gate"
       [
         Protocol.Resolve_child_workflow_start
           {
             seq = 1L;
             result = Protocol.Child_start_succeeded "child-run";
           };
       ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { run_id = "run-child-gate"; terminal = false; command_count = 0 }) ->
      ()
  | Ok _ -> failwith "child start acknowledgment unexpectedly completed the parent"
  | Error error ->
      failwith ("child start acknowledgment failed: " ^ error.message)
  end;
  enqueue supervisor
    (activation ~run_id:"run-child-gate"
       [
         Protocol.Resolve_child_workflow
           { seq = 1L; result = Protocol.Child_completed None };
       ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { run_id = "run-child-gate"; terminal = true; command_count = 1 }) ->
      ()
  | Ok _ -> failwith "child terminal result did not complete the parent"
  | Error error ->
      failwith ("child terminal result failed: " ^ error.message)
  end;
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Complete_workflow { result = None } ] -> ()
  | _ -> failwith "child terminal result did not submit parent completion"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "child lifecycle left a native lease outstanding"

(** A terminal child result is invalid until Core has acknowledged the child
    start. The execution turns that bridge defect into one terminal failure
    completion and the native adapter discards the parent execution so a later
    activation cannot resume corrupted state. *)
let test_child_terminal_before_start_retires_parent_lease () =
  let supervisor = fake_supervisor () in
  let child =
    Temporal.Workflow.remote ~name:"native_worker_child_before_start"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_child_before_start_parent"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let pending =
          Temporal.Child_workflow.start ~id:"child-before-start" child ()
        in
        Temporal.Future.await pending)
  in
  let run_id = "run-child-before-start" in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id
           ~workflow_type:"native_worker_child_before_start_parent" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin
    match Worker.poll worker with
    | Ok (Adapter.Completed { terminal = false; command_count = 1; _ }) -> ()
    | Ok _ -> failwith "child-before-start setup did not suspend"
    | Error error ->
        failwith ("child-before-start setup failed: " ^ error.message)
  end;
  enqueue supervisor
    (activation ~run_id
       [
         Protocol.Resolve_child_workflow
           { seq = 1L; result = Protocol.Child_completed None };
       ]);
  begin
    match Worker.poll worker with
    | Ok (Adapter.Completed { terminal = true; command_count = 1; _ }) -> ()
    | Ok _ -> failwith "terminal-before-start activation did not fail the workflow"
    | Error error ->
        failwith ("terminal-before-start failure was not acknowledged: " ^ error.message)
  end;
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Fail_workflow _ ] -> ()
    | _ -> failwith "terminal-before-start did not submit a failure completion"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "terminal-before-start left a native lease outstanding";
  match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | Ok _ -> failwith "terminal-before-start retained stale execution work"
  | Error error ->
      failwith ("terminal-before-start cleanup poll failed: " ^ error.message)

(** A second start acknowledgment for one child sequence is a protocol defect,
    even when it carries a different run ID. The adapter must not let that
    conflicting value overwrite the first acknowledgment or leave a leased
    activation unacknowledged. *)
let test_duplicate_child_start_acknowledgment_retires_parent_lease () =
  let supervisor = fake_supervisor () in
  let child =
    Temporal.Workflow.remote ~name:"native_worker_duplicate_child_start"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_duplicate_child_start_parent"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let pending =
          Temporal.Child_workflow.start ~id:"duplicate-child-start" child ()
        in
        Temporal.Future.await pending)
  in
  let run_id = "run-duplicate-child-start" in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id
           ~workflow_type:"native_worker_duplicate_child_start_parent" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (activation ~run_id
       [
         Protocol.Resolve_child_workflow_start
           { seq = 1L; result = Protocol.Child_start_succeeded "first-run" };
       ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (activation ~run_id
       [
         Protocol.Resolve_child_workflow_start
           { seq = 1L; result = Protocol.Child_start_succeeded "second-run" };
       ]);
  begin
    match Worker.poll worker with
    | Ok (Adapter.Completed { terminal = true; command_count = 1; _ }) -> ()
    | Ok _ -> failwith "duplicate child start acknowledgment did not fail the workflow"
    | Error error ->
        failwith ("duplicate child start failure was not acknowledged: " ^ error.message)
  end;
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Fail_workflow _ ] -> ()
    | _ -> failwith "duplicate child start did not submit a failure completion"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "duplicate child start left a native lease outstanding";
  match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | Ok _ -> failwith "duplicate child start retained stale execution work"
  | Error error ->
      failwith ("duplicate child start cleanup poll failed: " ^ error.message)

(** A duplicate terminal child result is rejected while the parent is still
    live and waiting on an unrelated timer. This distinguishes resolver
    ownership from the simpler unknown-run case: the first terminal result is
    valid, but the repeated result must not resolve any future twice or allow
    the parent to continue with inconsistent state. *)
let test_duplicate_child_terminal_while_parent_pending () =
  let supervisor = fake_supervisor () in
  let child =
    Temporal.Workflow.remote ~name:"native_worker_duplicate_child_terminal"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_duplicate_child_terminal_parent"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let child_future =
          Temporal.Child_workflow.start ~id:"duplicate-child-terminal" child ()
        in
        let timer_future = Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 25L) in
        match Temporal.Future.await (Temporal.Future.both child_future timer_future) with
        | Ok ((), ()) -> Ok ()
        | Error error -> Error error)
  in
  let run_id = "run-duplicate-child-terminal" in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id
           ~workflow_type:"native_worker_duplicate_child_terminal_parent" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let timer_seq =
    match
      List.find_map
        (function
          | Protocol.Start_timer { seq; _ } -> Some seq
          | _ -> None)
        (latest_completion supervisor).commands
    with
    | Some seq -> seq
    | None -> failwith "duplicate-terminal setup did not emit a timer"
  in
  enqueue supervisor
    (activation ~run_id
       [
         Protocol.Resolve_child_workflow_start
           { seq = 1L; result = Protocol.Child_start_succeeded "child-run" };
       ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (activation ~run_id
       [
         Protocol.Resolve_child_workflow
           { seq = 1L; result = Protocol.Child_completed None };
       ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "valid child terminal resolution left a native lease outstanding";
  enqueue supervisor
    (activation ~run_id
       [
         Protocol.Resolve_child_workflow
           { seq = 1L; result = Protocol.Child_completed None };
       ]);
  begin
    match Worker.poll worker with
    | Ok (Adapter.Completed { terminal = true; command_count = 1; _ }) -> ()
    | Ok _ -> failwith "duplicate child terminal did not fail the workflow"
    | Error error ->
        failwith ("duplicate child terminal failure was not acknowledged: " ^ error.message)
  end;
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Fail_workflow _ ] -> ()
    | _ -> failwith "duplicate child terminal did not submit a failure completion"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "duplicate child terminal left a native lease outstanding";
  (* The timer remains pending in the discarded parent, so there must be no
     later attempt to resume it after the bridge rejection retires the run. *)
  enqueue supervisor
    (activation ~run_id [ Protocol.Fire_timer { seq = timer_seq } ]);
  match Worker.poll worker with
  | Ok (Adapter.Rejected { error; lease_retired = true; _ })
    when String.equal error.code "unknown_run_id" -> ()
  | Ok _ -> failwith "duplicate child terminal retained stale parent state"
  | Error error ->
      failwith ("duplicate child terminal cleanup poll failed: " ^ error.message)

(** Checks the error observed by a parent when Core rejects a child start. The
    workflow deliberately inspects the same future after [await]: a successful
    rejection path must make the future ready, preserve the typed error, and
    resume the workflow exactly once before the parent completion is submitted.
    Keeping these checks inside the workflow also exercises the real native
    execution translation rather than only testing a standalone resolver. *)
let child_start_rejection_workflow ~child ~expected_message ~continuations () =
  let pending =
    Temporal.Child_workflow.start ~id:"child-start-rejection" child ()
  in
  match Temporal.Future.await pending with
  | Ok () ->
      Error
        (Temporal.Error.defect
           ~message:"rejected child start unexpectedly succeeded")
  | Error error ->
      incr continuations;
      let view = Temporal.Error.view error in
      let error_matches =
        view.category = `Child_workflow
        && view.non_retryable
        && String.equal view.message expected_message
      in
      let future_matches =
        match Temporal.Future.peek pending with
        | Some (Error peek_error) ->
            String.equal (Temporal.Error.message peek_error) expected_message
        | Some (Ok ()) -> false
        | None -> false
      in
      if error_matches && future_matches then Ok ()
      else
        Error
          (Temporal.Error.defect
             ~message:
               "child start rejection did not resolve its future consistently")

(** Runs one synthetic Core child-start rejection through the complete worker
    adapter. Both rejection causes use the same resolver cleanup contract: the
    child table entry is removed, the parent resumes once, and the leased
    activation is retired by one terminal completion. *)
let test_child_start_rejection_case ~label ~cause ~expected_cause () =
  let child_name = "native_worker_child_start_target_" ^ label in
  let workflow_name = "native_worker_child_start_rejection_" ^ label in
  let continuations = ref 0 in
  let child =
    Temporal.Workflow.remote ~name:child_name ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit
  in
  let expected_message =
    Printf.sprintf
      "child workflow start failed: id=child-start-rejection type=%s cause=%s"
      child_name expected_cause
  in
  let workflow =
    Temporal.Workflow.define ~name:workflow_name ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () ->
        child_start_rejection_workflow ~child ~expected_message ~continuations
          ())
  in
  let run_id = "run-child-start-rejection-" ^ label in
  let supervisor = fake_supervisor () in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id ~workflow_type:workflow_name ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { terminal = false; command_count = 1; _ }) -> ()
  | Ok _ -> failwith (label ^ " child start rejection did not suspend")
  | Error error ->
      failwith
        (label ^ " child start rejection setup failed: " ^ error.message)
  end;
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Start_child_workflow { workflow_id; workflow_type; _ } ]
    when String.equal workflow_id "child-start-rejection"
         && String.equal workflow_type child_name -> ()
  | _ -> failwith (label ^ " child start command was malformed")
  end;
  enqueue supervisor
    (activation ~run_id
       [
         Protocol.Resolve_child_workflow_start
           {
             seq = 1L;
             result =
               Protocol.Child_start_failed
                 { workflow_id = "child-start-rejection"; workflow_type = child_name; cause };
           };
       ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { terminal = true; command_count = 1; _ }) -> ()
  | Ok _ -> failwith (label ^ " child start rejection remained pending")
  | Error error ->
      failwith
        (label ^ " child start rejection failed to complete: " ^ error.message)
  end;
  if !continuations <> 1 then
    failwith (label ^ " child start rejection resumed more than once");
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Complete_workflow { result = None } ] -> ()
  | _ -> failwith (label ^ " child start rejection did not complete the parent")
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith (label ^ " child start rejection left a native lease outstanding");
  match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | Ok _ -> failwith (label ^ " child start rejection retained stale work")
  | Error error ->
      failwith
        (label ^ " child start rejection cleanup poll failed: " ^ error.message)

(** A nested application cause gives the terminal child failure a detail
    payload. The outer child-workflow record carries durable identity and retry
    state, while the nested application record exercises recursive decoding and
    the public error detail projection. *)
let child_terminal_failure : Protocol.failure =
  {
    message = "child workflow failed";
    source = "temporal-core";
    stack_trace = "";
    encoded_attributes = None;
    cause =
      Some
        {
          message = "child application failure";
          source = "child-worker";
          stack_trace = "";
          encoded_attributes = None;
          cause = None;
          info =
            Protocol.Application
              {
                type_name = "child_failure";
                non_retryable = true;
                details =
                  [
                    {
                      Protocol.metadata = [];
                      data = Bytes.of_string "child-details";
                    };
                  ];
              };
        };
    info =
      Protocol.Child_workflow
        {
          namespace = "temporal-sdk-test";
          workflow_id = "child-terminal-failure";
          run_id = "child-run";
          workflow_type = "native_worker_child_terminal_failure_target";
          initiated_event_id = 1L;
          started_event_id = 2L;
          retry_state = Protocol.Non_retryable_failure;
        };
  }

(** Propagates one terminal child failure through the worker adapter and checks
    the public error view before allowing the parent to complete. The nested
    detail bytes, readiness observation, one continuation, terminal completion,
    and final lease ledger together cover both resolver ownership and native
    activation retirement. *)
let test_child_terminal_failure () =
  let continuations = ref 0 in
  let child =
    Temporal.Workflow.remote ~name:"native_worker_child_terminal_failure_target"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_child_terminal_failure"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let pending =
          Temporal.Child_workflow.start ~id:"child-terminal-failure" child ()
        in
        match Temporal.Future.await pending with
        | Ok () ->
            Error
              (Temporal.Error.defect
                 ~message:"terminal child failure unexpectedly succeeded")
        | Error error ->
            incr continuations;
            let view = Temporal.Error.view error in
            let details_match =
              match view.details with
              | [ payload ] ->
                  List.is_empty payload.metadata
                  && String.equal (Bytes.to_string payload.data) "child-details"
              | _ -> false
            in
            let error_matches =
              view.category = `Child_workflow
              && view.non_retryable
              && String.equal view.message
                   "child workflow failed source=temporal-core child_workflow namespace=temporal-sdk-test id=child-terminal-failure run_id=child-run type=native_worker_child_terminal_failure_target initiated_event_id=1 started_event_id=2 retry_state=non_retryable_failure | child application failure source=child-worker application type=child_failure non_retryable=true details=1"
            in
            let future_matches =
              match Temporal.Future.peek pending with
              | Some (Error peek_error) ->
                  String.equal (Temporal.Error.message peek_error) view.message
              | Some (Ok ()) -> false
              | None -> false
            in
            if error_matches && details_match && future_matches then Ok ()
            else
              Error
                (Temporal.Error.defect
                   ~message:
                     "terminal child failure did not resolve its future consistently"))
  in
  let run_id = "run-child-terminal-failure" in
  let supervisor = fake_supervisor () in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id ~workflow_type:"native_worker_child_terminal_failure" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { terminal = false; command_count = 1; _ }) -> ()
  | Ok _ -> failwith "terminal child failure setup did not suspend"
  | Error error ->
      failwith ("terminal child failure setup failed: " ^ error.message)
  end;
  enqueue supervisor
    (activation ~run_id
       [
         Protocol.Resolve_child_workflow_start
           { seq = 1L; result = Protocol.Child_start_succeeded "child-run" };
       ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { terminal = false; command_count = 0; _ }) -> ()
  | Ok _ -> failwith "terminal child failure start acknowledgment completed parent"
  | Error error ->
      failwith
        ("terminal child failure start acknowledgment failed: " ^ error.message)
  end;
  enqueue supervisor
    (activation ~run_id
       [
         Protocol.Resolve_child_workflow
           { seq = 1L; result = Protocol.Child_failed child_terminal_failure };
       ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { terminal = true; command_count = 1; _ }) -> ()
  | Ok _ -> failwith "terminal child failure did not complete parent"
  | Error error ->
      failwith ("terminal child failure propagation failed: " ^ error.message)
  end;
  if !continuations <> 1 then
    failwith "terminal child failure resumed more than once";
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Complete_workflow { result = None } ] -> ()
  | _ -> failwith "terminal child failure did not submit parent completion"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "terminal child failure left a native lease outstanding";
  match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | Ok _ -> failwith "terminal child failure retained stale work"
  | Error error ->
      failwith ("terminal child failure cleanup poll failed: " ^ error.message)

(** Reuses the child-failure shape with a retryable Core state. The public
    error view must preserve that [Timeout] is retryable even though the same
    child-workflow category is non-retryable for [Non_retryable_failure]. *)
let test_retryable_child_failure_preserves_retryability () =
  let retryable_failure =
    match child_terminal_failure.info with
    | Protocol.Child_workflow info ->
        {
          child_terminal_failure with
          info = Protocol.Child_workflow { info with retry_state = Protocol.Timeout };
        }
    | Protocol.Application _ | Protocol.Canceled _ | Protocol.Activity _
    | Protocol.Timeout_failure _ ->
        failwith "child terminal failure fixture lost its child-workflow info"
  in
  let child =
    Temporal.Workflow.remote ~name:"native_worker_retryable_child_failure_target"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_retryable_child_failure"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let pending =
          Temporal.Child_workflow.start ~id:"retryable-child-failure" child ()
        in
        match Temporal.Future.await pending with
        | Ok () ->
            Error
              (Temporal.Error.defect
                 ~message:"retryable child failure unexpectedly succeeded")
        | Error error ->
            let view = Temporal.Error.view error in
            if view.category = `Child_workflow && not view.non_retryable then Ok ()
            else
              Error
                (Temporal.Error.defect
                   ~message:
                     "retryable child failure was incorrectly marked non-retryable"))
  in
  let run_id = "run-retryable-child-failure" in
  let supervisor = fake_supervisor () in
  enqueue supervisor
    (activation ~run_id
       [ initialize ~run_id ~workflow_type:"native_worker_retryable_child_failure" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (activation ~run_id
       [
         Protocol.Resolve_child_workflow_start
           { seq = 1L; result = Protocol.Child_start_succeeded "child-run" };
       ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (activation ~run_id
       [ Protocol.Resolve_child_workflow { seq = 1L; result = Protocol.Child_failed retryable_failure } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  begin
    match (latest_completion supervisor).commands with
    | [ Protocol.Complete_workflow { result = None } ] -> ()
    | _ -> failwith "retryable child failure did not complete the parent"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "retryable child failure left a native lease outstanding";
  match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | Ok _ -> failwith "retryable child failure retained stale execution work"
  | Error error ->
      failwith ("retryable child failure cleanup poll failed: " ^ error.message)

(** An activation for a run not present in the existential registry is rejected
    and completed as a non-retryable bridge failure. *)
let test_unknown_run_retires_lease () =
  let supervisor = fake_supervisor () in
  enqueue supervisor
    (activation ~run_id:"run-unknown" [ Protocol.Cancel_workflow { reason = "test" } ]);
  let worker = worker supervisor [] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ }) ->
      if not (String.equal error.code "unknown_run_id") then
        failwith "unknown run had the wrong rejection code"
  | _ -> failwith "unknown run was not rejected"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "unknown run left a native lease outstanding"

(** A malformed semantic activation is rejected by the lower supervisor layer;
    this adapter propagates its typed error and does not claim a completion it
    did not submit. The fake counter models the lower layer's lease retirement.
*)
let test_malformed_activation_error_is_typed () =
  let supervisor = fake_supervisor () in
  supervisor.poll_error :=
    Some { code = "invalid_message"; message = "activation field was malformed" };
  let worker = worker supervisor [] in
  begin match Worker.poll worker with
  | Error error when String.equal error.code "invalid_message" -> ()
  | Error _ -> failwith "malformed activation error classification changed"
  | Ok _ -> failwith "malformed activation unexpectedly produced an outcome"
  end;
  if !(supervisor.rejected_poll_count) <> 1 then
    failwith "lower supervisor did not retire malformed activation"

(** Duplicate and remote registrations are rejected before any worker state is
    published, preventing an ambiguous workflow type from reaching Core. *)
let test_registration_validation () =
  let definition () =
    Temporal.Workflow.define ~name:"duplicate" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  let supervisor = fake_supervisor () in
  begin match
    Worker.create ~supervisor
      ~workflows:[ Adapter.register (definition ()); Adapter.register (definition ()) ]
      ()
  with
  | Error { code = "duplicate_workflow"; _ } -> ()
  | _ -> failwith "duplicate workflow registration was accepted"
  end;
  let remote =
    Temporal.Workflow.remote ~name:"remote" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit
  in
  begin match Worker.create ~supervisor ~workflows:[ Adapter.register remote ] () with
  | Error { code = "not_executable"; _ } -> ()
  | _ -> failwith "remote workflow registration was accepted as executable"
  end

(** Rejects malformed worker defaults before the adapter publishes its
    registry. These values are the same four cases checked by the lower
    workflow context, but this test proves the worker-facing constructor
    returns a typed configuration error instead of deferring the defect to the
    first activation. *)
let test_task_queue_validation () =
  let expect_invalid label task_queue =
    let supervisor = fake_supervisor () in
    match Worker.create ~supervisor ~task_queue ~workflows:[] () with
    | Error { code = "invalid_configuration"; path = "$.task_queue"; message }
      when not (String.equal message "") -> ()
    | Error error ->
        failwith
          (Printf.sprintf
             "%s task queue returned %s at %s without a diagnostic" label
             error.code error.path)
    | Ok _ -> failwith (label ^ " task queue was accepted")
  in
  expect_invalid "empty" "";
  expect_invalid "NUL" "bad\000queue";
  expect_invalid "oversized" (String.make 65_537 'x');
  expect_invalid "UTF-8" (String.make 1 (Char.chr 0xff))

(** Runs all native worker adapter assertions. *)
let () =
  test_terminal_workflow ();
  test_activation_metadata_hook ();
  test_completion_metadata_hook_runs_after_acknowledgement ();
  test_completion_metadata_hook_survives_retry ();
  test_activation_metadata_hook_failure_is_typed ();
  test_timer_suspension_and_resume ();
  test_cancellation ();
  test_signal_handler_runs_on_scheduler ();
  test_public_query_handler_registration ();
  test_public_update_handler_registration ();
  test_unknown_update_handler_rejected ();
  test_update_input_arity_rejected ();
  test_update_completion_retry ();
  test_update_terminal_and_eviction_cleanup ();
  test_query_adapter_failure_uses_query_results ();
  test_query_validation_failure_preserves_run ();
  test_unhandled_query_failure_message_is_bounded ();
  test_unhandled_signal_fails_closed ();
  test_eviction ();
  test_eviction_allows_fresh_replay_execution ();
  test_duplicate_initialization_retires_existing_run ();
  test_child_replay_after_eviction_restarts_pending_child ();
  test_eviction_after_terminal_completion ();
  test_unexpected_completion_exception_is_retried ();
  test_completion_rejection_is_drained_without_redo ();
  test_failure_completion_exception_is_typed ();
  test_resumed_failure_removes_run ();
  test_activity_command_retires_lease ();
  test_discard_shuts_down_blocked_execution ();
  test_child_command_and_resolution_lifecycle ();
  test_child_terminal_before_start_retires_parent_lease ();
  test_duplicate_child_start_acknowledgment_retires_parent_lease ();
  test_duplicate_child_terminal_while_parent_pending ();
  test_child_start_rejection_case ~label:"exists"
    ~cause:Protocol.Child_start_workflow_already_exists
    ~expected_cause:"workflow_already_exists" ();
  test_child_start_rejection_case ~label:"unspecified"
    ~cause:Protocol.Child_start_unspecified ~expected_cause:"unspecified" ();
  test_child_terminal_failure ();
  test_retryable_child_failure_preserves_retryability ();
  test_unknown_run_retires_lease ();
  test_malformed_activation_error_is_typed ();
  test_registration_validation ();
  test_task_queue_validation ()
