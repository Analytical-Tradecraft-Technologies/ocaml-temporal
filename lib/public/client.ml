(** Implements typed client handles over the private semantic backend. *)

(** The client state is intentionally opaque in the public interface. A single
    backend value owns all native resources for this SDK instance. *)
type t = {
  (* The validated Temporal namespace used for every operation on this client. *)
  namespace : string;
  (* The backend owns the transport and native supervisor graph; no other
     client field retains a native handle. *)
  backend : Backend.client;
  (* Set before teardown begins so new operations fail without entering the
     backend after the lifecycle transition has been admitted. *)
  closed : bool Atomic.t;
  (* Serializes the first teardown with later callers that need the cached
     result, while backend shutdown itself remains outside public state. *)
  shutdown_mutex : Mutex.t;
  (* The first shutdown outcome is retained so every caller observes the same
     terminal result, including a native teardown error. *)
  mutable shutdown_result : (unit, Error.t) result option;
}

(** A handle retains the definition codecs and the execution it addresses:
    one exact run, or the workflow's current run. *)
type ('input, 'output) handle = {
  (* The owning client keeps the backend alive for all operations on this
     handle; shutdown is still explicit and invalidates future calls. *)
  client : t;
  (* The workflow definition supplies the output codec used by [wait] and the
     name used when [start] builds the backend request. *)
  workflow : ('input, 'output) Workflow.t;
  (* The durable ID selected by the caller and echoed by the start response. *)
  workflow_id : string;
  (* [Some] exact server run ID: every operation targets that run and [wait]
     never follows a successor implicitly. [None] (only from [get_handle]):
     every operation sends an empty run ID so Temporal resolves the current
     run, and [wait] follows the run chain to its last run. *)
  run_id : string option;
  (* [true] only when [start] created this run; [false] for a run returned
     by a [`Use_existing] start and for every handle built by [follow] or
     [get_handle]. *)
  started : bool;
}

(** Temporal's workflow ID conflict policy, applied when a start names a
    workflow ID whose current run is still open. *)
type id_conflict_policy = [ `Fail | `Use_existing | `Terminate_existing ]

(** Temporal's workflow ID reuse policy, applied when a start names a
    workflow ID whose latest run has already closed. *)
type id_reuse_policy =
  [ `Allow_duplicate | `Allow_duplicate_failed_only | `Reject_duplicate ]

(** A client-side handle for one admitted workflow update. It retains the
    update definition and encoded input so completion polls cannot be confused
    with another request or require any native pointer ownership. *)
type ('input, 'output) update_handle = {
  client : t;
  definition : ('input, 'output) Update.t;
  workflow_id : string;
  run_id : string;
  update_id : string;
  input : Payload.t;
  (* A completed admission outcome can be decoded without looking up a server
     record that may no longer exist. Immutable across caller Domains. *)
  outcome : Backend.update_outcome option;
}

(** Identifies a successor execution returned after a completed, failed,
    timed-out, or continued-as-new run. The identity is intentionally kept separate from a
    typed [handle]: callers must supply the workflow definition when they turn
    it back into a handle, so the output codec is never guessed from a run ID. *)
type execution = {
  (* Namespace that owns the successor execution. *)
  namespace : string;
  (* Durable workflow identity shared by the original and successor runs. *)
  workflow_id : string;
  (* Server-issued identity of the successor run. *)
  run_id : string;
}

(** Terminal outcomes mirror the backend while replacing payload bytes with the
    definition's typed output. *)
type 'output terminal_result =
  (* The terminal payload decoded with the workflow definition's output codec,
     and the cron or retry successor the completed run started, if any. *)
  | Completed of { output : 'output; successor : execution option }
  (* Failure and its optional successor run, which callers may follow explicitly. *)
  | Failed of { error : Error.t; successor : execution option }
  (* The exact run accepted a cancellation request and reached cancellation. *)
  | Cancelled of Error.t
  (* The exact run was terminated by an operator or another Temporal client. *)
  | Terminated of Error.t
  (* Timeout and its optional successor run, which callers may follow explicitly. *)
  | Timed_out of { error : Error.t; successor : execution option }
  (* The run continued as a new execution; callers choose whether to follow it. *)
  | Continued_as_new of execution

(** One execution row returned by the Temporal visibility service. *)
type visibility_execution = {
  workflow_id : string;
  run_id : string;
  workflow_type : string;
  task_queue : string;
  status : string;
}

(** A bounded visibility page and its opaque continuation token. *)
type visibility_page = {
  executions : visibility_execution list;
  next_page_token : string option;
}

(** Rejects empty, oversized, malformed UTF-8, or NUL-containing identifiers
    before they can enter a backend request. The UTF-8 and 65,536-byte bounds
    are shared by the JSON protocol and native bridge, so mock and native
    transports reject the same malformed operation rather than diverging at
    their respective boundaries. *)
let validate_name field value =
  if String.equal value "" then
    Error (Error.defect ~message:(field ^ " must not be empty"))
  else if String.length value > 65_536 then
    Error
      (Error.defect
         ~message:(field ^ " exceeds the protocol string safety limit"))
  else if not (Temporal_base.Codec.valid_utf_8 value) then
    Error (Error.defect ~message:(field ^ " must be valid UTF-8"))
  else if String.contains value '\000' then
    Error (Error.defect ~message:(field ^ " must not contain NUL"))
  else Ok ()

(** Builds the private backend configuration after checking every user-facing
    connection field. Routine configuration failures remain [result] values.
    An omitted identity is derived once here as [<pid>@<hostname>]; this runs
    outside workflow code, so reading process state is replay-safe. *)
let create ?identity ?io_threads ~target_url ~namespace () =
  let identity = Temporal_base.Process_identity.resolve identity in
  match Backend.validate_io_threads io_threads with
  | Error error -> Error error
  | Ok () ->
  match validate_name "namespace" namespace with
  | Error error -> Error error
  | Ok () -> (
      match validate_name "identity" identity with
      | Error error -> Error error
      | Ok () ->
          let config : Backend.config =
            { target_url; namespace; identity; task_queue = None }
          in
          Result.map
            (fun backend ->
              {
                namespace;
                backend;
                closed = Atomic.make false;
                shutdown_mutex = Mutex.create ();
                shutdown_result = None;
              })
            (Backend.client_create ?io_threads config))

(** Validates the optional Temporal idempotency key, workflow type, durable
    workflow ID, and task queue before encoding input or constructing a native
    request. Keeping these checks here makes malformed caller input a typed
    result and prevents it from crossing the supervisor boundary. *)
let validate_start_fields ~request_id ~workflow_name ~id ~task_queue =
  let request_result =
    match request_id with
    | None -> Ok ()
    | Some request_id -> validate_name "request id" request_id
  in
  match request_result with
  | Error _ as error -> error
  | Ok () -> (
      match validate_name "workflow type" workflow_name with
      | Error _ as error -> error
      | Ok () -> (
          match validate_name "workflow id" id with
          | Error _ as error -> error
          | Ok () -> validate_name "task queue" task_queue))

(** Checks metadata keys before they reach the protocol map representation.
    Rejecting duplicates and malformed UTF-8 here keeps caller-visible
    behavior independent of Rust's map implementation and JSON validation. *)
let validate_metadata_fields label fields =
  let rec loop seen = function
    | [] -> Ok ()
    | (key, _value) :: rest ->
        if List.mem key seen then
          Error
            (Error.make ~category:`Defect
               ~message:(Printf.sprintf "duplicate %s key %S" label key) ())
        else if String.equal key "" || String.contains key '\000' then
          Error
            (Error.make ~category:`Defect
               ~message:(Printf.sprintf "invalid %s key" label) ())
        else if String.length key > 65_536 then
          Error
            (Error.make ~category:`Defect
               ~message:(Printf.sprintf "%s key exceeds protocol limit" label) ())
        else if not (Temporal_base.Codec.valid_utf_8 key) then
          Error
            (Error.make ~category:`Defect
               ~message:(Printf.sprintf "%s key must be valid UTF-8" label) ())
        else loop (key :: seen) rest
  in
  loop [] fields

(** Validates keys inside each caller-supplied payload before either backend
    sees it. Native start serializes these lists as JSON objects, while the mock
    does not serialize them, so the public boundary must enforce one contract. *)
let validate_payload_metadata label fields =
  let rec loop = function
    | [] -> Ok ()
    | (_, (payload : Payload.t)) :: rest -> (
        match
          validate_metadata_fields (label ^ " payload metadata") payload.metadata
        with
        | Error _ as error -> error
        | Ok () -> loop rest)
  in
  loop fields

(** Validates both levels of start metadata before encoding input or creating
    a mock execution. Payload metadata values remain arbitrary binary bytes. *)
let validate_start_metadata ~memo ~search_attributes =
  let validate label fields =
    Result.bind (validate_metadata_fields label fields) (fun () ->
        validate_payload_metadata label fields)
  in
  Result.bind (validate "memo" memo) (fun () ->
      validate "search attribute" search_attributes)

(** The private protocol's bounds, shared so the mock and the native bridge
    reject the same values at the public boundary. *)
module Client_protocol = Temporal_sdk_kernel.Client_protocol

(** Validates an optional caller RPC deadline and converts it to whole
    milliseconds. The deadline bounds one client call; it must be at least
    1 ms and at most one minute, because control RPCs hold the supervisor's
    owner Domain for their whole budget. Violations are typed defects, so
    every backend (including the mock, which ignores the deadline) rejects
    the same values before any request is sent. *)
let rpc_timeout_ms = function
  | None -> Ok None
  | Some timeout ->
      let milliseconds = Duration.to_ms timeout in
      if
        Int64.compare milliseconds 1L < 0
        || Int64.compare milliseconds Client_protocol.max_rpc_timeout_ms > 0
      then
        Error
          (Error.defect
             ~message:"rpc_timeout must be between 1 ms and 60 seconds")
      else Ok (Some milliseconds)

(** Validated server-side execution policies of one start, in the
    millisecond form the backend carries. *)
type start_policies = {
  execution_timeout_ms : int64 option;
  run_timeout_ms : int64 option;
  task_timeout_ms : int64 option;
  start_rpc_timeout_ms : int64 option;
}

(** Validates a start's workflow timeouts and RPC deadline before encoding
    input, mirroring the protocol encoder and the
    Rust bridge so the mock rejects the same requests. A zero timeout is
    rejected because Temporal reads zero as "unset" and would silently apply
    its default; a task timeout above 120 seconds or above the run timeout
    (the execution timeout when there is no run timeout) and a run timeout
    above the execution timeout are rejected because Temporal would silently
    lower them. Every combination of the reuse and conflict policies is
    valid. Negative and overflowing values cannot be built as [Duration.t]. *)
let validate_start_policies ~execution_timeout ~run_timeout ~task_timeout
    ~rpc_timeout =
  let timeout label maximum = function
    | None -> Ok None
    | Some duration ->
        let milliseconds = Duration.to_ms duration in
        if Int64.compare milliseconds 0L <= 0 then
          Error (Error.defect ~message:(label ^ " must be positive"))
        else if Int64.compare milliseconds maximum > 0 then
          Error (Error.defect ~message:(label ^ " exceeds its maximum"))
        else Ok (Some milliseconds)
  in
  let ( let* ) = Result.bind in
  let* execution_timeout_ms =
    timeout "execution_timeout" Client_protocol.max_workflow_timeout_ms
      execution_timeout
  in
  let* run_timeout_ms =
    timeout "run_timeout" Client_protocol.max_workflow_timeout_ms run_timeout
  in
  let* task_timeout_ms =
    timeout "task_timeout (at most 120 seconds)"
      Client_protocol.max_workflow_task_timeout_ms task_timeout
  in
  let* () =
    match (execution_timeout_ms, run_timeout_ms) with
    | Some execution, Some run when Int64.compare run execution > 0 ->
        Error
          (Error.defect ~message:"run_timeout exceeds execution_timeout")
    | _ -> Ok ()
  in
  let* () =
    (* A run without its own timeout is bounded by the execution timeout. *)
    let run =
      match run_timeout_ms with Some _ -> run_timeout_ms | None -> execution_timeout_ms
    in
    match (task_timeout_ms, run) with
    | Some task, Some run when Int64.compare task run > 0 ->
        Error
          (Error.defect
             ~message:"task_timeout exceeds the run (or execution) timeout")
    | _ -> Ok ()
  in
  let* start_rpc_timeout_ms = rpc_timeout_ms rpc_timeout in
  Ok
    {
      execution_timeout_ms;
      run_timeout_ms;
      task_timeout_ms;
      start_rpc_timeout_ms;
    }

(** Starts a workflow after encoding its typed input and checking the backend's
    response still refers to the request. The response check prevents an
    adapter bug from creating a handle for a different execution. *)
let start client ?request_id ?(memo = []) ?(search_attributes = [])
    ?(id_conflict_policy : id_conflict_policy = `Fail)
    ?(id_reuse_policy : id_reuse_policy = `Allow_duplicate) ?execution_timeout
    ?run_timeout ?task_timeout ?retry_policy ?rpc_timeout ~workflow ~task_queue
    ~id ~input () =
  if Atomic.get client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match
      validate_start_fields ~request_id ~workflow_name:(Workflow.name workflow)
        ~id ~task_queue
    with
    | Error error -> Error error
    | Ok () -> (
        match
          Result.bind (validate_start_metadata ~memo ~search_attributes)
            (fun () ->
              validate_start_policies ~execution_timeout ~run_timeout
                ~task_timeout ~rpc_timeout)
        with
        | Error error -> Error error
        | Ok policies -> (
            match Codec.encode (Workflow.input workflow) input with
        | Error error -> Error error
        | Ok encoded_input ->
            let request : Backend.start_request =
              {
                request_id;
                workflow_name = Workflow.name workflow;
                workflow_id = id;
                task_queue;
                input = encoded_input;
                memo;
                search_attributes;
                id_conflict_policy;
                id_reuse_policy;
                execution_timeout_ms = policies.execution_timeout_ms;
                run_timeout_ms = policies.run_timeout_ms;
                task_timeout_ms = policies.task_timeout_ms;
                retry_policy =
                  Option.map Retry_policy_private.to_runtime retry_policy;
                rpc_timeout_ms = policies.start_rpc_timeout_ms;
              }
            in
            Result.bind (Backend.client_start client.backend request) (fun response ->
                if not (String.equal response.workflow_id id) then
                  Error
                    (Error.make ~category:`Bridge
                       ~message:"backend returned a different workflow id" ())
                else if String.equal response.run_id "" then
                  Error
                    (Error.make ~category:`Bridge
                       ~message:"backend returned an empty run id" ())
                else
                  Ok
                    {
                      client;
                      workflow;
                      workflow_id = id;
                      run_id = Some response.run_id;
                      started = response.started;
                    })))

(** Rebuilds a typed handle for a successor run without starting another
    execution. Temporal may return a successor identity in the original run's
    failed, timed-out, or continued-as-new outcome; this operation validates that
    identity at the same boundary as [start], retains the caller's client and
    supplied workflow codecs, and leaves the exact-run choice explicit to the
    caller. Namespace equality is checked before constructing a handle so an
    execution returned by one client cannot be waited through another client's
    namespace. *)
let follow client ~workflow ({ namespace; workflow_id; run_id } : execution) =
  if Atomic.get client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match validate_name "successor namespace" namespace with
    | Error error -> Error error
    | Ok () -> (
        if not (String.equal namespace client.namespace) then
          Error
            (Error.defect
               ~message:"successor execution belongs to a different namespace")
        else
          match validate_name "successor workflow id" workflow_id with
          | Error error -> Error error
          | Ok () -> (
              match validate_name "successor run id" run_id with
              | Error error -> Error error
              | Ok () ->
                  Ok
                    {
                      client;
                      workflow;
                      workflow_id;
                      run_id = Some run_id;
                      started = false;
                    }))

(** Builds a handle from a workflow ID the caller already knows, without any
    server round trip. The identifiers are validated at the same boundary as
    [start] so a malformed value is a typed defect rather than a protocol
    error. An omitted [run_id] yields a current-run handle; see the interface
    for the semantics of each operation on it. *)
let get_handle client ?run_id ~workflow ~id () =
  if Atomic.get client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match validate_name "workflow id" id with
    | Error error -> Error error
    | Ok () -> (
        let run_result =
          match run_id with
          | None -> Ok ()
          | Some run_id -> validate_name "run id" run_id
        in
        match run_result with
        | Error error -> Error error
        | Ok () ->
            Ok { client; workflow; workflow_id = id; run_id; started = false })

(** The run selector sent to the backend: the exact run ID, or the empty
    string that asks Temporal to resolve the workflow's current run. Exact run
    IDs are validated non-empty, so the two cases cannot be confused. *)
let run_selector (handle : ('input, 'output) handle) =
  Option.value handle.run_id ~default:""

(** Returns the next run a current-run wait must observe: the successor that
    a continued-as-new, cron, or retry close event started, or [None] when the
    observed run ended the chain. This mirrors the official SDKs' default
    run-following result for a handle obtained by workflow ID. *)
let chain_successor = function
  | Backend.Continued_as_new successor
  | Backend.Completed { successor = Some successor; _ }
  | Backend.Failed { successor = Some successor; _ }
  | Backend.Timed_out { successor = Some successor; _ } ->
      Some successor
  | Backend.Completed { successor = None; _ }
  | Backend.Failed { successor = None; _ }
  | Backend.Timed_out { successor = None; _ }
  | Backend.Cancelled _ | Backend.Terminated _ ->
      None

(** Waits for one backend terminal result. A current-run handle starts from
    the run Temporal resolves for an empty run ID and then waits on each
    successor by its exact run ID until a run closes without one, so the
    result is the last run's outcome and never [Continued_as_new]. Each step is
    a separate bounded backend wait, so client shutdown still interrupts the
    chain between and during steps. *)
let wait_backend (handle : ('input, 'output) handle) =
  let wait_run run_id =
    Backend.client_wait handle.client.backend
      ({ workflow_id = handle.workflow_id; run_id } : Backend.wait_request)
  in
  match handle.run_id with
  | Some run_id -> wait_run run_id
  | None ->
      let rec follow_chain run_id =
        match wait_run run_id with
        | Error _ as error -> error
        | Ok terminal -> (
            match chain_successor terminal with
            | None -> Ok terminal
            | Some (successor : Backend.successor) -> follow_chain successor.run_id)
      in
      follow_chain ""

(** Decodes a completed payload and maps terminal failures without exposing the
    private backend constructors. Each successor gains this client's namespace
    only after the protocol has checked it belongs to the waited execution. *)
let wait (handle : ('input, 'output) handle) =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    (* The backend keeps the validated run pair; the owning client supplies
       the namespace needed by [follow]. *)
    let public_successor =
      Option.map (fun (value : Backend.successor) ->
          {
            namespace = handle.client.namespace;
            workflow_id = value.workflow_id;
            run_id = value.run_id;
          })
    in
    Result.bind (wait_backend handle) (function
      | Backend.Completed { payload; successor } ->
          Result.map
            (fun output ->
              Completed { output; successor = public_successor successor })
            (Codec.decode (Workflow.output handle.workflow) payload)
      | Backend.Failed { error; successor } ->
          Ok (Failed { error; successor = public_successor successor })
      | Backend.Cancelled error -> Ok (Cancelled error)
      | Backend.Terminated error -> Ok (Terminated error)
      | Backend.Timed_out { error; successor } ->
          Ok (Timed_out { error; successor = public_successor successor })
      | Backend.Continued_as_new { workflow_id; run_id } ->
          Ok
            (Continued_as_new
               { namespace = handle.client.namespace; workflow_id; run_id }))

(** Validates cancellation metadata before it reaches the backend. The length
    limit protects the JSON bridge and matches the Rust-side bound; NUL is
    rejected because it cannot be represented safely by the C ABI contract. *)
let validate_cancel_fields ~request_id ~reason =
  let request_result =
    match request_id with
    | None -> Ok ()
    | Some request_id -> validate_name "cancellation request id" request_id
  in
  match request_result with
  | Error _ as error -> error
  | Ok () when String.length reason > 65_536 ->
      Error
        (Error.defect
           ~message:"cancellation reason exceeds the protocol safety limit")
  | Ok () when String.contains reason '\000' ->
      Error (Error.defect ~message:"cancellation reason must not contain NUL")
  | Ok () -> Ok ()

(** Validates signal metadata before encoding input or entering the backend.
    Signal names are validated when their definitions are built; the request
    ID still belongs to this particular delivery and is checked here. *)
let validate_signal_fields ~request_id =
  let request_result =
    match request_id with
    | None -> Ok ()
    | Some request_id -> validate_name "signal request id" request_id
  in
  request_result

(** Allocates a stable request ID for a cancellation call whose caller did not
    provide one. Hashing the handle's execution selector makes repeated calls
    on the same handle represent one idempotent control operation without
    keeping another mutable counter in the client state. A current-run handle
    hashes the empty selector; Temporal deduplicates cancellation request IDs
    per run, so the same key still cancels a later run of the workflow. *)
let generated_cancel_request_id (handle : ('input, 'output) handle) =
  "ocaml-client-cancel-"
  ^ Digest.to_hex
      (Digest.string (handle.workflow_id ^ "\000" ^ run_selector handle))

(** Sends a cancellation request for the run [handle] addresses and returns only after the
    server acknowledgement has been decoded. This operation is deliberately
    separate from [wait], because Temporal cancellation is asynchronous. *)
let cancel ?request_id ?(reason = "") ?rpc_timeout
    (handle : ('workflow_input, 'workflow_output) handle) =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match
      Result.bind (validate_cancel_fields ~request_id ~reason) (fun () ->
          rpc_timeout_ms rpc_timeout)
    with
    | Error error -> Error error
    | Ok rpc_timeout_ms ->
        let request_id =
          match request_id with
          | Some request_id -> request_id
          | None -> generated_cancel_request_id handle
        in
        let request : Backend.cancel_request =
          {
            workflow_id = handle.workflow_id;
            run_id = run_selector handle;
            request_id;
            reason;
            rpc_timeout_ms;
          }
        in
        Backend.client_cancel handle.client.backend request

(** Validates operator reason text before it crosses either the deterministic
    mock or native JSON bridge. *)
let validate_terminate_reason reason =
  if String.length reason > 65_536 then
    Error
      (Error.defect
         ~message:"termination reason exceeds the protocol safety limit")
  else if String.contains reason '\000' then
    Error (Error.defect ~message:"termination reason must not contain NUL")
  else Ok ()

(** Terminates the run [handle] addresses. The acknowledgement is deliberately separate
    from [wait], which observes the server's terminal history event. *)
let terminate ?(reason = "") ?rpc_timeout (handle : ('input, 'output) handle) =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match
      Result.bind (validate_terminate_reason reason) (fun () ->
          rpc_timeout_ms rpc_timeout)
    with
    | Error error -> Error error
    | Ok rpc_timeout_ms ->
        let request : Backend.terminate_request =
          {
            workflow_id = handle.workflow_id;
            run_id = run_selector handle;
            reason;
            rpc_timeout_ms;
          }
        in
        Backend.client_terminate handle.client.backend request

(** Validates reset metadata before the request crosses the serialized
    supervisor. Temporal event IDs are exact signed integers, never floats. *)
let validate_reset_fields ~request_id ~reason ~workflow_task_finish_event_id =
  let request_result =
    match request_id with
    | None -> Ok ()
    | Some request_id -> validate_name "reset request id" request_id
  in
  match request_result with
  | Error _ as error -> error
  | Ok () when workflow_task_finish_event_id <= 1L ->
      Error
        (Error.defect
           ~message:"reset workflow-task finish event ID must be greater than 1")
  | Ok () when String.length reason > 65_536 ->
      Error
        (Error.defect ~message:"reset reason exceeds the protocol safety limit")
  | Ok () when String.contains reason '\000' ->
      Error (Error.defect ~message:"reset reason must not contain NUL")
  | Ok () -> Ok ()

(** Allocates a fresh request ID for a reset whose caller did not supply one.
    Resetting the same run at the same event again is a legitimate operator
    action (for example when the first successor also went wrong), so each
    call must be a distinct server request; a key derived from the run and
    event would make the server silently return the earlier successor. Callers
    that need idempotent retries of one logical reset, for example after an
    uncertain transport result, pass an explicit [request_id]. *)
let generated_reset_request_id = Temporal_base.Client_request_id.create

(** Resets the run [handle] addresses and returns the new execution identity. A successful
    acknowledgement does not imply the new run has completed; use [follow] and
    [wait] to observe it explicitly. *)
let reset ?request_id ?(reason = "") ?rpc_timeout ~workflow_task_finish_event_id
    (handle : ('input, 'output) handle) =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match
      Result.bind
        (validate_reset_fields ~request_id ~reason ~workflow_task_finish_event_id)
        (fun () -> rpc_timeout_ms rpc_timeout)
    with
    | Error error -> Error error
    | Ok rpc_timeout_ms ->
        let request_id =
          match request_id with
          | Some request_id -> request_id
          | None -> generated_reset_request_id ()
        in
        let request : Backend.reset_request =
          {
            workflow_id = handle.workflow_id;
            run_id = run_selector handle;
            request_id;
            reason;
            workflow_task_finish_event_id;
            rpc_timeout_ms;
          }
        in
        (match Backend.client_reset handle.client.backend request with
        | Error error -> Error error
        | Ok (response : Backend.reset_response) ->
            Ok
              ({
                 namespace = handle.client.namespace;
                 workflow_id = response.workflow_id;
                 run_id = response.run_id;
               } : execution))

(** Allocates a fresh request ID for a signal when the caller did not supply
    one. Unlike cancellation, separate signal calls are distinct messages by
    default, even when they target the same run and signal name. Supplying an
    explicit ID gives a caller retry-safe idempotency semantics. *)
let generated_signal_request_id = Temporal_base.Client_request_id.create

(** Sends one typed signal to the run [handle] addresses. The input is
    encoded before the backend call, and success means only that Temporal
    acknowledged the RPC; workflow code may process it asynchronously. *)
let signal ?request_id ?rpc_timeout
    (handle : ('workflow_input, 'workflow_output) handle)
    ~(signal : 'signal Signal.t) ~input =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match
      Result.bind (validate_signal_fields ~request_id) (fun () ->
          rpc_timeout_ms rpc_timeout)
    with
    | Error error -> Error error
    | Ok rpc_timeout_ms -> (
        match Codec.encode (Signal.input signal) input with
        | Error error -> Error error
        | Ok encoded_input ->
            let request_id =
              match request_id with
              | Some request_id -> request_id
              | None -> generated_signal_request_id ()
            in
            let request : Backend.signal_request =
              {
                workflow_id = handle.workflow_id;
                run_id = run_selector handle;
                signal_name = Signal.name signal;
                request_id;
                input = encoded_input;
                rpc_timeout_ms;
              }
            in
            Backend.client_signal handle.client.backend request)

(** Executes one output-only query against the run [handle] addresses.
    Query arguments are intentionally absent in this first client slice: the
    workflow-side [Query] definition is already output-only, and the result is
    decoded with the definition's codec only after the native bridge has
    validated the response payload. *)
let query ?rpc_timeout (handle : ('workflow_input, 'workflow_output) handle)
    ~(query : 'query Query.t) =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match rpc_timeout_ms rpc_timeout with
    | Error error -> Error error
    | Ok rpc_timeout_ms ->
        let request : Backend.query_request =
          {
            workflow_id = handle.workflow_id;
            run_id = run_selector handle;
            query_name = Query.name query;
            input = [];
            rpc_timeout_ms;
          }
        in
        Result.bind (Backend.client_query handle.client.backend request)
          (fun payload -> Codec.decode (Query.output query) payload)

(** Lists one bounded visibility page through the client's backend. The public
    layer validates caller-controlled query metadata before the request enters
    either the deterministic mock or the native supervisor. *)
let list_visibility ?(page_size = 100) ?page_token ?rpc_timeout client ~query
    () =
  (* The deadline is checked before the closed flag, so a malformed deadline
     is reported as a defect even on a closed client. *)
  match rpc_timeout_ms rpc_timeout with
  | Error error -> Error error
  | Ok rpc_timeout_ms ->
  if Atomic.get client.closed then
    Error (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else if page_size < 1 || page_size > 1_000 then
    Error
      (Error.defect
         ~message:"visibility page_size must be between 1 and 1000")
  else
    if String.length query > 65_536 then
      Error
        (Error.defect
           ~message:"visibility query exceeds the protocol safety limit")
    else if String.contains query '\000' then
      Error (Error.defect ~message:"visibility query must not contain NUL")
    else (
        match page_token with
        | Some token when String.equal token "" ->
            Error
              (Error.defect ~message:"visibility page token must not be empty")
        | Some token -> (
            match validate_name "visibility page token" token with
            | Error error -> Error error
            | Ok () ->
                let request : Backend.visibility_request =
                  {
                    query;
                    page_size;
                    next_page_token = Some token;
                    rpc_timeout_ms;
                  }
                in
                Result.map
                  (fun (page : Backend.visibility_page) ->
                    {
                      executions =
                        List.map
                          (fun (execution : Backend.visibility_execution) ->
                            {
                              workflow_id = execution.workflow_id;
                              run_id = execution.run_id;
                              workflow_type = execution.workflow_type;
                              task_queue = execution.task_queue;
                              status = execution.status;
                            })
                          page.executions;
                      next_page_token = page.next_page_token;
                    })
                  (Backend.client_list_visibility client.backend request))
        | None ->
            let request : Backend.visibility_request =
              { query; page_size; next_page_token = None; rpc_timeout_ms }
            in
            Result.map
              (fun (page : Backend.visibility_page) ->
                {
                  executions =
                    List.map
                      (fun (execution : Backend.visibility_execution) ->
                        {
                          workflow_id = execution.workflow_id;
                          run_id = execution.run_id;
                          workflow_type = execution.workflow_type;
                          task_queue = execution.task_queue;
                          status = execution.status;
                        })
                      page.executions;
                  next_page_token = page.next_page_token;
                })
              (Backend.client_list_visibility client.backend request))

(** Executes a one-input typed query against the run [handle] addresses. Encoding happens before transport, so invalid input cannot
    consume a native request or become an ambiguous empty query. *)
let query_with_input ?rpc_timeout
    (handle : ('workflow_input, 'workflow_output) handle)
    ~(query : ('input, 'query) Query.typed) ~input =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match
      Result.bind (rpc_timeout_ms rpc_timeout) (fun rpc_timeout_ms ->
          Result.map
            (fun encoded_input -> (rpc_timeout_ms, encoded_input))
            (Codec.encode (Query.input query) input))
    with
    | Error error -> Error error
    | Ok (rpc_timeout_ms, encoded_input) ->
        let request : Backend.query_request =
          {
            workflow_id = handle.workflow_id;
            run_id = run_selector handle;
            query_name = Query.name_with_input query;
            input = [ encoded_input ];
            rpc_timeout_ms;
          }
        in
        Result.bind
           (Backend.client_query handle.client.backend request)
           (fun payload -> Codec.decode (Query.output_with_input query) payload)

(** Allocates an independent update identity across client instances and
    processes. Callers supply an explicit ID to reconcile an uncertain retry. *)
let generated_update_id = Temporal_base.Client_request_id.create

(** Canonical payload used when an update returns no result values. Temporal
    represents unit-like values with the same binary/null marker used by the
    exact-run workflow wait path. *)
let update_unit_payload : Payload.t =
  { Payload.metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }

(** Decodes the result cardinality promised by one update definition. *)
let decode_update_output definition = function
  | Backend.Update_completed [] ->
      Codec.decode (Update.output definition) update_unit_payload
  | Backend.Update_completed [ payload ] ->
      Codec.decode (Update.output definition) payload
  | Backend.Update_completed _ ->
      Error
        (Error.make ~category:`Codec
           ~message:"workflow update returned multiple result payloads" ())
  | Backend.Update_failed error -> Error error

(** Starts one typed update and returns once Temporal has accepted it. This
    acceptance/completion split lets callers issue several updates before
    waiting, while the supervisor still serializes every native operation. *)
let start_update ?update_id ?rpc_timeout
    (handle : ('workflow_input, 'workflow_output) handle)
    ~(update : ('input, 'output) Update.t) ~input () =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    let update_id =
      match update_id with
      | Some update_id -> update_id
      | None -> generated_update_id ()
    in
    match
      Result.bind (validate_name "update id" update_id) (fun () ->
          rpc_timeout_ms rpc_timeout)
    with
    | Error error -> Error error
    | Ok rpc_timeout_ms -> (
        match Codec.encode (Update.input update) input with
        | Error error -> Error error
        | Ok encoded_input ->
            let request : Backend.update_request =
              {
                workflow_id = handle.workflow_id;
                run_id = run_selector handle;
                update_id;
                update_name = Update.name update;
                input = encoded_input;
                rpc_timeout_ms;
              }
            in
            Result.bind
              (Backend.client_update handle.client.backend request)
              (fun (response : Backend.update_response) ->
                match response.outcome with
                | Some (Backend.Update_failed error) -> Error error
                | outcome ->
                    Ok
                      {
                        client = handle.client;
                        definition = update;
                        workflow_id = response.workflow_id;
                        run_id = response.run_id;
                        update_id = response.update_id;
                        input = encoded_input;
                        outcome;
                      }))

(** Waits for one admitted update, retrying bounded polls until Temporal
    supplies a terminal outcome. Each retry occurs on the caller Domain; the
    Rust bridge releases the OCaml runtime lock while its gRPC poll is active.
    Outcomes already supplied at admission are decoded without another RPC. *)
let wait_update handle =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match handle.outcome with
    | Some outcome -> decode_update_output handle.definition outcome
    | None ->
        let request : Backend.update_request =
          {
            workflow_id = handle.workflow_id;
            run_id = handle.run_id;
            update_id = handle.update_id;
            update_name = Update.name handle.definition;
            input = handle.input;
            (* Completion polls keep their own bounded window; see
               [Client.wait_update]. *)
            rpc_timeout_ms = None;
          }
        in
        let rec poll () =
          match Backend.client_poll_update handle.client.backend request with
          | Error error -> Error error
          | Ok { Backend.outcome = None } ->
              Thread.yield ();
              poll ()
          | Ok { Backend.outcome = Some outcome } ->
              decode_update_output handle.definition outcome
        in
        poll ()

(** Returns the durable update ID retained by a typed handle. *)
let update_id (handle : ('input, 'output) update_handle) = handle.update_id

(** Returns the durable workflow identity retained by a handle. *)
let workflow_id (handle : ('input, 'output) handle) = handle.workflow_id

(** Returns the exact server run identity retained by a handle, or [None]
    for a current-run handle. *)
let run_id (handle : ('input, 'output) handle) = handle.run_id

(** Reports whether the [start] that produced [handle] created its run. *)
let started (handle : ('input, 'output) handle) = handle.started

(** Exposes the conflicting run attached by the backend as a public
    [execution]. The backend owns the detail encoding so the mock and native
    transports cannot drift apart. *)
let already_started error =
  Option.map
    (fun (namespace, workflow_id, run_id) -> { namespace; workflow_id; run_id })
    (Backend.already_started_execution error)

(** Recognizes the uncertain-start error by its structural fields rather than
    its diagnostic message. *)
let is_start_outcome_uncertain error =
  let view = Error.view error in
  view.category = `Bridge && view.non_retryable
  && view.error_type = Some Backend.start_outcome_uncertain_error_type

(** Recognizes the backend's capacity rejection by its structural fields
    rather than its diagnostic message, so wording changes cannot alter the
    classification. *)
let is_at_capacity error =
  let view = Error.view error in
  view.category = `Bridge
  && (not view.non_retryable)
  && view.error_type = Some Backend.client_at_capacity_error_type

(** The public RPC classification; structurally identical to
    [Backend.rpc_status], which owns the code table. *)
type rpc_status = Backend.rpc_status

(** Delegates to the backend, which builds these errors, so the
    classification cannot drift from their construction. *)
let rpc_status = Backend.rpc_status

(** Delegates to the backend's structural query-failure check. *)
let is_query_failed = Backend.is_query_failed

(** Closes backend resources once and returns the same cached result to later
    shutdown callers.
    Native supervisor shutdown is terminal and cached: even when its result is
    an error, the backend contract says the complete graph was consumed or
    invalidated, so the atomic state transition cannot hide a live resource. *)
let shutdown client =
  Mutex.lock client.shutdown_mutex;
  (* [Fun.protect] is the single release path so a concurrent caller cannot be
     left waiting if backend teardown raises before it can return a result. *)
  Fun.protect
    ~finally:(fun () -> Mutex.unlock client.shutdown_mutex)
    (fun () ->
      match client.shutdown_result with
      | Some result -> result
      | None ->
          (* Close admission before entering native teardown. Concurrent starts
             that already passed their check are ordered by the supervisor; later
             callers observe the closed bit and cannot enqueue new work. A
             native start already holding a ticket is aborted by teardown and
             reports an uncertain outcome instead of failing this shutdown. *)
          Atomic.set client.closed true;
          let result =
            try Backend.client_shutdown client.backend with
            | exception_ ->
                Error
                  (Error.defect
                     ~message:
                       (Printf.sprintf "client shutdown raised: %s"
                          (Printexc.to_string exception_)))
          in
          client.shutdown_result <- Some result;
          result)
