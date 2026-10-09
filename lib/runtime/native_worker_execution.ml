(** Private worker-loop state for the first live OCaml workflow execution
    boundary.

    Rust/Core ownership and semantic JSON validation live below this module in
    the supervisor. This module therefore receives typed activations, resolves
    each run ID to an existentially typed [Execution.t], and sends each
    completion back through the same supervisor as the canonical bytes from
    its single encoder pass. The registry is mutable only behind one mutex; it
    is never shared with workflow fibers or native code. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Definition = Temporal_base.Definition
module Codec = Temporal_base.Codec
module Base_error = Temporal_base.Error
module Observability = Temporal_base.Observability
module Encoded_completion = Temporal_protocol.Encoded_workflow_completion

(** Result-bind notation keeps all expected boundary failures on typed paths. *)
let ( let* ) = Result.bind

(** The source-side operations that this adapter needs. The signature is kept
    independent of [Sdk_supervisor.Native] so semantic execution can be tested
    with a deterministic queue and can be wired to any future readiness API. *)
module type SUPERVISOR = sig
  type t
  type error

  val try_poll_workflow :
    t -> (Protocol.activation option, error) result

  (* [completion] is the read-only typed source of the encoded bytes, for
     inspection by test sources; the bytes are authoritative. *)
  val complete_workflow :
    t -> completion:Protocol.completion -> Encoded_completion.t ->
    (unit, error) result

  val error_code : error -> string
  val error_message : error -> string

  (* Returns [true] only when the source can prove that a failed completion
     left the exact native lease outstanding, so resubmitting the same retained
     completion cannot duplicate it. Every other failure is fail-closed. *)
  val error_is_retryable : error -> bool

  (* The same proof for an exception raised by [complete_workflow]. An
     exception is normally an uncertain acknowledgement and must return
     [false]. *)
  val exception_is_retryable : exn -> bool
end

(** Stable diagnostics deliberately contain no payload bytes or native values. *)
type error_view = { code : string; path : string; message : string }

(** Replay metadata exposed only to the private worker diagnostic hook. Payloads,
    continuations, and native handles remain inside the execution boundary. *)
type activation_info = {
  run_id : string;
  workflow_id : string option;
  is_replaying : bool;
  history_length : int64;
  cache_removal_reason : string option;
}

(** What the watchdog's replacement completion was and whether the supervisor
    acknowledged it. The kind mirrors [failure_completion]: an ordinary
    activation fails its workflow task ([`Task_failed]); a query-only
    activation answers every query with a failure and leaves the task itself
    untouched ([`Queries_failed]); an eviction-only activation is acknowledged
    with an empty completion ([`Eviction_acknowledged]). [`Not_acknowledged]
    means encoding or submission failed, so Core never received a completion
    for the lease and the task, query, or eviction is left to time out. *)
type abandonment =
  [ `Task_failed | `Queries_failed | `Eviction_acknowledged | `Not_acknowledged ]

(** Diagnostic identity of a workflow activation abandoned by the
    non-yielding-code watchdog (#493). It carries only identifiers, the replay
    flag, and the observed duration: never payloads, task tokens, or
    continuation state. [elapsed_ms] is the watchdog's own lower bound on how
    long the activation had been running without returning to the adapter.
    [abandoned] records what the watchdog submitted in place of the lane's
    completion and whether the supervisor acknowledged it; see
    {!abandonment}. *)
type stuck_activation = {
  run_id : string;
  workflow_id : string option;
  workflow_type : string option;
  is_replaying : bool;
  elapsed_ms : int;
  abandoned : abandonment;
}

(** Single-owner claim on the native lease of the activation currently inside
    workflow code. Exactly one party moves it out of [Unclaimed]: the workflow
    lane before it submits its own completion, or the watchdog before it
    submits a task failure. The loser never submits a completion for that
    lease. *)
type lease_claim = Unclaimed | Lane_claimed | Watchdog_claimed

(** The activation the workflow lane is processing, published atomically so
    the watchdog Domain can observe it without the adapter mutex (which the
    lane holds for the whole activation). [epoch] distinguishes successive
    activations, including two activations of the same run. Every field except
    the two atomics is immutable after publication. *)
type in_flight = {
  epoch : int;
  activation : Protocol.activation;
  flight_workflow_id : string option;
  flight_workflow_type : string option;
  claim : lease_claim Atomic.t;
  (* Written once by the watchdog after its failure submission returns, so a
     lane that later loses the claim can report whether the lease was
     retired. *)
  watchdog_outcome : (unit, error_view) result option Atomic.t;
}

(** Runtime signal and query handler aliases kept private to this adapter. *)
type signal = Execution.signal
type signal_handler = Execution.signal_handler
type query = Execution.query
type query_handler = Execution.query_handler
type update = Execution.update
type update_handler = Execution.update_handler

(** The public-facing existential registration. Its constructor remains
    private so callers can only produce values through [register]. *)
type registered_workflow =
  | Workflow :
      ('input, 'output,
       'input -> ('output, Base_error.t) result)
      Definition.t * Execution.signal_handler list * Execution.query_handler list
      * Execution.update_handler list ->
      registered_workflow

(** Builds the private scheduler callback package without widening the public
    native worker boundary. *)
let make_signal_handler = Execution.make_signal_handler

(** Returns the payload sequence retained by the execution runtime. *)
let signal_input (signal : Execution.signal) = signal.input

(** Returns the sender identity retained by the execution runtime. *)
let signal_identity (signal : Execution.signal) = signal.identity

(** Returns the signal headers retained by the execution runtime. *)
let signal_headers (signal : Execution.signal) = signal.headers

(** Returns a handler's stable signal name. *)
let signal_handler_name = Execution.signal_handler_name

(** Builds a private synchronous query callback package. *)
let make_query_handler = Execution.make_query_handler

(** Returns query arguments retained at the native boundary. *)
let query_arguments (query : Execution.query) = query.arguments

(** Returns query headers retained at the native boundary. *)
let query_headers (query : Execution.query) = query.headers

(** Returns a handler's stable query name. *)
let query_handler_name = Execution.query_handler_name

(** Builds a private update callback package. *)
let make_update_handler = Execution.make_update_handler

(** Returns a handler's stable update registration name. *)
let update_handler_name = Execution.update_handler_name

(** Returns all payloads carried by an update activation. *)
let update_input (update : Execution.update) = update.input

(** One typed execution hidden behind the run-ID map. Both the definition and
    execution share the same input/output type parameters, which prevents a
    completion from being encoded with the wrong codec. *)
type run =
  | Run :
      {
        definition :
          ('input, 'output,
           'input -> ('output, Base_error.t) result)
          Definition.t;
        execution : ('input, 'output) Execution.t;
        (* Retained from initialization so watchdog diagnostics for later
           activations of the run can name the workflow execution. *)
        workflow_id : string;
      }
      -> run

(** At most one run can be leased for each Temporal run ID. *)
module Run_map = Map.Make (String)

(** A completion retained after the native call did not acknowledge it. The
    completion is kept together with the bookkeeping that must happen only
    after acknowledgement, so retrying cannot execute the workflow twice or
    remove its run state prematurely. *)
type pending_result =
  | Pending_completed of {
      command_count : int;
      terminal : bool;
      evicted : bool;
      (* Completion observers must run after native acknowledgement, including
         when an initially rejected completion succeeds on a later poll. Keep
         the immutable metadata beside the retained completion so retry cannot
         lose the observer event or reconstruct it from already-dropped state. *)
      activation_info : activation_info;
    }
  | Pending_rejected of {
      error : error_view;
      remove_run : bool;
    }

(** The completion is owned by this adapter until the supervisor accepts it.

    [submission] holds the canonical bytes from the completion's single
    encoder pass (issue #846). They are an immutable snapshot, so retaining
    them needs no payload copy and a retry resubmits exactly the same bytes.
    The typed completion is kept beside them only so the source can inspect
    it without decoding; it is never encoded again.
    [Error] records a completion the encoder rejected: it can never be
    submitted, so its first attempt fails closed exactly as a non-retryable
    supervisor rejection did when the supervisor ran the encoder.

    [retry_refusal] records the first submission failure that the source did
    not explicitly classify as retryable. Once it is [Some], the completion is
    never submitted again: the failure may have consumed the native lease (or
    Core may already have accepted the value), so a second attempt could
    duplicate the completion or attach it to a later activation of the same
    run. Later polls and drains return the recorded error unchanged, and only
    terminal [discard] releases the entry (issue #843). *)
type pending_completion = {
  run_id : string;
  submission : (Native_execution.encoded_completion, error_view) result;
  result : pending_result;
  mutable retry_refusal : error_view option;
}

(** One worker-loop result. *)
type outcome =
  | Not_ready
  | Completed of {
      run_id : string;
      command_count : int;
      terminal : bool;
    }
  | Rejected of {
      run_id : string option;
      error : error_view;
      lease_retired : bool;
    }

(** Existentially typed definition map values. *)
type registered_definition =
  | Registered_definition :
      ('input, 'output,
       'input -> ('output, Base_error.t) result)
      Definition.t * Execution.signal_handler list * Execution.query_handler list
      * Execution.update_handler list ->
      registered_definition

(** Bounds diagnostics that may contain application codec messages before they
    enter Logs or a Temporal failure. Invalid UTF-8 is replaced because protocol
    string fields are strict UTF-8; truncation never splits a multibyte code
    unit, matching the activity adapter. *)
let bounded_message value =
  let maximum = 1_024 in
  let fallback = "invalid workflow diagnostic" in
  if not (Temporal_base.Codec.valid_utf_8 value) then fallback
  else if String.length value <= maximum then value
  else
    (* Back off one byte at a time until the truncated prefix is valid UTF-8;
       the protocol must never receive a diagnostic split in the middle of a
       multibyte code point. *)
    let rec prefix length =
      if length <= 0 then fallback
      else
        let candidate = String.sub value 0 length in
        if Temporal_base.Codec.valid_utf_8 candidate then candidate ^ "..."
        else prefix (length - 1)
    in
    prefix (maximum - 3)

(** Bounds arbitrary source classifications before they reach the stable
    adapter error view. *)
let bounded_code value =
  let maximum = 128 in
  if String.length value <= maximum then value
  else String.sub value 0 (maximum - 3) ^ "..."

(** Creates an immutable diagnostic in one place so all branches preserve the
    same privacy and size rules. *)
let make_error ?(path = "$") code message : error_view =
  { code = bounded_code code; path; message = bounded_message message }

(** Converts an unexpected OCaml exception into a bounded diagnostic. The
    adapter catches such exceptions at the lease boundary so a defect in a
    codec or scheduler cannot unwind past [poll] while leaving a native lease
    silently unacknowledged. Exceptions still indicate defects, not ordinary
    workflow failures; this conversion is a last-resort cleanup guard. *)
let exception_error ?(path = "$") exception_ =
  let message =
    try Printexc.to_string exception_ with _ -> "unprintable OCaml exception"
  in
  make_error ~path "ocaml_exception" message

(** Converts a supervisor error without trusting its accessor functions to be
    exception-free. A broken diagnostic accessor must not make lease handling
    itself escape the typed error boundary. *)
let supervisor_error ?(path = "$") ~error_code ~error_message source_error =
  try
    make_error ~path
      (error_code source_error)
      (error_message source_error)
  with exception_ -> exception_error ~path exception_

(** Converts a native execution diagnostic without exposing its representation. *)
let native_error error =
  let view = Native_execution.error_view error in
  make_error ~path:view.path view.code view.message

(** Converts an application error from a workflow codec or implementation into
    a bridge failure description. Details remain in the typed error only until
    this point and are deliberately not copied into the message. *)
let application_error ?(path = "$") error =
  let view = Base_error.view error in
  make_error ~path (Base_error.kind error) view.message

(** Builds a non-retryable Temporal application failure for an adapter-level
    rejection. The failure is submitted through the ordinary completion path,
    which lets Core retire the exact lease instead of abandoning it. *)
let failure_of_error (error : error_view) : Protocol.failure =
  Protocol.
    {
      message = error.message;
      source = "ocaml-temporal";
      stack_trace = "";
      encoded_attributes = None;
      cause = None;
      info =
        Application
          {
            type_name = "ocaml_temporal_native_worker";
            non_retryable = true;
            details = [];
            category = Application_category_unspecified;
            next_retry_delay = None;
          };
    }

(** Converts one protocol payload into the runtime payload representation. The
    runtime stores metadata as strings, so binary metadata is rejected rather
    than decoded with replacement characters. Both metadata and body bytes are
    copied before a workflow execution can retain them. *)
let runtime_payload path (payload : Protocol.payload) =
  (* Validate metadata in input order while accumulating it backwards; the
     final reversal restores the protocol's declared ordering without a
     quadratic append. *)
  let rec metadata_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | (key, bytes) :: rest ->
        if String.length key = 0 || String.contains key '\000' then
          Error
            (make_error ~path:(path ^ ".metadata") "invalid_message"
               "metadata key must be non-empty and must not contain NUL")
        else
          let value = Bytes.to_string bytes in
          if not (Codec.valid_utf_8 value) then
            Error
              (make_error ~path:(path ^ ".metadata." ^ key) "unsupported"
                 "binary metadata cannot be represented by the runtime")
          else metadata_loop ((key, value) :: reversed) rest
  in
  let* metadata = metadata_loop [] payload.metadata in
  Ok
    {
      Temporal_base.Payload.metadata;
      data = Bytes.copy payload.data;
    }

(** The protocol uses an argument list, while a typed OCaml workflow definition
    accepts one value. Zero arguments are interpreted as the canonical unit
    payload; one argument is decoded normally and more than one is rejected so
    the adapter never silently drops a Core argument. *)
let decode_input definition arguments =
  let payload_result =
    match arguments with
    | [] -> Ok { Temporal_base.Payload.metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }
    | [ payload ] -> runtime_payload "$.jobs[0].arguments[0]" payload
    | _ ->
        Error
          (make_error ~path:"$.jobs[0].arguments" "unsupported"
             "workflow definitions currently accept exactly one input value")
  in
  let* payload = payload_result in
  match Codec.decode (Definition.input definition) payload with
  | Ok input -> Ok input
  | Error error -> Error (application_error ~path:"$.jobs[0].arguments" error)

(** Returns the one initialization record and rejects duplicate start markers.
    Initialization is expected to be the first job because later jobs may refer
    to the execution it creates; accepting a later marker would make run
    registration order-dependent. *)
let initialization (activation : Protocol.activation) :
    (Native_execution.initialization option, error_view) result =
  (* Track the first job index as well as the initialization value so a marker
     that appears after any other job is rejected deterministically. *)
  let rec collect index found = function
    | [] -> (
        match found with
        | Some (0, init) -> Ok (Some init)
        | Some (_, _) ->
            Error
              (make_error ~path:"$.jobs" "invalid_message"
                 "Initialize_workflow must be the first activation job")
        | None -> Ok None)
    | Protocol.Initialize_workflow
        { workflow_id; workflow_type; arguments; randomness_seed; attempt; context }
      :: rest -> (
        match found with
        | None ->
            let init : Native_execution.initialization =
              {
                workflow_id;
                workflow_type;
                arguments;
                randomness_seed;
                attempt;
                context;
              }
            in
            collect (index + 1) (Some (index, init)) rest
        | Some _ ->
            Error
              (make_error ~path:(Printf.sprintf "$.jobs[%d]" index)
                 "invalid_message"
                 "activation contains more than one Initialize_workflow job"))
    | _ :: rest -> collect (index + 1) found rest
  in
  collect 0 None activation.jobs

(** Checks whether a completion contains a terminal command. The semantic
    protocol encoder has already validated that any terminal command is last. *)
let is_terminal completion =
  List.exists
    (function
      | Protocol.Complete_workflow _
      | Protocol.Fail_workflow _
      | Protocol.Continue_as_new _
      | Protocol.Cancel_workflow_execution -> true
      | Protocol.Schedule_activity _
      | Protocol.Schedule_local_activity _
      | Protocol.Start_child_workflow _
      | Protocol.Cancel_child_workflow _
      | Protocol.Signal_external_workflow _
      | Protocol.Request_cancel_external_workflow _
      | Protocol.Request_cancel_activity _
      | Protocol.Request_cancel_local_activity _
      | Protocol.Start_timer _
      | Protocol.Cancel_timer _
      | Protocol.Query_result _
      | Protocol.Update_response _
      | Protocol.Set_patch_marker _
      | Protocol.Upsert_search_attributes _ -> false)
    completion.Protocol.commands

(** Reports one bounded lifecycle message without allowing a reporter defect to
    affect worker progress. *)
let report level ~operation ?error_kind () =
  try
    let tags = Observability.tags ~operation ?error_kind () in
    Observability.report ~src:Observability.Source.lifecycle level ~tags
      "native workflow worker adapter event"
  with _ -> ()

(** Adds or rejects one definition in the name map. *)
let add_definition definitions
    (Workflow (definition, signal_handlers, query_handlers, update_handlers)) =
  let name = Definition.name definition in
  let rec validate_signal_names seen = function
    | [] -> Ok ()
    | handler :: rest ->
        let signal_name = signal_handler_name handler in
        if List.mem signal_name seen then
          Error
            (make_error ~path:("$.workflows." ^ name ^ ".signals")
               "duplicate_signal_handler"
               ("signal name is registered more than once: " ^ signal_name))
        else validate_signal_names (signal_name :: seen) rest
  in
  let rec validate_query_names seen = function
    | [] -> Ok ()
    | handler :: rest ->
        let query_name = query_handler_name handler in
        if List.mem query_name seen then
          Error
            (make_error ~path:("$.workflows." ^ name ^ ".queries")
               "duplicate_query_handler"
               ("query name is registered more than once: " ^ query_name))
        else validate_query_names (query_name :: seen) rest
  in
  let rec validate_update_names seen = function
    | [] -> Ok ()
    | handler :: rest ->
        let update_name = update_handler_name handler in
        if List.mem update_name seen then
          Error
            (make_error ~path:("$.workflows." ^ name ^ ".updates")
               "duplicate_update_handler"
               ("update name is registered more than once: " ^ update_name))
        else validate_update_names (update_name :: seen) rest
  in
  if Run_map.mem name definitions then
    Error
      (make_error ~path:"$.workflows" "duplicate_workflow"
         ("workflow type is registered more than once: " ^ name))
  else if Option.is_none (Definition.implementation definition) then
    Error
      (make_error ~path:("$.workflows." ^ name) "not_executable"
         "workflow registration has no local implementation")
  else
    match validate_signal_names [] signal_handlers with
    | Error error -> Error error
    | Ok () -> (
        match validate_query_names [] query_handlers with
        | Error error -> Error error
        | Ok () -> (
            match validate_update_names [] update_handlers with
            | Error error -> Error error
            | Ok () ->
                Ok
                  (Run_map.add name
                     (Registered_definition
                        (definition, signal_handlers, query_handlers, update_handlers))
                     definitions)))

(** Builds the immutable definition registry before publishing any mutable
    worker state. *)
let build_definitions workflows =
  List.fold_left
    (fun result workflow ->
      let* definitions = result in
      add_definition definitions workflow)
    (Ok Run_map.empty) workflows

(** Validates the worker's implicit activity queue before [create] publishes a
    definition registry. The same predicate is used by
    [Workflow_context_store.create], so an invalid queue cannot survive worker
    construction and later become an activation-time [Invalid_argument]. *)
let validate_task_queue task_queue =
  match Workflow_context_store.validate_task_queue task_queue with
  | Ok () -> Ok ()
  | Error message ->
      Error
        (make_error ~path:"$.task_queue" "invalid_configuration" message)

(** Validates the worker namespace with the same predicate as
    [Workflow_context_store.create], for the same reason as
    [validate_task_queue]: an invalid value is reported at worker
    construction rather than as an activation-time [Invalid_argument]. *)
let validate_namespace namespace =
  match Workflow_context_store.validate_namespace namespace with
  | Ok () -> Ok ()
  | Error message ->
      Error (make_error ~path:"$.namespace" "invalid_configuration" message)

(** Finds one registered workflow by its Temporal type name. *)
let find_definition definitions workflow_type =
  match Run_map.find_opt workflow_type definitions with
  | Some definition -> Ok definition
  | None ->
      Error
        (make_error ~path:"$.jobs[0].workflow_type" "unknown_workflow_type"
           ("no executable workflow is registered for type " ^ workflow_type))

(** The functor implementation uses a concrete record containing the source
    module and source value. The public [t] stores that record behind the
    functor's abstract type, preserving both the source's abstract type and the
    invariant that all calls pass through the adapter mutex. *)
module Make (Supervisor : SUPERVISOR) = struct
  (** Mutable state owned by one adapter instance. [supervisor], [task_queue],
      [namespace], and [definitions] are immutable after construction; [runs] and
      [pending] are changed only while [mutex] is held, so workflow execution
      state cannot race with completion retries or shutdown draining. *)
  type adapter_state = {
    (* The opaque source handle. Calls use the same owner-confined supervisor
       that created this adapter and are made only while [mutex] is held. *)
    supervisor : Supervisor.t;
    (* Validated default activity queue copied into each new execution context;
       it is immutable for the lifetime of this worker. *)
    task_queue : string;
    (* Validated worker namespace copied into each new execution context so
       [Temporal.Workflow.info] can report it; activations do not carry it. *)
    namespace : string;
    (* Existential workflow definitions, built and validated before the state
       record is published. *)
    definitions : registered_definition Run_map.t;
    (* Cached workflow executions keyed by the exact Temporal run ID. Terminal
       runs retain queryable state until Core eviction; their schedulers and
       continuations are already shut down. Adapter failures also remove runs
       after their failure completion is acknowledged. *)
    mutable runs : run Run_map.t;
    (* Canonical bytes of completions whose source acknowledgement failed. The
       value remains here until the exact same bytes are accepted. *)
    mutable pending : pending_completion Run_map.t;
    (* Serializes all access to [runs], [pending], and the source operation so
       another Domain cannot overtake an activation or retry a completion. *)
    mutex : Mutex.t;
    (* Optional owner-Domain diagnostic hook. It is called only after strict
       translation and before user workflow code, so a replay observer cannot
       see partially validated protocol data or run on an arbitrary Rust
       thread. *)
    on_activation : (activation_info -> unit) option;
    (* Optional owner-Domain completion hook. It is called only after the
       supervisor acknowledges an activation completion, including the empty
       completion used for cache eviction, so a test can use it as an exact
       admission barrier without inferring completion from an earlier
       activation callback. *)
    on_completion : (activation_info -> unit) option;
    (* The activation currently between poll and completion on the workflow
       lane. Written only by the lane (while it holds [mutex]); read by the
       watchdog Domain without [mutex]. *)
    in_flight : in_flight option Atomic.t;
    (* Next activation epoch. Changed only while [mutex] is held. *)
    mutable next_epoch : int;
    (* The first activation abandoned by the watchdog. Sticky: once set, the
       worker stays unhealthy until the process is replaced, because the code
       that failed to yield may have left process state inconsistent. *)
    stuck : stuck_activation option Atomic.t;
  }

  (** The public worker handle is the mutex-confined state above. *)
  type t = adapter_state

  (** Creates the immutable definition registry and an empty run registry.
      Queue validation happens before definitions are published, so malformed
      empty, NUL-containing, oversized, or non-UTF-8 defaults fail as a typed
      configuration result rather than breaking the first workflow activation.
      No supervisor operation or workflow implementation runs on this path. *)
  let create ?on_activation ?on_completion ?(task_queue = "default")
      ?(namespace = "default") ~supervisor ~workflows () =
    let* () = validate_task_queue task_queue in
    let* () = validate_namespace namespace in
    let* definitions = build_definitions workflows in
    Ok
      {
        supervisor;
        task_queue;
        namespace;
        definitions;
        runs = Run_map.empty;
        pending = Run_map.empty;
        mutex = Mutex.create ();
        on_activation;
        on_completion;
        in_flight = Atomic.make None;
        next_epoch = 0;
        stuck = Atomic.make None;
      }

  (** Distinguishes source rejection from an uncertain raised acknowledgement.
      Both preserve the exact pending completion; neither permits replacement
      commands or re-execution of workflow code. [retryable] carries the
      source's explicit proof that the lease is still outstanding, without
      which the retained completion is never resubmitted. *)
  type completion_attempt =
    | Accepted
    | Rejected_by_supervisor of { error : error_view; retryable : bool }
    | Raised_by_supervisor of { exception_ : exn; retryable : bool }

  (** A faulty source classifier must not turn a diagnostic defect into a
      duplicate submission. Only an explicit [true] authorizes a retry. *)
  let source_error_is_retryable source_error =
    try Supervisor.error_is_retryable source_error with _ -> false

  (** Exception classification is equally conservative: an exception is an
      uncertain acknowledgement unless the source explicitly proves otherwise. *)
  let completion_exception_is_retryable exception_ =
    try Supervisor.exception_is_retryable exception_ with _ -> false

  (** Calls the supervisor completion operation without losing whether an
      exception occurred. A returned source error still means that the
      supervisor completed the call normally but did not acknowledge it. *)
  let attempt_completion supervisor
      ({ completion; encoded } : Native_execution.encoded_completion) =
    try
      match Supervisor.complete_workflow supervisor ~completion encoded with
      | Ok () -> Accepted
      | Error source_error ->
          let source =
            supervisor_error ~path:"$.completion"
              ~error_code:Supervisor.error_code
              ~error_message:Supervisor.error_message source_error
          in
          Rejected_by_supervisor
            {
              error =
                make_error ~path:"$.completion" "completion_failed"
                  (Printf.sprintf "supervisor rejected completion (%s): %s"
                     source.code source.message);
              retryable = source_error_is_retryable source_error;
            }
    with exception_ ->
      Raised_by_supervisor
        { exception_; retryable = completion_exception_is_retryable exception_ }

  (** Converts a completion exception to the stable typed error used when a
      failure-completion attempt itself cannot be acknowledged. *)
  let completion_exception_error exception_ =
    make_error ~path:"$.completion" "completion_failed"
      (Printf.sprintf "supervisor completion raised: %s"
         (exception_error exception_).message)

  (** Drops one run from the registry and always tears down its scheduler.
      Terminal and eviction paths already shut down the execution; a second
      [Execution.shutdown] is idempotent. Reject paths that inserted a run
      before failing must still release paused effect continuations here. *)
  let drop_run adapter run_id =
    match Run_map.find_opt run_id adapter.runs with
    | None -> ()
    | Some (Run { execution; _ }) ->
        (* Contain teardown defects: after a completion is acknowledged the
           lease is already retired, so a raising shutdown must not become a
           second failure-completion attempt for a stale run. *)
        (try Execution.shutdown execution with _ -> ());
        adapter.runs <- Run_map.remove run_id adapter.runs

  (** Reports an acknowledged activation without allowing a diagnostic
      observer defect to undo an already acknowledged native lease. Keeping
      this call in [accepted_pending] gives immediate submissions and retained
      retries the same exactly-once notification path. *)
  let notify_completion adapter (info : activation_info) =
    match adapter.on_completion with
    | None -> ()
    | Some callback ->
        (try callback info with _ ->
          report Logs.Warning ~operation:"workflow_completion_diagnostic_failed" ())

  (** Applies bookkeeping only after the supervisor acknowledges a retained
      completion. The retained completion bytes are released here, while a
      successfully completed run keeps its final query state until Core
      eviction. *)
  let accepted_pending adapter pending =
    adapter.pending <- Run_map.remove pending.run_id adapter.pending;
    match pending.result with
    | Pending_completed
        { command_count; terminal; evicted; activation_info } ->
        if evicted then drop_run adapter pending.run_id;
        notify_completion adapter activation_info;
        report Logs.Debug ~operation:"workflow_activation_completed" ();
        Ok
          (Completed
             {
               run_id = pending.run_id;
               command_count;
               terminal;
             })
    | Pending_rejected { error; remove_run } ->
        if remove_run then drop_run adapter pending.run_id;
        report Logs.Warning ~operation:"workflow_activation_rejected"
          ~error_kind:error.code ();
        Ok
          (Rejected
             {
               run_id = Some pending.run_id;
               error;
               lease_retired = true;
             })

  (** Attempts one retained completion. A rejected or raised native call leaves
      the same value in [pending]. A failure the source did not explicitly
      classify as retryable sets [retry_refusal]; from then on this function
      returns that recorded error without calling the supervisor, so neither a
      later poll nor a shutdown drain can submit the completion a second time
      (issue #843). *)
  let finish_pending adapter pending =
    match pending.retry_refusal with
    | Some error -> Error error
    | None -> (
        (* Records a fail-closed refusal before reporting the failure, so the
           entry is never resubmitted even if the caller retries [poll]. *)
        let refuse_unless retryable error =
          if not retryable then pending.retry_refusal <- Some error;
          Error error
        in
        match pending.submission with
        | Error error ->
            (* The encoder rejected this completion, so no bytes exist to
               submit. Nothing reached the supervisor and the native lease is
               still held; failing closed matches the earlier behavior, when
               the supervisor's own encode rejected it non-retryably. *)
            refuse_unless false error
        | Ok submission ->
        match attempt_completion adapter.supervisor submission with
        | Accepted -> accepted_pending adapter pending
        | Rejected_by_supervisor { error; retryable } ->
            refuse_unless retryable error
        | Raised_by_supervisor { exception_; retryable } ->
            (* Core may already have accepted this exact value. Never replace
               it with a task failure or rerun workflow code after an
               uncertain acknowledgement. *)
            refuse_unless retryable (completion_exception_error exception_))

  (** Returns query IDs only when an activation consists solely of workflow
      queries. Query-only activations are read-only leases: adapter-level
      failures may retire the query requests, but must not remove the live
      workflow run from the registry. *)
  let query_only_ids (activation : Protocol.activation) =
    let query_ids =
      List.filter_map
        (function
          | Protocol.Query_workflow { query_id; _ } -> Some query_id
          | _ -> None)
        activation.Protocol.jobs
    in
    if query_ids <> [] && List.length query_ids = List.length activation.Protocol.jobs
    then Some query_ids
    else None

  (** The diagnostic a lane reports for an activation the watchdog failed. *)
  let deadline_exceeded_error () =
    make_error ~path:"$.workflow_execution" "activation_deadline_exceeded"
      "workflow activation exceeded the worker's activation deadline without \
       yielding; the watchdog released its lease with a failure completion"

  (** Waits for the watchdog to publish the result of the failure submission
      it started after winning the lease claim. The wait is bounded by one
      supervisor completion call, which the watchdog has already begun. *)
  let rec await_watchdog_outcome flight =
    match Atomic.get flight.watchdog_outcome with
    | Some outcome -> outcome
    | None ->
        Thread.delay 0.001;
        await_watchdog_outcome flight

  (** Handles a lane that returned from workflow code after the watchdog had
      already failed its workflow task. The lane's own completion is dropped
      without a native call, because the lease belongs to the watchdog's
      submission. Unless the activation was a read-only query, the run is
      shut down and removed: Core evicts a run whose task failed, and its
      eviction activation is then acknowledged by the [Some _, None] path. A
      watchdog submission that was not acknowledged leaves the lease state
      unknown, which is returned as an [Error] so the worker loop stops. *)
  let abandoned_by_watchdog adapter flight =
    let activation = flight.activation in
    if Option.is_none (query_only_ids activation) then
      drop_run adapter activation.run_id;
    match await_watchdog_outcome flight with
    | Ok () ->
        report Logs.Warning ~operation:"workflow_activation_late_completion_dropped"
          ~error_kind:"activation_deadline_exceeded" ();
        Ok
          (Rejected
             {
               run_id = Some activation.run_id;
               error = deadline_exceeded_error ();
               lease_retired = true;
             })
    | Error error -> Error error

  (** Claims the lease of the in-flight activation for the lane. Returns
      [Some flight] only when the watchdog claimed it first. A completion for a
      run other than the in-flight activation (none is produced today) is not
      subject to the watchdog. *)
  let lane_lost_claim adapter run_id =
    match Atomic.get adapter.in_flight with
    | Some flight when String.equal flight.activation.Protocol.run_id run_id ->
        if Atomic.compare_and_set flight.claim Unclaimed Lane_claimed then None
        else (
          match Atomic.get flight.claim with
          | Watchdog_claimed -> Some flight
          | Unclaimed | Lane_claimed -> None)
    | _ -> None

  (** Records a completion before its first native attempt. This ordering is
      intentional: even an exception from the native binding leaves an exact
      owned completion in [pending], where it either awaits an explicitly
      retryable later attempt or blocks the run until terminal [discard].
      Every lane submission first claims the in-flight lease; if the watchdog
      already failed the task, the completion is dropped instead (#493). *)
  let enqueue_pending adapter pending =
    match lane_lost_claim adapter pending.run_id with
    | Some flight -> abandoned_by_watchdog adapter flight
    | None ->
    if Run_map.mem pending.run_id adapter.pending then
      Error
        (make_error ~path:"$.run_id" "duplicate_pending_completion"
           "a workflow run already has an unacknowledged completion")
    else (
      adapter.pending <- Run_map.add pending.run_id pending adapter.pending;
      finish_pending adapter pending)

  (** Query failures answer their request IDs. Other adapter defects fail the
      workflow task with no commands, preserving its durable execution. Eviction
      is an empty acknowledgement even if a diagnostic callback failed. *)
  let failure_completion (activation : Protocol.activation) error =
    let failure = failure_of_error error in
    match query_only_ids activation with
    | Some query_ids ->
        Protocol.{ run_id = activation.run_id; task_failure = None;
          commands = List.map (fun query_id -> Query_result
            { query_id; result = Query_failed failure }) query_ids }
    | None when List.exists (function Protocol.Remove_from_cache _ -> true | _ -> false)
        activation.jobs ->
        Protocol.{ run_id = activation.run_id; commands = []; task_failure = None }
    | None ->
        Protocol.{ run_id = activation.run_id; commands = []; task_failure = Some failure }

  (** Runs the canonical encoder once over a completion built by this adapter
      (a failure or an eviction acknowledgement). An encoder rejection becomes
      the same [completion_failed] diagnostic, at the same path, that a
      non-retryable supervisor rejection produced when the supervisor ran the
      encoder; [finish_pending] fails it closed without a native call. *)
  let encode_submission completion =
    match Encoded_completion.encode completion with
    | Ok encoded -> Ok { Native_execution.completion; encoded }
    | Error error ->
        let view = Protocol.error_view error in
        Error
          (make_error ~path:"$.completion" "completion_failed"
             (Printf.sprintf "workflow completion encoding failed: %s at %s: %s"
                view.code view.path view.message))

  (** Encodes and submits an adapter-level failure. A successful submission is
      the lease-retirement proof for the activation; a failed submission
      preserves a source error rather than claiming the lease was retired.
      [remove_run] is deliberately false for query-only activations because
      failed queries are read-only and must not mutate the workflow registry. *)
  let retire_with_failure ?(remove_run = false) adapter
      (activation : Protocol.activation) error =
    let completion = failure_completion activation error in
    let remove_run = remove_run || Option.is_none (query_only_ids activation) in
    (* Stop unsafe code immediately, but preserve the exact completion and its
       release bookkeeping until the supervisor acknowledges ownership. *)
    if remove_run then (
      match Run_map.find_opt activation.run_id adapter.runs with
      | Some (Run { execution; _ }) -> (try Execution.shutdown execution with _ -> ())
      | None -> ());
    let pending =
      {
        run_id = activation.run_id;
        submission = encode_submission completion;
        result = Pending_rejected { error; remove_run };
        retry_refusal = None;
      }
    in
    enqueue_pending adapter pending

  (** Converts Core's closed eviction-reason variant to the stable diagnostic
      spelling exposed to the private activation observer. *)
  let eviction_reason_name = function
    | Protocol.Eviction_unspecified -> "unspecified"
    | Protocol.Cache_full -> "cache_full"
    | Protocol.Cache_miss -> "cache_miss"
    | Protocol.Nondeterminism -> "nondeterminism"
    | Protocol.Lang_fail -> "lang_fail"
    | Protocol.Lang_requested -> "lang_requested"
    | Protocol.Task_not_found -> "task_not_found"
    | Protocol.Unhandled_command -> "unhandled_command"
    | Protocol.Fatal -> "fatal"
    | Protocol.Pagination_or_history_fetch -> "pagination_or_history_fetch"
    | Protocol.Workflow_execution_ending -> "workflow_execution_ending"

  (** Produces activation metadata once so activation and completion observers
      receive the exact same translated identity and replay state. *)
  let activation_info_of_translated
      (translated : Native_execution.translated_activation) : activation_info =
    {
      run_id = translated.run_id;
      workflow_id =
        Option.map
          (fun (initialization : Native_execution.initialization) ->
            initialization.workflow_id)
          translated.initialization;
      is_replaying = translated.is_replaying;
      history_length = translated.history_length;
      cache_removal_reason =
        Option.map
          (fun (removal : Native_execution.cache_removal) ->
            eviction_reason_name removal.reason)
          translated.cache_removal;
    }

  (** Submits the checked completion of a successfully executed activation and
      updates the registry only after the supervisor confirms retirement. The
      bytes produced by [Native_execution]'s single encoder pass are retained
      and submitted as they are; the typed value is read only for bookkeeping.
      A failed or raised submission keeps the exact bytes pending; Core may
      already have accepted them, so they are never replaced by a task
      failure. *)
  let submit_completion adapter activation
      (checked : Native_execution.encoded_completion) ~run_id ~activation_info =
    let completion = checked.completion in
    let pending =
      {
        run_id;
        submission = Ok checked;
        result =
          Pending_completed
            {
              command_count = List.length completion.commands;
              terminal = is_terminal completion;
              evicted =
                Option.is_some completion.task_failure || List.exists
                  (function Protocol.Remove_from_cache _ -> true | _ -> false)
                  activation.Protocol.jobs;
              activation_info;
            };
        retry_refusal = None;
      }
    in
    enqueue_pending adapter pending

  (** A cache-eviction activation is acknowledged with a successful empty
      completion even when its workflow run has already been removed after an
      adapter failure. Like every submission it uses the retained-completion
      path: if the native completion call raises, the exact empty
      acknowledgement remains pending (resubmitted only after an explicitly
      retryable failure) and is never replaced by an invalid failure
      command. *)
  let submit_eviction_acknowledgement adapter (activation : Protocol.activation)
      ~activation_info =
    let completion =
      Protocol.{ run_id = activation.run_id; task_failure = None; commands = [] }
    in
    let pending =
      {
        run_id = activation.run_id;
        submission = encode_submission completion;
        result =
          Pending_completed
            {
              command_count = 0;
              terminal = false;
              evicted = true;
              activation_info;
            };
        retry_refusal = None;
      }
    in
    enqueue_pending adapter pending

  (** Delivers replay and eviction metadata to the optional private observer.
      The observer runs while the adapter mutex is held, on the same Domain
      that owns the deterministic execution registry. A raised observer is
      converted into a typed activation error so the leased activation still
      follows the normal failure-completion path instead of escaping with an
      unacknowledged lease. *)
  let notify_activation adapter (translated : Native_execution.translated_activation) =
    match adapter.on_activation with
    | None -> Ok ()
    | Some callback ->
        let info = activation_info_of_translated translated in
        (try
           callback info;
           Ok ()
         with exception_ ->
           Error
             (exception_error ~path:"$.activation.replay_metadata" exception_))

  (** Applies one activation while the adapter mutex is held. No source call or
      map mutation is performed after an error that has not been acknowledged
      by a successful completion. *)
  let process_one_unsafe adapter activation : (outcome, error_view) result =
    match Native_execution.translate_activation activation with
    | Error error ->
        retire_with_failure adapter activation (native_error error)
    | Ok translated ->
        let activation_info = activation_info_of_translated translated in
        (match notify_activation adapter translated with
        | Error error -> retire_with_failure adapter activation error
        | Ok () ->
            match
              (translated.cache_removal, Run_map.find_opt activation.run_id adapter.runs)
            with
            | Some _, None ->
                (* Core can evict a run after the OCaml registry has already
                   removed it for an adapter failure. The eviction still
                   owns a native lease and must receive the exact successful
                   empty completion; a failure command is invalid here. *)
                    submit_eviction_acknowledgement adapter activation
                      ~activation_info
            | _ -> (
                match initialization activation with
                | Error error -> retire_with_failure adapter activation error
                | Ok (Some init) ->
                    if Run_map.mem activation.run_id adapter.runs then
                      (* Reporting a terminal lifecycle defect also retires the
                         existing generation; otherwise its suspended fiber can
                         resume after Core has accepted the task failure. *)
                      retire_with_failure ~remove_run:true adapter activation
                        (make_error ~path:"$.run_id" "duplicate_run_id"
                           "workflow run is already present in the execution registry")
                    else
                      begin
                        match find_definition adapter.definitions init.workflow_type with
                        | Error error -> retire_with_failure adapter activation error
                        | Ok
                            (Registered_definition
                              (definition, signal_handlers, query_handlers,
                               update_handlers)) ->
                            begin
                              match decode_input definition init.arguments with
                              | Error error -> retire_with_failure adapter activation error
                              | Ok input ->
                                  let execution =
                                    Execution.start ~task_queue:adapter.task_queue
                                      ~namespace:adapter.namespace
                                      ~randomness_seed:init.randomness_seed
                                      ~signal_handlers ~query_handlers
                                      ~update_handlers definition input
                                  in
                                  let run =
                                    Run { definition; execution; workflow_id = init.workflow_id }
                                  in
                                  adapter.runs <-
                                    Run_map.add activation.run_id run adapter.runs;
                                  begin
                                    match
                                      Native_execution.activate_translated execution
                                        translated
                                    with
                                    | Error error ->
                                        retire_with_failure ~remove_run:true adapter
                                          activation (native_error error)
                                    | Ok completion ->
                                        submit_completion adapter activation completion
                                          ~run_id:activation.run_id ~activation_info
                                  end
                            end
                      end
                | Ok None ->
                    match Run_map.find_opt activation.run_id adapter.runs with
                    | None ->
                        retire_with_failure adapter activation
                          (make_error ~path:"$.run_id" "unknown_run_id"
                             "activation does not identify a registered running workflow")
                    | Some (Run { execution; _ }) ->
                        (match
                           Native_execution.activate_translated execution translated
                         with
                        | Error error ->
                            let remove_run =
                              Option.is_none (query_only_ids activation)
                            in
                            retire_with_failure ~remove_run adapter activation
                              (native_error error)
                        | Ok completion ->
                            submit_completion adapter activation completion
                              ~run_id:activation.run_id ~activation_info)))

  (** Applies one activation with a final cleanup guard. All expected
      rejections already use [retire_with_failure]; this catch handles a
      programmer defect or unexpected codec exception before completion. It
      attempts exactly one failure completion and reports [Error] if that
      acknowledgement cannot be proven, rather than claiming retirement. *)
  let process_one adapter activation : (outcome, error_view) result =
    try process_one_unsafe adapter activation with exception_ ->
      let error = exception_error ~path:"$.workflow_execution" exception_ in
      retire_with_failure
        ~remove_run:(Run_map.mem activation.run_id adapter.runs)
        adapter activation error

  (** Names the workflow execution an activation belongs to for watchdog
      diagnostics: from its initialization job when it starts a run, otherwise
      from the cached run. Called while [mutex] is held. *)
  let activation_identity adapter (activation : Protocol.activation) =
    match
      List.find_map
        (function
          | Protocol.Initialize_workflow { workflow_id; workflow_type; _ } ->
              Some (workflow_id, workflow_type)
          | _ -> None)
        activation.jobs
    with
    | Some (workflow_id, workflow_type) -> (Some workflow_id, Some workflow_type)
    | None -> (
        match Run_map.find_opt activation.run_id adapter.runs with
        | Some (Run { definition; workflow_id; _ }) ->
            (Some workflow_id, Some (Definition.name definition))
        | None -> (None, None))

  (** Publishes the activation for the watchdog, processes it, and withdraws
      it. The publication happens before any user code (codecs, observers, or
      workflow fibers) runs, and withdrawal happens only after the completion
      was submitted or dropped, so the watchdog can never claim a lease the
      lane has already completed. *)
  let process_tracked adapter (activation : Protocol.activation) =
    let flight_workflow_id, flight_workflow_type =
      activation_identity adapter activation
    in
    let epoch = adapter.next_epoch in
    adapter.next_epoch <- epoch + 1;
    let flight =
      {
        epoch;
        activation;
        flight_workflow_id;
        flight_workflow_type;
        claim = Atomic.make Unclaimed;
        watchdog_outcome = Atomic.make None;
      }
    in
    Atomic.set adapter.in_flight (Some flight);
    Fun.protect
      ~finally:(fun () -> Atomic.set adapter.in_flight None)
      (fun () -> process_one adapter activation)

  (** Returns the epoch of the activation currently running workflow code, or
      [None] when the lane is polling, idle, or already submitting a
      completion. Safe to call from any Domain without the adapter mutex. *)
  let running_epoch adapter =
    match Atomic.get adapter.in_flight with
    | Some flight when Atomic.get flight.claim = Unclaimed -> Some flight.epoch
    | _ -> None

  (** Logs the bounded watchdog diagnostic. Identifiers are bounded by the
      observability layer; no payload or failure text is included. *)
  let report_stuck (stuck : stuck_activation) =
    try
      let tags =
        Observability.tags ~operation:"workflow_activation_deadline_exceeded"
          ~duration_ms:(Float.of_int stuck.elapsed_ms)
          ?workflow_type:stuck.workflow_type ?workflow_id:stuck.workflow_id
          ~run_id:stuck.run_id
          ~error_kind:
            (match stuck.abandoned with
             | `Task_failed -> "workflow_task_failed"
             | `Queries_failed -> "workflow_queries_failed"
             | `Eviction_acknowledged -> "eviction_acknowledged"
             | `Not_acknowledged -> "completion_unacknowledged")
          ()
      in
      Observability.report ~src:Observability.Source.workflow Logs.Error ~tags
        "workflow activation did not yield before its deadline; the worker \
         is unhealthy and must be restarted"
    with _ -> ()

  (** Classifies the watchdog's replacement [completion], as built by
      [failure_completion], together with its submission [outcome]. Only an
      acknowledged completion carrying [task_failure] is reported as a failed
      workflow task; failed query answers and an empty eviction acknowledgement
      leave the workflow task itself unfailed. *)
  let abandonment_of (completion : Protocol.completion) outcome : abandonment =
    match (outcome, completion) with
    | Error _, _ -> `Not_acknowledged
    | Ok (), { task_failure = Some _; _ } -> `Task_failed
    | Ok (), { task_failure = None; commands = _ :: _; _ } -> `Queries_failed
    | Ok (), { task_failure = None; commands = []; _ } -> `Eviction_acknowledged

  (** Called by the watchdog when activation [epoch] has run workflow code for
      at least [elapsed_ms]. If that activation is still unclaimed, the
      watchdog takes its lease, submits the [failure_completion] for it
      through the supervisor without the adapter mutex (the lane still holds
      it), records the sticky [stuck] report with the resulting
      {!abandonment}, and logs it. For an ordinary activation Temporal then
      retries the failed task, normally on another worker. Returns [None] when the lane finished or
      claimed the lease first; the watchdog never touches workflow state, so
      the stuck code keeps running until it returns on its own. *)
  let abandon_activation adapter ~epoch ~elapsed_ms =
    match Atomic.get adapter.in_flight with
    | Some flight
      when flight.epoch = epoch
           && Atomic.compare_and_set flight.claim Unclaimed Watchdog_claimed ->
        let completion =
          failure_completion flight.activation (deadline_exceeded_error ())
        in
        let outcome =
          match encode_submission completion with
          | Error error -> Error error
          | Ok submission -> (
              match attempt_completion adapter.supervisor submission with
              | Accepted -> Ok ()
              | Rejected_by_supervisor { error; _ } -> Error error
              | Raised_by_supervisor { exception_; _ } ->
                  Error (completion_exception_error exception_))
        in
        Atomic.set flight.watchdog_outcome (Some outcome);
        let stuck =
          {
            run_id = flight.activation.run_id;
            workflow_id = flight.flight_workflow_id;
            workflow_type = flight.flight_workflow_type;
            is_replaying = flight.activation.is_replaying;
            elapsed_ms;
            abandoned = abandonment_of completion outcome;
          }
        in
        ignore (Atomic.compare_and_set adapter.stuck None (Some stuck));
        report_stuck stuck;
        Some stuck
    | _ -> None

  (** The first activation the watchdog abandoned, if any. *)
  let stuck adapter = Atomic.get adapter.stuck

  (** Retries retained workflow completions while the adapter mutex is held.
      Shutdown uses this operation before closing Rust so an explicitly
      retryable transport failure never leaves a lease behind. A completion
      whose earlier failure was not classified retryable is not resubmitted:
      [finish_pending] returns its recorded error, which the worker treats as a
      terminal drain failure before force-releasing the native graph. *)
  let drain_locked adapter : (unit, error_view) result =
    (* [min_binding_opt] gives retries a stable order. The loop stops on the
       first error, retaining that completion and every later one; a
       fail-closed entry stops it without any native call. *)
    let rec loop () =
      match Run_map.min_binding_opt adapter.pending with
      | None -> Ok ()
      | Some (_, pending) -> (
          match finish_pending adapter pending with
          | Ok _ -> loop ()
          | Error error -> Error error)
    in
    loop ()

  (** Takes the adapter mutex for [drain_locked]. *)
  let drain adapter : (unit, error_view) result =
    Mutex.lock adapter.mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.mutex)
      (fun () -> drain_locked adapter)

  (** Discards OCaml-owned executions and retained completion bytes after the
      native graph has been force-released by terminal worker shutdown. This
      path never calls the supervisor: the Rust runtime has already retired its
      leases, and retrying a retained completion would risk a duplicate. Each
      execution is explicitly shut down so paused workflow continuations and
      scheduler state do not wait for a later garbage collection cycle. *)
  let discard_locked adapter =
    Run_map.iter
      (fun _ (Run { execution; _ }) ->
        (try Execution.shutdown execution with _ -> ()))
      adapter.runs;
    adapter.runs <- Run_map.empty;
    adapter.pending <- Run_map.empty

  (** Takes the adapter mutex for [discard_locked]. *)
  let discard adapter =
    Mutex.lock adapter.mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.mutex)
      (fun () -> discard_locked adapter)

  (** Runs [f] with the adapter mutex only if it is free right now. The
      workflow lane holds that mutex from poll through completion, so [None]
      is the non-blocking signal that an activation is still in progress. *)
  let if_idle adapter f =
    if Mutex.try_lock adapter.mutex then
      Some (Fun.protect ~finally:(fun () -> Mutex.unlock adapter.mutex) f)
    else None

  (** Non-blocking [drain] for bounded shutdown (#495). *)
  let try_drain adapter = if_idle adapter (fun () -> drain_locked adapter)

  (** Non-blocking [discard] for bounded shutdown (#495). *)
  let try_discard adapter =
    Option.is_some (if_idle adapter (fun () -> discard_locked adapter))

  (** Lock-free read of the lane's in-flight slot, set from poll until the
      completion is submitted, whether or not the watchdog has already
      claimed the activation's lease. *)
  let activation_in_flight adapter = Option.is_some (Atomic.get adapter.in_flight)

  (** Serializes one poll/execute/complete transaction. A mutex is required in
      addition to supervisor serialization because the run map and scheduler
      state are OCaml values owned by this adapter, not by Rust. A retained
      completion blocks new activations: it is retried only when its failure
      was explicitly retryable, and otherwise reported again unchanged. *)
  let poll adapter =
    Mutex.lock adapter.mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.mutex)
      (fun () ->
        match Run_map.min_binding_opt adapter.pending with
        | Some (_, pending) -> finish_pending adapter pending
        | None ->
            (* Convert exceptions from the source poll into a typed error
               before inspecting the result, so the mutex is always released
               by the surrounding [Fun.protect]. *)
            let polled =
              try Ok (Supervisor.try_poll_workflow adapter.supervisor)
              with exception_ -> Error (exception_error ~path:"$.poll" exception_)
            in
            let* polled = polled in
            match polled with
            | Error source_error ->
                Error
                  (supervisor_error ~path:"$.poll"
                     ~error_code:Supervisor.error_code
                     ~error_message:Supervisor.error_message source_error)
            | Ok None ->
                report Logs.Debug ~operation:"workflow_poll_not_ready" ();
                Ok Not_ready
            | Ok (Some activation) ->
                process_tracked adapter activation)
end

(** Exposes registration without exposing its existential constructor. *)
let register ?(signal_handlers = []) ?(query_handlers = []) ?(update_handlers = [])
    definition =
  Workflow (definition, signal_handlers, query_handlers, update_handlers)
