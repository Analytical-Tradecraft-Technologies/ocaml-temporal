(** Private worker-loop state for the first live OCaml workflow execution
    boundary.

    Rust/Core ownership and semantic JSON validation live below this module in
    the supervisor. This module therefore receives typed activations, resolves
    each run ID to an existentially typed [Execution.t], and sends typed
    completions back through the same supervisor. The registry is mutable only
    behind one mutex; it is never shared with workflow fibers or native code. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Definition = Temporal_base.Definition
module Codec = Temporal_base.Codec
module Base_error = Temporal_base.Error
module Observability = Temporal_base.Observability

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

  val complete_workflow :
    t -> Protocol.completion -> (unit, error) result

  val error_code : error -> string
  val error_message : error -> string
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

(** The protocol value is owned by this adapter until the supervisor accepts
    it. Its binary payloads are copied before the value enters mutable state. *)
type pending_completion = {
  run_id : string;
  completion : Protocol.completion;
  result : pending_result;
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
      and [definitions] are immutable after construction; [runs] and
      [pending] are changed only while [mutex] is held, so workflow execution
      state cannot race with completion retries or shutdown draining. *)
  type adapter_state = {
    (* The opaque source handle. Calls use the same owner-confined supervisor
       that created this adapter and are made only while [mutex] is held. *)
    supervisor : Supervisor.t;
    (* Validated default activity queue copied into each new execution context;
       it is immutable for the lifetime of this worker. *)
    task_queue : string;
    (* Existential workflow definitions, built and validated before the state
       record is published. *)
    definitions : registered_definition Run_map.t;
    (* Active workflow executions keyed by the exact Temporal run ID. A run is
       removed only after a terminal or failure completion is acknowledged. *)
    mutable runs : run Run_map.t;
    (* Owned copies of completions whose source acknowledgement failed. The
       value remains here until the exact same completion is accepted. *)
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
  }

  (** The public worker handle is the mutex-confined state above. *)
  type t = adapter_state

  (** Creates the immutable definition registry and an empty run registry.
      Queue validation happens before definitions are published, so malformed
      empty, NUL-containing, oversized, or non-UTF-8 defaults fail as a typed
      configuration result rather than breaking the first workflow activation.
      No supervisor operation or workflow implementation runs on this path. *)
  let create ?on_activation ?on_completion ?(task_queue = "default") ~supervisor
      ~workflows () =
    let* () = validate_task_queue task_queue in
    let* definitions = build_definitions workflows in
    Ok
      {
        supervisor;
        task_queue;
        definitions;
        runs = Run_map.empty;
        pending = Run_map.empty;
        mutex = Mutex.create ();
        on_activation;
        on_completion;
      }

  (** Copies a payload without retaining a mutable buffer owned by an earlier
      execution step. Metadata keys are immutable strings; metadata and body
      bytes are the only mutable protocol values. *)
  let copy_payload (payload : Protocol.payload) : Protocol.payload =
    {
      metadata =
        List.map (fun (key, value) -> (key, Bytes.copy value)) payload.metadata;
      data = Bytes.copy payload.data;
    }

  (** Copies a failure recursively, including nested causes and detail
      payloads. Failure text is immutable, while every payload buffer is owned
      by the retained completion. *)
  let rec copy_failure (failure : Protocol.failure) : Protocol.failure =
    let info =
      match failure.info with
      | Protocol.Application { type_name; non_retryable; details } ->
          Protocol.Application
            {
              type_name;
              non_retryable;
              details = List.map copy_payload details;
            }
      | Protocol.Canceled { details; identity } ->
          Protocol.Canceled
            { details = List.map copy_payload details; identity }
      | Protocol.Activity _ as info -> info
      | Protocol.Child_workflow _ as info -> info
      | Protocol.Timeout_failure { timeout_type; last_heartbeat_details } ->
          Protocol.Timeout_failure
            {
              timeout_type;
              last_heartbeat_details = List.map copy_payload last_heartbeat_details;
            }
    in
    {
      message = failure.message;
      source = failure.source;
      stack_trace = failure.stack_trace;
      encoded_attributes = Option.map copy_payload failure.encoded_attributes;
      cause = Option.map copy_failure failure.cause;
      info;
    }

  (** Copies every payload-bearing command in a workflow completion. Keeping
      this operation explicit makes the ownership boundary auditable without a
      JSON round trip or an alias to a workflow implementation's buffer. *)
  let copy_completion (completion : Protocol.completion) : Protocol.completion =
    (* Commands without payload-bearing fields are returned unchanged; every
       command that can retain mutable bytes is copied before it enters
       [pending]. *)
    let copy_command = function
      | Protocol.Schedule_activity command ->
          Protocol.Schedule_activity
            {
              command with
              arguments = List.map copy_payload command.arguments;
            }
      | Protocol.Schedule_local_activity command ->
          Protocol.Schedule_local_activity
            {
              command with
              arguments = List.map copy_payload command.arguments;
            }
      | Protocol.Complete_workflow { result } ->
          Protocol.Complete_workflow { result = Option.map copy_payload result }
      | Protocol.Fail_workflow { failure } ->
          Protocol.Fail_workflow { failure = copy_failure failure }
      | Protocol.Continue_as_new command ->
          Protocol.Continue_as_new
            { command with input = List.map copy_payload command.input }
      | Protocol.Start_child_workflow command ->
          Protocol.Start_child_workflow
            { command with input = List.map copy_payload command.input }
      | Protocol.Signal_external_workflow command ->
          Protocol.Signal_external_workflow
            {
              command with
              input = List.map copy_payload command.input;
              headers =
                List.map (fun (key, payload) -> (key, copy_payload payload))
                  command.headers;
            }
      | Protocol.Query_result { query_id; result } ->
          let result =
            match result with
            | Protocol.Query_succeeded payload ->
                Protocol.Query_succeeded (copy_payload payload)
            | Protocol.Query_failed failure -> Protocol.Query_failed (copy_failure failure)
          in
          Protocol.Query_result { query_id; result }
      | Protocol.Update_response { protocol_instance_id; response } ->
          let response =
            match response with
            | Protocol.Update_accepted -> Protocol.Update_accepted
            | Protocol.Update_rejected failure ->
                Protocol.Update_rejected (copy_failure failure)
            | Protocol.Update_completed payload ->
                Protocol.Update_completed (copy_payload payload)
          in
          Protocol.Update_response { protocol_instance_id; response }
      | Protocol.Cancel_child_workflow _ as command -> command
      | Protocol.Request_cancel_external_workflow _ as command -> command
      | Protocol.Request_cancel_activity _ as command -> command
      | Protocol.Request_cancel_local_activity _ as command -> command
      | Protocol.Start_timer _ as command -> command
      | Protocol.Cancel_timer _ as command -> command
      | Protocol.Set_patch_marker _ as command -> command
      | Protocol.Upsert_search_attributes { search_attributes } ->
          Protocol.Upsert_search_attributes
            {
              search_attributes =
                List.map
                  (fun (key, payload) -> (key, copy_payload payload))
                  search_attributes;
            }
      | Protocol.Cancel_workflow_execution as command -> command
    in
    {
      run_id = completion.run_id;
      commands = List.map copy_command completion.commands;
    }

  (** Distinguishes an ordinary source rejection from an exception raised while
      completing. The latter is allowed to reach [process_one]'s cleanup guard:
      the adapter can then make one explicit failure-completion attempt instead
      of silently abandoning the leased activation. *)
  type completion_attempt =
    | Accepted
    | Rejected_by_supervisor of error_view
    | Raised_by_supervisor of exn

  (** Calls the supervisor completion operation without losing whether an
      exception occurred. A returned source error still means that the
      supervisor completed the call normally but did not acknowledge it. *)
  let attempt_completion supervisor completion =
    try
      match Supervisor.complete_workflow supervisor completion with
      | Ok () -> Accepted
      | Error source_error ->
          let source =
            supervisor_error ~path:"$.completion"
              ~error_code:Supervisor.error_code
              ~error_message:Supervisor.error_message source_error
          in
          Rejected_by_supervisor
            (make_error ~path:"$.completion" "completion_failed"
               (Printf.sprintf "supervisor rejected completion (%s): %s"
                  source.code source.message))
    with exception_ -> Raised_by_supervisor exception_

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
      completion. This is the single release point for both normal and
      adapter-generated failure completions. *)
  let accepted_pending adapter pending =
    adapter.pending <- Run_map.remove pending.run_id adapter.pending;
    match pending.result with
    | Pending_completed
        { command_count; terminal; evicted; activation_info } ->
        if terminal || evicted then drop_run adapter pending.run_id;
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
      the same value in [pending], preserving the only safe retry path. *)
  let finish_pending adapter pending =
    match attempt_completion adapter.supervisor pending.completion with
    | Accepted -> accepted_pending adapter pending
    | Rejected_by_supervisor error -> Error error
    | Raised_by_supervisor exception_ ->
        Error (completion_exception_error exception_)

  (** Records a completion before its first native attempt. This ordering is
      intentional: even an exception from the native binding leaves an exact
      owned completion available for a later poll or shutdown drain. *)
  let enqueue_pending adapter pending =
    if Run_map.mem pending.run_id adapter.pending then
      Error
        (make_error ~path:"$.run_id" "duplicate_pending_completion"
           "a workflow run already has an unacknowledged completion")
    else (
      adapter.pending <- Run_map.add pending.run_id pending adapter.pending;
      finish_pending adapter pending)

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

  (** Builds the completion for an adapter-level failure. Query activations are
      read-only requests, so Core rejects a workflow-failure command for them;
      each query ID must instead receive a failed query result. Ordinary and
      mixed envelopes keep the existing workflow-failure path. The source
      decoder guarantees query IDs are valid and unique before this helper is
      reached. *)
  let failure_commands (activation : Protocol.activation) error =
    let failure = failure_of_error error in
    match query_only_ids activation with
    | Some query_ids ->
        List.map
          (fun query_id ->
            Protocol.Query_result
              { query_id; result = Protocol.Query_failed failure })
          query_ids
    | None -> [ Protocol.Fail_workflow { failure } ]

  (** Encodes and submits an adapter-level failure. A successful submission is
      the lease-retirement proof for the activation; a failed submission
      preserves a source error rather than claiming the lease was retired.
      [remove_run] is deliberately false for query-only activations because
      failed queries are read-only and must not mutate the workflow registry. *)
  let retire_with_failure ?(remove_run = false) adapter
      (activation : Protocol.activation) error =
    let completion : Protocol.completion =
      {
        run_id = activation.Protocol.run_id;
        commands = failure_commands activation error;
      }
    in
    let pending =
      {
        run_id = activation.run_id;
        completion = copy_completion completion;
        result = Pending_rejected { error; remove_run };
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

  (** Produces a typed completion for a successfully executed activation and
      updates the registry only after the supervisor confirms retirement. *)
  let submit_completion adapter activation completion ~run_id ~activation_info =
    let pending =
      {
        run_id;
        completion = copy_completion completion;
        result =
          Pending_completed
            {
              command_count = List.length completion.commands;
              terminal = is_terminal completion;
              evicted =
                List.exists
                  (function Protocol.Remove_from_cache _ -> true | _ -> false)
                  activation.Protocol.jobs;
              activation_info;
            };
      }
    in
    if Run_map.mem pending.run_id adapter.pending then
      Error
        (make_error ~path:"$.run_id" "duplicate_pending_completion"
           "a workflow run already has an unacknowledged completion")
    else begin
      adapter.pending <- Run_map.add pending.run_id pending adapter.pending;
      match attempt_completion adapter.supervisor pending.completion with
      | Accepted -> accepted_pending adapter pending
      | Rejected_by_supervisor error -> Error error
      | Raised_by_supervisor exception_ ->
          (* Eviction acknowledgements must stay empty completions. Removing
             them and re-raising lets the outer guard submit Fail_workflow,
             which is invalid for an eviction lease. Keep the empty pending
             entry for retry, matching [finish_pending]. *)
          (match pending.result with
          | Pending_completed { evicted = true; _ } ->
              Error (completion_exception_error exception_)
          | Pending_completed _ | Pending_rejected _ ->
              (* Ordinary completions: preserve the historical cleanup contract
                 so the outer guard can submit one explicit failure completion. *)
              adapter.pending <- Run_map.remove pending.run_id adapter.pending;
              raise exception_)
    end

  (** A cache-eviction activation is acknowledged with a successful empty
      completion even when its workflow run has already been removed after a
      terminal completion. This uses the retained-completion path rather than
      the ordinary submission path: if the native completion call raises, the
      exact empty acknowledgement remains pending for a later retry and is
      never replaced by an invalid failure command. *)
  let submit_eviction_acknowledgement adapter (activation : Protocol.activation)
      ~activation_info =
    let completion =
      Protocol.{ run_id = activation.run_id; commands = [] }
    in
    let pending =
      {
        run_id = activation.run_id;
        completion = copy_completion completion;
        result =
          Pending_completed
            {
              command_count = 0;
              terminal = false;
              evicted = true;
              activation_info;
            };
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
                   removed it for a terminal completion. The eviction still
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
                         resume after Core has accepted [Fail_workflow]. *)
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
                                      ~randomness_seed:init.randomness_seed
                                      ~signal_handlers ~query_handlers
                                      ~update_handlers definition input
                                  in
                                  let run = Run { definition; execution } in
                                  adapter.runs <-
                                    Run_map.add activation.run_id run adapter.runs;
                                  begin
                                    match
                                      Native_execution.activate execution activation
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
                        (match Native_execution.activate execution activation with
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

  (** Retries every retained workflow completion while the adapter mutex is
      held. Shutdown uses this operation before closing Rust so an
      acknowledged lease is never left behind by a prior transport failure. *)
  let drain adapter : (unit, error_view) result =
    Mutex.lock adapter.mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.mutex)
      (fun () ->
        (* [min_binding_opt] gives retries a stable order. The loop stops on
           the first source error, retaining that completion and every later
           one for a subsequent drain attempt. *)
        let rec loop () =
          match Run_map.min_binding_opt adapter.pending with
          | None -> Ok ()
          | Some (_, pending) -> (
              match finish_pending adapter pending with
              | Ok _ -> loop ()
              | Error error -> Error error)
        in
        loop ())

  (** Discards OCaml-owned executions and retained completion bytes after the
      native graph has been force-released by terminal worker shutdown. This
      path never calls the supervisor: the Rust runtime has already retired its
      leases, and retrying a copied completion would risk a duplicate. Each
      execution is explicitly shut down so paused workflow continuations and
      scheduler state do not wait for a later garbage collection cycle. *)
  let discard adapter =
    Mutex.lock adapter.mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.mutex)
      (fun () ->
        Run_map.iter
          (fun _ (Run { execution; _ }) ->
            (try Execution.shutdown execution with _ -> ()))
          adapter.runs;
        adapter.runs <- Run_map.empty;
        adapter.pending <- Run_map.empty)

  (** Serializes one poll/execute/complete transaction. A mutex is required in
      addition to supervisor serialization because the run map and scheduler
      state are OCaml values owned by this adapter, not by Rust. *)
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
                process_one adapter activation)
end

(** Exposes registration without exposing its existential constructor. *)
let register ?(signal_handlers = []) ?(query_handlers = []) ?(update_handlers = [])
    definition =
  Workflow (definition, signal_handlers, query_handlers, update_handlers)
