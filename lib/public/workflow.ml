(** Defines the public workflow description. Its record is deliberately kept
    private so callers can only create a validated definition through [define]
    or a command-only reference through [remote]. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

(** Deployment identity selected for one workflow task by Temporal's worker
    versioning layer. The public record contains only validated text and does
    not expose the private activation protocol. *)
type deployment_version = { deployment_name : string; build_id : string }

type ('input, 'output) t = {
  (* Stable Temporal workflow type name used in registrations and child
     workflow commands; it is validated before this record is published. *)
  name : string;
  (* Codec used to decode the payload supplied when this workflow starts. *)
  input : 'input Codec.t;
  (* Codec used to encode successful outputs returned to Temporal. *)
  output : 'output Codec.t;
  (* Local executable code, or [None] for a command-only remote reference. *)
  implementation : ('input, 'output) implementation option;
}

(** Maximum byte length accepted by the closed JSON/native identifier contract. *)
let max_name_bytes = 65_536

(** Rejects names that could not be represented safely in Temporal history.
    Definition construction is the earliest point at which a workflow type
    name is available, so enforcing the bridge's complete identifier contract
    here prevents a malformed name from surviving until worker registration or
    a child/continue-as-new command is emitted. *)
let validate_name name =
  if String.length name = 0 then invalid_arg "Temporal definition name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal definition name contains a NUL byte"
  else if String.length name > max_name_bytes then
    invalid_arg "Temporal definition name exceeds 65536 bytes"
  else if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "Temporal definition name must be valid UTF-8"

(** Registers executable workflow code after validating its stable type name. *)
let define ~name ~input ~output implementation =
  validate_name name;
  { name; input; output; implementation = Some implementation }

(** Creates a typed reference to a workflow implemented by another worker. The
    reference retains codecs for child-result correlation but no executable
    callback, so it cannot accidentally be registered as local worker code. *)
let remote ~name ~input ~output =
  validate_name name;
  { name; input; output; implementation = None }

(** Returns the exact Temporal workflow type name used by registration and
    child-workflow commands. *)
let name definition = definition.name

(** Returns the input codec retained by an opaque workflow definition. *)
let input definition = definition.input

(** Returns the output codec retained by an opaque workflow definition. *)
let output definition = definition.output

(** Returns executable code for a local workflow, or [None] for a remote
    reference that can only be invoked as a child. *)
let implementation definition = definition.implementation

(** Starts a durable timer without waiting for it. The runtime future is
    wrapped before it crosses into the public API, converting native errors to
    public errors and preserving the workflow scheduler owner. *)
let start_sleep duration =
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None ->
      Future_private.resolved
        ~outside_error:(fun () ->
          Error.defect ~message:"workflow sleep used outside a workflow execution")
        (Error
           (Error.defect ~message:"workflow sleep used outside a workflow execution"))
  | Some context ->
      let milliseconds = Duration.to_ms duration in
      if milliseconds = 0L then
        (* A zero-duration timer is already ready; omitting the command keeps
           replay history free of a timer that cannot suspend the workflow. *)
        Future_private.of_internal
          (Temporal_sdk_kernel.Workflow_context_store.resolved context (Ok ()))
      else
        Future_private.of_internal
          (Temporal_sdk_kernel.Workflow_context_store.start_timer context milliseconds)

(** Implements direct-style sleep as timer creation followed by a future wait. *)
let sleep duration = Future.await (start_sleep duration)

(** Validates a workflow execution target before retaining it in a command. A
    run ID may be empty to let Temporal resolve the current run, but workflow
    IDs and signal names are always required identifiers. *)
let validate_external_target ~workflow_id ~run_id =
  let validate_required field value =
    if String.equal value "" then
      Error (Error.defect ~message:(field ^ " must not be empty"))
    else if String.contains value '\000' then
      Error (Error.defect ~message:(field ^ " must not contain NUL"))
    else if String.length value > max_name_bytes then
      Error (Error.defect ~message:(field ^ " exceeds 65536 bytes"))
    else if not (Temporal_base.Codec.valid_utf_8 value) then
      Error (Error.defect ~message:(field ^ " must be valid UTF-8"))
    else Ok ()
  in
  let validate_optional field value =
    if String.length value > max_name_bytes then
      Error (Error.defect ~message:(field ^ " exceeds 65536 bytes"))
    else if String.contains value '\000' then
      Error (Error.defect ~message:(field ^ " must not contain NUL"))
    else if not (Temporal_base.Codec.valid_utf_8 value) then
      Error (Error.defect ~message:(field ^ " must be valid UTF-8"))
    else Ok ()
  in
  match validate_required "external workflow id" workflow_id with
  | Error _ as error -> error
  | Ok () -> validate_optional "external run id" run_id

(** Sends a typed signal to another workflow execution. The returned future
    suspends the current workflow until Core reports delivery or a structured
    failure; it does not perform nondeterministic network I/O itself. *)
let signal_external_workflow ~workflow_id ~run_id ~(signal : 'input Signal.t)
    ~input =
  let outside_error () =
    Error.defect ~message:"external workflow signal used outside a workflow execution"
  in
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None -> Future_private.resolved ~outside_error (Error (outside_error ()))
  | Some context -> (
      match validate_external_target ~workflow_id ~run_id with
      | Error error ->
          Future_private.resolved ~outside_error (Error error)
      | Ok () -> (
          match Codec_private.encode_base (Signal.input signal) input with
          | Error error ->
              Future_private.resolved ~outside_error
                (Error (Error_private.of_base error))
          | Ok payload ->
              let future =
                Temporal_sdk_kernel.Workflow_context_store.signal_external_workflow
                  context ~workflow_id ~run_id
                  ~signal_name:(Signal.name signal) ~input:[ payload ] ()
              in
              Future_private.of_internal future))

(** Requests cancellation of another workflow execution. Core owns delivery and
    target lookup; the returned future therefore reports acknowledgement or a
    typed Temporal failure rather than raising an exception. *)
let cancel_external_workflow ~workflow_id ~run_id ~reason =
  let outside_error () =
    Error.defect ~message:"external workflow cancellation used outside a workflow execution"
  in
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None -> Future_private.resolved ~outside_error (Error (outside_error ()))
  | Some context -> (
      match validate_external_target ~workflow_id ~run_id with
      | Error error ->
          Future_private.resolved ~outside_error (Error error)
      | Ok () when String.equal reason "" ->
          Future_private.resolved ~outside_error
            (Error (Error.defect ~message:"external cancellation reason must not be empty"))
      | Ok () when String.contains reason '\000' ->
          Future_private.resolved ~outside_error
            (Error (Error.defect ~message:"external cancellation reason must not contain NUL"))
      | Ok () when String.length reason > max_name_bytes ->
          Future_private.resolved ~outside_error
            (Error (Error.defect ~message:"external cancellation reason exceeds 65536 bytes"))
      | Ok () when not (Temporal_base.Codec.valid_utf_8 reason) ->
          Future_private.resolved ~outside_error
            (Error (Error.defect ~message:"external cancellation reason must be valid UTF-8"))
      | Ok () ->
          Future_private.of_internal
            (Temporal_sdk_kernel.Workflow_context_store.cancel_external_workflow
               context ~workflow_id ~run_id ~reason ()))

(** Reads the deterministic timestamp captured from the current Temporal
    activation. The runtime stores it in the workflow context before invoking
    user code, which means repeated calls during one activation observe the
    same value and replay does not consult local time. *)
let now () =
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None ->
      Error
        (Error.defect
           ~message:"Temporal.Workflow.now used outside a workflow execution")
  | Some context -> (
      match
        Temporal_sdk_kernel.Workflow_context_store.activation_timestamp context
      with
      | None ->
          Error
            (Error.defect
               ~message:
                 "Temporal.Workflow.now is unavailable for this activation")
      | Some timestamp ->
            Time.of_unix ~seconds:timestamp.seconds
            ~nanoseconds:timestamp.nanoseconds)

(** Draws a deterministic pseudo-random integer for the current workflow run.
    Temporal supplies the run seed in the initialization activation; the
    private runtime advances an execution-local stream so replay observes the
    same value at each call without reading host randomness. *)
let random_int ~bound =
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None ->
      Error
        (Error.defect
           ~message:"Temporal.Workflow.random_int used outside a workflow execution")
  | Some context ->
      Temporal_sdk_kernel.Workflow_context_store.random_int context ~bound
      |> Result.map_error Error_private.of_base

(** Reads the deployment identity retained for the current activation. The
    runtime clears this field before every task, so [None] cannot accidentally
    report a previous task's build. *)
let current_deployment_version () =
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None -> None
  | Some context ->
      Option.map
        (fun (deployment_name, build_id) -> { deployment_name; build_id })
        (Temporal_sdk_kernel.Workflow_context_store.activation_deployment_version
           context)

(** Validates and snapshots a patch ID before consulting workflow state. The
    copy prevents a caller-created mutable string from changing the hash-table
    key or emitted command after this call returns. *)
let validated_patch_id ~operation id =
  let prefix = "Temporal.Workflow." ^ operation ^ " id " in
  if String.length id = 0 then
    invalid_arg (prefix ^ "is empty");
  if String.contains id '\000' then
    invalid_arg (prefix ^ "contains a NUL byte")
  else if String.length id > max_name_bytes then
    invalid_arg (prefix ^ "exceeds 65536 bytes")
  else if not (Temporal_base.Codec.valid_utf_8 id) then
    invalid_arg (prefix ^ "must be valid UTF-8");
  Bytes.to_string (Bytes.of_string id)

(** Returns the replay-safe branch decision for one named workflow patch.
    Core notifications and the activation replay flag determine the first
    decision; the execution context retains it for later calls in this run. *)
let patched ~id =
  let patch_id = validated_patch_id ~operation:"patched" id in
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None ->
      invalid_arg "Temporal.Workflow.patched used outside a workflow execution"
  | Some context ->
      Temporal_sdk_kernel.Workflow_context_store.patched context ~patch_id

(** Records that one previously active patch is being phased out. The runtime
    retains Core's deterministic decision internally but exposes no branch
    result: application code should replace the old [patched] gate with this
    lifecycle marker while compatible histories drain. *)
let deprecate_patch ~id =
  let patch_id = validated_patch_id ~operation:"deprecate_patch" id in
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None ->
      invalid_arg
        "Temporal.Workflow.deprecate_patch used outside a workflow execution"
  | Some context ->
      Temporal_sdk_kernel.Workflow_context_store.deprecate_patch context ~patch_id

(** Validates one search-attribute key at the public boundary. Search
    attributes are persisted in workflow history, so rejecting malformed keys
    before buffering a command prevents a programmer error from becoming a
    delayed worker failure. *)
let validate_search_attribute_key key =
  if String.equal key "" then invalid_arg "Temporal search-attribute key is empty";
  if String.contains key '\000' then
    invalid_arg "Temporal search-attribute key contains a NUL byte";
  if String.length key > max_name_bytes then
    invalid_arg "Temporal search-attribute key exceeds 65536 bytes";
  if not (Temporal_base.Codec.valid_utf_8 key) then
    invalid_arg "Temporal search-attribute key must be valid UTF-8"

(** Converts public payloads into private owned values and emits one Core
    search-attribute merge command. No network or nondeterministic operation
    occurs until the enclosing workflow activation is completed. *)
let upsert_search_attributes values =
  let rec validate_keys seen = function
    | [] -> ()
    | (key, _) :: rest ->
        validate_search_attribute_key key;
        if List.exists (String.equal key) seen then
          invalid_arg "Temporal search-attribute keys must be unique"
        else validate_keys (key :: seen) rest
  in
  validate_keys [] values;
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None ->
      invalid_arg
        "Temporal.Workflow.upsert_search_attributes used outside a workflow execution"
  | Some context ->
      let values = List.map (fun (key, payload) -> (key, Payload_private.to_base payload)) values in
      Temporal_sdk_kernel.Workflow_context_store.upsert_search_attributes context values

(** Requests a fresh run of the same workflow type with [input]. This is a
    terminal direct-style operation: it encodes the successor input, buffers a
    Core continue-as-new command, and aborts the current private workflow
    fiber. If encoding fails, the current run is failed with that typed codec
    error instead of raising it through the worker loop. *)
let continue_as_new definition next_input =
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None ->
      invalid_arg "Temporal.Workflow.continue_as_new used outside a workflow execution"
  | Some context -> (
      match Codec_private.encode_base (input definition) next_input with
      | Ok payload ->
          Temporal_sdk_kernel.Workflow_context_store.continue_as_new context
            ~workflow_type:(name definition) ~input:payload
      | Error error ->
          Temporal_sdk_kernel.Workflow_context_store.terminate context
            (Temporal_sdk_kernel.Activation.Fail_workflow error))

(** Independently owned metadata observed on the run's durable start event. *)
type start_metadata = {
  memo : (string * Payload.t) list option;
  search_attributes : (string * Payload.t) list option;
  execution_expiration_time : Time.t option;
}

(** Converts the retained exact expiration timestamp to public time without
    changing the server-owned deadline or scheduling a workflow command. *)
let start_metadata () =
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None -> Error (Error.defect
      ~message:"Temporal.Workflow.start_metadata used outside a workflow execution")
  | Some context ->
      match Temporal_sdk_kernel.Workflow_context_store.start_metadata context with
      | None -> Error (Error.defect
          ~message:"Temporal.Workflow.start_metadata is unavailable for this execution")
      | Some metadata ->
          let expiration = match metadata.execution_expiration_time with
            | None -> Ok None
            | Some time ->
                Result.map Option.some
                  (Time.of_unix ~seconds:time.seconds ~nanoseconds:time.nanoseconds)
          in
          Result.map (fun execution_expiration_time ->
            { memo = Option.map (List.map (fun (key, value) -> (key, Payload_private.of_base value))) metadata.memo;
              search_attributes = Option.map (List.map (fun (key, value) -> (key, Payload_private.of_base value))) metadata.search_attributes;
              execution_expiration_time }) expiration
