(** OCaml names for Rust bridge status codes. An unrecognized number is kept in
    [Unknown] so diagnostics remain available across a version mismatch. *)
type status =
  | Invalid_argument
  | Abi_mismatch
  | Panic
  | Internal
  | Invalid_state
  | Configuration
  | Connection
  | Worker
  | Outstanding_tasks
  | Not_ready
  | Protocol
  | Already_started
  | Retryable
  | Async_heartbeat_rejected
  | Resource_exhausted
  | Unknown of int

(** Error data copied into OCaml. It never owns Rust memory. *)
type error = {
  status : status;
  message : string;
}

(** Shared source and tag vocabulary. It contains reporter exceptions so a
    consumer's logging setup cannot change bridge behavior. *)
module Observability = Temporal_base.Observability

(** The shared strict parser also owns the canonical base64 implementation used
    by replay-history documents. Keeping this validation in OCaml means a
    caller cannot bypass the semantic boundary merely by calling the private
    native bridge module instead of the supervisor. *)
module Control_protocol = Temporal_protocol.Control_protocol

(** Version requested by this binding layer. *)
let abi_version = 4l

(** Private OCaml value implemented in C. It owns a Rust result allocation until
    [decode] frees it or the OCaml garbage collector runs its finalizer. *)
type response

(** Opaque owner of one Temporal Core runtime and its Tokio executor. Only the
    SDK supervisor may use or close it; workflow code never sees this type. *)
type runtime

(** Opaque owner of one Core runtime shared by several [runtime] graphs
    (#832). Each attached graph holds its own native reference to the shared
    Core, so closing this value never frees Core under a live graph. *)
type shared_runtime

(** Validated client connection settings. The concrete JSON representation is
    private so callers cannot bypass sender-side checks. *)
type client_config = {
  target_url : string;
  identity : string;
}

(** Selects whether Core should use no worker versioning or its legacy build-ID
    routing. The top-level [build_id] remains present in both modes because it
    is also the worker identity sent in Core metadata. *)
type default_versioning_behavior =
  | Auto_upgrade
  | Pinned

type worker_versioning =
  | No_versioning
  | Legacy_build_id of string
  | Deployment_based of {
      deployment_name : string;
      build_id : string;
      use_worker_versioning : bool;
      default_versioning_behavior : default_versioning_behavior option;
    }

let ( let* ) = Result.bind

(** Autoscaling bounds for Core's workflow-task pollers (#498). *)
type poller_autoscaling = { minimum : int; maximum : int; initial : int }

(** Optional Core worker settings added after the original worker document
    (#498). [None] leaves Core's own default in force, and an all-[None]
    value is not serialized at all, so default workers keep sending the
    exact document that older bridge archives accept. *)
type worker_tuning = {
  workflow_task_poller_autoscaling : poller_autoscaling option;
  sticky_queue_schedule_to_start_timeout_ms : int64 option;
  max_heartbeat_throttle_interval_ms : int64 option;
  default_heartbeat_throttle_interval_ms : int64 option;
  max_worker_activities_per_second : float option;
  max_task_queue_activities_per_second : float option;
}

(** Leaves every tuning setting at Core's default. *)
let default_worker_tuning =
  {
    workflow_task_poller_autoscaling = None;
    sticky_queue_schedule_to_start_timeout_ms = None;
    max_heartbeat_throttle_interval_ms = None;
    default_heartbeat_throttle_interval_ms = None;
    max_worker_activities_per_second = None;
    max_task_queue_activities_per_second = None;
  }

(** Validated workflow-only worker settings retained as ordinary OCaml data
    until the supervisor serializes worker construction. *)
type worker_config = {
  namespace : string;
  task_queue : string;
  build_id : string;
  versioning : worker_versioning;
  max_cached_workflows : int;
  max_outstanding_workflow_tasks : int;
  max_concurrent_workflow_task_polls : int;
  graceful_shutdown_timeout_ms : int64;
  tuning : worker_tuning;
      (** Optional Core settings; see {!worker_tuning}. *)
  workflow_tasks : bool;
      (** Poll workflow tasks. False only when no workflow is registered. *)
  activity_tasks : bool;
      (** Poll remote activity tasks. False only when no activity is
          registered, so the worker never takes an activity that a sibling
          worker on the same task queue could execute (#805). *)
}

(** Private transport-safety ceiling mirrored and revalidated by Rust. This is
    not a Temporal Server identifier policy; Core and Server perform semantic
    field validation. *)
let max_transport_string_bytes = 65_536

(** Resource ceiling mirrored by the Rust worker-config adapter. *)
let max_worker_count = 1_000_000

(** Temporal Core rejects a cached-workflow worker unless it has at least two
    workflow-task pollers. Keep this sender-side invariant mirrored in Rust so
    invalid JSON cannot reach Core through another bridge entry point. *)
let min_cached_workflow_polls = 2

(** Maximum accepted graceful shutdown period in milliseconds. *)
let max_graceful_shutdown_timeout_ms = 86_400_000L

(** Maximum accepted sticky-queue timeout and heartbeat throttle interval in
    milliseconds (one day), mirrored by the Rust bridge. A longer value is far
    more likely to be a unit mistake than a deliberate policy, and the bound
    keeps every value representable as a protobuf duration. *)
let max_worker_tuning_duration_ms = 86_400_000L

(** Largest explicit Tokio worker-thread count for one runtime. Mirrors
    [MAX_RUNTIME_WORKER_THREADS] in the Rust bridge, which rejects larger
    values again; validating here keeps an invalid count from allocating the
    OCaml runtime owner at all. *)
let max_runtime_worker_threads = 256

(** Validates an explicit runtime worker-thread count; [None] selects the
    bridge default and is always accepted. *)
let validate_runtime_worker_threads = function
  | None -> Ok ()
  | Some count when count >= 1 && count <= max_runtime_worker_threads -> Ok ()
  | Some count ->
      Error
        {
          status = Invalid_argument;
          message =
            Printf.sprintf
              "runtime worker thread count must be between 1 and %d, got %d"
              max_runtime_worker_threads count;
        }

external check_abi_version_raw : int32 -> response
  = "ocaml_temporal_check_abi_version"

external echo_raw : bytes -> response = "ocaml_temporal_echo"

external conformance_wait_ms_raw : int -> response
  = "ocaml_temporal_conformance_wait_ms"

(** Reads the process-local monotonic clock in nanoseconds; see the
    interface. Implemented in C, not Rust, so the bridge ABI is unchanged. *)
external monotonic_now_ns : unit -> int64 = "ocaml_temporal_monotonic_now_ns"

external response_status : response -> int = "ocaml_temporal_response_status"
external response_value : response -> bytes = "ocaml_temporal_response_value"
external response_error : response -> string = "ocaml_temporal_response_error"
external response_free : response -> unit = "ocaml_temporal_response_free"
external runtime_create_raw : int -> runtime * response
  = "ocaml_temporal_runtime_create"

external runtime_close_raw : runtime -> int = "ocaml_temporal_runtime_close"

external shared_runtime_create_raw : int -> shared_runtime * response
  = "ocaml_temporal_shared_runtime_create"

external runtime_attach_raw : shared_runtime -> runtime * response
  = "ocaml_temporal_runtime_attach"

external shared_runtime_close_raw : shared_runtime -> int
  = "ocaml_temporal_shared_runtime_close"

external client_connect_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_connect"

external client_start_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_start_workflow_json"

external client_cancel_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_cancel_workflow_json"

external client_reset_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_reset_workflow_json"

external client_terminate_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_terminate_workflow_json"

external client_signal_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_signal_workflow_json"

external client_list_visibility_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_list_visibility_json"

external client_query_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_query_workflow_json"

external client_update_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_update_workflow_json"

external client_poll_update_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_poll_update_workflow_json"

external client_begin_start_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_begin_start_workflow_json"

external client_poll_start_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_poll_start_workflow_json"

external client_wait_start_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_wait_start_workflow_json"

external client_wait_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_wait_workflow_json"

external client_complete_async_activity_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_complete_async_activity_json"

external client_record_async_activity_heartbeat_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_record_async_activity_heartbeat_json"

external worker_start_raw : runtime -> bytes -> response
  = "ocaml_temporal_worker_start"

external replay_worker_start_raw : runtime -> bytes -> response
  = "ocaml_temporal_replay_worker_start"

external replay_worker_feed_history_raw : runtime -> bytes -> response
  = "ocaml_temporal_replay_worker_feed_history"

external replay_worker_finish_input_raw : runtime -> response
  = "ocaml_temporal_replay_worker_finish_input"

external replay_worker_try_poll_workflow_raw : runtime -> response
  = "ocaml_temporal_replay_worker_try_poll_workflow"

external replay_worker_wait_workflow_raw : runtime -> response
  = "ocaml_temporal_replay_worker_wait_workflow"

external replay_worker_complete_workflow_raw : runtime -> bytes -> response
  = "ocaml_temporal_replay_worker_complete_workflow"

external replay_worker_reject_workflow_raw : runtime -> bytes -> response
  = "ocaml_temporal_replay_worker_reject_workflow"

external replay_worker_finalize_raw : runtime -> response
  = "ocaml_temporal_replay_worker_finalize"

external replay_worker_dispose_raw : runtime -> response
  = "ocaml_temporal_replay_worker_dispose"

external worker_try_poll_workflow_raw : runtime -> response
  = "ocaml_temporal_worker_try_poll_workflow"

external worker_wait_workflow_raw : runtime -> response
  = "ocaml_temporal_worker_wait_workflow"

external worker_complete_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_worker_complete_workflow_json"

external worker_reject_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_worker_reject_workflow_json"

external worker_try_poll_activity_raw : runtime -> response
  = "ocaml_temporal_worker_try_poll_activity"

external worker_wait_activity_raw : runtime -> response
  = "ocaml_temporal_worker_wait_activity"

external worker_wait_any_raw : runtime -> response
  = "ocaml_temporal_worker_wait_any"

external worker_wait_activity_completion_retry_backoff_raw : runtime -> response
  = "ocaml_temporal_worker_wait_activity_completion_retry_backoff"

external worker_complete_activity_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_worker_complete_activity_json"

external worker_record_activity_heartbeat_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_worker_record_activity_heartbeat_json"

external worker_reject_activity_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_worker_reject_activity_json"

external worker_shutdown_raw : runtime -> response
  = "ocaml_temporal_worker_shutdown"

external client_disconnect_raw : runtime -> response
  = "ocaml_temporal_client_disconnect"

(** Converts known numeric statuses and retains every newer value as [Unknown]. *)
let status = function
  | 1 -> Invalid_argument
  | 2 -> Abi_mismatch
  | 3 -> Panic
  | 4 -> Internal
  | 5 -> Invalid_state
  | 6 -> Configuration
  | 7 -> Connection
  | 8 -> Worker
  | 9 -> Outstanding_tasks
  | 10 -> Not_ready
  | 11 -> Protocol
  | 12 -> Already_started
  | 13 -> Retryable
  | 14 -> Async_heartbeat_rejected
  | 15 -> Resource_exhausted
  | code -> Unknown code

(** Converts a bridge status to a bounded stable tag value without exposing the
    Rust-owned diagnostic message. *)
let status_name = function
  | Invalid_argument -> "invalid_argument"
  | Abi_mismatch -> "abi_mismatch"
  | Panic -> "panic"
  | Internal -> "internal"
  | Invalid_state -> "invalid_state"
  | Configuration -> "configuration"
  | Connection -> "connection"
  | Worker -> "worker"
  | Outstanding_tasks -> "outstanding_tasks"
  | Not_ready -> "not_ready"
  | Protocol -> "protocol"
  | Already_started -> "already_started"
  | Retryable -> "retryable"
  | Async_heartbeat_rejected -> "async_heartbeat_rejected"
  | Resource_exhausted -> "resource_exhausted"
  | Unknown _ -> "unknown"

(** Constructs a local configuration failure without entering native code. *)
let configuration_error message = Error { status = Configuration; message }

(** Validates only bridge-owned string invariants before Core sees the value. *)
let validate_identifier name value =
  if String.length value = 0 then
    configuration_error (name ^ " must not be empty")
  else if String.contains value '\000' then
    configuration_error (name ^ " must not contain NUL")
  else if String.length value > max_transport_string_bytes then
    configuration_error
      (Printf.sprintf "%s exceeds %d UTF-8 bytes" name
         max_transport_string_bytes)
  else Ok ()

(** Performs the inexpensive sender-side absolute HTTP(S) shape check. Rust's
    URL parser repeats and completes validation before network access. *)
let validate_target_url value =
  let host_after prefix =
    let prefix_length = String.length prefix in
    String.starts_with ~prefix value
    && String.length value > prefix_length
    &&
    let remainder =
      String.sub value prefix_length (String.length value - prefix_length)
    in
    let host =
      match String.index_opt remainder '/' with
      | None -> remainder
      | Some index -> String.sub remainder 0 index
    in
    String.length host > 0
    && not (String.exists (fun character -> Char.code character <= 32) host)
  in
  if String.length value > max_transport_string_bytes then
    configuration_error
      (Printf.sprintf "target_url exceeds %d UTF-8 bytes"
         max_transport_string_bytes)
  else if host_after "http://" || host_after "https://" then Ok ()
  else configuration_error "target_url must be an absolute http or https URL"

(** Validates one bounded count, allowing zero only for disabled cache size. *)
let validate_count ~allow_zero name value =
  let minimum = if allow_zero then 0 else 1 in
  if value < minimum || value > max_worker_count then
    configuration_error
      (Printf.sprintf "%s must be between %d and %d" name minimum
         max_worker_count)
  else Ok ()

(** Creates client settings only after all sender-side invariants hold. *)
let client_config ~target_url ~identity =
  match validate_target_url target_url with
  | Error _ as error -> error
  | Ok () ->
      Result.map
        (fun () -> { target_url; identity })
        (validate_identifier "identity" identity)

(** Checks one optional positive tuning duration against
    [max_worker_tuning_duration_ms]. *)
let validate_tuning_duration name = function
  | None -> Ok ()
  | Some value
    when Int64.compare value 0L > 0
         && Int64.compare value max_worker_tuning_duration_ms <= 0 ->
      Ok ()
  | Some _ ->
      configuration_error
        (Printf.sprintf "%s must be between 1 and %Ld" name
           max_worker_tuning_duration_ms)

(** Checks one optional rate: Core rejects zero, negative, NaN, infinite and
    subnormal worker rates, and the task-queue rate is held to the same rule
    because the server treats it as a positive dispatch limit. *)
let validate_rate name = function
  | None -> Ok ()
  | Some value when Float.classify_float value = FP_normal && value > 0.0 ->
      Ok ()
  | Some _ -> configuration_error (name ^ " must be a positive finite number")

(** Smallest accepted per-worker activity rate: one poll per day. *)
let min_worker_rate = 1.0 /. 86_400.0

(** Checks the per-worker rate. Core computes its poll interval as
    [Duration::from_secs_f64 (1 / rate)], which panics inside worker
    construction when the reciprocal overflows, so the reciprocal is bounded
    to one day. The task-queue rate is only forwarded to the server. *)
let validate_worker_rate name value =
  let* () = validate_rate name value in
  match value with
  | Some rate when rate < min_worker_rate ->
      configuration_error (name ^ " must be at least one per day (1/86400)")
  | _ -> Ok ()

(** Validates the optional settings together with the poller maximum they
    must agree with. An autoscaling poller's maximum is carried in
    [max_concurrent_workflow_task_polls], so the two can never disagree. *)
let validate_tuning ~max_concurrent_workflow_task_polls tuning =
  let* () =
    match tuning.workflow_task_poller_autoscaling with
    | None -> Ok ()
    | Some { minimum; maximum; initial } ->
        let* () =
          validate_count ~allow_zero:false
            "workflow_task_poller_autoscaling.minimum" minimum
        in
        let* () =
          validate_count ~allow_zero:false
            "workflow_task_poller_autoscaling.maximum" maximum
        in
        if maximum < minimum then
          configuration_error
            "workflow_task_poller_autoscaling.maximum must be at least minimum"
        else if initial < minimum || initial > maximum then
          configuration_error
            "workflow_task_poller_autoscaling.initial must be between minimum \
             and maximum"
        else if maximum <> max_concurrent_workflow_task_polls then
          configuration_error
            "workflow_task_poller_autoscaling.maximum must equal \
             max_concurrent_workflow_task_polls"
        else Ok ()
  in
  let* () =
    validate_tuning_duration "sticky_queue_schedule_to_start_timeout_ms"
      tuning.sticky_queue_schedule_to_start_timeout_ms
  in
  let* () =
    validate_tuning_duration "max_heartbeat_throttle_interval_ms"
      tuning.max_heartbeat_throttle_interval_ms
  in
  let* () =
    validate_tuning_duration "default_heartbeat_throttle_interval_ms"
      tuning.default_heartbeat_throttle_interval_ms
  in
  let* () =
    (* Core silently clips the default interval to the maximum; reject the
       explicit contradiction instead of hiding it. *)
    match
      ( tuning.default_heartbeat_throttle_interval_ms,
        tuning.max_heartbeat_throttle_interval_ms )
    with
    | Some default, Some maximum when Int64.compare default maximum > 0 ->
        configuration_error
          "default_heartbeat_throttle_interval_ms must not exceed \
           max_heartbeat_throttle_interval_ms"
    | _ -> Ok ()
  in
  let* () =
    validate_worker_rate "max_worker_activities_per_second"
      tuning.max_worker_activities_per_second
  in
  validate_rate "max_task_queue_activities_per_second"
    tuning.max_task_queue_activities_per_second

(** Creates workflow-only worker settings after validating every field. *)
let worker_config ~namespace ~task_queue ~build_id ?(versioning = No_versioning)
    ~max_cached_workflows
    ~max_outstanding_workflow_tasks ~max_concurrent_workflow_task_polls
    ~graceful_shutdown_timeout_ms ?(tuning = default_worker_tuning)
    ?(workflow_tasks = true) ?(activity_tasks = true) () =
  let validations =
    [
      (if workflow_tasks || activity_tasks then Ok ()
       else
         configuration_error "task_types must enable workflows or activities");
      validate_identifier "namespace" namespace;
      validate_identifier "task_queue" task_queue;
      validate_identifier "build_id" build_id;
      (match versioning with
      | No_versioning -> Ok ()
      | Legacy_build_id versioning_build_id ->
          (match validate_identifier "versioning.build_id" versioning_build_id with
          | Error _ as error -> error
          | Ok () ->
              if String.equal build_id versioning_build_id then Ok ()
              else configuration_error "versioning.build_id must match build_id")
      | Deployment_based
          {
            deployment_name;
            build_id = deployment_build_id;
            use_worker_versioning;
            default_versioning_behavior;
          } ->
          let* () = validate_identifier "versioning.deployment_name" deployment_name in
          let* () = validate_identifier "versioning.build_id" deployment_build_id in
          let* () =
            if String.equal build_id deployment_build_id then Ok ()
            else configuration_error "versioning.build_id must match build_id"
          in
          (* Mirrors the Rust validator: completions never carry a
             per-workflow behavior, so a versioned worker must configure the
             default Core substitutes for UNSPECIFIED (issue #817). *)
          (match (use_worker_versioning, default_versioning_behavior) with
          | true, Some (Auto_upgrade | Pinned) | false, None -> Ok ()
          | true, None ->
              configuration_error
                "versioning.use_worker_versioning requires \
                 default_versioning_behavior"
          | false, Some _ ->
              configuration_error
                "versioning.default_versioning_behavior requires \
                 use_worker_versioning"));
      validate_count ~allow_zero:true "max_cached_workflows"
        max_cached_workflows;
      validate_count ~allow_zero:false "max_outstanding_workflow_tasks"
        max_outstanding_workflow_tasks;
      validate_count ~allow_zero:false "max_concurrent_workflow_task_polls"
        max_concurrent_workflow_task_polls;
      (* Core splits only a fixed poller count between the sticky and normal
         queues; autoscaling bounds apply to each queue, so the two-poller
         rule does not apply to them. *)
      (if
         max_cached_workflows > 0
         && Option.is_none tuning.workflow_task_poller_autoscaling
         && max_concurrent_workflow_task_polls < min_cached_workflow_polls
       then
         configuration_error
           "max_concurrent_workflow_task_polls must be at least 2 when max_cached_workflows is greater than zero"
       else Ok ());
      (if
         Int64.compare graceful_shutdown_timeout_ms 0L >= 0
         && Int64.compare graceful_shutdown_timeout_ms
              max_graceful_shutdown_timeout_ms
            <= 0
       then Ok ()
       else
         configuration_error
           "graceful_shutdown_timeout_ms must be between 0 and 86400000");
      validate_tuning ~max_concurrent_workflow_task_polls tuning;
    ]
  in
  match List.find_opt Result.is_error validations with
  | Some (Error _ as error) -> error
  | Some (Ok ()) -> assert false
  | None ->
      Ok
        {
          namespace;
          task_queue;
          build_id;
          versioning;
          max_cached_workflows;
          max_outstanding_workflow_tasks;
          max_concurrent_workflow_task_polls;
          graceful_shutdown_timeout_ms;
          tuning;
          workflow_tasks;
          activity_tasks;
        }

(** Encodes the exact strict client document accepted by the Rust adapter. *)
let encode_client_config config =
  `Assoc
    [
      ("target_url", `String config.target_url);
      ("identity", `String config.identity);
    ]
  |> Yojson.Safe.to_string |> Bytes.of_string

(** Encodes the optional [tuning] member, listing only explicit settings.
    Returns [None] for an all-default value so the member is omitted and the
    document stays byte-for-byte identical to the pre-#498 encoding. *)
let encode_worker_tuning tuning =
  let duration name = Option.map (fun ms -> (name, `Intlit (Int64.to_string ms))) in
  let rate name = Option.map (fun value -> (name, `Float value)) in
  let fields =
    List.filter_map Fun.id
      [
        Option.map
          (fun { minimum; maximum; initial } ->
            ( "workflow_task_poller_autoscaling",
              `Assoc
                [
                  ("minimum", `Int minimum);
                  ("maximum", `Int maximum);
                  ("initial", `Int initial);
                ] ))
          tuning.workflow_task_poller_autoscaling;
        duration "sticky_queue_schedule_to_start_timeout_ms"
          tuning.sticky_queue_schedule_to_start_timeout_ms;
        duration "max_heartbeat_throttle_interval_ms"
          tuning.max_heartbeat_throttle_interval_ms;
        duration "default_heartbeat_throttle_interval_ms"
          tuning.default_heartbeat_throttle_interval_ms;
        rate "max_worker_activities_per_second"
          tuning.max_worker_activities_per_second;
        rate "max_task_queue_activities_per_second"
          tuning.max_task_queue_activities_per_second;
      ]
  in
  match fields with [] -> None | fields -> Some ("tuning", `Assoc fields)

(** Encodes the exact strict workflow-worker document accepted by Rust. *)
let worker_config_document config =
  `Assoc
    ([
      ("namespace", `String config.namespace);
      ("task_queue", `String config.task_queue);
      ("build_id", `String config.build_id);
      ( "versioning",
        match config.versioning with
        | No_versioning -> `Assoc [ ("kind", `String "none") ]
        | Legacy_build_id build_id ->
            `Assoc
              [ ("kind", `String "legacy_build_id");
                ("build_id", `String build_id) ]
        | Deployment_based
            {
              deployment_name;
              build_id;
              use_worker_versioning;
              default_versioning_behavior;
            } ->
            `Assoc
              [ ("kind", `String "deployment_based");
                ("deployment_name", `String deployment_name);
                ("build_id", `String build_id);
                ("use_worker_versioning", `Bool use_worker_versioning);
                ( "default_versioning_behavior",
                  match default_versioning_behavior with
                  | None -> `Null
                  | Some Auto_upgrade -> `String "auto_upgrade"
                  | Some Pinned -> `String "pinned" ) ] );
      ("max_cached_workflows", `Int config.max_cached_workflows);
      ( "max_outstanding_workflow_tasks",
        `Int config.max_outstanding_workflow_tasks );
      ( "max_concurrent_workflow_task_polls",
        `Int config.max_concurrent_workflow_task_polls );
      ( "graceful_shutdown_timeout_ms",
        `Intlit (Int64.to_string config.graceful_shutdown_timeout_ms) );
      ( "task_types",
        `Assoc
          [
            ("workflows", `Bool config.workflow_tasks);
            ("activities", `Bool config.activity_tasks);
          ] );
    ]
    @ Option.to_list (encode_worker_tuning config.tuning))
  |> Yojson.Safe.to_string

(** The worker document as the bytes handed to the C stub. *)
let encode_worker_config config = Bytes.of_string (worker_config_document config)

(** Returns one closed protocol error for malformed replay input. The Rust
    side repeats the complete checks; this sender-side copy rejects bad JSON
    before a native call and keeps diagnostics independent of input content. *)
let replay_protocol_error () =
  Error
    {
      status = Protocol;
      message = "replay history document failed OCaml validation";
    }

(** Validates the replay-history envelope before it crosses the C boundary.
    The shared parser rejects duplicate keys and resource attacks, while the
    nested payload decoder proves canonical padded base64 and its byte limit. *)
let validate_replay_history input =
  let invalid () = replay_protocol_error () in
  match Control_protocol.decode_payload_object (Bytes.to_string input) with
  | Error _ -> invalid ()
  | Ok (`Assoc entries) -> (
      match
        ( List.assoc_opt "workflow_id" entries,
          List.assoc_opt "history" entries,
          List.length entries )
      with
      | Some (`String workflow_id), Some history_json, 2
        when String.length workflow_id > 0
             && String.length workflow_id <= max_transport_string_bytes
             && not (String.contains workflow_id '\000') -> (
          match Control_protocol.decode_payload_json history_json with
          | Ok _ -> Ok ()
          | Error _ -> invalid ())
      | _ -> invalid ())
  | Ok _ -> invalid ()

(** Copies either the successful bytes or error message into OCaml, then always
    frees the Rust allocation. [Fun.protect] still runs cleanup if copying
    raises an OCaml exception. *)
let decode response =
  Fun.protect
    ~finally:(fun () -> response_free response)
    (fun () ->
      let code = response_status response in
      if code = 0 then Ok (response_value response)
      else
        Error
          { status = status code; message = response_error response })

(** Chooses a log level and constant message for each typed bridge status. *)
let bridge_error_log_level = function
  | Not_ready ->
      (* Empty worker lanes and an open exact-run wait whose bounded interval
         elapsed are normal scheduler state, not failures that should page an
         operator. *)
      (Logs.Debug, "bridge operation not ready")
  | Outstanding_tasks ->
      (* Live worker shutdown force-completed a lease the language side never
         completed and still released the worker; replay reports undrained
         input. Keep this visible without classifying it as a bridge failure. *)
      (Logs.Warning, "bridge operation waiting for outstanding tasks")
  | _ ->
      (* Protocol, lifecycle, configuration, and native failures all indicate
         that the requested operation did not complete and need investigation. *)
      (Logs.Error, "bridge operation failed")

(** Measures one complete bridge operation, reports its structural outcome,
    and returns the original [result] unchanged. *)
let bridge_call operation action =
  let result, duration_ms = Observability.measure_ms action in
  let duration_tags = Observability.tags ~operation ~duration_ms () in
  Observability.report ~src:Observability.Source.bridge Logs.Debug
    ~tags:duration_tags "bridge operation completed";
  (match result with
  | Ok _ -> ()
  | Error error ->
      let tags =
        Observability.tags ~operation
          ~bridge_status:(status_name error.status) ()
      in
      let level, message = bridge_error_log_level error.status in
      Observability.report ~src:Observability.Source.bridge level ~tags message);
  result

(** Converts successful test operations with no useful output to [Ok ()] after
    [decode] has performed the normal memory cleanup. *)
let check_abi_version version =
  bridge_call "check_abi_version" (fun () ->
      Result.map (fun _ -> ()) (decode (check_abi_version_raw version)))

let echo input = bridge_call "echo" (fun () -> decode (echo_raw input))

let conformance_wait_ms milliseconds =
  bridge_call "conformance_wait_ms" (fun () ->
      Result.map (fun _ -> ())
        (decode (conformance_wait_ms_raw milliseconds)))

(** Connects the official Temporal client through the Rust-owned runtime. *)
let client_connect runtime config =
  bridge_call "client_connect" (fun () ->
      Result.map (fun _ -> ())
        (decode (client_connect_raw runtime (encode_client_config config))))

(** Starts a workflow through the Rust-owned client. The response or closed
    error document is copied before the Rust allocation is released. *)
let client_start_workflow_json runtime input =
  bridge_call "client_start_workflow_json" (fun () ->
      decode (client_start_workflow_json_raw runtime input))

(** Requests cancellation of one exact workflow run through Rust's official
    Temporal client. The call crosses only copied JSON; the C binding releases
    the OCaml runtime lock while the RPC executes. *)
let client_cancel_workflow_json runtime input =
  bridge_call "client_cancel_workflow_json" (fun () ->
      decode (client_cancel_workflow_json_raw runtime input))

(** Resets one exact workflow run and returns the new run identity copied from
    Rust's validated response. *)
let client_reset_workflow_json runtime input =
  bridge_call "client_reset_workflow_json" (fun () ->
      decode (client_reset_workflow_json_raw runtime input))

(** Terminates one exact workflow run through the Rust-owned client. *)
let client_terminate_workflow_json runtime input =
  bridge_call "client_terminate_workflow_json" (fun () ->
      decode (client_terminate_workflow_json_raw runtime input))

(** Sends one signal to one exact workflow run through the Rust-owned client. *)
let client_signal_workflow_json runtime input =
  bridge_call "client_signal_workflow_json" (fun () ->
      decode (client_signal_workflow_json_raw runtime input))

(** Lists one bounded visibility page through the Rust-owned client. *)
let client_list_visibility_json runtime input =
  bridge_call "client_list_visibility_json" (fun () ->
      decode (client_list_visibility_json_raw runtime input))

(** Executes one output-only query through the Rust-owned Temporal client. *)
let client_query_workflow_json runtime input =
  bridge_call "client_query_workflow_json" (fun () ->
      decode (client_query_workflow_json_raw runtime input))

let client_update_workflow_json runtime input =
  bridge_call "client_update_workflow_json" (fun () ->
      decode (client_update_workflow_json_raw runtime input))

let client_poll_update_workflow_json runtime input =
  bridge_call "client_poll_update_workflow_json" (fun () ->
      decode (client_poll_update_workflow_json_raw runtime input))

(** Admits one asynchronous workflow start and returns an opaque ticket JSON
    document. The native owner retains the Tokio task and all request metadata;
    this call only copies the admission result into OCaml. *)
let client_begin_start_workflow_json runtime input =
  bridge_call "client_begin_start_workflow_json" (fun () ->
      decode (client_begin_start_workflow_json_raw runtime input))

(** Polls an asynchronous start ticket without waiting. [Not_ready] is an
    expected result while the Rust task remains in flight. *)
let client_poll_start_workflow_json runtime input =
  bridge_call "client_poll_start_workflow_json" (fun () ->
      decode (client_poll_start_workflow_json_raw runtime input))

(** Waits for one bounded interval for an asynchronous start ticket. The C
    stub releases the OCaml runtime lock while Rust waits, so the supervisor
    Domain never blocks unrelated OCaml Domains. *)
let client_wait_start_workflow_json runtime input =
  bridge_call "client_wait_start_workflow_json" (fun () ->
      decode (client_wait_start_workflow_json_raw runtime input))

(** Waits for one exact run through the Rust-owned client. Each native call
    polls the retained history future for at most 100 ms while the C binding
    releases the OCaml runtime lock. [Not_ready] preserves the RPC and its
    pagination state so the caller can resume through the supervisor mailbox
    without occupying the owner indefinitely. *)
let client_wait_workflow_json runtime input =
  bridge_call "client_wait_workflow_json" (fun () ->
      decode (client_wait_workflow_json_raw runtime input))

(** Completes an admitted asynchronous activity through Rust's official
    namespace-bound client. The input is strict activity-completion JSON and is
    copied by the bridge before the C stub releases the OCaml runtime lock. *)
let client_complete_async_activity_json runtime input =
  bridge_call "client_complete_async_activity_json" (fun () ->
      Result.map (fun _ -> ())
        (decode (client_complete_async_activity_json_raw runtime input)))

(** Records a heartbeat for an admitted asynchronous activity. The worker task
    ledger is intentionally not consulted by this client operation. *)
let client_record_async_activity_heartbeat_json runtime input =
  bridge_call "client_record_async_activity_heartbeat_json" (fun () ->
      Result.map (fun _ -> ())
        (decode
           (client_record_async_activity_heartbeat_json_raw runtime input)))

(** Constructs and namespace-validates the official workflow-only worker. *)
let worker_start runtime config =
  bridge_call "worker_start" (fun () ->
      Result.map (fun _ -> ())
        (decode (worker_start_raw runtime (encode_worker_config config))))

(** Constructs the private workflow-only replay worker. It uses the same
    validated settings as a live worker but consumes caller-supplied histories
    instead of polling a Temporal Server. *)
let replay_worker_start runtime config =
  bridge_call "replay_worker_start" (fun () ->
      Result.map (fun _ -> ())
        (decode
           (replay_worker_start_raw runtime (encode_worker_config config))))

(** Validates and feeds one strict replay-history JSON document. Rust keeps the
    feeder bounded and applies backpressure while the C call releases the
    OCaml runtime lock. *)
let replay_worker_feed_history runtime input =
  bridge_call "replay_worker_feed_history" (fun () ->
      match validate_replay_history input with
      | Error _ as error -> error
      | Ok () ->
          Result.map (fun _ -> ())
            (decode (replay_worker_feed_history_raw runtime input)))

(** Closes replay input; later calls only drain and complete already admitted
    histories. *)
let replay_worker_finish_input runtime =
  bridge_call "replay_worker_finish_input" (fun () ->
      Result.map (fun _ -> ()) (decode (replay_worker_finish_input_raw runtime)))

(** Takes one already-ready replay activation without waiting. *)
let replay_worker_try_poll_workflow runtime =
  bridge_call "replay_worker_try_poll_workflow" (fun () ->
      decode (replay_worker_try_poll_workflow_raw runtime))

(** Waits for replay readiness under the same bounded lock-release contract as
    the live worker wait. *)
let replay_worker_wait_workflow runtime =
  bridge_call "replay_worker_wait_workflow" (fun () ->
      Result.map (fun _ -> ()) (decode (replay_worker_wait_workflow_raw runtime)))

(** Validates and submits one replay workflow completion. *)
let replay_worker_complete_workflow_json runtime input =
  bridge_call "replay_worker_complete_workflow" (fun () ->
      Result.map (fun _ -> ())
        (decode (replay_worker_complete_workflow_raw runtime input)))

(** Retires one replay activation after OCaml semantic decode failure. *)
let replay_worker_reject_workflow_json runtime input =
  bridge_call "replay_worker_reject_workflow" (fun () ->
      Result.map (fun _ -> ())
        (decode (replay_worker_reject_workflow_raw runtime input)))

(** Finalizes a naturally drained replay, retaining the native graph on error. *)
let replay_worker_finalize runtime =
  bridge_call "replay_worker_finalize" (fun () ->
      Result.map (fun _ -> ()) (decode (replay_worker_finalize_raw runtime)))

(** Explicitly abandons replay and force-completes native debts. *)
let replay_worker_dispose runtime =
  bridge_call "replay_worker_dispose" (fun () ->
      Result.map (fun _ -> ()) (decode (replay_worker_dispose_raw runtime)))

(** Takes one already-ready workflow activation without waiting for Core. The
    [Not_ready] status is an expected result while both poll lanes are empty;
    callers should yield or use the future readiness wait rather than treat it
    as worker failure. The returned bytes are a validated semantic JSON
    document owned by the OCaml heap after [decode] copies it. *)
let worker_try_poll_workflow runtime =
  bridge_call "worker_try_poll_workflow" (fun () ->
      decode (worker_try_poll_workflow_raw runtime))

(** Waits for workflow-lane readiness without consuming the activation. The C
    stub releases the OCaml runtime lock while Rust waits. [Not_ready] means
    the bounded wait elapsed; retry from the supervisor mailbox so lifecycle
    messages remain serviceable. A successful result is only a wake signal, so
    callers must drain with [worker_try_poll_workflow]. *)
let worker_wait_workflow runtime =
  bridge_call "worker_wait_workflow" (fun () ->
      Result.map (fun _ -> ()) (decode (worker_wait_workflow_raw runtime)))

(** Validates and submits one workflow activation completion. The caller must
    use the exact run identifier from a previously leased activation; Rust's
    task ledger rejects unknown or duplicate completions before Core sees them.
    Input bytes are copied by the C stub before the OCaml runtime lock is
    released and are never retained after this call. *)
let worker_complete_workflow_json runtime input =
  bridge_call "worker_complete_workflow_json" (fun () ->
      Result.map (fun _ -> ())
        (decode (worker_complete_workflow_json_raw runtime input)))

(** Returns an activation document produced by Rust when OCaml's semantic
    decoder cannot accept it. Rust reparses and compares the complete value
    with its retained activation before retiring the one-shot lease. *)
let worker_reject_workflow_json runtime input =
  bridge_call "worker_reject_workflow_json" (fun () ->
      Result.map (fun _ -> ())
        (decode (worker_reject_workflow_json_raw runtime input)))

(** Takes one already-ready remote activity task without waiting for Core. The
    returned bytes contain the closed activity-task JSON document; activity
    cancellation remains correlated by its opaque token in that document. *)
let worker_try_poll_activity runtime =
  bridge_call "worker_try_poll_activity" (fun () ->
      decode (worker_try_poll_activity_raw runtime))

(** Waits for remote-activity-lane readiness under the same bounded,
    runtime-lock-free contract as [worker_wait_workflow]. The wake does not
    consume a task; drain it with [worker_try_poll_activity]. *)
let worker_wait_activity runtime =
  bridge_call "worker_wait_activity" (fun () ->
      Result.map (fun _ -> ()) (decode (worker_wait_activity_raw runtime)))

(** Waits for readiness on either worker lane under the same bounded,
    runtime-lock-free contract as [worker_wait_workflow]. A queued task on
    either lane ends the wait without being consumed; drain it with the
    matching [worker_try_poll_*] call. *)
let worker_wait_any runtime =
  bridge_call "worker_wait_any" (fun () ->
      Result.map (fun _ -> ()) (decode (worker_wait_any_raw runtime)))

(** Applies the fixed native delay used only after Rust explicitly reports a
    retryable activity-completion transport outcome. The C stub releases the
    OCaml runtime lock while Rust sleeps on the supervisor owner Domain. *)
let worker_wait_activity_completion_retry_backoff runtime =
  bridge_call "worker_wait_activity_completion_retry_backoff" (fun () ->
      Result.map (fun _ -> ())
        (decode (worker_wait_activity_completion_retry_backoff_raw runtime)))

(** Validates and submits one remote activity completion. Rust checks the
    opaque task token against the outstanding ledger before completing Core, so
    a stale or duplicated completion is reported as a typed bridge error. *)
let worker_complete_activity_json runtime input =
  bridge_call "worker_complete_activity_json" (fun () ->
      Result.map (fun _ -> ())
        (decode (worker_complete_activity_json_raw runtime input)))

(** Validates and submits one heartbeat for a currently leased activity. Rust
    checks the opaque token against the outstanding ledger but does not retire
    it, because terminal completion remains a separate operation. The result is
    deliberately an acknowledgement only: pinned Temporal Core reports
    cancellation, pause, and reset flags asynchronously in a later Cancel task
    rather than fabricating synchronous status here. *)
let worker_record_activity_heartbeat_json runtime input =
  bridge_call "worker_record_activity_heartbeat_json" (fun () ->
      Result.map (fun _ -> ())
        (decode (worker_record_activity_heartbeat_json_raw runtime input)))

(** Returns a Rust-produced activity-task document after OCaml decode failure.
    Rust reparses and compares the complete task with retained handoff state;
    only then may its opaque-token obligation be retired. *)
let worker_reject_activity_json runtime input =
  bridge_call "worker_reject_activity_json" (fun () ->
      Result.map (fun _ -> ())
        (decode (worker_reject_activity_json_raw runtime input)))

(** Gracefully closes the worker. Rust treats repetition as success. *)
let worker_shutdown runtime =
  bridge_call "worker_shutdown" (fun () ->
      Result.map (fun _ -> ()) (decode (worker_shutdown_raw runtime)))

(** Drops the connected client after its worker is absent. *)
let client_disconnect runtime =
  bridge_call "client_disconnect" (fun () ->
      Result.map (fun _ -> ()) (decode (client_disconnect_raw runtime)))

(** Closes the native owner after first clearing its OCaml-held pointer. This
    makes repeated sequential calls safe; the production supervisor serializes
    all lifecycle calls across Domains. *)
let runtime_close runtime =
  let result =
    bridge_call "runtime_close" (fun () ->
        match runtime_close_raw runtime with
        | 0 -> Ok ()
        | code ->
            Error
              {
                status = status code;
                message = "Temporal Core runtime close failed";
              })
  in
  let level, message, bridge_status =
    match result with
    | Ok () -> (Logs.Info, "runtime closed", None)
    | Error error ->
        (Logs.Error, "runtime shutdown failed", Some (status_name error.status))
  in
  let tags =
    Observability.tags ~operation:"runtime_close" ?bridge_status ()
  in
  Observability.report ~src:Observability.Source.lifecycle level ~tags message;
  result

(** Checks the linked bridge contract once, then creates the native runtime.
    An invalid [worker_threads] is rejected before any allocation. If creation
    fails after allocating the OCaml owner, cleanup remains safe because its
    native pointer is either null or explicitly closed here. The C ABI encodes
    the bridge default as [0]. *)
let runtime_create ?worker_threads () =
  let result =
    bridge_call "runtime_create" (fun () ->
        match validate_runtime_worker_threads worker_threads with
        | Error _ as error -> error
        | Ok () -> (
        match check_abi_version abi_version with
        | Error _ as error -> error
        | Ok () ->
            let runtime, response =
              runtime_create_raw (Option.value worker_threads ~default:0)
            in
            (match decode response with
            | Ok _ -> Ok runtime
            | Error error ->
                ignore (runtime_close runtime);
                Error error)))
  in
  let level, message, bridge_status =
    match result with
    | Ok _ -> (Logs.Info, "runtime initialized", None)
    | Error error ->
        (Logs.Error, "runtime initialization failed", Some (status_name error.status))
  in
  let tags =
    Observability.tags ~operation:"runtime_create" ?bridge_status ()
  in
  Observability.report ~src:Observability.Source.lifecycle level ~tags message;
  result

(** Reports one shared-runtime lifecycle transition on the lifecycle source,
    mirroring [runtime_create] and [runtime_close]. *)
let report_shared_lifecycle ~operation ~ok_message ~error_message result =
  let level, message, bridge_status =
    match result with
    | Ok _ -> (Logs.Info, ok_message, None)
    | Error error -> (Logs.Error, error_message, Some (status_name error.status))
  in
  let tags = Observability.tags ~operation ?bridge_status () in
  Observability.report ~src:Observability.Source.lifecycle level ~tags message;
  result

(** Releases the shared handle's Core reference. Repeating it is safe; Core
    is destroyed before this returns only when no attached graph remains. *)
let shared_runtime_close shared =
  bridge_call "shared_runtime_close" (fun () ->
      match shared_runtime_close_raw shared with
      | 0 -> Ok ()
      | code ->
          Error
            {
              status = status code;
              message = "Temporal Core shared runtime close failed";
            })
  |> report_shared_lifecycle ~operation:"shared_runtime_close"
       ~ok_message:"shared runtime closed"
       ~error_message:"shared runtime shutdown failed"

(** Checks the bridge contract, then creates one shareable Core runtime. The
    thread-count contract and failure cleanup match [runtime_create]. *)
let shared_runtime_create ?worker_threads () =
  bridge_call "shared_runtime_create" (fun () ->
      match validate_runtime_worker_threads worker_threads with
      | Error _ as error -> error
      | Ok () -> (
          match check_abi_version abi_version with
          | Error _ as error -> error
          | Ok () -> (
              let shared, response =
                shared_runtime_create_raw (Option.value worker_threads ~default:0)
              in
              match decode response with
              | Ok _ -> Ok shared
              | Error error ->
                  ignore (shared_runtime_close shared);
                  Error error)))
  |> report_shared_lifecycle ~operation:"shared_runtime_create"
       ~ok_message:"shared runtime initialized"
       ~error_message:"shared runtime initialization failed"

(** Creates one graph on [shared]'s Core. A failed attach closes the
    (necessarily empty) graph owner before returning. *)
let runtime_attach shared =
  bridge_call "runtime_attach" (fun () ->
      let runtime, response = runtime_attach_raw shared in
      match decode response with
      | Ok _ -> Ok runtime
      | Error error ->
          ignore (runtime_close runtime);
          Error error)
  |> report_shared_lifecycle ~operation:"runtime_attach"
       ~ok_message:"runtime attached to shared runtime"
       ~error_message:"runtime attach failed"
