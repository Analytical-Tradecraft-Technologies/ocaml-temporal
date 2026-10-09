(** Implements the public worker over the private semantic backend. *)

module Bridge = Temporal_sdk_kernel.Bridge

(** Heterogeneous workflow registration package. *)
type registered_workflow =
  | Workflow :
      ('input, 'output) Workflow.t * Signal.Handler.t list * Query.Handler.t list
      * Update.Handler.t list ->
      registered_workflow

(** Immutable validated settings for one worker construction. Keeping this
    record private means the public API cannot construct an invalid legacy
    build ID or cache bound that would fail later during native startup. *)
module Options = struct
  type versioning =
    | No_versioning
    | Legacy_build_id of string
    | Deployment_based of {
        deployment_name : string;
        build_id : string;
        use_worker_versioning : bool;
        default_versioning_behavior : [ `Auto_upgrade | `Pinned ] option;
      }

  type activation_deadline = [ `After of Duration.t | `Disabled ]

  type workflow_task_pollers =
    | Fixed of int
    | Autoscaling of { minimum : int; maximum : int; initial : int }

  (* Resource settings (#498) are [None] when the caller left them unset, so
     the native layer can omit them from the bridge document and keep the
     exact pre-#498 encoding for a default worker. The accessors below
     report the effective value, filling in each documented default. *)
  type t = {
    versioning : versioning;
    max_cached_workflows : int option;
    workflow_activation_deadline : activation_deadline;
    max_concurrent_workflow_tasks : int option;
    workflow_task_pollers : workflow_task_pollers option;
    sticky_queue_schedule_to_start_timeout : Duration.t option;
    graceful_shutdown_period : Duration.t option;
    max_heartbeat_throttle_interval : Duration.t option;
    default_heartbeat_throttle_interval : Duration.t option;
    max_worker_activities_per_second : float option;
    max_task_queue_activities_per_second : float option;
  }

  (** Two seconds matches the Python SDK's deadlock-detection timeout and is
      above Go's one-second default. An activation is one workflow task's
      worth of work (Core splits a replay into one activation per historical
      task), so legitimate activations finish far sooner. *)
  let default_workflow_activation_deadline = `After (Duration.of_ms 2_000L)

  (** Sticky-cache bound used when [max_cached_workflows] is omitted; the
      private native worker applies the same value. *)
  let default_max_cached_workflows = 1_000

  (** Workflow-task admission limit used when the option is omitted. *)
  let default_max_concurrent_workflow_tasks = 1_000

  (** Workflow poller behavior used when the option is omitted: the minimum
      Core accepts for a caching worker. *)
  let default_workflow_task_pollers = Fixed 2

  (** Temporal Core's default sticky-queue schedule-to-start timeout,
      restated so the effective configuration is inspectable. *)
  let default_sticky_queue_schedule_to_start_timeout = Duration.of_ms 10_000L

  (** Grace period the native worker has always passed to Core. *)
  let default_graceful_shutdown_period = Duration.of_ms 30_000L

  (** Temporal Core's default longest heartbeat throttle interval. *)
  let default_max_heartbeat_throttle_interval = Duration.of_ms 60_000L

  (** Temporal Core's default heartbeat throttle interval for activities
      without a heartbeat timeout. *)
  let default_default_heartbeat_throttle_interval = Duration.of_ms 30_000L

  let default =
    {
      versioning = No_versioning;
      max_cached_workflows = None;
      workflow_activation_deadline = default_workflow_activation_deadline;
      max_concurrent_workflow_tasks = None;
      workflow_task_pollers = None;
      sticky_queue_schedule_to_start_timeout = None;
      graceful_shutdown_period = None;
      max_heartbeat_throttle_interval = None;
      default_heartbeat_throttle_interval = None;
      max_worker_activities_per_second = None;
      max_task_queue_activities_per_second = None;
    }

  (** Largest accepted deadline: one hour. A longer stall is indistinguishable
      from a hung process for any practical liveness probe, and the bound keeps
      the millisecond value well inside a native [int]. *)
  let max_workflow_activation_deadline_ms = 3_600_000L

  (** Largest accepted count for any worker resource setting, mirroring the
      private bridge's allocation guard. *)
  let max_count = 1_000_000

  (** Largest accepted resource duration: one day, mirroring the bridge. A
      longer sticky timeout, heartbeat throttle or grace period is far more
      likely to be a unit mistake than a deliberate policy. *)
  let max_resource_duration_ms = 86_400_000L

  (** Rejects a zero or over-long watchdog deadline. *)
  let validate_activation_deadline = function
    | `Disabled -> Ok ()
    | `After duration ->
        let milliseconds = Duration.to_ms duration in
        if Int64.compare milliseconds 0L <= 0 then
          Error
            (Error.defect
               ~message:
                 "workflow_activation_deadline must be positive; use \
                  `Disabled to turn the watchdog off")
        else if
          Int64.compare milliseconds max_workflow_activation_deadline_ms > 0
        then
          Error
            (Error.defect
               ~message:"workflow_activation_deadline exceeds one hour")
        else Ok ()

  (** Checks the bridge's transport-level identifier invariants before an
      option value can be retained by a caller. *)
  let validate_build_id value =
    if String.equal value "" then
      Error (Error.defect ~message:"build_id must not be empty")
    else if String.contains value '\000' then
      Error (Error.defect ~message:"build_id must not contain NUL")
    else if String.length value > 65_536 then
      Error
        (Error.defect
           ~message:"build_id exceeds 65536 UTF-8 bytes")
    else Ok ()

  (** Validates the optional sticky-cache override using the same bounded
      resource policy enforced again by the private native bridge. *)
  let validate_cache = function
    | None -> Ok ()
    | Some value when value >= 0 && value <= 1_000_000 -> Ok ()
    | Some _ ->
        Error
          (Error.defect
             ~message:
               "max_cached_workflows must be between 0 and 1000000")

  (** Rejects a count below one or above [max_count]. *)
  let validate_count field value =
    if value >= 1 && value <= max_count then Ok ()
    else
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "%s must be between 1 and %d" field max_count))

  (** Checks the workflow-task resource settings against each other and
      against Core's rule that a caching worker needs at least two workflow
      task slots and two workflow pollers: one for its sticky queue and one
      for the normal queue, so a busy sticky queue cannot starve discovery of
      new workflows. *)
  let validate_workflow_resources ~max_cached_workflows
      ~max_concurrent_workflow_tasks ~workflow_task_pollers =
    let ( let* ) = Result.bind in
    let caching =
      Option.value max_cached_workflows ~default:default_max_cached_workflows
      > 0
    in
    let* () =
      match max_concurrent_workflow_tasks with
      | None -> Ok ()
      | Some value ->
          let* () = validate_count "max_concurrent_workflow_tasks" value in
          if caching && value < 2 then
            Error
              (Error.defect
                 ~message:
                   "max_concurrent_workflow_tasks must be at least 2 when \
                    max_cached_workflows is greater than zero")
          else Ok ()
    in
    match workflow_task_pollers with
    | None -> Ok ()
    | Some (Fixed count) ->
        let* () = validate_count "workflow_task_pollers" count in
        if caching && count < 2 then
          Error
            (Error.defect
               ~message:
                 "workflow_task_pollers must be at least 2 when \
                  max_cached_workflows is greater than zero")
        else Ok ()
    | Some (Autoscaling { minimum; maximum; initial }) ->
        let* () = validate_count "workflow_task_pollers minimum" minimum in
        let* () = validate_count "workflow_task_pollers maximum" maximum in
        if maximum < minimum then
          Error
            (Error.defect
               ~message:"workflow_task_pollers maximum must be at least minimum")
        else if initial < minimum || initial > maximum then
          Error
            (Error.defect
               ~message:
                 "workflow_task_pollers initial must be between minimum and \
                  maximum")
        else if caching && maximum < 2 then
          Error
            (Error.defect
               ~message:
                 "workflow_task_pollers maximum must be at least 2 when \
                  max_cached_workflows is greater than zero")
        else Ok ()

  (** Rejects an optional duration outside [minimum_ms] to one day. *)
  let validate_duration ~minimum_ms field = function
    | None -> Ok ()
    | Some duration ->
        let milliseconds = Duration.to_ms duration in
        if
          Int64.compare milliseconds minimum_ms >= 0
          && Int64.compare milliseconds max_resource_duration_ms <= 0
        then Ok ()
        else
          Error
            (Error.defect
               ~message:
                 (Printf.sprintf "%s must be between %Ld ms and one day" field
                    minimum_ms))

  (** Rejects a rate that Core or the server would refuse or misread: zero,
      negative, NaN, infinite or subnormal. *)
  let validate_rate field = function
    | None -> Ok ()
    | Some value when Float.classify_float value = FP_normal && value > 0.0 ->
        Ok ()
    | Some _ ->
        Error (Error.defect ~message:(field ^ " must be a positive finite number"))

  (** Validates every timing and rate setting. Core clips the default
      heartbeat throttle interval to the maximum, so an explicit default above
      an explicit maximum is a contradiction reported rather than hidden. *)
  let validate_timing ~sticky_queue_schedule_to_start_timeout
      ~graceful_shutdown_period ~max_heartbeat_throttle_interval
      ~default_heartbeat_throttle_interval ~max_worker_activities_per_second
      ~max_task_queue_activities_per_second =
    let ( let* ) = Result.bind in
    let* () =
      validate_duration ~minimum_ms:1L "sticky_queue_schedule_to_start_timeout"
        sticky_queue_schedule_to_start_timeout
    in
    let* () =
      validate_duration ~minimum_ms:0L "graceful_shutdown_period"
        graceful_shutdown_period
    in
    let* () =
      validate_duration ~minimum_ms:1L "max_heartbeat_throttle_interval"
        max_heartbeat_throttle_interval
    in
    let* () =
      validate_duration ~minimum_ms:1L "default_heartbeat_throttle_interval"
        default_heartbeat_throttle_interval
    in
    let* () =
      match
        (default_heartbeat_throttle_interval, max_heartbeat_throttle_interval)
      with
      | Some default, Some maximum
        when Int64.compare (Duration.to_ms default) (Duration.to_ms maximum) > 0
        ->
          Error
            (Error.defect
               ~message:
                 "default_heartbeat_throttle_interval must not exceed \
                  max_heartbeat_throttle_interval")
      | _ -> Ok ()
    in
    let* () =
      validate_rate "max_worker_activities_per_second"
        max_worker_activities_per_second
    in
    validate_rate "max_task_queue_activities_per_second"
      max_task_queue_activities_per_second

  (** Validates the routing mode's identifiers and its behavior pairing. *)
  let validate_versioning = function
    | No_versioning -> Ok ()
    | Legacy_build_id build_id -> validate_build_id build_id
    | Deployment_based
        {
          deployment_name;
          build_id;
          use_worker_versioning;
          default_versioning_behavior;
        } -> (
        let validate_name field value =
          if String.equal value "" then
            Error (Error.defect ~message:(field ^ " must not be empty"))
          else if String.contains value '\000' then
            Error (Error.defect ~message:(field ^ " must not contain NUL"))
          else if String.length value > 65_536 then
            Error (Error.defect ~message:(field ^ " exceeds 65536 bytes"))
          else Ok ()
        in
        match validate_name "deployment_name" deployment_name with
        | Error _ as error -> error
        | Ok () -> (
            match validate_build_id build_id with
            | Error _ as error -> error
            | Ok () -> (
                (* The SDK has no per-workflow behavior API, so every
                   completion leaves the behavior unspecified and Core
                   substitutes only a configured worker default. A
                   versioned worker therefore needs a default; otherwise
                   every completion would declare the workflow unversioned
                   (issue #817). *)
                match (use_worker_versioning, default_versioning_behavior) with
                | true, Some _ | false, None -> Ok ()
                | true, None ->
                    Error
                      (Error.defect
                         ~message:
                           "use_worker_versioning requires \
                            default_versioning_behavior")
                | false, Some _ ->
                    Error
                      (Error.defect
                         ~message:
                           "default_versioning_behavior requires \
                            use_worker_versioning"))))

  (** Builds an immutable option value after validating every user-supplied
      field. Rust repeats these checks because JSON is an independent trust
      boundary, not because callers should normally see duplicate failures. *)
  let make ?(versioning = No_versioning) ?max_cached_workflows
      ?(workflow_activation_deadline = default_workflow_activation_deadline)
      ?max_concurrent_workflow_tasks ?workflow_task_pollers
      ?sticky_queue_schedule_to_start_timeout ?graceful_shutdown_period
      ?max_heartbeat_throttle_interval ?default_heartbeat_throttle_interval
      ?max_worker_activities_per_second ?max_task_queue_activities_per_second
      () =
    let ( let* ) = Result.bind in
    let* () = validate_versioning versioning in
    let* () = validate_cache max_cached_workflows in
    let* () = validate_activation_deadline workflow_activation_deadline in
    let* () =
      validate_workflow_resources ~max_cached_workflows
        ~max_concurrent_workflow_tasks ~workflow_task_pollers
    in
    let* () =
      validate_timing ~sticky_queue_schedule_to_start_timeout
        ~graceful_shutdown_period ~max_heartbeat_throttle_interval
        ~default_heartbeat_throttle_interval ~max_worker_activities_per_second
        ~max_task_queue_activities_per_second
    in
    Ok
      {
        versioning;
        max_cached_workflows;
        workflow_activation_deadline;
        max_concurrent_workflow_tasks;
        workflow_task_pollers;
        sticky_queue_schedule_to_start_timeout;
        graceful_shutdown_period;
        max_heartbeat_throttle_interval;
        default_heartbeat_throttle_interval;
        max_worker_activities_per_second;
        max_task_queue_activities_per_second;
      }

  let versioning options = options.versioning
  let max_cached_workflows options = options.max_cached_workflows

  let workflow_activation_deadline options =
    options.workflow_activation_deadline

  let max_concurrent_workflow_tasks options =
    Option.value options.max_concurrent_workflow_tasks
      ~default:default_max_concurrent_workflow_tasks

  let workflow_task_pollers options =
    Option.value options.workflow_task_pollers
      ~default:default_workflow_task_pollers

  let sticky_queue_schedule_to_start_timeout options =
    Option.value options.sticky_queue_schedule_to_start_timeout
      ~default:default_sticky_queue_schedule_to_start_timeout

  let graceful_shutdown_period options =
    Option.value options.graceful_shutdown_period
      ~default:default_graceful_shutdown_period

  let max_heartbeat_throttle_interval options =
    Option.value options.max_heartbeat_throttle_interval
      ~default:default_max_heartbeat_throttle_interval

  (* Core applies [min default maximum], so report that effective value. *)
  let default_heartbeat_throttle_interval options =
    let default =
      Option.value options.default_heartbeat_throttle_interval
        ~default:default_default_heartbeat_throttle_interval
    in
    let maximum = max_heartbeat_throttle_interval options in
    if Int64.compare (Duration.to_ms default) (Duration.to_ms maximum) > 0 then
      maximum
    else default

  let max_worker_activities_per_second options =
    options.max_worker_activities_per_second

  let max_task_queue_activities_per_second options =
    options.max_task_queue_activities_per_second

  (** Converts the explicit resource settings into the private bridge's
      tuning record. Unset settings stay [None] so the bridge omits them. *)
  let native_tuning options : Temporal_sdk_kernel.Bridge.worker_tuning =
    let milliseconds = Option.map Duration.to_ms in
    {
      workflow_task_poller_autoscaling =
        (match options.workflow_task_pollers with
        | Some (Autoscaling { minimum; maximum; initial }) ->
            Some { minimum; maximum; initial }
        | Some (Fixed _) | None -> None);
      sticky_queue_schedule_to_start_timeout_ms =
        milliseconds options.sticky_queue_schedule_to_start_timeout;
      max_heartbeat_throttle_interval_ms =
        milliseconds options.max_heartbeat_throttle_interval;
      default_heartbeat_throttle_interval_ms =
        milliseconds options.default_heartbeat_throttle_interval;
      max_worker_activities_per_second =
        options.max_worker_activities_per_second;
      max_task_queue_activities_per_second =
        options.max_task_queue_activities_per_second;
    }

  (** The workflow poller maximum Core receives: the fixed count, or the
      autoscaling maximum that the bridge requires to match it. *)
  let native_workflow_task_polls options =
    match workflow_task_pollers options with
    | Fixed count -> count
    | Autoscaling { maximum; _ } -> maximum
end

(** Worker liveness as observed by the workflow activation watchdog. *)
module Health = struct
  type abandonment =
    [ `Task_failed | `Queries_failed | `Eviction_acknowledged | `Not_acknowledged ]

  type stuck_workflow_activation = {
    workflow_type : string option;
    workflow_id : string option;
    run_id : string;
    is_replaying : bool;
    elapsed : Duration.t;
    abandoned : abandonment;
  }

  type t = Healthy | Stuck_workflow_activation of stuck_workflow_activation
end

(** Heterogeneous activity registration package. *)
type registered_activity =
  | Activity : ('input, 'output) Activity.t -> registered_activity

(** Packs a workflow definition for the heterogeneous registration list. *)
let workflow ?(signals = []) ?(queries = []) ?(updates = []) definition =
  Workflow (definition, signals, queries, updates)

(** Packs an activity definition for the heterogeneous registration list. *)
let activity definition = Activity definition

(** A local workflow entry keeps its definition and implementation together so
    decoding and execution cannot accidentally use different codecs. *)
type workflow_entry =
  | Workflow_entry : {
      (* The definition supplies the registered name and the codecs used at
         the backend boundary. *)
      definition : ('input, 'output) Workflow.t;
      (* Signal handlers remain attached to this definition through native
         registration, preventing an accidental cross-workflow association. *)
      signals : Signal.Handler.t list;
      (* Query handlers are synchronous and read-only; they are registered
         next to the workflow so native dispatch cannot cross definitions. *)
      queries : Query.Handler.t list;
      (* Update handlers are run synchronously on the owner Domain in the
         current native slice; their callbacks remain paired with codecs. *)
      updates : Update.Handler.t list;
      (* This callback has the same input and output types as [definition], so
         the existential package cannot pair a function with another codec. *)
      implementation : ('input, 'output) Workflow.implementation;
    }
      -> workflow_entry

(** A local activity entry keeps the definition and whichever typed callback
    was registered together. The two optional fields preserve the distinction
    between ordinary and context-aware activity APIs for dispatch. *)
type activity_entry =
  | Activity_entry : {
      (* The definition supplies the stable activity name and payload codecs. *)
      definition : ('input, 'output) Activity.t;
      (* A plain callback is present when the activity does not need runtime
         context such as heartbeat metadata. *)
      implementation : ('input, 'output) Activity.implementation option;
      (* A context-aware callback is retained separately so dispatch can build
         the appropriate context without changing the public callback type. *)
      contextual_implementation :
        ('input, 'output) Activity.contextual_implementation option;
      (* Deferred callback retained separately so the native adapter can
         create its completion capability only after Core accepts handoff. *)
      async_implementation :
        ('input, 'output) Activity.async_implementation option;
    }
      -> activity_entry

(** String keys give stable registration and lookup order without relying on
    hash-table iteration, which matters when the backend config is serialized. *)
module Name_map = Map.Make (String)

(** A worker owns either the deterministic mock backend or the real native
    adapters. The choice is made once at construction and cannot change while
    polling, which keeps lifecycle ownership explicit. *)
type backend =
  (* Deterministic in-memory task streams used by unit tests and examples. *)
  | Mock_backend of Backend.worker
  (* OCaml-owned native worker adapters backed by the Rust/Core bridge. *)
  | Native_backend of Native_worker.t

(** A worker owns one backend and immutable registries after construction. *)
type t = {
  (* The backend is the sole owner of the native/mock handle graph; lifecycle
     operations reach it through [run] and [shutdown]. *)
  backend : backend;
  (* Workflow definitions are keyed by their stable names; the map is never
     mutated after construction, which keeps dispatch independent of callers. *)
  workflows : workflow_entry Name_map.t;
  (* Activity definitions follow the same immutable name-based lookup rule. *)
  activities : activity_entry Name_map.t;
  (* The validated options this worker was created with, retained only so
     [options] can report the effective configuration (#498). *)
  options : Options.t;
  (* This atomic gate records shutdown admission without holding a lock while
     backend polling blocks, allowing repeated shutdown calls to be harmless. *)
  closed : bool Atomic.t;
  (* Sticky, non-blocking stop request posted by [request_shutdown] (#830).
     Unlike [closed] it does not admit teardown; it only makes [run] return so
     the caller can then run [shutdown]. Written only by one [Atomic.set], so it
     is safe from a signal handler on any Domain. *)
  stop_requested : bool Atomic.t;
  (* Serializes the first teardown with later callers that need the cached
     result, matching [Client.shutdown]. *)
  shutdown_mutex : Mutex.t;
  (* The first terminal shutdown outcome is retained so every caller observes
     the same result, including a permanent native teardown error. *)
  mutable shutdown_result : (unit, Error.t) result option;
}

(** Rejects empty or NUL-containing worker settings before backend allocation. *)
let validate_name field value =
  if String.equal value "" then
    Error (Error.defect ~message:(field ^ " must not be empty"))
  else if String.contains value '\000' then
    Error (Error.defect ~message:(field ^ " must not contain NUL"))
  else Ok ()

(** Adds a workflow to the registry while rejecting duplicate names and remote
    references that do not contain executable OCaml code. *)
let add_workflow registry (Workflow (definition, signals, queries, updates)) =
  let name = Workflow.name definition in
  match Workflow.implementation definition with
  | None ->
      Error
        (Error.defect
           ~message:("workflow " ^ name ^ " has no local implementation"))
  | Some implementation ->
      if Name_map.mem name registry then
        Error
          (Error.defect ~message:("duplicate workflow registration: " ^ name))
      else
        Result.map
          (fun () ->
            Name_map.add name
              (Workflow_entry { definition; signals; queries; updates; implementation })
              registry)
          (Interaction.create ~signals ~queries ~updates ()
          |> Result.map (fun _ -> ()))

(** Adds an activity to the registry with the same duplicate and implementation
    checks used for workflows. *)
let add_activity registry (Activity definition) =
  let name = Activity.name definition in
  match
    ( Activity.implementation definition,
      Activity.implementation_with_context definition,
      Activity.implementation_async definition )
  with
  | None, None, None ->
      Error
        (Error.defect
           ~message:("activity " ^ name ^ " has no local implementation"))
  | Some _, Some _, _ | Some _, _, Some _ | _, Some _, Some _ ->
      Error
        (Error.defect
           ~message:
             ("activity " ^ name
             ^ " must choose exactly one implementation mode"))
  | implementation, contextual_implementation, async_implementation ->
      if Name_map.mem name registry then
        Error
          (Error.defect ~message:("duplicate activity registration: " ^ name))
      else
        Ok
          (Name_map.add name
             (Activity_entry
                {
                  definition;
                  implementation;
                  contextual_implementation;
                  async_implementation;
                })
             registry)

(** Builds a workflow registry before opening any backend resource. *)
let collect_workflows definitions =
  List.fold_left
    (fun result definition ->
      Result.bind result (fun registry -> add_workflow registry definition))
    (Ok Name_map.empty) definitions

(** Builds an activity registry before opening any backend resource. *)
let collect_activities definitions =
  List.fold_left
    (fun result definition ->
      Result.bind result (fun registry -> add_activity registry definition))
    (Ok Name_map.empty) definitions

(** Creates the private backend only after all local registration invariants are
    proven. This ordering prevents leaked graphs on invalid definitions. *)
let resolve_options options max_cached_workflows =
  match (options, max_cached_workflows) with
  | Some _, Some _ ->
      Error
        (Error.defect
           ~message:
             "Worker.create accepts either ~options or ~max_cached_workflows, not both")
  | Some options, None -> Ok options
  | None, Some max_cached_workflows ->
      Options.make ~max_cached_workflows ()
  | None, None -> Ok Options.default

let create ?identity ?options ?max_cached_workflows ?io_threads ?runtime
    ~target_url
    ~namespace ~task_queue ~workflows ~activities () =
  match Backend.validate_io_threads io_threads with
  | Error error -> Error error
  | Ok () ->
  match Backend.validate_runtime_source ~io_threads ~runtime with
  | Error error -> Error error
  | Ok () ->
  match resolve_options options max_cached_workflows with
  | Error error -> Error error
  | Ok options ->
    let effective_max_cached_workflows =
      Options.max_cached_workflows options
    in
    let native_versioning =
      match Options.versioning options with
      | Options.No_versioning -> Bridge.No_versioning
      | Options.Legacy_build_id build_id -> Bridge.Legacy_build_id build_id
      | Options.Deployment_based
          {
            deployment_name;
            build_id;
            use_worker_versioning;
            default_versioning_behavior;
          } ->
          Bridge.Deployment_based
            {
              deployment_name;
              build_id;
              use_worker_versioning;
              default_versioning_behavior =
                Option.map
                  (function
                    | `Auto_upgrade -> Bridge.Auto_upgrade
                    | `Pinned -> Bridge.Pinned)
                  default_versioning_behavior;
            }
    in
  (* An omitted identity is derived once per worker as [<pid>@<hostname>] so
     pollers from different processes are distinguishable in Temporal. This
     is worker construction, not workflow code, so process state is safe. *)
  let identity = Temporal_base.Process_identity.resolve identity in
  match validate_name "namespace" namespace with
  | Error error -> Error error
  | Ok () -> (
      match validate_name "task queue" task_queue with
      | Error error -> Error error
      | Ok () -> (
          match validate_name "identity" identity with
          | Error error -> Error error
          | Ok () -> (
              match collect_workflows workflows with
              | Error error -> Error error
              | Ok workflows -> (
                  match collect_activities activities with
                  | Error error -> Error error
                  | Ok activities ->
                      let config : Backend.config =
                        { target_url; namespace; identity; task_queue = Some task_queue }
                      in
                      let workflow_names =
                        Name_map.bindings workflows |> List.map fst
                      in
                      let activity_names =
                        Name_map.bindings activities |> List.map fst
                      in
                      let async_activity =
                        Name_map.bindings activities
                        |> List.find_map
                             (fun (name, Activity_entry { async_implementation; _ }) ->
                               Option.map (fun _ -> name) async_implementation)
                      in
                      (* The mock backend has no Temporal task token to retain
                         for a later asynchronous completion, so reject such a
                         registration at construction rather than failing each
                         task during [run]. *)
                      if String.starts_with ~prefix:"mock://" target_url
                         && Option.is_some async_activity
                      then
                        Error
                          (Error.defect
                             ~message:
                               ("asynchronous activity "
                               ^ Option.get async_activity
                               ^ " requires the native worker backend"))
                      else if String.starts_with ~prefix:"mock://" target_url then
                        Result.map
                          (fun backend ->
                            {
                              backend = Mock_backend backend;
                              workflows;
                              activities;
                              options;
                              closed = Atomic.make false;
                              stop_requested = Atomic.make false;
                              shutdown_mutex = Mutex.create ();
                              shutdown_result = None;
                            })
                          (Backend.worker_create ?runtime config
                             ~workflow_names ~activity_names)
                      else
                        let native_workflows =
                          Name_map.bindings workflows
                          |> List.map (fun (_, Workflow_entry { definition; signals; queries; updates; _ }) ->
                                 Native_worker.register_workflow
                                   ~signals
                                   ~queries
                                   ~updates
                                   (Workflow_private.to_base definition))
                        in
                        let native_activities =
                          Name_map.bindings activities
                          |> List.map
                               (fun
                                 (_,
                                  Activity_entry
                                    { definition; async_implementation; _ }) ->
                                 match async_implementation with
                                 | Some _ ->
                                     Native_worker.register_async_activity
                                       (Activity_private.to_base_async definition)
                                 | None ->
                                     Native_worker.register_activity
                                       (Activity_private.to_base definition))
                        in
                        let activation_deadline_ms =
                          match Options.workflow_activation_deadline options with
                          | `Disabled -> None
                          | `After duration ->
                              (* Validated to at most one hour by [Options]. *)
                              Some (Int64.to_int (Duration.to_ms duration))
                        in
                        let native_result =
                          Native_worker.create
                            ?max_cached_workflows:effective_max_cached_workflows
                            ~max_outstanding_workflow_tasks:
                              (Options.max_concurrent_workflow_tasks options)
                            ~max_concurrent_workflow_task_polls:
                              (Options.native_workflow_task_polls options)
                            ~graceful_shutdown_timeout_ms:
                              (Duration.to_ms
                                 (Options.graceful_shutdown_period options))
                            ~tuning:(Options.native_tuning options)
                            ?io_threads ?runtime ?activation_deadline_ms
                            ~versioning:native_versioning ~target_url
                            ~namespace ~identity
                            ~task_queue ~workflows:native_workflows
                            ~activities:native_activities ()
                          |> Result.map_error Error_private.of_base
                        in
                        Result.map
                          (fun backend ->
                            {
                              backend = Native_backend backend;
                              workflows;
                              activities;
                              options;
                              closed = Atomic.make false;
                              stop_requested = Atomic.make false;
                              shutdown_mutex = Mutex.create ();
                              shutdown_result = None;
                            })
                          native_result))))

(** Converts an implementation exception into a structured defect rather than
    letting a user callback tear down the worker poll loop. *)
let protect_implementation operation implementation input =
  match implementation input with
  | result -> result
  | exception exn ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "%s implementation raised: %s" operation
                (Printexc.to_string exn)))

(** Dispatches one workflow task through its typed codec and implementation. *)
let dispatch_workflow worker task =
  match Name_map.find_opt task.Backend.workflow_name worker.workflows with
  | None ->
      Error
        (Error.make ~category:`Workflow
           ~message:("unregistered workflow task: " ^ task.workflow_name) ())
  | Some (Workflow_entry { definition; implementation; _ }) -> (
      match
        Codec.decode (Workflow.input definition) task.input
      with
      | Error error -> Error error
      | Ok input -> (
          match protect_implementation "workflow" implementation input with
          | Error error -> Error error
          | Ok output -> Codec.encode (Workflow.output definition) output))

(** Dispatches one activity task through its typed codec and implementation. *)
let dispatch_activity worker task =
  match Name_map.find_opt task.Backend.activity_name worker.activities with
  | None ->
      Error
        (Error.make ~category:`Activity
           ~message:("unregistered activity task: " ^ task.activity_name) ())
  | Some
      (Activity_entry
        {
          definition;
          implementation;
          contextual_implementation;
          async_implementation;
        }) -> (
      match
        Codec.decode (Activity.input definition) task.input
      with
      | Error error -> Error error
      | Ok input -> (
          let result =
            match async_implementation with
            | Some _ ->
                (* Unreachable through [create], which rejects asynchronous
                   registrations for the mock backend; kept so dispatch never
                   invokes a callback without a native completion lease. *)
                Error
                  (Error.make ~non_retryable:true ~category:`Activity
                     ~message:
                       "asynchronous activities require the native worker backend"
                     ())
            | None -> (
                match contextual_implementation with
                | Some implementation ->
                (* Mock tasks have no native activity lease, so the callback
                   receives an explicit unavailable context instead of a
                   fabricated heartbeat capability. *)
                let context =
                  Temporal_base.Activity_context.unavailable ~details:[]
                    ~heartbeat_timeout:None
                in
                protect_implementation "activity" (implementation context)
                  input
                | None -> (
                    match implementation with
                    | Some implementation ->
                        protect_implementation "activity" implementation input
                    | None ->
                        Error
                          (Error.defect
                             ~message:
                               "activity registry entry has no implementation")))
          in
          match result with
          | Error error -> Error error
          | Ok output -> Codec.encode (Activity.output definition) output))

(** Completes a workflow task even when dispatch produced a typed failure. The
    backend receives the failure so Core cannot be left waiting for a response;
    once that acknowledgement succeeds, the worker keeps polling because a
    failed workflow task is not a worker-level transport failure. *)
let complete_workflow worker backend task =
  match dispatch_workflow worker task with
  | Ok output ->
      Result.map
        (fun () -> ())
        (Backend.worker_complete_workflow backend
           (Backend.Workflow_completed { task_token = task.task_token; output }))
  | Error error ->
      Result.bind
        (Backend.worker_complete_workflow backend
           (Backend.Workflow_failed
              { task_token = task.task_token; error }))
        (fun () -> Ok ())

(** Completes an activity task even when local decoding or execution failed.
    Activity failures are ordinary Temporal outcomes; after the backend
    accepts the failure, the worker remains available for later tasks and
    retries. *)
let complete_activity worker backend task =
  match dispatch_activity worker task with
  | Ok output ->
      Result.map
        (fun () -> ())
        (Backend.worker_complete_activity backend
           (Backend.Activity_completed { task_token = task.task_token; output }))
  | Error error ->
      Result.bind
        (Backend.worker_complete_activity backend
           (Backend.Activity_failed
              { task_token = task.task_token; error }))
        (fun () -> Ok ())

(** Polls both streams until each reports shutdown. Core-backed adapters may
    block inside their poll calls; this loop never waits on an OCaml lock. *)
let run_mock worker backend =
  if Atomic.get worker.closed then
    Error
      (Error.make ~category:`Bridge ~message:"worker is shut down" ())
  else
    (* Keep independent shutdown state for the workflow and activity streams.
       A ready task is completed before either stream is considered drained,
       so observing shutdown on one stream cannot discard work on the other. *)
    let rec loop workflow_shutdown activity_shutdown =
      if workflow_shutdown && activity_shutdown then Ok ()
      else if Atomic.get worker.stop_requested then
        (* A stop request ends polling between tasks, like the native loop's
           stop check; every task taken so far has already been completed. *)
        Ok ()
      else
        let workflow_result =
          if workflow_shutdown then Ok Backend.Shutdown
          else Backend.worker_poll_workflow backend
        in
        Result.bind workflow_result (function
          | Backend.Task task ->
              Result.bind (complete_workflow worker backend task) (fun () ->
                loop false activity_shutdown)
          | Backend.Idle ->
              let activity_result =
                if activity_shutdown then Ok Backend.Shutdown
                else Backend.worker_poll_activity backend
              in
              Result.bind activity_result (function
                | Backend.Task task ->
                    Result.bind (complete_activity worker backend task) (fun () ->
                        loop workflow_shutdown false)
                | Backend.Idle -> loop workflow_shutdown activity_shutdown
                | Backend.Shutdown -> loop workflow_shutdown true)
          | Backend.Shutdown ->
              let activity_result =
                if activity_shutdown then Ok Backend.Shutdown
                else Backend.worker_poll_activity backend
              in
              Result.bind activity_result (function
                | Backend.Task task ->
                    Result.bind (complete_activity worker backend task) (fun () ->
                        loop true false)
                | Backend.Idle -> loop true activity_shutdown
                | Backend.Shutdown -> loop true true))
    in
    loop false false

(** Runs the selected backend while it is open. The public admission check is
    intentionally before backend dispatch: mock polling and native readiness
    loops both treat an already-closed worker as a clean stop, but a caller
    re-entering [run] after shutdown must receive the same typed lifecycle
    error on either backend. *)
let run worker =
  if Atomic.get worker.closed then
    Error (Error.make ~category:`Bridge ~message:"worker is shut down" ())
  else
    match worker.backend with
    | Mock_backend backend -> run_mock worker backend
    | Native_backend backend ->
        Native_worker.run backend |> Result.map_error Error_private.of_base

(** Returns the immutable options value retained at construction. *)
let options worker = worker.options

(** Reads the sticky watchdog report without taking any lock, so a liveness
    probe can call it while the workflow lane is stuck. The mock backend runs
    no watchdog and is always healthy. *)
let health worker =
  match worker.backend with
  | Mock_backend _ -> Health.Healthy
  | Native_backend backend -> (
      match Native_worker.stuck_activation backend with
      | None -> Health.Healthy
      | Some
          {
            run_id;
            workflow_id;
            workflow_type;
            is_replaying;
            elapsed_ms;
            abandoned;
          } ->
          Health.Stuck_workflow_activation
            {
              workflow_type;
              workflow_id;
              run_id;
              is_replaying;
              elapsed = Duration.of_ms (Int64.of_int (Int.max 0 elapsed_ms));
              abandoned;
            })

(** Asks [run] to return without waiting for it (#830). The function performs
    only atomic writes to cells allocated with the worker: no lock, I/O,
    logging, or supervisor message. That makes it safe to call from an OCaml
    signal handler, which runs at a safe point of whichever thread the runtime
    picks, possibly the run loop's own thread inside a workflow activation. *)
let request_shutdown worker =
  Atomic.set worker.stop_requested true;
  match worker.backend with
  | Native_backend backend -> Native_worker.request_stop backend
  | Mock_backend _ -> ()

(** Shuts down the backend once and remembers that no new poll may be admitted.

    The execution-thread check deliberately precedes [shutdown_mutex] (#764).
    A concurrent external caller may hold that mutex while it waits for the
    native run loop to exit; a workflow or activity callback on that loop which
    then blocked on the same mutex would never return to let the loop exit. The
    check is per system thread rather than per Domain (#763), so a sibling
    thread of the run loop's Domain proceeds and waits like any other caller.
    Callers that pass the check are serialized by the mutex: the first one
    performs teardown and later ones return its cached terminal result. *)
let shutdown worker =
  if
    match worker.backend with
    | Native_backend backend -> Native_worker.is_execution_thread backend
    | Mock_backend _ -> false
  then begin
    (* This thread cannot wait for its own loop, but it can ask that loop to
       return. An OCaml signal handler that runs on the run loop's thread lands
       here too, so posting the request lets a natural SIGTERM handler stop the
       worker (#830); the caller completes teardown after [run] returns. *)
    request_shutdown worker;
    Error
      (Error.defect
         ~message:
           "cannot shut down a worker from inside its own workflow or activity \
            execution thread; a stop was requested instead, so call shutdown \
            again after run returns")
  end
  else begin
  Mutex.lock worker.shutdown_mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock worker.shutdown_mutex)
    (fun () ->
      match worker.shutdown_result with
      | Some result -> result
      | None ->
          Atomic.set worker.closed true;
          let result =
            match worker.backend with
            | Mock_backend backend -> (
                match Backend.worker_shutdown backend with
                | Ok () as result -> result
                | Error _ as error ->
                    (* The mock backend can retry a failed shutdown admission. *)
                    Atomic.set worker.closed false;
                    error)
            | Native_backend backend -> (
                let result =
                  Native_worker.shutdown backend
                  |> Result.map_error Error_private.of_base
                in
                match result with
                | Ok () as result -> result
                | Error _ as error ->
                    (* Native adapter-drain failures are retryable only when the
                       private supervisor explicitly says teardown did not begin. *)
                    if Native_worker.shutdown_retryable backend then
                      Atomic.set worker.closed false;
                    error)
          in
          (* Cache only terminal outcomes. Retryable failures leave the worker
             open so a later call can attempt shutdown again. *)
          if Atomic.get worker.closed then worker.shutdown_result <- Some result;
          result)
  end
