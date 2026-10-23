(** Deterministic in-process workflow test engine. See the interface for the
    simulated semantics; the comments below explain how each piece of server
    behavior is approximated. *)

module Error = Temporal_base.Error
module Codec = Temporal_base.Codec
module Payload = Temporal_base.Payload
module Definition = Temporal_base.Definition
module Activity_context = Temporal_base.Activity_context
module Protocol = Temporal_protocol.Workflow_protocol

(** Private registration package. The existential keeps the decoder used for
    the start input paired with the implementation that receives it. *)
type workflow =
  | Workflow : {
      override : bool;
      definition :
        ('input, 'output, 'input -> ('output, Error.t) result) Definition.t;
      signal_handlers : Execution.signal_handler list;
      query_handlers : Execution.query_handler list;
      update_handlers : Execution.update_handler list;
    }
      -> workflow

(** What the engine runs for one activity type name. *)
type activity_code =
  (* Executable code; the existential pairs the codecs with the callback. *)
  | Code :
      ( 'input,
        'output,
        Activity_context.t -> 'input -> ('output, Error.t) result )
      Definition.t
      -> activity_code
  (* Every attempt fails with this fixed, non-retryable diagnostic. *)
  | Unsupported of string

(** A named activity registration and whether it replaces an earlier one. *)
type activity = { name : string; override : bool; code : activity_code }

let workflow ?(override = false) ?(signal_handlers = []) ?(query_handlers = [])
    ?(update_handlers = []) definition =
  Workflow
    { override; definition; signal_handlers; query_handlers; update_handlers }

let activity ?(override = false) definition =
  { name = Definition.name definition; override; code = Code definition }

let unsupported_activity ?(override = false) ~name ~reason () =
  { name; override; code = Unsupported reason }

(** Hides the input and output types of a live execution so runs of different
    workflow types share one registry. Only payload-level operations are
    needed after construction. *)
type execution = Execution : ('input, 'output) Execution.t -> execution

(** The latest phase recorded for one update protocol instance. *)
type update_state =
  | Update_accepted
  | Update_completed of Codec.payload
  | Update_rejected of Error.t

(** The activity command fields the engine needs to run, retry, and cancel
    attempts. [heartbeat_details] carries the last details recorded by an
    attempt into the next one, matching Temporal's heartbeat-details
    hand-off. [scheduled_ms] is the virtual time of the first attempt. *)
type activity_request = {
  activity_id : string;
  activity_type : string;
  arguments : Codec.payload list;
  retry_policy : Activation.retry_policy option;
  cancellation_type : Activation.activity_cancellation_type;
  is_local : bool;
  schedule_to_close_timeout : int64 option;
  start_to_close_timeout : int64 option;
  heartbeat_timeout : int64 option;
  scheduled_ms : int64;
  mutable heartbeat_details : Payload.t list;
}

(** A workflow ID's sequence of runs. [current] is [None] only while the
    first run is being constructed. [outcome] is set exactly once, when the
    last run closes; continue-as-new and a retry replace [current] without
    setting it. [parent] identifies the parent run and the child command
    sequence to resolve when the chain closes. [retry_policy] is the child
    command's workflow retry policy, which the server applies to every run
    of the chain; top-level starts have none. [task_queue] is the queue
    reported by [Workflow.info] for every run of the chain. *)
type chain = {
  workflow_id : string;
  mutable current : run option;
  mutable first_run_id : string;
  mutable outcome : (Codec.payload, Error.t) result option;
  parent : (run * int64) option;
  parent_close_policy : Activation.child_workflow_parent_close_policy option;
  retry_policy : Activation.retry_policy option;
  task_queue : string;
}

(** One workflow run and the engine-side state of its outstanding commands.
    [jobs] holds activation jobs not yet delivered, newest first; [queued]
    records membership in the environment's ready queue so a run is queued
    at most once. A closed run keeps its execution so it stays queryable.
    [attempt] and [input] are the run's retry attempt (1 for a first run or
    a continue-as-new successor) and encoded start input, kept so a failed
    run can be retried with the same input. A retry run waits out its
    backoff before its first activation: until [started] is set its jobs
    accumulate without queueing the run, and [start_event] is the pending
    virtual-time event that starts it. *)
and run = {
  run_id : string;
  workflow_type : string;
  chain : chain;
  execution : execution;
  attempt : int;
  input : Codec.payload;
  mutable started : bool;
  mutable start_event : event option;
  mutable jobs : Activation.job list;
  mutable queued : bool;
  mutable closed : bool;
  mutable history_length : int;
  timers : (int64, event) Hashtbl.t;
  activities : (int64, activity_request) Hashtbl.t;
  activity_retries : (int64, event) Hashtbl.t;
  children : (int64, chain) Hashtbl.t;
  query_results : (string, (Codec.payload, Error.t) result) Hashtbl.t;
  update_results : (string, update_state) Hashtbl.t;
}

(** A future virtual-time action. Cancelling one only clears [live]; the
    queue drops dead events lazily when they reach the front. *)
and event = { at : int64; order : int; mutable live : bool; action : action }

(** The work performed when an event falls due. [Start_run] delivers the
    first activation of a retry run once its backoff has elapsed. *)
and action =
  | Fire_timer of run * int64
  | Retry_activity of run * int64 * activity_request * int
  | Start_run of run

(** Events ordered by due time and then by creation order, so simultaneous
    events fire in the order their commands were emitted. *)
module Event_queue = Map.Make (struct
  type t = int64 * int

  let compare (left_at, left_order) (right_at, right_order) =
    match Int64.compare left_at right_at with
    | 0 -> Int.compare left_order right_order
    | order -> order
end)

(** Deterministically ordered registration lookup. *)
module Name_map = Map.Make (String)

type t = {
  namespace : string;
  task_queue : string;
  max_activity_attempts : int;
  workflows : workflow Name_map.t;
  activities : activity_code Name_map.t;
  mutable now_ms : int64;
  mutable events : event Event_queue.t;
  mutable next_order : int;
  mutable next_run : int;
  mutable next_workflow : int;
  mutable next_request : int;
  (* Runs with undelivered jobs, in the order they first became runnable. *)
  ready : run Queue.t;
  (* The latest chain for each workflow ID; a closed chain's ID may be
     reused by a later start, as with Temporal's default reuse policy. *)
  chains : (string, chain) Hashtbl.t;
  (* Every run ever created, newest first, so shutdown releases them all. *)
  mutable runs : run list;
  mutable shut_down : bool;
}

(** A chain together with the environment that drives it. *)
type handle = { environment : t; target : chain }

(** 2024-01-01T00:00:00Z, a fixed default so tests never read the host clock. *)
let default_start_time_ms = 1_704_067_200_000L

(** The sender identity reported for engine-originated signals and updates. *)
let identity = "temporal-testing"

(** Builds a non-retryable SDK-usage defect naming this environment. *)
let defect message = Error.defect ~message:("Temporal.Testing: " ^ message)

let ( let* ) = Result.bind

(** Returns a chain's current run. [current] is only [None] inside
    [new_chain] before the first run exists, which no caller can observe. *)
let current chain =
  match chain.current with
  | Some run -> run
  | None -> invalid_arg "Temporal.Testing: chain has no run"

(** Converts virtual milliseconds to the protobuf timestamp installed on each
    activation, which [Temporal.Workflow.now] reports. *)
let protocol_timestamp milliseconds : Protocol.timestamp =
  {
    seconds = Int64.div milliseconds 1_000L;
    nanoseconds = Int64.to_int (Int64.rem milliseconds 1_000L) * 1_000_000;
  }

(** The same conversion for activity-context metadata. *)
let activity_timestamp milliseconds : Activity_context.timestamp =
  {
    seconds = Int64.div milliseconds 1_000L;
    nanoseconds = Int64.to_int (Int64.rem milliseconds 1_000L) * 1_000_000;
  }

(** Adds a non-negative virtual-time offset to an instant, saturating at
    [Int64.max_int] instead of wrapping to a negative time. A saturated
    deadline lies beyond every reachable instant, which is what a caller
    asking for an enormous duration means. *)
let saturating_add at offset =
  if Int64.compare offset 0L > 0 && Int64.compare at (Int64.sub Int64.max_int offset) > 0
  then Int64.max_int
  else Int64.add at offset

(** The application failure type the native activity adapter reports for a
    task it cannot dispatch: an unknown or non-executable activity type, an
    unsupported argument list, and an input or output codec failure
    (including a codec that raises). See [failure_of_error] in
    [Native_activity_execution]; the literal is repeated here because that
    module is the worker's private adapter, not a shared classification
    API. Only an exception raised by the activity callback itself is typed
    [ocaml_exception]. *)
let native_dispatch_failure_type = "ocaml_temporal_native_activity"

(** The [error_type] the native worker reports for a child-workflow failure
    caused by [cause]. The native path sets it to the innermost Temporal
    application failure's type ([Failure_diagnostic.application_failure_type]).
    A workflow failing with a [Cancelled] error is encoded as a canceled
    failure, and a termination as a terminated failure, so neither carries
    an application type; every other error is encoded as an application
    failure typed by {!Error.application_failure_type}. (An activity's
    error is always encoded as an application failure, so activity
    failures always carry that type.) *)
let child_error_type cause =
  match (Error.view cause).category with
  | `Cancelled | `Terminated -> None
  | `Activity | `Bridge | `Child_workflow | `Codec | `Defect | `Nexus
  | `Timeout | `Update | `Workflow ->
      Some (Error.application_failure_type cause)

(** Wraps [cause] as the activity or child-workflow failure a workflow
    observes, mirroring the native worker's translation of Core's wrapper
    failure ([runtime_error_of_failure]): the new [category], the cause's
    details, the innermost application type [error_type], and the given
    retryability. Retryability is decided by the caller because it follows
    Temporal's retry state: an activity whose retries ended, or a child
    whose retry policy ended, is non-retryable, while a child without a
    retry policy inherits its cause's flag. *)
let wrap ~category ~error_type ~non_retryable ~prefix cause =
  let view = Error.view cause in
  Error.make ~non_retryable ?error_type ~details:view.details ~category
    ~message:(prefix ^ view.message) ()

(** The error a workflow observes for a cancelled operation or run. *)
let cancelled message = Error.make ~category:`Cancelled ~message ()

(** Applies the bridge's identifier rules to a caller-supplied workflow ID. *)
let validate_workflow_id workflow_id =
  if String.equal workflow_id "" then
    Error (defect "workflow ID must not be empty")
  else if String.contains workflow_id '\000' then
    Error (defect "workflow ID must not contain NUL")
  else if String.length workflow_id > 65_536 then
    Error (defect "workflow ID exceeds 65536 bytes")
  else if not (Codec.valid_utf_8 workflow_id) then
    Error (defect "workflow ID must be valid UTF-8")
  else Ok ()

(** Folds registrations into a name map. An override (a mock) replaces the
    ordinary registration with the same name wherever it appears in the
    list, so a test may prepend or append its mocks to an application's
    registration list. Two ordinary registrations, or two overrides, with
    one name are a defect. *)
let register kind ~name_of ~override_of ~value_of items =
  (* [ordinary] and [mocks] record every name seen of each kind, separately
     from the winning entry in [map], so a duplicate is caught whatever its
     position relative to a mock (e.g. [ordinary; mock; ordinary]). *)
  let* map, _ordinary, _mocks =
    List.fold_left
      (fun acc item ->
        let* map, ordinary, mocks = acc in
        let name = name_of item and override = override_of item in
        let seen = if override then mocks else ordinary in
        if Name_map.mem name seen then
          Error
            (defect
               (Printf.sprintf "duplicate %s%s registration: %s"
                  (if override then "mock " else "")
                  kind name))
        else if override then
          Ok (Name_map.add name item map, ordinary, Name_map.add name () mocks)
        else
          let map =
            match Name_map.find_opt name map with
            | Some existing when override_of existing -> map
            | Some _ | None -> Name_map.add name item map
          in
          Ok (map, Name_map.add name () ordinary, mocks))
      (Ok (Name_map.empty, Name_map.empty, Name_map.empty))
      items
  in
  Ok (Name_map.map value_of map)

(** Rejects a workflow registration that names two signal, query, or update
    handlers alike. [Execution.start] treats such a list as a programming error
    and raises, so checking here keeps [create] the single place where a bad
    registration surfaces, as a typed defect, exactly as the worker's own
    registration path does before any execution exists. *)
let validate_handler_names (Workflow
    { definition; signal_handlers; query_handlers; update_handlers; _ }) =
  let check kind names =
    let rec loop seen = function
      | [] -> Ok ()
      | name :: rest ->
          if List.mem name seen then
            Error
              (defect
                 (Printf.sprintf "duplicate %s handler %S in workflow %s" kind
                    name (Definition.name definition)))
          else loop (name :: seen) rest
    in
    loop [] names
  in
  let* () =
    check "signal" (List.map Execution.signal_handler_name signal_handlers)
  in
  let* () =
    check "query" (List.map Execution.query_handler_name query_handlers)
  in
  check "update" (List.map Execution.update_handler_name update_handlers)

let create ?(namespace = "default") ?(task_queue = "temporal-testing")
    ?(start_time_ms = default_start_time_ms) ?(max_activity_attempts = 10)
    ~workflows ~activities () =
  let* () =
    Workflow_context_store.validate_namespace namespace
    |> Result.map_error (fun message -> defect ("namespace: " ^ message))
  in
  let* () =
    Workflow_context_store.validate_task_queue task_queue
    |> Result.map_error (fun message -> defect ("task queue: " ^ message))
  in
  let* () =
    if Int64.compare start_time_ms 0L < 0 then
      Error (defect "start time must not be before the Unix epoch")
    else Ok ()
  in
  let* () =
    if max_activity_attempts < 1 then
      Error (defect "max_activity_attempts must be positive")
    else Ok ()
  in
  let* () =
    List.fold_left
      (fun acc workflow -> Result.bind acc (fun () -> validate_handler_names workflow))
      (Ok ()) workflows
  in
  let* workflows =
    register "workflow"
      ~name_of:(fun (Workflow { definition; _ }) -> Definition.name definition)
      ~override_of:(fun (Workflow { override; _ }) -> override)
      ~value_of:Fun.id workflows
  in
  let* activities =
    register "activity"
      ~name_of:(fun (activity : activity) -> activity.name)
      ~override_of:(fun (activity : activity) -> activity.override)
      ~value_of:(fun (activity : activity) -> activity.code)
      activities
  in
  Ok
    {
      namespace;
      task_queue;
      max_activity_attempts;
      workflows;
      activities;
      now_ms = start_time_ms;
      events = Event_queue.empty;
      next_order = 0;
      next_run = 0;
      next_workflow = 0;
      next_request = 0;
      ready = Queue.create ();
      chains = Hashtbl.create 16;
      runs = [];
      shut_down = false;
    }

let now_ms environment = environment.now_ms

(** Rejects use after [shutdown], when executions have released their
    fibers and can no longer be driven. *)
let ensure_open environment =
  if environment.shut_down then Error (defect "the environment is shut down")
  else Ok ()

(** Returns a fresh deterministic identifier for a query or update. *)
let fresh_request_id environment prefix =
  environment.next_request <- environment.next_request + 1;
  Printf.sprintf "%s-%d" prefix environment.next_request

(** Schedules [action] [delay_ms] after the current virtual time. A delay
    past the end of representable time saturates (see {!saturating_add}),
    so an enormous timer stays in the future instead of wrapping into the
    past and firing at once. *)
let schedule environment ~delay_ms action =
  let event =
    {
      at = saturating_add environment.now_ms delay_ms;
      order = environment.next_order;
      live = true;
      action;
    }
  in
  environment.next_order <- environment.next_order + 1;
  environment.events <-
    Event_queue.add (event.at, event.order) event environment.events;
  event

(** Returns the earliest live event's due time, discarding cancelled events
    at the front of the queue. *)
let rec next_event_time environment =
  match Event_queue.min_binding_opt environment.events with
  | None -> None
  | Some (key, event) when not event.live ->
      environment.events <- Event_queue.remove key environment.events;
      next_event_time environment
  | Some (_, event) -> Some event.at

(** Appends a job for [run]'s next activation and queues the run. Jobs for a
    closed run are dropped: Core never activates a closed run either. A
    retry run still in its backoff keeps the job for its first activation
    but is not queued until its [Start_run] event starts it. *)
let push_job environment run job =
  if not run.closed then begin
    run.jobs <- job :: run.jobs;
    if run.started && not run.queued then begin
      run.queued <- true;
      Queue.push run environment.ready
    end
  end

(** Records [chain]'s final outcome once and resolves the parent's child
    future. [cancelled] marks a run that closed by acknowledging a
    cancellation request ([Cancel_workflow_execution]); only that outcome
    reaches the parent as a cancellation, as Core resolves it with a
    cancelled child result. Every other error, including a workflow failure
    whose error merely has the [Cancelled] category, is wrapped as a
    [Child_workflow] failure. [retries_exhausted] marks a chain whose retry
    policy ended its retries, which the server reports with a
    non-retryable retry state; otherwise the failure inherits the cause's
    retryability, as Core reports a child without a retry policy. *)
let rec close_chain ?(cancelled = false) ?(retries_exhausted = false)
    environment chain outcome =
  if Option.is_none chain.outcome then begin
    chain.outcome <- Some outcome;
    match chain.parent with
    | Some (parent, seq) when not parent.closed ->
        Hashtbl.remove parent.children seq;
        let result =
          match outcome with
          | Ok payload -> Ok payload
          | Error error when cancelled -> Error error
          | Error error ->
              Error
                (wrap ~category:`Child_workflow
                   ~error_type:(child_error_type error)
                   ~non_retryable:
                     (retries_exhausted || (Error.view error).non_retryable)
                   ~prefix:
                     (Printf.sprintf "child workflow %s failed: "
                        chain.workflow_id)
                   error)
        in
        push_job environment parent
          (Activation.Resolve_child_workflow { seq; result })
    | Some _ | None -> ()
  end

(** Marks [run] closed, discards its pending timers, retries, and jobs, and
    applies the parent-close policy to its open children in workflow-ID
    order: the default and [Parent_terminate] terminate them,
    [Parent_request_cancel] delivers a cancellation, and [Parent_abandon]
    leaves them running. *)
and close_run environment run =
  if not run.closed then begin
    run.closed <- true;
    run.jobs <- [];
    Option.iter (fun event -> event.live <- false) run.start_event;
    run.start_event <- None;
    Hashtbl.iter (fun _ event -> event.live <- false) run.timers;
    Hashtbl.reset run.timers;
    Hashtbl.iter (fun _ event -> event.live <- false) run.activity_retries;
    Hashtbl.reset run.activity_retries;
    let children =
      Hashtbl.fold (fun _ chain acc -> chain :: acc) run.children []
      |> List.sort (fun left right ->
             String.compare left.workflow_id right.workflow_id)
    in
    Hashtbl.reset run.children;
    List.iter
      (fun child ->
        if Option.is_none child.outcome then
          match child.parent_close_policy with
          | Some Activation.Parent_abandon -> ()
          | Some Activation.Parent_request_cancel ->
              push_job environment (current child) Activation.Cancel_workflow
          | Some Activation.Parent_terminate | None ->
              close_run environment (current child);
              close_chain environment child
                (Error
                   (Error.make ~non_retryable:true ~category:`Terminated
                      ~message:"terminated because the parent workflow closed"
                      ())))
      children
  end

(** Converts a whole-millisecond interval to a protocol duration. *)
let protocol_duration milliseconds : Protocol.duration =
  {
    seconds = Int64.div milliseconds 1_000L;
    nanoseconds = Int64.to_int (Int64.rem milliseconds 1_000L) * 1_000_000;
  }

(** Converts a command retry policy to the form [Workflow.info] reports, as
    Core echoes a child's policy in its initialization job. *)
let protocol_retry_policy (policy : Activation.retry_policy) :
    Protocol.retry_policy =
  {
    initial_interval = protocol_duration policy.initial_interval;
    backoff_coefficient_bits = policy.backoff_coefficient_bits;
    maximum_interval = protocol_duration policy.maximum_interval;
    maximum_attempts = policy.maximum_attempts;
    non_retryable_error_types = policy.non_retryable_error_types;
  }

(** Creates the next run of [chain], decoding [input] with the registered
    codec, installing the run identity, and queueing its initialization
    activation. Nothing is allocated when the type is unknown or the input
    does not decode. [initiator] records why a successor exists (the
    previous run is the chain's current run) and [attempt] is the run's
    retry attempt. With [delay_ms] the initialization activation waits for
    that much virtual time, as the server delays a retry run's first
    workflow task by the retry backoff. *)
let create_run ?(attempt = 1) ?delay_ms
    ?(initiator = Protocol.Continue_as_new_workflow) environment (chain : chain)
    ~workflow_type ~input =
  match Name_map.find_opt workflow_type environment.workflows with
  | None ->
      Error
        (defect
           (Printf.sprintf "workflow type %s is not registered" workflow_type))
  | Some
      (Workflow
         { definition; signal_handlers; query_handlers; update_handlers; _ })
    -> (
      match Codec.decode (Definition.input definition) input with
      | exception exn ->
          Error
            (defect ("workflow input decoder raised: " ^ Printexc.to_string exn))
      | Error error -> Error error
      | Ok decoded ->
          environment.next_run <- environment.next_run + 1;
          let run_id = Printf.sprintf "test-run-%d" environment.next_run in
          (* The run counter is deterministic, so it also serves as a
             reproducible seed for [Temporal.Workflow.random_int]. *)
          let execution =
            Execution.start ~task_queue:chain.task_queue
              ~namespace:environment.namespace
              ~randomness_seed:(string_of_int environment.next_run)
              ~signal_handlers ~query_handlers ~update_handlers definition
              decoded
          in
          if String.equal chain.first_run_id "" then
            chain.first_run_id <- run_id;
          Execution.set_start_metadata execution
            (Some
               {
                 Workflow_context_store.memo = None;
                 search_attributes = None;
                 execution_expiration_time = None;
               });
          Execution.set_run_info execution
            (Some
               {
                 Workflow_context_store.workflow_id = chain.workflow_id;
                 run_id;
                 workflow_type;
                 attempt;
                 first_execution_run_id = chain.first_run_id;
                 parent =
                   Option.map
                     (fun ((parent : run), _) ->
                       {
                         Protocol.namespace = environment.namespace;
                         workflow_id = parent.chain.workflow_id;
                         run_id = parent.run_id;
                       })
                     chain.parent;
                 start_time = Some (protocol_timestamp environment.now_ms);
                 (* The environment records no workflow timeouts, matching a
                    server start without those options. Only a child command
                    supplies a retry policy. *)
                 execution_timeout = None;
                 run_timeout = None;
                 task_timeout = None;
                 retry_policy = Option.map protocol_retry_policy chain.retry_policy;
                 (* [chain.current] still names the predecessor here: it is
                    replaced only after this run is built. *)
                 continued_from =
                   Option.map
                     (fun (previous : run) -> (previous.run_id, initiator))
                     chain.current;
               });
          let run =
            {
              run_id;
              workflow_type;
              chain;
              execution = Execution execution;
              attempt;
              input;
              started = Option.is_none delay_ms;
              start_event = None;
              jobs = [];
              queued = false;
              closed = false;
              history_length = 0;
              timers = Hashtbl.create 8;
              activities = Hashtbl.create 8;
              activity_retries = Hashtbl.create 8;
              children = Hashtbl.create 4;
              query_results = Hashtbl.create 4;
              update_results = Hashtbl.create 4;
            }
          in
          chain.current <- Some run;
          environment.runs <- run :: environment.runs;
          push_job environment run Activation.Start_workflow;
          Option.iter
            (fun delay_ms ->
              run.start_event <-
                Some (schedule environment ~delay_ms (Start_run run)))
            delay_ms;
          Ok run)

(** Returns the chain registered for [workflow_id] when it is still open and,
    if [run_id] is non-empty, its current run has that ID. *)
let find_open_chain environment ~workflow_id ~run_id =
  match Hashtbl.find_opt environment.chains workflow_id with
  | Some chain
    when Option.is_none chain.outcome
         && (String.equal run_id "" || String.equal run_id (current chain).run_id)
    ->
      Some chain
  | Some _ | None -> None

(** Creates and registers a chain whose first run is [workflow_type].
    [task_queue] defaults to the environment's queue. *)
let new_chain ?retry_policy ?task_queue environment ~workflow_id
    ~workflow_type ~input ~parent ~parent_close_policy =
  let chain =
    {
      workflow_id;
      current = None;
      first_run_id = "";
      outcome = None;
      parent;
      parent_close_policy;
      retry_policy;
      task_queue = Option.value task_queue ~default:environment.task_queue;
    }
  in
  let* _run = create_run environment chain ~workflow_type ~input in
  Hashtbl.replace environment.chains workflow_id chain;
  Ok chain

(** Parses the exact IEEE-754 bit pattern carried by a retry policy. *)
let float_of_bits bits =
  match Int64.of_string_opt ("0u" ^ bits) with
  | Some bits -> Int64.float_of_bits bits
  | None -> Float.nan

(** Returns the virtual delay before the next attempt, or [None] when the
    failure is final. This follows Temporal's retry rules, which the server
    applies alike to activity attempts and to the runs of a child workflow
    with a retry policy: a non-retryable error, a listed non-retryable
    error type, or an exhausted attempt budget ends the retries; otherwise
    the delay grows by the backoff coefficient up to the maximum interval.
    Unset fields take the server defaults (1s initial interval, coefficient
    2.0, maximum 100 times the initial interval), and an unlimited budget is
    capped at the environment's [max_activity_attempts]. An error with the
    [Cancelled] category is an ordinary failure here: the native worker
    treats it as a cancellation only when Core actually delivered one, and
    the engine never routes its own cancellations through this function. *)
let retry_delay environment policy ~attempt error =
  let view = Error.view error in
  if view.non_retryable then None
  else
    let initial, coefficient, maximum_interval, maximum_attempts, non_retryable =
      match (policy : Activation.retry_policy option) with
      | None -> (0L, Float.nan, 0L, 0, [])
      | Some policy ->
          ( policy.initial_interval,
            float_of_bits policy.backoff_coefficient_bits,
            policy.maximum_interval,
            policy.maximum_attempts,
            policy.non_retryable_error_types )
    in
    let initial = if Int64.compare initial 0L <= 0 then 1_000L else initial in
    let coefficient =
      if Float.is_nan coefficient || coefficient < 1.0 then 2.0 else coefficient
    in
    let maximum_interval =
      if Int64.compare maximum_interval 0L <= 0 then Int64.mul initial 100L
      else maximum_interval
    in
    let limit =
      if maximum_attempts > 0 then maximum_attempts
      else environment.max_activity_attempts
    in
    if attempt >= limit then None
    else if List.mem (Error.application_failure_type error) non_retryable then
      None
    else
      let delay =
        Int64.to_float initial *. (coefficient ** float_of_int (attempt - 1))
      in
      if Float.is_nan delay || delay >= Int64.to_float maximum_interval then
        Some maximum_interval
      else Some (Int64.of_float (Float.ceil delay))

(** How a run's terminal command closed it. Only [Run_failed] is eligible
    for a child's workflow retry policy, and only [Run_cancelled] reaches a
    parent as a cancellation. [Task_failed] is the environment's stand-in
    for a workflow task failure, which a server would retry rather than
    close the run with, so it is never retried as a workflow failure. *)
type closure = Run_completed | Run_failed | Run_cancelled | Task_failed

(** Closes [run] and, when it is its chain's current run, either retries the
    chain under its retry policy or closes the chain. A retry creates the
    next run of the same workflow type and input with [attempt + 1], linked
    by a retry continuation, whose first activation waits out the backoff;
    the parent is not told, exactly as with server-side child retries. *)
let finish environment run closure outcome =
  close_run environment run;
  if current run.chain == run then
    let chain = run.chain in
    match (closure, outcome, chain.retry_policy) with
    | Run_failed, Error error, (Some _ as policy) -> (
        match retry_delay environment policy ~attempt:run.attempt error with
        | Some delay_ms -> (
            match
              create_run ~attempt:(run.attempt + 1) ~delay_ms
                ~initiator:Protocol.Continue_as_new_retry environment chain
                ~workflow_type:run.workflow_type ~input:run.input
            with
            | Ok _ -> ()
            | Error error -> close_chain environment chain (Error error))
        | None -> close_chain ~retries_exhausted:true environment chain outcome)
    | Run_cancelled, _, _ -> close_chain ~cancelled:true environment chain outcome
    | (Run_completed | Run_failed | Task_failed), _, _ ->
        close_chain environment chain outcome

(** Runs one attempt of [request] synchronously, outside every workflow
    context, and returns its encoded output or failure. Activities take zero
    virtual time. Failures are classified as the native activity adapter
    classifies them: a task that cannot be dispatched (unknown or
    unsupported type, bad argument list, codec failure or a raising codec)
    fails non-retryably with {!native_dispatch_failure_type}, and an
    exception from the callback itself fails non-retryably as
    [ocaml_exception]. *)
let attempt_activity environment run request ~attempt =
  let failure ?(error_type = native_dispatch_failure_type) message =
    Error
      (Error.make ~non_retryable:true ~error_type ~category:`Activity ~message
         ())
  in
  match Name_map.find_opt request.activity_type environment.activities with
  | None ->
      failure
        (Printf.sprintf
           "activity type %s is not registered with the test environment"
           request.activity_type)
  | Some (Unsupported reason) -> failure reason
  | Some (Code definition) -> (
      match Definition.implementation definition with
      | None ->
          failure
            (Printf.sprintf "activity %s has no implementation"
               request.activity_type)
      | Some implementation -> (
          let* payload =
            match request.arguments with
            | [] -> Ok (Payload.unit_null ())
            | [ payload ] -> Ok payload
            | _ ->
                failure
                  "activity definitions currently accept exactly one input \
                   value"
          in
          let* input =
            match Codec.decode (Definition.input definition) payload with
            | Ok input -> Ok input
            | Error error ->
                failure
                  ("activity input decoding failed: " ^ Error.message error)
            | exception exn ->
                failure
                  ("activity input decoder raised: " ^ Printexc.to_string exn)
          in
          let milliseconds = Option.map Temporal_base.Duration.of_ms in
          let scheduled = Some (activity_timestamp request.scheduled_ms) in
          let now = Some (activity_timestamp environment.now_ms) in
          let info : Activity_context.info =
            {
              namespace = environment.namespace;
              workflow_id = run.chain.workflow_id;
              workflow_run_id = run.run_id;
              workflow_type = run.workflow_type;
              activity_id = request.activity_id;
              activity_type = request.activity_type;
              attempt;
              is_local = request.is_local;
              scheduled_time = scheduled;
              current_attempt_scheduled_time = now;
              started_time = now;
              schedule_to_close_timeout =
                milliseconds request.schedule_to_close_timeout;
              start_to_close_timeout = milliseconds request.start_to_close_timeout;
              task_heartbeat_timeout = milliseconds request.heartbeat_timeout;
            }
          in
          let context =
            Activity_context.create_with_info ~info
              ~heartbeat:(fun details ->
                request.heartbeat_details <- details;
                Ok ())
              ~details:request.heartbeat_details
              ~heartbeat_timeout:(milliseconds request.heartbeat_timeout)
          in
          let result =
            match implementation context input with
            | result -> result
            | exception exn ->
                failure ~error_type:"ocaml_exception"
                  ("activity callback raised: " ^ Printexc.to_string exn)
          in
          Activity_context.invalidate context;
          match result with
          | Error error -> Error error
          | Ok output -> (
              match Codec.encode (Definition.output definition) output with
              | Ok payload -> Ok payload
              | Error error ->
                  failure
                    ("activity output encoding failed: " ^ Error.message error)
              | exception exn ->
                  failure
                    ("activity output encoder raised: " ^ Printexc.to_string exn)
              )))

(** Runs attempt [attempt] of the activity identified by [seq] and either
    queues its resolution for [run] or schedules the next attempt after the
    retry delay. A final failure is wrapped as a non-retryable [Activity]
    error, as the native worker reports a failure whose retry state is
    [NON_RETRYABLE_FAILURE] or [MAXIMUM_ATTEMPTS_REACHED], the only ways
    the simulated retries end. An error with the [Cancelled] category
    returned by the activity is wrapped too: only {!cancel_activity}
    produces a workflow-visible activity cancellation. *)
let run_activity environment run seq request ~attempt =
  match attempt_activity environment run request ~attempt with
  | Ok payload ->
      push_job environment run
        (Activation.Resolve_activity { seq; result = Ok payload })
  | Error error -> (
      match retry_delay environment request.retry_policy ~attempt error with
      | Some delay_ms ->
          let event =
            schedule environment ~delay_ms
              (Retry_activity (run, seq, request, attempt + 1))
          in
          Hashtbl.replace run.activity_retries seq event
      | None ->
          let error =
            wrap ~category:`Activity ~non_retryable:true
              ~error_type:(Some (Error.application_failure_type error))
              ~prefix:(Printf.sprintf "activity %s failed: " request.activity_type)
              error
          in
          push_job environment run
            (Activation.Resolve_activity { seq; result = Error error }))

(** Applies a workflow's activity cancellation request. An activity waiting
    for a retry is cancelled at once. An attempt that already finished but
    whose result has not been delivered is reported as cancelled unless the
    workflow asked to wait for cancellation to complete, in which case the
    real result stands because the attempt cannot observe the request. *)
let cancel_activity environment run seq =
  let result = Error (cancelled "activity cancelled") in
  match Hashtbl.find_opt run.activity_retries seq with
  | Some event ->
      event.live <- false;
      Hashtbl.remove run.activity_retries seq;
      push_job environment run (Activation.Resolve_activity { seq; result })
  | None -> (
      match Hashtbl.find_opt run.activities seq with
      | Some { cancellation_type = Activation.Wait_cancellation_completed; _ }
      | None ->
          ()
      | Some _ ->
          run.jobs <-
            List.map
              (function
                | Activation.Resolve_activity { seq = pending; _ }
                  when Int64.equal pending seq ->
                    Activation.Resolve_activity { seq; result }
                | job -> job)
              run.jobs)

(** Converts a command's workflow argument list into the single payload an
    OCaml workflow decodes, as a native worker does: zero arguments (a
    [Codec.unit] input) become the canonical unit payload and one argument is
    used as is. OCaml workflows take exactly one input, so more is a defect. *)
let workflow_input_of_arguments = function
  | [] -> Ok (Payload.unit_null ())
  | [ payload ] -> Ok payload
  | _ -> Error (defect "workflow definitions accept exactly one input value")

(** Starts a child chain for a [Start_child_workflow] command and queues the
    start acknowledgement. A running workflow ID, an unregistered type, or an
    input the child cannot decode fails the start instead. The child's
    [retry_policy] governs its runs as on a server, and [task_queue] is
    reported by its [Workflow.info]; every registered workflow runs
    regardless of the queue, as if a worker polled each one. *)
let start_child environment run ~seq ~id ~name ~input ~parent_close_policy
    ~retry_policy ~task_queue =
  let start_failed cause =
    push_job environment run
      (Activation.Resolve_child_workflow_start
         {
           seq;
           result =
             Error
               (Error.make ~non_retryable:true ~category:`Child_workflow
                  ~message:
                    (Printf.sprintf
                       "child workflow start failed: id=%s type=%s cause=%s" id
                       name cause)
                  ());
         })
  in
  if Option.is_some (find_open_chain environment ~workflow_id:id ~run_id:"")
  then start_failed "workflow_already_exists"
  else
    match
      Result.bind (workflow_input_of_arguments input) (fun input ->
          new_chain ?retry_policy ?task_queue environment ~workflow_id:id
            ~workflow_type:name ~input ~parent:(Some (run, seq))
            ~parent_close_policy)
    with
    | Error error -> start_failed (Error.message error)
    | Ok chain ->
        Hashtbl.replace run.children seq chain;
        push_job environment run
          (Activation.Resolve_child_workflow_start
             { seq; result = Ok (current chain).run_id })

(** Delivers [job] to an external workflow and acknowledges the requesting
    run with [resolve]. A missing or closed target fails the request. *)
let deliver_external environment run ~workflow_id ~run_id ~job ~resolve =
  let result =
    match find_open_chain environment ~workflow_id ~run_id with
    | Some target ->
        push_job environment (current target) job;
        Ok ()
    | None ->
        Error
          (Error.make ~non_retryable:true ~category:`Workflow
             ~message:("external workflow not found: " ^ workflow_id)
             ())
  in
  push_job environment run (resolve result)

(** Interprets one command emitted by [run]. Query answers and update
    responses are recorded even after the run closes, because a query-only
    activation of a closed run and a terminal activation that also answers an
    update both legitimately produce them. *)
let rec apply_command environment run command =
  match (command : Activation.command) with
  | Query_result { query_id; result } ->
      Hashtbl.replace run.query_results query_id result
  | Update_response { protocol_instance_id; response } ->
      let state =
        match response with
        | `Accepted -> Update_accepted
        | `Rejected error -> Update_rejected error
        | `Completed payload -> Update_completed payload
      in
      Hashtbl.replace run.update_results protocol_instance_id state
  | _ when run.closed -> ()
  | Schedule_activity
      {
        seq;
        activity_id;
        activity_type;
        arguments;
        retry_policy;
        cancellation_type;
        schedule_to_close_timeout;
        start_to_close_timeout;
        heartbeat_timeout;
        _;
      } ->
      let request =
        {
          activity_id;
          activity_type;
          arguments;
          retry_policy;
          cancellation_type;
          is_local = false;
          schedule_to_close_timeout;
          start_to_close_timeout;
          heartbeat_timeout;
          scheduled_ms = environment.now_ms;
          heartbeat_details = [];
        }
      in
      Hashtbl.replace run.activities seq request;
      run_activity environment run seq request ~attempt:1
  | Schedule_local_activity
      {
        seq;
        activity_id;
        activity_type;
        arguments;
        retry_policy;
        cancellation_type;
        schedule_to_close_timeout;
        start_to_close_timeout;
        _;
      } ->
      let request =
        {
          activity_id;
          activity_type;
          arguments;
          retry_policy;
          cancellation_type;
          is_local = true;
          schedule_to_close_timeout;
          start_to_close_timeout;
          heartbeat_timeout = None;
          scheduled_ms = environment.now_ms;
          heartbeat_details = [];
        }
      in
      Hashtbl.replace run.activities seq request;
      run_activity environment run seq request ~attempt:1
  | Request_cancel_activity { seq } | Request_cancel_local_activity { seq } ->
      cancel_activity environment run seq
  | Start_timer { seq; milliseconds } ->
      let event =
        schedule environment ~delay_ms:milliseconds (Fire_timer (run, seq))
      in
      Hashtbl.replace run.timers seq event
  | Cancel_timer { seq } -> (
      match Hashtbl.find_opt run.timers seq with
      | Some event ->
          event.live <- false;
          Hashtbl.remove run.timers seq
      | None -> ())
  | Start_child_workflow
      { seq; id; name; input; parent_close_policy; retry_policy; task_queue; _ }
    ->
      start_child environment run ~seq ~id ~name ~input ~parent_close_policy
        ~retry_policy ~task_queue
  | Cancel_child_workflow { seq; _ } -> (
      match Hashtbl.find_opt run.children seq with
      | Some child ->
          push_job environment (current child) Activation.Cancel_workflow
      | None -> ())
  | Signal_external_workflow
      { seq; workflow_id; run_id; signal_name; input; headers; _ } ->
      deliver_external environment run ~workflow_id ~run_id
        ~job:
          (Activation.Signal_workflow
             { signal_name; input; identity; headers })
        ~resolve:(fun result ->
          Activation.Resolve_signal_external_workflow { seq; result })
  | Request_cancel_external_workflow { seq; workflow_id; run_id; _ } ->
      deliver_external environment run ~workflow_id ~run_id
        ~job:Activation.Cancel_workflow
        ~resolve:(fun result ->
          Activation.Resolve_request_cancel_external_workflow { seq; result })
  | Set_patch_marker _ | Upsert_search_attributes _ -> ()
  | Complete_workflow payload -> finish environment run Run_completed (Ok payload)
  | Fail_workflow error -> finish environment run Run_failed (Error error)
  | Cancel_workflow_execution ->
      finish environment run Run_cancelled
        (Error (cancelled "workflow cancelled"))
  | Continue_as_new { workflow_type; input } -> (
      close_run environment run;
      match
        Result.bind (workflow_input_of_arguments input) (fun input ->
            create_run environment run.chain ~workflow_type ~input)
      with
      | Ok _ -> ()
      | Error error -> close_chain environment run.chain (Error error))

(** Reports whether [command] closes the run that emitted it. *)
and is_terminal_command : Activation.command -> bool = function
  | Complete_workflow _ | Fail_workflow _ | Cancel_workflow_execution
  | Continue_as_new _ ->
      true
  | Schedule_activity _ | Schedule_local_activity _ | Start_child_workflow _
  | Cancel_child_workflow _ | Signal_external_workflow _
  | Request_cancel_external_workflow _ | Request_cancel_activity _
  | Request_cancel_local_activity _ | Start_timer _ | Cancel_timer _
  | Query_result _ | Update_response _ | Set_patch_marker _
  | Upsert_search_attributes _ ->
      false

(** Reports whether [command] asks the server to act outside the emitting
    run. The server records such a command but drops its transfer task when
    the same workflow task also closed the run: an activity task fails with
    "workflow completed" (and eager dispatch is skipped), a child start is
    abandoned even under [Parent_abandon], and external signals and
    cancellations, including a child cancellation, are never sent. Core
    likewise never runs a local activity for a completed run. *)
and is_dispatched_by_server : Activation.command -> bool = function
  | Schedule_activity _ | Schedule_local_activity _ | Start_child_workflow _
  | Cancel_child_workflow _ | Signal_external_workflow _
  | Request_cancel_external_workflow _ ->
      true
  | Request_cancel_activity _ | Request_cancel_local_activity _
  | Start_timer _ | Cancel_timer _ | Query_result _ | Update_response _
  | Set_patch_marker _ | Upsert_search_attributes _ | Complete_workflow _
  | Fail_workflow _ | Cancel_workflow_execution | Continue_as_new _ ->
      false

(** Delivers [jobs] to [run] in one activation stamped with the current
    virtual time and interprets the resulting commands in emission order.
    When the batch closes the run, the commands a server would never
    dispatch for a closed run ({!is_dispatched_by_server}) are dropped, so a
    test cannot observe an activity, child, or external signal that
    production never runs. A workflow task failure closes the run with that
    error: the environment does not retry tasks, so the defect is reported
    to the test instead of being retried forever. [counts_toward_history]
    is [false] for a query-only activation, which Core answers without
    appending workflow history, so [Workflow.Info.history_length] does not
    grow with test queries. *)
and activate ?(counts_toward_history = true) environment run jobs =
  let (Execution execution) = run.execution in
  if counts_toward_history then
    run.history_length <- run.history_length + List.length jobs + 2;
  Execution.set_activation_timestamp execution
    (Some (protocol_timestamp environment.now_ms));
  Execution.set_activation_is_replaying execution false;
  Execution.set_activation_history execution
    {
      Workflow_context_store.history_length = run.history_length;
      history_size_bytes = None;
      continue_as_new_suggested = false;
      continue_as_new_reasons = [];
    };
  let commands = Execution.activate execution jobs in
  (match Execution.task_failure execution with
  | Some error when not run.closed ->
      finish environment run Task_failed (Error error)
  | Some _ | None -> ());
  let commands =
    if List.exists is_terminal_command commands then
      List.filter (fun command -> not (is_dispatched_by_server command)) commands
    else commands
  in
  List.iter (apply_command environment run) commands

(** Ranks [job] in Core's activation order: initialization; patch
    notifications and random-seed updates; signals and updates; every other
    job; local-activity resolutions; queries (see the pinned Core
    [workflow_activation.proto]). The workflow runtime relies on this order,
    for example to run a signal handler before a root continuation woken by
    a timer in the same activation. A [Resolve_activity] is a local-activity
    resolution when [run] scheduled that sequence as a local activity. *)
let job_rank (run : run) : Activation.job -> int = function
  | Start_workflow -> 0
  | Notify_has_patch _ | Update_random_seed _ -> 1
  | Signal_workflow _ | Do_update _ -> 2
  | Resolve_local_activity_backoff _ -> 4
  | Resolve_activity { seq; _ } -> (
      match Hashtbl.find_opt run.activities seq with
      | Some { is_local = true; _ } -> 4
      | Some { is_local = false; _ } | None -> 3)
  | Query_workflow _ -> 5
  | Resolve_child_workflow_start _ | Resolve_child_workflow _
  | Resolve_signal_external_workflow _
  | Resolve_request_cancel_external_workflow _ | Fire_timer _
  | Cancel_workflow | Remove_from_cache ->
      3

(** Activates runnable executions in FIFO order until none has undelivered
    jobs. Each run's jobs are stable-sorted into Core's order
    ({!job_rank}), so jobs of one rank keep their arrival order. Virtual
    time does not move. *)
let drain environment =
  while not (Queue.is_empty environment.ready) do
    let run = Queue.pop environment.ready in
    run.queued <- false;
    let jobs =
      List.rev run.jobs
      |> List.stable_sort (fun left right ->
             Int.compare (job_rank run left) (job_rank run right))
    in
    run.jobs <- [];
    if (not run.closed) && jobs <> [] then activate environment run jobs
  done

(** Moves virtual time to [at] and performs every live event due by then in
    order, then runs the executions they woke. *)
let fire_due environment at =
  if Int64.compare at environment.now_ms > 0 then environment.now_ms <- at;
  let rec loop () =
    match Event_queue.min_binding_opt environment.events with
    | Some (key, event) when Int64.compare event.at environment.now_ms <= 0 ->
        environment.events <- Event_queue.remove key environment.events;
        (if event.live then
           match event.action with
           | Fire_timer (run, seq) ->
               Hashtbl.remove run.timers seq;
               push_job environment run (Activation.Fire_timer { seq })
           | Retry_activity (run, seq, request, attempt) ->
               Hashtbl.remove run.activity_retries seq;
               if not run.closed then
                 run_activity environment run seq request ~attempt
           | Start_run run ->
               (* The retry backoff has elapsed: release the jobs gathered
                  since the run was created, led by its initialization. *)
               run.start_event <- None;
               if not run.closed then begin
                 run.started <- true;
                 if (not run.queued) && run.jobs <> [] then begin
                   run.queued <- true;
                   Queue.push run environment.ready
                 end
               end);
        loop ()
    | Some _ | None -> ()
  in
  loop ();
  drain environment

(** Runs the environment until [finished] holds, skipping virtual time to
    the next event whenever nothing else can progress. [describe] names the
    awaited operation in the blocked and timeout diagnostics. A deadline past
    the end of representable virtual time saturates at [Int64.max_int]
    instead of overflowing into an already-expired one. A finite timeout
    stays a bound even then: once the clock has reached [Int64.max_int],
    time cannot advance, so an event due there again (for example a timer a
    looping workflow keeps scheduling) ends the wait with [Timeout] rather
    than firing forever. *)
let run_until environment ?timeout_ms ~describe finished =
  let deadline =
    Option.map
      (fun timeout_ms -> saturating_add environment.now_ms timeout_ms)
      timeout_ms
  in
  (* Whether the next event at [at] lies beyond the caller's bound. *)
  let past_deadline at deadline =
    Int64.compare at deadline > 0
    || Int64.equal at Int64.max_int
       && Int64.equal environment.now_ms Int64.max_int
  in
  let rec loop () =
    drain environment;
    if finished () then Ok ()
    else
      match next_event_time environment with
      | None ->
          Error
            (defect
               (describe
              ^ " is blocked with no pending timer or activity retry; it waits \
                 for a signal, update, or condition that nothing will satisfy"
               ))
      | Some at -> (
          match deadline with
          | Some deadline when past_deadline at deadline ->
              if Int64.compare deadline environment.now_ms > 0 then
                environment.now_ms <- deadline;
              Error
                (Error.make ~non_retryable:true ~category:`Timeout
                   ~message:
                     ("Temporal.Testing: " ^ describe
                    ^ " did not finish within the virtual-time timeout")
                   ())
          | Some _ | None ->
              fire_due environment at;
              loop ())
  in
  loop ()

let start ?workflow_id environment ~workflow_type ~input =
  let* () = ensure_open environment in
  let workflow_id =
    match workflow_id with
    | Some workflow_id -> workflow_id
    | None ->
        environment.next_workflow <- environment.next_workflow + 1;
        Printf.sprintf "workflow-%d" environment.next_workflow
  in
  let* () = validate_workflow_id workflow_id in
  if Option.is_some (find_open_chain environment ~workflow_id ~run_id:"") then
    Error (defect ("workflow ID is already running: " ^ workflow_id))
  else
    let* chain =
      new_chain environment ~workflow_id ~workflow_type ~input ~parent:None
        ~parent_close_policy:None
    in
    drain environment;
    Ok { environment; target = chain }

let workflow_id handle = handle.target.workflow_id
let run_id handle = (current handle.target).run_id

let result ?timeout_ms { environment; target = chain } =
  let* () = ensure_open environment in
  let* () =
    run_until environment ?timeout_ms
      ~describe:("workflow " ^ chain.workflow_id)
      (fun () -> Option.is_some chain.outcome)
  in
  match chain.outcome with
  | Some outcome -> outcome
  | None -> Error (defect "workflow finished without an outcome")

(** Returns an error when [chain] has closed, so requests aimed at a closed
    workflow fail as they would against a server. *)
let ensure_running chain =
  if Option.is_some chain.outcome then
    Error (defect ("workflow is closed: " ^ chain.workflow_id))
  else Ok ()

let signal { environment; target = chain } ~name ~input =
  let* () = ensure_open environment in
  drain environment;
  let* () = ensure_running chain in
  push_job environment (current chain)
    (Activation.Signal_workflow
       { signal_name = name; input; identity; headers = [] });
  drain environment;
  Ok ()

let query { environment; target = chain } ~name ~arguments =
  let* () = ensure_open environment in
  drain environment;
  let run = current chain in
  let* () =
    if run.started then Ok ()
    else
      Error
        (defect
           ("query cannot be answered while a retry run waits for its first \
             workflow task: " ^ chain.workflow_id))
  in
  let query_id = fresh_request_id environment "query" in
  (* A query is answered in its own activation, as Core delivers it, so it
     cannot run workflow fibers or append commands, and it appends no
     history. *)
  activate ~counts_toward_history:false environment run
    [
      Activation.Query_workflow
        { query_id; query_type = name; arguments; headers = [] };
    ];
  drain environment;
  match Hashtbl.find_opt run.query_results query_id with
  | Some result ->
      Hashtbl.remove run.query_results query_id;
      result
  | None -> Error (defect ("query produced no answer: " ^ name))

let update ?timeout_ms { environment; target = chain } ~name ~input =
  let* () = ensure_open environment in
  drain environment;
  let* () = ensure_running chain in
  let run = current chain in
  let update_id = fresh_request_id environment "update" in
  push_job environment run
    (Activation.Do_update
       {
         id = update_id;
         protocol_instance_id = update_id;
         name;
         input;
         headers = [];
         identity;
         update_id;
         run_validator = true;
       });
  let settled () =
    match Hashtbl.find_opt run.update_results update_id with
    | Some (Update_completed _ | Update_rejected _) -> true
    | Some Update_accepted | None -> run.closed
  in
  let* () =
    run_until environment ?timeout_ms ~describe:("update " ^ name) settled
  in
  match Hashtbl.find_opt run.update_results update_id with
  | Some (Update_completed payload) -> Ok payload
  | Some (Update_rejected error) -> Error error
  | Some Update_accepted | None ->
      Error
        (Error.make ~non_retryable:true ~category:`Update
           ~message:
             ("workflow closed before update " ^ name ^ " completed: "
            ^ chain.workflow_id)
           ())

let cancel { environment; target = chain } =
  let* () = ensure_open environment in
  drain environment;
  let* () = ensure_running chain in
  push_job environment (current chain) Activation.Cancel_workflow;
  drain environment;
  Ok ()

let skip environment ~milliseconds =
  let* () = ensure_open environment in
  if Int64.compare milliseconds 0L < 0 then
    Error (defect "skip duration must not be negative")
  else begin
    (* Saturate so an enormous skip pins the clock at the last
       representable instant instead of wrapping it negative. *)
    let target = saturating_add environment.now_ms milliseconds in
    drain environment;
    let rec loop () =
      match next_event_time environment with
      | Some at when Int64.compare at target <= 0 ->
          fire_due environment at;
          loop ()
      | Some _ | None -> ()
    in
    loop ();
    environment.now_ms <- target;
    Ok ()
  end

let shutdown environment =
  if not environment.shut_down then begin
    environment.shut_down <- true;
    Queue.clear environment.ready;
    environment.events <- Event_queue.empty;
    List.iter
      (fun run ->
        let (Execution execution) = run.execution in
        Execution.shutdown execution)
      environment.runs
  end
