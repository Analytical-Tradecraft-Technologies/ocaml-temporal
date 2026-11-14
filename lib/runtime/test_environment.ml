(** Deterministic in-process workflow test engine. See the interface for the
    simulated semantics; the comments below explain how each piece of server
    behavior is approximated. *)

module Error = Temporal_base.Error
module Codec = Temporal_base.Codec
module Payload = Temporal_base.Payload
module Definition = Temporal_base.Definition
module Activity_context = Temporal_base.Activity_context
module Async_activity = Temporal_base.Async_activity
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
  (* Executable code; the existential pairs the codecs with the callback.
     The list gives the virtual milliseconds each attempt takes, in attempt
     order, with the last element repeating; empty means every attempt
     takes zero time. *)
  | Code :
      ( 'input,
        'output,
        Activity_context.t -> 'input -> ('output, Error.t) result )
      Definition.t
      * int64 list
      -> activity_code
  (* An asynchronous callback (#1160). An attempt whose callback returns
     [Completed] or [Failed] takes its listed duration like [Code]; one that
     returns [Will_complete_async] stays pending until the test finishes its
     handle or a timeout expires. *)
  | Async_code :
      ( 'input,
        'output,
        'output Async_activity.context ->
        'input ->
        'output Async_activity.async_result )
      Definition.t
      * int64 list
      -> activity_code
  (* Every attempt fails with this fixed, non-retryable diagnostic. *)
  | Unsupported of string

(** A named activity registration and whether it replaces an earlier one. *)
type activity = { name : string; override : bool; code : activity_code }

let workflow ?(override = false) ?(signal_handlers = []) ?(query_handlers = [])
    ?(update_handlers = []) definition =
  Workflow
    { override; definition; signal_handlers; query_handlers; update_handlers }

let activity ?(override = false) ?(durations_ms = []) definition =
  {
    name = Definition.name definition;
    override;
    code = Code (definition, durations_ms);
  }

let async_activity ?(override = false) ?(durations_ms = []) definition =
  {
    name = Definition.name definition;
    override;
    code = Async_code (definition, durations_ms);
  }

let unsupported_activity ?(override = false) ~name ~reason () =
  { name; override; code = Unsupported reason }

type task_failure_mode = Fail_run | Retry_task

type id_conflict_policy =
  | Conflict_fail
  | Conflict_use_existing
  | Conflict_terminate_existing

(** Hides the input and output types of a live execution so runs of different
    workflow types share one registry. Only payload-level operations are
    needed after construction. *)
type execution = Execution : ('input, 'output) Execution.t -> execution

(** The latest phase recorded for one update protocol instance. *)
type update_state =
  | Update_accepted
  | Update_completed of Codec.payload
  | Update_rejected of Error.t

(** An activity timeout the engine enforces in virtual time. *)
type activity_timeout = [ `Start_to_close | `Schedule_to_close | `Heartbeat ]

(** How one activity attempt ended. [Finished] is what the callback (or a
    dispatch failure) returned; [Timed_out] is a timeout the engine itself
    expired. Keeping them apart means an error a callback returns is always
    an application failure, whatever origin it carries, as on a native
    worker, which transmits neither its origin nor its heartbeat details. *)
type attempt_outcome =
  | Finished of (Codec.payload, Error.t) result
  | Timed_out of activity_timeout

(** The activity command fields the engine needs to run, retry, and cancel
    attempts. [heartbeat_details] carries the last details recorded by an
    attempt into the next one, matching Temporal's heartbeat-details
    hand-off. [scheduled_ms] is the virtual time of the first attempt.
    [cancel_requested] records a cancellation the engine is waiting out
    under [Wait_cancellation_completed]: the running attempt's outcome
    still stands, but no further attempt starts. *)
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
  mutable cancel_requested : bool;
}

(** A workflow ID's sequence of runs. [current] is [None] only while the
    first run is being constructed. [outcome] is set exactly once, when the
    last run closes; continue-as-new and a retry replace [current] without
    setting it. [parent] identifies the parent run and the child command
    sequence to resolve when the chain closes. [retry_policy] is the
    workflow retry policy the server applies to the current run (a child
    command's policy; top-level starts have none). [task_queue] is the queue
    [Workflow.info] reports for the current run, and [run_timeout] and
    [task_timeout] the run and workflow-task timeouts in milliseconds. Each
    run's run timeout is enforced in virtual time (#1160); the task timeout
    is only reported, because a simulated workflow task never waits for a
    worker. A continue-as-new command that sets any of them (#1153) replaces
    it for the successor and later runs, as the server does; an unset one is
    inherited. [execution_timeout] bounds the whole chain from its first
    run's start, so [execution_deadline_ms] is fixed when the chain is
    created and every run reports it as its execution expiration time;
    [execution_event] is the pending event that expires it. *)
type chain = {
  workflow_id : string;
  mutable current : run option;
  mutable first_run_id : string;
  mutable outcome : (Codec.payload, Error.t) result option;
  parent : (run * int64) option;
  parent_close_policy : Activation.child_workflow_parent_close_policy option;
  mutable retry_policy : Activation.retry_policy option;
  mutable task_queue : string;
  execution_timeout : int64 option;
  execution_deadline_ms : int64 option;
  mutable run_timeout : int64 option;
  mutable task_timeout : int64 option;
  mutable execution_event : event option;
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
    virtual-time event that starts it. [start_memo] and
    [start_search_attributes] are the maps the run started with, and [memo]
    and [search_attributes] their current values after the run's upserts,
    merged as the server merges them (#1154).

    [execution] is replaced when a failed workflow task is retried
    (#1160): [make_execution] builds a fresh execution with the run's
    original identity, seed and start snapshot from a freshly decoded copy
    of its start input (so a run-local mutation of the decoded input never
    reaches a rebuild), or returns the decoder's error, and [history] (newest
    first) holds the activations it has completed, which a rebuild replays
    before delivering the failed task again. [history] is recorded only in
    {!Retry_task} mode. [stuck] holds the failure and jobs of a workflow
    task that failed in that mode and waits for a retry; while it is set
    the run is never activated and new jobs accumulate in [jobs].
    [run_timeout_event] expires the run's run timeout, and
    [async_attempts] holds the activity attempts handed off with
    [Will_complete_async], keyed by command sequence. *)
and run = {
  run_id : string;
  workflow_type : string;
  chain : chain;
  mutable execution : execution;
  make_execution : unit -> (execution, Error.t) result;
  mutable history : recorded_activation list;
  mutable stuck : (Error.t * Activation.job list) option;
  mutable run_timeout_event : event option;
  async_attempts : (int64, async_attempt) Hashtbl.t;
  attempt : int;
  input : Codec.payload;
  start_memo : (string * Codec.payload) list;
  start_search_attributes : (string * Codec.payload) list;
  mutable memo : (string * Codec.payload) list;
  mutable search_attributes : (string * Codec.payload) list;
  mutable started : bool;
  mutable start_event : event option;
  mutable jobs : Activation.job list;
  mutable queued : bool;
  mutable closed : bool;
  mutable history_length : int;
  timers : (int64, event) Hashtbl.t;
  activities : (int64, activity_request) Hashtbl.t;
  activity_retries : (int64, event) Hashtbl.t;
  activity_attempts : (int64, event) Hashtbl.t;
  children : (int64, chain) Hashtbl.t;
  query_results : (string, (Codec.payload, Error.t) result) Hashtbl.t;
  update_results : (string, update_state) Hashtbl.t;
}

(** One completed activation kept for replay: its jobs (with rejected
    updates removed and accepted ones marked as not needing validation, as
    Core replays them from history), the patch IDs whose markers its
    commands recorded, the virtual time it was stamped with, and the history
    length it reported. *)
and recorded_activation = {
  recorded_jobs : Activation.job list;
  recorded_patches : string list;
  recorded_at_ms : int64;
  recorded_history_length : int;
}

(** An activity attempt whose callback returned [Will_complete_async]
    (#1160). Its handle reaches the engine through the submit callback that
    captures this record, so an operation applies only while the record is
    still the run's entry for [async_seq]. [async_started_ms] and
    [last_heartbeat_ms] anchor the start-to-close and heartbeat deadlines,
    and [async_deadline] is the pending event for the earliest one.
    [async_resolved] marks an attempt whose result the workflow already
    received because it cancelled the activity without waiting; its later
    operations are accepted and ignored, as a server ignores a completion
    the workflow no longer waits for. *)
and async_attempt = {
  async_seq : int64;
  async_request : activity_request;
  async_attempt_number : int;
  async_started_ms : int64;
  mutable last_heartbeat_ms : int64;
  mutable async_resolved : bool;
  mutable async_deadline : event option;
}

(** A future virtual-time action. Cancelling one only clears [live]; the
    queue drops dead events lazily when they reach the front. *)
and event = { at : int64; order : int; mutable live : bool; action : action }

(** The work performed when an event falls due. [Start_run] delivers the
    first activation of a retry run once its backoff has elapsed.
    [Settle_activity] delivers the outcome of an attempt that takes virtual
    time, or of a timeout that expires while an attempt runs or waits for
    its retry, at the virtual instant it occurs. [Async_deadline] expires a
    timeout of an asynchronously completed attempt, and [Run_timeout] and
    [Execution_timeout] a workflow run's or chain's timeout (#1160). *)
and action =
  | Fire_timer of run * int64
  | Retry_activity of run * int64 * activity_request * int
  | Settle_activity of run * int64 * activity_request * int * attempt_outcome
  | Async_deadline of run * async_attempt * activity_timeout
  | Start_run of run
  | Run_timeout of run
  | Execution_timeout of chain

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
  task_failure_mode : task_failure_mode;
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

(** A chain together with the environment that drives it. [started] is
    [false] when the start attached to a running chain under
    {!Conflict_use_existing}. *)
type handle = { environment : t; target : chain; started : bool }

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
    [Native_execution.protocol_failure] encodes a workflow failing with a
    [Cancelled] error as a canceled failure, which carries no application
    type, and every other error, a [Terminated]-category one included, as an
    application failure typed by {!Error.application_failure_type}. (An
    activity's error is always encoded as an application failure, so
    activity failures always carry that type.) *)
let child_error_type cause =
  match (Error.view cause).category with
  | `Cancelled -> None
  | `Activity | `Bridge | `Child_workflow | `Codec | `Defect | `Nexus
  | `Terminated | `Timeout | `Update | `Workflow ->
      Some (Error.application_failure_type cause)

(** The origin the native worker reports for a child failure caused by
    [cause] (#1215). [Native_execution.protocol_failure] serializes a
    workflow failure as a fresh, cause-less failure without the error's own
    origin: a canceled failure for a [Cancelled] error and an application
    failure for every other one. The parent classifies that first layer, so
    an origin [cause] received (a timeout, say) is not forwarded. *)
let child_origin cause =
  match (Error.view cause).category with
  | `Cancelled -> `Cancelled
  | `Activity | `Bridge | `Child_workflow | `Codec | `Defect | `Nexus
  | `Terminated | `Timeout | `Update | `Workflow ->
      `Application

(** Wraps [cause] as the activity or child-workflow failure a workflow
    observes, mirroring the native worker's translation of Core's wrapper
    failure ([runtime_error_of_failure]): the new [category], the cause's
    details, the innermost application type [error_type], and the given
    retryability. Retryability is decided by the caller because it follows
    Temporal's retry state: an activity whose retries ended, or a child
    whose retry policy ended, is non-retryable, while a child without a
    retry policy inherits its cause's flag. [origin], when given, is the
    classification of the cause's first non-wrapper layer and
    [last_heartbeat_details] the heartbeat payloads a timeout layer
    records, both kept apart from [details] (#1143). *)
let wrap ?origin ?last_heartbeat_details ~category ~error_type ~non_retryable
    ~prefix cause =
  let view = Error.view cause in
  Error.make ~non_retryable ?error_type ~details:view.details ?origin
    ?last_heartbeat_details ~category
    ~message:(prefix ^ view.message) ()

(** The error a workflow observes for a cancelled operation or run. Its
    origin is [`Cancelled], as the native worker classifies a received
    canceled failure (#1215). *)
let cancelled message =
  Error.make ~category:`Cancelled ~origin:`Cancelled ~message ()

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
    ?(task_failure_mode = Fail_run) ~workflows ~activities () =
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
      task_failure_mode;
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
let namespace environment = environment.namespace

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
    but is not queued until its [Start_run] event starts it, and a run whose
    workflow task is failing ([stuck]) keeps it for the retried task. *)
let push_job environment run job =
  if not run.closed then begin
    run.jobs <- job :: run.jobs;
    if run.started && (not run.queued) && Option.is_none run.stuck then begin
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
    retryability, as Core reports a child without a retry policy.
    [timed_out] and [terminated] mark a chain the server itself closed by
    a workflow timeout or a termination (#1160): Core reports those as a
    child failure whose cause is a timeout ([`Timeout `Start_to_close],
    retryable unless the retry policy was exhausted) or a termination
    (non-retryable), neither carrying an application type. *)
let rec close_chain ?(cancelled = false) ?(retries_exhausted = false)
    ?(timed_out = false) ?(terminated = false) environment chain outcome =
  if Option.is_none chain.outcome then begin
    chain.outcome <- Some outcome;
    Option.iter (fun event -> event.live <- false) chain.execution_event;
    chain.execution_event <- None;
    match chain.parent with
    | Some (parent, seq) when not parent.closed ->
        Hashtbl.remove parent.children seq;
        let prefix =
          Printf.sprintf "child workflow %s failed: " chain.workflow_id
        in
        let result =
          match outcome with
          | Ok payload -> Ok payload
          | Error error when cancelled -> Error error
          | Error error when timed_out ->
              Error
                (wrap ~category:`Child_workflow
                   ~origin:(`Timeout `Start_to_close) ~error_type:None
                   ~non_retryable:retries_exhausted ~prefix error)
          | Error error when terminated ->
              Error
                (wrap ~category:`Child_workflow ~origin:`Terminated
                   ~error_type:None ~non_retryable:true ~prefix error)
          | Error error ->
              Error
                (wrap ~category:`Child_workflow ~origin:(child_origin error)
                   ~error_type:(child_error_type error)
                   ~non_retryable:
                     (retries_exhausted || (Error.view error).non_retryable)
                   ~prefix error)
        in
        push_job environment parent
          (Activation.Resolve_child_workflow { seq; result })
    | Some _ | None -> ()
  end

(** Marks [run] closed, discards its pending timers, retries, timeouts,
    asynchronous attempts (whose handles then report the activity gone),
    failing workflow task, and jobs, and applies the parent-close policy to
    its open children in workflow-ID order: the default and
    [Parent_terminate] terminate them, [Parent_request_cancel] delivers a
    cancellation, and [Parent_abandon] leaves them running. *)
and close_run environment run =
  if not run.closed then begin
    run.closed <- true;
    run.jobs <- [];
    run.stuck <- None;
    Option.iter (fun event -> event.live <- false) run.start_event;
    run.start_event <- None;
    Option.iter (fun event -> event.live <- false) run.run_timeout_event;
    run.run_timeout_event <- None;
    Hashtbl.iter
      (fun _ entry ->
        Option.iter (fun event -> event.live <- false) entry.async_deadline;
        entry.async_deadline <- None)
      run.async_attempts;
    Hashtbl.reset run.async_attempts;
    Hashtbl.iter (fun _ event -> event.live <- false) run.timers;
    Hashtbl.reset run.timers;
    Hashtbl.iter (fun _ event -> event.live <- false) run.activity_retries;
    Hashtbl.reset run.activity_retries;
    Hashtbl.iter (fun _ event -> event.live <- false) run.activity_attempts;
    Hashtbl.reset run.activity_attempts;
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
              close_chain ~terminated:true environment child
                (Error
                   (Error.make ~non_retryable:true ~category:`Terminated
                      ~origin:`Terminated
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
    workflow task by the retry backoff. [memo] and [search_attributes] (both
    empty by default) are the maps the run starts with; [Workflow.start_metadata]
    reports an empty one as absent, as for a start that sets none.

    The execution is built by [make_execution], which captures the run's
    identity, seed and start snapshot once and decodes a fresh copy of the
    start input on every call, so a retried workflow task (#1160) rebuilds
    an identical execution that never sees a mutation an earlier execution
    made to its decoded input. A chain run timeout starts the
    run's [Run_timeout] event, measured, as the server measures it, from the
    run's creation plus its first-task delay. *)
let create_run ?(attempt = 1) ?delay_ms
    ?(initiator = Protocol.Continue_as_new_workflow) ?(memo = [])
    ?(search_attributes = []) environment (chain : chain) ~workflow_type ~input =
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
          let randomness_seed = string_of_int environment.next_run in
          if String.equal chain.first_run_id "" then
            chain.first_run_id <- run_id;
          let start_metadata =
            {
              Workflow_context_store.memo =
                (match memo with [] -> None | _ :: _ -> Some memo);
              search_attributes =
                (match search_attributes with
                | [] -> None
                | _ :: _ -> Some search_attributes);
              execution_expiration_time =
                Option.map protocol_timestamp chain.execution_deadline_ms;
            }
          in
          let run_info =
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
              (* The chain's start options or a continue-as-new set these
                 timeouts and the retry policy; unset ones stay absent, as
                 for a server start without them. *)
              execution_timeout =
                Option.map protocol_duration chain.execution_timeout;
              run_timeout = Option.map protocol_duration chain.run_timeout;
              task_timeout = Option.map protocol_duration chain.task_timeout;
              retry_policy = Option.map protocol_retry_policy chain.retry_policy;
              (* The simulator has no cron schedules or start delays
                 (#1158): every run's first task is dispatched at once. *)
              first_workflow_task_backoff = None;
              cron_schedule = None;
              (* [chain.current] still names the predecessor here: it is
                 replaced only after this run is built. *)
              continued_from =
                Option.map
                  (fun (previous : run) -> (previous.run_id, initiator))
                  chain.current;
            }
          in
          let task_queue = chain.task_queue in
          let build decoded =
            let execution =
              Execution.start ~task_queue ~namespace:environment.namespace
                ~randomness_seed ~signal_handlers ~query_handlers
                ~update_handlers definition decoded
            in
            Execution.set_start_metadata execution (Some start_metadata);
            Execution.set_run_info execution (Some run_info);
            Execution execution
          in
          (* Decodes a copy of the payload, so even a codec that aliases the
             payload bytes yields an input no earlier execution has seen. *)
          let make_execution () =
            match
              Codec.decode (Definition.input definition)
                { input with data = Bytes.copy input.data }
            with
            | Ok decoded -> Ok (build decoded)
            | Error error -> Error error
            | exception exn ->
                Error
                  (defect
                     ("workflow input decoder raised: " ^ Printexc.to_string exn))
          in
          let run =
            {
              run_id;
              workflow_type;
              chain;
              execution = build decoded;
              make_execution;
              history = [];
              stuck = None;
              run_timeout_event = None;
              async_attempts = Hashtbl.create 4;
              attempt;
              input;
              start_memo = memo;
              start_search_attributes = search_attributes;
              memo;
              search_attributes;
              started = Option.is_none delay_ms;
              start_event = None;
              jobs = [];
              queued = false;
              closed = false;
              history_length = 0;
              timers = Hashtbl.create 8;
              activities = Hashtbl.create 8;
              activity_retries = Hashtbl.create 8;
              activity_attempts = Hashtbl.create 8;
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
          Option.iter
            (fun timeout_ms ->
              run.run_timeout_event <-
                Some
                  (schedule environment
                     ~delay_ms:
                       (saturating_add
                          (Option.value delay_ms ~default:0L)
                          timeout_ms)
                     (Run_timeout run)))
            chain.run_timeout;
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
    [task_queue] defaults to the environment's queue. The timeouts are in
    milliseconds; an execution timeout starts the chain's
    [Execution_timeout] event, which no retry or continue-as-new resets.
    [memo] and [search_attributes] are the first run's start maps. *)
let new_chain ?retry_policy ?task_queue ?execution_timeout ?run_timeout
    ?task_timeout ?memo ?search_attributes environment ~workflow_id
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
      execution_timeout;
      execution_deadline_ms =
        Option.map (saturating_add environment.now_ms) execution_timeout;
      run_timeout;
      task_timeout;
      execution_event = None;
    }
  in
  let* _run =
    create_run ?memo ?search_attributes environment chain ~workflow_type ~input
  in
  chain.execution_event <-
    Option.map
      (fun delay_ms -> schedule environment ~delay_ms (Execution_timeout chain))
      execution_timeout;
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
    the engine never routes its own cancellations through this function.
    [application] (default [true]) says [error] is an application failure;
    a timeout is not, so the policy's non-retryable error types, which
    name application failure types, do not apply to it. *)
let retry_delay ?(application = true) environment policy ~attempt error =
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
    else if
      application
      && List.mem (Error.application_failure_type error) non_retryable
    then
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

(** Starts the retry run that follows [run] after [delay_ms]: the same
    workflow type and input with [attempt + 1], linked by a retry
    continuation, whose first activation waits out the backoff. The parent
    is not told, exactly as with server-side child retries. A retry run
    starts from the original start request, so it gets the start maps, not
    the failed run's upserts. *)
let retry_run environment run ~delay_ms =
  match
    create_run ~attempt:(run.attempt + 1) ~delay_ms
      ~initiator:Protocol.Continue_as_new_retry ~memo:run.start_memo
      ~search_attributes:run.start_search_attributes environment run.chain
      ~workflow_type:run.workflow_type ~input:run.input
  with
  | Ok _ -> ()
  | Error error -> close_chain environment run.chain (Error error)

(** The error a workflow run or execution timeout closes a chain with
    (#1160). It carries origin [`Timeout `Start_to_close], the timeout type
    Temporal records for a workflow that timed out, in category [Timeout]. *)
let workflow_timeout_error scope =
  Error.make ~category:`Timeout ~origin:(`Timeout `Start_to_close)
    ~message:("workflow " ^ scope ^ " timeout expired")
    ()

(** Expires [run]'s run timeout (#1160). The run closes; a chain with a
    retry policy retries it as the server retries a timed-out run (the
    policy's non-retryable error types do not apply to a timeout), and
    otherwise the chain closes as timed out. *)
let time_out_run environment run =
  close_run environment run;
  let error = workflow_timeout_error "run" in
  match run.chain.retry_policy with
  | None -> close_chain ~timed_out:true environment run.chain (Error error)
  | Some _ as policy -> (
      match
        retry_delay ~application:false environment policy ~attempt:run.attempt
          error
      with
      | Some delay_ms -> retry_run environment run ~delay_ms
      | None ->
          close_chain ~timed_out:true ~retries_exhausted:true environment
            run.chain (Error error))

(** Expires [chain]'s execution timeout (#1160): the current run closes and
    the chain closes as timed out, without a retry, as the server ends a
    workflow whose execution timeout expired. *)
let time_out_execution environment chain =
  if Option.is_none chain.outcome then begin
    close_run environment (current chain);
    close_chain ~timed_out:true environment chain
      (Error (workflow_timeout_error "execution"))
  end

(** Terminates [chain]'s current run, as a server termination does: the run
    closes with a non-retryable [Terminated] error, and a parent observes a
    child failure caused by the termination. *)
let terminate_chain environment chain ~reason =
  if Option.is_none chain.outcome then begin
    close_run environment (current chain);
    close_chain ~terminated:true environment chain
      (Error
         (Error.make ~non_retryable:true ~category:`Terminated
            ~origin:`Terminated ~message:reason ()))
  end

(** Closes [run] and, when it is its chain's current run, either retries the
    chain under its retry policy ({!retry_run}) or closes the chain. *)
let finish environment run closure outcome =
  close_run environment run;
  if current run.chain == run then
    let chain = run.chain in
    match (closure, outcome, chain.retry_policy) with
    | Run_failed, Error error, Some _
      when (match (Error.view error).category with
            | `Cancelled -> true
            | _ -> false) ->
        (* The native worker encodes such a failure as a canceled failure,
           which Temporal Server never retries under a workflow retry policy
           (#1215). A [Terminated]-category error is encoded as an ordinary
           application failure and is retried; a real termination closes the
           chain through [close_run] instead. *)
        close_chain environment chain outcome
    | Run_failed, Error error, (Some _ as policy) -> (
        match retry_delay environment policy ~attempt:run.attempt error with
        | Some delay_ms -> retry_run environment run ~delay_ms
        | None -> close_chain ~retries_exhausted:true environment chain outcome)
    | Run_cancelled, _, _ -> close_chain ~cancelled:true environment chain outcome
    | (Run_completed | Run_failed | Task_failed), _, _ ->
        close_chain environment chain outcome

(** What one activity attempt's callback produced: an outcome to settle
    ([Ran]), or an asynchronous hand-off whose outcome a test supplies later
    through the attempt's handle ([Handed_off], #1160). *)
type attempt_result = Ran of (Codec.payload, Error.t) result | Handed_off of async_attempt

(** Runs one attempt of [request] (command sequence [seq]) synchronously,
    outside every workflow context. Failures are classified as the native
    activity adapter classifies them (#1031, #1215):

    - an unregistered type fails as ["NotFoundError"], and a bad argument
      list or an input the codec cannot decode (or that makes it raise) as
      {!native_dispatch_failure_type}. These are retryable for a remote
      activity, so its retry policy decides, as a later attempt could reach
      another worker, and non-retryable for a local one, which always runs
      here;
    - an unsupported definition (a remote reference), a definition without
      an implementation, and an output codec failure fail non-retryably
      with {!native_dispatch_failure_type};
    - an exception from the callback itself fails non-retryably as
      [ocaml_exception].

    A local activity's context has no heartbeat hand-off: its heartbeats are
    accepted and dropped, and every attempt sees no details, because Core
    builds every local attempt with empty details and drops heartbeats for
    local tokens (#1221).

    An asynchronous callback (#1160) gets a fresh dormant handle whose
    operations go to [submit] with this attempt's record. As in the native
    adapter, a [Will_complete_async] outcome for a remote activity reserves
    and activates that handle and the attempt is [Handed_off]; a local
    activity cannot be completed asynchronously and fails non-retryably;
    every other outcome closes the handle if it is still dormant, so a
    retained copy reports a non-retryable error. *)
let attempt_activity environment run seq request ~attempt ~submit =
  let failure ?(error_type = native_dispatch_failure_type)
      ?(non_retryable = true) message =
    Error (Error.make ~non_retryable ~error_type ~category:`Activity ~message ())
  in
  (* A task another worker might run fails retryably when it is remote. *)
  let unrunnable ?error_type message =
    failure ?error_type ~non_retryable:request.is_local message
  in
  let decode_input input_codec =
    let* payload =
      match request.arguments with
      | [] -> Ok (Payload.unit_null ())
      | [ payload ] -> Ok payload
      | _ ->
          unrunnable "activity definitions currently accept exactly one input value"
    in
    match Codec.decode input_codec payload with
    | Ok input -> Ok input
    | Error error ->
        unrunnable ("activity input decoding failed: " ^ Error.message error)
    | exception exn ->
        unrunnable ("activity input decoder raised: " ^ Printexc.to_string exn)
  in
  let encode_output output_codec output =
    match Codec.encode output_codec output with
    | Ok payload -> Ok payload
    | Error error ->
        failure ("activity output encoding failed: " ^ Error.message error)
    | exception exn ->
        failure ("activity output encoder raised: " ^ Printexc.to_string exn)
  in
  let milliseconds = Option.map Temporal_base.Duration.of_ms in
  let info () : Activity_context.info =
    let now = Some (activity_timestamp environment.now_ms) in
    {
      namespace = environment.namespace;
      workflow_id = run.chain.workflow_id;
      workflow_run_id = run.run_id;
      workflow_type = run.workflow_type;
      activity_id = request.activity_id;
      activity_type = request.activity_type;
      attempt;
      is_local = request.is_local;
      scheduled_time = Some (activity_timestamp request.scheduled_ms);
      current_attempt_scheduled_time = now;
      started_time = now;
      schedule_to_close_timeout = milliseconds request.schedule_to_close_timeout;
      start_to_close_timeout = milliseconds request.start_to_close_timeout;
      task_heartbeat_timeout = milliseconds request.heartbeat_timeout;
    }
  in
  let raised exn =
    failure ~error_type:"ocaml_exception"
      ("activity callback raised: " ^ Printexc.to_string exn)
  in
  match Name_map.find_opt request.activity_type environment.activities with
  | None ->
      Ran
        (unrunnable ~error_type:"NotFoundError"
           (Printf.sprintf
              "activity type %s is not registered with the test environment"
              request.activity_type))
  | Some (Unsupported reason) -> Ran (failure reason)
  | Some (Code (definition, _)) -> (
      match Definition.implementation definition with
      | None ->
          Ran
            (failure
               (Printf.sprintf "activity %s has no implementation"
                  request.activity_type))
      | Some implementation -> (
          match decode_input (Definition.input definition) with
          | Error error -> Ran (Error error)
          | Ok input ->
              let info = info () in
              let heartbeat_timeout = milliseconds request.heartbeat_timeout in
              let context =
                if request.is_local then
                  Activity_context.create_with_info ~info
                    ~heartbeat:(fun _ -> Ok ())
                    ~details:[] ~heartbeat_timeout
                else
                  Activity_context.create_with_info ~info
                    ~heartbeat:(fun details ->
                      request.heartbeat_details <- details;
                      Ok ())
                    ~details:request.heartbeat_details ~heartbeat_timeout
              in
              let result =
                match implementation context input with
                | result -> result
                | exception exn -> raised exn
              in
              Activity_context.invalidate context;
              Ran
                (Result.bind result
                   (encode_output (Definition.output definition)))))
  | Some (Async_code (definition, _)) -> (
      match Definition.implementation definition with
      | None ->
          Ran
            (failure
               (Printf.sprintf "activity %s has no implementation"
                  request.activity_type))
      | Some implementation -> (
          match decode_input (Definition.input definition) with
          | Error error -> Ran (Error error)
          | Ok input ->
              let entry =
                {
                  async_seq = seq;
                  async_request = request;
                  async_attempt_number = attempt;
                  async_started_ms = environment.now_ms;
                  last_heartbeat_ms = environment.now_ms;
                  async_resolved = false;
                  async_deadline = None;
                }
              in
              let output_codec = Definition.output definition in
              let handle =
                Async_activity.create ~submit:(submit entry)
                  ~encode_output:(fun output -> Codec.encode output_codec output)
                  ()
              in
              let context = Async_activity.context ~info:(info ()) handle in
              let context_handle = Async_activity.handle context in
              Fun.protect
                ~finally:(fun () -> Async_activity.close_if_dormant context_handle)
                (fun () ->
                  match implementation context input with
                  | exception exn -> Ran (raised exn)
                  | Async_activity.Completed output ->
                      Ran (encode_output output_codec output)
                  | Async_activity.Failed error -> Ran (Error error)
                  | Async_activity.Will_complete_async _ when request.is_local ->
                      ignore (Async_activity.close context_handle);
                      Ran
                        (failure
                           "local activities cannot be completed asynchronously")
                  | Async_activity.Will_complete_async returned -> (
                      match
                        Result.bind
                          (Async_activity.prepare_handoff ~expected:context_handle
                             returned)
                          (fun () -> Async_activity.activate returned)
                      with
                      | Ok () -> Handed_off entry
                      | Error error ->
                          Ran
                            (failure
                               ("asynchronous activity handoff failed: "
                              ^ Error.message error))))))

(** The virtual milliseconds attempt [attempt] of [request] takes: the
    registration's entry for that attempt, its last entry for every later
    attempt, and zero for a registration without durations or a type the
    engine cannot run. An asynchronous attempt that is handed off lasts
    until its handle finishes it instead. *)
let attempt_duration_ms environment request ~attempt =
  let rec pick attempt = function
    | [] -> 0L
    | [ last ] -> last
    | duration :: rest -> if attempt <= 1 then duration else pick (attempt - 1) rest
  in
  match Name_map.find_opt request.activity_type environment.activities with
  | Some (Code (_, durations)) | Some (Async_code (_, durations)) ->
      pick attempt durations
  | Some (Unsupported _) | None -> 0L

(** A configured activity timeout, with zero read as unset: Temporal
    treats a zero timeout as absent, and the native activity adapter
    normalizes it to [None] the same way. *)
let configured_timeout = function
  | Some milliseconds when Int64.compare milliseconds 0L > 0 -> Some milliseconds
  | Some _ | None -> None

(** The virtual instant at which [request]'s schedule-to-close timeout
    expires, measured from the first attempt's scheduling as on a server. *)
let schedule_to_close_deadline request =
  Option.map
    (saturating_add request.scheduled_ms)
    (configured_timeout request.schedule_to_close_timeout)

(** The error an expired activity timeout produces, in the shape the native
    worker decodes from Temporal's [TimeoutFailure] (#1143): category
    [Activity], no application type, origin [`Timeout timeout], and the
    details of the last heartbeat any attempt recorded. *)
let activity_timeout_error request timeout =
  let name =
    match timeout with
    | `Start_to_close -> "start-to-close"
    | `Schedule_to_close -> "schedule-to-close"
    | `Heartbeat -> "heartbeat"
  in
  Error.make ~category:`Activity
    ~origin:(`Timeout (timeout :> Error.timeout_type))
    ~last_heartbeat_details:request.heartbeat_details
    ~message:(name ^ " timeout expired")
    ()

(** Schedules the earliest pending timeout of an asynchronously completed
    attempt (#1160), replacing any earlier deadline event: schedule-to-close
    from the first attempt's scheduling, start-to-close from the attempt's
    start, and heartbeat from its latest heartbeat (or start), so every
    heartbeat moves that deadline as a server's heartbeat timer does.
    Simultaneous deadlines prefer schedule-to-close, then start-to-close,
    then heartbeat, as for a synchronous attempt. *)
let schedule_async_deadline environment run entry =
  Option.iter (fun event -> event.live <- false) entry.async_deadline;
  let request = entry.async_request in
  let from at timeout = Option.map (saturating_add at) (configured_timeout timeout) in
  let earliest =
    List.fold_left
      (fun earliest (timeout, deadline) ->
        match (deadline, earliest) with
        | None, _ -> earliest
        | Some at, Some (_, earliest_at) when Int64.compare at earliest_at >= 0 ->
            earliest
        | Some at, _ -> Some (timeout, at))
      None
      [
        (`Schedule_to_close, schedule_to_close_deadline request);
        (`Start_to_close, from entry.async_started_ms request.start_to_close_timeout);
        (`Heartbeat, from entry.last_heartbeat_ms request.heartbeat_timeout);
      ]
  in
  entry.async_deadline <-
    Option.map
      (fun (timeout, at) ->
        schedule environment
          ~delay_ms:(Int64.max 0L (Int64.sub at environment.now_ms))
          (Async_deadline (run, entry, timeout)))
      earliest

(** Runs attempt [attempt] of the activity identified by [seq] and settles
    its outcome, at once when the attempt takes zero virtual time and no
    timeout has expired, and otherwise through a [Settle_activity] event.

    The callback runs when the attempt starts, so its heartbeats are all
    recorded at that instant, and the attempt then lasts its registered
    duration. A timeout expires when its deadline falls strictly before the
    attempt would finish: start-to-close and heartbeat deadlines are
    measured from the attempt's start, schedule-to-close from the first
    attempt's scheduling. The earliest deadline wins, and simultaneous
    deadlines prefer schedule-to-close, then start-to-close, then
    heartbeat. Local activities have no heartbeat timeout. An expired
    attempt's own result is discarded, as a server discards a late
    completion. An attempt handed off asynchronously instead waits for its
    handle, bounded by {!schedule_async_deadline}. *)
let rec run_activity environment run seq request ~attempt =
  let started = environment.now_ms in
  match
    attempt_activity environment run seq request ~attempt
      ~submit:(async_submit environment run)
  with
  | Handed_off entry ->
      Hashtbl.replace run.async_attempts seq entry;
      schedule_async_deadline environment run entry
  | Ran result -> (
      let duration = attempt_duration_ms environment request ~attempt in
      let finished = saturating_add started duration in
      let from_start timeout =
        Option.map (saturating_add started) (configured_timeout timeout)
      in
      let deadlines =
        [
          (`Schedule_to_close, schedule_to_close_deadline request);
          (`Start_to_close, from_start request.start_to_close_timeout);
          ( `Heartbeat,
            if request.is_local then None
            else from_start request.heartbeat_timeout );
        ]
      in
      let expiry =
        List.fold_left
          (fun earliest (timeout, deadline) ->
            match (deadline, earliest) with
            | None, _ -> earliest
            | Some at, _ when Int64.compare at finished >= 0 -> earliest
            | Some at, Some (_, earliest_at)
              when Int64.compare at earliest_at >= 0 ->
                earliest
            | Some at, _ -> Some (timeout, at))
          None deadlines
      in
      let settle_at at result =
        let event =
          schedule environment
            ~delay_ms:(Int64.sub at environment.now_ms)
            (Settle_activity (run, seq, request, attempt, result))
        in
        Hashtbl.replace run.activity_attempts seq event
      in
      match expiry with
      | Some (timeout, at) -> settle_at at (Timed_out timeout)
      | None when Int64.equal duration 0L ->
          settle_activity environment run seq request ~attempt (Finished result)
      | None -> settle_at finished (Finished result))

(** Settles one attempt's outcome: queues a success, schedules the next
    attempt after the retry delay, or queues the final failure. A final
    failure is wrapped as an [Activity] error and is non-retryable, as the
    native worker reports a failure whose retry state is
    [NON_RETRYABLE_FAILURE] or [MAXIMUM_ATTEMPTS_REACHED]. A callback's
    error becomes an application failure with origin [`Application] and its
    application type; an engine timeout keeps origin [`Timeout t], no type,
    and the last heartbeat details, and is retried without consulting the
    policy's non-retryable error types. An expired schedule-to-close
    timeout is final at once and stays retryable, as Temporal reports retry
    state [TIMEOUT]; a retry that would start at or after that deadline is
    replaced by the timeout, which a backoff wait observes at the deadline.
    After a cancellation request the engine waited out
    ([cancel_requested]), a failure is final and retryable, as Temporal
    records retry state [CANCEL_REQUESTED] instead of retrying.
    An error with the [Cancelled] category returned by the activity is
    wrapped too: only {!cancel_activity} produces a workflow-visible
    activity cancellation. *)
and settle_activity environment run seq request ~attempt outcome =
  let prefix = Printf.sprintf "activity %s failed: " request.activity_type in
  let resolve result =
    push_job environment run (Activation.Resolve_activity { seq; result })
  in
  let retry_or_final ~application ~final error =
    match
      retry_delay ~application environment request.retry_policy ~attempt error
    with
    | _ when request.cancel_requested -> final ~non_retryable:false
    | None -> final ~non_retryable:true
    | Some delay_ms -> (
        let retry_at = saturating_add environment.now_ms delay_ms in
        match schedule_to_close_deadline request with
        | Some deadline when Int64.compare retry_at deadline >= 0 ->
            let event =
              schedule environment
                ~delay_ms:(Int64.sub deadline environment.now_ms)
                (Settle_activity
                   (run, seq, request, attempt, Timed_out `Schedule_to_close))
            in
            Hashtbl.replace run.activity_retries seq event
        | Some _ | None ->
            let event =
              schedule environment ~delay_ms
                (Retry_activity (run, seq, request, attempt + 1))
            in
            Hashtbl.replace run.activity_retries seq event)
  in
  match outcome with
  | Finished (Ok payload) -> resolve (Ok payload)
  | Finished (Error error) ->
      let final ~non_retryable =
        resolve
          (Error
             (wrap ~category:`Activity ~non_retryable ~origin:`Application
                ~error_type:(Some (Error.application_failure_type error))
                ~prefix error))
      in
      retry_or_final ~application:true ~final error
  | Timed_out timeout ->
      let error = activity_timeout_error request timeout in
      let final ~non_retryable =
        resolve
          (Error
             (wrap ~category:`Activity ~non_retryable
                ~origin:(`Timeout (timeout :> Error.timeout_type))
                ~last_heartbeat_details:request.heartbeat_details
                ~error_type:None ~prefix error))
      in
      if timeout = `Schedule_to_close then final ~non_retryable:false
      else retry_or_final ~application:false ~final error

(** Applies one operation a test submitted through an asynchronous
    attempt's handle (#1160), as Temporal Server answers the matching
    request. The operation runs on the caller's thread and only records the
    outcome; the workflow observes it when the environment next runs.

    - When the environment is shut down, the run closed, or [entry] is no
      longer the attempt pending for its sequence (it completed, timed out,
      or was superseded by a retry), the server would report the activity
      gone: the result is a terminal submission error, which retires the
      handle.
    - A heartbeat records its details for the next attempt and the timeout
      error, and restarts the heartbeat timeout. After the workflow
      requested cancellation it reports that request as an
      [Activity_state] error, as the native heartbeat path does; the handle
      stays live.
    - Cancelling without a cancellation request is a definitive rejection
      that leaves the handle live, as on a server.
    - [Complete], [Fail], and an accepted [Cancel] retire the attempt and
      settle it like a synchronous outcome, so a failure is retried under
      the retry policy. An attempt the workflow already stopped waiting for
      ([async_resolved]) is accepted without effect. *)
and async_submit environment run entry operation =
  let seq = entry.async_seq in
  let request = entry.async_request in
  let pending =
    (not environment.shut_down) && (not run.closed)
    &&
    match Hashtbl.find_opt run.async_attempts seq with
    | Some current -> current == entry
    | None -> false
  in
  let retire () =
    Hashtbl.remove run.async_attempts seq;
    Option.iter (fun event -> event.live <- false) entry.async_deadline;
    entry.async_deadline <- None
  in
  let settle outcome =
    retire ();
    if not entry.async_resolved then
      settle_activity environment run seq request
        ~attempt:entry.async_attempt_number outcome;
    Ok ()
  in
  if not pending then
    Error
      (Async_activity.Terminal_submission
         (Error.make ~non_retryable:true ~category:`Activity
            ~message:
              "asynchronous activity is no longer pending in the test \
               environment"
            ()))
  else
    match (operation : Async_activity.operation) with
    | Async_activity.Heartbeat details ->
        request.heartbeat_details <- details;
        entry.last_heartbeat_ms <- environment.now_ms;
        schedule_async_deadline environment run entry;
        if request.cancel_requested then
          Error
            (Async_activity.Activity_state
               (Error.make ~non_retryable:true ~category:`Cancelled
                  ~message:"activity cancellation requested: cancelled" ()))
        else Ok ()
    | Async_activity.Cancel _ when not request.cancel_requested ->
        Error
          (Async_activity.Rejected_submission
             (Error.make ~non_retryable:true ~category:`Activity
                ~message:"activity cancellation was not requested" ()))
    | Async_activity.Cancel details ->
        retire ();
        if not entry.async_resolved then
          push_job environment run
            (Activation.Resolve_activity
               {
                 seq;
                 result =
                   Error
                     (Error.make ~category:`Cancelled ~origin:`Cancelled
                        ~details ~message:"activity cancelled" ());
               });
        Ok ()
    | Async_activity.Complete payload -> settle (Finished (Ok payload))
    | Async_activity.Fail error -> settle (Finished (Error error))

(** Applies a workflow's activity cancellation request. An activity waiting
    for a retry is cancelled at once. An attempt that already finished but
    whose result has not been delivered is reported as cancelled unless the
    workflow asked to wait for cancellation to complete, in which case the
    real result stands because the attempt cannot observe the request. The
    same rule covers an attempt still running in virtual time: it is
    cancelled at once, or under [Wait_cancellation_completed] its outcome
    (a result or an expired timeout) is delivered when it occurs.

    An asynchronously completed attempt (#1160) learns of the request from
    its next heartbeat. Under [Wait_cancellation_completed] the workflow
    waits for its handle's outcome; [Try_cancel] reports the cancellation at
    once while the server-side request still reaches the handle; [Abandon]
    reports it at once and sends no request. *)
let cancel_activity environment (run : run) seq =
  let result = Error (cancelled "activity cancelled") in
  let waits_for_completion () =
    match Hashtbl.find_opt run.activities seq with
    | Some { cancellation_type = Activation.Wait_cancellation_completed; _ } ->
        true
    | Some _ | None -> false
  in
  match Hashtbl.find_opt run.async_attempts seq with
  | Some entry when entry.async_resolved -> ()
  | Some entry -> (
      let request = entry.async_request in
      match request.cancellation_type with
      | Activation.Wait_cancellation_completed -> request.cancel_requested <- true
      | Activation.Try_cancel ->
          request.cancel_requested <- true;
          entry.async_resolved <- true;
          push_job environment run (Activation.Resolve_activity { seq; result })
      | Activation.Abandon ->
          entry.async_resolved <- true;
          push_job environment run (Activation.Resolve_activity { seq; result }))
  | None -> (
  match
    (Hashtbl.find_opt run.activity_retries seq,
     Hashtbl.find_opt run.activity_attempts seq)
  with
  | Some event, _ ->
      event.live <- false;
      Hashtbl.remove run.activity_retries seq;
      push_job environment run (Activation.Resolve_activity { seq; result })
  | None, Some _ when waits_for_completion () -> (
      match Hashtbl.find_opt run.activities seq with
      | Some request -> request.cancel_requested <- true
      | None -> ())
  | None, Some event ->
      event.live <- false;
      Hashtbl.remove run.activity_attempts seq;
      push_job environment run (Activation.Resolve_activity { seq; result })
  | None, None -> (
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
              run.jobs))

(** Converts a command's workflow argument list into the single payload an
    OCaml workflow decodes, as a native worker does: zero arguments (a
    [Codec.unit] input) become the canonical unit payload and one argument is
    used as is. OCaml workflows take exactly one input, so more is a defect. *)
let workflow_input_of_arguments = function
  | [] -> Ok (Payload.unit_null ())
  | [ payload ] -> Ok payload
  | _ -> Error (defect "workflow definitions accept exactly one input value")

(** Reports whether [policy] refuses a new start for the ID of the closed
    [chain], as Temporal applies a workflow ID reuse policy (#1160): the
    default allows any reuse, [Reuse_allow_duplicate_failed_only] refuses
    only after a run that completed successfully, and
    [Reuse_reject_duplicate] refuses every reuse. An open chain is the
    conflict policy's business, never the reuse policy's. *)
let reuse_refused (policy : Activation.child_workflow_id_reuse_policy option)
    (chain : chain) =
  match (policy, chain.outcome) with
  | _, None -> false
  | (None | Some Activation.Reuse_allow_duplicate), Some _ -> false
  | Some Activation.Reuse_allow_duplicate_failed_only, Some outcome ->
      Result.is_ok outcome
  | Some Activation.Reuse_reject_duplicate, Some _ -> true

(** Starts a child chain for a [Start_child_workflow] command and queues the
    start acknowledgement. A running workflow ID, a closed one the
    [id_reuse_policy] refuses, an unregistered type, or an input the child
    cannot decode fails the start instead. The child's [retry_policy]
    governs its runs as on a server, its execution and run timeouts are
    enforced in virtual time (#1160), and [task_queue] and [task_timeout]
    are reported by its [Workflow.info]; every registered workflow runs
    regardless of the queue, as if a worker polled each one. [memo] and
    [search_attributes] (#1135) become the child's start maps, read back by
    its [Workflow.start_metadata] as on a server. *)
let start_child environment run ~seq ~id ~name ~input ~parent_close_policy
    ~retry_policy ~task_queue ~execution_timeout ~run_timeout ~task_timeout
    ~id_reuse_policy ~memo ~search_attributes =
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
  let conflicting =
    match Hashtbl.find_opt environment.chains id with
    | Some chain -> Option.is_none chain.outcome || reuse_refused id_reuse_policy chain
    | None -> false
  in
  if conflicting then start_failed "workflow_already_exists"
  else
    match
      Result.bind (workflow_input_of_arguments input) (fun input ->
          new_chain ?retry_policy ?task_queue ?execution_timeout ?run_timeout
            ?task_timeout ?memo ?search_attributes environment ~workflow_id:id
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

(** Installs the per-activation facts Core sends with every activation:
    its virtual timestamp, replay status, and history length. *)
let stamp_activation execution ~at_ms ~replaying ~history_length =
  Execution.set_activation_timestamp execution (Some (protocol_timestamp at_ms));
  Execution.set_activation_is_replaying execution replaying;
  Execution.set_activation_history execution
    {
      Workflow_context_store.history_length;
      history_size_bytes = None;
      continue_as_new_suggested = false;
      continue_as_new_reasons = [];
    }

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
          cancel_requested = false;
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
          cancel_requested = false;
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
      {
        seq;
        id;
        name;
        input;
        parent_close_policy;
        retry_policy;
        task_queue;
        execution_timeout;
        run_timeout;
        task_timeout;
        id_reuse_policy;
        memo;
        search_attributes;
        _;
      } ->
      start_child environment run ~seq ~id ~name ~input ~parent_close_policy
        ~retry_policy ~task_queue ~execution_timeout ~run_timeout ~task_timeout
        ~id_reuse_policy ~memo ~search_attributes
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
  | Set_patch_marker _ -> ()
  | Upsert_search_attributes { search_attributes } ->
      run.search_attributes <-
        Workflow_context_store.merge_upsert run.search_attributes search_attributes
  | Modify_workflow_properties { upserted_memo } ->
      run.memo <- Workflow_context_store.merge_upsert run.memo upserted_memo
  | Complete_workflow payload -> finish environment run Run_completed (Ok payload)
  | Fail_workflow error -> finish environment run Run_failed (Error error)
  | Cancel_workflow_execution ->
      finish environment run Run_cancelled
        (Error (cancelled "workflow cancelled"))
  | Continue_as_new { workflow_type; input; memo; options } -> (
      close_run environment run;
      (* As Core does, an empty command memo reuses the start memo, and the
         successor inherits the current search attributes unless the command
         replaces them. Set routing, timeout and retry options replace the
         chain's for the successor and later runs; unset ones are inherited
         (#1153). The versioning behavior has no effect without deployments. *)
      let memo = match memo with [] -> run.start_memo | _ :: _ -> memo in
      let search_attributes =
        Option.value options.search_attributes ~default:run.search_attributes
      in
      let chain = run.chain in
      Option.iter (fun queue -> chain.task_queue <- queue) options.task_queue;
      Option.iter (fun timeout -> chain.run_timeout <- Some timeout) options.run_timeout;
      Option.iter (fun timeout -> chain.task_timeout <- Some timeout) options.task_timeout;
      Option.iter (fun policy -> chain.retry_policy <- Some policy) options.retry_policy;
      match
        Result.bind (workflow_input_of_arguments input) (fun input ->
            create_run ~memo ~search_attributes environment chain
              ~workflow_type ~input)
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
  | Upsert_search_attributes _ | Modify_workflow_properties _ ->
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
  | Set_patch_marker _ | Upsert_search_attributes _
  | Modify_workflow_properties _ | Complete_workflow _ | Fail_workflow _
  | Cancel_workflow_execution | Continue_as_new _ ->
      false

(** Delivers [jobs] to [run] in one activation stamped with the current
    virtual time and interprets the resulting commands in emission order.
    When the batch closes the run, the commands a server would never
    dispatch for a closed run ({!is_dispatched_by_server}) are dropped, so a
    test cannot observe an activity, child, or external signal that
    production never runs. [counts_toward_history] is [false] for a
    query-only activation, which Core answers without appending workflow
    history, so [Workflow.Info.history_length] does not grow with test
    queries.

    A workflow task failure (a poisoned activation, which emits no
    commands) is handled by the environment's mode. In {!Fail_run} mode it
    closes the run with that error, so the defect is reported to the test
    instead of being retried forever. In {!Retry_task} mode (#1160) the run
    stays open and [stuck] with the failure and the jobs, which a later
    {!retry_workflow_tasks} delivers again to a rebuilt execution; every
    other activation that counts toward history is recorded for that
    replay ({!record_activation}). *)
and activate ?(counts_toward_history = true) environment run jobs =
  let (Execution execution) = run.execution in
  if counts_toward_history then
    run.history_length <- run.history_length + List.length jobs + 2;
  stamp_activation execution ~at_ms:environment.now_ms ~replaying:false
    ~history_length:run.history_length;
  let commands = Execution.activate execution jobs in
  match Execution.task_failure execution with
  | Some error
    when (not run.closed) && counts_toward_history
         && environment.task_failure_mode = Retry_task ->
      run.stuck <- Some (error, jobs)
  | failure ->
      (match failure with
      | Some error when not run.closed ->
          finish environment run Task_failed (Error error)
      | Some _ | None -> ());
      let commands =
        if List.exists is_terminal_command commands then
          List.filter
            (fun command -> not (is_dispatched_by_server command))
            commands
        else commands
      in
      List.iter (apply_command environment run) commands;
      if
        counts_toward_history && Option.is_none failure
        && environment.task_failure_mode = Retry_task
      then record_activation environment run jobs commands

(** Appends a completed activation to [run]'s replay history, shaped as
    Core replays it: an update the validator rejected leaves no history and
    is dropped, an accepted one is replayed without running its validator
    again, and the patch markers the activation's [commands] recorded are
    kept so the replay can announce them. *)
and record_activation environment run jobs commands =
  let replayable =
    List.filter_map
      (function
        | Activation.Do_update update -> (
            match Hashtbl.find_opt run.update_results update.protocol_instance_id with
            | Some (Update_rejected _) -> None
            | Some (Update_accepted | Update_completed _) | None ->
                Some (Activation.Do_update { update with run_validator = false }))
        | job -> Some job)
      jobs
  in
  run.history <-
    {
      recorded_jobs = replayable;
      recorded_patches =
        List.filter_map
          (function
            | Activation.Set_patch_marker { patch_id; _ } -> Some patch_id
            | _ -> None)
          commands;
      recorded_at_ms = environment.now_ms;
      recorded_history_length = run.history_length;
    }
    :: run.history

(** Replaces [run]'s execution with a fresh one rebuilt from its history
    (#1160), as Core replays a run after a failed workflow task: each
    recorded activation is delivered again with its original timestamp and
    history length while the execution reports that it is replaying, and
    the commands it emits are discarded because they were already applied.
    An activation whose commands recorded patch markers is preceded by a
    [Notify_has_patch] job for each, placed after initialization as Core
    orders them, so [Workflow.patched] answers [true] on replay exactly as
    when the marker was recorded. The replay does not compare commands with
    the recorded ones, so nondeterminism is not detected here. Returns the
    task failure when the replay itself fails, leaving the rebuilt execution
    in place, or the input decoder's error, leaving the previous one. *)
and rebuild run =
  let* fresh = run.make_execution () in
  let (Execution previous) = run.execution in
  Execution.shutdown previous;
  run.execution <- fresh;
  let (Execution execution) = run.execution in
  let with_patches record =
    let notifications =
      List.map
        (fun patch_id -> Activation.Notify_has_patch { patch_id })
        record.recorded_patches
    in
    match record.recorded_jobs with
    | (Activation.Start_workflow as start) :: rest -> start :: (notifications @ rest)
    | jobs -> notifications @ jobs
  in
  let rec replay = function
    | [] -> Ok ()
    | record :: rest -> (
        stamp_activation execution ~at_ms:record.recorded_at_ms ~replaying:true
          ~history_length:record.recorded_history_length;
        ignore (Execution.activate execution (with_patches record));
        match Execution.task_failure execution with
        | Some error -> Error error
        | None -> replay rest)
  in
  replay (List.rev run.history)

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
    (* A run whose workflow task is failing keeps its jobs for the retry. *)
    if Option.is_none run.stuck then begin
      let jobs =
        List.rev run.jobs
        |> List.stable_sort (fun left right ->
               Int.compare (job_rank run left) (job_rank run right))
      in
      run.jobs <- [];
      if (not run.closed) && jobs <> [] then activate environment run jobs
    end
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
           | Settle_activity (run, seq, request, attempt, result) ->
               (* Exactly one table holds the event: the attempt table for
                  a running attempt, the retry table for a backoff that the
                  schedule-to-close deadline cut short. *)
               Hashtbl.remove run.activity_attempts seq;
               Hashtbl.remove run.activity_retries seq;
               if not run.closed then
                 settle_activity environment run seq request ~attempt result
           | Async_deadline (run, entry, timeout) -> (
               entry.async_deadline <- None;
               match Hashtbl.find_opt run.async_attempts entry.async_seq with
               | Some pending when pending == entry ->
                   Hashtbl.remove run.async_attempts entry.async_seq;
                   if (not entry.async_resolved) && not run.closed then
                     settle_activity environment run entry.async_seq
                       entry.async_request ~attempt:entry.async_attempt_number
                       (Timed_out timeout)
               | Some _ | None -> ())
           | Run_timeout run ->
               run.run_timeout_event <- None;
               if (not run.closed) && current run.chain == run then
                 time_out_run environment run
           | Execution_timeout chain ->
               chain.execution_event <- None;
               time_out_execution environment chain
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

(** Names the runs whose workflow task is failing, for the blocked
    diagnostic: a workflow waiting on such a run (a parent waiting for a
    stuck child, for example) cannot progress until the test retries it. *)
let stuck_note environment =
  match
    List.filter_map
      (fun run ->
        if (not run.closed) && Option.is_some run.stuck then
          Some run.chain.workflow_id
        else None)
      (List.rev environment.runs)
  with
  | [] -> ""
  | ids ->
      "; the workflow task of " ^ String.concat ", " ids
      ^ " is failing and waits for Temporal.Testing.retry_workflow_tasks"

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
              ^ stuck_note environment))
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

let start ?workflow_id ?memo ?search_attributes ?id_reuse_policy
    ?(id_conflict_policy = Conflict_fail) ?execution_timeout_ms ?run_timeout_ms
    ?task_timeout_ms ?retry_policy ~already_started environment ~workflow_type
    ~input =
  let* () = ensure_open environment in
  let workflow_id =
    match workflow_id with
    | Some workflow_id -> workflow_id
    | None ->
        environment.next_workflow <- environment.next_workflow + 1;
        Printf.sprintf "workflow-%d" environment.next_workflow
  in
  let* () = validate_workflow_id workflow_id in
  drain environment;
  let start_new () =
    let* chain =
      new_chain ?retry_policy ?execution_timeout:execution_timeout_ms
        ?run_timeout:run_timeout_ms ?task_timeout:task_timeout_ms ?memo
        ?search_attributes environment ~workflow_id ~workflow_type ~input
        ~parent:None ~parent_close_policy:None
    in
    drain environment;
    Ok { environment; target = chain; started = true }
  in
  match Hashtbl.find_opt environment.chains workflow_id with
  | Some chain when Option.is_none chain.outcome -> (
      match id_conflict_policy with
      | Conflict_fail ->
          Error (already_started ~workflow_id ~run_id:(current chain).run_id)
      | Conflict_use_existing -> Ok { environment; target = chain; started = false }
      | Conflict_terminate_existing ->
          (* Check the registration first, so a start the environment cannot
             run never terminates the existing workflow. *)
          if not (Name_map.mem workflow_type environment.workflows) then
            Error
              (defect
                 (Printf.sprintf "workflow type %s is not registered"
                    workflow_type))
          else begin
            terminate_chain environment chain
              ~reason:"terminated by a start with the terminate-existing policy";
            start_new ()
          end)
  | Some chain when reuse_refused id_reuse_policy chain ->
      Error (already_started ~workflow_id ~run_id:(current chain).run_id)
  | Some _ | None -> start_new ()

let workflow_id handle = handle.target.workflow_id
let run_id handle = (current handle.target).run_id
let started handle = handle.started

(** Sorts a map by key, the order [Client.describe] reports, on a copy. *)
let sorted_by_key values =
  List.sort (fun (left, _) (right, _) -> String.compare left right)
    (List.map
       (fun (key, (payload : Codec.payload)) ->
         (key, { payload with data = Bytes.copy payload.data }))
       values)

let memo handle = sorted_by_key (current handle.target).memo
let search_attributes handle = sorted_by_key (current handle.target).search_attributes

(** Returns the failure of [chain]'s current workflow task when it is
    failing in {!Retry_task} mode. *)
let stuck_failure chain = Option.map fst (current chain).stuck

let workflow_task_failure handle = stuck_failure handle.target

let result ?timeout_ms { environment; target = chain; _ } =
  let* () = ensure_open environment in
  let* () =
    run_until environment ?timeout_ms
      ~describe:("workflow " ^ chain.workflow_id)
      (fun () -> Option.is_some chain.outcome || Option.is_some (stuck_failure chain))
  in
  match (chain.outcome, stuck_failure chain) with
  | Some outcome, _ -> outcome
  | None, Some error -> Error error
  | None, None -> Error (defect "workflow finished without an outcome")

(** Returns an error when [chain] has closed, so requests aimed at a closed
    workflow fail as they would against a server. *)
let ensure_running chain =
  if Option.is_some chain.outcome then
    Error (defect ("workflow is closed: " ^ chain.workflow_id))
  else Ok ()

let signal { environment; target = chain; _ } ~name ~input =
  let* () = ensure_open environment in
  drain environment;
  let* () = ensure_running chain in
  push_job environment (current chain)
    (Activation.Signal_workflow
       { signal_name = name; input; identity; headers = [] });
  drain environment;
  Ok ()

let query { environment; target = chain; _ } ~name ~arguments =
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
  (* A run whose workflow task is failing is answered from its last
     completed task, as a server answers it after replaying the history
     without the failing task (#1160). *)
  let* () =
    match run.stuck with
    | Some _ when not run.closed -> rebuild run
    | Some _ | None -> Ok ()
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

let update ?timeout_ms { environment; target = chain; _ } ~name ~input =
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
    | Some Update_accepted | None -> run.closed || Option.is_some run.stuck
  in
  let* () =
    run_until environment ?timeout_ms ~describe:("update " ^ name) settled
  in
  match (Hashtbl.find_opt run.update_results update_id, run.stuck) with
  | Some (Update_completed payload), _ -> Ok payload
  | Some (Update_rejected error), _ -> Error error
  | (Some Update_accepted | None), Some (error, _) when not run.closed ->
      (* The update stays queued for the retried task, as a server keeps it
         while the workflow task fails; report the failure that blocks it. *)
      Error error
  | (Some Update_accepted | None), _ ->
      Error
        (Error.make ~non_retryable:true ~category:`Update
           ~message:
             ("workflow closed before update " ^ name ^ " completed: "
            ^ chain.workflow_id)
           ())

let cancel { environment; target = chain; _ } =
  let* () = ensure_open environment in
  drain environment;
  let* () = ensure_running chain in
  push_job environment (current chain) Activation.Cancel_workflow;
  drain environment;
  Ok ()

let retry_workflow_tasks environment =
  let* () = ensure_open environment in
  drain environment;
  let stuck =
    List.filter
      (fun run -> (not run.closed) && Option.is_some run.stuck)
      (List.rev environment.runs)
  in
  match stuck with
  | [] -> Error (defect "no workflow task is failing")
  | _ :: _ ->
      List.iter
        (fun run ->
          match run.stuck with
          | None -> ()
          | Some (_, failed_jobs) -> (
              match rebuild run with
              | Error error ->
                  (* The replay itself failed: the task is still failing. *)
                  run.stuck <- Some (error, failed_jobs)
              | Ok () ->
                  (* The failed jobs precede everything that arrived while the
                     task was failing; [jobs] is newest first. *)
                  run.stuck <- None;
                  run.jobs <- run.jobs @ List.rev failed_jobs;
                  if run.started && (not run.queued) && run.jobs <> [] then begin
                    run.queued <- true;
                    Queue.push run environment.ready
                  end))
        stuck;
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
