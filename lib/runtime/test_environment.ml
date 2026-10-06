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
    last run closes; continue-as-new replaces [current] without setting it.
    [parent] identifies the parent run and the child command sequence to
    resolve when the chain closes. *)
type chain = {
  workflow_id : string;
  mutable current : run option;
  mutable first_run_id : string;
  mutable outcome : (Codec.payload, Error.t) result option;
  parent : (run * int64) option;
  parent_close_policy : Activation.child_workflow_parent_close_policy option;
}

(** One workflow run and the engine-side state of its outstanding commands.
    [jobs] holds activation jobs not yet delivered, newest first; [queued]
    records membership in the environment's ready queue so a run is queued
    at most once. A closed run keeps its execution so it stays queryable. *)
and run = {
  run_id : string;
  workflow_type : string;
  chain : chain;
  execution : execution;
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

(** The work performed when an event falls due. *)
and action =
  | Fire_timer of run * int64
  | Retry_activity of run * int64 * activity_request * int

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

(** Rebuilds an error with a new category and message prefix while retaining
    its retryability, application type, and details, mirroring how Core wraps
    a cause in an activity or child-workflow failure. *)
let wrap ~category ~prefix error =
  let view = Error.view error in
  Error.make ~non_retryable:view.non_retryable ?error_type:view.error_type
    ~details:view.details ~category ~message:(prefix ^ view.message) ()

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

(** Folds registrations into a name map. A repeated name is a defect unless
    the later registration is an override, which replaces the earlier one. *)
let register kind ~name_of ~override_of ~value_of items =
  List.fold_left
    (fun acc item ->
      let* map = acc in
      let name = name_of item in
      if Name_map.mem name map && not (override_of item) then
        Error
          (defect (Printf.sprintf "duplicate %s registration: %s" kind name))
      else Ok (Name_map.add name (value_of item) map))
    (Ok Name_map.empty) items

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

(** Schedules [action] [delay_ms] after the current virtual time. *)
let schedule environment ~delay_ms action =
  let event =
    {
      at = Int64.add environment.now_ms delay_ms;
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
    closed run are dropped: Core never activates a closed run either. *)
let push_job environment run job =
  if not run.closed then begin
    run.jobs <- job :: run.jobs;
    if not run.queued then begin
      run.queued <- true;
      Queue.push run environment.ready
    end
  end

(** Records [chain]'s final outcome once and resolves the parent's child
    future. Cancellation keeps its category so the parent can distinguish it
    from a child failure; every other error is wrapped as Core does. *)
let rec close_chain environment chain outcome =
  if Option.is_none chain.outcome then begin
    chain.outcome <- Some outcome;
    match chain.parent with
    | Some (parent, seq) when not parent.closed ->
        Hashtbl.remove parent.children seq;
        let result =
          match outcome with
          | Ok payload -> Ok payload
          | Error error when (Error.view error).category = `Cancelled ->
              Error error
          | Error error ->
              Error
                (wrap ~category:`Child_workflow
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

(** Closes [run] and, when it is its chain's current run, the chain. *)
let finish environment run outcome =
  close_run environment run;
  if current run.chain == run then close_chain environment run.chain outcome

(** Creates the next run of [chain], decoding [input] with the registered
    codec, installing the run identity, and queueing its initialization
    activation. Nothing is allocated when the type is unknown or the input
    does not decode. *)
let create_run environment chain ~workflow_type ~input =
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
            Execution.start ~task_queue:environment.task_queue
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
                 attempt = 1;
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
               });
          let run =
            {
              run_id;
              workflow_type;
              chain;
              execution = Execution execution;
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

(** Creates and registers a chain whose first run is [workflow_type]. *)
let new_chain environment ~workflow_id ~workflow_type ~input ~parent
    ~parent_close_policy =
  let chain =
    {
      workflow_id;
      current = None;
      first_run_id = "";
      outcome = None;
      parent;
      parent_close_policy;
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
    failure is final. This follows Temporal's activity retry rules: a
    non-retryable error, a listed non-retryable error type, or an exhausted
    attempt budget ends the activity; otherwise the delay grows by the
    backoff coefficient up to the maximum interval. Unset fields take the
    server defaults (1s initial interval, coefficient 2.0, maximum 100 times
    the initial interval), and an unlimited budget is capped at the
    environment's [max_activity_attempts]. *)
let retry_delay environment policy ~attempt error =
  let view = Error.view error in
  if view.non_retryable || view.category = `Cancelled then None
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

(** Runs one attempt of [request] synchronously, outside every workflow
    context, and returns its encoded output or failure. Activities take zero
    virtual time. Exceptions follow the worker contract: they become
    non-retryable [ocaml_exception] failures. *)
let attempt_activity environment run request ~attempt =
  let failure ?error_type message =
    Error
      (Error.make ~non_retryable:true ?error_type ~category:`Activity ~message
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
                failure ~error_type:"codec"
                  ("activity input decoding failed: " ^ Error.message error)
            | exception exn ->
                failure ~error_type:"ocaml_exception"
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
                  failure ~error_type:"codec"
                    ("activity output encoding failed: " ^ Error.message error)
              | exception exn ->
                  failure ~error_type:"ocaml_exception"
                    ("activity output encoder raised: " ^ Printexc.to_string exn)
              )))

(** Runs attempt [attempt] of the activity identified by [seq] and either
    queues its resolution for [run] or schedules the next attempt after the
    retry delay. A final failure is wrapped as an [Activity] error, as Core
    reports it; a cancellation keeps its category. *)
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
            if (Error.view error).category = `Cancelled then error
            else
              wrap ~category:`Activity
                ~prefix:
                  (Printf.sprintf "activity %s failed: " request.activity_type)
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

(** Starts a child chain for a [Start_child_workflow] command and queues the
    start acknowledgement. A running workflow ID, an unregistered type, or an
    input the child cannot decode fails the start instead. *)
let start_child environment run ~seq ~id ~name ~input ~parent_close_policy =
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
      new_chain environment ~workflow_id:id ~workflow_type:name ~input
        ~parent:(Some (run, seq)) ~parent_close_policy
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
  | Start_child_workflow { seq; id; name; input; parent_close_policy; _ } ->
      start_child environment run ~seq ~id ~name ~input ~parent_close_policy
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
  | Complete_workflow payload -> finish environment run (Ok payload)
  | Fail_workflow error -> finish environment run (Error error)
  | Cancel_workflow_execution ->
      finish environment run (Error (cancelled "workflow cancelled"))
  | Continue_as_new { workflow_type; input } -> (
      close_run environment run;
      match create_run environment run.chain ~workflow_type ~input with
      | Ok _ -> ()
      | Error error -> close_chain environment run.chain (Error error))

(** Delivers [jobs] to [run] in one activation stamped with the current
    virtual time and interprets the resulting commands in emission order. A
    workflow task failure closes the run with that error: the environment
    does not retry tasks, so the defect is reported to the test instead of
    being retried forever. *)
and activate environment run jobs =
  let (Execution execution) = run.execution in
  run.history_length <- run.history_length + List.length jobs + 2;
  Execution.set_activation_timestamp execution
    (Some (protocol_timestamp environment.now_ms));
  Execution.set_activation_is_replaying execution false;
  Execution.set_activation_history execution
    {
      Workflow_context_store.history_length = run.history_length;
      history_size_bytes = None;
      continue_as_new_suggested = false;
    };
  let commands = Execution.activate execution jobs in
  (match Execution.task_failure execution with
  | Some error when not run.closed -> finish environment run (Error error)
  | Some _ | None -> ());
  List.iter (apply_command environment run) commands

(** Activates runnable executions in FIFO order until none has undelivered
    jobs. Virtual time does not move. *)
let drain environment =
  while not (Queue.is_empty environment.ready) do
    let run = Queue.pop environment.ready in
    run.queued <- false;
    let jobs = List.rev run.jobs in
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
                 run_activity environment run seq request ~attempt);
        loop ()
    | Some _ | None -> ()
  in
  loop ();
  drain environment

(** Runs the environment until [finished] holds, skipping virtual time to
    the next event whenever nothing else can progress. [describe] names the
    awaited operation in the blocked and timeout diagnostics. *)
let run_until environment ?timeout_ms ~describe finished =
  let deadline = Option.map (Int64.add environment.now_ms) timeout_ms in
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
          | Some deadline when Int64.compare at deadline > 0 ->
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
  let query_id = fresh_request_id environment "query" in
  (* A query is answered in its own activation, as Core delivers it, so it
     cannot run workflow fibers or append commands. *)
  activate environment run
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
    let target = Int64.add environment.now_ms milliseconds in
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
