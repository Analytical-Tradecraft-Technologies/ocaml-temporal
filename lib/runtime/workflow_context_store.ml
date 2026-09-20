(** Function saved for each pending activity. It receives the raw payload,
    decodes it to the activity's declared output type, and completes the future
    returned to workflow code. *)
type activity_resolution =
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result -> unit

(** Retains the immutable request fields needed to re-emit one local activity
    after Core asks the language layer to back off. The resolver remains in the
    ordinary activity table so terminal completions use the same lifecycle;
    [backoff_timer_seq] is set while the retry timer is outstanding and makes
    duplicate Core backoff jobs fail closed. *)
type local_activity_state = {
  activity_id : string;
  activity_type : string;
  arguments : Temporal_base.Codec.payload list;
  schedule_to_close_timeout : int64 option;
  schedule_to_start_timeout : int64 option;
  start_to_close_timeout : int64 option;
  retry_policy : Activation.retry_policy option;
  local_retry_threshold : int64 option;
  cancellation_type : Activation.activity_cancellation_type;
  mutable attempt : int64;
  mutable original_schedule_time :
    Temporal_protocol.Workflow_protocol.timestamp option;
  mutable backoff_timer_seq : int64 option;
}

(** Function saved for each pending child workflow. Child and activity
    resolutions have the same wire payload shape but remain in separate tables,
    preventing one job kind from resolving an operation of the other kind. *)
type child_workflow_resolution =
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result -> unit

(** A child resolver survives its start acknowledgment. Core first reports the
    assigned run ID, then later reports the terminal payload or failure. Keeping
    both pieces in one table entry prevents a successful start from being
    mistaken for completion and gives duplicate acknowledgments a typed error. *)
type child_workflow_state = {
  resolve : child_workflow_resolution;
  mutable start_run_id : string option;
}

(** Resolver retained for one external signal or cancellation command. The
    command future resolves only after Core reports delivery or a structured
    failure, preserving Temporal's asynchronous semantics. *)
type external_workflow_resolution =
  (unit, Temporal_base.Error.t) result -> unit

(** The Core job kind is retained with each external resolver. Signal and
    cancellation completions have the same payload type, so checking this
    discriminator prevents a malformed or reordered activation from resolving
    the wrong workflow future. *)
type external_operation = [ `Signal | `Cancel ]

type external_workflow_state = {
  operation : external_operation;
  resolve : external_workflow_resolution;
}

(** Identifies the one language-level patch operation selected for a durable
    patch ID during this reconstructed execution. Temporal Core keeps the first
    marker command for an ID, so silently mixing active and deprecated commands
    would make source order determine durable history. *)
type patch_mode = Active | Deprecated

(** Replay evidence and source-level API mode retained for one durable patch
    ID. History notification may update [decision] before or after source code
    selects [mode], while [mode = None] means no local patch call has run yet. *)
type patch_state = {
  mutable decision : bool;
  mutable mode : patch_mode option;
}

(** A typed key for one workflow-local value. The integer is allocated once at
    key creation; each execution context stores its own value under that key,
    so two workflow runs cannot observe one another's mutable state. The
    representation is private because the runtime is the only code allowed to
    associate a key with an erased value in the execution store. *)
type 'a local = { id : int }

(** Allocates unique identities for workflow-local keys without sharing any
    application value between executions. *)
let next_local_id = Atomic.make 0

(** State shared by SDK operations in one workflow execution. Activities and
    timers use increasing sequence numbers so later activation jobs can identify
    the command they complete. Commands are stored in reverse order because
    adding to the front of an OCaml list is constant time. *)
type t = {
  scheduler : Scheduler.t;
  task_queue : string;
  conditions : Condition_store.t;
  mutable activation_timestamp :
    Temporal_protocol.Workflow_protocol.timestamp option;
  (* Deployment metadata is task-local. Core may route successive tasks for
     one run to different builds, so replace it before every activation. *)
  mutable activation_deployment_version : (string * string) option;
  (* Replaced before every activation. Patch decisions use this flag only when
     their ID has not already been fixed for this workflow execution. *)
  mutable activation_is_replaying : bool;
  (* Patch decisions are execution-local durable-language state. A cached
     [false] is as significant as [true], because replay without a marker must
     continue taking the pre-patch branch for the lifetime of this run. *)
  patches : (string, patch_state) Hashtbl.t;
  mutable next_sequence : int64;
  activities : (int64, activity_resolution) Hashtbl.t;
  local_activities : (int64, local_activity_state) Hashtbl.t;
  child_workflows : (int64, child_workflow_state) Hashtbl.t;
  external_workflows : (int64, external_workflow_state) Hashtbl.t;
  timers : (int64, unit -> unit) Hashtbl.t;
  (* Values keyed here belong only to this execution context. The [Obj]
     boundary is private and safe because callers can only use the same
     existentially typed key returned by [create_local]. *)
  locals : (int, Obj.t) Hashtbl.t;
  (* Seeded deterministic pseudo-random state. Temporal supplies the initial
     seed and replacements after reset; keeping the state in the execution context
     makes repeated calls replay identically without consulting process-global
     randomness or the host clock. *)
  mutable random_state : int64;
  (* Update validators are a read-only live-request check.  The runtime flips
     this guard only while a validator callback runs so deterministic helpers
     such as [random_int] cannot mutate workflow state on the live path that
     replay intentionally skips. *)
  mutable randomness_disabled : bool;
  mutable commands_rev : Activation.command list;
  (* Once true, [emit] ignores new commands. Set at the start of [shutdown]
     before waiters are discontinued so Fun.protect finally blocks cannot
     append cancel commands after a terminal command. *)
  mutable sealed : bool;
}

(** Validates the worker queue before it becomes an implicit activity option.
    Queue names cross the strict JSON boundary even when workflow code omits
    [~task_queue], so rejecting an empty, NUL-containing, oversized, or
    non-UTF-8 default at execution construction keeps configuration failures
    out of the later workflow command path. The result carries the stable
    diagnostic so worker construction can reject the same value without
    catching an exception. *)
let validate_task_queue task_queue =
  if String.equal task_queue "" then
    Error "task_queue must not be empty"
  else if String.contains task_queue '\000' then
    Error "task_queue must not contain NUL"
  else if String.length task_queue > 65_536 then
    Error "task_queue exceeds 65536 bytes"
  else if not (Temporal_base.Codec.valid_utf_8 task_queue) then
    Error "task_queue must be valid UTF-8"
  else Ok ()

(** Converts canonical uint64 decimal text into the generator's bit pattern.
    Zero retains the existing nonzero fallback required by xorshift. *)
let seed_of_decimal seed =
  let maximum = "18446744073709551615" in
  if String.length seed = 0
     || (String.length seed > 1 && Char.equal seed.[0] '0')
     || not (String.for_all (function '0' .. '9' -> true | _ -> false) seed)
     || String.length seed > String.length maximum
     || (String.length seed = String.length maximum
        && String.compare seed maximum > 0)
  then invalid_arg "randomness seed must be an unsigned decimal string";
  (* Arithmetic on [Int64] deliberately wraps modulo 2^64.  The protocol
     validator already bounds the input to uint64, so this parser accepts the
     full Core range without an intermediate signed conversion. *)
  let state =
    String.fold_left
      (fun state digit ->
        Int64.add (Int64.mul state 10L)
          (Int64.of_int (Char.code digit - Char.code '0')))
      0L seed
  in
  if Int64.equal state 0L then 1L else state

(** Creates empty activity and timer tables. The tables grow normally if a
    workflow has more than the small initial capacity. *)
let create ?(task_queue = "default") ?(randomness_seed = "0") scheduler =
  match validate_task_queue task_queue with
  | Error message ->
      (* Preserve the existing execution-construction contract for callers
         that create a runtime directly: invalid worker configuration is a
         programmer defect at this lower-level API. The worker adapter uses
         [validate_task_queue] directly and returns a typed configuration
         error before it publishes any execution state. *)
      invalid_arg message
  | Ok () ->
      {
        scheduler;
        task_queue;
        conditions = Condition_store.create scheduler;
        activation_timestamp = None;
        activation_deployment_version = None;
        activation_is_replaying = false;
        patches = Hashtbl.create 8;
        next_sequence = 0L;
        activities = Hashtbl.create 16;
        local_activities = Hashtbl.create 16;
        child_workflows = Hashtbl.create 16;
        external_workflows = Hashtbl.create 16;
        timers = Hashtbl.create 16;
        locals = Hashtbl.create 8;
        random_state = seed_of_decimal randomness_seed;
        randomness_disabled = false;
        commands_rev = [];
        sealed = false;
      }

(** Stores the currently running workflow separately on each OCaml Domain, so
    workflow code running on different Domains cannot see the wrong context. *)
let current_key = Domain.DLS.new_key (fun () -> None)
let current () = Domain.DLS.get current_key

(** Creates a key whose value, when set, is retained only in each execution's
    private context. Key creation is allowed outside workflow code so one
    definition can share the key between its workflow body and registered
    interaction handlers. *)
let create_local () =
  { id = Atomic.fetch_and_add next_local_id 1 }

(** Reads one execution-local value without consulting process-global mutable
    state. The caller must have obtained [key] from [create_local]; the private
    representation preserves the corresponding OCaml type. *)
let get_local context key =
  Option.map Obj.obj (Hashtbl.find_opt context.locals key.id)

(** Stores one value in the current execution context. Replacing a value is
    deterministic because the owner Domain serializes workflow and interaction
    handler callbacks for that execution. *)
let set_local context key value =
  Hashtbl.replace context.locals key.id (Obj.repr value)

(** Stores the deterministic clock value for the activation being dispatched.
    The field is mutable because one execution context is reused across many
    workflow tasks; replacing it before dispatch prevents a later activation
    from observing the previous task's timestamp. *)
let set_activation_timestamp context timestamp =
  context.activation_timestamp <- timestamp

(** Reads the timestamp most recently installed for this execution context. *)
let activation_timestamp context = context.activation_timestamp

(** Stores the deployment/build identity attached to the current task. [None]
    clears stale metadata for unversioned or synthetic activations. *)
let set_activation_deployment_version context version =
  context.activation_deployment_version <- version

(** Returns the deployment/build identity supplied for the current task. *)
let activation_deployment_version context =
  context.activation_deployment_version

(** Stores Core's replay status for the activation about to run. The value is
    task-local; previously established patch decisions remain execution-local
    and therefore are not cleared here. *)
let set_activation_is_replaying context is_replaying =
  context.activation_is_replaying <- is_replaying

(** Copies a protocol identifier before retaining it in execution state or an
    emitted command. Although OCaml strings are normally immutable, callers at
    private/native boundaries can construct aliases with [Bytes.unsafe_to_string].
    A private copy keeps later mutation from corrupting a hash-table key or
    changing a command after the runtime has accepted it. *)
let snapshot_identifier value = Bytes.to_string (Bytes.of_string value)

(** Applies passive patch metadata before workflow fibers run. Replacing a
    prior decision is deliberate: a marker reported by Core is authoritative
    history evidence and must win over any earlier provisional absence. The ID
    is copied because this execution retains it beyond the activation adapter. *)
let notify_has_patch context ~patch_id =
  let patch_id = snapshot_identifier patch_id in
  match Hashtbl.find_opt context.patches patch_id with
  | Some state -> state.decision <- true
  | None ->
      Hashtbl.add context.patches patch_id { decision = true; mode = None }

(** Evaluates one patch operation and emits its Core marker command on every
    call. Repeated same-mode commands are intentional because Core owns normal
    marker deduplication. A conflicting mode is rejected before emission:
    Core retains the first command for an ID, so accepting both would silently
    make durable deprecation depend on call order. *)
let call_patch_api context ~patch_id ~mode =
  if context.sealed then
    invalid_arg "Temporal workflow patch API used after workflow execution ended";
  let patch_id = snapshot_identifier patch_id in
  let state =
    match Hashtbl.find_opt context.patches patch_id with
    | Some state -> state
    | None ->
        let state =
          {
            decision = not context.activation_is_replaying;
            mode = None;
          }
        in
        Hashtbl.add context.patches patch_id state;
        state
  in
  (match state.mode with
  | Some existing when existing <> mode ->
      invalid_arg
        "Temporal workflow patch ID cannot be both active and deprecated in one execution"
  | Some _ -> ()
  | None -> state.mode <- Some mode);
  context.commands_rev <-
    Activation.Set_patch_marker
      { patch_id; deprecated = (mode = Deprecated) }
    :: context.commands_rev;
  state.decision

(** Returns the deterministic branch decision for one active patch ID. *)
let patched context ~patch_id =
  call_patch_api context ~patch_id ~mode:Active

(** Marks one patch ID as deprecated without exposing a branch decision. The
    internal decision remains cached so replay state matches [patched], but the
    public operation exists only to record the marker lifecycle transition. *)
let deprecate_patch context ~patch_id =
  ignore (call_patch_api context ~patch_id ~mode:Deprecated)

(** Installs Core's reset seed during the ordered activation job pass, before
    any resumed fiber can draw from the stream. Parsing finishes before mutation. *)
let update_random_seed context ~randomness_seed =
  if context.sealed then invalid_arg "cannot update randomness after shutdown";
  context.random_state <- seed_of_decimal randomness_seed

(** Advances the execution-local xorshift generator.  The generator is not
    exposed publicly: its only contract is that a given Temporal seed and call
    order produce the same stream during live execution and replay. *)
let next_random_word context =
  let value = context.random_state in
  let value = Int64.logxor value (Int64.shift_left value 13) in
  let value = Int64.logxor value (Int64.shift_right_logical value 7) in
  let value = Int64.logxor value (Int64.shift_left value 17) in
  let value = if Int64.equal value 0L then 1L else value in
  context.random_state <- value;
  value

(** Returns one unbiased integer below [bound] from the deterministic stream.
    Rejection sampling avoids the modulo bias that would otherwise make the
    same workflow produce a skewed distribution for non-power-of-two bounds. *)
let random_int context ~bound =
  if context.sealed then
    Error
      (Temporal_base.Error.defect
         ~message:"workflow randomness used after workflow execution ended")
  else if context.randomness_disabled then
    Error
      (Temporal_base.Error.defect
         ~message:"workflow randomness is unavailable in a read-only update validator")
  else if bound <= 0 then
    Error
      (Temporal_base.Error.defect
         ~message:"workflow random_int bound must be positive")
  else
    let bound64 = Int64.of_int bound in
    let remainder = Int64.rem Int64.max_int bound64 in
    let limit = Int64.sub Int64.max_int remainder in
    let rec draw () =
      let sample = Int64.logand (next_random_word context) Int64.max_int in
      if Int64.compare sample limit >= 0 then draw ()
      else Ok (Int64.to_int (Int64.rem sample bound64))
    in
    draw ()

(** Runs a read-only callback with workflow randomness disabled.  Update
    validators are evaluated only on live requests and are deliberately not
    replayed; restoring the previous flag in [finally] keeps nested callbacks
    and subsequent workflow handlers from inheriting this temporary mode. *)
let with_randomness_disabled context action =
  let previous = context.randomness_disabled in
  context.randomness_disabled <- true;
  Fun.protect action ~finally:(fun () -> context.randomness_disabled <- previous)

(** Buffers a search-attribute merge without consulting wall-clock or process
    state. The payload list is copied at the public boundary before this
    command is retained by the execution context. *)
let upsert_search_attributes context search_attributes =
  if context.sealed then
    invalid_arg
      "Temporal workflow search attributes used after workflow execution ended";
  let copied =
    List.map
      (fun (key, { Temporal_base.Payload.metadata; data }) ->
        (snapshot_identifier key,
         { Temporal_base.Payload.metadata = List.map Fun.id metadata;
           data = Bytes.copy data }))
      search_attributes
  in
  context.commands_rev <-
    Activation.Upsert_search_attributes { search_attributes = copied }
    :: context.commands_rev

(** Makes [context] current while [action] runs, then restores the previous
    value even if [action] raises. This prevents later code on the same Domain
    from mistakenly appearing to run inside a workflow. *)
let with_context context action =
  let previous = current () in
  Domain.DLS.set current_key (Some context);
  Fun.protect ~finally:(fun () -> Domain.DLS.set current_key previous) action

(** Runs infrastructure code with no workflow installed, then restores the
    previous context. This prevents re-entrant callbacks such as application
    log reporters from mutating deterministic workflow state. *)
let without_context action =
  let previous = current () in
  Domain.DLS.set current_key None;
  Fun.protect ~finally:(fun () -> Domain.DLS.set current_key previous) action

(** Builds the error returned when code waits for a workflow future from the
    wrong scheduler or after workflow execution has ended. *)
let outside_error () =
  Temporal_base.Error.defect
    ~message:"future awaited outside its workflow scheduler"

(** Creates a future through the normal scheduler path and immediately supplies
    its result, keeping the pending-future count correct. *)
let resolved context result =
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  resolve result;
  future

(** Allocates a scheduler-owned notification future without adding a history
    command. The normal scheduler teardown releases it if the workflow ends
    before the owning helper signals it. *)
let create_signal context = Scheduler.promise context.scheduler ~outside_error

(** Evaluates a workflow-local condition and suspends its current fiber only
    when the predicate is false.  Keeping this wrapper beside the other
    scheduler-owned operations prevents public code from reaching into the
    condition store or retaining its callbacks directly. *)
let wait_until context ~predicate =
  Condition_store.wait_until context.conditions ~predicate

(** Rechecks all condition predicates after one activation drain.  The boolean
    tells the execution loop whether it must drain newly queued continuations
    before completing the activation. *)
let notify_conditions context = Condition_store.notify context.conditions

(** Returns an already-failed future when an SDK operation is called without an
    active workflow, without creating fake global workflow state. *)
let detached_error ~message =
  Future_store.resolved ~outside_error
    (Error (Temporal_base.Error.defect ~message))

(** Returns the next command number. Zero is reserved, so the first is one. *)
let allocate_sequence context =
  context.next_sequence <- Int64.succ context.next_sequence;
  context.next_sequence

(** Adds a command to the front of the internal list; [take_commands] restores
    creation order before returning it. After [shutdown] or [terminate] has
    sealed the context, only query responses are allowed. Discontinued waiters
    cannot append durable commands after a terminal completion, while Core can
    still query the retained final state without reopening the scheduler. *)
let emit context command =
  match command with
  | Activation.Query_result _ ->
      context.commands_rev <- command :: context.commands_rev
  | _ ->
      if not context.sealed then
        context.commands_rev <- command :: context.commands_rev

(** Records a terminal command before aborting the current scheduler. The
    command is therefore retained even though the calling continuation is not
    resumed. The context is sealed before the abort unwinds the fiber, exactly
    as [emit_terminal]/[shutdown] seal the sibling terminal path, so a
    [Fun.protect] cleanup running during that unwind cannot append a command
    after this terminal one. *)
let terminate context command =
  emit context command;
  context.sealed <- true;
  Scheduler.abort_workflow ()

(** Records the successor command before aborting the current scheduler. The
    command must be visible even though the terminal effect never resumes its
    caller. *)
let continue_as_new context ~workflow_type ~input =
  terminate context (Activation.Continue_as_new { workflow_type; input })

(** Builds an error indicating that Core and the OCaml runtime disagree about
    which operation sequence is pending or that a bridge lifecycle rule was
    violated. *)
let bridge_error message =
  Temporal_base.Error.make ~non_retryable:true ~category:`Bridge ~message ()

(** Saves the future resolver before emitting the schedule command. This order
    ensures even an immediate synthetic result can find the pending activity.
    The returned cancellation operation closes over the activity's terminal
    state, so a late retry remains idempotent after the resolver is removed. *)
let schedule_activity context ~name ~input ?activity_id ?task_queue
    ?schedule_to_close_timeout ?schedule_to_start_timeout ?start_to_close_timeout
    ?heartbeat_timeout ?retry_policy ?priority
    ?(cancellation_type = Activation.Try_cancel)
    ?(do_not_eagerly_execute = false) ?(local = false) ~decode () =
  let seq = allocate_sequence context in
  (* These flags belong to the handle, not the pending table. Core removes an
     activity entry before delivering its result, so the closure must retain
     enough lifecycle state to recognize valid late cancellation retries. *)
  let cancellation_requested = ref false in
  let terminal = ref false in
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  Hashtbl.add context.activities seq (fun result ->
      terminal := true;
      match result with
      | Error error -> resolve (Error error)
      | Ok payload ->
          (* User/codec decoders must not escape job processing half-applied.
             Contain unexpected exceptions as structured defects. *)
          (match decode payload with
          | result -> resolve result
          | exception exn ->
              resolve
                (Error
                   (Temporal_base.Error.defect
                      ~message:
                        ("activity result decoder raised: "
                        ^ Printexc.to_string exn)))));
  let activity_id =
    match activity_id with
    | Some value -> value
    | None -> "ocaml-activity-" ^ Int64.to_string seq
  in
  let task_queue = Option.value task_queue ~default:context.task_queue in
  (* Temporal requires at least one activity timeout. A deterministic
     start-to-close default keeps the long-standing [Activity.start] call
     usable while labelled timeout arguments remain available for workflows
     that need a different policy. *)
  let start_to_close_timeout =
    match (schedule_to_close_timeout, start_to_close_timeout) with
    | None, None -> Some 60_000L
    | _, value -> value
  in
  if local then begin
    let local_state =
      {
        activity_id;
        activity_type = name;
        arguments = [ input ];
        schedule_to_close_timeout;
        schedule_to_start_timeout;
        start_to_close_timeout;
        retry_policy;
        local_retry_threshold = None;
        cancellation_type;
        attempt = 1L;
        original_schedule_time = None;
        backoff_timer_seq = None;
      }
    in
    Hashtbl.add context.local_activities seq local_state;
    emit context
      (Activation.Schedule_local_activity
         {
           seq;
           activity_id;
           activity_type = name;
           attempt = 1L;
           original_schedule_time = None;
           arguments = [ input ];
           schedule_to_close_timeout;
           schedule_to_start_timeout;
           start_to_close_timeout;
           retry_policy;
           local_retry_threshold = None;
           cancellation_type;
         })
  end
  else
    emit context
      (Activation.Schedule_activity
         {
           seq;
           activity_id;
           activity_type = name;
           task_queue;
           arguments = [ input ];
           schedule_to_close_timeout;
           schedule_to_start_timeout;
           start_to_close_timeout;
           heartbeat_timeout;
           retry_policy;
           priority;
           cancellation_type;
           do_not_eagerly_execute;
         });
  let cancel () =
    match current () with
    | Some current when current == context -> (
        match Hashtbl.find_opt context.activities seq with
        | None when not !terminal && not !cancellation_requested ->
            Error
              (bridge_error
                 (Printf.sprintf
                    "unknown or completed activity sequence %Ld" seq))
        | None -> Ok ()
        | Some _ when !cancellation_requested -> Ok ()
        | Some _ ->
            (* Core's request-cancel command identifies the activity by sequence
               only, so the handle exposes no ignored reason argument. *)
            emit context
              (if local then
                 Activation.Request_cancel_local_activity { seq }
               else Activation.Request_cancel_activity { seq });
            cancellation_requested := true;
            Ok ())
    | _ ->
        Error
          (Temporal_base.Error.defect
             ~message:"activity cancellation used outside its workflow")
  in
  (future, cancel)

(** Schedules a local activity through Core's local activity manager. The
    callback still runs through the worker's typed activity registry, while
    Core owns retry timers and records the eventual result as a history marker.
    Local activities intentionally ignore remote-only task queue, heartbeat,
    priority, and eager-execution controls. *)
let schedule_local_activity context ~name ~input ?activity_id
    ?schedule_to_close_timeout ?schedule_to_start_timeout ?start_to_close_timeout
    ?retry_policy ?(cancellation_type = Activation.Wait_cancellation_completed)
    ~decode () =
  schedule_activity context ~name ~input ?activity_id
    ?schedule_to_close_timeout ?schedule_to_start_timeout ?start_to_close_timeout
    ?retry_policy ~cancellation_type ~local:true ~decode ()

(** Saves a child resolver before emitting its command. The explicit [id] is
    application-owned durable identity; an optional retry policy is copied
    into the command for Core's durable retry state machine; the private [seq]
    only correlates Core completion jobs with this in-memory execution. *)
let start_child_workflow context ~id ~name ~input
    ?retry_policy ?(cancellation_type = Activation.Child_try_cancel) ~decode () =
  let seq = allocate_sequence context in
  (* Keep this bit in the handle closure rather than in the pending table so a
    repeated cancel remains idempotent even after Core has removed the child
    state while delivering its terminal result. *)
  let cancellation_requested = ref false in
  (* Core removes the table entry before the resolver runs. Retain this
     terminal marker in the handle closure so a valid cancellation arriving
     after natural completion or a start failure is the documented no-op,
     while a cancellation after an unrelated shutdown still reports a bridge
     defect. *)
  let terminal = ref false in
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  Hashtbl.add context.child_workflows seq
    {
      resolve =
        (fun result ->
          terminal := true;
          match result with
          | Error error -> resolve (Error error)
          | Ok payload -> (
              match decode payload with
              | result -> resolve result
              | exception exn ->
                  resolve
                    (Error
                       (Temporal_base.Error.defect
                          ~message:
                            ("child workflow result decoder raised: "
                            ^ Printexc.to_string exn)))));
      start_run_id = None;
    };
  emit context
    (Activation.Start_child_workflow
       { seq; id; name; input; retry_policy; cancellation_type });
  let cancel ~reason =
    match current () with
    | Some current when current == context ->
        if String.equal reason "" then
          Error
            (Temporal_base.Error.defect
               ~message:"child cancellation reason must not be empty")
        else if String.contains reason '\000' then
          Error
            (Temporal_base.Error.defect
               ~message:"child cancellation reason must not contain NUL")
        else if String.length reason > 65_536 then
          Error
            (Temporal_base.Error.defect
               ~message:"child cancellation reason exceeds 65536 bytes")
        else if not (Temporal_base.Codec.valid_utf_8 reason) then
          Error
            (Temporal_base.Error.defect
               ~message:"child cancellation reason must be valid UTF-8")
        else (
          match Hashtbl.find_opt context.child_workflows seq with
          | None when not !terminal && not !cancellation_requested ->
              Error
                (bridge_error
                   (Printf.sprintf
                      "unknown or completed child workflow sequence %Ld" seq))
          | None -> Ok ()
          | Some _ when !cancellation_requested -> Ok ()
          | Some _ ->
              emit context (Activation.Cancel_child_workflow { seq; reason });
              cancellation_requested := true;
              Ok ())
    | _ ->
        Error
          (Temporal_base.Error.defect
             ~message:"child workflow cancellation used outside its workflow")
  in
  (future, cancel)

(** Starts a timer whose future completes with [()] because a timer firing has
    no result payload. *)
let start_timer context milliseconds =
  let seq = allocate_sequence context in
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  Hashtbl.add context.timers seq (fun () -> resolve (Ok ()));
  emit context (Activation.Start_timer { seq; milliseconds });
  future

(** Schedules one external workflow signal and retains a resolver until Core
    acknowledges delivery. *)
let signal_external_workflow context ~workflow_id ~run_id ~signal_name ~input
    ?(child_workflow_only = false) ?(headers = []) () =
  let seq = allocate_sequence context in
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  Hashtbl.add context.external_workflows seq
    { operation = `Signal; resolve };
  emit context
    (Activation.Signal_external_workflow
       { seq; workflow_id; run_id; signal_name; input; child_workflow_only; headers });
  future

(** Schedules one external workflow cancellation request and waits for Core's
    matching resolution job. *)
let cancel_external_workflow context ~workflow_id ~run_id ~reason () =
  let seq = allocate_sequence context in
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  Hashtbl.add context.external_workflows seq
    { operation = `Cancel; resolve };
  emit context
    (Activation.Request_cancel_external_workflow
       { seq; workflow_id; run_id; reason });
  future

(** Removes the activity before invoking its resolver. If resolving triggers a
    repeated completion immediately, the repeated sequence is rejected instead
    of completing the same future twice. *)
let resolve_activity context ~seq result =
  match Hashtbl.find_opt context.activities seq with
  | None ->
      Error
        (bridge_error
           (Printf.sprintf "unknown or duplicate activity sequence %Ld" seq))
  | Some resolve ->
      Hashtbl.remove context.activities seq;
      Hashtbl.remove context.local_activities seq;
      resolve result;
      Ok ()

(** Starts the language-owned timer requested by Core for a local retry. The
    original activity sequence stays pending while the timer uses its own
    sequence; when it fires, the callback updates the attempt metadata and
    emits the same local activity command with Core's supplied provenance. *)
let resolve_local_activity_backoff context ~seq ~attempt ~backoff_milliseconds
    ~original_schedule_time =
  match Hashtbl.find_opt context.local_activities seq with
  | None ->
      Error
        (bridge_error
           (Printf.sprintf
              "local activity backoff references unknown sequence %Ld" seq))
  | Some state -> (
      match state.backoff_timer_seq with
      | Some _ ->
          Error
            (bridge_error
               (Printf.sprintf
                  "duplicate local activity backoff sequence %Ld" seq))
      | None when attempt <= 0L ->
          Error
            (bridge_error
               (Printf.sprintf
                  "local activity backoff attempt %Ld must be positive" attempt))
      | None when backoff_milliseconds < 0L ->
          Error
            (bridge_error
               (Printf.sprintf
                  "local activity backoff duration %Ld must be non-negative"
                  backoff_milliseconds))
      | None when Int64.compare attempt state.attempt <= 0 ->
          Error
            (bridge_error
               (Printf.sprintf
                  "local activity backoff attempt %Ld does not advance sequence %Ld"
                  attempt seq))
      | None ->
          let timer_seq = allocate_sequence context in
          state.backoff_timer_seq <- Some timer_seq;
          Hashtbl.add context.timers timer_seq (fun () ->
              match Hashtbl.find_opt context.local_activities seq with
              | None -> ()
              | Some state ->
                  state.backoff_timer_seq <- None;
                  state.attempt <- attempt;
                  state.original_schedule_time <- original_schedule_time;
                  emit context
                    (Activation.Schedule_local_activity
                       {
                         seq;
                         activity_id = state.activity_id;
                         activity_type = state.activity_type;
                         attempt = state.attempt;
                         original_schedule_time = state.original_schedule_time;
                         arguments = state.arguments;
                         schedule_to_close_timeout = state.schedule_to_close_timeout;
                         schedule_to_start_timeout = state.schedule_to_start_timeout;
                         start_to_close_timeout = state.start_to_close_timeout;
                         retry_policy = state.retry_policy;
                         local_retry_threshold = state.local_retry_threshold;
                         cancellation_type = state.cancellation_type;
                       }));
          emit context
            (Activation.Start_timer
               { seq = timer_seq; milliseconds = backoff_milliseconds });
          Ok ())

(** Records the start acknowledgment or removes the child resolver on a start
    failure. Once a run ID is recorded, every later start result is rejected so
    a conflicting acknowledgment cannot complete or discard the child future. *)
let resolve_child_workflow_start context ~seq result =
  match Hashtbl.find_opt context.child_workflows seq with
  | None ->
      Error
        (bridge_error
           (Printf.sprintf
              "unknown or duplicate child workflow start sequence %Ld" seq))
  | Some state -> (
      match (state.start_run_id, result) with
      | Some _, _ ->
          Error
            (bridge_error
               (Printf.sprintf
                  "duplicate child workflow start sequence %Ld" seq))
      | None, Ok run_id ->
          if String.equal run_id "" then
            Error (bridge_error "child workflow start returned an empty run ID")
          else (
            state.start_run_id <- Some run_id;
            Ok ())
      | _, Error error ->
          Hashtbl.remove context.child_workflows seq;
          state.resolve (Error error);
          Ok ())

let resolve_child_workflow context ~seq result =
  match Hashtbl.find_opt context.child_workflows seq with
  | None ->
      Error
        (bridge_error
           (Printf.sprintf "unknown or duplicate child workflow sequence %Ld" seq))
  | Some state -> (
      match state.start_run_id with
      | None ->
          Error
            (bridge_error
               (Printf.sprintf
                  "child workflow sequence %Ld resolved before start acknowledgment"
                  seq))
      | Some _ ->
          Hashtbl.remove context.child_workflows seq;
          state.resolve result;
          Ok ())

(** Completes one external workflow operation and rejects duplicate or unknown
    sequence numbers as bridge defects instead of dropping a Core event. *)
let resolve_external_workflow context ~operation ~seq result =
  match Hashtbl.find_opt context.external_workflows seq with
  | None ->
      Error
        (bridge_error
           (Printf.sprintf
              "unknown or duplicate external workflow sequence %Ld" seq))
  | Some state ->
      if state.operation <> operation then
        Error
          (bridge_error
             (Printf.sprintf
                "external workflow sequence %Ld resolved with the wrong operation"
                seq))
      else (
        Hashtbl.remove context.external_workflows seq;
        state.resolve result;
        Ok ())

(** Removes a timer before completing its future, matching activity handling. *)
let fire_timer context ~seq =
  match Hashtbl.find_opt context.timers seq with
  | None ->
      Error
        (bridge_error (Printf.sprintf "unknown or duplicate timer sequence %Ld" seq))
  | Some fire ->
      Hashtbl.remove context.timers seq;
      fire ();
      Ok ()


(** Reports whether the command buffer already contains a workflow-terminal
    command. Used by the activation loop to avoid appending a second terminal
    when the scheduler reports a sibling fiber exception after continue-as-new
    or terminate has already buffered a terminal command. *)
let has_buffered_terminal context =
  List.exists
    (function
      | Activation.Complete_workflow _
      | Fail_workflow _
      | Cancel_workflow_execution
      | Continue_as_new _ -> true
      | _ -> false)
    context.commands_rev

(** Returns commands created so far and clears the buffer. Commands created
    later are returned by the next call. *)
let take_commands context =
  let commands = List.rev context.commands_rev in
  context.commands_rev <- [];
  commands

(** Closes pending futures before clearing activity and timer callbacks. This
    releases paused workflow fibers and the OCaml values they reference. The
    context is sealed first so discontinued waiters cannot emit new commands
    while their Fun.protect cleanups run. *)
let shutdown context =
  context.sealed <- true;
  Condition_store.shutdown context.conditions;
  Scheduler.shutdown context.scheduler;
  Hashtbl.clear context.activities;
  Hashtbl.clear context.local_activities;
  Hashtbl.clear context.child_workflows;
  Hashtbl.clear context.external_workflows;
  Hashtbl.clear context.timers;
  Hashtbl.clear context.patches
