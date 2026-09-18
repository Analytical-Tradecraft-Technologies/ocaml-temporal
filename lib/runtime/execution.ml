(** The OCaml function stored in a local workflow definition. *)
type ('input, 'output) implementation =
  'input -> ('output, Temporal_base.Error.t) result

(** Shared logging vocabulary. It deliberately contains reporter exceptions so
    workflow command and error semantics never depend on application logging. *)
module Observability = Temporal_base.Observability

(** The complete incoming event made available to a native signal handler. The
    activation translator has already validated and copied every payload. *)
type signal = {
  input : Temporal_base.Codec.payload list;
  identity : string;
  headers : (string * Temporal_base.Codec.payload) list;
}

(** A handler is kept private to the runtime so public callers cannot observe
    scheduler callbacks, continuations, or native boundary values. *)
type signal_handler = {
  name : string;
  dispatch : signal -> (unit, Temporal_base.Error.t) result;
}

(** The complete input available to a synchronous query handler. The current
    public API intentionally rejects non-empty arguments, but the internal
    representation preserves them so the boundary can evolve without losing
    Core data. *)
type query = {
  arguments : Temporal_base.Codec.payload list;
  headers : (string * Temporal_base.Codec.payload) list;
}

(** A query callback is run inline on the owner Domain. It may inspect
    read-only application state but cannot suspend a workflow continuation or
    append a workflow command. *)
type query_handler = {
  name : string;
  dispatch : query -> (Temporal_base.Codec.payload, Temporal_base.Error.t) result;
}

(** The complete request delivered to an update handler. The public update
    adapter currently accepts one typed payload; the runtime keeps the full
    repeated list and metadata so unsupported arity is rejected explicitly. *)
type update = {
  id : string;
  protocol_instance_id : string;
  name : string;
  input : Temporal_base.Codec.payload list;
  headers : (string * Temporal_base.Codec.payload) list;
  identity : string;
  update_id : string;
}

(** An update callback returns its encoded result and may suspend on a workflow
    future. [on_validated] is called before the callback so the owner can emit
    the accepted response while the callback is parked. *)
type update_handler = {
  name : string;
  dispatch :
    run_validator:bool -> on_validated:(unit -> unit) -> update ->
    (Temporal_base.Codec.payload, Temporal_base.Error.t) result;
}

(** Uses the same bounded identifier contract as workflow and activity names.
    Invalid names are internal registration defects because public signal
    definitions have already performed this validation. *)
let validate_signal_name name =
  if String.equal name "" then invalid_arg "signal handler name is empty";
  if String.contains name '\000' then
    invalid_arg "signal handler name contains NUL";
  if String.length name > 65_536 then
    invalid_arg "signal handler name exceeds 65536 bytes";
  if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "signal handler name must be valid UTF-8"

(** Builds one scheduler-owned callback package. *)
let make_signal_handler ~name ~dispatch : signal_handler =
  validate_signal_name name;
  { name; dispatch }

(** Returns the stable lookup key of an internal handler. *)
let signal_handler_name (handler : signal_handler) = handler.name

(** Uses the same bounded identifier contract as signal names for query
    registration. A violation is an internal registration defect because the
    public query module validates names before constructing a worker. *)
let validate_query_name name =
  if String.equal name "" then invalid_arg "query handler name is empty";
  if String.contains name '\000' then
    invalid_arg "query handler name contains NUL";
  if String.length name > 65_536 then
    invalid_arg "query handler name exceeds 65536 bytes";
  if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "query handler name must be valid UTF-8"

(** Builds one synchronous query callback package. *)
let make_query_handler ~name ~dispatch : query_handler =
  validate_query_name name;
  { name; dispatch }

(** Returns the stable lookup key of an internal query handler. *)
let query_handler_name (handler : query_handler) = handler.name

(** Uses the same bounded identifier contract for update registration names. *)
let validate_update_name name =
  if String.equal name "" then invalid_arg "update handler name is empty";
  if String.contains name '\000' then
    invalid_arg "update handler name contains NUL";
  if String.length name > 65_536 then
    invalid_arg "update handler name exceeds 65536 bytes";
  if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "update handler name must be valid UTF-8"

(** Builds one private update callback package. *)
let make_update_handler ~name ~dispatch : update_handler =
  validate_update_name name;
  { name; dispatch }

(** Returns the stable lookup key of an internal update handler. *)
let update_handler_name (handler : update_handler) = handler.name

(** Signal names are resolved through an immutable map. Construction validates
    duplicates in caller order; execution lookup never depends on hash-table
    iteration or mutable global state. *)
module Signal_map = Map.Make (String)

(** Query handlers are immutable after execution creation, making dispatch
    deterministic and independent of map mutation or hash iteration order. *)
module Query_map = Map.Make (String)

(** Update handlers are immutable after execution creation, so dispatch never
    depends on mutable or hash-table iteration state. *)
module Update_map = Map.Make (String)

(** Reports with workflow context masked so an application reporter cannot
    re-enter workflow APIs and mutate the deterministic command buffer. *)
let report ~src level ~tags message =
  Workflow_context_store.without_context (fun () ->
      Observability.report ~src level ~tags message)

(** In-memory state for one workflow. Only the activation loop changes it.
    [terminal] prevents a second completion command, and [evicted] prevents use
    after Core asks the worker to remove the execution from its cache. *)
type ('input, 'output) t = {
  definition :
    ('input, 'output, ('input, 'output) implementation) Temporal_base.Definition.t;
  input : 'input;
  scheduler : Scheduler.t;
  context : Workflow_context_store.t;
  signal_handlers : signal_handler Signal_map.t;
  query_handlers : query_handler Query_map.t;
  update_handlers : update_handler Update_map.t;
  (** Protocol IDs whose update handlers have acknowledged validation but are
      still suspended. This table belongs to the execution owner and is never
      accessed by Rust or another Domain. *)
  pending_updates : (string, unit) Hashtbl.t;
  mutable started : bool;
  mutable terminal : bool;
  mutable evicted : bool;
}

(** Creates execution state without calling user workflow code. The code starts
    only after Temporal delivers a start job, matching replay behavior. *)
let start ?(task_queue = "default") ?(randomness_seed = "0")
    ?(signal_handlers = []) ?(query_handlers = []) ?(update_handlers = [])
    definition input =
  let signal_handler_map : signal_handler Signal_map.t =
    List.fold_left
      (fun (handlers : signal_handler Signal_map.t) (handler : signal_handler) ->
        let name = signal_handler_name handler in
        if Signal_map.mem name handlers then
          invalid_arg ("duplicate signal handler: " ^ name)
        else Signal_map.add name handler handlers)
      Signal_map.empty signal_handlers
  in
  let query_handler_map : query_handler Query_map.t =
    List.fold_left
      (fun (handlers : query_handler Query_map.t) (handler : query_handler) ->
        let name = query_handler_name handler in
        if Query_map.mem name handlers then
          invalid_arg ("duplicate query handler: " ^ name)
        else Query_map.add name handler handlers)
      Query_map.empty query_handlers
  in
  let update_handler_map : update_handler Update_map.t =
    List.fold_left
      (fun (handlers : update_handler Update_map.t) (handler : update_handler) ->
        let name = update_handler_name handler in
        if Update_map.mem name handlers then
          invalid_arg ("duplicate update handler: " ^ name)
        else Update_map.add name handler handlers)
      Update_map.empty update_handlers
  in
  let scheduler = Scheduler.create () in
  let execution : ('input, 'output) t =
    {
      definition;
      input;
      scheduler;
      context =
        Workflow_context_store.create ~task_queue ~randomness_seed scheduler;
      signal_handlers = signal_handler_map;
      query_handlers = query_handler_map;
      update_handlers = update_handler_map;
      pending_updates = Hashtbl.create 8;
      started = false;
      terminal = false;
      evicted = false;
    }
  in
  let tags =
    Observability.tags ~operation:"execution_created"
      ~workflow_type:(Temporal_base.Definition.name definition) ()
  in
  report ~src:Observability.Source.workflow Logs.Debug ~tags
    "workflow execution state created";
  execution

(** Returns the registered type used for bounded workflow log metadata. *)
let workflow_type execution =
  Temporal_base.Definition.name execution.definition

(** Updates the reusable execution context with the current activation clock.
    Keeping this setter behind the execution abstraction prevents native code
    from reaching into the context record or bypassing its lifecycle rules. *)
let set_activation_timestamp execution timestamp =
  Workflow_context_store.set_activation_timestamp execution.context timestamp

(** Installs Core's deployment/build identity before workflow fibers run. *)
let set_activation_deployment_version execution version =
  Workflow_context_store.set_activation_deployment_version execution.context
    version

(** Updates the task-local replay flag before patch notifications and workflow
    code are dispatched through this execution. *)
let set_activation_is_replaying execution is_replaying =
  Workflow_context_store.set_activation_is_replaying execution.context is_replaying

(** Validates the opaque protocol instance identifier before it is retained as
    a pending continuation key. IDs are transport strings, but accepting an
    empty, NUL-containing, or invalid UTF-8 key would make duplicate detection
    and diagnostics ambiguous. *)
let valid_protocol_instance_id id =
  not (String.equal id "")
  && not (String.contains id '\000')
  && String.length id <= 65_536
  && Temporal_base.Codec.valid_utf_8 id

(** Removes all suspended update bookkeeping during terminal teardown. The
    scheduler separately releases the actual OCaml continuations. *)
let clear_pending_updates execution = Hashtbl.clear execution.pending_updates

(** Emits the workflow's completion, failure, or cancellation command once and
    immediately releases every paused fiber. *)
let emit_terminal execution command =
  if not execution.terminal then (
    execution.terminal <- true;
    Workflow_context_store.emit execution.context command;
    clear_pending_updates execution;
    Workflow_context_store.shutdown execution.context;
    match command with
    | Activation.Complete_workflow _ ->
        let tags =
          Observability.tags ~operation:"workflow_completed"
            ~workflow_type:(workflow_type execution) ()
        in
        report ~src:Observability.Source.workflow Logs.Info ~tags
          "workflow completed"
    | Fail_workflow _ | Cancel_workflow_execution
    | Schedule_activity _ | Schedule_local_activity _ | Start_child_workflow _
    | Request_cancel_activity _ | Request_cancel_local_activity _
    | Cancel_child_workflow _ | Signal_external_workflow _
    | Request_cancel_external_workflow _ | Start_timer _ | Cancel_timer _
    | Query_result _ | Update_response _ | Set_patch_marker _
    | Upsert_search_attributes _
    | Continue_as_new _ -> ())

(** Fails the workflow through the same one-terminal-command check. *)
let fail execution error =
  if not execution.terminal then (
    let tags =
      Observability.tags ~operation:"workflow_failed"
        ~workflow_type:(workflow_type execution)
        ~error_kind:(Temporal_base.Error.kind error) ()
    in
    report ~src:Observability.Source.workflow Logs.Error ~tags
      "workflow failed");
  emit_terminal execution (Activation.Fail_workflow error)

(** Creates a non-retryable error when Core and the OCaml runtime disagree about
    activation state. This is an SDK/bridge failure, not an application error. *)
let bridge_error message =
  Temporal_base.Error.make ~non_retryable:true ~category:`Bridge ~message ()

(** Starts the workflow function exactly once. Its output is encoded inside the
    scheduled fiber so an exception or codec failure follows the normal
    workflow-failure path. *)
let start_workflow execution =
  if execution.started then
    fail execution (bridge_error "workflow received duplicate start job")
  else if not execution.terminal then begin
    execution.started <- true;
    let tags =
      Observability.tags ~operation:"workflow_started"
        ~workflow_type:(workflow_type execution) ()
    in
    report ~src:Observability.Source.workflow Logs.Info ~tags
      "workflow started";
    Scheduler.spawn execution.scheduler (fun () ->
        match Temporal_base.Definition.implementation execution.definition with
        | None -> fail execution (bridge_error "remote workflow has no implementation")
        | Some implementation ->
            begin match
              try implementation execution.input with
              | Scheduler.Workflow_aborted as exn -> raise exn
              | Future_store.Scheduler_shutdown as exn -> raise exn
              | exn ->
                  Error
                    (Temporal_base.Error.defect
                       ~message:
                         ("workflow implementation raised: "
                         ^ Printexc.to_string exn))
            with
            | Error error -> fail execution error
            | Ok output ->
                begin match
                  try
                    Temporal_base.Codec.encode
                      (Temporal_base.Definition.output execution.definition)
                      output
                  with
                  | exn ->
                      Error
                        (Temporal_base.Error.defect
                           ~message:
                             ("workflow result encoder raised: "
                             ^ Printexc.to_string exn))
                with
                | Error error -> fail execution error
                | Ok payload ->
                    emit_terminal execution (Activation.Complete_workflow payload)
                end
            end)
  end

(** Applies one activation job. Activity and timer sequence numbers locate the
    future created by the earlier command. Unknown or repeated numbers fail the
    workflow because ignoring them would make replay disagree with Core. *)
let process_job execution = function
  | Activation.Start_workflow -> start_workflow execution
  | Resolve_activity { seq; result } -> (
      match Workflow_context_store.resolve_activity execution.context ~seq result with
      | Ok () -> ()
      | Error error -> fail execution error)
  | Resolve_local_activity_backoff
      { seq; attempt; backoff_milliseconds; original_schedule_time } -> (
      match
        Workflow_context_store.resolve_local_activity_backoff execution.context
          ~seq ~attempt ~backoff_milliseconds ~original_schedule_time
      with
      | Ok () -> ()
      | Error error -> fail execution error)
  | Resolve_child_workflow_start { seq; result } -> (
      match
        Workflow_context_store.resolve_child_workflow_start execution.context
          ~seq result
      with
      | Ok () -> ()
      | Error error -> fail execution error)
  | Resolve_child_workflow { seq; result } -> (
      match
        Workflow_context_store.resolve_child_workflow execution.context ~seq result
      with
      | Ok () -> ()
      | Error error -> fail execution error)
  | Resolve_signal_external_workflow { seq; result } -> (
      match
        Workflow_context_store.resolve_external_workflow execution.context
          ~operation:`Signal ~seq result
      with
      | Ok () -> ()
      | Error error -> fail execution error)
  | Resolve_request_cancel_external_workflow { seq; result } -> (
      match
        Workflow_context_store.resolve_external_workflow execution.context
          ~operation:`Cancel ~seq result
      with
      | Ok () -> ()
      | Error error -> fail execution error)
  | Activation.Query_workflow { query_id; query_type; arguments; headers } ->
      (* Queries are deliberately answered inline. A missing handler or a
         typed handler failure becomes QueryResult.failed; it must never fail
         the workflow or resume a paused workflow continuation. *)
      let query = { arguments; headers } in
      let result =
        match Query_map.find_opt query_type execution.query_handlers with
        | None ->
            let tags =
              Observability.tags ~operation:"workflow_query_unhandled"
                ~workflow_type:(workflow_type execution) ()
            in
            report ~src:Observability.Source.workflow Logs.Warning ~tags
              "workflow query has no registered handler";
            Error
              (bridge_error ("unhandled workflow query: " ^ query_type))
        | Some handler -> (
            let dispatched =
              try
                (* Query work must not enter [run_scheduler]: Core accepts only
                   query responses for a query-only activation.  It nevertheless
                   needs the owning execution context so a handler can inspect
                   deterministic execution-local state established by the
                   workflow or an earlier signal.  The dynamic binding is
                   restored before the QueryResult is emitted. Disabling the
                   deterministic random stream also prevents this synchronous,
                   non-replayed callback from advancing that replay-visible
                   state. *)
                Workflow_context_store.with_context execution.context (fun () ->
                    Workflow_context_store.with_randomness_disabled execution.context
                      (fun () -> handler.dispatch query))
              with
              | exn ->
                  Error
                    (Temporal_base.Error.defect
                       ~message:
                         ("query handler raised: " ^ Printexc.to_string exn))
            in
            match dispatched with
            | Ok payload ->
                let tags =
                  Observability.tags ~operation:"workflow_query_completed"
                    ~workflow_type:(workflow_type execution) ()
                in
                report ~src:Observability.Source.workflow Logs.Debug ~tags
                  "workflow query completed";
                Ok payload
            | Error error ->
                let tags =
                  Observability.tags ~operation:"workflow_query_failed"
                    ~workflow_type:(workflow_type execution)
                    ~error_kind:(Temporal_base.Error.kind error) ()
                in
                report ~src:Observability.Source.workflow Logs.Warning ~tags
                  "workflow query failed";
                Error error)
      in
      Workflow_context_store.emit execution.context
        (Activation.Query_result { query_id; result })
  | Activation.Do_update
      {
        id;
        protocol_instance_id;
        name;
        input;
        headers;
        identity;
        update_id;
        run_validator;
      } ->
      (* Updates share the scheduler with workflow continuations.  Resolving a
         future earlier in this activation only enqueues its continuation;
         queueing the update here preserves that source order and runs the
         handler under [Workflow_context_store.with_context].  Dispatching it
         inline would let the update observe stale workflow state and would
         run it outside the deterministic workflow context. *)
      let update =
        { id; protocol_instance_id; name; input; headers; identity; update_id }
      in
      Scheduler.spawn execution.scheduler (fun () ->
          let emit response =
            Workflow_context_store.emit execution.context
              (Activation.Update_response { protocol_instance_id; response })
          in
          let accepted = ref false in
          let acknowledged = ref false in
          let reject error =
            if !accepted then Hashtbl.remove execution.pending_updates protocol_instance_id;
            emit (`Rejected error)
          in
          match Update_map.find_opt name execution.update_handlers with
          | None ->
              let error = bridge_error ("unhandled workflow update: " ^ name) in
              report ~src:Observability.Source.workflow Logs.Error
                ~tags:
                  (Observability.tags ~operation:"workflow_update_unhandled"
                     ~workflow_type:(workflow_type execution) ())
                "workflow update has no registered handler";
              reject error
          | Some handler ->
              if not (valid_protocol_instance_id protocol_instance_id) then
                reject (bridge_error "workflow update has an invalid protocol instance ID")
              else if Hashtbl.mem execution.pending_updates protocol_instance_id then
                reject (bridge_error "workflow update protocol instance ID is already pending")
              else
                let on_validated () =
                  if !acknowledged then
                    invalid_arg "workflow update validation acknowledged twice"
                  else if Hashtbl.mem execution.pending_updates protocol_instance_id then
                    invalid_arg "workflow update protocol instance ID became pending twice"
                  else begin
                    Hashtbl.add execution.pending_updates protocol_instance_id ();
                    acknowledged := true;
                    (* Mark ownership before emitting. If the context has
                       already shut down, [emit] can raise; the rejection path
                       must still remove the entry this handler created. *)
                    accepted := true;
                    emit `Accepted
                  end
                in
                let dispatched =
                  try handler.dispatch ~run_validator ~on_validated update with
                | exn ->
                    Error
                      (Temporal_base.Error.defect
                         ~message:
                           ("update handler raised: " ^ Printexc.to_string exn))
              in
                begin match dispatched with
                | Error error -> reject error
                | Ok payload ->
                    if not !accepted then
                      reject (bridge_error "workflow update completed without validation acknowledgement")
                    else begin
                      Hashtbl.remove execution.pending_updates protocol_instance_id;
                      emit (`Completed payload)
                    end
                end)
  | Activation.Signal_workflow { signal_name; input; identity; headers } ->
      (* Signals are queued as scheduler work rather than dispatched inline.
         This preserves FIFO ordering with root and resolver continuations and
         gives a handler the same direct-style suspension semantics as the
         workflow body. *)
      let signal = { input; identity; headers } in
      begin match Signal_map.find_opt signal_name execution.signal_handlers with
      | None ->
          let tags =
            Observability.tags ~operation:"workflow_signal_unhandled"
              ~workflow_type:(workflow_type execution) ()
          in
          report ~src:Observability.Source.workflow Logs.Error ~tags
            "workflow signal has no registered handler";
          fail execution
            (Temporal_base.Error.make ~non_retryable:true ~category:`Workflow
               ~message:("unhandled workflow signal: " ^ signal_name) ())
      | Some handler ->
          let tags =
            Observability.tags ~operation:"workflow_signal_received"
              ~workflow_type:(workflow_type execution) ()
          in
          report ~src:Observability.Source.workflow Logs.Debug ~tags
            "workflow signal queued for its registered handler";
          Scheduler.spawn execution.scheduler (fun () ->
              match handler.dispatch signal with
              | Ok () ->
                  let tags =
                    Observability.tags ~operation:"workflow_signal_handled"
                      ~workflow_type:(workflow_type execution) ()
                  in
                  report ~src:Observability.Source.workflow Logs.Debug ~tags
                    "workflow signal handler completed"
              | Error error -> fail execution error)
      end
  | Activation.Notify_has_patch { patch_id } ->
      (* Core history is authoritative. Applying the notification during the
         job pass guarantees the decision is installed before any workflow
         fiber is drained for this activation. *)
      Workflow_context_store.notify_has_patch execution.context ~patch_id
  | Fire_timer { seq } -> (
      match Workflow_context_store.fire_timer execution.context ~seq with
      | Ok () -> ()
      | Error error -> fail execution error)
  | Cancel_workflow ->
      if not execution.terminal then (
        let tags =
          Observability.tags ~operation:"workflow_cancelled"
            ~workflow_type:(workflow_type execution) ()
        in
        report ~src:Observability.Source.workflow Logs.Info ~tags
          "workflow cancellation requested");
      emit_terminal execution Activation.Cancel_workflow_execution
  | Remove_from_cache ->
      execution.evicted <- true;
      let tags =
        Observability.tags ~operation:"execution_evicted"
          ~workflow_type:(workflow_type execution) ()
      in
      report ~src:Observability.Source.workflow Logs.Debug ~tags
        "workflow execution evicted";
      clear_pending_updates execution;
      Workflow_context_store.shutdown execution.context

(** Runs queued fibers with this workflow installed as the current context. An
    uncaught OCaml exception becomes a non-retryable defect instead of escaping
    the worker loop. *)
let run_scheduler execution =
  let rec drain () =
    let status =
      Workflow_context_store.with_context execution.context (fun () ->
          Scheduler.run execution.scheduler)
    in
    (match status with
    | Scheduler.Failed exception_ ->
        (* A sibling fiber may have raised after continue-as-new or terminate
           already buffered a terminal command. Do not append a second
           terminal; the existing command remains authoritative. *)
        if
          execution.terminal
          || Workflow_context_store.has_buffered_terminal execution.context
        then ()
        else
          fail execution
            (Temporal_base.Error.defect ~message:(Printexc.to_string exception_))
    | Scheduler.Complete | Scheduler.Blocked -> ());
    (* Predicates are checked only after runnable workflow code has drained.
       If a state mutation satisfies one, resolving its private signal queues a
       continuation; drain again so that continuation participates in this
       activation instead of waiting for a synthetic timer or later task. *)
    if
      execution.terminal
      || execution.evicted
      || Workflow_context_store.has_buffered_terminal execution.context
    then ()
    else
      let woke =
        Workflow_context_store.with_context execution.context (fun () ->
            Workflow_context_store.notify_conditions execution.context)
      in
      if woke then drain ()
  in
  drain ()

(** Releases every paused fiber and pending operation table for this execution.
    Safe to call more than once: the context and scheduler ignore a second
    shutdown after they have already become inactive. Adapters must call this
    whenever a run is removed from the registry for a path that did not already
    emit a terminal or eviction command, so one-shot effect continuations cannot
    leak after a rejected activation. *)
let shutdown execution =
  (* Teardown must not raise into lease-ack bookkeeping. Continuations that
     mishandle the private shutdown exception are contained here. *)
  clear_pending_updates execution;
  try Workflow_context_store.shutdown execution.context with _ -> ()

(** Processes jobs in order, runs fibers, and returns new commands. Cache
    removal stops processing immediately and returns no commands for the old
    in-memory execution. *)
let activate execution jobs =
  let job_count = List.length jobs in
  let query_only =
    match jobs with
    | [] -> false
    | _ -> List.for_all (function Activation.Query_workflow _ -> true | _ -> false) jobs
  in
  let commands, duration_ms =
    Observability.measure_ms (fun () ->
        if execution.evicted then (
          let tags =
            Observability.tags ~operation:"activation_ignored"
              ~workflow_type:(workflow_type execution) ~job_count ()
          in
          report ~src:Observability.Source.workflow Logs.Warning
            ~tags "activation ignored after cache eviction";
          [])
        else (
          List.iter
            (fun job ->
              if not execution.evicted then
                match job with
                | Activation.Query_workflow _ | Remove_from_cache ->
                    (* Core can query a closed execution until it evicts it.
                       Neither path may restart the disposed scheduler. *)
                    process_job execution job
                | _ when not execution.terminal -> process_job execution job
                | _ -> ())
            jobs;
          if execution.evicted then []
          else (
            (* A query-only activation must not run ordinary workflow fibers:
               doing so could append commands while Core is expecting only
               query answers and could retain a continuation at the boundary. *)
            if not execution.terminal && not query_only then run_scheduler execution;
            let commands = Workflow_context_store.take_commands execution.context in
            (* Terminal commands emitted through [terminate] (continue-as-new or
               a Fail_workflow from a failed continue-as-new encode) do not go
               through [emit_terminal], so finalize here after the scheduler
               has stopped. Any terminal command seals the run so a later
               activation cannot append a second terminal. *)
            if
              List.exists
                (function
                  | Activation.Complete_workflow _
                  | Fail_workflow _
                  | Cancel_workflow_execution
                  | Continue_as_new _ -> true
                  | _ -> false)
                commands
            then (
              execution.terminal <- true;
              clear_pending_updates execution;
              Workflow_context_store.shutdown execution.context);
            commands)))
  in
  let tags =
    Observability.tags ~operation:"activate" ~duration_ms
      ~workflow_type:(workflow_type execution) ~job_count
      ~command_count:(List.length commands) ()
  in
  report ~src:Observability.Source.workflow Logs.Debug ~tags
    "workflow activation processed";
  commands
