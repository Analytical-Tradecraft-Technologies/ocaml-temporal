(** Deterministic race matrix for cancellation, retry/redelivery, interaction
    handlers, and child workflows (#520).

    Every row drives the real private workflow runtime ({!Execution}) with a
    scripted sequence of Core activations, so each competing ordering is forced
    exactly rather than hoped for under a live scheduler. A row records:

    - the activations Core delivers, one list per workflow task, and the exact
      command history the workflow returns for each of them;
    - the ordered trace of what workflow code observed (handler invocations
      and the typed results the root received); and
    - the classified outcome of every update the script delivers.

    Generic history checks run for every row: at most one terminal command,
    the terminal command last in its batch and nothing durable after it, no
    duplicate or out-of-order update protocol phases, and no workflow-task
    failure. A conflicting terminal command, a duplicated cancellation
    request, or an accepted update that silently disappears from an open run
    therefore fails the row even where its expected history would not.

    {2 Ordering rules taken from Temporal Core}

    Rows only use job orders that the pinned Core can deliver. Core's
    activation contract (workflow_activation.proto at the pinned revision
    95e97686, "Job ordering guarantees and semantics") orders jobs as:
    initialization, patches, random-seed updates, signals and updates, all
    other jobs in history order (timer, activity, and child resolutions, and
    workflow cancellation), local-activity resolutions, then queries and
    evictions in their own activations. So a signal or update can share an
    activation with a later cancellation, and an activity resolution can sit
    on either side of a cancellation, but a local-activity resolution always
    follows the cancellation.

    {2 Contract being asserted}

    These rows pin the SDK's current, documented contract rather than another
    SDK's semantics:

    - Workflow cancellation is immediate (#514 owns cooperative cancellation).
      A [Cancel_workflow] job emits [Cancel_workflow_execution] while the jobs
      are applied and seals the run: no queued continuation runs afterwards,
      so a resolution applied earlier in the same activation is never observed
      by the root, and the SDK does not itself request cancellation of
      outstanding activities or children. Children follow their parent close
      policy on the server.
    - The first terminal command wins and seals the run. A later activation,
      repeated cancellation, or late resolution produces no command and no
      task failure.
    - Within an activation, jobs are applied to futures first and fibers then
      run FIFO, so a handler queued by a signal or update job runs before a
      root continuation woken by a later job in the same activation.
    - The SDK does not wait for unfinished handlers (see the interaction
      reference). An accepted update whose handler has not finished when the
      run closes is abandoned; the worker never sends its completion, and the
      Temporal Server fails the caller's update request when the run closes.
      That server step is not exercised offline.

    {2 Known gaps recorded rather than invented}

    Two rows record behavior that differs from Core's contract. They are
    pinned here so a fix changes this table deliberately; the decision and the
    fix are tracked in #962:

    - A signal or update delivered in the same activation as [Cancel_workflow]
      is never invoked, so the update receives neither an acceptance nor a
      rejection, although Core orders handlers before other jobs and the
      native-interaction design promises a validation response in the
      delivering activation.
    - An update handler that becomes ready in the same activation in which the
      root completes is discarded when the root's continuation runs first in
      FIFO order. Core moves the terminal command to the end of the batch
      precisely so such update results can still be delivered
      (managed_run.rs [preprocess_command_sequence]; temporalio/features#481).

    {2 Redelivery}

    Core redelivers a run by replaying its history into a fresh execution
    after eviction or a failed workflow task. Every row is therefore executed
    twice on fresh executions and must produce byte-for-byte identical
    histories and traces. Rows whose updates were accepted live are also
    replayed with [run_validator = false] and a validator that would now
    reject: the replayed history must still follow the recorded acceptance. *)

module T = Temporal
module Execution = Temporal_runtime.Execution
module Activation = Temporal_runtime.Activation

(** Copies a public payload into the runtime representation, as the native
    adapter does at registration and dispatch boundaries. *)
let base_payload (payload : T.Payload.t) : Temporal_base.Payload.t =
  { metadata = payload.metadata; data = Bytes.copy payload.data }

(** Copies a runtime payload into the public representation before a public
    handler decodes it. *)
let public_payload (payload : Temporal_base.Payload.t) : T.Payload.t =
  { metadata = payload.metadata; data = Bytes.copy payload.data }

(** Preserves a public error's classification across the runtime boundary. *)
let base_error error =
  let view = T.Error.view error in
  Temporal_base.Error.make ~category:view.category ~message:view.message
    ~non_retryable:view.non_retryable
    ~details:(List.map base_payload view.details) ()

(** The canonical encoded unit value, used for every signal, update, activity,
    and child payload in this matrix. *)
let unit_payload =
  match T.Codec.encode T.Codec.unit () with
  | Ok payload -> base_payload payload
  | Error error -> failwith (T.Error.message error)

(** The runtime error Core reports for an operation it settled as cancelled. *)
let cancelled_error =
  Temporal_base.Error.make ~category:`Cancelled ~message:"cancelled by Core" ()

(** The runtime error Core reports when a child workflow could not start. *)
let child_start_error =
  Temporal_base.Error.make ~category:`Child_workflow
    ~message:"workflow execution already started" ()

(** Labels a typed result the way workflow code observed it. Only the
    stable error kind is recorded, so the trace stays independent of message
    wording. *)
let outcome_label = function
  | Ok () -> "ok"
  | Error error -> "error:" ^ T.Error.kind error

(** Labels a runtime error by its stable kind for command rendering. *)
let base_kind error = Temporal_base.Error.kind error

(** Renders one command compactly. Sequence numbers are kept because they are
    the identity Core uses to match a command with its history event; payload
    bytes are omitted because every payload in this matrix is the unit
    value. *)
let render = function
  | Activation.Schedule_activity { seq; _ } ->
      Printf.sprintf "schedule-activity(%Ld)" seq
  | Activation.Schedule_local_activity { seq; attempt; _ } ->
      Printf.sprintf "schedule-local-activity(%Ld,attempt=%Ld)" seq attempt
  | Activation.Request_cancel_activity { seq } ->
      Printf.sprintf "request-cancel-activity(%Ld)" seq
  | Activation.Request_cancel_local_activity { seq } ->
      Printf.sprintf "request-cancel-local-activity(%Ld)" seq
  | Activation.Start_child_workflow { seq; id; _ } ->
      Printf.sprintf "start-child(%Ld,%s)" seq id
  | Activation.Cancel_child_workflow { seq; _ } ->
      Printf.sprintf "cancel-child(%Ld)" seq
  | Activation.Start_timer { seq; milliseconds } ->
      Printf.sprintf "start-timer(%Ld,%Ldms)" seq milliseconds
  | Activation.Cancel_timer { seq } -> Printf.sprintf "cancel-timer(%Ld)" seq
  | Activation.Update_response { protocol_instance_id; response = `Accepted } ->
      Printf.sprintf "update-accepted(%s)" protocol_instance_id
  | Activation.Update_response
      { protocol_instance_id; response = `Completed _ } ->
      Printf.sprintf "update-completed(%s)" protocol_instance_id
  | Activation.Update_response
      { protocol_instance_id; response = `Rejected error } ->
      Printf.sprintf "update-rejected(%s,%s)" protocol_instance_id
        (base_kind error)
  | Activation.Complete_workflow _ -> "complete-workflow"
  | Activation.Fail_workflow error ->
      Printf.sprintf "fail-workflow(%s)" (base_kind error)
  | Activation.Cancel_workflow_execution -> "cancel-workflow-execution"
  | Activation.Continue_as_new { workflow_type; _ } ->
      Printf.sprintf "continue-as-new(%s)" workflow_type
  | Activation.Signal_external_workflow { seq; _ } ->
      Printf.sprintf "signal-external(%Ld)" seq
  | Activation.Request_cancel_external_workflow { seq; _ } ->
      Printf.sprintf "cancel-external(%Ld)" seq
  | Activation.Query_result { query_id; _ } ->
      Printf.sprintf "query-result(%s)" query_id
  | Activation.Set_patch_marker { patch_id; _ } ->
      Printf.sprintf "patch-marker(%s)" patch_id
  | Activation.Upsert_search_attributes _ -> "upsert-search-attributes"

(** Recognizes the commands that close the current run. *)
let is_terminal = function
  | Activation.Complete_workflow _ | Activation.Fail_workflow _
  | Activation.Cancel_workflow_execution | Activation.Continue_as_new _ ->
      true
  | _ -> false

(** The narrow view of one execution that a row needs. Packing the closures
    hides the execution's input/output type parameters so rows with different
    fixtures fit in one table. *)
type runner = {
  activate : Activation.job list -> Activation.command list;
      (** Applies one activation and returns its commands. *)
  task_failed : unit -> bool;
      (** Reports whether the current cache generation failed its task. *)
  shutdown : unit -> unit;
      (** Releases parked fibers; safe after a terminal or eviction. *)
}

(** Builds a runner for a unit-to-unit workflow whose root is [root], with the
    given private signal and update registrations. *)
let runner ?(signals = []) ?(updates = []) root =
  let definition =
    Temporal_base.Definition.make ~name:"race-matrix"
      ~input:Temporal_base.Codec.unit ~output:Temporal_base.Codec.unit
      ~implementation:(Some (fun () -> Result.map_error base_error (root ())))
  in
  let execution =
    Execution.start ~signal_handlers:signals ~update_handlers:updates definition
      ()
  in
  {
    activate = Execution.activate execution;
    task_failed = (fun () -> Option.is_some (Execution.task_failure execution));
    shutdown = (fun () -> Execution.shutdown execution);
  }

(** Registers a public unit signal handler through the same runtime adapter
    shape as the native worker: the repeated payload list is handed to the
    public dispatcher, and its typed error is preserved. *)
let signal_handler name handler =
  let signal = T.Signal.define ~name ~input:T.Codec.unit in
  let public = T.Signal.Handler.make signal handler in
  Execution.make_signal_handler ~name ~dispatch:(fun (signal : Execution.signal) ->
      T.Signal.Handler.dispatch_payloads public
        (List.map public_payload signal.input)
      |> Result.map_error base_error)

(** Registers a public unit update handler with an optional validator, through
    the same two-phase runtime adapter as the native worker. *)
let update_handler ?validator name handler =
  let update = T.Update.define ~name ~input:T.Codec.unit ~output:T.Codec.unit in
  let public = T.Update.Handler.make ?validator update handler in
  Execution.make_update_handler ~name
    ~dispatch:(fun ~run_validator ~on_validated (update : Execution.update) ->
      T.Update.Handler.dispatch_payloads ~run_validator ~on_validated public
        (List.map public_payload update.input)
      |> Result.map base_payload |> Result.map_error base_error)

(** The initialization job. *)
let start = Activation.Start_workflow

(** Core's request to cancel the workflow run. *)
let cancel = Activation.Cancel_workflow

(** One delivery of the named unit signal. *)
let signal name =
  Activation.Signal_workflow
    { signal_name = name; input = [ unit_payload ]; identity = "race-client";
      headers = [] }

(** One delivery of the named unit update. The protocol instance and update
    ID are the same string so rendered commands name the request directly.
    [run_validator] is false when Core replays an already-accepted update. *)
let update ~run_validator name id =
  Activation.Do_update
    { id; protocol_instance_id = id; name; input = [ unit_payload ];
      headers = []; identity = "race-client"; update_id = id; run_validator }

(** Fires the timer created by command [seq]. *)
let fire seq = Activation.Fire_timer { seq }

(** Resolves the activity created by command [seq]. *)
let resolve_activity seq result = Activation.Resolve_activity { seq; result }

(** Core's request to retry local activity [seq] after a language-owned
    backoff timer. *)
let local_backoff seq ~attempt ~milliseconds =
  Activation.Resolve_local_activity_backoff
    { seq; attempt; backoff_milliseconds = milliseconds;
      original_schedule_time = None }

(** Acknowledges that child [seq] started. *)
let child_started seq =
  Activation.Resolve_child_workflow_start { seq; result = Ok "race-child-run" }

(** Reports that child [seq] could not be started. *)
let child_start_failed seq =
  Activation.Resolve_child_workflow_start { seq; result = Error child_start_error }

(** Reports child [seq]'s terminal result. *)
let child_resolved seq result = Activation.Resolve_child_workflow { seq; result }

(** Remote activity used by the activity and scope fixtures. Scheduling and
    cancellation are owned by Core, so no implementation is needed. *)
let race_activity =
  T.Activity.remote ~name:"race-activity" ~input:T.Codec.unit
    ~output:T.Codec.unit

(** Remote child workflow used by the child fixture. *)
let race_child =
  T.Workflow.remote ~name:"race-child" ~input:T.Codec.unit ~output:T.Codec.unit

(** Duration of the root's timer in the handler fixture. *)
let root_timer = T.Duration.of_ms 100L

(** Duration of a suspending update handler's timer. It differs from the
    root's so a rendered history shows which fiber started each timer. *)
let handler_timer = T.Duration.of_ms 10L

(** Fixture for activity completion against cancellation. The root schedules
    one activity, keeps its handle for a ["cancel-activity"] signal handler,
    and records the typed result it awaited before completing. *)
let activity_fixture ~record ~validator_rejects:_ =
  let handle = ref None in
  let cancel_handler =
    signal_handler "cancel-activity" (fun () ->
        match !handle with
        | None -> failwith "cancel-activity signal ran before the root"
        | Some handle ->
            record
              ("signal:cancel-activity-" ^ outcome_label (T.Activity.cancel handle));
            Ok ())
  in
  runner ~signals:[ cancel_handler ] (fun () ->
      let started = T.Activity.start_handle race_activity () in
      handle := Some started;
      let result = T.Future.await (T.Activity.future started) in
      record ("root:activity-" ^ outcome_label result);
      Ok ())

(** Fixture for a timer against workflow cancellation. *)
let timer_fixture ~record ~validator_rejects:_ =
  runner (fun () ->
      let result = T.Workflow.sleep root_timer in
      record ("root:timer-" ^ outcome_label result);
      Ok ())

(** Fixture for structured scope propagation to an activity. The root awaits
    the activity through its scope, so a ["cancel-scope"] signal both emits the
    activity's Core cancellation command and wakes the root locally. *)
let scope_fixture ~record ~validator_rejects:_ =
  let scope = ref None in
  let cancel_handler =
    signal_handler "cancel-scope" (fun () ->
        match !scope with
        | None -> failwith "cancel-scope signal ran before the root"
        | Some scope ->
            record ("signal:cancel-scope-" ^ outcome_label (T.Scope.cancel scope));
            Ok ())
  in
  runner ~signals:[ cancel_handler ] (fun () ->
      match T.Scope.create () with
      | Error _ as error -> error
      | Ok created ->
          scope := Some created;
          let future = T.Activity.start ~scope:created race_activity () in
          let result = T.Scope.await created future in
          record ("root:scoped-activity-" ^ outcome_label result);
          Ok ())

(** Fixture for child completion and start failure against parent and scope
    cancellation. The root starts one child inside a scope and awaits it
    through that scope; a ["cancel-scope"] signal cancels the scope. *)
let child_fixture ~record ~validator_rejects:_ =
  let scope = ref None in
  let cancel_handler =
    signal_handler "cancel-scope" (fun () ->
        match !scope with
        | None -> failwith "cancel-scope signal ran before the root"
        | Some scope ->
            record ("signal:cancel-scope-" ^ outcome_label (T.Scope.cancel scope));
            Ok ())
  in
  runner ~signals:[ cancel_handler ] (fun () ->
      match T.Scope.create () with
      | Error _ as error -> error
      | Ok created ->
          scope := Some created;
          let future =
            T.Child_workflow.start ~scope:created ~id:"race-child" race_child ()
          in
          let result = T.Scope.await created future in
          record ("root:child-" ^ outcome_label result);
          Ok ())

(** Fixture for handler suspension against workflow completion and
    cancellation. The root sleeps on [root_timer] and completes. Three
    handlers are registered:

    - ["suspending-update"] records its start, sleeps on [handler_timer], and
      records its finish;
    - ["immediate-update"] records itself and returns without suspending; and
    - ["note"] is a signal that records its invocation.

    Both validators accept unless [validator_rejects] is set, which replay
    rows use to prove that Core-replayed updates skip validation. *)
let handler_fixture ~record ~validator_rejects =
  let validator () =
    if validator_rejects then
      Error (T.Error.make ~category:`Update ~message:"rejected by validator" ())
    else Ok ()
  in
  let suspending =
    update_handler ~validator "suspending-update" (fun () ->
        record "update:start";
        let result = T.Workflow.sleep handler_timer in
        record ("update:finish-" ^ outcome_label result);
        result)
  in
  let immediate =
    update_handler ~validator "immediate-update" (fun () ->
        record "update:immediate";
        Ok ())
  in
  let note = signal_handler "note" (fun () -> record "signal:note"; Ok ()) in
  runner ~signals:[ note ] ~updates:[ suspending; immediate ] (fun () ->
      let result = T.Workflow.sleep root_timer in
      record ("root:timer-" ^ outcome_label result);
      Ok ())

(** Fixture for a local activity whose retry backoff races cancellation. The
    root awaits one local activity; Core owns the attempt counter and asks the
    SDK to start the backoff timer. *)
let local_activity_fixture ~record ~validator_rejects:_ =
  runner (fun () ->
      let result = T.Future.await (T.Activity.start_local race_activity ()) in
      record ("root:local-activity-" ^ outcome_label result);
      Ok ())

(** How the script's [Do_update] jobs request validation. A live delivery
    validates; a Core replay of an already-accepted update does not. *)
type mode = Live | Replay

(** The classified fate of one delivered update after the whole script.

    - [Completed]: accepted and completed; the caller receives the result.
    - [Rejected]: rejected before or after acceptance; the caller receives the
      rejection.
    - [Abandoned]: accepted, and the run closed before the handler finished.
      The worker never completes it; the server fails the caller when the run
      closes.
    - [Pending]: accepted and still running in an open run; the caller is
      legitimately still waiting.
    - [Unanswered]: delivered but neither accepted nor rejected by the
      worker. Only a known gap (#962) may produce this. *)
type update_fate = Completed | Rejected | Abandoned | Pending | Unanswered

(** Renders an update fate for diagnostics. *)
let fate_label = function
  | Completed -> "completed"
  | Rejected -> "rejected"
  | Abandoned -> "abandoned"
  | Pending -> "pending"
  | Unanswered -> "unanswered"

(** One row of the matrix. [script] maps the replay mode to the activations
    Core delivers, each paired with the exact rendered commands expected for
    it. [trace] lists what workflow code observed, in order. [updates] gives
    the expected fate of every delivered update ID. [replayable] marks rows
    whose live updates were all accepted, so a Core replay with validation
    disabled must reproduce the same history. [live_validator_rejects] makes
    validators reject live requests. *)
type case = {
  name : string;
  fixture :
    record:(string -> unit) -> validator_rejects:bool -> runner;
  script : mode -> (Activation.job list * string list) list;
  trace : string list;
  updates : (string * update_fate) list;
  replayable : bool;
  live_validator_rejects : bool;
}

(** Default row fields: no updates, no replay variant, accepting validators. *)
let case ~name ~fixture ?(updates = []) ?(replayable = false)
    ?(live_validator_rejects = false) ~trace script =
  { name; fixture; script; trace; updates; replayable; live_validator_rejects }

(** Formats a list for a mismatch diagnostic. *)
let show values = "[" ^ String.concat "; " values ^ "]"

(** Fails a row with both sides of a mismatch. *)
let expect label expected actual =
  if expected <> actual then
    failwith
      (Printf.sprintf "%s:\n  expected %s\n  actual   %s" label (show expected)
         (show actual))

(** Returns the update protocol IDs delivered by [jobs], in delivery order. *)
let delivered_updates jobs =
  List.filter_map
    (function
      | Activation.Do_update { protocol_instance_id; _ } -> Some protocol_instance_id
      | _ -> None)
    jobs

(** Checks the protocol and terminal invariants of one complete history and
    classifies every delivered update. [history] pairs each activation's jobs
    with the commands it produced. A violation fails the row with [label].

    Invariants:
    - at most one terminal command in the whole history (no conflicting
      completion and cancellation);
    - a terminal command is the last command of its batch, and no later
      activation emits any command;
    - an update's first response arrives in the activation that delivered it
      (the validation stage of the native-interaction design), its phases are
      never repeated, and completion only follows acceptance. *)
let check_history label history =
  let terminals =
    List.concat_map (fun (_, commands) -> List.filter is_terminal commands) history
  in
  if List.length terminals > 1 then
    failwith
      (Printf.sprintf "%s: conflicting terminal commands %s" label
         (show (List.map render terminals)));
  let closed = ref false in
  List.iteri
    (fun index (_, commands) ->
      if !closed && commands <> [] then
        failwith
          (Printf.sprintf "%s: activation %d emitted %s after the terminal command"
             label (index + 1) (show (List.map render commands)));
      match List.rev commands with
      | last :: earlier ->
          if List.exists is_terminal earlier then
            failwith
              (Printf.sprintf "%s: terminal command is not last in activation %d"
                 label (index + 1));
          if is_terminal last then closed := true
      | [] -> ())
    history;
  let responses id =
    List.concat
      (List.mapi
         (fun index (_, commands) ->
           List.filter_map
             (function
               | Activation.Update_response { protocol_instance_id; response }
                 when String.equal protocol_instance_id id ->
                   Some (index, response)
               | _ -> None)
             commands)
         history)
  in
  let delivered =
    List.concat
      (List.mapi
         (fun index (jobs, _) ->
           List.map (fun id -> (id, index)) (delivered_updates jobs))
         history)
  in
  List.map
    (fun (id, delivered_at) ->
      let fate =
        match responses id with
        | [] -> Unanswered
        | (first_at, _) :: _ when first_at <> delivered_at ->
            failwith
              (Printf.sprintf
                 "%s: update %s first answered in activation %d, delivered in %d"
                 label id (first_at + 1) (delivered_at + 1))
        | [ (_, `Rejected _) ] | [ (_, `Accepted); (_, `Rejected _) ] -> Rejected
        | [ (_, `Accepted); (_, `Completed _) ] -> Completed
        | [ (_, `Accepted) ] -> if !closed then Abandoned else Pending
        | _ ->
            failwith
              (Printf.sprintf "%s: update %s has an invalid response sequence"
                 label id)
      in
      (id, fate))
    delivered

(** Runs one row once in [mode] on a fresh execution and returns its rendered
    history and trace after checking the generic invariants. *)
let run_once row mode =
  let label =
    Printf.sprintf "%s (%s)" row.name
      (match mode with Live -> "live" | Replay -> "replay")
  in
  let trace = ref [] in
  let validator_rejects =
    match mode with Live -> row.live_validator_rejects | Replay -> true
  in
  let runner =
    row.fixture ~record:(fun entry -> trace := entry :: !trace)
      ~validator_rejects
  in
  Fun.protect ~finally:runner.shutdown (fun () ->
      let history =
        List.mapi
          (fun index (jobs, expected) ->
            let commands = runner.activate jobs in
            expect
              (Printf.sprintf "%s activation %d" label (index + 1))
              expected (List.map render commands);
            (jobs, commands))
          (row.script mode)
      in
      if runner.task_failed () then
        failwith (label ^ ": a race failed the workflow task");
      let fates = check_history label history in
      expect (label ^ " update fates")
        (List.map (fun (id, fate) -> id ^ "=" ^ fate_label fate) row.updates)
        (List.map (fun (id, fate) -> id ^ "=" ^ fate_label fate) fates);
      expect (label ^ " trace") row.trace (List.rev !trace);
      (List.map (fun (_, commands) -> List.map render commands) history,
       List.rev !trace))

(** Runs a row live twice on fresh executions, as Core redelivery after
    eviction or task failure would, and once as a validation-free replay when
    the row is replayable. All runs must agree exactly. *)
let run_row row =
  let first = run_once row Live in
  if run_once row Live <> first then
    failwith (row.name ^ ": redelivery produced a different history");
  if row.replayable && run_once row Replay <> first then
    failwith (row.name ^ ": validation-free replay produced a different history")

(** Activity completion against workflow cancellation and repeated
    cancellation. Under the immediate-cancellation contract the root never
    observes a result that shares an activation with [Cancel_workflow],
    whichever side of the cancellation Core placed it on. *)
let activity_rows =
  let ok = resolve_activity 1L (Ok unit_payload) in
  [
    case ~name:"activity completes, cancellation arrives later"
      ~fixture:activity_fixture ~trace:[ "root:activity-ok" ] (fun _ ->
        [ ([ start ], [ "schedule-activity(1)" ]);
          ([ ok ], [ "complete-workflow" ]);
          ([ cancel ], []) ]);
    case ~name:"activity completion then cancellation in one activation"
      ~fixture:activity_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "schedule-activity(1)" ]);
          ([ ok; cancel ], [ "cancel-workflow-execution" ]) ]);
    case ~name:"cancellation then activity completion in one activation"
      ~fixture:activity_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "schedule-activity(1)" ]);
          ([ cancel; ok ], [ "cancel-workflow-execution" ]) ]);
    case ~name:"cancellation first, activity completion redelivered later"
      ~fixture:activity_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "schedule-activity(1)" ]);
          ([ cancel ], [ "cancel-workflow-execution" ]);
          ([ ok ], []) ]);
    case ~name:"repeated cancellation requests in one activation"
      ~fixture:activity_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "schedule-activity(1)" ]);
          ([ cancel; cancel ], [ "cancel-workflow-execution" ]);
          ([ cancel ], []) ]);
    (* Core orders the signal before the resolution, but jobs are applied to
       futures before any fiber runs, so the handler finds the activity
       already settled: the cancellation is a no-op and the root observes the
       completion. *)
    case ~name:"activity cancel request and completion in one activation"
      ~fixture:activity_fixture
      ~trace:[ "signal:cancel-activity-ok"; "root:activity-ok" ] (fun _ ->
        [ ([ start ], [ "schedule-activity(1)" ]);
          ([ signal "cancel-activity"; ok ], [ "complete-workflow" ]) ]);
    (* With Try_cancel Core settles the activity as cancelled after the
       request; the root sees exactly that typed result. *)
    case ~name:"activity cancel request, Core reports cancellation"
      ~fixture:activity_fixture
      ~trace:[ "signal:cancel-activity-ok"; "root:activity-error:cancelled" ]
      (fun _ ->
        [ ([ start ], [ "schedule-activity(1)" ]);
          ([ signal "cancel-activity" ], [ "request-cancel-activity(1)" ]);
          ([ resolve_activity 1L (Error cancelled_error) ], [ "complete-workflow" ]) ]);
    (* The activity may finish before the cancellation reaches it; the result
       Core reports wins and no second request is emitted. *)
    case ~name:"activity cancel request, activity completes first"
      ~fixture:activity_fixture
      ~trace:[ "signal:cancel-activity-ok"; "root:activity-ok" ] (fun _ ->
        [ ([ start ], [ "schedule-activity(1)" ]);
          ([ signal "cancel-activity" ], [ "request-cancel-activity(1)" ]);
          ([ ok ], [ "complete-workflow" ]) ]);
    case ~name:"duplicate activity cancel requests emit one command"
      ~fixture:activity_fixture
      ~trace:[ "signal:cancel-activity-ok"; "signal:cancel-activity-ok";
               "root:activity-error:cancelled" ]
      (fun _ ->
        [ ([ start ], [ "schedule-activity(1)" ]);
          ([ signal "cancel-activity"; signal "cancel-activity" ],
           [ "request-cancel-activity(1)" ]);
          ([ resolve_activity 1L (Error cancelled_error) ], [ "complete-workflow" ]) ]);
  ]

(** Timer firing against workflow cancellation. *)
let timer_rows =
  [
    case ~name:"timer fires then cancellation in one activation"
      ~fixture:timer_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "start-timer(1,100ms)" ]);
          ([ fire 1L; cancel ], [ "cancel-workflow-execution" ]);
          ([ fire 1L ], []) ]);
  ]

(** Scope cancellation propagating to an attached activity, against the
    activity's own completion. *)
let scope_rows =
  [
    (* The scope hook buffers the activity cancellation and wakes the root
       locally, so the root may complete before Core settles the activity; the
       late Core result is ignored by the closed run. *)
    case ~name:"scope cancellation propagates to its activity"
      ~fixture:scope_fixture
      ~trace:[ "signal:cancel-scope-ok"; "root:scoped-activity-error:cancelled" ]
      (fun _ ->
        [ ([ start ], [ "schedule-activity(1)" ]);
          ([ signal "cancel-scope" ],
           [ "request-cancel-activity(1)"; "complete-workflow" ]);
          ([ resolve_activity 1L (Error cancelled_error) ], []) ]);
    (* The completion is applied before the handler runs, which removes the
       activity's scope hook: no cancellation command is emitted for settled
       work and the root keeps the result it was already woken with. *)
    case ~name:"scope cancellation and activity completion in one activation"
      ~fixture:scope_fixture
      ~trace:[ "signal:cancel-scope-ok"; "root:scoped-activity-ok" ] (fun _ ->
        [ ([ start ], [ "schedule-activity(1)" ]);
          ([ signal "cancel-scope"; resolve_activity 1L (Ok unit_payload) ],
           [ "complete-workflow" ]) ]);
  ]

(** Child completion and start failure against parent cancellation and scope
    cancellation. Immediate parent cancellation emits no child cancellation;
    the child's parent close policy applies on the server. *)
let child_rows =
  let ok = child_resolved 1L (Ok unit_payload) in
  [
    case ~name:"child completes, parent cancellation arrives later"
      ~fixture:child_fixture ~trace:[ "root:child-ok" ] (fun _ ->
        [ ([ start ], [ "start-child(1,race-child)" ]);
          ([ child_started 1L ], []);
          ([ ok ], [ "complete-workflow" ]);
          ([ cancel ], []) ]);
    case ~name:"child completion then parent cancellation in one activation"
      ~fixture:child_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "start-child(1,race-child)" ]);
          ([ child_started 1L; ok; cancel ], [ "cancel-workflow-execution" ]) ]);
    case ~name:"parent cancellation then child completion in one activation"
      ~fixture:child_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "start-child(1,race-child)" ]);
          ([ child_started 1L ], []);
          ([ cancel; ok ], [ "cancel-workflow-execution" ]) ]);
    case ~name:"child start failure and parent cancellation in one activation"
      ~fixture:child_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "start-child(1,race-child)" ]);
          ([ child_start_failed 1L; cancel ], [ "cancel-workflow-execution" ]) ]);
    case ~name:"parent cancelled before the child start is acknowledged"
      ~fixture:child_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "start-child(1,race-child)" ]);
          ([ cancel ], [ "cancel-workflow-execution" ]);
          ([ child_started 1L ], []);
          ([ ok ], []) ]);
    case ~name:"scope cancellation propagates to its child"
      ~fixture:child_fixture
      ~trace:[ "signal:cancel-scope-ok"; "root:child-error:cancelled" ]
      (fun _ ->
        [ ([ start ], [ "start-child(1,race-child)" ]);
          ([ child_started 1L ], []);
          ([ signal "cancel-scope" ], [ "cancel-child(1)"; "complete-workflow" ]);
          ([ child_resolved 1L (Error cancelled_error) ], []) ]);
    case ~name:"scope cancellation and child completion in one activation"
      ~fixture:child_fixture
      ~trace:[ "signal:cancel-scope-ok"; "root:child-ok" ] (fun _ ->
        [ ([ start ], [ "start-child(1,race-child)" ]);
          ([ child_started 1L ], []);
          ([ signal "cancel-scope"; ok ], [ "complete-workflow" ]) ]);
    case ~name:"scope cancellation and child start failure in one activation"
      ~fixture:child_fixture
      ~trace:[ "signal:cancel-scope-ok"; "root:child-error:child_workflow" ]
      (fun _ ->
        [ ([ start ], [ "start-child(1,race-child)" ]);
          ([ signal "cancel-scope"; child_start_failed 1L ],
           [ "complete-workflow" ]) ]);
  ]

(** Signal and update handlers against workflow completion and cancellation.
    Update jobs follow [mode]: a replay delivers them with validation
    disabled. *)
let handler_rows =
  let suspending mode =
    update ~run_validator:(mode = Live) "suspending-update" "u1"
  in
  let immediate mode =
    update ~run_validator:(mode = Live) "immediate-update" "u1"
  in
  [
    case ~name:"suspended update finishes before the root completes"
      ~fixture:handler_fixture ~replayable:true ~updates:[ ("u1", Completed) ]
      ~trace:[ "update:start"; "update:finish-ok"; "root:timer-ok" ]
      (fun mode ->
        [ ([ start ], [ "start-timer(1,100ms)" ]);
          ([ suspending mode ], [ "update-accepted(u1)"; "start-timer(2,10ms)" ]);
          ([ fire 2L ], [ "update-completed(u1)" ]);
          ([ fire 1L ], [ "complete-workflow" ]) ]);
    case ~name:"handler and root timers fire together, handler first"
      ~fixture:handler_fixture ~replayable:true ~updates:[ ("u1", Completed) ]
      ~trace:[ "update:start"; "update:finish-ok"; "root:timer-ok" ]
      (fun mode ->
        [ ([ start ], [ "start-timer(1,100ms)" ]);
          ([ suspending mode ], [ "update-accepted(u1)"; "start-timer(2,10ms)" ]);
          ([ fire 2L; fire 1L ], [ "update-completed(u1)"; "complete-workflow" ]) ]);
    (* Known gap #962: the handler's continuation is already runnable in this
       activation, but the root's terminal command seals the run first, so the
       accepted update is abandoned instead of completed. *)
    case ~name:"handler and root timers fire together, root first"
      ~fixture:handler_fixture ~replayable:true ~updates:[ ("u1", Abandoned) ]
      ~trace:[ "update:start"; "root:timer-ok" ]
      (fun mode ->
        [ ([ start ], [ "start-timer(1,100ms)" ]);
          ([ suspending mode ], [ "update-accepted(u1)"; "start-timer(2,10ms)" ]);
          ([ fire 1L; fire 2L ], [ "complete-workflow" ]) ]);
    case ~name:"suspended update when the root completes"
      ~fixture:handler_fixture ~replayable:true ~updates:[ ("u1", Abandoned) ]
      ~trace:[ "update:start"; "root:timer-ok" ]
      (fun mode ->
        [ ([ start ], [ "start-timer(1,100ms)" ]);
          ([ suspending mode ], [ "update-accepted(u1)"; "start-timer(2,10ms)" ]);
          ([ fire 1L ], [ "complete-workflow" ]);
          ([ fire 2L ], []) ]);
    case ~name:"suspended update when the workflow is cancelled"
      ~fixture:handler_fixture ~replayable:true ~updates:[ ("u1", Abandoned) ]
      ~trace:[ "update:start" ]
      (fun mode ->
        [ ([ start ], [ "start-timer(1,100ms)" ]);
          ([ suspending mode ], [ "update-accepted(u1)"; "start-timer(2,10ms)" ]);
          ([ cancel ], [ "cancel-workflow-execution" ]) ]);
    case ~name:"suspended update stays pending while the run is open"
      ~fixture:handler_fixture ~replayable:true ~updates:[ ("u1", Pending) ]
      ~trace:[ "update:start" ]
      (fun mode ->
        [ ([ start ], [ "start-timer(1,100ms)" ]);
          ([ suspending mode ], [ "update-accepted(u1)"; "start-timer(2,10ms)" ]) ]);
    (* Core orders updates before the timer resolution; the immediate handler
       runs before the woken root and both responses precede the terminal. *)
    case ~name:"immediate update and root completion in one activation"
      ~fixture:handler_fixture ~replayable:true ~updates:[ ("u1", Completed) ]
      ~trace:[ "update:immediate"; "root:timer-ok" ]
      (fun mode ->
        [ ([ start ], [ "start-timer(1,100ms)" ]);
          ([ immediate mode; fire 1L ],
           [ "update-accepted(u1)"; "update-completed(u1)"; "complete-workflow" ]) ]);
    case ~name:"rejected update and root completion in one activation"
      ~fixture:handler_fixture ~live_validator_rejects:true
      ~updates:[ ("u1", Rejected) ] ~trace:[ "root:timer-ok" ]
      (fun mode ->
        [ ([ start ], [ "start-timer(1,100ms)" ]);
          ([ immediate mode; fire 1L ],
           [ "update-rejected(u1,update)"; "complete-workflow" ]) ]);
    case ~name:"signal and root completion in one activation"
      ~fixture:handler_fixture ~trace:[ "signal:note"; "root:timer-ok" ]
      (fun _ ->
        [ ([ start ], [ "start-timer(1,100ms)" ]);
          ([ signal "note"; fire 1L ], [ "complete-workflow" ]) ]);
    (* Known gap #962: Core orders the update before the cancellation, but the
       cancellation seals the run while jobs are applied, so the handler is
       never invoked and the update gets no validation response. *)
    case ~name:"update and cancellation in one activation"
      ~fixture:handler_fixture ~updates:[ ("u1", Unanswered) ] ~trace:[]
      (fun mode ->
        [ ([ start ], [ "start-timer(1,100ms)" ]);
          ([ suspending mode; cancel ], [ "cancel-workflow-execution" ]) ]);
    (* Known gap #962: the same ordering drops a signal handler invocation. *)
    case ~name:"signal and cancellation in one activation"
      ~fixture:handler_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "start-timer(1,100ms)" ]);
          ([ signal "note"; cancel ], [ "cancel-workflow-execution" ]) ]);
  ]

(** Local-activity retry backoff against cancellation. Core owns the attempt
    counter; the SDK owns the backoff timer and the rescheduled attempt. Core
    places local-activity jobs after the cancellation in a shared
    activation. *)
let retry_rows =
  let backoff = local_backoff 1L ~attempt:2L ~milliseconds:1_000L in
  [
    case ~name:"cancellation then local-activity backoff in one activation"
      ~fixture:local_activity_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "schedule-local-activity(1,attempt=1)" ]);
          ([ cancel; backoff ], [ "cancel-workflow-execution" ]) ]);
    case ~name:"cancellation while the backoff timer is pending"
      ~fixture:local_activity_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "schedule-local-activity(1,attempt=1)" ]);
          ([ backoff ], [ "start-timer(2,1000ms)" ]);
          ([ cancel ], [ "cancel-workflow-execution" ]);
          ([ fire 2L ], []) ]);
    case ~name:"backoff timer and cancellation in one activation"
      ~fixture:local_activity_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "schedule-local-activity(1,attempt=1)" ]);
          ([ backoff ], [ "start-timer(2,1000ms)" ]);
          ([ fire 2L; cancel ], [ "cancel-workflow-execution" ]) ]);
    case ~name:"retried attempt result reaches the root once"
      ~fixture:local_activity_fixture ~trace:[ "root:local-activity-ok" ]
      (fun _ ->
        [ ([ start ], [ "schedule-local-activity(1,attempt=1)" ]);
          ([ backoff ], [ "start-timer(2,1000ms)" ]);
          ([ fire 2L ], [ "schedule-local-activity(1,attempt=2)" ]);
          ([ resolve_activity 1L (Ok unit_payload) ], [ "complete-workflow" ]);
          ([ cancel ], []) ]);
    case ~name:"cancellation then retried attempt result in one activation"
      ~fixture:local_activity_fixture ~trace:[] (fun _ ->
        [ ([ start ], [ "schedule-local-activity(1,attempt=1)" ]);
          ([ backoff ], [ "start-timer(2,1000ms)" ]);
          ([ fire 2L ], [ "schedule-local-activity(1,attempt=2)" ]);
          ([ cancel; resolve_activity 1L (Ok unit_payload) ],
           [ "cancel-workflow-execution" ]) ]);
  ]

(** Proves that the generic history checks are not vacuous: each synthetic
    history below violates exactly one invariant and must be rejected. These
    are the failure shapes the issue asks the matrix to detect: conflicting
    terminal commands, durable commands after the run closed, and update
    responses that are late, repeated, or out of protocol order. *)
let test_checker_rejects_violations () =
  let accepted id =
    Activation.Update_response { protocol_instance_id = id; response = `Accepted }
  in
  let completed id =
    Activation.Update_response
      { protocol_instance_id = id; response = `Completed unit_payload }
  in
  let complete = Activation.Complete_workflow unit_payload in
  let timer = Activation.Start_timer { seq = 1L; milliseconds = 1L } in
  let deliver = update ~run_validator:true "suspending-update" "u1" in
  let violations =
    [ ("conflicting terminals",
       [ ([ cancel ], [ complete; Activation.Cancel_workflow_execution ]) ]);
      ("terminal not last", [ ([ start ], [ complete; timer ]) ]);
      ("command after terminal", [ ([ start ], [ complete ]); ([ fire 1L ], [ timer ]) ]);
      ("late validation response", [ ([ deliver ], []); ([ fire 1L ], [ accepted "u1" ]) ]);
      ("completion without acceptance", [ ([ deliver ], [ completed "u1" ]) ]);
      ("repeated acceptance", [ ([ deliver ], [ accepted "u1"; accepted "u1" ]) ]) ]
  in
  List.iter
    (fun (label, history) ->
      match check_history label history with
      | exception Failure _ -> ()
      | _ -> failwith ("history checker accepted a violation: " ^ label))
    violations

(** Runs the checker self-test and every row, and reports the matrix size. *)
let () =
  test_checker_rejects_violations ();
  let rows =
    List.concat
      [ activity_rows; timer_rows; scope_rows; child_rows; handler_rows;
        retry_rows ]
  in
  List.iter run_row rows;
  Printf.printf "race matrix: %d rows ok\n" (List.length rows)
