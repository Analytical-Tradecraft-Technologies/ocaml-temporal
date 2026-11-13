(** Public adapter over the private deterministic test engine. This module
    owns only typed conversions: codecs, public/base errors and payloads, and
    interaction handlers. Every simulated server behavior lives in
    [Temporal_sdk_kernel.Test_environment]. *)

module Engine = Temporal_sdk_kernel.Test_environment
module Execution = Temporal_sdk_kernel.Execution

type registered_workflow = Engine.workflow
type registered_activity = Engine.activity

(** Converts a public signal handler into the runtime callback. The handler
    owns the payload-arity policy, as on the native worker path. *)
let runtime_signal_handler (handler : Signal.Handler.t) =
  Execution.make_signal_handler ~name:(Signal.Handler.name handler)
    ~dispatch:(fun (signal : Execution.signal) ->
      List.map Payload_private.of_base signal.input
      |> Signal.Handler.dispatch_payloads handler
      |> Result.map_error Error_private.to_base)

(** Converts a public query handler into the synchronous runtime callback. *)
let runtime_query_handler (handler : Query.Handler.t) =
  Execution.make_query_handler ~name:(Query.Handler.name handler)
    ~dispatch:(fun (query : Execution.query) ->
      List.map Payload_private.of_base query.arguments
      |> Query.Handler.dispatch_payloads handler
      |> Result.map Payload_private.to_base
      |> Result.map_error Error_private.to_base)

(** Converts a public update handler into the runtime callback, forwarding
    the validator switch and acceptance hook unchanged. *)
let runtime_update_handler (handler : Update.Handler.t) =
  Execution.make_update_handler ~name:(Update.Handler.name handler)
    ~dispatch:(fun ~run_validator ~on_validated (update : Execution.update) ->
      List.map Payload_private.of_base update.input
      |> Update.Handler.dispatch_payloads ~run_validator ~on_validated handler
      |> Result.map Payload_private.to_base
      |> Result.map_error Error_private.to_base)

(** Packs a workflow and its handlers; [override] marks a replacement. A
    remote reference is registered with a body that fails as a defect naming
    the fix, rather than the runtime's generic missing-implementation error. *)
let register_workflow ~override ~signals ~queries ~updates definition =
  let definition =
    match Workflow.implementation definition with
    | Some _ -> definition
    | None ->
        let name = Workflow.name definition in
        Workflow.define ~name ~input:(Workflow.input definition)
          ~output:(Workflow.output definition) (fun _ ->
            Error
              (Error.defect
                 ~message:
                   ("workflow " ^ name
                  ^ " is a remote reference with no implementation; register \
                     a Temporal.Testing.mock_workflow for it")))
  in
  Engine.workflow ~override
    ~signal_handlers:(List.map runtime_signal_handler signals)
    ~query_handlers:(List.map runtime_query_handler queries)
    ~update_handlers:(List.map runtime_update_handler updates)
    (Workflow_private.to_base definition)

let workflow ?(signals = []) ?(queries = []) ?(updates = []) definition =
  register_workflow ~override:false ~signals ~queries ~updates definition

let mock_workflow ?(signals = []) ?(queries = []) ?(updates = []) definition
    implementation =
  Workflow.define ~name:(Workflow.name definition)
    ~input:(Workflow.input definition) ~output:(Workflow.output definition)
    implementation
  |> register_workflow ~override:true ~signals ~queries ~updates

(** Packs an activity: synchronous and asynchronous callbacks run in the
    engine, and a remote reference becomes the engine's placeholder, which
    fails every attempt at once and so ignores [durations], the virtual
    attempt durations. *)
let register_activity ~override ?(durations = []) definition =
  let name = Activity.name definition in
  let durations_ms = List.map Duration.to_ms durations in
  match
    ( Activity.implementation_with_context definition,
      Activity.implementation definition,
      Activity.implementation_async definition )
  with
  | Some _, _, _ | None, Some _, _ ->
      Engine.activity ~override ~durations_ms (Activity_private.to_base definition)
  | None, None, Some _ ->
      Engine.async_activity ~override ~durations_ms
        (Activity_private.to_base_async definition)
  | None, None, None ->
      Engine.unsupported_activity ~override ~name
        ~reason:
          ("activity " ^ name
         ^ " is a remote reference with no implementation; register a \
            Temporal.Testing.mock_activity for it")
        ()

let activity ?durations definition =
  register_activity ~override:false ?durations definition

let mock_activity ?durations definition implementation =
  Activity.define ~name:(Activity.name definition)
    ~input:(Activity.input definition) ~output:(Activity.output definition)
    implementation
  |> register_activity ~override:true ?durations

let mock_async_activity ?durations definition implementation =
  Activity.define_async ~name:(Activity.name definition)
    ~input:(Activity.input definition) ~output:(Activity.output definition)
    implementation
  |> register_activity ~override:true ?durations

type workflow_task_failure = [ `Fail_run | `Retry ]

type t = Engine.t

type ('input, 'output) handle = {
  engine : Engine.handle;
  (* Decodes the output of the chain's last run with the definition the
     caller started, as [Temporal.Client.wait] does. *)
  output : 'output Codec.t;
}

(** Converts a public instant to whole virtual milliseconds. Sub-millisecond
    precision is rounded down because virtual time has millisecond
    resolution, matching timer durations. *)
let milliseconds_of_time time =
  Int64.add
    (Int64.mul (Time.seconds time) 1_000L)
    (Int64.of_int (Time.nanoseconds time / 1_000_000))

let create ?namespace ?task_queue ?start_time ?max_activity_attempts
    ?workflow_task_failure ~workflows ~activities () =
  Engine.create ?namespace ?task_queue
    ?start_time_ms:(Option.map milliseconds_of_time start_time)
    ?max_activity_attempts
    ?task_failure_mode:
      (Option.map
         (function `Fail_run -> Engine.Fail_run | `Retry -> Engine.Retry_task)
         workflow_task_failure)
    ~workflows ~activities ()
  |> Result.map_error Error_private.of_base

let shutdown = Engine.shutdown

let now environment =
  let milliseconds = Engine.now_ms environment in
  match
    Time.of_unix
      ~seconds:(Int64.div milliseconds 1_000L)
      ~nanoseconds:(Int64.to_int (Int64.rem milliseconds 1_000L) * 1_000_000)
  with
  | Ok time -> time
  (* The engine keeps virtual time non-negative, so the fraction is always
     in range; reaching this branch is an engine invariant violation. *)
  | Error _ -> invalid_arg "Temporal.Testing: virtual time is out of range"

let skip environment duration =
  Engine.skip environment ~milliseconds:(Duration.to_ms duration)
  |> Result.map_error Error_private.of_base

(** Maps the client's ID reuse policy to the engine's, which shares the
    runtime type child workflow commands carry. *)
let engine_id_reuse_policy : Client.id_reuse_policy -> _ = function
  | `Allow_duplicate -> Temporal_sdk_kernel.Activation.Reuse_allow_duplicate
  | `Allow_duplicate_failed_only ->
      Temporal_sdk_kernel.Activation.Reuse_allow_duplicate_failed_only
  | `Reject_duplicate -> Temporal_sdk_kernel.Activation.Reuse_reject_duplicate

(** Maps the client's ID conflict policy to the engine's. *)
let engine_id_conflict_policy : Client.id_conflict_policy -> _ = function
  | `Fail -> Engine.Conflict_fail
  | `Use_existing -> Engine.Conflict_use_existing
  | `Terminate_existing -> Engine.Conflict_terminate_existing

(** Converts a public start map to the engine's payloads. *)
let base_map values =
  List.map (fun (key, payload) -> (key, Payload_private.to_base payload)) values

let start ?id ?memo ?search_attributes ?id_conflict_policy ?id_reuse_policy
    ?execution_timeout ?run_timeout ?task_timeout ?retry_policy environment
    definition input =
  let ( let* ) = Result.bind in
  (* The same rules as [Client.start] and child starts, so a value the
     server would reject or silently lower is a defect here too. *)
  let* execution_timeout_ms, run_timeout_ms, task_timeout_ms =
    Workflow_timeouts_private.validate ~execution_timeout ~run_timeout
      ~task_timeout
  in
  let* payload = Codec.encode (Workflow.input definition) input in
  let namespace = Engine.namespace environment in
  Engine.start ?workflow_id:id
    ?memo:(Option.map base_map memo)
    ?search_attributes:(Option.map base_map search_attributes)
    ?id_reuse_policy:(Option.map engine_id_reuse_policy id_reuse_policy)
    ?id_conflict_policy:(Option.map engine_id_conflict_policy id_conflict_policy)
    ?execution_timeout_ms ?run_timeout_ms ?task_timeout_ms
    ?retry_policy:(Option.map Retry_policy_private.to_runtime retry_policy)
    ~already_started:(fun ~workflow_id ~run_id ->
      Backend.already_started_error ~namespace ~workflow_id
        ~existing_run_id:(Some run_id)
      |> Error_private.to_base)
    environment ~workflow_type:(Workflow.name definition)
    ~input:(Payload_private.to_base payload)
  |> Result.map (fun engine -> { engine; output = Workflow.output definition })
  |> Result.map_error Error_private.of_base

(** Decodes an engine payload result through a public codec. *)
let decode codec = function
  | Ok payload -> Codec.decode codec (Payload_private.of_base payload)
  | Error error -> Error (Error_private.of_base error)

let result ?timeout handle =
  Engine.result ?timeout_ms:(Option.map Duration.to_ms timeout) handle.engine
  |> decode handle.output

let execute ?id ?memo ?search_attributes ?id_conflict_policy ?id_reuse_policy
    ?execution_timeout ?run_timeout ?task_timeout ?retry_policy ?timeout
    environment definition input =
  Result.bind
    (start ?id ?memo ?search_attributes ?id_conflict_policy ?id_reuse_policy
       ?execution_timeout ?run_timeout ?task_timeout ?retry_policy environment
       definition input)
    (result ?timeout)

(** Encodes one typed value as the argument list the runtime delivers, using
    the same codec-decided rule as Temporal commands: zero payloads for
    [Codec.unit], one payload otherwise (#1012). *)
let arguments codec value =
  Codec_private.encode_arguments codec value
  |> Result.map_error Error_private.of_base

let signal handle signal value =
  Result.bind (arguments (Signal.input signal) value) (fun input ->
      Engine.signal handle.engine ~name:(Signal.name signal) ~input
      |> Result.map_error Error_private.of_base)

let query handle query =
  Engine.query handle.engine ~name:(Query.name query) ~arguments:[]
  |> decode (Query.output query)

let query_with_input handle query value =
  Result.bind (Codec.encode (Query.input query) value) (fun payload ->
      Engine.query handle.engine
        ~name:(Query.name_with_input query)
        ~arguments:[ Payload_private.to_base payload ]
      |> decode (Query.output_with_input query))

let update ?timeout handle update value =
  Result.bind (arguments (Update.input update) value) (fun input ->
      Engine.update
        ?timeout_ms:(Option.map Duration.to_ms timeout)
        handle.engine ~name:(Update.name update) ~input
      |> decode (Update.output update))

let cancel handle =
  Engine.cancel handle.engine |> Result.map_error Error_private.of_base

let workflow_id handle = Engine.workflow_id handle.engine
let run_id handle = Engine.run_id handle.engine
let started handle = Engine.started handle.engine

let workflow_task_failure handle =
  Option.map Error_private.of_base (Engine.workflow_task_failure handle.engine)

let retry_workflow_tasks environment =
  Engine.retry_workflow_tasks environment |> Result.map_error Error_private.of_base

(** Converts the engine's sorted, copied map to public payloads. *)
let public_map values =
  List.map (fun (key, payload) -> (key, Payload_private.of_base payload)) values

let memo handle = public_map (Engine.memo handle.engine)
let search_attributes handle = public_map (Engine.search_attributes handle.engine)
