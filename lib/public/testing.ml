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

(** Packs an activity, choosing the engine's placeholder for definitions that
    cannot run synchronously in-process. *)
let register_activity ~override definition =
  let name = Activity.name definition in
  match
    ( Activity.implementation_with_context definition,
      Activity.implementation definition,
      Activity.implementation_async definition )
  with
  | Some _, _, _ | None, Some _, _ ->
      Engine.activity ~override (Activity_private.to_base definition)
  | None, None, Some _ ->
      Engine.unsupported_activity ~override ~name
        ~reason:
          ("asynchronous activity " ^ name
         ^ " cannot run in Temporal.Testing; register a \
            Temporal.Testing.mock_activity for it")
        ()
  | None, None, None ->
      Engine.unsupported_activity ~override ~name
        ~reason:
          ("activity " ^ name
         ^ " is a remote reference with no implementation; register a \
            Temporal.Testing.mock_activity for it")
        ()

let activity definition = register_activity ~override:false definition

let mock_activity definition implementation =
  Activity.define ~name:(Activity.name definition)
    ~input:(Activity.input definition) ~output:(Activity.output definition)
    implementation
  |> register_activity ~override:true

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

let create ?namespace ?task_queue ?start_time ?max_activity_attempts ~workflows
    ~activities () =
  Engine.create ?namespace ?task_queue
    ?start_time_ms:(Option.map milliseconds_of_time start_time)
    ?max_activity_attempts ~workflows ~activities ()
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

let start ?id environment definition input =
  match Codec.encode (Workflow.input definition) input with
  | Error error -> Error error
  | Ok payload ->
      Engine.start ?workflow_id:id environment
        ~workflow_type:(Workflow.name definition)
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

let execute ?id ?timeout environment definition input =
  Result.bind (start ?id environment definition input) (result ?timeout)

(** Encodes one typed value as the argument list the runtime delivers, using
    the same unit-as-no-arguments convention as Temporal commands. *)
let arguments codec value =
  Codec.encode codec value
  |> Result.map (fun payload ->
         Temporal_base.Payload.input_arguments (Payload_private.to_base payload))

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
