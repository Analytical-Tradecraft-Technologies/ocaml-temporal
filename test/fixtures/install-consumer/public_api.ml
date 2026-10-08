(* This module is a compile-time compatibility witness for the installed
   package.  The annotations deliberately spell out the supported types at
   the consumer boundary instead of inferring them from [Temporal] itself.
   A removed value, changed label, changed result type, or accidentally hidden
   module therefore fails the installed-consumer build before a release can be
   published.  The witness has no side effects and is not a runtime test. *)

module T = Temporal

(* The root module is the public allow-list.  These aliases make every intended
   module name part of the consumer compilation while private implementation
   modules remain absent from the fixture's include path. *)
module Activity = T.Activity
module Child_workflow = T.Child_workflow
module Client = T.Client
module Codec = T.Codec
module Condition = T.Condition
module Duration = T.Duration
module Error = T.Error
module Future = T.Future
module Interaction = T.Interaction
module Payload = T.Payload
module Query = T.Query
module Result_syntax = T.Result_syntax
module Runtime = T.Runtime
module Runtime_info = T.Runtime_info
module Scope = T.Scope
module Signal = T.Signal
module Testing = T.Testing
module Time = T.Time
module Update = T.Update
module Worker = T.Worker
module Workflow = T.Workflow
module Workflow_context = T.Workflow_context

(* Core value definitions and their accessors are the stable authoring
   boundary.  Keep the type variables explicit: an annotation that silently
   becomes monomorphic would make this witness weaker than a real consumer. *)
let _activity_define :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    ('input -> ('output, T.Error.t) result) ->
    ('input, 'output) T.Activity.t =
  T.Activity.define

let _activity_define_with_context :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    (T.Activity.context ->
     'input ->
     ('output, T.Error.t) result) ->
    ('input, 'output) T.Activity.t =
  T.Activity.define_with_context

let _activity_remote :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    ('input, 'output) T.Activity.t =
  T.Activity.remote

let _activity_define_async :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    ('input, 'output) T.Activity.async_implementation ->
    ('input, 'output) T.Activity.t =
  T.Activity.define_async

let _activity_name : ('input, 'output) T.Activity.t -> string = T.Activity.name

let _activity_input :
    ('input, 'output) T.Activity.t -> 'input T.Codec.t =
  T.Activity.input

let _activity_output :
    ('input, 'output) T.Activity.t -> 'output T.Codec.t =
  T.Activity.output

let _activity_implementation :
    ('input, 'output) T.Activity.t ->
    ('input, 'output) T.Activity.implementation option =
  T.Activity.implementation

let _activity_implementation_with_context :
    ('input, 'output) T.Activity.t ->
    ('input, 'output) T.Activity.contextual_implementation option =
  T.Activity.implementation_with_context

let _activity_implementation_async :
    ('input, 'output) T.Activity.t ->
    ('input, 'output) T.Activity.async_implementation option =
  T.Activity.implementation_async

let _activity_async_handle_complete :
    'output T.Activity.async_handle -> 'output -> (unit, T.Error.t) result =
  T.Activity.Async_handle.complete

let _activity_async_handle_fail :
    'output T.Activity.async_handle -> T.Error.t -> (unit, T.Error.t) result =
  T.Activity.Async_handle.fail

let _activity_async_handle_cancel :
    'output T.Activity.async_handle -> T.Payload.t list -> (unit, T.Error.t) result =
  T.Activity.Async_handle.cancel

let _activity_async_handle_heartbeat :
    'output T.Activity.async_handle -> T.Payload.t list -> (unit, T.Error.t) result =
  T.Activity.Async_handle.heartbeat

let _activity_async_context_handle :
    'output T.Activity.async_context -> 'output T.Activity.async_handle =
  T.Activity.Async_context.handle

let _activity_context_heartbeat_payloads :
    T.Activity.context -> T.Payload.t list -> (unit, T.Error.t) result =
  T.Activity.Context.heartbeat_payloads

let _activity_context_heartbeat :
    T.Activity.context -> 'a T.Codec.t -> 'a -> (unit, T.Error.t) result =
  T.Activity.Context.heartbeat

let _activity_context_details : T.Activity.context -> T.Payload.t list =
  T.Activity.Context.details

let _activity_context_heartbeat_timeout :
    T.Activity.context -> T.Duration.t option =
  T.Activity.Context.heartbeat_timeout

let _activity_context_info :
    T.Activity.context -> (T.Activity.Info.t, T.Error.t) result =
  T.Activity.Context.info

let _activity_info_namespace : T.Activity.Info.t -> string =
  T.Activity.Info.namespace

let _activity_info_workflow :
    T.Activity.Info.t -> T.Activity.Info.workflow =
  T.Activity.Info.workflow

let _activity_info_activity_id : T.Activity.Info.t -> string =
  T.Activity.Info.activity_id

let _activity_info_activity_type : T.Activity.Info.t -> string =
  T.Activity.Info.activity_type

let _activity_info_attempt : T.Activity.Info.t -> int = T.Activity.Info.attempt
let _activity_info_is_local : T.Activity.Info.t -> bool = T.Activity.Info.is_local

let _activity_info_scheduled_time : T.Activity.Info.t -> T.Time.t option =
  T.Activity.Info.scheduled_time

let _activity_info_current_attempt_scheduled_time :
    T.Activity.Info.t -> T.Time.t option =
  T.Activity.Info.current_attempt_scheduled_time

let _activity_info_started_time : T.Activity.Info.t -> T.Time.t option =
  T.Activity.Info.started_time

let _activity_info_schedule_to_close_timeout :
    T.Activity.Info.t -> T.Duration.t option =
  T.Activity.Info.schedule_to_close_timeout

let _activity_info_start_to_close_timeout :
    T.Activity.Info.t -> T.Duration.t option =
  T.Activity.Info.start_to_close_timeout

let _activity_info_heartbeat_timeout : T.Activity.Info.t -> T.Duration.t option =
  T.Activity.Info.heartbeat_timeout

let _activity_async_context_info :
    unit T.Activity.async_context -> (T.Activity.Info.t, T.Error.t) result =
  T.Activity.Async_context.info

(* Record fields of the public workflow identity are part of the contract. *)
let _activity_info_workflow_fields
    ({ workflow_id; run_id; workflow_type } : T.Activity.Info.workflow) =
  ignore (workflow_id : string);
  ignore (run_id : string);
  ignore (workflow_type : string)

let _activity_execute :
    ?scope:T.Scope.t ->
    ?activity_id:string ->
    ?task_queue:string ->
    ?schedule_to_close_timeout:T.Duration.t ->
    ?schedule_to_start_timeout:T.Duration.t ->
    ?start_to_close_timeout:T.Duration.t ->
    ?heartbeat_timeout:T.Duration.t ->
    ?retry_policy:T.Activity.Retry_policy.t ->
    ?priority:T.Activity.Priority.t ->
    ?cancellation_type:T.Activity.cancellation_type ->
    ?do_not_eagerly_execute:bool ->
    ('input, 'output) T.Activity.t ->
    'input ->
    ('output, T.Error.t) result =
  T.Activity.execute

let _activity_future :
    'output T.Activity.handle -> ('output, T.Error.t) T.Future.t =
  T.Activity.future

let _activity_cancel :
    'output T.Activity.handle -> (unit, T.Error.t) result =
  T.Activity.cancel

let _activity_heartbeat :
    T.Activity.context -> 'a T.Codec.t -> 'a -> (unit, T.Error.t) result =
  T.Activity.heartbeat

let _activity_retry_policy_make :
    initial_interval:T.Duration.t ->
    backoff_coefficient:float ->
    maximum_interval:T.Duration.t ->
    maximum_attempts:int ->
    ?non_retryable_error_types:string list ->
    unit -> (T.Activity.Retry_policy.t, T.Error.t) result =
  T.Activity.Retry_policy.make

let _activity_retry_policy_create :
    initial_interval:T.Duration.t ->
    backoff_coefficient:float ->
    maximum_interval:T.Duration.t ->
    maximum_attempts:int ->
    ?non_retryable_error_types:string list ->
    unit -> (T.Activity.Retry_policy.t, T.Error.t) result =
  T.Activity.Retry_policy.create

let _activity_retry_policy_initial_interval :
    T.Activity.Retry_policy.t -> T.Duration.t =
  T.Activity.Retry_policy.initial_interval

let _activity_retry_policy_backoff :
    T.Activity.Retry_policy.t -> float =
  T.Activity.Retry_policy.backoff_coefficient

let _activity_retry_policy_maximum_interval :
    T.Activity.Retry_policy.t -> T.Duration.t =
  T.Activity.Retry_policy.maximum_interval

let _activity_retry_policy_maximum_attempts :
    T.Activity.Retry_policy.t -> int =
  T.Activity.Retry_policy.maximum_attempts

let _activity_retry_policy_non_retryable :
    T.Activity.Retry_policy.t -> string list =
  T.Activity.Retry_policy.non_retryable_error_types

let _activity_start_handle :
    ?scope:T.Scope.t ->
    ?activity_id:string ->
    ?task_queue:string ->
    ?schedule_to_close_timeout:T.Duration.t ->
    ?schedule_to_start_timeout:T.Duration.t ->
    ?start_to_close_timeout:T.Duration.t ->
    ?heartbeat_timeout:T.Duration.t ->
    ?retry_policy:T.Activity.Retry_policy.t ->
    ?priority:T.Activity.Priority.t ->
    ?cancellation_type:T.Activity.cancellation_type ->
    ?do_not_eagerly_execute:bool ->
    ('input, 'output) T.Activity.t ->
    'input -> 'output T.Activity.handle =
  T.Activity.start_handle

let _activity_start :
    ?scope:T.Scope.t ->
    ?activity_id:string ->
    ?task_queue:string ->
    ?schedule_to_close_timeout:T.Duration.t ->
    ?schedule_to_start_timeout:T.Duration.t ->
    ?start_to_close_timeout:T.Duration.t ->
    ?heartbeat_timeout:T.Duration.t ->
    ?retry_policy:T.Activity.Retry_policy.t ->
    ?priority:T.Activity.Priority.t ->
    ?cancellation_type:T.Activity.cancellation_type ->
    ?do_not_eagerly_execute:bool ->
    ('input, 'output) T.Activity.t ->
    'input -> ('output, T.Error.t) T.Future.t =
  T.Activity.start

let _child_start :
    ?scope:T.Scope.t ->
    ?cancellation_type:T.Child_workflow.cancellation_type ->
    ?retry_policy:T.Activity.Retry_policy.t ->
    ?task_queue:string ->
    ?parent_close_policy:T.Child_workflow.Parent_close_policy.t ->
    id:string ->
    ('input, 'output) T.Workflow.t ->
    'input ->
    ('output, T.Error.t) T.Future.t =
  T.Child_workflow.start

let _child_start_handle :
    ?scope:T.Scope.t ->
    ?cancellation_type:T.Child_workflow.cancellation_type ->
    ?retry_policy:T.Activity.Retry_policy.t ->
    ?task_queue:string ->
    ?parent_close_policy:T.Child_workflow.Parent_close_policy.t ->
    id:string ->
    ('input, 'output) T.Workflow.t ->
    'input -> 'output T.Child_workflow.handle =
  T.Child_workflow.start_handle

let _child_future :
    'output T.Child_workflow.handle -> ('output, T.Error.t) T.Future.t =
  T.Child_workflow.future

let _child_cancel :
    ?reason:string ->
    'output T.Child_workflow.handle -> (unit, T.Error.t) result =
  T.Child_workflow.cancel

let _child_execute :
    ?scope:T.Scope.t ->
    ?cancellation_type:T.Child_workflow.cancellation_type ->
    ?retry_policy:T.Activity.Retry_policy.t ->
    ?task_queue:string ->
    ?parent_close_policy:T.Child_workflow.Parent_close_policy.t ->
    id:string ->
    ('input, 'output) T.Workflow.t ->
    'input ->
    ('output, T.Error.t) result =
  T.Child_workflow.execute

(* Codec construction and the built-in codecs define the payload boundary
   shared by workflows, activities, and the client. *)
let _codec_make :
    encoding:string ->
    encode:('a -> (bytes, T.Error.t) result) ->
    decode:(bytes -> ('a, T.Error.t) result) ->
    'a T.Codec.t =
  T.Codec.make

let _codec_encode :
    'a T.Codec.t -> 'a -> (T.Codec.payload, T.Error.t) result =
  T.Codec.encode

let _codec_decode :
    'a T.Codec.t -> T.Codec.payload -> ('a, T.Error.t) result =
  T.Codec.decode

let _codec_option : 'a T.Codec.t -> 'a option T.Codec.t = T.Codec.option
let _codec_string : string T.Codec.t = T.Codec.string
let _codec_bytes : bytes T.Codec.t = T.Codec.bytes
let _codec_unit : unit T.Codec.t = T.Codec.unit
let _codec_int : int T.Codec.t = T.Codec.int
let _codec_int64 : int64 T.Codec.t = T.Codec.int64
let _codec_bool : bool T.Codec.t = T.Codec.bool
let _codec_float : float T.Codec.t = T.Codec.float
let _codec_json : Yojson.Safe.t T.Codec.t = T.Codec.json

let _codec_json_conv :
    to_json:('a -> Yojson.Safe.t) ->
    of_json:(Yojson.Safe.t -> ('a, T.Error.t) result) ->
    'a T.Codec.t =
  T.Codec.json_conv

let _duration_of_ms : int64 -> T.Duration.t = T.Duration.of_ms
let _duration_to_ms : T.Duration.t -> int64 = T.Duration.to_ms

let _error_make :
    ?non_retryable:bool ->
    ?error_type:string ->
    ?details:T.Payload.t list ->
    category:T.Error.category ->
    message:string ->
    unit -> T.Error.t =
  T.Error.make

let _error_view : T.Error.t -> T.Error.view = T.Error.view
let _error_kind : T.Error.t -> string = T.Error.kind
let _error_message : T.Error.t -> string = T.Error.message
let _error_error_type : T.Error.t -> string option = T.Error.error_type

(* The application failure type is also part of the inspectable view record. *)
let _error_view_error_type (view : T.Error.view) : string option =
  view.error_type
let _error_codec : message:string -> T.Error.t = T.Error.codec
let _error_defect : message:string -> T.Error.t = T.Error.defect

(* The future combinators are intentionally checked separately from workflow
   command starters: they are the public direct-style composition vocabulary. *)
let _future_await :
    ('value, 'error) T.Future.t -> ('value, 'error) result =
  T.Future.await

let _future_map :
    ('value -> 'mapped) ->
    ('value, 'error) T.Future.t ->
    ('mapped, 'error) T.Future.t =
  T.Future.map

let _future_map_error :
    ('error -> 'mapped_error) ->
    ('value, 'error) T.Future.t ->
    ('value, 'mapped_error) T.Future.t =
  T.Future.map_error

let _future_both :
    ('left, T.Error.t) T.Future.t ->
    ('right, T.Error.t) T.Future.t ->
    ('left * 'right, T.Error.t) T.Future.t =
  T.Future.both

let _future_all :
    ('value, T.Error.t) T.Future.t list ->
    ('value list, T.Error.t) T.Future.t =
  T.Future.all

let _future_race :
    ('left, T.Error.t) T.Future.t ->
    ('right, T.Error.t) T.Future.t ->
    (('left, 'right) T.Future.race, T.Error.t) T.Future.t =
  T.Future.race

let _future_first :
    ('value, T.Error.t) T.Future.t ->
    ('value, T.Error.t) T.Future.t list ->
    ('value, T.Error.t) T.Future.t =
  T.Future.first

let _future_is_ready : ('value, 'error) T.Future.t -> bool = T.Future.is_ready
let _future_peek :
    ('value, 'error) T.Future.t -> ('value, 'error) result option =
  T.Future.peek

(* Workflow and interaction definitions are ordinary typed values.  Their
   annotations protect the direct-style authoring API and handler registration
   types without attempting to execute a workflow in this consumer fixture. *)
let _workflow_define :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    ('input -> ('output, T.Error.t) result) ->
    ('input, 'output) T.Workflow.t =
  T.Workflow.define

let _workflow_remote :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    ('input, 'output) T.Workflow.t =
  T.Workflow.remote

let _workflow_name : ('input, 'output) T.Workflow.t -> string = T.Workflow.name
let _workflow_input : ('input, 'output) T.Workflow.t -> 'input T.Codec.t = T.Workflow.input
let _workflow_output : ('input, 'output) T.Workflow.t -> 'output T.Codec.t = T.Workflow.output
let _workflow_implementation :
    ('input, 'output) T.Workflow.t ->
    ('input, 'output) T.Workflow.implementation option =
  T.Workflow.implementation

let _workflow_start_sleep :
    T.Duration.t -> (unit, T.Error.t) T.Future.t =
  T.Workflow.start_sleep

let _workflow_sleep : T.Duration.t -> (unit, T.Error.t) result = T.Workflow.sleep
let _workflow_now : unit -> (T.Time.t, T.Error.t) result = T.Workflow.now
let _workflow_patched : id:string -> bool = T.Workflow.patched
let _workflow_deprecate_patch : id:string -> unit = T.Workflow.deprecate_patch
let _workflow_random_int : bound:int -> (int, T.Error.t) result =
  T.Workflow.random_int
let _workflow_continue_as_new :
    ('input, 'output) T.Workflow.t -> 'input -> 'value =
  T.Workflow.continue_as_new

let _signal_define :
    name:string -> input:'input T.Codec.t -> 'input T.Signal.t =
  T.Signal.define

let _signal_name : 'input T.Signal.t -> string = T.Signal.name
let _signal_input : 'input T.Signal.t -> 'input T.Codec.t = T.Signal.input

let _signal_handler_make :
    'input T.Signal.t ->
    ('input -> (unit, T.Error.t) result) ->
    T.Signal.Handler.t =
  T.Signal.Handler.make

let _signal_handler_handle :
    'input T.Signal.t ->
    ('input -> (unit, T.Error.t) result) ->
    T.Signal.Handler.t =
  T.Signal.Handler.handle
let _signal_handler_name : T.Signal.Handler.t -> string = T.Signal.Handler.name
let _signal_handler_dispatch :
    T.Signal.Handler.t -> T.Payload.t -> (unit, T.Error.t) result =
  T.Signal.Handler.dispatch
let _signal_handler_dispatch_payloads :
    T.Signal.Handler.t -> T.Payload.t list -> (unit, T.Error.t) result =
  T.Signal.Handler.dispatch_payloads

let _query_define :
    name:string -> output:'output T.Codec.t -> 'output T.Query.t =
  T.Query.define

let _query_define_with_input :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    ('input, 'output) T.Query.typed =
  T.Query.define_with_input

let _query_name : 'output T.Query.t -> string = T.Query.name
let _query_output : 'output T.Query.t -> 'output T.Codec.t = T.Query.output
let _query_name_with_input : ('input, 'output) T.Query.typed -> string =
  T.Query.name_with_input
let _query_input : ('input, 'output) T.Query.typed -> 'input T.Codec.t =
  T.Query.input
let _query_output_with_input :
    ('input, 'output) T.Query.typed -> 'output T.Codec.t =
  T.Query.output_with_input

let _query_handler_make :
    'output T.Query.t ->
    (unit -> ('output, T.Error.t) result) ->
    T.Query.Handler.t =
  T.Query.Handler.make

let _query_handler_handle :
    'output T.Query.t ->
    (unit -> ('output, T.Error.t) result) ->
    T.Query.Handler.t =
  T.Query.Handler.handle
let _query_handler_make_with_input :
    ('input, 'output) T.Query.typed ->
    ('input -> ('output, T.Error.t) result) ->
    T.Query.Handler.t =
  T.Query.Handler.make_with_input
let _query_handler_handle_with_input :
    ('input, 'output) T.Query.typed ->
    ('input -> ('output, T.Error.t) result) ->
    T.Query.Handler.t =
  T.Query.Handler.handle_with_input
let _query_handler_name : T.Query.Handler.t -> string = T.Query.Handler.name
let _query_handler_dispatch :
    T.Query.Handler.t -> (T.Payload.t, T.Error.t) result =
  T.Query.Handler.dispatch
let _query_handler_dispatch_payloads :
    T.Query.Handler.t -> T.Payload.t list -> (T.Payload.t, T.Error.t) result =
  T.Query.Handler.dispatch_payloads

let _update_define :
    name:string ->
    input:'input T.Codec.t ->
    output:'output T.Codec.t ->
    ('input, 'output) T.Update.t =
  T.Update.define

let _update_name : ('input, 'output) T.Update.t -> string = T.Update.name
let _update_input : ('input, 'output) T.Update.t -> 'input T.Codec.t = T.Update.input
let _update_output : ('input, 'output) T.Update.t -> 'output T.Codec.t = T.Update.output

let _update_handler_make :
    ?validator:('input -> (unit, T.Error.t) result) ->
    ('input, 'output) T.Update.t ->
    ('input -> ('output, T.Error.t) result) ->
    T.Update.Handler.t =
  T.Update.Handler.make

let _update_handler_handle :
    ?validator:('input -> (unit, T.Error.t) result) ->
    ('input, 'output) T.Update.t ->
    ('input -> ('output, T.Error.t) result) ->
    T.Update.Handler.t =
  T.Update.Handler.handle
let _update_handler_name : T.Update.Handler.t -> string = T.Update.Handler.name
let _update_handler_dispatch :
    ?run_validator:bool ->
    ?on_validated:(unit -> unit) ->
    T.Update.Handler.t -> T.Payload.t -> (T.Payload.t, T.Error.t) result =
  T.Update.Handler.dispatch
let _update_handler_dispatch_payloads :
    ?run_validator:bool ->
    ?on_validated:(unit -> unit) ->
    T.Update.Handler.t -> T.Payload.t list -> (T.Payload.t, T.Error.t) result =
  T.Update.Handler.dispatch_payloads

let _interaction_create :
    ?signals:T.Signal.Handler.t list ->
    ?queries:T.Query.Handler.t list ->
    ?updates:T.Update.Handler.t list ->
    unit -> (T.Interaction.t, T.Error.t) result =
  T.Interaction.create

let _interaction_signal :
    T.Interaction.t -> 'input T.Signal.t -> 'input -> (unit, T.Error.t) result =
  T.Interaction.signal

let _interaction_query :
    T.Interaction.t -> 'output T.Query.t -> ('output, T.Error.t) result =
  T.Interaction.query

let _interaction_query_with_input :
    T.Interaction.t ->
    ('input, 'output) T.Query.typed ->
    'input -> ('output, T.Error.t) result =
  T.Interaction.query_with_input

let _interaction_update :
    T.Interaction.t ->
    ('input, 'output) T.Update.t ->
    'input -> ('output, T.Error.t) result =
  T.Interaction.update

(* Client and worker annotations make the process lifecycle contract explicit;
   these are the operations an installed consumer must be able to compose. *)
let _client_create :
    ?identity:string ->
    ?io_threads:int ->
    ?runtime:T.Runtime.t ->
    target_url:string ->
    namespace:string ->
    unit -> (T.Client.t, T.Error.t) result =
  T.Client.create

let _client_start :
    T.Client.t ->
    ?request_id:string ->
    ?memo:(string * T.Payload.t) list ->
    ?search_attributes:(string * T.Payload.t) list ->
    ?id_conflict_policy:T.Client.id_conflict_policy ->
    workflow:('input, 'output) T.Workflow.t ->
    task_queue:string ->
    id:string ->
    input:'input ->
    unit -> (('input, 'output) T.Client.handle, T.Error.t) result =
  T.Client.start

(* The conflict policy is a closed polymorphic variant, so an exhaustive
   match here breaks if a constructor is added or renamed. *)
let _client_id_conflict_policy_name : T.Client.id_conflict_policy -> string =
  function
  | `Fail -> "fail"
  | `Use_existing -> "use_existing"
  | `Terminate_existing -> "terminate_existing"

let _client_follow :
    T.Client.t ->
    workflow:('input, 'output) T.Workflow.t ->
    T.Client.execution ->
    (('input, 'output) T.Client.handle, T.Error.t) result =
  T.Client.follow

let _client_execution_fields (execution : T.Client.execution) : string * string * string =
  (execution.namespace, execution.workflow_id, execution.run_id)

let _client_wait :
    ('input, 'output) T.Client.handle ->
    ('output T.Client.terminal_result, T.Error.t) result =
  T.Client.wait

let _client_cancel :
    ?request_id:string ->
    ?reason:string ->
    ('input, 'output) T.Client.handle ->
    (unit, T.Error.t) result =
  T.Client.cancel

let _client_reset :
    ?request_id:string ->
    ?reason:string ->
    workflow_task_finish_event_id:int64 ->
    ('input, 'output) T.Client.handle ->
    (T.Client.execution, T.Error.t) result =
  T.Client.reset

let _client_signal :
    ?request_id:string ->
    ('workflow_input, 'workflow_output) T.Client.handle ->
    signal:'signal T.Signal.t ->
    input:'signal ->
    (unit, T.Error.t) result =
  T.Client.signal

let _client_query :
    ('workflow_input, 'workflow_output) T.Client.handle ->
    query:'query T.Query.t ->
    ('query, T.Error.t) result =
  T.Client.query

let _client_query_with_input :
    ('workflow_input, 'workflow_output) T.Client.handle ->
    query:('input, 'query) T.Query.typed ->
    input:'input -> ('query, T.Error.t) result =
  T.Client.query_with_input

let _client_workflow_id :
    ('input, 'output) T.Client.handle -> string =
  T.Client.workflow_id

let _client_run_id : ('input, 'output) T.Client.handle -> string = T.Client.run_id
let _client_started : ('input, 'output) T.Client.handle -> bool = T.Client.started
let _client_already_started : T.Error.t -> T.Client.execution option =
  T.Client.already_started
let _client_is_at_capacity : T.Error.t -> bool = T.Client.is_at_capacity
let _client_is_query_failed : T.Error.t -> bool = T.Client.is_query_failed
let _client_rpc_status : T.Error.t -> T.Client.rpc_status option =
  T.Client.rpc_status

(** Witnesses every [Client.rpc_status] constructor, so removing or renaming
    one breaks this installed-consumer build. *)
let _client_rpc_status_name : T.Client.rpc_status -> string = function
  | `Cancelled -> "Cancelled"
  | `Unknown -> "Unknown"
  | `Invalid_argument -> "InvalidArgument"
  | `Deadline_exceeded -> "DeadlineExceeded"
  | `Not_found -> "NotFound"
  | `Already_exists -> "AlreadyExists"
  | `Permission_denied -> "PermissionDenied"
  | `Resource_exhausted -> "ResourceExhausted"
  | `Failed_precondition -> "FailedPrecondition"
  | `Aborted -> "Aborted"
  | `Out_of_range -> "OutOfRange"
  | `Unimplemented -> "Unimplemented"
  | `Internal -> "Internal"
  | `Unavailable -> "Unavailable"
  | `Data_loss -> "DataLoss"
  | `Unauthenticated -> "Unauthenticated"
  | `Termination_outcome_uncertain -> "TerminationOutcomeUncertain"
let _client_shutdown : T.Client.t -> (unit, T.Error.t) result = T.Client.shutdown

let _worker_workflow :
    ?signals:T.Signal.Handler.t list ->
    ?queries:T.Query.Handler.t list ->
    ?updates:T.Update.Handler.t list ->
    ('input, 'output) T.Workflow.t -> T.Worker.registered_workflow =
  T.Worker.workflow

let _worker_activity :
    ('input, 'output) T.Activity.t -> T.Worker.registered_activity =
  T.Worker.activity

let _worker_create :
    ?identity:string ->
    ?options:T.Worker.Options.t ->
    ?max_cached_workflows:int ->
    ?io_threads:int ->
    ?runtime:T.Runtime.t ->
    target_url:string ->
    namespace:string ->
    task_queue:string ->
    workflows:T.Worker.registered_workflow list ->
    activities:T.Worker.registered_activity list ->
    unit -> (T.Worker.t, T.Error.t) result =
  T.Worker.create

let _workflow_current_deployment_version :
    unit -> T.Workflow.deployment_version option =
  T.Workflow.current_deployment_version

let _workflow_info : unit -> (T.Workflow.Info.t, T.Error.t) result =
  T.Workflow.info

let _workflow_is_replaying : unit -> bool = T.Workflow.is_replaying
let _workflow_info_workflow_id : T.Workflow.Info.t -> string =
  T.Workflow.Info.workflow_id
let _workflow_info_run_id : T.Workflow.Info.t -> string = T.Workflow.Info.run_id

let _workflow_info_first_execution_run_id : T.Workflow.Info.t -> string option =
  T.Workflow.Info.first_execution_run_id

let _workflow_info_workflow_type : T.Workflow.Info.t -> string =
  T.Workflow.Info.workflow_type

let _workflow_info_namespace : T.Workflow.Info.t -> string =
  T.Workflow.Info.namespace

let _workflow_info_task_queue : T.Workflow.Info.t -> string =
  T.Workflow.Info.task_queue

let _workflow_info_attempt : T.Workflow.Info.t -> int = T.Workflow.Info.attempt

let _workflow_info_parent : T.Workflow.Info.t -> T.Workflow.Info.parent option =
  T.Workflow.Info.parent

let _workflow_info_start_time : T.Workflow.Info.t -> T.Time.t option =
  T.Workflow.Info.start_time

let _workflow_info_is_replaying : T.Workflow.Info.t -> bool =
  T.Workflow.Info.is_replaying

let _workflow_info_history_length : T.Workflow.Info.t -> int =
  T.Workflow.Info.history_length

let _workflow_info_history_size_bytes : T.Workflow.Info.t -> int option =
  T.Workflow.Info.history_size_bytes

let _workflow_info_continue_as_new_suggested : T.Workflow.Info.t -> bool =
  T.Workflow.Info.continue_as_new_suggested

let _workflow_info_continue_as_new_reasons :
    T.Workflow.Info.t -> T.Workflow.Info.continue_as_new_reason list =
  T.Workflow.Info.continue_as_new_reasons

(* The reason variant is closed; an exhaustive match pins its constructors. *)
let _workflow_info_continue_as_new_reason_name :
    T.Workflow.Info.continue_as_new_reason -> string = function
  | `History_size_too_large -> "history_size_too_large"
  | `Too_many_history_events -> "too_many_history_events"
  | `Too_many_updates -> "too_many_updates"

(* Record fields of the public parent identity are part of the contract. *)
let _workflow_info_parent_fields
    ({ namespace; workflow_id; run_id } : T.Workflow.Info.parent) =
  ignore (namespace : string);
  ignore (workflow_id : string);
  ignore (run_id : string)

let _worker_run : T.Worker.t -> (unit, T.Error.t) result = T.Worker.run
let _worker_shutdown : T.Worker.t -> (unit, T.Error.t) result = T.Worker.shutdown
let _worker_request_shutdown : T.Worker.t -> unit = T.Worker.request_shutdown

(* The remaining small modules still participate in the public contract. *)
let _condition_wait_until : (unit -> bool) -> (unit, T.Error.t) result =
  T.Condition.wait_until

let _condition_wait_until_result :
    T.Condition.predicate -> (unit, T.Error.t) result =
  T.Condition.wait_until_result

let _runtime_abi_version : unit -> (int32, T.Error.t) result =
  T.Runtime_info.native_bridge_abi_version

(* A shared runtime is an explicit resource: created, counted, and shut down
   by the application, with typed results on every path (#832). *)
let _runtime_create : ?io_threads:int -> unit -> (T.Runtime.t, T.Error.t) result =
  T.Runtime.create
let _runtime_attached : T.Runtime.t -> int = T.Runtime.attached
let _runtime_shutdown : T.Runtime.t -> (unit, T.Error.t) result =
  T.Runtime.shutdown

let _scope_create : unit -> (T.Scope.t, T.Error.t) result = T.Scope.create
let _scope_with_scope :
    (T.Scope.t -> ('value, T.Error.t) result) ->
    ('value, T.Error.t) result =
  T.Scope.with_scope
let _scope_cancel : T.Scope.t -> (unit, T.Error.t) result = T.Scope.cancel
let _scope_is_cancelled : T.Scope.t -> (bool, T.Error.t) result = T.Scope.is_cancelled
let _scope_check : T.Scope.t -> (unit, T.Error.t) result = T.Scope.check
let _scope_await :
    T.Scope.t -> ('value, T.Error.t) T.Future.t -> ('value, T.Error.t) result =
  T.Scope.await

let _time_of_unix :
    seconds:int64 -> nanoseconds:int -> (T.Time.t, T.Error.t) result =
  T.Time.of_unix

let _time_compare : T.Time.t -> T.Time.t -> int = T.Time.compare
let _time_seconds : T.Time.t -> int64 = T.Time.seconds
let _time_nanoseconds : T.Time.t -> int = T.Time.nanoseconds
let _time_equal : T.Time.t -> T.Time.t -> bool = T.Time.equal
let _workflow_context_active : unit -> bool = T.Workflow_context.is_active

let _workflow_local_create : unit -> 'a T.Workflow_context.Local.t =
  T.Workflow_context.Local.create
let _workflow_local_get :
    'a T.Workflow_context.Local.t -> ('a option, T.Error.t) result =
  T.Workflow_context.Local.get
let _workflow_local_set :
    'a T.Workflow_context.Local.t -> 'a -> (unit, T.Error.t) result =
  T.Workflow_context.Local.set

(* Keep the public payload record and result syntax visible to a real consumer.
   These expressions are never evaluated by the test executable; they only
   force the installed CMI to expose the documented record fields and operator
   types. *)
let _payload_fields (payload : T.Payload.t) : (string * string) list * bytes =
  (payload.metadata, payload.data)

let _result_bind :
    ('a, 'error) result ->
    ('a -> ('b, 'error) result) ->
    ('b, 'error) result =
  T.Result_syntax.( let* )

let _result_map : ('a, 'error) result -> ('a -> 'b) -> ('b, 'error) result =
  T.Result_syntax.( let+ )

(** Installed consumers can inspect start metadata without private types. *)
let _workflow_start_metadata : unit -> (T.Workflow.start_metadata, T.Error.t) result =
  T.Workflow.start_metadata

(** Compiles explicit child routing and lifecycle options through the installed
    public package, so private type leakage cannot hide in source-tree tests. *)
let _routed_child definition input =
  T.Child_workflow.start ~id:"cross-sdk-child" ~task_queue:"go-llm-worker"
    ~parent_close_policy:T.Child_workflow.Parent_close_policy.Abandon
    definition input

(* Values below completed the witness for #841.  Since then the in-tree
   [test/api_witness] gate fails `dune runtest` when any value exported by a
   public interface is not referenced here, so a new public value must gain an
   explicit annotation in the same change that exports it. *)

(* Activity priorities and local activities. *)
let _activity_priority_make :
    ?priority_key:int ->
    ?fairness_key:string ->
    ?fairness_weight:float ->
    unit -> (T.Activity.Priority.t, T.Error.t) result =
  T.Activity.Priority.make

let _activity_priority_create :
    ?priority_key:int ->
    ?fairness_key:string ->
    ?fairness_weight:float ->
    unit -> (T.Activity.Priority.t, T.Error.t) result =
  T.Activity.Priority.create

let _activity_priority_key : T.Activity.Priority.t -> int option =
  T.Activity.Priority.priority_key

let _activity_priority_fairness_key : T.Activity.Priority.t -> string option =
  T.Activity.Priority.fairness_key

let _activity_priority_fairness_weight :
    T.Activity.Priority.t -> float option =
  T.Activity.Priority.fairness_weight

let _activity_start_local :
    ?activity_id:string ->
    ?schedule_to_close_timeout:T.Duration.t ->
    ?schedule_to_start_timeout:T.Duration.t ->
    ?start_to_close_timeout:T.Duration.t ->
    ?retry_policy:T.Activity.Retry_policy.t ->
    ?cancellation_type:T.Activity.cancellation_type ->
    ('input, 'output) T.Activity.t ->
    'input -> ('output, T.Error.t) T.Future.t =
  T.Activity.start_local

let _activity_execute_local :
    ?activity_id:string ->
    ?schedule_to_close_timeout:T.Duration.t ->
    ?schedule_to_start_timeout:T.Duration.t ->
    ?start_to_close_timeout:T.Duration.t ->
    ?retry_policy:T.Activity.Retry_policy.t ->
    ?cancellation_type:T.Activity.cancellation_type ->
    ('input, 'output) T.Activity.t ->
    'input -> ('output, T.Error.t) result =
  T.Activity.execute_local

(* Client termination, visibility listing, and workflow updates. *)
let _client_terminate :
    ?reason:string ->
    ('input, 'output) T.Client.handle -> (unit, T.Error.t) result =
  T.Client.terminate

let _client_list_visibility :
    ?page_size:int ->
    ?page_token:string ->
    T.Client.t ->
    query:string ->
    unit -> (T.Client.visibility_page, T.Error.t) result =
  T.Client.list_visibility

let _client_visibility_page_fields (page : T.Client.visibility_page) :
    (string * string * string * string * string) list * string option =
  ( List.map
      (fun (execution : T.Client.visibility_execution) ->
        ( execution.workflow_id,
          execution.run_id,
          execution.workflow_type,
          execution.task_queue,
          execution.status ))
      page.executions,
    page.next_page_token )

let _client_start_update :
    ?update_id:string ->
    ('workflow_input, 'workflow_output) T.Client.handle ->
    update:('input, 'output) T.Update.t ->
    input:'input ->
    unit -> (('input, 'output) T.Client.update_handle, T.Error.t) result =
  T.Client.start_update

let _client_wait_update :
    ('input, 'output) T.Client.update_handle -> ('output, T.Error.t) result =
  T.Client.wait_update

let _client_update_id : ('input, 'output) T.Client.update_handle -> string =
  T.Client.update_id

(* Scope cancellation callbacks. *)
let _scope_on_cancel :
    ?until:('value, 'error) T.Future.t ->
    T.Scope.t ->
    (unit -> (unit, T.Error.t) result) ->
    (unit, T.Error.t) result =
  T.Scope.on_cancel

(* Worker options. *)
let _worker_options_default : T.Worker.Options.t = T.Worker.Options.default

let _worker_options_make :
    ?versioning:T.Worker.Options.versioning ->
    ?max_cached_workflows:int ->
    unit -> (T.Worker.Options.t, T.Error.t) result =
  T.Worker.Options.make

let _worker_options_versioning :
    T.Worker.Options.t -> T.Worker.Options.versioning =
  T.Worker.Options.versioning

let _worker_options_max_cached_workflows : T.Worker.Options.t -> int option =
  T.Worker.Options.max_cached_workflows

(* External workflow commands and search attributes from workflow code. *)
let _workflow_signal_external_workflow :
    workflow_id:string ->
    run_id:string ->
    signal:'input T.Signal.t ->
    input:'input -> (unit, T.Error.t) T.Future.t =
  T.Workflow.signal_external_workflow

let _workflow_cancel_external_workflow :
    workflow_id:string ->
    run_id:string ->
    reason:string -> (unit, T.Error.t) T.Future.t =
  T.Workflow.cancel_external_workflow

let _workflow_upsert_search_attributes :
    (string * T.Payload.t) list -> unit =
  T.Workflow.upsert_search_attributes

(* In-process, time-skipping workflow test environment. *)
let _testing_workflow :
    ?signals:T.Signal.Handler.t list ->
    ?queries:T.Query.Handler.t list ->
    ?updates:T.Update.Handler.t list ->
    ('input, 'output) T.Workflow.t -> T.Testing.registered_workflow =
  T.Testing.workflow

let _testing_mock_workflow :
    ?signals:T.Signal.Handler.t list ->
    ?queries:T.Query.Handler.t list ->
    ?updates:T.Update.Handler.t list ->
    ('input, 'output) T.Workflow.t ->
    ('input -> ('output, T.Error.t) result) ->
    T.Testing.registered_workflow =
  T.Testing.mock_workflow

let _testing_activity :
    ('input, 'output) T.Activity.t -> T.Testing.registered_activity =
  T.Testing.activity

let _testing_mock_activity :
    ('input, 'output) T.Activity.t ->
    ('input -> ('output, T.Error.t) result) ->
    T.Testing.registered_activity =
  T.Testing.mock_activity

let _testing_create :
    ?namespace:string ->
    ?task_queue:string ->
    ?start_time:T.Time.t ->
    ?max_activity_attempts:int ->
    workflows:T.Testing.registered_workflow list ->
    activities:T.Testing.registered_activity list ->
    unit -> (T.Testing.t, T.Error.t) result =
  T.Testing.create

let _testing_shutdown : T.Testing.t -> unit = T.Testing.shutdown
let _testing_now : T.Testing.t -> T.Time.t = T.Testing.now

let _testing_skip : T.Testing.t -> T.Duration.t -> (unit, T.Error.t) result =
  T.Testing.skip

let _testing_start :
    ?id:string ->
    T.Testing.t ->
    ('input, 'output) T.Workflow.t ->
    'input -> (('input, 'output) T.Testing.handle, T.Error.t) result =
  T.Testing.start

let _testing_result :
    ?timeout:T.Duration.t ->
    ('input, 'output) T.Testing.handle -> ('output, T.Error.t) result =
  T.Testing.result

let _testing_execute :
    ?id:string ->
    ?timeout:T.Duration.t ->
    T.Testing.t ->
    ('input, 'output) T.Workflow.t -> 'input -> ('output, T.Error.t) result =
  T.Testing.execute

let _testing_signal :
    ('input, 'output) T.Testing.handle ->
    'signal T.Signal.t -> 'signal -> (unit, T.Error.t) result =
  T.Testing.signal

let _testing_query :
    ('input, 'output) T.Testing.handle ->
    'query T.Query.t -> ('query, T.Error.t) result =
  T.Testing.query

let _testing_query_with_input :
    ('input, 'output) T.Testing.handle ->
    ('query_input, 'query) T.Query.typed ->
    'query_input -> ('query, T.Error.t) result =
  T.Testing.query_with_input

let _testing_update :
    ?timeout:T.Duration.t ->
    ('input, 'output) T.Testing.handle ->
    ('update_input, 'update_output) T.Update.t ->
    'update_input -> ('update_output, T.Error.t) result =
  T.Testing.update

let _testing_cancel :
    ('input, 'output) T.Testing.handle -> (unit, T.Error.t) result =
  T.Testing.cancel

let _testing_workflow_id : ('input, 'output) T.Testing.handle -> string =
  T.Testing.workflow_id

let _testing_run_id : ('input, 'output) T.Testing.handle -> string =
  T.Testing.run_id
