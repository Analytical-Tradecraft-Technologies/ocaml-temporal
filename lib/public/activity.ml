(** Defines the public activity description. The implementation is retained in
    a private record and converted to the base definition only when a native
    worker is created. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

(** Opaque context for one activity attempt. The private runtime guards its
    lifetime so retaining this value cannot use a released task token. *)
type context = Temporal_base.Activity_context.t

(** Context-aware implementation form for activities that inspect prior
    heartbeat details or report progress. *)
type ('input, 'output) contextual_implementation =
  context -> 'input -> ('output, Error.t) result

(** Opaque capability retained after an asynchronous activity has returned
    [Will_complete_async]. The private adapter binds it to one accepted
    Temporal task token and validates every later operation. *)
type 'output async_handle =
  'output Temporal_base.Async_activity.handle

(** Context supplied to an asynchronous implementation. It exists only to
    obtain the attempt's completion capability; it carries no nondeterministic
    state and is safe to retain until the handle is terminal. *)
type 'output async_context =
  'output Temporal_base.Async_activity.context

(** The explicit result of an asynchronous activity callback. Returning a
    handle is the only way to defer completion; returning [Completed] or
    [Failed] keeps completion owned by the worker task. *)
type 'output async_result =
  | Completed of 'output
  | Failed of Error.t
  | Will_complete_async of 'output async_handle

(** Callback form for activities whose completion may occur after the worker
    task has been acknowledged. *)
type ('input, 'output) async_implementation =
  'output async_context -> 'input -> 'output async_result

(** Immutable public activity definition. The paired codecs describe the
    payload boundary, while exactly one implementation mode is retained:
    local, context-aware, or remote-only. Keeping the mode explicit prevents a
    command-only reference from being invoked as though it owned worker code. *)
type ('input, 'output) t = {
  (* Stable Temporal activity type name used by registration and schedule
     commands; construction validates that it is bridge-safe. *)
  name : string;
  (* Codec used to decode arguments delivered to this activity. *)
  input : 'input Codec.t;
  (* Codec used to encode successful activity results. *)
  output : 'output Codec.t;
  (* Plain local callback, absent for contextual and remote definitions. *)
  implementation : ('input, 'output) implementation option;
  (* Context-aware local callback, absent for plain and remote definitions. *)
  contextual_implementation :
    ('input, 'output) contextual_implementation option;
  (* Asynchronous implementation, absent for ordinary/contextual/remote
     definitions. *)
  async_implementation : ('input, 'output) async_implementation option;
}

(** Validated scheduling controls are kept in the public module while the
    runtime receives only the opaque exact wire representation. *)
module Priority = struct
  type t = {
    priority_key : int option;
    fairness_key : string option;
    fairness_weight : float option;
    base : Temporal_base.Priority.t;
  }

  (** Checks the protocol's short UTF-8 fairness-key contract. *)
  let validate_fairness_key = function
    | None -> Ok ()
    | Some value when String.equal value "" ->
        Error (Error.defect ~message:"priority fairness key must not be empty")
    | Some value when String.contains value '\000' ->
        Error (Error.defect ~message:"priority fairness key must not contain NUL")
    | Some value when String.length value > 64 ->
        Error (Error.defect ~message:"priority fairness key exceeds 64 bytes")
    | Some value when not (Temporal_base.Codec.valid_utf_8 value) ->
        Error (Error.defect ~message:"priority fairness key must be valid UTF-8")
    | Some _ -> Ok ()

  (** Checks the server-supported priority range without allowing NaN or
      infinity to cross the OCaml/Rust boundary. *)
  let validate_weight = function
    | None -> Ok ()
    | Some value
      when Float.is_nan value || Float.is_infinite value
           || (value <> 0.0 && value < 0.001) || value > 1000.0 ->
        Error
          (Error.defect
             ~message:"priority fairness weight must be finite and in [0.001, 1000]")
    | Some _ -> Ok ()

  (** Constructs immutable priority metadata and stores the single-precision
      weight bits used by Core, avoiding a later float-rounding ambiguity. *)
  let make ?priority_key ?fairness_key ?fairness_weight () =
    if Option.is_none priority_key && Option.is_none fairness_key
       && Option.is_none fairness_weight
    then Error (Error.defect ~message:"priority requires at least one field")
    else
      let key_result =
        match priority_key with
        | None -> Ok 0l
        | Some value when value <= 0 ->
            Error (Error.defect ~message:"priority key must be positive")
        | Some value when Int64.of_int value > Int64.of_int32 Int32.max_int ->
            Error (Error.defect ~message:"priority key exceeds the wire range")
        | Some value -> Ok (Int32.of_int value)
      in
      match key_result with
      | Error _ as error -> error
      | Ok priority_key -> (
          match validate_fairness_key fairness_key with
          | Error _ as error -> error
          | Ok () -> (
              match validate_weight fairness_weight with
              | Error _ as error -> error
              | Ok () ->
                  let priority_key_value =
                    Option.map Int32.to_int
                      (if priority_key = 0l then None else Some priority_key)
                  in
                  let fairness_key_value =
                    if String.equal (Option.value fairness_key ~default:"") ""
                    then None
                    else fairness_key
                  in
                  let fairness_weight_bits =
                    Option.fold ~none:0l ~some:Int32.bits_of_float fairness_weight
                  in
                  Ok
                    {
                      priority_key = priority_key_value;
                      fairness_key = fairness_key_value;
                      fairness_weight =
                        Option.map (fun _ -> Int32.float_of_bits fairness_weight_bits)
                          fairness_weight;
                      base =
                        Temporal_base.Priority.make ~priority_key
                          ~fairness_key:(Option.value fairness_key ~default:"")
                          ~fairness_weight_bits;
                    }))

  (** Alias for callers who prefer constructor terminology. *)
  let create = make

  (** Returns the optional positive priority key. *)
  let priority_key value = value.priority_key

  (** Returns the optional fairness key. *)
  let fairness_key value = value.fairness_key

  (** Returns the optional rounded single-precision fairness weight. *)
  let fairness_weight value = value.fairness_weight

  (** Private conversion used only by the workflow runtime. *)
  let to_base value = value.base
end

(* Maximum byte length accepted by the closed JSON/native identifier contract. *)
let max_name_bytes = 65_536

(* Validates a stable activity type name before it can enter registration or
   command history. Empty, oversized, NUL-containing, or malformed UTF-8 names
   cannot be represented safely by the bridge protocol, so they are
   programmer-facing construction errors rather than typed activity failures. *)
let validate_name name =
  if String.length name = 0 then invalid_arg "Temporal definition name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal definition name contains a NUL byte"
  else if String.length name > max_name_bytes then
    invalid_arg "Temporal definition name exceeds 65536 bytes"
  else if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "Temporal definition name must be valid UTF-8"

(** Constructs an activity implemented by this worker. *)
let define ~name ~input ~output implementation =
  validate_name name;
  {
    name;
    input;
    output;
    implementation = Some implementation;
    contextual_implementation = None;
    async_implementation = None;
  }

(** Constructs a local activity that receives an opaque attempt context. *)
let define_with_context ~name ~input ~output implementation =
  validate_name name;
  {
    name;
    input;
    output;
    implementation = None;
    contextual_implementation = Some implementation;
    async_implementation = None;
  }

(** Constructs a command-only reference to an activity on another worker. *)
let remote ~name ~input ~output =
  validate_name name;
  {
    name;
    input;
    output;
    implementation = None;
    contextual_implementation = None;
    async_implementation = None;
  }

(** Constructs a local activity that explicitly chooses whether to complete
    immediately or retain an opaque completion handle. *)
let define_async ~name ~input ~output implementation =
  validate_name name;
  {
    name;
    input;
    output;
    implementation = None;
    contextual_implementation = None;
    async_implementation = Some implementation;
  }

(* Returns the exact Temporal activity type name used by registration and
   schedule commands. *)
let name definition = definition.name

(* Returns the input codec retained by an opaque activity definition. *)
let input definition = definition.input

(* Returns the output codec retained by an opaque activity definition. *)
let output definition = definition.output

(* Returns executable code for a local activity, or [None] for a remote
   reference that can only be scheduled. *)
let implementation definition = definition.implementation

(** Returns the context-aware callback, if this is a local contextual
    definition. *)
let implementation_with_context definition = definition.contextual_implementation

(** Returns the asynchronous callback, if this is a local deferred
    definition. *)
let implementation_async definition = definition.async_implementation

(** Converts one public payload to the private representation while copying
    mutable bytes so a caller cannot change a request after submission. *)
let payload_to_base ({ Payload.metadata; data } : Payload.t) : Temporal_base.Payload.t =
  {
    Temporal_base.Payload.metadata = List.map (fun (key, value) -> (key, value)) metadata;
    data = Bytes.copy data;
  }

(** Operations on a retained asynchronous activity capability. Every method
    returns a typed error; it never raises for a normal lifecycle or transport
    failure. *)
module Async_handle = struct
  type 'output t = 'output async_handle

  (** Completes the activity with a typed output encoded by its definition. *)
  let complete handle output =
    Temporal_base.Async_activity.complete handle output
    |> Result.map_error Error_private.of_base

  (** Reports an application failure and makes the activity terminal. *)
  let fail handle error =
    Temporal_base.Async_activity.fail handle (Error_private.to_base error)
    |> Result.map_error Error_private.of_base

  (** Reports cancellation with optional detail payloads. *)
  let cancel handle details =
    Temporal_base.Async_activity.cancel handle (List.map payload_to_base details)
    |> Result.map_error Error_private.of_base

  (** Reports heartbeat details while retaining the terminal capability. *)
  let heartbeat handle details =
    Temporal_base.Async_activity.heartbeat handle
      (List.map payload_to_base details)
    |> Result.map_error Error_private.of_base
end

(** Accessors for the attempt-scoped asynchronous context. *)
module Async_context = struct
  type 'output t = 'output async_context

  (** Returns the retained completion capability. *)
  let handle context = Temporal_base.Async_activity.handle context
end

(** Encodes and submits one typed heartbeat value for the current activity. *)
let heartbeat (context : context) (codec : 'a Codec.t) (value : 'a) =
  match Codec.encode codec value with
  | Error error -> Error error
  | Ok ({ Payload.metadata; data } : Payload.t) ->
      let payload : Temporal_base.Payload.t =
        {
          Temporal_base.Payload.metadata =
            List.map (fun (key, value) -> (key, value)) metadata;
          data = Bytes.copy data;
        }
      in
      Temporal_base.Activity_context.heartbeat context [ payload ]
      |> Result.map_error Error_private.of_base

(** Safe operations exposed to contextual activity implementations. *)
module Context = struct
  (** Alias used by contextual activity helpers; the private context controls
      attempt lifetime and copies payloads at every public boundary. *)
  type t = context

  (** Sends one typed heartbeat value. *)
  let heartbeat context codec value = heartbeat context codec value

  (** Sends already encoded detail payloads in order. *)
  let heartbeat_payloads context payloads =
    let payloads =
      List.map
        (fun ({ Payload.metadata; data } : Payload.t) ->
          {
            Temporal_base.Payload.metadata =
              List.map (fun (key, value) -> (key, value)) metadata;
            data = Bytes.copy data;
          })
        payloads
    in
    Temporal_base.Activity_context.heartbeat context payloads
    |> Result.map_error Error_private.of_base

  (** Returns details retained from the preceding heartbeat attempt. *)
  let details context =
    Temporal_base.Activity_context.details context
    |> List.map
         (fun ({ Temporal_base.Payload.metadata; data } : Temporal_base.Payload.t) ->
           {
             Payload.metadata =
               List.map (fun (key, value) -> (key, value)) metadata;
             data = Bytes.copy data;
           })

  (** Returns the server-supplied heartbeat interval, if configured. *)
  let heartbeat_timeout context =
    Temporal_base.Activity_context.heartbeat_timeout context
    |> Option.map (fun duration ->
           Duration.of_ms (Temporal_base.Duration.to_ms duration))
end

(** Cancellation policy carried by a scheduled activity command. The closed
    variant keeps the deterministic mapping to Temporal Core explicit and
    avoids exposing the private runtime constructors in the public API. *)
type cancellation_type =
  (* Ask Core to request cancellation and resolve the parent when possible. *)
  | Try_cancel
  (* Keep the parent future pending until Core confirms cancellation completed. *)
  | Wait_cancellation_completed
  (* Resolve the parent without requesting cancellation of the activity. *)
  | Abandon

(** Couples a typed activity result future with the only cancellation operation
    that can target its private runtime sequence. The record is hidden by the
    public interface so callers cannot forge a sequence or bypass the owning
    workflow scheduler. *)
type 'output handle = {
  (* Future resolved by Core with the activity's decoded result or failure. *)
  future : ('output, Error.t) Future.t;
  (* Owner-checked operation that emits at most one cancel command. *)
  cancel : unit -> (unit, Error.t) result;
}

(* Converts the public cancellation policy to the private runtime variant at
   the last boundary before a command is emitted. *)
let runtime_cancellation_type = function
  | Try_cancel -> Temporal_sdk_kernel.Activation.Try_cancel
  | Wait_cancellation_completed ->
      Temporal_sdk_kernel.Activation.Wait_cancellation_completed
  | Abandon -> Temporal_sdk_kernel.Activation.Abandon

module Retry_policy = struct
  (** The public retry policy keeps the source float for accessors and its
      exact IEEE-754 representation for the JSON bridge.  Storing both avoids
      re-parsing a decimal value during every command while ensuring that
      replay sees precisely the bits selected by the caller. *)
  type t = {
    (* Delay before the first retry attempt, represented exactly in ms. *)
    initial_interval : Duration.t;
    (* Finite multiplier applied to each retry delay. *)
    backoff_coefficient : float;
    (* Canonical unsigned IEEE-754 bits used by the JSON bridge. *)
    backoff_coefficient_bits : string;
    (* Upper bound applied to computed retry delays. *)
    maximum_interval : Duration.t;
    (* Maximum attempt count; zero means that Temporal imposes no limit. *)
    maximum_attempts : int;
    (* Error type names that should fail without another attempt. *)
    non_retryable_error_types : string list;
  }

  (** Formats an arbitrary 64-bit bit pattern as the canonical unsigned
      decimal required by the bridge protocol.  OCaml's [Int64.to_string]
      treats patterns with the high bit set as negative, so the conversion
      uses two base-[2^32] limbs and repeated division by ten instead. *)
  let unsigned_int64_decimal bits =
    if Int64.compare bits 0L >= 0 then Int64.to_string bits
    else
      let base = 4_294_967_296L in
      let high = Int64.logand (Int64.shift_right_logical bits 32) 0xffff_ffffL in
      let low = Int64.logand bits 0xffff_ffffL in
      let rec digits high low acc =
        if Int64.equal high 0L && Int64.equal low 0L then acc
        else
          let high_quotient = Int64.div high 10L in
          let high_remainder = Int64.rem high 10L in
          let combined =
            Int64.add (Int64.mul high_remainder base) low
          in
          let low_quotient = Int64.div combined 10L in
          let digit = Int64.to_int (Int64.rem combined 10L) in
          digits high_quotient low_quotient
            (Char.chr (Char.code '0' + digit) :: acc)
      in
      let characters = digits high low [] in
      String.concat "" (List.map (String.make 1) characters)

  (** Validates names embedded in the retry policy.  Keeping this validation at
      construction means a policy cannot later fail merely because a worker
      tries to serialize it. *)
  let validate_non_retryable_error_type index value =
    let field = "non_retryable_error_types[" ^ string_of_int index ^ "]" in
    if String.equal value "" then
      Error (Error.defect ~message:(field ^ " must not be empty"))
    else if String.contains value '\000' then
      Error (Error.defect ~message:(field ^ " must not contain NUL"))
    else if String.length value > 65_536 then
      Error (Error.defect ~message:(field ^ " exceeds 65536 bytes"))
    else if not (Temporal_base.Codec.valid_utf_8 value) then
      Error (Error.defect ~message:(field ^ " must be valid UTF-8"))
    else Ok ()

  (** Validates and constructs a policy without allowing invalid values to
      cross into workflow history.  The checks mirror Temporal Core's retry
      policy constraints and are deliberately expressed as [result] values so
      routine configuration failures never use exceptions as control flow. *)
  let make ~initial_interval ~backoff_coefficient ~maximum_interval
      ~maximum_attempts ?(non_retryable_error_types = []) () =
    let initial_ms = Duration.to_ms initial_interval in
    let maximum_ms = Duration.to_ms maximum_interval in
    let invalid message = Error (Error.defect ~message) in
    if Int64.compare initial_ms 0L <= 0 then
      invalid "retry policy initial_interval must be positive"
    else if Int64.compare maximum_ms initial_ms < 0 then
      invalid
        "retry policy maximum_interval must be at least initial_interval"
    else
      match classify_float backoff_coefficient with
      | FP_nan | FP_infinite ->
          invalid "retry policy backoff_coefficient must be finite"
      | FP_zero | FP_subnormal | FP_normal when backoff_coefficient < 1.0 ->
          invalid "retry policy backoff_coefficient must be at least 1.0"
      | FP_zero | FP_subnormal | FP_normal ->
          let maximum_int32 = Int32.to_int Int32.max_int in
          if maximum_attempts < 0 || maximum_attempts > maximum_int32 then
            invalid
              "retry policy maximum_attempts must be between 0 and Int32.max_int"
          else
            let rec validate_types index = function
              | [] -> Ok ()
              | value :: rest -> (
                  match validate_non_retryable_error_type index value with
                  | Error _ as error -> error
                  | Ok () -> validate_types (index + 1) rest)
            in
            (match validate_types 0 non_retryable_error_types with
            | Error _ as error -> error
            | Ok () ->
                Ok
                  {
                    initial_interval;
                    backoff_coefficient;
                    backoff_coefficient_bits =
                      unsigned_int64_decimal
                        (Int64.bits_of_float backoff_coefficient);
                    maximum_interval;
                    maximum_attempts;
                    non_retryable_error_types =
                      List.map Fun.id non_retryable_error_types;
                  })

  (** The [create] spelling is useful at call sites that treat policies as
      immutable values rather than mutable builders. *)
  let create ~initial_interval ~backoff_coefficient ~maximum_interval
      ~maximum_attempts ?non_retryable_error_types () =
    make ~initial_interval ~backoff_coefficient ~maximum_interval
      ~maximum_attempts ?non_retryable_error_types ()

  (** Returns the exact delay before the first retry. *)
  let initial_interval policy = policy.initial_interval

  (** Returns the multiplier applied to the previous retry delay. *)
  let backoff_coefficient policy = policy.backoff_coefficient

  (** Returns the upper bound for a retry delay. *)
  let maximum_interval policy = policy.maximum_interval

  (** Returns the attempt cap, where zero means unlimited attempts. *)
  let maximum_attempts policy = policy.maximum_attempts

  (** A fresh list protects the policy's immutable representation from callers
      that retain and mutate a list value they received from an accessor. *)
  let non_retryable_error_types policy =
    List.map Fun.id policy.non_retryable_error_types
end

(* Copies a validated public policy into the private runtime record.  This is
   the only place where the public activity module knows the bridge's compact
   representation; the public signature keeps those details hidden. *)
let runtime_retry_policy policy =
  Temporal_sdk_kernel.Activation
  .{
    initial_interval = Duration.to_ms (Retry_policy.initial_interval policy);
    backoff_coefficient_bits = policy.backoff_coefficient_bits;
    maximum_interval = Duration.to_ms (Retry_policy.maximum_interval policy);
    maximum_attempts = Retry_policy.maximum_attempts policy;
    non_retryable_error_types = Retry_policy.non_retryable_error_types policy;
  }

(** Validates optional identifiers before a deterministic command sequence is
    allocated. *)
let validate_optional_identifier field = function
  | None -> Ok ()
  | Some value when String.equal value "" ->
      Error (Error.defect ~message:(field ^ " must not be empty"))
  | Some value when String.contains value '\000' ->
      Error (Error.defect ~message:(field ^ " must not contain NUL"))
  | Some value when String.length value > 65_536 ->
      Error (Error.defect ~message:(field ^ " exceeds 65536 bytes"))
  | Some value when not (Temporal_base.Codec.valid_utf_8 value) ->
      Error (Error.defect ~message:(field ^ " must be valid UTF-8"))
  | Some _ -> Ok ()

(* Supplies the typed error used when a caller tries to schedule an activity
   without an active workflow execution. *)
let outside_error () =
  Error.defect ~message:"activity operation used outside a workflow"

(** Builds a ready public future for validation failures and detached calls.
    Inside a workflow the current scheduler owns the future so combinators
    such as [Future.both] report the real defect instead of a cross-execution
    ownership error from an inert owner id. *)
let resolved result =
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | Some context ->
      Future_private.of_internal
        (Temporal_sdk_kernel.Workflow_context_store.resolved context
           (Result.map_error Error_private.to_base result))
  | None -> Future_private.resolved ~outside_error result

(** Builds a ready handle for a request that failed before an activity command
    could be emitted. The future contains the original typed defect and cancel
    returns that same defect, so invalid or detached operations cannot leave
    hidden cancellation state behind. *)
let failed_handle error =
  { future = resolved (Error error); cancel = (fun () -> Error error) }

(** Verifies an optional structured-cancellation scope before validating or
    encoding an activity request. A cancelled or foreign scope therefore
    cannot cause a new Temporal command or invoke user codec code. *)
let check_scope = function
  | None -> Ok ()
  | Some scope -> Scope.check scope

(** Schedules an activity after validating command options and encoding input.
    Option validation deliberately happens first: a malformed activity ID or
    task queue must not invoke an application codec, allocate workflow state,
    or perform any user conversion before the request is rejected. The native
    runtime receives a base payload; its result decoder is converted back to
    public errors at the same boundary. *)
let start_handle_internal ?scope ?activity_id ?task_queue ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
    ?retry_policy ?priority ?(cancellation_type = Try_cancel)
    ?(do_not_eagerly_execute = false) ?(local = false)
    definition input =
  match check_scope scope with
  | Error error -> failed_handle error
  | Ok () -> (
      match validate_optional_identifier "activity id" activity_id with
      | Error error -> failed_handle error
      | Ok () -> (
      match validate_optional_identifier "task queue" task_queue with
      | Error error -> failed_handle error
      | Ok () -> (
          match Codec_private.encode_base definition.input input with
          | Error error -> failed_handle (Error_private.of_base error)
          | Ok input -> (
              match Temporal_sdk_kernel.Workflow_context_store.current () with
              | None -> failed_handle (outside_error ())
              | Some context ->
                  let schedule_to_close_timeout =
                    Option.map Duration.to_ms schedule_to_close_timeout
                  in
                  let schedule_to_start_timeout =
                    Option.map Duration.to_ms schedule_to_start_timeout
                  in
                  let start_to_close_timeout =
                    Option.map Duration.to_ms start_to_close_timeout
                  in
                  let heartbeat_timeout =
                    Option.map Duration.to_ms heartbeat_timeout
                  in
                  (* All optional durations are now exact integer milliseconds;
                     the runtime owns default timeout insertion and validation. *)
                  let retry_policy =
                    Option.map runtime_retry_policy retry_policy
                  in
                  let schedule_result =
                    if local then
                      if Option.is_some task_queue || Option.is_some heartbeat_timeout
                         || Option.is_some priority || do_not_eagerly_execute
                      then
                        Error
                          (Error.defect
                             ~message:
                               "local activity does not support remote-only scheduling options")
                      else
                        Ok
                          (Temporal_sdk_kernel.Workflow_context_store.schedule_local_activity
                             context ~name:(name definition) ~input ?activity_id
                             ?schedule_to_close_timeout ?schedule_to_start_timeout
                             ?start_to_close_timeout ?retry_policy
                             ~cancellation_type:(runtime_cancellation_type cancellation_type)
                             ~decode:(Codec_private.decode_base definition.output)
                             ())
                    else
                      Ok
                        (Temporal_sdk_kernel.Workflow_context_store.schedule_activity
                           context ~name:(name definition) ~input ?activity_id
                           ?task_queue ?schedule_to_close_timeout
                           ?schedule_to_start_timeout ?start_to_close_timeout
                           ?heartbeat_timeout ?retry_policy
                           ?priority:(Option.map Priority.to_base priority)
                           ~cancellation_type:(runtime_cancellation_type cancellation_type)
                           ~do_not_eagerly_execute
                           ~decode:(Codec_private.decode_base definition.output)
                           ())
                  in
                  match schedule_result with
                  | Error error -> failed_handle error
                  | Ok (future, cancel) ->
                      let request_cancel () =
                        Result.map_error Error_private.of_base (cancel ())
                      in
                      let () =
                        Option.iter
                          (fun scope ->
                            (* Scope ownership was checked before scheduling;
                               this registration is therefore deterministic and
                               cannot race another Domain. *)
                            ignore (Scope.on_cancel ~until:(Future_private.of_internal future) scope request_cancel))
                          scope
                      in
                      {
                        future = Future_private.of_internal future;
                        cancel = request_cancel;
                      }))))

(* Public wrapper keeps the implementation-only [local] switch out of the
   published signature; local callers use the dedicated [start_local] helper. *)
let start_handle ?scope ?activity_id ?task_queue ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
    ?retry_policy ?priority ?cancellation_type ?do_not_eagerly_execute definition input =
  start_handle_internal ?scope ?activity_id ?task_queue ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
    ?retry_policy ?priority ?cancellation_type ?do_not_eagerly_execute
    ~local:false definition input

(** Returns the typed future associated with an activity handle. *)
let future handle = handle.future

(** Requests cancellation of the exact activity represented by [handle].
    Repeated calls are idempotent, including calls after the activity has
    already resolved. The Temporal Core command carries the private sequence,
    so this operation intentionally has no unused reason argument. *)
let cancel handle = handle.cancel ()

(** Schedules an activity and returns only its future. Callers that need to
    cancel explicitly should retain the handle returned by [start_handle]. *)
let start ?scope ?activity_id ?task_queue ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
    ?retry_policy ?priority ?cancellation_type ?do_not_eagerly_execute definition input =
  future
    (start_handle ?scope ?activity_id ?task_queue ?schedule_to_close_timeout
       ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
       ?retry_policy ?priority ?cancellation_type ?do_not_eagerly_execute definition input)

(** Direct-style convenience for scheduling and awaiting one activity. *)
let execute ?scope ?activity_id ?task_queue ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
    ?retry_policy ?priority ?cancellation_type ?do_not_eagerly_execute definition input =
  Future.await
    (start ?scope ?activity_id ?task_queue ?schedule_to_close_timeout
       ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
       ?retry_policy ?priority ?cancellation_type ?do_not_eagerly_execute definition input)

(** Schedules an activity on Temporal Core's local activity lane. Local
    activities execute in this worker process and are recorded as history
    markers, so they are useful for short deterministic helper calls that do
    not need a remote task queue. *)
let start_local ?activity_id ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?retry_policy
    ?(cancellation_type = Wait_cancellation_completed) definition input =
  start_handle_internal ~local:true ?activity_id ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?retry_policy
    ~cancellation_type definition input
  |> future

(** Executes a local activity and waits for its typed result. *)
let execute_local ?activity_id ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?retry_policy
    ?cancellation_type definition input =
  Future.await
    (start_local ?activity_id ?schedule_to_close_timeout
       ?schedule_to_start_timeout ?start_to_close_timeout ?retry_policy
       ?cancellation_type definition input)
