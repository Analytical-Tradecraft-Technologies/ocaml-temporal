(** Typed signal definitions and their deterministic handler boundary. *)

type 'input definition = {
  (* The validated Temporal signal name retained for registration and lookup. *)
  name : string;
  (* The codec that owns the signal's payload representation. *)
  input : 'input Codec.t;
}

(* The public [t] alias keeps the definition opaque while allowing the nested
   handler to name the definition without colliding with [Handler.t]. *)
type 'input t = 'input definition

(* Temporal's closed semantic identifier contract uses this same byte ceiling
   for workflow, activity, and interaction names. *)
let max_name_bytes = 65_536

(** Validates one interaction name before it can enter a registry or payload
    boundary. Keeping this check local means definitions remain safe even when
    they are used without a worker or dispatcher. *)
let validate_name name =
  if String.equal name "" then invalid_arg "Temporal signal name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal signal name contains a NUL byte";
  if String.length name > max_name_bytes then
    invalid_arg "Temporal signal name exceeds 65536 bytes";
  if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "Temporal signal name must be valid UTF-8"

(** Creates a validated signal definition without allocating any runtime state. *)
let define ~name ~input =
  validate_name name;
  { name; input }

(** Returns the stable signal name. *)
let name signal = signal.name

(** Returns the signal input codec. *)
let input signal = signal.input

module Handler = struct
  (** A handler keeps the definition and callback existentially paired so a
      registry cannot accidentally decode an input with another codec. *)
  type t = Handler : {
    definition : 'input definition;
    implementation : 'input -> (unit, Error.t) result;
  } -> t

  (** Builds a handler whose callback is associated with [signal]'s codec. The
      private native worker adapts this existential package to its
      scheduler-owned signal activation path. *)
  let make signal implementation = Handler { definition = signal; implementation }

  (** Registration-friendly alias for [make]. *)
  let handle = make

  (** Returns the name used by the interaction dispatcher. *)
  let name (Handler { definition; _ }) = definition.name

  (** Reclassifies an error returned while decoding a signal payload so it
      always fails the workflow task. A custom [Codec.make] decoder may return
      any category (for example [`Workflow]), which would otherwise close the
      run with a terminal failure. Decode-time [`Codec], [`Defect], and
      [`Bridge] errors already fail the task and are kept as they are; any other
      category is rewrapped as a non-retryable [`Codec] error with the same
      message, details, and error type. Errors returned by the handler itself
      are not passed through here, so their classification is preserved. *)
  let as_decode_failure error =
    let view = Error.view error in
    match view.category with
    | `Codec | `Defect | `Bridge -> error
    | _ ->
        Error.make ~non_retryable:true ?error_type:view.error_type
          ~details:view.details ~category:`Codec ~message:view.message ()

  (** Decodes and invokes one signal payload. [Codec.make] reports ordinary
      decoder exceptions as typed codec errors; unexpected codec exceptions and
      handler exceptions become non-retryable defects. Private terminal and
      shutdown exceptions from the handler reach the scheduler unchanged. *)
  let dispatch (Handler { definition; implementation }) payload =
    match Codec.decode definition.input payload with
    | result -> (
        match result with
        | Error error -> Error (as_decode_failure error)
        | Ok input -> (
            try implementation input with
            | Temporal_sdk_kernel.Scheduler.Workflow_aborted as exception_ ->
                raise exception_
            | Temporal_sdk_kernel.Future_store.Scheduler_shutdown as exception_ ->
                raise exception_
            | exception_ ->
                Error
                  (Error.defect
                     ~message:
                       (Printf.sprintf "signal handler raised: %s"
                          (Printexc.to_string exception_)))))
    | exception (Temporal_sdk_kernel.Scheduler.Workflow_aborted as exception_) ->
        raise exception_
    | exception (Temporal_sdk_kernel.Future_store.Scheduler_shutdown as exception_) ->
        raise exception_
    | exception exception_ ->
        Error
          (Error.defect
             ~message:
               (Printf.sprintf "signal input codec raised: %s"
                  (Printexc.to_string exception_)))

  (** Adapts Temporal's repeated signal payload list to the one typed input.
      Zero payloads are what the Temporal CLI, Web UI, other SDKs, and (since
      #819) this SDK's client and external signals send for a no-argument
      signal, so they decode as the canonical [binary/null] unit payload, as
      workflow start input already does. Older OCaml senders' single
      [binary/null] payload still decodes through the one-payload case. A
      handler whose codec rejects unit reports its own codec error.

      Multiple payloads are a payload-shape mismatch with the registered codec,
      so they are reported in the [`Codec] category rather than silently
      dropping data. Under the v1 fail-closed signal policy (#811) the worker
      runtime classifies [`Codec] as a workflow-task failure, exactly like an
      undecodable payload or a signal with no registered handler: the signal
      stays in history, the run stays open, and it makes no progress until a
      compatible worker replays it or the run is reset or terminated. It never
      closes the run as Failed. *)
  let dispatch_payloads handler = function
    | [] ->
        dispatch handler
          { Payload.metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }
    | [ payload ] -> dispatch handler payload
    | _ ->
        Error
          (Error.make ~non_retryable:true ~category:`Codec
             ~message:
               (Printf.sprintf
                  "signal %s must contain at most one payload for its \
                   registered OCaml handler"
                  (name handler))
             ())
end
