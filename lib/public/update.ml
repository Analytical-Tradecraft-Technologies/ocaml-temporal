(** Typed update definitions and deterministic validator/handler dispatch. *)

type ('input, 'output) definition = {
  (* The validated update name used in registration and routing. *)
  name : string;
  (* Codec for the request payload. *)
  input : 'input Codec.t;
  (* Codec for the successful update result. *)
  output : 'output Codec.t;
}

(* The public [t] alias keeps the definition opaque while avoiding the nested
   [Handler.t] name inside the existential package. *)
type ('input, 'output) t = ('input, 'output) definition

(* This bound matches the bridge's identifier safety contract. *)
let max_name_bytes = 65_536

(** Validates one update name before any handler can retain it. *)
let validate_name name =
  if String.equal name "" then invalid_arg "Temporal update name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal update name contains a NUL byte";
  if String.length name > max_name_bytes then
    invalid_arg "Temporal update name exceeds 65536 bytes";
  if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "Temporal update name must be valid UTF-8"

(** Creates a validated update definition. *)
let define ~name ~input ~output =
  validate_name name;
  { name; input; output }

(** Returns the stable update name. *)
let name update = update.name

(** Returns the update input codec. *)
let input update = update.input

(** Returns the update output codec. *)
let output update = update.output

module Handler = struct
  (** An update handler keeps validator and implementation types paired with
      the same definition inside one existential package. *)
  type t = Handler : {
    definition : ('input, 'output) definition;
    validator : ('input -> (unit, Error.t) result) option;
    implementation : 'input -> ('output, Error.t) result;
  } -> t

  (** Builds a handler with an optional precondition validator. *)
  let make ?validator definition implementation =
    Handler { definition; validator; implementation }

  (** Registration-friendly alias for [make]. *)
  let handle = make

  (** Returns the name used by the dispatcher. *)
  let name (Handler { definition; _ }) = definition.name

  (** Performs the strict input-decode, validator, implementation, and output
      encode sequence. [on_validated] runs exactly once after input and
      validator checks succeed and immediately before the implementation. The
      runtime uses that boundary to acknowledge a handler before it parks on a
      workflow future. Callback exceptions are converted to defects, except
      private terminal/shutdown control flow which belongs to the scheduler. *)
  let dispatch ?(run_validator = true) ?on_validated
      (Handler { definition; validator; implementation }) payload =
    match Codec.decode definition.input payload with
    | exception exception_ ->
        Error
          (Error.defect
             ~message:
               (Printf.sprintf "update input codec raised: %s"
                  (Printexc.to_string exception_)))
    | Error error -> Error error
    | Ok input ->
        let validation_result =
          if not run_validator then Ok ()
          else
            match validator with
            | None -> Ok ()
            | Some validate -> (
                try
                  (* Validators run only for live requests, so a deterministic
                     helper must not advance mutable workflow state here.  The
                     runtime guard is scoped to this callback and restored
                     before the update implementation runs. *)
                  match Temporal_sdk_kernel.Workflow_context_store.current () with
                  | None -> validate input
                  | Some context ->
                      Temporal_sdk_kernel.Workflow_context_store.with_randomness_disabled
                        context (fun () -> validate input)
                with
                | exception_ ->
                    Error
                      (Error.defect
                         ~message:
                           (Printf.sprintf "update validator raised: %s"
                              (Printexc.to_string exception_))))
        in
        (match validation_result with
        | Error _ as error -> error
        | Ok () ->
            let acknowledgement_error =
              match on_validated with
              | None -> None
              | Some callback ->
                  (* The acknowledgement hook is deliberately effect-only.  A
                     callback defect becomes a typed update failure, so the
                     implementation is skipped when it raises. *)
                  (try
                     callback ();
                     None
                   with
                  | exception_ ->
                      Some
                        (Error.defect
                           ~message:
                             (Printf.sprintf
                                "update validation acknowledgement raised: %s"
                                (Printexc.to_string exception_))))
            in
            (match acknowledgement_error with
            | Some error -> Error error
            | None ->
                let result =
                  try implementation input with
                  | Temporal_sdk_kernel.Scheduler.Workflow_aborted as exception_ ->
                      raise exception_
                  | Temporal_sdk_kernel.Future_store.Scheduler_shutdown as exception_ ->
                      raise exception_
                  | exception_ ->
                      Error
                        (Error.defect
                           ~message:
                             (Printf.sprintf "update handler raised: %s"
                                (Printexc.to_string exception_)))
                in
                (match result with
                | Error _ as error -> error
                | Ok output -> (
                    match Codec.encode definition.output output with
                    | result -> result
                    | exception exception_ ->
                        Error
                          (Error.defect
                             ~message:
                               (Printf.sprintf "update output codec raised: %s"
                                  (Printexc.to_string exception_)))))))
end
