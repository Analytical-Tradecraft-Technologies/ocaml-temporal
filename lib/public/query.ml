(** Typed query definitions and their read-only handler boundary. *)

type 'output definition = {
  (* The validated query name used by the local registry and native worker
     adapter. *)
  name : string;
  (* The result codec retained by the query definition. *)
  output : 'output Codec.t;
}

(* The public [t] alias keeps the definition opaque while avoiding the nested
   [Handler.t] name inside the existential package. *)
type 'output t = 'output definition

(* A query with one typed input value.  Keeping this as a separate public
   type preserves source compatibility for existing output-only queries while
   allowing new handlers to use the payload list already supported by the
   native protocol. *)
type ('input, 'output) typed_definition = {
  name : string;
  input : 'input Codec.t;
  output : 'output Codec.t;
}

type ('input, 'output) typed = ('input, 'output) typed_definition

(* Keep interaction names aligned with the strict native identifier contract. *)
let max_name_bytes = 65_536

(** Rejects an unsafe query name before a handler or registry can retain it. *)
let validate_name name =
  if String.equal name "" then invalid_arg "Temporal query name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal query name contains a NUL byte";
  if String.length name > max_name_bytes then
    invalid_arg "Temporal query name exceeds 65536 bytes";
  if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "Temporal query name must be valid UTF-8"

(** Creates a validated query definition. *)
let define ~name ~output =
  validate_name name;
  { name; output }

(** Creates a query definition whose handler receives one value decoded with
    [input].  The native bridge still carries an ordered payload list; the
    public API deliberately accepts exactly one typed value so arity errors
    remain explicit rather than silently dropping arguments. *)
let define_with_input ~name ~input ~output =
  validate_name name;
  { name; input; output }

(** Returns the stable query name. *)
let name (query : 'output t) = query.name

(** Returns the stable name of a typed-input query. *)
let name_with_input (query : ('input, 'output) typed) = query.name

(** Returns the query result codec. *)
let output (query : 'output t) = query.output

(** Returns the codec used to decode a typed query's input. *)
let input (query : ('input, 'output) typed) = query.input

(** Returns the codec used to decode a typed query's result. *)
let output_with_input (query : ('input, 'output) typed) = query.output

module Handler = struct
  (** A handler retains its query definition and callback as one existential
      package, preventing output codec mismatches in a registry. *)
  type t = Handler : {
    name : string;
    (* The payload-level closure is the single existential boundary.  It
       decodes typed arguments and encodes typed results before the runtime
       sees the handler, so no unsafe cast or scheduler state crosses here. *)
    dispatch_payloads : Payload.t list -> (Payload.t, Error.t) result;
  } -> t

  (** Associates a read-only callback with [query]. *)
  let make (query : 'output definition) implementation =
    Handler
      {
        name = query.name;
        dispatch_payloads = (fun payloads ->
          match payloads with
          | [] -> (
              let result =
                try implementation () with
                | exception_ ->
                    Error
                      (Error.defect
                         ~message:
                           (Printf.sprintf "query handler raised: %s"
                              (Printexc.to_string exception_)))
              in
              match result with
              | Error _ as error -> error
              | Ok value -> (
                  try Codec.encode query.output value with
                  | Temporal_sdk_kernel.Scheduler.Workflow_aborted as exception_ ->
                      raise exception_
                  | Temporal_sdk_kernel.Future_store.Scheduler_shutdown as exception_ ->
                      raise exception_
                  | exception_ ->
                      Error
                        (Error.defect
                           ~message:
                             (Printf.sprintf "query output codec raised: %s"
                                (Printexc.to_string exception_)))))
          | _ ->
              Error
                (Error.defect
                   ~message:"output-only query received an input payload"));
      }

  (** Associates a one-input callback with [query]. Input and output codecs
      are applied inside the handler closure, which keeps worker dispatch
      independent of the existential input type. *)
  let make_with_input (query : ('input, 'output) typed) implementation =
    Handler
      {
        name = query.name;
        dispatch_payloads = (fun payloads ->
          match payloads with
          | [ payload ] -> (
              (* [Codec.make] reports ordinary callback exceptions as typed
                 codec errors. Keep any unexpected codec exception inside the
                 existential worker boundary as a defect. *)
              match Codec.decode query.input payload with
              | exception (Temporal_sdk_kernel.Scheduler.Workflow_aborted as exception_) ->
                  raise exception_
              | exception (Temporal_sdk_kernel.Future_store.Scheduler_shutdown as exception_) ->
                  raise exception_
              | exception exception_ ->
                  Error
                    (Error.defect
                       ~message:
                         (Printf.sprintf "query input codec raised: %s"
                            (Printexc.to_string exception_)))
              | Error error -> Error error
              | Ok value -> (
                  let result =
                    try implementation value with
                    | exception_ ->
                        Error
                          (Error.defect
                             ~message:
                               (Printf.sprintf "query handler raised: %s"
                                  (Printexc.to_string exception_)))
                  in
                  match result with
                  | Error _ as error -> error
                  | Ok output -> (
                      try Codec.encode query.output output with
                      | Temporal_sdk_kernel.Scheduler.Workflow_aborted as exception_ ->
                          raise exception_
                      | Temporal_sdk_kernel.Future_store.Scheduler_shutdown as exception_ ->
                          raise exception_
                      | exception_ ->
                          Error
                            (Error.defect
                               ~message:
                                 (Printf.sprintf
                                    "query output codec raised: %s"
                                    (Printexc.to_string exception_))))))
          | [] ->
              Error
                (Error.defect
                   ~message:"typed query requires exactly one input payload")
          | _ ->
              Error
                (Error.defect
                   ~message:"typed query received multiple input payloads"));
      }

  (** Registration-friendly alias for [make]. *)
  let handle : 'output definition -> (unit -> ('output, Error.t) result) -> t = make

  (** Registration-friendly alias for one-input handlers. *)
  let handle_with_input = make_with_input

  (** Returns the name used by the dispatcher. *)
  let name (Handler { name; _ }) = name

  (** Runs the query callback and encodes its output. Ordinary callback
      exceptions become defects, and [Codec.make] reports ordinary codec
      callback exceptions as typed codec errors. Scheduler control exceptions
      from codecs still unwind the workflow fiber. *)
  let dispatch_payloads (Handler { dispatch_payloads; _ }) =
    dispatch_payloads

  let dispatch handler = dispatch_payloads handler []
end
