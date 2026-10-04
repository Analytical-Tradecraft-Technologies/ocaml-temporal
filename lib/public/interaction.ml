(** Deterministic local routing for typed workflow interactions. *)

module Name_map = Map.Make (String)

(** The three registries are separate even though they share a name type:
    Temporal permits a signal, query, and update to use the same name without
    making one kind shadow another. Persistent maps make the dispatcher safe
    to share between callers after construction. *)
type t = {
  signals : Signal.Handler.t Name_map.t;
  queries : Query.Handler.t Name_map.t;
  updates : Update.Handler.t Name_map.t;
}

(** Builds one named map and reports the first duplicate registration. The
    handler is already existentially packaged, so no unsafe cast is needed to
    store callbacks with different input or output types together. *)
let add_handler kind name handler map =
  if Name_map.mem name map then
    Error
      (Error.defect
         ~message:(Printf.sprintf "duplicate %s handler registration: %s" kind name))
  else Ok (Name_map.add name handler map)

(** Folds a handler list into an immutable map while preserving the caller's
    left-to-right order for error reporting. *)
let collect kind name_of handlers =
  List.fold_left
    (fun result handler ->
      Result.bind result (fun map ->
          add_handler kind (name_of handler) handler map))
    (Ok Name_map.empty) handlers

(** Creates a dispatcher only after all three registries pass duplicate checks.
    This all-or-nothing construction prevents a partially configured worker
    from accepting some interaction kinds while silently dropping others. *)
let create ?(signals = []) ?(queries = []) ?(updates = []) () =
  match collect "signal" Signal.Handler.name signals with
  | Error error -> Error error
  | Ok signals -> (
      match collect "query" Query.Handler.name queries with
      | Error error -> Error error
      | Ok queries -> (
          match collect "update" Update.Handler.name updates with
          | Error error -> Error error
          | Ok updates -> Ok { signals; queries; updates }))

(** Returns the error used when a name has no local handler. The category
    matches the operation so callers preserve normal Temporal failure
    classification whether the same definition is dispatched locally or
    through the native worker bridge. *)
let missing_handler ~kind ~name ~category =
  Error
    (Error.make ~non_retryable:true ~category
       ~message:(Printf.sprintf "unregistered %s handler: %s" kind name) ())

(** Encodes and dispatches one signal. Encoding occurs before lookup so a
    malformed input is reported as a codec failure and cannot be confused with
    a missing registration; no callback runs in either failure case. Ordinary
    codec callback exceptions become typed codec errors, while private
    scheduler control exceptions still unwind the workflow fiber. *)
let signal dispatcher definition input =
  match Codec.encode (Signal.input definition) input with
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
  | Error error -> Error error
  | Ok payload -> (
      match Name_map.find_opt (Signal.name definition) dispatcher.signals with
      | None ->
          missing_handler ~kind:"signal" ~name:(Signal.name definition)
            ~category:`Workflow
      | Some handler -> Signal.Handler.dispatch handler payload)

(** Routes a query and decodes its encoded result with the requested
    definition. A name/codec mismatch is deliberately surfaced by [Codec]
    rather than being hidden by the existential handler package. Ordinary
    decoder callback exceptions become typed codec errors. *)
let query dispatcher definition =
  match Name_map.find_opt (Query.name definition) dispatcher.queries with
  | None ->
      missing_handler ~kind:"query" ~name:(Query.name definition)
        ~category:`Workflow
  | Some handler -> (
      match Query.Handler.dispatch handler with
      | Error error -> Error error
      | Ok payload -> (
          match Codec.decode (Query.output definition) payload with
          | result -> result
          | exception (Temporal_sdk_kernel.Scheduler.Workflow_aborted as exception_) ->
              raise exception_
          | exception (Temporal_sdk_kernel.Future_store.Scheduler_shutdown as exception_) ->
              raise exception_
          | exception exception_ ->
              Error
                (Error.defect
                   ~message:
                   (Printf.sprintf "query output codec raised: %s"
                      (Printexc.to_string exception_)))))

(** Routes a typed one-input query through the local dispatcher.  This mirrors
    [Client.query_with_input] without involving the native transport, which
    makes deterministic unit tests exercise the same codec and name checks. *)
let query_with_input dispatcher definition input =
  match Codec.encode (Query.input definition) input with
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
  | Ok encoded_input -> (
      match Name_map.find_opt (Query.name_with_input definition) dispatcher.queries with
      | None ->
          missing_handler ~kind:"query" ~name:(Query.name_with_input definition)
            ~category:`Workflow
      | Some handler -> (
          match
            Query.Handler.dispatch_payloads handler [ encoded_input ]
          with
          | Error error -> Error error
          | Ok payload -> (
              match Codec.decode (Query.output_with_input definition) payload with
              | result -> result
              | exception (Temporal_sdk_kernel.Scheduler.Workflow_aborted as exception_) ->
                  raise exception_
              | exception (Temporal_sdk_kernel.Future_store.Scheduler_shutdown as exception_) ->
                  raise exception_
              | exception exception_ ->
                  Error
                    (Error.defect
                       ~message:
                         (Printf.sprintf "query output codec raised: %s"
                            (Printexc.to_string exception_))))))

(** Encodes an update request, dispatches the validator/implementation pair,
    and decodes its result. The handler owns the validator order; this wrapper
    only owns the typed codec boundaries and name routing. Ordinary codec
    callback exceptions become typed codec errors. *)
let update dispatcher definition input =
  match Codec.encode (Update.input definition) input with
  | exception (Temporal_sdk_kernel.Scheduler.Workflow_aborted as exception_) ->
      raise exception_
  | exception (Temporal_sdk_kernel.Future_store.Scheduler_shutdown as exception_) ->
      raise exception_
  | exception exception_ ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "update input codec raised: %s"
                (Printexc.to_string exception_)))
  | Error error -> Error error
  | Ok payload -> (
      match Name_map.find_opt (Update.name definition) dispatcher.updates with
      | None ->
          missing_handler ~kind:"update" ~name:(Update.name definition)
            ~category:`Update
      | Some handler -> (
          match Update.Handler.dispatch handler payload with
          | Error error -> Error error
          | Ok output -> (
              match Codec.decode (Update.output definition) output with
              | result -> result
              | exception (Temporal_sdk_kernel.Scheduler.Workflow_aborted as exception_) ->
                  raise exception_
              | exception (Temporal_sdk_kernel.Future_store.Scheduler_shutdown as exception_) ->
                  raise exception_
              | exception exception_ ->
                  Error
                    (Error.defect
                       ~message:
                         (Printf.sprintf "update output codec raised: %s"
                            (Printexc.to_string exception_))))))
