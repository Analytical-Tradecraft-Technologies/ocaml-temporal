(** Driver for the live sticky-cache eviction acceptance.

    The client starts two exact runs while the worker is configured with one
    Core cache slot. Each run schedules a long deterministic timer, so the
    first task reaches a durable pending boundary. Before admitting B, the
    driver waits for the worker's payload-free completion marker for A. This
    marker is written only after Core acknowledges A's initial activation, so
    the cache transition is synchronized without issuing a control-plane query
    that can wait on a separate poller during cache pressure. The driver then
    requires the worker's payload-free eviction marker. The second run's
    normal-completion marker is retained only as timeout diagnostics: pinned
    Core ordering buffers B until A's [RemoveFromCache(CacheFull)] activation
    has been acknowledged, so B is never evidence that eviction has happened.
    It cancels both exact runs and requires each to reach Temporal's typed
    cancellation outcome. The client never registers or executes workflow
    code. *)

module Client = Temporal.Client
module Error = Temporal.Error
module Definitions = Smoke_definitions

(** Records one client-driver phase without exposing payloads or native
    connection details. The phase markers distinguish setup, start, marker,
    cancellation, and wait failures when a live RPC returns a generic bridge
    status. *)
let phase operation status =
  Printf.eprintf "cache eviction phase=%s status=%s\n%!" operation status

(** Binds start and verified terminal observations to the exact client handle.
    The diagnostic bundle keeps these identifiers but never a payload. *)
let execution_phase operation status handle =
  Printf.eprintf
    "cache eviction phase=%s status=%s workflow_id=%s run_id=%s\n%!" operation
    status (Client.workflow_id handle) (Client.run_id handle)

(** Reads a required non-empty environment value as a typed configuration
    result rather than raising while the executable is initializing. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when value <> "" -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Converts the bounded driver timeout into a positive floating-point wait
    budget used only for the file-backed test coordination marker. *)
let timeout_seconds () =
  (* The cache fixture has a larger budget than the ordinary client driver
     because Core may wait for a later workflow-task boundary before emitting
     RemoveFromCache.  Keep the generic variable as a local/manual fallback. *)
  let configured_timeout () =
    match Sys.getenv_opt "SMOKE_CACHE_EVICTION_TIMEOUT_SECONDS" with
    | Some value when value <> "" -> Some value
    | _ -> Sys.getenv_opt "SMOKE_DRIVER_TIMEOUT_SECONDS"
  in
  match configured_timeout () with
  | None -> Ok 300.
  | Some value -> (
      try
        let seconds = float_of_string value in
        if Float.is_finite seconds && seconds > 0. then Ok seconds
        else Error (Error.defect ~message:"SMOKE_DRIVER_TIMEOUT_SECONDS must be positive")
      with _ ->
        Error
          (Error.defect
             ~message:"SMOKE_DRIVER_TIMEOUT_SECONDS must be a number"))

(** Removes a prior coordination marker and verifies that the shared bind
    mount no longer exposes it. Failing closed here prevents an interrupted
    acceptance run from being mistaken for the current workflow's readiness or
    eviction event. *)
let clear_marker path =
  try
    if Sys.file_exists path then Sys.remove path;
    if Sys.file_exists path then
      Error (Error.defect ~message:("cannot remove stale marker " ^ path))
    else Ok ()
  with exception_ ->
    Error
      (Error.defect
         ~message:
           (Printf.sprintf "cannot remove stale marker %s: %s" path
              (Printexc.to_string exception_)))

(** Converts a file-wait diagnostic to the driver's typed failure. Both waits
    use the process deadline; neither grants a new budget after an earlier phase. *)
let wait_for_marker ~path ~expected ~timeout =
  Cache_eviction_wait.wait_for_marker ~path ~expected ~timeout
  |> Result.map_error (fun message -> Error.defect ~message)

(** Records both observations before returning the cache-pressure failure. *)
let wait_for_eviction_with_second_diagnostic ~eviction ~second_ready ~timeout =
  Cache_eviction_wait.wait_for_eviction_with_second_diagnostic
    ~eviction ~second_ready ~timeout
  |> Result.map_error (fun message -> Error.defect ~message)

(** Requires an exact run to report Temporal's typed cancellation category. *)
let require_cancelled label = function
  | Client.Cancelled error ->
      let view = Error.view error in
      if view.category = `Cancelled && not view.non_retryable then Ok ()
      else
        Error
          (Error.defect
             ~message:
               (Printf.sprintf "%s returned unexpected cancellation metadata"
                  label))
  | Client.Completed _ ->
      Error (Error.defect ~message:(label ^ " completed instead of cancelling"))
  | Client.Failed error
  | Client.Terminated error
  | Client.Timed_out error ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "%s ended as %s: %s" label (Error.kind error)
                (Error.message error)))
  | Client.Continued_as_new execution ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "%s continued as new at run %s" label
                execution.run_id))

(** Cancels one exact run with a distinct idempotency key. *)
let cancel handle ~request_id =
  Client.cancel ~request_id ~reason:"cache eviction acceptance requested" handle

(** Starts two exact executions against the one-slot worker cache. The
    cache-only workflow has no durable command before it parks, so each initial
    workflow task can complete as an empty non-terminal activation while Core
    retains the execution in its sticky cache. Admitting the second execution
    then forces a real cache-full eviction; the driver observes that marker and
    proves cancellation still reaches each exact run. *)
let run () =
  match Sys.getenv_opt "TEMPORAL_TWO_BINARY_LIVE" with
  | Some "1" ->
      let open Temporal.Result_syntax in
      let* target_url = required_env "TEMPORAL_ADDRESS" in
      let* namespace = required_env "TEMPORAL_NAMESPACE" in
      let* marker = required_env "SMOKE_CACHE_EVICTION_FILE" in
      let* ready = required_env "SMOKE_CACHE_EVICTION_READY_FILE" in
      let* second_ready =
        required_env "SMOKE_CACHE_EVICTION_SECOND_READY_FILE"
      in
      let* timeout = timeout_seconds () in
      phase "client_create" "begin";
      let* client =
        Client.create ~target_url ~namespace
          ~identity:"ocaml-temporal-cache-eviction-driver" ()
      in
      phase "client_create" "ok";
      let finish result =
        (* Emit the causal failure before shutdown, which may itself consume
           the watchdog reserve if the native client cannot make progress. *)
        (match result with
        | Ok () -> ()
        | Error error ->
            Printf.eprintf "cache eviction diagnostic: %s\n%!"
              (Error.message error));
        match Client.shutdown client with
        | Ok () ->
            phase "client_shutdown" "ok";
            result
        | Error error -> Error error
      in
      let result =
        let* () = clear_marker marker in
        let* () = clear_marker ready in
        let* () = clear_marker second_ready in
        phase "start_a" "begin";
        let* first =
          Client.start client ~workflow:Definitions.cache_eviction
            ~task_queue:Definitions.task_queue
            ~id:"two-binary-cache-eviction-a" ~input:"first" ()
        in
        phase "start_a" "ok";
        execution_phase "start_a" "accepted" first;
        Printf.eprintf "cache eviction execution=a workflow_id=%s run_id=%s\n%!"
          "two-binary-cache-eviction-a" (Client.run_id first);
        phase "cache_settling" "begin";
        let* () =
          wait_for_marker ~path:ready ~expected:"initial-completion\n" ~timeout
        in
        phase "cache_settling" "observed";
        phase "start_b" "begin";
        let* second =
          Client.start client ~workflow:Definitions.cache_eviction
            ~task_queue:Definitions.task_queue
            ~id:"two-binary-cache-eviction-b" ~input:"second" ()
        in
        phase "start_b" "ok";
        execution_phase "start_b" "accepted" second;
        Printf.eprintf "cache eviction execution=b workflow_id=%s run_id=%s\n%!"
          "two-binary-cache-eviction-b" (Client.run_id second);
        phase "eviction_marker" "begin";
        let* () =
          wait_for_eviction_with_second_diagnostic ~eviction:marker
            ~second_ready ~timeout
        in
        phase "eviction_marker" "observed";
        phase "cancel_b" "begin";
        let* () = cancel second ~request_id:"two-binary-cache-eviction-cancel-b" in
        phase "cancel_b" "ok";
        phase "wait_b" "begin";
        let* second_outcome = Client.wait second in
        phase "wait_b" "ok";
        let* () = require_cancelled "cache eviction run B" second_outcome in
        execution_phase "wait_b" "cancelled" second;
        phase "cancel_a" "begin";
        let* () = cancel first ~request_id:"two-binary-cache-eviction-cancel-a" in
        phase "cancel_a" "ok";
        phase "wait_a" "begin";
        let* first_outcome = Client.wait first in
        phase "wait_a" "ok";
        let* () = require_cancelled "cache eviction run A" first_outcome in
        execution_phase "wait_a" "cancelled" first;
        Ok ()
      in
      finish result
  | _ ->
      Error
        (Error.defect
           ~message:
             "cache eviction acceptance is not enabled; set TEMPORAL_TWO_BINARY_LIVE=1")

(** Converts the typed process result into a useful command status without
    exposing workflow payloads or native protocol details. *)
let () =
  match run () with
  | Ok () -> Printf.printf "cache eviction driver assertions passed\n%!"
  | Error error ->
      Printf.eprintf "cache eviction driver failed (%s): %s\n%!"
        (Error.kind error) (Error.message error);
      exit 1
