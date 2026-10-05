(** Live lifecycle acceptance test driven through the OCaml supervisor, C
    stubs, Rust bridge, official Core client, and real Temporal Server. *)
module Native = Sdk_supervisor.Native
module Bridge = Temporal_core_bridge.Native_bridge

(** Converts a native bridge status to stable diagnostic text without relying
    on the server's potentially variable error prose. *)
let bridge_status = function
  | Bridge.Invalid_argument -> "invalid_argument"
  | Abi_mismatch -> "abi_mismatch"
  | Panic -> "panic"
  | Internal -> "internal"
  | Invalid_state -> "invalid_state"
  | Configuration -> "configuration"
  | Connection -> "connection"
  | Worker -> "worker"
  | Outstanding_tasks -> "outstanding_tasks"
  | Not_ready -> "not_ready"
  | Protocol -> "protocol"
  | Already_started -> "already_started"
  | Retryable -> "retryable"
  | Async_heartbeat_rejected -> "async_heartbeat_rejected"
  | Resource_exhausted -> "resource_exhausted"
  | Unknown code -> Printf.sprintf "unknown(%d)" code

(** Raises a test failure while preserving structured bridge diagnostics. *)
let require = function
  | Ok value -> value
  | Error { Bridge.status; message } ->
      failwith (Printf.sprintf "%s: %s" (bridge_status status) message)

(** Raises a test failure for any supervisor-level lifecycle error. *)
let require_supervisor = function
  | Ok value -> value
  | Error (Native.Backend error) -> require (Error error)
  | Error Native.Closed -> failwith "supervisor closed unexpectedly"
  | Error (Native.Supervisor_failed exn | Native.Owner_unavailable exn) -> raise exn

(** Accepts only the structured lifecycle-state error expected by a negative
    transition assertion and fails with the full unexpected diagnostic. *)
let require_invalid_state = function
  | Error (Native.Backend { Bridge.status = Invalid_state; message }) ->
      if String.length message = 0 then failwith "empty invalid-state diagnostic"
  | result ->
      ignore (require_supervisor result);
      failwith "invalid lifecycle transition unexpectedly succeeded"

(** Connects and validates a workflow-only worker, then proves repeated reverse
    shutdown through the real OCaml-owned graph. *)
let () =
  let address = Sys.getenv "TEMPORAL_ADDRESS" in
  let namespace =
    Sys.getenv_opt "TEMPORAL_NAMESPACE"
    |> Option.value ~default:"temporal-sdk-test"
  in
  let client =
    require
      (Native.client_config ~target_url:address
         ~identity:"ocaml-temporal-live-lifecycle")
  in
  let worker =
    require
      (Native.worker_config ~namespace
         ~task_queue:"ocaml-temporal-live-lifecycle"
         ~build_id:"live-lifecycle-build" ~max_cached_workflows:100
         ~max_outstanding_workflow_tasks:100
         ~max_concurrent_workflow_task_polls:5
         ~graceful_shutdown_timeout_ms:1_000L ())
  in
  let supervisor = require_supervisor (Native.create ~capacity:8 ()) in
  require_supervisor (Native.perform supervisor (Native.Connect_client client));
  require_invalid_state
    (Native.perform supervisor (Native.Connect_client client));
  require_supervisor (Native.perform supervisor (Native.Start_worker worker));
  require_invalid_state (Native.perform supervisor (Native.Start_worker worker));
  require_invalid_state (Native.perform supervisor Native.Disconnect_client);
  require_supervisor (Native.perform supervisor Native.Shutdown_worker);
  require_supervisor (Native.perform supervisor Native.Shutdown_worker);
  require_supervisor (Native.perform supervisor Native.Disconnect_client);
  require_supervisor (Native.perform supervisor Native.Disconnect_client);
  (* Rebuild the children and let terminal supervisor shutdown prove the normal
     reverse-order worker-client-runtime path without prior child operations. *)
  require_supervisor (Native.perform supervisor (Native.Connect_client client));
  require_supervisor (Native.perform supervisor (Native.Start_worker worker));
  require_supervisor (Native.shutdown supervisor);
  require_supervisor (Native.shutdown supervisor)
