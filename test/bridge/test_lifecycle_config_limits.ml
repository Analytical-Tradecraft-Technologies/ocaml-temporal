(** Boundary regression for #1224: every lifecycle string at the documented
    65,536-byte limit reaches native code.

    [Native_bridge.client_config] and [worker_config] accept each identifier
    up to 65,536 bytes, but Rust used to cap the whole lifecycle document at
    64 KiB. A single maximal identity, or a worker whose names together
    exceeded that, passed every OCaml check and then failed late with a
    [Configuration] "lifecycle configuration exceeds" error. Both documents
    below must now decode in Rust: the client then fails only to connect to
    a refused endpoint, and the replay worker, which needs no client,
    starts. *)

module Bridge = Temporal_core_bridge.Native_bridge

(** Unwraps a bridge result, failing the test with [label]. *)
let ok label = function
  | Ok value -> value
  | Error (error : Bridge.error) ->
      failwith (Printf.sprintf "%s failed: %s" label error.message)

(** The documented per-field limit. *)
let limit = 65_536

(** Loopback port 1 is closed on every supported platform, so connecting
    is refused without a Temporal Server. *)
let refused_target = "http://127.0.0.1:1"

(** Fails when [error] is the document-size rejection this test guards. *)
let reject_document_cap label (error : Bridge.error) =
  if
    error.status = Bridge.Configuration
    && String.starts_with ~prefix:"lifecycle configuration exceeds"
         error.message
  then failwith (label ^ ": rejected by the lifecycle document cap")

(** A maximal identity, once plain and once of control characters that each
    take a six-byte JSON escape, reaches the connect step. *)
let test_client_identity_at_limit () =
  List.iter
    (fun (label, identity) ->
      let runtime = ok "runtime" (Bridge.runtime_create ()) in
      let config =
        ok (label ^ " config")
          (Bridge.client_config ~target_url:refused_target ~identity ())
      in
      (match Bridge.client_connect runtime config with
      | Ok () -> failwith (label ^ ": connected to a refused endpoint")
      | Error error ->
          reject_document_cap label error;
          if error.status <> Bridge.Connection then
            failwith
              (Printf.sprintf "%s: expected a connection error, got %s" label
                 error.message));
      ok (label ^ " close") (Bridge.runtime_close runtime))
    [
      ("plain identity", String.make limit 'i');
      ("escaped identity", String.make limit '\001');
    ]

(** A replay worker whose namespace, task queue and build ID are each at the
    limit decodes and starts. *)
let test_worker_names_at_limit () =
  let runtime = ok "runtime" (Bridge.runtime_create ()) in
  let config =
    ok "worker config"
      (Bridge.worker_config ~namespace:(String.make limit 'n')
         ~task_queue:(String.make limit 'q') ~build_id:(String.make limit 'b')
         ~max_cached_workflows:0 ~max_outstanding_workflow_tasks:1
         ~max_concurrent_workflow_task_polls:1
         ~graceful_shutdown_timeout_ms:1_000L ())
  in
  (match Bridge.replay_worker_start runtime config with
  | Ok () -> ()
  | Error error ->
      reject_document_cap "worker names" error;
      failwith ("replay worker did not start: " ^ error.message));
  ok "worker close" (Bridge.runtime_close runtime)

(** Runs both boundary cases. *)
let () =
  test_client_identity_at_limit ();
  test_worker_names_at_limit ()
