module Bridge = Temporal_core_bridge.Native_bridge

(** Extracts a successful bridge result or reports the native status and
    message as a test failure. *)
let unwrap = function
  | Ok value -> value
  | Error error -> failwith error.Bridge.message

let () =
  assert
    (Temporal.Runtime_info.native_bridge_abi_version ()
    = Ok Bridge.abi_version);
  unwrap (Bridge.check_abi_version Bridge.abi_version);
  (match Bridge.check_abi_version 1l with
  | Error { status = Abi_mismatch; message } -> assert (String.length message > 0)
  | _ -> failwith "ABI mismatch was not returned as a typed error");
  let input = Bytes.init 256 Char.chr in
  assert (unwrap (Bridge.echo input) = input);
  let progressed = Atomic.make false in
  let waiter = Domain.spawn (fun () -> unwrap (Bridge.conformance_wait_ms 100)) in
  let worker =
    Domain.spawn (fun () ->
        let deadline = Unix.gettimeofday () +. 0.05 in
        while Unix.gettimeofday () < deadline do
          Domain.cpu_relax ()
        done;
        Atomic.set progressed true)
  in
  Domain.join worker;
  Domain.join waiter;
  assert (Atomic.get progressed);
  let runtime = unwrap (Bridge.runtime_create ()) in
  (match Bridge.client_config ~target_url:"not a URL" ~identity:"worker" with
  | Error { status = Configuration; message } ->
      assert (String.length message > 0)
  | _ -> failwith "invalid client configuration was accepted");
  (* The sender-side mirror rejects the Core cache invariant before JSON is
     serialized, so this invalid document cannot reach native worker startup. *)
  (match
     Bridge.worker_config ~namespace:"temporal-sdk-test"
       ~task_queue:"ocaml-temporal-unit" ~build_id:"unit-build"
       ~max_cached_workflows:100 ~max_outstanding_workflow_tasks:100
       ~max_concurrent_workflow_task_polls:1 ~graceful_shutdown_timeout_ms:1_000L
       ()
   with
  | Error { status = Configuration; message } ->
      assert
        (String.starts_with
           ~prefix:"max_concurrent_workflow_task_polls must be at least 2"
           message)
  | _ -> failwith "cached worker configuration with one poller was accepted");
  (match
     Bridge.worker_config ~namespace:"temporal-sdk\000test"
       ~task_queue:"ocaml-temporal-unit" ~build_id:"unit-build"
       ~max_cached_workflows:0 ~max_outstanding_workflow_tasks:100
       ~max_concurrent_workflow_task_polls:1 ~graceful_shutdown_timeout_ms:1_000L
       ()
   with
  | Error { status = Configuration; message } ->
      assert (String.starts_with ~prefix:"namespace must not contain NUL" message)
  | _ -> failwith "worker configuration with a NUL namespace was accepted");
  let worker_config =
    unwrap
      (Bridge.worker_config ~namespace:"temporal-sdk-test"
         ~task_queue:"ocaml-temporal-unit" ~build_id:"unit-build"
         ~max_cached_workflows:100 ~max_outstanding_workflow_tasks:100
         ~max_concurrent_workflow_task_polls:5
         ~graceful_shutdown_timeout_ms:1_000L ())
  in
  (match Bridge.worker_start runtime worker_config with
  | Error { status = Invalid_state; message } ->
      assert (String.length message > 0)
  | _ -> failwith "worker construction without a client was accepted");
  (* Constructing the modern routing variant exercises the OCaml serializer
     and Rust decoder together before the expected missing-client failure. *)
  let deployment_worker_config =
    unwrap
      (Bridge.worker_config ~namespace:"temporal-sdk-test"
         ~task_queue:"ocaml-temporal-unit" ~build_id:"agents-v3"
         ~versioning:
           (Bridge.Deployment_based
              {
                deployment_name = "agents";
                build_id = "agents-v3";
                use_worker_versioning = true;
                default_versioning_behavior = Some Bridge.Pinned;
              })
         ~max_cached_workflows:0 ~max_outstanding_workflow_tasks:100
         ~max_concurrent_workflow_task_polls:1
         ~graceful_shutdown_timeout_ms:1_000L ())
  in
  (* The sender-side mirror rejects a versioned worker without a default
     behavior; completions never carry a per-workflow behavior, so Core would
     otherwise send UNSPECIFIED for every task (issue #817). *)
  (match
     Bridge.worker_config ~namespace:"temporal-sdk-test"
       ~task_queue:"ocaml-temporal-unit" ~build_id:"agents-v3"
       ~versioning:
         (Bridge.Deployment_based
            {
              deployment_name = "agents";
              build_id = "agents-v3";
              use_worker_versioning = true;
              default_versioning_behavior = None;
            })
       ~max_cached_workflows:0 ~max_outstanding_workflow_tasks:100
       ~max_concurrent_workflow_task_polls:1
       ~graceful_shutdown_timeout_ms:1_000L ()
   with
  | Error { status = Configuration; message } ->
      assert
        (String.starts_with
           ~prefix:
             "versioning.use_worker_versioning requires \
              default_versioning_behavior"
           message)
  | _ -> failwith "versioned worker without a default behavior was accepted");
  (match Bridge.worker_start runtime deployment_worker_config with
  | Error { status = Invalid_state; message } ->
      assert (String.length message > 0)
  | _ -> failwith "deployment worker construction without a client was accepted");
  (* A worker must poll only the task kinds it registered (#805). Reaching the
     missing-client state, rather than a configuration error, proves Rust
     strictly decoded each serialized task_types document. *)
  List.iter
    (fun (workflow_tasks, activity_tasks) ->
      let config =
        unwrap
          (Bridge.worker_config ~namespace:"temporal-sdk-test"
             ~task_queue:"ocaml-temporal-unit" ~build_id:"unit-build"
             ~max_cached_workflows:100 ~max_outstanding_workflow_tasks:100
             ~max_concurrent_workflow_task_polls:2
             ~graceful_shutdown_timeout_ms:1_000L ~workflow_tasks
             ~activity_tasks ())
      in
      match Bridge.worker_start runtime config with
      | Error { status = Invalid_state; message } ->
          assert (String.length message > 0)
      | _ -> failwith "task-type worker construction without a client was accepted")
    [ (true, false); (false, true) ];
  (match
     Bridge.worker_config ~namespace:"temporal-sdk-test"
       ~task_queue:"ocaml-temporal-unit" ~build_id:"unit-build"
       ~max_cached_workflows:100 ~max_outstanding_workflow_tasks:100
       ~max_concurrent_workflow_task_polls:2 ~graceful_shutdown_timeout_ms:1_000L
       ~workflow_tasks:false ~activity_tasks:false ()
   with
  | Error { status = Configuration; message } ->
      assert (message = "task_types must enable workflows or activities")
  | _ -> failwith "worker configuration without task types was accepted");
  (match Bridge.worker_try_poll_workflow runtime with
  | Error { status = Invalid_state; message } ->
      assert (String.length message > 0)
  | _ -> failwith "workflow polling without a worker was accepted");
  (match Bridge.worker_wait_workflow runtime with
  | Error { status = Invalid_state; message } ->
      assert (String.length message > 0)
  | _ -> failwith "workflow readiness wait without a worker was accepted");
  (match Bridge.worker_try_poll_activity runtime with
  | Error { status = Invalid_state; message } ->
      assert (String.length message > 0)
  | _ -> failwith "activity polling without a worker was accepted");
  (match Bridge.worker_wait_activity runtime with
  | Error { status = Invalid_state; message } ->
      assert (String.length message > 0)
  | _ -> failwith "activity readiness wait without a worker was accepted");
  (match Bridge.worker_wait_any runtime with
  | Error { status = Invalid_state; message } ->
      assert (String.length message > 0)
  | _ -> failwith "combined readiness wait without a worker was accepted");
  (* A submitted client call (#807) validates its document, then needs a
     connected client; neither failure registers a call. *)
  (match Bridge.client_submit_json runtime Bridge.Call_wait Bytes.empty with
  | Error { status = Protocol; message } -> assert (String.length message > 0)
  | _ -> failwith "malformed submitted wait was accepted");
  (match
     Bridge.client_submit_json runtime Bridge.Call_signal
       (Bytes.of_string
          {|{"namespace":"default","workflow_id":"w","run_id":"r","signal_name":"s","request_id":"q","input":[]}|})
   with
  | Error { status = Invalid_state; message } -> assert (String.length message > 0)
  | _ -> failwith "submitted signal without a client was accepted");
  (match Bridge.worker_complete_workflow_json runtime Bytes.empty with
  | Error { status = Protocol; message } ->
      assert (String.length message > 0)
  | _ -> failwith "malformed workflow completion was accepted");
  (match Bridge.worker_complete_activity_json runtime Bytes.empty with
  | Error { status = Protocol; message } ->
      assert (String.length message > 0)
  | _ -> failwith "malformed activity completion was accepted");
  (match Bridge.worker_reject_workflow_json runtime Bytes.empty with
  | Error { status = Protocol; message } ->
      assert (String.length message > 0)
  | _ -> failwith "malformed workflow rejection was accepted");
  (match Bridge.worker_reject_activity_json runtime Bytes.empty with
  | Error { status = Protocol; message } ->
      assert (String.length message > 0)
  | _ -> failwith "malformed activity rejection was accepted");
  (* Replay histories are checked in OCaml before the C call, then checked
     again by Rust. Starting the worker here proves this private path does not
     require a client connection; malformed input proves the sender-side
     validator rejects it without creating a feeder entry. *)
  unwrap (Bridge.replay_worker_start runtime worker_config);
  (match
     Bridge.replay_worker_feed_history runtime
       (Bytes.of_string
          {|{"workflow_id":"run","history":{"encoding":"base64","data":"not canonical"}}|})
   with
  | Error { status = Protocol; message } ->
      assert (String.length message > 0)
  | _ -> failwith "malformed replay history was accepted");
  unwrap (Bridge.replay_worker_dispose runtime);
  unwrap (Bridge.replay_worker_dispose runtime);
  unwrap (Bridge.worker_shutdown runtime);
  unwrap (Bridge.worker_shutdown runtime);
  unwrap (Bridge.client_disconnect runtime);
  unwrap (Bridge.client_disconnect runtime);
  unwrap (Bridge.runtime_close runtime);
  unwrap (Bridge.runtime_close runtime)
