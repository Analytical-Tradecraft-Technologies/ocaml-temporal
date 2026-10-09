(** Boundary tests for the worker resource and shutdown options (#498).

    The private bridge must reject each invalid value before a native call,
    encode valid values as the exact document that
    [rust/core-bridge/tests/support/worker_tuning.rs] maps onto Core's
    [WorkerConfig], and keep a default worker's document byte-for-byte equal
    to the pre-#498 encoding. Reaching the bridge's missing-client state with
    a tuned document proves that Rust strictly decoded and validated it. *)

module Bridge = Temporal_core_bridge.Native_bridge

(** Extracts a successful bridge result or fails with its message. *)
let unwrap = function
  | Ok value -> value
  | Error error -> failwith error.Bridge.message

(** Every #498 setting, matching the Rust fixture's [TUNED_DOCUMENT]. *)
let tuned : Bridge.worker_tuning =
  {
    workflow_task_poller_autoscaling =
      Some { minimum = 2; maximum = 6; initial = 3 };
    sticky_queue_schedule_to_start_timeout_ms = Some 2_500L;
    max_heartbeat_throttle_interval_ms = Some 20_000L;
    default_heartbeat_throttle_interval_ms = Some 5_000L;
    max_worker_activities_per_second = Some 2.5;
    max_task_queue_activities_per_second = Some 40.5;
  }

(** Builds a worker document varying only the resource settings. *)
let config ?(max_cached_workflows = 4) ?(max_outstanding_workflow_tasks = 8)
    ?(max_concurrent_workflow_task_polls = 6)
    ?(graceful_shutdown_timeout_ms = 1_500L) ?tuning () =
  Bridge.worker_config ~namespace:"tuning-test" ~task_queue:"tuning-test"
    ~build_id:"tuning-test" ~max_cached_workflows
    ~max_outstanding_workflow_tasks ~max_concurrent_workflow_task_polls
    ~graceful_shutdown_timeout_ms ?tuning ()

(** Expects a [Configuration] error whose message starts with [prefix]. *)
let expect_rejected prefix result =
  match result with
  | Error { Bridge.status = Configuration; message } ->
      if not (String.starts_with ~prefix message) then
        failwith (Printf.sprintf "expected %S, got %S" prefix message)
  | Error { message; _ } -> failwith ("unexpected error status: " ^ message)
  | Ok _ -> failwith ("invalid worker tuning was accepted: " ^ prefix)

(** The encoder emits every explicit setting, and only those. *)
let test_tuned_document () =
  let document = Bridge.worker_config_document (unwrap (config ~tuning:tuned ())) in
  let expected =
    {|{"namespace":"tuning-test","task_queue":"tuning-test",|}
    ^ {|"build_id":"tuning-test","versioning":{"kind":"none"},|}
    ^ {|"max_cached_workflows":4,"max_outstanding_workflow_tasks":8,|}
    ^ {|"max_concurrent_workflow_task_polls":6,|}
    ^ {|"graceful_shutdown_timeout_ms":1500,|}
    ^ {|"task_types":{"workflows":true,"activities":true},|}
    ^ {|"tuning":{"workflow_task_poller_autoscaling":|}
    ^ {|{"minimum":2,"maximum":6,"initial":3},|}
    ^ {|"sticky_queue_schedule_to_start_timeout_ms":2500,|}
    ^ {|"max_heartbeat_throttle_interval_ms":20000,|}
    ^ {|"default_heartbeat_throttle_interval_ms":5000,|}
    ^ {|"max_worker_activities_per_second":2.5,|}
    ^ {|"max_task_queue_activities_per_second":40.5}}|}
  in
  if not (String.equal document expected) then
    failwith ("unexpected tuned worker document: " ^ document)

(** A default worker sends no [tuning] member, so an older bridge archive
    still accepts it; a partial tuning lists only the explicit settings. *)
let test_default_and_partial_documents () =
  let default_document =
    Bridge.worker_config_document
      (unwrap
         (config ~max_cached_workflows:1_000 ~max_outstanding_workflow_tasks:1_000
            ~max_concurrent_workflow_task_polls:2
            ~graceful_shutdown_timeout_ms:30_000L
            ~tuning:Bridge.default_worker_tuning ()))
  in
  let expected =
    {|{"namespace":"tuning-test","task_queue":"tuning-test",|}
    ^ {|"build_id":"tuning-test","versioning":{"kind":"none"},|}
    ^ {|"max_cached_workflows":1000,"max_outstanding_workflow_tasks":1000,|}
    ^ {|"max_concurrent_workflow_task_polls":2,|}
    ^ {|"graceful_shutdown_timeout_ms":30000,|}
    ^ {|"task_types":{"workflows":true,"activities":true}}|}
  in
  if not (String.equal default_document expected) then
    failwith ("unexpected default worker document: " ^ default_document);
  let partial =
    Bridge.worker_config_document
      (unwrap
         (config
            ~tuning:
              {
                Bridge.default_worker_tuning with
                max_worker_activities_per_second = Some 1.0;
              }
            ()))
  in
  if
    not
      (String.ends_with ~suffix:{|"tuning":{"max_worker_activities_per_second":1.0}}|}
         partial)
  then failwith ("unexpected partial worker document: " ^ partial)

(** Each invalid setting is rejected by the sender-side mirror before JSON
    reaches Rust, with a message naming the offending field. *)
let test_invalid_tuning () =
  let with_tuning tuning = config ~tuning () in
  let autoscaling minimum maximum initial =
    with_tuning
      {
        tuned with
        workflow_task_poller_autoscaling = Some { minimum; maximum; initial };
      }
  in
  expect_rejected "workflow_task_poller_autoscaling.minimum" (autoscaling 0 6 3);
  expect_rejected "workflow_task_poller_autoscaling.maximum must be at least"
    (autoscaling 6 5 5);
  expect_rejected "workflow_task_poller_autoscaling.initial" (autoscaling 2 6 7);
  expect_rejected "workflow_task_poller_autoscaling.maximum must equal"
    (autoscaling 2 5 3);
  List.iter
    (fun value ->
      expect_rejected "sticky_queue_schedule_to_start_timeout_ms"
        (with_tuning
           { tuned with sticky_queue_schedule_to_start_timeout_ms = Some value }))
    [ 0L; -1L; 86_400_001L; Int64.max_int ];
  expect_rejected "max_heartbeat_throttle_interval_ms"
    (with_tuning { tuned with max_heartbeat_throttle_interval_ms = Some 0L });
  expect_rejected "default_heartbeat_throttle_interval_ms must be"
    (with_tuning
       { tuned with default_heartbeat_throttle_interval_ms = Some (-5L) });
  expect_rejected "default_heartbeat_throttle_interval_ms must not exceed"
    (with_tuning
       { tuned with default_heartbeat_throttle_interval_ms = Some 20_001L });
  List.iter
    (fun value ->
      expect_rejected "max_worker_activities_per_second"
        (with_tuning { tuned with max_worker_activities_per_second = Some value });
      expect_rejected "max_task_queue_activities_per_second"
        (with_tuning
           { tuned with max_task_queue_activities_per_second = Some value }))
    [ 0.0; -1.0; Float.nan; Float.infinity; 5e-324 ];
  (* The worker rate's reciprocal becomes a Core Duration, so it is bounded
     to one day; the task-queue rate is only forwarded to the server. *)
  List.iter
    (fun value ->
      expect_rejected "max_worker_activities_per_second must be at least one"
        (with_tuning { tuned with max_worker_activities_per_second = Some value }))
    [ Float.min_float; 1e-6 ];
  ignore
    (unwrap
       (with_tuning
          {
            tuned with
            max_worker_activities_per_second = Some (1.0 /. 86_400.0);
            max_task_queue_activities_per_second = Some Float.min_float;
          }));
  (* Autoscaling bounds are per poll queue, so a caching worker with one
     poll per queue is valid even though one fixed poller is not. *)
  ignore
    (unwrap
       (config ~max_concurrent_workflow_task_polls:1
          ~tuning:
            {
              tuned with
              workflow_task_poller_autoscaling =
                Some { minimum = 1; maximum = 1; initial = 1 };
            }
          ()));
  expect_rejected "max_concurrent_workflow_task_polls must be at least 2"
    (config ~max_concurrent_workflow_task_polls:1 ());
  (* Grace period and counts keep their original bounds. *)
  expect_rejected "graceful_shutdown_timeout_ms"
    (config ~graceful_shutdown_timeout_ms:86_400_001L ());
  expect_rejected "max_outstanding_workflow_tasks"
    (config ~max_outstanding_workflow_tasks:0 ())

(** Reaching [Invalid_state] (no client connected) rather than a
    configuration or protocol error proves Rust decoded and validated the
    tuned document through the real C entry point. *)
let test_rust_accepts_tuned_document () =
  let runtime = unwrap (Bridge.runtime_create ()) in
  Fun.protect
    ~finally:(fun () -> ignore (Bridge.runtime_close runtime))
    (fun () ->
      match Bridge.worker_start runtime (unwrap (config ~tuning:tuned ())) with
      | Error { status = Invalid_state; _ } -> ()
      | Error { message; _ } ->
          failwith ("tuned worker document was rejected: " ^ message)
      | Ok () -> failwith "worker started without a client")

let () =
  test_tuned_document ();
  test_default_and_partial_documents ();
  test_invalid_tuning ();
  test_rust_accepts_tuned_document ();
  print_endline "worker tuning bridge tests passed"
