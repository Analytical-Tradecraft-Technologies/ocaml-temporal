(** SDK-side worker metrics recorded into a real runtime scrape (#500).

    No Temporal Server is needed: the runtime's Prometheus endpoint is real,
    and the activity adapter runs against a small fake supervisor. The test
    drives the adapter paths that record metrics (callback admission,
    occupancy and duration; refusals by reason; asynchronous operations the
    bridge did not admit because its client-call registry was full, #1104;
    lease expiry, #1043), records the worker-level outcomes the public
    worker reports (completion retries, detached lanes, shutdown outcomes),
    and checks each value in the scrape. It also checks the cardinality
    contract: every SDK series carries only the bounded labels documented in
    [docs/reference/observability.md], and no execution identifier from the
    tasks ever reaches the scrape. *)

module Protocol = Temporal_protocol.Activity_protocol
module Adapter = Temporal_runtime.Native_activity_execution
module Metrics = Temporal_runtime.Native_worker_metrics
module Bridge = Temporal_core_bridge.Native_bridge
module Worker_policy = Temporal_runtime.Native_worker_policy
module Base_async = Temporal_base.Async_activity
module Shutdown = Temporal_runtime.Native_worker_shutdown

(** A bounded-shutdown report with the given lane facts. *)
let shutdown_report ?(teardown = Shutdown.Completed) ~lanes_stopped ~activity
    ~workflow () : Shutdown.report =
  {
    elapsed_s = 0.;
    lanes_stopped;
    abandoned_activity_callbacks = activity;
    abandoned_workflow_activations = workflow;
    teardown;
  }

(** Shutdown classification (review of #1114): both abandoned lanes are
    reported, [Native_call] only when no count is positive, and an error
    outcome (including [Release_unproven]) keeps its abandonment facts. *)
let test_shutdown_classification () =
  let check label ?failure report expected =
    if Metrics.classify_shutdown ?failure report <> expected then
      failwith ("shutdown classification: " ^ label)
  in
  check "clean"
    (shutdown_report ~lanes_stopped:true ~activity:0 ~workflow:0 ())
    (Metrics.Clean, []);
  check "both lanes abandoned"
    (shutdown_report ~lanes_stopped:false ~activity:1 ~workflow:2 ())
    (Metrics.Abandoned_work, [ Metrics.Activity_callback; Metrics.Workflow_activation ]);
  check "workflow lane only"
    (shutdown_report ~lanes_stopped:false ~activity:0 ~workflow:1 ())
    (Metrics.Abandoned_work, [ Metrics.Workflow_activation ]);
  check "lane blocked in a native call"
    (shutdown_report ~lanes_stopped:false ~activity:0 ~workflow:0 ())
    (Metrics.Abandoned_work, [ Metrics.Native_call ]);
  check "detached teardown"
    (shutdown_report ~teardown:Shutdown.Detached ~lanes_stopped:true ~activity:0
       ~workflow:0 ())
    (Metrics.Teardown_detached, []);
  check "release raised after abandoning both lanes"
    ~failure:Metrics.Release_unproven
    (shutdown_report ~teardown:Shutdown.Detached ~lanes_stopped:false
       ~activity:1 ~workflow:1 ())
    (Metrics.Release_unproven, [ Metrics.Activity_callback; Metrics.Workflow_activation ])

(** Unwraps a public result in fixture setup. *)
let get = function
  | Ok value -> value
  | Error error -> failwith (Temporal.Error.message error)

(** Whether [needle] occurs in [haystack]. *)
let contains haystack needle =
  let n = String.length needle and h = String.length haystack in
  let rec at i = i + n <= h && (String.sub haystack i n = needle || at (i + 1)) in
  at 0

(** Fake native error: [status] drives the production async classifier. *)
type fake_error = { status : Bridge.status }

(** Fake native state: a task queue, and whether namespace-bound async calls
    are refused as by a full client-call registry. *)
type fake = { tasks : Protocol.task Queue.t; registry_full : bool ref }

(** Minimal supervisor: every worker completion is accepted, and every async
    call is refused with [Resource_exhausted] while [registry_full] is set
    (the bridge's #1104 back-pressure) and accepted otherwise. *)
module Fake_supervisor = struct
  type t = fake
  type error = fake_error

  (** Leases the next queued task. *)
  let try_poll_activity fake =
    Ok (if Queue.is_empty fake.tasks then None else Some (Queue.take fake.tasks))

  (** Accepts every worker completion. *)
  let complete_activity _fake _encoded = Ok ()

  (** Refuses while the fake registry is full. *)
  let refuse_if_full fake ok =
    if !(fake.registry_full) then Error { status = Bridge.Resource_exhausted }
    else Ok ok

  (** Async completions are subject to the fake registry. *)
  let complete_async_activity fake _encoded = refuse_if_full fake ()

  (** Accepts every synchronous heartbeat. *)
  let record_activity_heartbeat _fake _encoded = Ok ()

  (** Async heartbeats are subject to the fake registry. *)
  let record_async_activity_heartbeat fake _encoded =
    refuse_if_full fake Protocol.Heartbeat_recorded

  (** Stable diagnostics; no task token is ever included. *)
  let error_code _error = "resource_exhausted"

  let error_message _error = "client-call registry full"

  (** No worker completion fails here, so nothing is retryable. *)
  let error_is_retryable _error = false

  (** The production classifier, so [Resource_exhausted] is [Not_admitted]. *)
  let async_operation_error_disposition { status } =
    Worker_policy.async_operation_disposition status

  let exception_is_retryable _exception = false
end

module Worker = Adapter.Make (Fake_supervisor)

(** A payload codec for [unit] that needs no public codec. *)
let unit_codec =
  Temporal_base.Codec.of_payload
    ~encode:(fun () ->
      Ok
        {
          Temporal_base.Payload.metadata = [ ("encoding", "binary/null") ];
          data = Bytes.empty;
        })
    ~decode:(fun _ -> Ok ())

(** Identifiers carried by every task; the scrape must never contain them. *)
let workflow_id = "metrics-workflow-id-7f3a"

let run_id = "metrics-run-id-91c2"
let activity_id = "metrics-activity-id-4d8e"

(** Builds one remote start task for [activity_type]. A positive
    [start_to_close] gives an async lease an expiry deadline. *)
let start_task ?start_to_close ~token activity_type : Protocol.task =
  let start : Protocol.activity_start =
    {
      is_local = false;
      workflow_namespace = "default";
      workflow_type = "metrics_workflow";
      workflow_execution = { Protocol.workflow_id; run_id };
      activity_id;
      activity_type;
      header_fields = [];
      input = [];
      heartbeat_details = [];
      scheduled_time = None;
      current_attempt_scheduled_time = None;
      started_time = None;
      attempt = 1L;
      schedule_to_close_timeout = None;
      start_to_close_timeout =
        Option.map
          (fun seconds -> { Protocol.seconds = Int64.of_int seconds; nanoseconds = 0 })
          start_to_close;
      heartbeat_timeout = None;
      retry_policy = None;
      priority = None;
      standalone_run_id = "";
    }
  in
  { Protocol.task_token = Bytes.of_string token; variant = Start start }

(** Splits ["host:port"] (IPv4 loopback in this test). *)
let socket_of address =
  let colon = String.rindex address ':' in
  Unix.ADDR_INET
    ( Unix.inet_addr_of_string (String.sub address 0 colon),
      int_of_string
        (String.sub address (colon + 1) (String.length address - colon - 1)) )

(** Fetches [/metrics] and returns the body. The response is complete once
    its [Content-Length] body has arrived (the server's close timing is not
    part of the contract, #1058); every wait is bounded by [Unix.select]. *)
let scrape address =
  let deadline = Unix.gettimeofday () +. 30. in
  let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Fun.protect
    ~finally:(fun () -> Unix.close socket)
    (fun () ->
      Unix.connect socket (socket_of address);
      let request =
        Printf.sprintf "GET /metrics HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n"
          address
      in
      ignore (Unix.write_substring socket request 0 (String.length request));
      let buffer = Buffer.create 8192 and chunk = Bytes.create 8192 in
      let body () =
        let text = Buffer.contents buffer in
        let rec marker i =
          if i + 4 > String.length text then None
          else if String.sub text i 4 = "\r\n\r\n" then Some i
          else marker (i + 1)
        in
        match marker 0 with
        | None -> None
        | Some header_end ->
            let headers = String.lowercase_ascii (String.sub text 0 header_end) in
            let length =
              String.split_on_char '\n' headers
              |> List.find_map (fun line ->
                     match String.index_opt line ':' with
                     | Some colon when String.sub line 0 colon = "content-length" ->
                         String.sub line (colon + 1) (String.length line - colon - 1)
                         |> String.trim |> int_of_string_opt
                     | _ -> None)
            in
            let start = header_end + 4 in
            Option.bind length (fun length ->
                if String.length text - start >= length then
                  Some (String.sub text start length)
                else None)
      in
      let rec read () =
        match body () with
        | Some body -> body
        | None ->
            let remaining = deadline -. Unix.gettimeofday () in
            if remaining <= 0. then failwith "metrics scrape timed out";
            (match Unix.select [ socket ] [] [] remaining with
            | [], _, _ -> ()
            | _ -> (
                match Unix.read socket chunk 0 (Bytes.length chunk) with
                | 0 -> failwith "metrics endpoint closed before the body"
                | count -> Buffer.add_subbytes buffer chunk 0 count));
            read ()
      in
      read ())

(** The value of the sample of [family] whose labels include every
    [label="value"] fragment of [labels]; fails when absent. *)
let sample text family labels =
  let matches line =
    String.starts_with ~prefix:(family ^ "{") line
    && List.for_all (fun (name, value) -> contains line (Printf.sprintf "%s=%S" name value)) labels
  in
  match List.find_opt matches (String.split_on_char '\n' text) with
  | None ->
      failwith
        (Printf.sprintf "no %s sample with the expected labels in:\n%s" family text)
  | Some line ->
      let value = String.rindex line ' ' in
      float_of_string (String.sub line (value + 1) (String.length line - value - 1))

(** Requires one sample to have [expected] as its value. *)
let expect text family ?(labels = []) expected =
  let actual = sample text family labels in
  if actual <> expected then
    failwith
      (Printf.sprintf "%s %s: expected %g, got %g" family
         (String.concat "," (List.map (fun (n, v) -> n ^ "=" ^ v) labels))
         expected actual)

(** Label names an SDK series may carry: the worker's two fixed labels,
    the closed enumerations, Core's [service_name], the histogram [le], and
    this test's global tag. *)
let allowed_labels =
  [ "namespace"; "task_queue"; "reason"; "operation"; "outcome"; "kind";
    "service_name"; "le"; "deployment" ]

(** Checks the label names of every [temporal_ocaml_] sample. *)
let check_cardinality text =
  String.split_on_char '\n' text
  |> List.iter (fun line ->
         if String.starts_with ~prefix:"temporal_ocaml_" line then
           match String.index_opt line '{' with
           | None -> ()
           | Some open_ ->
               let close = String.index line '}' in
               String.sub line (open_ + 1) (close - open_ - 1)
               |> String.split_on_char ','
               |> List.iter (fun pair ->
                      let name = List.hd (String.split_on_char '=' pair) in
                      if not (List.mem name allowed_labels) then
                        failwith ("unexpected SDK metric label: " ^ line)));
  List.iter
    (fun identifier ->
      if contains text identifier then
        failwith ("an execution identifier reached the scrape: " ^ identifier))
    [ workflow_id; run_id; activity_id; "never_registered" ]

let () =
  test_shutdown_classification ();
  let metrics_config =
    get
      (Temporal.Runtime.Metrics.prometheus ~bind_address:"127.0.0.1:0"
         ~global_tags:[ ("deployment", "metrics-test") ] ())
  in
  let runtime = get (Temporal.Runtime.create ~io_threads:1 ~metrics:metrics_config ()) in
  let address = Option.get (Temporal.Runtime.metrics_address runtime) in
  let meter =
    match Temporal_sdk_kernel.Shared_runtime.metric_meter runtime with
    | Some meter -> meter
    | None -> failwith "an exporting runtime has no SDK metric meter"
  in
  let namespace = "metrics-ns" and task_queue = "metrics-queue" in
  let labels = [ ("namespace", namespace); ("task_queue", task_queue) ] in
  let metrics =
    match Metrics.create meter ~namespace ~task_queue with
    | Ok metrics -> metrics
    | Error { Bridge.message; _ } -> failwith message
  in
  assert (Metrics.enabled metrics);
  assert (not (Metrics.enabled Metrics.disabled));
  (* Registration exports every counter and the occupancy gauge at zero. *)
  let initial = scrape address in
  expect initial "temporal_ocaml_activity_tasks_admitted" ~labels 0.;
  expect initial "temporal_ocaml_activity_callbacks_running" ~labels 0.;
  expect initial "temporal_ocaml_worker_shutdowns"
    ~labels:(("outcome", "clean") :: labels) 0.;
  (* A second worker on the same queue shares the series. *)
  (match Metrics.create meter ~namespace ~task_queue with
  | Ok _ -> ()
  | Error { Bridge.message; _ } -> failwith message);

  let clock = ref 1_000. in
  let fake = { tasks = Queue.create (); registry_full = ref false } in
  let running_during_callback = ref nan in
  let ok =
    Temporal_base.Definition.make ~name:"ok" ~input:unit_codec ~output:unit_codec
      ~implementation:
        (Some
           (fun _context () ->
             (* The occupancy gauge is raised while the callback runs. *)
             running_during_callback :=
               sample (scrape address) "temporal_ocaml_activity_callbacks_running"
                 labels;
             Ok ()))
  in
  let boom =
    Temporal_base.Definition.make ~name:"boom" ~input:unit_codec ~output:unit_codec
      ~implementation:(Some (fun _context () -> failwith "callback defect"))
  in
  let retained = ref None in
  let leak =
    Temporal_base.Definition.make ~name:"leak" ~input:unit_codec ~output:unit_codec
      ~implementation:
        (Some
           (fun context () ->
             let handle = Base_async.handle context in
             retained := Some handle;
             Base_async.Will_complete_async handle))
  in
  let worker =
    match
      Worker.create_with_clock
        ~now:(fun () -> !clock)
        ~supervisor:fake
        ~activities:
          [ Adapter.register ok; Adapter.register boom; Adapter.register_async leak ]
        ~worker_shutting_down:(fun () -> false)
    with
    | Ok worker -> worker
    | Error (error : Adapter.error_view) -> failwith error.message
  in
  Worker.attach_metrics worker metrics;
  List.iter (fun task -> Queue.add task fake.tasks)
    [
      start_task ~token:"ok-token" "ok";
      start_task ~token:"boom-token" "boom";
      start_task ~token:"unregistered-token" "never_registered";
      start_task ~start_to_close:30 ~token:"leak-token" "leak";
    ];
  for _ = 1 to 4 do
    match Worker.poll worker with
    | Ok (Adapter.Completed _ | Adapter.Rejected { lease_retired = true; _ }) -> ()
    | Ok _ -> failwith "unexpected adapter outcome"
    | Error (error : Adapter.error_view) -> failwith error.message
  done;
  if !running_during_callback <> 1. then
    failwith
      (Printf.sprintf "occupancy during the callback was %g" !running_during_callback);
  (* A full client-call registry refuses one heartbeat and one completion
     before any RPC (#1104). *)
  let handle = Option.get !retained in
  fake.registry_full := true;
  ignore (Base_async.heartbeat handle []);
  ignore (Base_async.complete handle ());
  (* The attempt's start-to-close timeout plus slack has passed: draining
     expires the leaked lease (#1043). *)
  clock := 1_000. +. 30. +. 6.;
  (match Worker.drain worker with
  | Ok () -> ()
  | Error (error : Adapter.error_view) -> failwith ("drain: " ^ error.code));
  (* Worker-level outcomes, recorded by the public worker. *)
  Metrics.activity_completion_retry metrics;
  Metrics.activity_lane_detached metrics;
  Metrics.worker_shutdown metrics Metrics.Clean;
  Metrics.worker_shutdown metrics Metrics.Teardown_detached;
  Metrics.worker_shutdown_abandoned metrics Metrics.Activity_callback;
  (* Both lanes abandoned, then a release that raised: each kind and the
     [release_unproven] outcome are counted once. *)
  Metrics.record_shutdown metrics
    (shutdown_report ~lanes_stopped:false ~activity:1 ~workflow:1 ());
  Metrics.record_shutdown metrics ~failure:Metrics.Release_unproven
    (shutdown_report ~lanes_stopped:false ~activity:0 ~workflow:1 ());
  (* Recording on [disabled] is a no-op and never raises. *)
  Metrics.activity_callback_started Metrics.disabled;
  Metrics.worker_shutdown Metrics.disabled Metrics.Clean;

  let text = scrape address in
  (* [ok], [boom] and [leak] each ran a callback; the unregistered task did
     not reach one. *)
  expect text "temporal_ocaml_activity_tasks_admitted" ~labels 3.;
  expect text "temporal_ocaml_activity_callbacks_running" ~labels 0.;
  expect text "temporal_ocaml_activity_callback_duration_count" ~labels 3.;
  expect text "temporal_ocaml_activity_tasks_refused"
    ~labels:(("reason", "unrunnable") :: labels) 1.;
  expect text "temporal_ocaml_activity_tasks_refused"
    ~labels:(("reason", "callback_raised") :: labels) 1.;
  expect text "temporal_ocaml_activity_tasks_refused"
    ~labels:(("reason", "invalid") :: labels) 0.;
  expect text "temporal_ocaml_async_activity_not_admitted"
    ~labels:(("operation", "heartbeat") :: labels) 1.;
  expect text "temporal_ocaml_async_activity_not_admitted"
    ~labels:(("operation", "completion") :: labels) 1.;
  expect text "temporal_ocaml_async_activity_leases_expired" ~labels 1.;
  expect text "temporal_ocaml_activity_completion_retries" ~labels 1.;
  expect text "temporal_ocaml_activity_lanes_detached" ~labels 1.;
  expect text "temporal_ocaml_worker_shutdowns"
    ~labels:(("outcome", "clean") :: labels) 1.;
  expect text "temporal_ocaml_worker_shutdowns"
    ~labels:(("outcome", "teardown_detached") :: labels) 1.;
  expect text "temporal_ocaml_worker_shutdown_abandoned_work"
    ~labels:(("kind", "activity_callback") :: labels) 2.;
  expect text "temporal_ocaml_worker_shutdown_abandoned_work"
    ~labels:(("kind", "workflow_activation") :: labels) 2.;
  expect text "temporal_ocaml_worker_shutdown_abandoned_work"
    ~labels:(("kind", "native_call") :: labels) 0.;
  expect text "temporal_ocaml_worker_shutdowns"
    ~labels:(("outcome", "abandoned_work") :: labels) 1.;
  expect text "temporal_ocaml_worker_shutdowns"
    ~labels:(("outcome", "release_unproven") :: labels) 1.;
  (* Every documented family is present, with Core's labels and the global
     tag, and nothing unbounded. *)
  List.iter
    (fun name ->
      if not (contains text ("# TYPE " ^ name ^ " ")) then
        failwith ("missing SDK metric family " ^ name))
    Metrics.metric_names;
  expect text "temporal_ocaml_activity_tasks_admitted"
    ~labels:(("service_name", "temporal-core-sdk") :: ("deployment", "metrics-test") :: labels)
    3.;
  check_cardinality text;

  (* The meter outlives the runtime: recording after shutdown is safe. *)
  get (Temporal.Runtime.shutdown runtime);
  Metrics.activity_completion_retry metrics;
  print_endline "native worker metrics: ok"
