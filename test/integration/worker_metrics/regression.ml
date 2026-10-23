(** Live #500 regression: Prometheus worker metrics and log correlation.

    One shared [Runtime] exports Core's metrics on an ephemeral loopback
    port. A worker and a client attached to it run a workflow that calls one
    activity; the test then scrapes the endpoint and checks that:
    - the documented worker, task, and client metric families are present,
      with the [temporal_] prefix the official SDKs use;
    - every series uses only the bounded label set (no workflow ID, run ID,
      or other execution identifier ever appears in the scrape), which is the
      cardinality contract in docs/reference/observability.md;
    - the configured global tag is attached;
    and that the SDK's [workflow_activation_completed] log records for the
    execution carry its workflow ID, run ID, and replay flag. The SDK's own
    [temporal_ocaml_] series (#500) are checked too: the activity callback
    is counted as admitted and timed, and a second scrape after
    [Worker.shutdown], while the shared runtime still serves, shows one
    clean shutdown for the worker's task queue. After [Runtime.shutdown] the
    endpoint must be closed.

    Run against a disposable Temporal Server only:
    [dune exec test/integration/worker_metrics/regression.exe -- check
    http://127.0.0.1:7233]. [TEMPORAL_NAMESPACE] defaults to
    [temporal-sdk-test]. CI runs it through
    [make test-temporal-live-regressions]. *)
open Temporal

module Observability = Temporal_base.Observability

(** Converts the public result boundary into a short fixture failure. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

let namespace =
  Sys.getenv_opt "TEMPORAL_NAMESPACE"
  |> Option.value ~default:"temporal-sdk-test"

(** Metric families every healthy worker that ran one workflow with one
    activity must export. Names are Core's, prefixed with [temporal_]. *)
let required_metrics =
  [
    (* Client RPCs, including long polls. *)
    "temporal_request";
    "temporal_request_latency";
    "temporal_long_request";
    "temporal_long_request_latency";
    (* Worker capacity and polling. *)
    "temporal_num_pollers";
    "temporal_worker_task_slots_available";
    "temporal_worker_task_slots_used";
    "temporal_sticky_cache_size";
    "temporal_worker_start";
    (* Workflow tasks and outcomes. *)
    "temporal_workflow_task_queue_poll_succeed";
    "temporal_workflow_task_schedule_to_start_latency";
    "temporal_workflow_task_execution_latency";
    "temporal_workflow_task_replay_latency";
    "temporal_workflow_completed";
    "temporal_workflow_endtoend_latency";
    (* Activity tasks. *)
    "temporal_activity_task_received";
    "temporal_activity_schedule_to_start_latency";
    "temporal_activity_execution_latency";
    "temporal_activity_succeed_endtoend_latency";
    (* SDK-side series (#500); counters are exported from worker creation. *)
    "temporal_ocaml_activity_callbacks_running";
    "temporal_ocaml_activity_callback_duration";
    "temporal_ocaml_activity_tasks_admitted";
    "temporal_ocaml_activity_tasks_refused";
    "temporal_ocaml_async_activity_not_admitted";
    "temporal_ocaml_async_activity_leases_expired";
    "temporal_ocaml_activity_completion_retries";
    "temporal_ocaml_activity_lanes_detached";
    "temporal_ocaml_worker_shutdowns";
    "temporal_ocaml_worker_shutdown_abandoned_work";
  ]

(** Label names a scraped series may carry: Core's bounded labels plus the
    configured global tag. Anything else would be a cardinality regression. *)
let allowed_labels =
  [
    "service_name";
    "namespace";
    "task_queue";
    "workflow_type";
    "activity_type";
    "poller_type";
    "worker_type";
    "eager";
    "failure_reason";
    "operation";
    "status_code";
    "le";
    "deployment";
    (* Closed enumerations of the SDK-side series (#500). *)
    "reason";
    "outcome";
    "kind";
  ]

(** Returns whether [needle] occurs in [haystack]. *)
let contains ~haystack ~needle =
  let haystack_length = String.length haystack in
  let needle_length = String.length needle in
  let rec search offset =
    offset + needle_length <= haystack_length
    && (String.sub haystack offset needle_length = needle || search (offset + 1))
  in
  search 0

(** Splits ["a.b.c.d:port"] into a socket address. *)
let socket_of address =
  match String.rindex_opt address ':' with
  | None -> failwith ("not a socket address: " ^ address)
  | Some colon ->
      Unix.ADDR_INET
        ( Unix.inet_addr_of_string (String.sub address 0 colon),
          int_of_string
            (String.sub address (colon + 1) (String.length address - colon - 1))
        )

(** Fetches [/metrics] over HTTP/1.1 and returns the response body. *)
let scrape address =
  let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Fun.protect
    ~finally:(fun () -> Unix.close socket)
    (fun () ->
      Unix.setsockopt_float socket Unix.SO_RCVTIMEO 10.;
      Unix.connect socket (socket_of address);
      let request =
        Printf.sprintf
          "GET /metrics HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n"
          address
      in
      ignore (Unix.write_substring socket request 0 (String.length request));
      let buffer = Buffer.create 65_536 in
      let chunk = Bytes.create 65_536 in
      let rec read () =
        match Unix.read socket chunk 0 (Bytes.length chunk) with
        | 0 -> ()
        | count ->
            Buffer.add_subbytes buffer chunk 0 count;
            read ()
      in
      read ();
      let response = Buffer.contents buffer in
      if not (String.starts_with ~prefix:"HTTP/1.1 200" response) then
        failwith "metrics endpoint did not answer 200";
      response)

(** The family name of one exposition sample line ([name{...} value] or
    [name value]), with histogram suffixes removed. *)
let family_of_sample line =
  let stop =
    match (String.index_opt line '{', String.index_opt line ' ') with
    | Some brace, Some space -> min brace space
    | Some brace, None -> brace
    | None, Some space -> space
    | None, None -> String.length line
  in
  let name = String.sub line 0 stop in
  List.fold_left
    (fun name suffix ->
      if String.ends_with ~suffix name then
        String.sub name 0 (String.length name - String.length suffix)
      else name)
    name [ "_bucket"; "_sum"; "_count" ]

(** Label names of one sample line, in order of appearance. Values never
    contain [=] followed by a quote in this fixture, so a light parser is
    enough: each label is [name="value"] separated by commas. *)
let labels_of_sample line =
  match (String.index_opt line '{', String.rindex_opt line '}') with
  | Some open_, Some close when close > open_ ->
      let body = String.sub line (open_ + 1) (close - open_ - 1) in
      let rec names acc position =
        match String.index_from_opt body position '=' with
        | None -> List.rev acc
        | Some equals ->
            let name = String.trim (String.sub body position (equals - position)) in
            (* Skip the quoted value, honouring backslash escapes. *)
            let rec value_end index escaped =
              if index >= String.length body then index
              else
                match body.[index] with
                | '\\' when not escaped -> value_end (index + 1) true
                | '"' when not escaped -> index
                | _ -> value_end (index + 1) false
            in
            let closing = value_end (equals + 2) false in
            let next = min (String.length body) (closing + 2) in
            names (name :: acc) next
      in
      names [] 0
  | _ -> []

(** The value of the one sample of [family] whose line contains every
    fragment of [fragments] (for example [{|outcome="clean"|}]); fails when
    there is none. *)
let sample_value lines family fragments =
  match
    List.find_opt
      (fun line ->
        String.starts_with ~prefix:(family ^ "{") line
        && List.for_all (fun needle -> contains ~haystack:line ~needle) fragments)
      lines
  with
  | None -> failwith ("missing sample " ^ family ^ " " ^ String.concat " " fragments)
  | Some line ->
      let space = String.rindex line ' ' in
      float_of_string (String.sub line (space + 1) (String.length line - space - 1))

(** Sample lines of an exposition body (comments and blank lines dropped). *)
let samples body =
  let body =
    (* Drop the HTTP header block. *)
    match String.split_on_char '\n' body with
    | lines ->
        let rec after_headers = function
          | [] -> []
          | line :: rest ->
              if String.trim line = "" then rest else after_headers rest
        in
        after_headers lines
  in
  List.filter
    (fun line -> line <> "" && line.[0] <> '#' && line.[0] <> '\r')
    (List.map String.trim body)

(** Lifecycle records captured for correlation assertions. Workers report
    from several Domains, so the store is guarded by a mutex. *)
let captured = ref []
let captured_mutex = Mutex.create ()

(** Keeps only the workflow lease records; other sources stay silent. *)
let capture_reporter =
  let report _source _level ~over continuation messagef =
    messagef (fun ?header:_ ?(tags = Logs.Tag.empty) format ->
        Format.ikfprintf
          (fun _ ->
            (match Logs.Tag.find Observability.Tag.operation tags with
            | Some "workflow_activation_completed" ->
                Mutex.lock captured_mutex;
                captured := tags :: !captured;
                Mutex.unlock captured_mutex
            | _ -> ());
            over ();
            continuation ())
          Format.str_formatter format)
  in
  { Logs.report }

let greet =
  Activity.define ~name:"worker-metrics.greet" ~input:Codec.string
    ~output:Codec.string (fun input -> Ok ("hello " ^ input))

let workflow =
  Workflow.define ~name:"worker-metrics.workflow" ~input:Codec.string
    ~output:Codec.string (fun input -> Activity.execute greet input)

(** Runs the scenario; see the module comment. *)
let check address =
  ignore (Unix.alarm 120);
  Logs.set_reporter capture_reporter;
  Logs.Src.set_level Observability.Source.lifecycle (Some Logs.Debug);
  let suffix =
    Printf.sprintf "%d-%d" (Unix.getpid ())
      (Random.State.bits (Random.State.make_self_init ()))
  in
  let queue = "worker-metrics-" ^ suffix in
  let workflow_id = "worker-metrics-wf-" ^ suffix in
  let metrics =
    get
      (Runtime.Metrics.prometheus ~bind_address:"127.0.0.1:0"
         ~global_tags:[ ("deployment", "worker-metrics-regression") ] ())
  in
  let runtime = get (Runtime.create ~metrics ()) in
  let endpoint = Option.get (Runtime.metrics_address runtime) in
  let worker =
    get
      (Worker.create ~runtime ~target_url:address ~namespace ~task_queue:queue
         ~identity:"worker-metrics" ~workflows:[ Worker.workflow workflow ]
         ~activities:[ Worker.activity greet ] ())
  in
  let running = Domain.spawn (fun () -> Worker.run worker) in
  let client = get (Client.create ~runtime ~target_url:address ~namespace ()) in
  let body =
    try
      let handle =
        get
          (Client.start client ~workflow ~task_queue:queue ~id:workflow_id
             ~input:"metrics" ())
      in
      (match get (Client.wait handle) with
      | Client.Completed { output = "hello metrics"; _ } -> ()
      | _ -> failwith "workflow did not complete with the expected output");
      let run_id = Option.get (Client.run_id handle) in
      Ok (run_id, scrape endpoint)
    with exception_ -> Error exception_
  in
  ignore (Client.shutdown client);
  ignore (get (Worker.shutdown worker));
  ignore (Domain.join running);
  (* The shared runtime outlives the worker, so its shutdown outcome is
     still scrapeable here (#500). *)
  let after_shutdown =
    match body with
    | Ok _ -> ( try Some (scrape endpoint) with _ -> None)
    | Error _ -> None
  in
  get (Runtime.shutdown runtime);
  let run_id, response =
    match body with Ok value -> value | Error exception_ -> raise exception_
  in
  let lines = samples response in
  let families = List.sort_uniq compare (List.map family_of_sample lines) in
  let missing =
    List.filter (fun name -> not (List.mem name families)) required_metrics
  in
  if missing <> [] then
    failwith ("missing metric families: " ^ String.concat ", " missing);
  List.iter
    (fun line ->
      List.iter
        (fun label ->
          if not (List.mem label allowed_labels) then
            failwith ("unexpected metric label " ^ label ^ " in " ^ line))
        (labels_of_sample line))
    lines;
  List.iter
    (fun identifier ->
      if contains ~haystack:response ~needle:identifier then
        failwith "an execution identifier appeared in the metrics scrape")
    [ workflow_id; run_id ];
  if not (contains ~haystack:response ~needle:"deployment=\"worker-metrics-regression\"")
  then failwith "global tag missing from the scrape";
  if not (contains ~haystack:response ~needle:("task_queue=\"" ^ queue ^ "\"")) then
    failwith "task_queue label missing from the scrape";
  (* SDK-side series (#500): the one activity callback was admitted and
     timed, nothing is still running, and the worker shut down cleanly. *)
  let queue_label = "task_queue=\"" ^ queue ^ "\"" in
  if sample_value lines "temporal_ocaml_activity_tasks_admitted" [ queue_label ] <> 1.
  then failwith "the activity callback was not counted as admitted";
  if sample_value lines "temporal_ocaml_activity_callback_duration_count"
       [ queue_label ] <> 1.
  then failwith "the activity callback duration was not observed";
  if sample_value lines "temporal_ocaml_activity_callbacks_running" [ queue_label ]
     <> 0.
  then failwith "an activity callback is still counted as running";
  (match after_shutdown with
  | None -> failwith "no scrape after Worker.shutdown"
  | Some response ->
      let after = samples response in
      if sample_value after "temporal_ocaml_worker_shutdowns"
           [ queue_label; {|outcome="clean"|} ] <> 1.
      then failwith "the clean worker shutdown was not counted");
  (* Correlation: the lease records of this run name it exactly. *)
  Mutex.lock captured_mutex;
  let records = !captured in
  Mutex.unlock captured_mutex;
  let for_run =
    List.filter
      (fun tags -> Logs.Tag.find Observability.Tag.run_id tags = Some run_id)
      records
  in
  if for_run = [] then failwith "no workflow_activation_completed record for the run";
  List.iter
    (fun tags ->
      if Logs.Tag.find Observability.Tag.workflow_id tags <> Some workflow_id then
        failwith "workflow_activation_completed lacks the workflow ID";
      if Logs.Tag.find Observability.Tag.replaying tags <> Some false then
        failwith "a first execution was reported as replaying")
    for_run;
  (* The endpoint closes with the runtime. *)
  (match
     let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
     Fun.protect
       ~finally:(fun () -> Unix.close socket)
       (fun () -> Unix.connect socket (socket_of endpoint))
   with
  | () -> failwith "metrics endpoint still open after Runtime.shutdown"
  | exception Unix.Unix_error (Unix.ECONNREFUSED, _, _) -> ());
  Printf.printf
    "worker metrics live regression: %d families, %d samples, %d correlated \
     records\nfamilies: %s\n%!"
    (List.length families) (List.length lines) (List.length for_run)
    (String.concat " " families)

let () =
  match Sys.argv with
  | [| _; "check"; address |] -> check address
  | _ ->
      prerr_endline "usage: regression.exe check TEMPORAL_ADDRESS";
      exit 2
