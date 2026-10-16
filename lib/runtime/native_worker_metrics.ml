module Bridge = Temporal_core_bridge.Native_bridge

(** [ids.(i)] is the bridge identifier of the series at index [i] of
    {!series}. Both arrays are immutable after [create], so recording reads
    them without synchronization. *)
type t =
  | Disabled
  | Enabled of { meter : Bridge.metric_meter; ids : int array }

let disabled = Disabled
let enabled = function Disabled -> false | Enabled _ -> true

type refusal = Unrunnable | Callback_raised | Invalid
type async_operation = Heartbeat | Completion

type shutdown_outcome =
  | Clean
  | Abandoned_work
  | Teardown_detached
  | Completion_lost
  | Release_failed
  | Release_unproven

type abandoned_work = Activity_callback | Workflow_activation | Native_call

(** One registered series: kind, metric name without Core's [temporal_]
    prefix, help text, and its one optional closed-enumeration label. *)
type series = {
  kind : Bridge.metric_kind;
  name : string;
  description : string;
  label : (string * string) option;
}

(** Builds one series description. *)
let series ?label kind name description = { kind; name; description; label }

(* Metric names without Core's [temporal_] prefix, and the help texts
   shared by the several series of one labelled name. *)
let running_name = "ocaml_activity_callbacks_running"
let duration_name = "ocaml_activity_callback_duration"
let admitted_name = "ocaml_activity_tasks_admitted"
let refused_name = "ocaml_activity_tasks_refused"
let not_admitted_name = "ocaml_async_activity_not_admitted"
let expired_name = "ocaml_async_activity_leases_expired"
let retry_name = "ocaml_activity_completion_retries"
let detached_name = "ocaml_activity_lanes_detached"
let shutdown_name = "ocaml_worker_shutdowns"
let abandoned_name = "ocaml_worker_shutdown_abandoned_work"

let refused_description =
  "Activity tasks the SDK refused and acknowledged with a failure."

let not_admitted_description =
  "Asynchronous activity operations the bridge refused to admit because its \
   client-call registry was full."

let shutdown_description = "Bounded worker shutdowns, by outcome."
let abandoned_description = "Work abandoned by bounded worker shutdowns, by kind."

(* Indices into [all_series]; every recording function names one. *)
let running = 0
let duration = 1
let admitted = 2
let refused_unrunnable = 3
let refused_callback_raised = 4
let refused_invalid = 5
let not_admitted_heartbeat = 6
let not_admitted_completion = 7
let expired = 8
let retry = 9
let detached = 10
let shutdown_clean = 11
let shutdown_abandoned_work = 12
let shutdown_teardown_detached = 13
let shutdown_completion_lost = 14
let shutdown_release_failed = 15
let shutdown_release_unproven = 16
let abandoned_activity_callback = 17
let abandoned_workflow_activation = 18
let abandoned_native_call = 19

(** Every series a worker registers, in index order. The array order is
    load-bearing: the constants above index it. *)
let all_series =
  let open Bridge in
  [|
    series Up_down_counter running_name
      "Activity callbacks currently running on this worker's activity lane.";
    series Duration_histogram duration_name
      "Wall-clock duration of one activity callback, from dispatch to return.";
    series Counter admitted_name
      "Activity start tasks dispatched to a registered callback.";
    series ~label:("reason", "unrunnable") Counter refused_name
      refused_description;
    series ~label:("reason", "callback_raised") Counter refused_name
      refused_description;
    series ~label:("reason", "invalid") Counter refused_name refused_description;
    series ~label:("operation", "heartbeat") Counter not_admitted_name
      not_admitted_description;
    series ~label:("operation", "completion") Counter not_admitted_name
      not_admitted_description;
    series Counter expired_name
      "Asynchronous activity leases closed because the server must already \
       have timed the attempt out.";
    series Counter retry_name
      "Retained activity completions whose submission returned the retryable \
       status and will be resubmitted unchanged.";
    series Counter detached_name
      "Worker runs that returned without joining an activity lane whose \
       callback outlived the grace period.";
    series ~label:("outcome", "clean") Counter shutdown_name shutdown_description;
    series ~label:("outcome", "abandoned_work") Counter shutdown_name
      shutdown_description;
    series ~label:("outcome", "teardown_detached") Counter shutdown_name
      shutdown_description;
    series ~label:("outcome", "completion_lost") Counter shutdown_name
      shutdown_description;
    series ~label:("outcome", "release_failed") Counter shutdown_name
      shutdown_description;
    series ~label:("outcome", "release_unproven") Counter shutdown_name
      shutdown_description;
    series ~label:("kind", "activity_callback") Counter abandoned_name
      abandoned_description;
    series ~label:("kind", "workflow_activation") Counter abandoned_name
      abandoned_description;
    series ~label:("kind", "native_call") Counter abandoned_name
      abandoned_description;
  |]

let metric_names =
  Array.to_list all_series
  |> List.map (fun { name; _ } -> "temporal_" ^ name)
  |> List.sort_uniq String.compare

let create meter ~namespace ~task_queue =
  let ids = Array.make (Array.length all_series) (-1) in
  let rec register index =
    if index = Array.length all_series then Ok ()
    else
      let { kind; name; description; label } = all_series.(index) in
      let labels =
        ("namespace", namespace) :: ("task_queue", task_queue)
        :: Option.to_list label
      in
      match Bridge.metric_register meter ~kind ~name ~description ~labels with
      | Error _ as error -> error
      | Ok id ->
          ids.(index) <- id;
          (* Recording zero creates the series, so a counter or gauge is
             exported (at 0) before its first event and a rate or absence
             alert has a baseline. A histogram is left alone because a zero
             would be a real observation. *)
          (match kind with
          | Counter | Up_down_counter -> ignore (Bridge.metric_record meter id 0)
          | Gauge | Histogram | Duration_histogram -> ());
          register (index + 1)
  in
  match register 0 with
  | Ok () -> Ok (Enabled { meter; ids })
  | Error _ as error -> error

(** Adds [value] to the series at [index]. A refused recording is dropped:
    metrics never change worker behavior. *)
let record metrics index value =
  match metrics with
  | Disabled -> ()
  | Enabled { meter; ids } -> ignore (Bridge.metric_record meter ids.(index) value)

let activity_callback_started metrics =
  record metrics admitted 1;
  record metrics running 1

let activity_callback_finished metrics ~elapsed_ns =
  match metrics with
  | Disabled -> ()
  | Enabled _ ->
      record metrics running (-1);
      (* Microseconds, clamped at zero should the clock source misbehave. *)
      record metrics duration (max 0 (Int64.to_int (Int64.div elapsed_ns 1_000L)))

let activity_task_refused metrics = function
  | Unrunnable -> record metrics refused_unrunnable 1
  | Callback_raised -> record metrics refused_callback_raised 1
  | Invalid -> record metrics refused_invalid 1

let async_activity_not_admitted metrics = function
  | Heartbeat -> record metrics not_admitted_heartbeat 1
  | Completion -> record metrics not_admitted_completion 1

let async_activity_lease_expired metrics = record metrics expired 1
let activity_completion_retry metrics = record metrics retry 1
let activity_lane_detached metrics = record metrics detached 1

let worker_shutdown metrics = function
  | Clean -> record metrics shutdown_clean 1
  | Abandoned_work -> record metrics shutdown_abandoned_work 1
  | Teardown_detached -> record metrics shutdown_teardown_detached 1
  | Completion_lost -> record metrics shutdown_completion_lost 1
  | Release_failed -> record metrics shutdown_release_failed 1
  | Release_unproven -> record metrics shutdown_release_unproven 1

let worker_shutdown_abandoned metrics = function
  | Activity_callback -> record metrics abandoned_activity_callback 1
  | Workflow_activation -> record metrics abandoned_workflow_activation 1
  | Native_call -> record metrics abandoned_native_call 1
