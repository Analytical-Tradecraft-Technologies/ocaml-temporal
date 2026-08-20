(** Definitions shared by the live two-process Temporal acceptance test.

    Keeping the workflow and activity values in one private test library makes
    the driver and worker compile against exactly the same names and codecs. The
    workflow bodies deliberately contain no process, filesystem, network, or
    clock access: those operations would make them non-replayable. The
    process-local counters below belong only to non-deterministic activity
    implementations and are never read by workflow code. *)

(** The task queue used only by this fixture. It is intentionally distinct from
    every production queue so an accidentally reused local namespace cannot
    dispatch test work to an application worker. *)
let task_queue = "ocaml-temporal-two-binary-smoke"

(** The mock activity uppercases its input. The implementation is deterministic
    so the driver can assert the exact result without depending on external
    services or wall-clock state. *)
let mock_transform =
  Temporal.Activity.define ~name:"smoke.mock_transform"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      Ok (String.uppercase_ascii input))

(** The typed signal used by the live interaction scenario. The definition
    carries the same string codec in the driver and worker processes, so the
    client-side request and worker-side handler cannot silently disagree about
    the payload representation. *)
let signal_value =
  Temporal.Signal.define ~name:"smoke.set_value" ~input:Temporal.Codec.string

(** The output-only query used by the live client/worker interaction scenario.
    The query shares its state source with [signal_value_handler], but it is
    registered independently so the acceptance test proves that a client query
    is delivered as a read-only activation rather than being emulated through a
    signal or an activity. *)
let signal_condition_status =
  Temporal.Query.define ~name:"smoke.signal_condition_status"
    ~output:Temporal.Codec.string

(** The typed-input query used by the live interaction scenario. Its input is
    deliberately independent from workflow state so the acceptance test can
    prove that the request payload was decoded and delivered to the handler,
    rather than merely receiving the output-only query's answer. *)
let signal_value_echo =
  Temporal.Query.define_with_input ~name:"smoke.signal_value_echo"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string

(** The immediate update used by the live interaction scenario. Unlike the
    signal, an update has a client-visible admission and completion lifecycle;
    the handler still writes the same workflow-local state so the parked
    condition can prove that the update changed the running execution. *)
let signal_value_update =
  Temporal.Update.define ~name:"smoke.set_value_update"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string

(** The signal handler and workflow body share this key, while every Temporal
    execution receives an independent value slot. A new run therefore starts
    with [None] even when the same worker process has already handled another
    signal, which keeps this acceptance assertion tied to the current run. *)
let signal_value_state = Temporal.Workflow_context.Local.create ()

(** The marker path used to prove that the signal workflow's first worker-side
    task has been accepted before the driver sends its signal. The file is only
    test coordination state; it is never read by workflow code. *)
let signal_condition_ready_file_env = "SMOKE_SIGNAL_CONDITION_READY_FILE"

(** Validates the signal readiness marker path before an activity or driver uses
    it. Absolute paths and NUL rejection keep the shared bind-mount write
    bounded and prevent a malformed environment value from escaping the test
    fixture's intended file. *)
let validate_signal_condition_ready_file path =
  if String.equal path "" then
    Error
      (Temporal.Error.defect
         ~message:(signal_condition_ready_file_env ^ " must not be empty"))
  else if String.contains path '\000' then
    Error
      (Temporal.Error.defect
         ~message:(signal_condition_ready_file_env ^ " must not contain NUL"))
  else if Filename.is_relative path then
    Error
      (Temporal.Error.defect
         ~message:(signal_condition_ready_file_env ^ " must be an absolute path"))
  else Ok path

(** Reads and validates the signal readiness marker path from the worker and
    driver environment. *)
let signal_condition_ready_file () =
  match Sys.getenv_opt signal_condition_ready_file_env with
  | None ->
      Error
        (Temporal.Error.defect
           ~message:(signal_condition_ready_file_env ^ " must be set"))
  | Some path -> validate_signal_condition_ready_file path

(** Removes a stale signal readiness marker before a fresh live run. *)
let clear_signal_condition_ready_file path =
  try if Sys.file_exists path then Sys.remove path with _ -> ()

(** Handles one typed signal by retaining its value in the current execution's
    local state for the suspended workflow condition. The callback performs no
    I/O and does not call the client, so the signal activation remains
    deterministic. *)
let signal_value_handler =
  Temporal.Signal.Handler.make signal_value (fun value ->
      Temporal.Workflow_context.Local.set signal_value_state value)

(** Reads the parked signal workflow's current state without changing it. The
    initial [None] result is deliberately a stable marker rather than an
    optional value, so the external driver can distinguish a real query reply
    from a missing or undecodable response. [Local.get] is safe here because
    the native query dispatcher installs the current execution context while
    it invokes the handler. *)
let signal_condition_status_handler =
  Temporal.Query.Handler.make signal_condition_status (fun () ->
      match Temporal.Workflow_context.Local.get signal_value_state with
      | Ok None -> Ok "SMOKE:QUERY:PENDING"
      | Ok (Some value) ->
          Ok ("SMOKE:QUERY:SIGNAL:" ^ String.uppercase_ascii value)
      | Error error -> Error error)

(** Answers a typed query without mutating the workflow or suspending. The
    current signal state is included only as a read-only marker so the driver
    can distinguish a live handler result from a hard-coded response. *)
let signal_value_echo_handler =
  Temporal.Query.Handler.make_with_input signal_value_echo (fun input ->
      match Temporal.Workflow_context.Local.get signal_value_state with
      | Ok None -> Ok ("SMOKE:TYPED_QUERY:" ^ String.uppercase_ascii input ^ ":PENDING")
      | Ok (Some value) ->
          Ok
            ("SMOKE:TYPED_QUERY:" ^ String.uppercase_ascii input ^ ":SIGNAL:"
           ^ String.uppercase_ascii value)
      | Error error -> Error error)

(** Applies one live update to the parked workflow and returns a distinct
    result payload. The callback is deterministic and performs no external
    I/O; [Local.set] also wakes the condition scheduler for this execution. *)
let signal_value_update_handler =
  Temporal.Update.Handler.make signal_value_update (fun value ->
      match Temporal.Workflow_context.Local.set signal_value_state value with
      | Ok () -> Ok ("SMOKE:UPDATE:ACCEPTED:" ^ String.uppercase_ascii value)
      | Error error -> Error error)

(** Counts attempts for the intentionally transient activity used by the live
    retry scenario. This state belongs to the activity worker process rather
    than workflow state: activities are allowed to perform non-deterministic
    work, and a fresh worker process is started for every isolated Compose run.
    The atomic counter also keeps the fixture correct if the worker later
    dispatches activity tasks from more than one Domain. *)
let retry_once_attempts = Atomic.make 0

(** Fails exactly the first activity attempt and reports the attempt number on
    the succeeding call. The retryable [Activity] error leaves Temporal free to
    apply the workflow's explicit retry policy; the attempt suffix gives the
    driver an observable proof that the final result came from attempt 2. *)
let retry_once_activity =
  Temporal.Activity.define ~name:"smoke.retry_once" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string (fun input ->
      let attempt = Atomic.fetch_and_add retry_once_attempts 1 + 1 in
      if attempt = 1 then
        Error
          (Temporal.Error.make ~category:`Activity
             ~message:"intentional transient failure for retry acceptance" ())
      else
        Ok
          (Printf.sprintf "%s:ATTEMPT:%d"
             (String.uppercase_ascii input)
             attempt))

(** The environment variable used by the cancellation handshake activity. The
    marker lives in the repository bind mount so the independent driver can
    observe completion without adding another Temporal workflow signal. *)
let cancellation_ready_file_env = "SMOKE_CANCELLATION_READY_FILE"

(** Checks the marker path before any activity attempts to write it. A live
    acceptance run must provide an absolute, non-empty path without NUL bytes;
    rejecting anything else keeps an accidental host-path or malformed
    environment setting from turning into an uncontrolled filesystem write. *)
let validate_cancellation_ready_file path =
  if String.equal path "" then
    Error
      (Temporal.Error.defect
         ~message:(cancellation_ready_file_env ^ " must not be empty"))
  else if String.contains path '\000' then
    Error
      (Temporal.Error.defect
         ~message:(cancellation_ready_file_env ^ " must not contain NUL"))
  else if Filename.is_relative path then
    Error
      (Temporal.Error.defect
         ~message:(cancellation_ready_file_env ^ " must be an absolute path"))
  else Ok path

(** Reads and validates the shared marker path from the process environment.
    This is called by both the worker startup path and the activity itself so
    configuration is checked before the worker becomes ready and again at the
    side-effect boundary. *)
let cancellation_ready_file () =
  match Sys.getenv_opt cancellation_ready_file_env with
  | None ->
      Error
        (Temporal.Error.defect
           ~message:(cancellation_ready_file_env ^ " must be set"))
  | Some path -> validate_cancellation_ready_file path

(** Removes a prior marker as best-effort test cleanup. The marker is not
    workflow state; deleting it between runs prevents a driver from mistaking a
    previous activity completion for the current execution's handshake. *)
let clear_cancellation_ready_file path =
  try if Sys.file_exists path then Sys.remove path with _ -> ()

(** Publishes the per-run token with a unique temporary file followed by
    [Sys.rename]. The temporary file is created in the marker's directory, so
    the rename is atomic on the shared Linux bind mount and the driver observes
    either the complete marker or no marker, never a partially written file. A
    unique name matters because one worker can execute more than one test
    activity at a time; a PID-only name would let concurrent invocations
    overwrite one another's staging file. Any temporary file is removed on
    failure before a typed activity error is returned. *)
let publish_marker_token path token =
  let temporary = ref None in
  try
    let generated =
      Filename.temp_file ~temp_dir:(Filename.dirname path)
        (Filename.basename path ^ ".tmp.")
        ""
    in
    temporary := Some generated;
    let channel = open_out_bin generated in
    Fun.protect
      ~finally:(fun () -> close_out_noerr channel)
      (fun () ->
        output_string channel token;
        output_char channel '\n';
        flush channel);
    Sys.rename generated path;
    temporary := None;
    Ok ()
  with exception_ ->
    Option.iter
      (fun generated -> try Sys.remove generated with _ -> ())
      !temporary;
    Error
      (Temporal.Error.make ~category:`Activity
         ~message:
           (Printf.sprintf "cannot publish readiness marker: %s"
              (Printexc.to_string exception_))
         ())

(** Publishes the cancellation-specific marker using the shared atomic marker
    implementation. Keeping this alias preserves the activity's descriptive name
    at its call site while both live handshakes get identical file ownership and
    cleanup behavior. *)
let publish_cancellation_ready = publish_marker_token

(** A test-only activity that publishes the cancellation handshake token. Its
    filesystem side effect is intentionally isolated to the activity process;
    the workflow itself remains deterministic and only schedules this activity.
    The cache-eviction workflow uses a replay-safe condition instead of a second
    process-local coordination mechanism. *)
let cancellation_ready_activity =
  Temporal.Activity.define ~name:"smoke.cancellation_ready"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.unit (fun token ->
      match cancellation_ready_file () with
      | Error error -> Error error
      | Ok path -> publish_cancellation_ready path token)

(** Publishes a token from the signal workflow's first activity task. The driver
    waits for this worker-side marker before sending the typed signal, so the
    live scenario cannot be reduced to a start request whose signal was buffered
    before the worker accepted any workflow work. *)
let signal_condition_ready_activity =
  Temporal.Activity.define ~name:"smoke.signal_condition_ready"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.unit (fun token ->
      match signal_condition_ready_file () with
      | Error error -> Error error
      | Ok path -> publish_marker_token path token)

(** Starts with a worker-visible readiness activity, then parks the workflow on
    a deterministic condition. The signal handler stores its value in the
    current execution's local state; the condition is rechecked after that
    scheduler-owned handler mutates the same run. *)
let signal_condition_workflow =
  Temporal.Workflow.define ~name:"smoke.signal_condition"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun token ->
      let open Temporal.Result_syntax in
      let* () =
        Temporal.Activity.execute signal_condition_ready_activity token
      in
      let* () =
        Temporal.Condition.wait_until_result (fun () ->
            match Temporal.Workflow_context.Local.get signal_value_state with
            | Error error -> Error error
            | Ok value -> Ok (Option.is_some value))
      in
      match Temporal.Workflow_context.Local.get signal_value_state with
      | Ok (Some value) -> Ok ("SMOKE:SIGNAL:" ^ String.uppercase_ascii value)
      | Ok None ->
          Error
            (Temporal.Error.defect
               ~message:"signal condition resumed without a signal value")
      | Error error -> Error error)

(** Signals a separately started workflow execution through Temporal's
    workflow-to-workflow command path. The driver supplies the exact target
    identity as two newline-separated fields; parsing remains deterministic
    and keeps the public fixture codec intentionally small. *)
let external_signal_parent =
  Temporal.Workflow.define ~name:"smoke.external_signal_parent"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun target ->
      let open Temporal.Result_syntax in
      match String.split_on_char '\n' target with
      | [ workflow_id; run_id ] ->
          let* () =
            Temporal.Workflow.signal_external_workflow ~workflow_id ~run_id
              ~signal:signal_value ~input:"external"
            |> Temporal.Future.await
          in
          Ok "SMOKE:EXTERNAL:SIGNAL:ACK"
      | _ ->
          Error
            (Temporal.Error.defect
               ~message:"external signal target must contain workflow and run IDs"))

(** Attempts to cancel an external execution with a deliberately unrelated run
    ID. The workflow succeeds only when Temporal rejects the mismatched exact-run
    identity, proving the integration does not silently fall back to workflow ID
    alone. The pinned Core bridge reports that rejection as a retryable workflow
    error with a stable "not found" diagnostic prefix. *)
let external_cancellation_wrong_run_parent =
  Temporal.Workflow.define ~name:"smoke.external_cancellation_wrong_run_parent"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun target ->
      match String.split_on_char (Char.chr 10) target with
      | [ workflow_id; wrong_run_id ] -> (
          match
            Temporal.Workflow.cancel_external_workflow ~workflow_id
              ~run_id:wrong_run_id ~reason:"external acceptance mismatched run ID"
            |> Temporal.Future.await
          with
          | Error error ->
              let view = Temporal.Error.view error in
              if
                String.equal (Temporal.Error.kind error) "workflow"
                && not view.non_retryable
                && String.starts_with ~prefix:
                     "Unable to cancel external workflow because not found"
                     view.message
              then Ok "SMOKE:EXTERNAL:CANCEL:WRONG-RUN"
              else
                Error
                  (Temporal.Error.defect
                     ~message:
                       (Printf.sprintf
                          "wrong-run cancellation returned an unexpected error: kind=%s non_retryable=%b message=%S"
                          (Temporal.Error.kind error) view.non_retryable
                          view.message))
          | Ok () ->
              Error
                (Temporal.Error.defect
                   ~message:"external cancellation ignored the run ID"))
      | _ ->
          Error
            (Temporal.Error.defect
               ~message:
                 "wrong-run cancellation target must contain workflow and run IDs"))

(** Recognizes the retryable typed failure emitted by Core when an exact
    external-signal target no longer exists. Core includes the application
    failure type in the diagnostic because the public error view currently
    exposes only the broad workflow category. *)
let is_completed_external_signal_not_found error =
  let view = Temporal.Error.view error in
  view.category = `Workflow
  && not view.non_retryable
  && String.starts_with
       ~prefix:
         "Unable to signal external workflow because it was not found application type=ExternalWorkflowExecutionNotFound"
       view.message

(** Attempts to signal an external execution after its exact run has
    completed. Temporal must reject the terminal target with a typed not-found
    workflow failure rather than acknowledging delivery to a completed run. *)
let external_signal_completed_parent =
  Temporal.Workflow.define ~name:"smoke.external_signal_completed_parent"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun target ->
      match String.split_on_char (Char.chr 10) target with
      | [ workflow_id; run_id ] -> (
          match
            Temporal.Workflow.signal_external_workflow ~workflow_id ~run_id
              ~signal:signal_value ~input:"after-completion"
            |> Temporal.Future.await
          with
          | Error error ->
              let view = Temporal.Error.view error in
              if is_completed_external_signal_not_found error then
                Ok "SMOKE:EXTERNAL:SIGNAL:COMPLETED"
              else
                Error
                  (Temporal.Error.defect
                     ~message:
                       (Printf.sprintf
                          "completed-target signal returned an unexpected error: kind=%s non_retryable=%b message=%S"
                          (Temporal.Error.kind error) view.non_retryable
                          view.message))
          | Ok () ->
              Error
                (Temporal.Error.defect
                   ~message:"external signal succeeded for a completed target"))
      | _ ->
          Error
            (Temporal.Error.defect
               ~message:
                 "completed-target signal target must contain workflow and run IDs"))

(** Requests cancellation of a separately started workflow execution through
    Temporal's workflow-to-workflow command path. The driver supplies exact
    workflow/run IDs as two newline-separated fields and waits for the typed Core
    acknowledgement before checking the target's cancellation. *)
let external_cancellation_parent =
  Temporal.Workflow.define ~name:"smoke.external_cancellation_parent"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun target ->
      let open Temporal.Result_syntax in
      match String.split_on_char (Char.chr 10) target with
      | [ workflow_id; run_id ] ->
          let* () =
            Temporal.Workflow.cancel_external_workflow ~workflow_id ~run_id
              ~reason:"external acceptance requested cancellation"
            |> Temporal.Future.await
          in
          Ok "SMOKE:EXTERNAL:CANCEL:ACK"
      | _ ->
          Error
            (Temporal.Error.defect
               ~message:
                 "external cancellation target must contain workflow and run IDs"))

(** Builds the short, bounded policy used by [activity_retry]. Keeping this as a
    result lets the workflow return a typed configuration defect if the public
    constructor's validation ever changes, instead of hiding a construction
    exception in a module initializer. *)
let retry_policy =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 100L)
    ~backoff_coefficient:1.0
    ~maximum_interval:(Temporal.Duration.of_ms 100L)
    ~maximum_attempts:2 ()

(** The deliberately delayed policy used by [activity_long_backoff_retry]. A
    two-second interval is long enough for the activity itself to reject an
    immediate second attempt, while keeping the live acceptance bounded and
    independent of the much slower timeout-triggered scenarios. *)
let long_backoff_retry_policy =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 2_000L)
    ~backoff_coefficient:1.0
    ~maximum_interval:(Temporal.Duration.of_ms 2_000L)
    ~maximum_attempts:2 ()

(** Counts callbacks for the delayed retry in the worker process. The counter is
    intentionally outside workflow code: worker-local state is valid test
    instrumentation for an activity, while the workflow result remains the only
    cross-process acceptance oracle. *)
let long_backoff_retry_attempts = Atomic.make 0

(** Records when the first delayed-retry callback ran. The second callback
    checks elapsed worker time so an accidentally removed server backoff cannot
    still produce the expected second-attempt marker. *)
let long_backoff_retry_first_attempt_at = Atomic.make 0.0

(** Lower bound accepted by the activity-side delay assertion. It is below the
    configured two-second policy to tolerate scheduler and container clock
    jitter without accepting an immediate application retry. *)
let long_backoff_retry_minimum_delay_seconds = 1.0

(** Fails once, then succeeds only after Temporal has waited through the
    configured long retry interval. The elapsed-time guard makes this a useful
    live backoff test rather than another ordinary attempt-two assertion. *)
let long_backoff_retry_activity =
  Temporal.Activity.define ~name:"smoke.long_backoff_retry"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      let attempt = Atomic.fetch_and_add long_backoff_retry_attempts 1 + 1 in
      match attempt with
      | 1 ->
          Atomic.set long_backoff_retry_first_attempt_at (Unix.gettimeofday ());
          Error
            (Temporal.Error.make ~category:`Activity
               ~message:"intentional delayed retry for acceptance" ())
      | 2 ->
          let elapsed =
            Unix.gettimeofday ()
            -. Atomic.get long_backoff_retry_first_attempt_at
          in
          if elapsed < long_backoff_retry_minimum_delay_seconds then
            Error
              (Temporal.Error.defect
                 ~message:
                   (Printf.sprintf "long-backoff retry arrived after only %.3fs"
                      elapsed))
          else Ok ("SMOKE:BACKOFF:RETRIED:" ^ String.uppercase_ascii input)
      | attempt ->
          Error
            (Temporal.Error.defect
               ~message:
                 (Printf.sprintf
                    "long-backoff retry activity received unexpected attempt %d"
                    attempt)))

(** The heartbeat acceptance activity and workflow use a deliberately short
    timeout. It is long enough for the Core heartbeat manager to flush one
    request over the local Compose network, while still proving that the timeout
    delivered in the next activity attempt is the value selected by the workflow
    command. *)
let heartbeat_timeout = Temporal.Duration.of_ms 500L

(** The first heartbeat detail is a stable, human-readable marker. It is written
    by the first activity attempt and must be returned by Temporal in the
    retrying attempt's [Context.details] list; the driver never relies on
    worker-local mutable state for this assertion. *)
let heartbeat_progress_detail = "SMOKE:HEARTBEAT:PROGRESS:1"

(** Verifies the timeout that Temporal attached to this activity attempt. A
    missing or changed timeout is a protocol/configuration defect rather than an
    ordinary retryable activity failure, so the activity reports it as a
    non-retryable typed error. *)
let require_heartbeat_timeout context =
  match Temporal.Activity.Context.heartbeat_timeout context with
  | Some timeout
    when Int64.equal
           (Temporal.Duration.to_ms timeout)
           (Temporal.Duration.to_ms heartbeat_timeout) ->
      Ok ()
  | Some timeout ->
      Error
        (Temporal.Error.defect
           ~message:
             (Printf.sprintf "heartbeat timeout was %Ldms, expected %Ldms"
                (Temporal.Duration.to_ms timeout)
                (Temporal.Duration.to_ms heartbeat_timeout)))
  | None ->
      Error
        (Temporal.Error.defect
           ~message:"heartbeat timeout was absent from the activity attempt")

(** Sends one heartbeat, waits briefly for Core's asynchronous heartbeat manager
    to flush it, then returns a retryable application failure. The deliberate
    delay is activity-side code and cannot affect workflow replay; it prevents
    the immediate failure completion from racing the heartbeat request on a busy
    local Temporal Server. *)
let heartbeat_retry_activity =
  Temporal.Activity.define_with_context ~name:"smoke.heartbeat_retry"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string
    (fun context input ->
      let open Temporal.Result_syntax in
      let* () = require_heartbeat_timeout context in
      match Temporal.Activity.Context.details context with
      | [] ->
          let* () =
            Temporal.Activity.Context.heartbeat context Temporal.Codec.string
              heartbeat_progress_detail
          in
          Unix.sleepf 0.1;
          Error
            (Temporal.Error.make ~category:`Activity
               ~message:
                 "intentional retry after recording an activity heartbeat"
               ())
      | [ detail ] ->
          let* progress = Temporal.Codec.decode Temporal.Codec.string detail in
          if String.equal progress heartbeat_progress_detail then
            Ok ("SMOKE:HEARTBEAT:RETRIED:" ^ String.uppercase_ascii input)
          else
            Error
              (Temporal.Error.defect
                 ~message:
                   (Printf.sprintf "unexpected heartbeat detail %S on retry"
                      progress))
      | details ->
          Error
            (Temporal.Error.defect
               ~message:
                 (Printf.sprintf
                    "expected one heartbeat detail on retry, received %d"
                    (List.length details))))

(** A small live asynchronous activity used to prove the complete handoff
    lifecycle. The worker callback returns [Will_complete_async] immediately,
    then an OCaml-owned Domain waits outside the activity dispatch call and
    submits the typed result through the retained handle. The delay is
    deliberately longer than one scheduler turn, so a worker that accidentally
    completes the task synchronously would not satisfy this scenario. *)
let async_completion_delay_seconds = 0.25

let async_delayed_completion_activity =
  Temporal.Activity.define_async ~name:"smoke.async_delayed_completion"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string
    (fun context input ->
      let handle = Temporal.Activity.Async_context.handle context in
      let output = "SMOKE:ASYNC:COMPLETED:" ^ String.uppercase_ascii input in
      (* The Domain is created by OCaml rather than by Rust, so the retained
         handle remains inside the SDK's typed ownership boundary. Its only
         cross-domain operation is the public completion call, which is
         serialized by the async adapter's mutex and supervisor mailbox. *)
      ignore
        (Domain.spawn (fun () ->
             Unix.sleepf async_completion_delay_seconds;
             match Temporal.Activity.Async_handle.complete handle output with
             | Ok () -> ()
             | Error error ->
                 Printf.eprintf
                   "two-binary async activity completion failed (%s): %s\n%!"
                   (Temporal.Error.kind error)
                   (Temporal.Error.message error)));
      Temporal.Activity.Will_complete_async handle)

(** Schedules the delayed activity and returns its result through an ordinary
    direct-style workflow expression. The workflow never sees the retained
    handle or the activity-side Domain; it only observes the durable activity
    future, which keeps the public authoring model identical to a synchronous
    activity. *)
let async_activity_completion =
  Temporal.Workflow.define ~name:"smoke.async_activity_completion"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      Temporal.Activity.execute async_delayed_completion_activity seed)

(** Starts two independent activity commands before awaiting either result.
    [Future.all] preserves input order, making this scenario test both fan-out
    and deterministic aggregation when the live worker path exercises the
    complete activity command fields. *)
let fan_out =
  Temporal.Workflow.define ~name:"smoke.fan_out" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string (fun seed ->
      let left = Temporal.Activity.start mock_transform (seed ^ ":left") in
      let right = Temporal.Activity.start mock_transform (seed ^ ":right") in
      match Temporal.Future.await (Temporal.Future.all [ left; right ]) with
      | Error error -> Error error
      | Ok values -> Ok (String.concat "|" values))

(** Waits for a short durable timer before scheduling one activity. The timer is
    an SDK command rather than an OCaml sleep, so replay can resolve it from
    Temporal history when this definition is run by Core. *)
let timer_then_activity =
  Temporal.Workflow.define ~name:"smoke.timer_then_activity"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match Temporal.Workflow.sleep (Temporal.Duration.of_ms 10L) with
      | Error error -> Error error
      | Ok () -> Temporal.Activity.execute mock_transform (seed ^ ":timer"))

(** Runs the same typed activity through Core's local-activity lane. The live
    assertion checks that local activity scheduling, worker dispatch, and the
    history marker path work against a real server without turning the helper
    into a remote task-queue call. *)
let local_activity_workflow =
  Temporal.Workflow.define ~name:"smoke.local_activity"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      Temporal.Activity.execute_local
        ~start_to_close_timeout:(Temporal.Duration.of_ms 1_000L)
        mock_transform seed)

(** Command-only reference used by the executable continuation below. Keeping
    this reference separate avoids a recursive OCaml value: the workflow
    definition that the worker registers remains local and executable, while the
    continue-as-new command only needs the successor type name and input codec.
    Both values therefore describe the same Temporal workflow type. *)
let continue_as_new_target =
  Temporal.Workflow.remote ~name:"smoke.continue_as_new"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string

(** Continues exactly once so the live driver can verify both sides of the
    exact-run boundary. The first run emits a terminal continue-as-new command
    with fresh input; the successor run returns a normal typed result. Any
    unexpected input is a deterministic workflow defect, which prevents a
    malformed test request from looking like a successful continuation. *)
let continue_as_new =
  Temporal.Workflow.define ~name:"smoke.continue_as_new"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (function
    | "first" ->
        Temporal.Workflow.continue_as_new continue_as_new_target "second"
    | "second" -> Ok "SMOKE:CONTINUED:SECOND"
    | input ->
        Error
          (Temporal.Error.defect
             ~message:
               (Printf.sprintf "unexpected continue-as-new input %S" input)))

(** Schedules the transient activity with an explicit two-attempt retry policy.
    The activity itself fails once and then succeeds, so a live terminal result
    ending in [ATTEMPT:2] proves that Temporal Server and Core delivered a
    second activity task rather than the worker merely returning a local
    success. *)
let activity_retry =
  Temporal.Workflow.define ~name:"smoke.activity_retry"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match retry_policy with
      | Error error -> Error error
      | Ok policy ->
          Temporal.Activity.execute ~retry_policy:policy retry_once_activity
            seed)

(** Schedules [long_backoff_retry_activity] with the delayed policy. Eager
    execution is disabled so both attempts cross the normal worker poll and
    completion path used by the other live retry scenarios. *)
let activity_long_backoff_retry =
  Temporal.Workflow.define ~name:"smoke.activity_long_backoff_retry"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match long_backoff_retry_policy with
      | Error error -> Error error
      | Ok policy ->
          Temporal.Activity.execute ~retry_policy:policy
            ~do_not_eagerly_execute:true long_backoff_retry_activity seed)

(** Schedules [heartbeat_retry_activity] with both an explicit heartbeat timeout
    and the same bounded two-attempt retry policy used elsewhere in the fixture.
    The first attempt records progress and fails; the second attempt can finish
    only when Temporal has returned that progress detail and the configured
    timeout through its activity-task context. *)
let activity_heartbeat_retry =
  Temporal.Workflow.define ~name:"smoke.activity_heartbeat_retry"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match retry_policy with
      | Error error -> Error error
      | Ok policy ->
          Temporal.Activity.execute ~heartbeat_timeout ~retry_policy:policy
            heartbeat_retry_activity seed)

(** The start-to-close timeout for the timeout-only retry scenario. The first
    activity attempt intentionally runs longer than this value, so the second
    result can only be produced after Temporal observes the timeout and applies
    the workflow's retry policy. *)
let timeout_retry_start_to_close_timeout = Temporal.Duration.of_ms 500L

(** Delay used by the first timeout-retry attempt. Core adds a five-second local
    buffer to the 500ms server lease, so its local timeout is about 5.5s. The
    six-second delay leaves a small scheduling margin. When the callback
    returns, Core has already marked the timed-out token as not found and can
    discard the late success instead of waiting on a completion RPC for an
    expired token. The worker can then poll the retry without making this
    single-threaded activity adapter depend on server timing. This is activity
    code, not workflow code, so it cannot affect deterministic replay. *)
let timeout_retry_first_attempt_sleep_seconds = 6.0

(** Delays the timeout retry until after the intentionally slow first callback
    has released the serialized activity adapter. The ordinary 100ms policy is
    correct for short activities, but using it here would queue a second task
    while the first callback still owns the adapter mutex; that retry's own
    500ms start-to-close lease would then expire before it could be polled.
    Keeping this policy local to the timeout workflow preserves the short retry
    coverage for the other activities without hiding the adapter's ordering
    invariant by enlarging their leases. *)
let timeout_retry_policy =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 7_000L)
    ~backoff_coefficient:1.0
    ~maximum_interval:(Temporal.Duration.of_ms 7_000L)
    ~maximum_attempts:2 ()

(** Counts executions of the timeout-only activity in the worker process. A
    fresh Compose worker is created for each acceptance run, and the counter is
    never read by workflow code; it only gives the activity a test-local way to
    distinguish the intentionally slow first callback from Temporal's retry. *)
let timeout_retry_attempts = Atomic.make 0

(** Succeeds too late on its first callback for the configured start-to-close
    lease, then returns a distinct marker on the retry. The first callback does
    not return an application error: if the timeout path is absent, Temporal
    would accept its late success and the driver's exact second-attempt marker
    assertion would fail. *)
let timeout_retry_activity =
  Temporal.Activity.define ~name:"smoke.timeout_retry"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      let attempt = Atomic.fetch_and_add timeout_retry_attempts 1 + 1 in
      match attempt with
      | 1 ->
          Unix.sleepf timeout_retry_first_attempt_sleep_seconds;
          Ok "SMOKE:TIMEOUT:ATTEMPT:1"
      | 2 -> Ok ("SMOKE:TIMEOUT:RETRIED:" ^ String.uppercase_ascii input)
      | attempt ->
          Error
            (Temporal.Error.defect
               ~message:
                 (Printf.sprintf
                    "timeout retry activity received unexpected attempt %d"
                    attempt)))

(** Schedules [timeout_retry_activity] with a short start-to-close lease and the
    dedicated delayed two-attempt policy. The activity's late first success
    makes the final marker a server-visible proof that a timeout, rather than an
    application failure returned by the callback, caused the retry. *)
let activity_timeout_retry =
  Temporal.Workflow.define ~name:"smoke.activity_timeout_retry"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match timeout_retry_policy with
      | Error error -> Error error
      | Ok policy ->
          Temporal.Activity.execute
            ~start_to_close_timeout:timeout_retry_start_to_close_timeout
            ~retry_policy:policy ~do_not_eagerly_execute:true
            timeout_retry_activity seed)

(** The start-to-close lease for the heartbeat-timeout scenario is deliberately
    much longer than the heartbeat lease. If the server does not enforce the
    heartbeat timeout, the first callback can return successfully and the driver
    will observe the wrong attempt marker instead of a false retry. *)
let heartbeat_timeout_retry_start_to_close_timeout =
  Temporal.Duration.of_ms 10_000L

(** The first callback remains active long enough for Temporal to observe the
    missing heartbeat, but finishes before its start-to-close lease. Returning
    late is important: it proves the retry was caused by the server-managed
    heartbeat timeout rather than by an application error. *)
let heartbeat_timeout_retry_first_attempt_sleep_seconds = 6.0

(** Delays the heartbeat-timeout retry until the intentionally late first
    callback has released the serialized activity adapter. The retry policy is
    local to this scenario so the next task cannot be mistaken for a queue
    timeout caused by the still-running first callback. *)
let heartbeat_timeout_retry_policy =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 7_000L)
    ~backoff_coefficient:1.0
    ~maximum_interval:(Temporal.Duration.of_ms 7_000L)
    ~maximum_attempts:2 ()

(** Counts only activity callbacks in the worker process. The counter is never
    read by workflow code; a fresh Compose worker starts each acceptance run,
    and the second marker therefore proves that Temporal dispatched a retry task
    after the first callback stopped heartbeating. *)
let heartbeat_timeout_retry_attempts = Atomic.make 0

(** Stops heartbeating on the first attempt and returns only after the 500 ms
    heartbeat lease has expired. The second attempt validates its activity
    context and returns a distinct marker. If Temporal accepted the late first
    success, the driver would receive [ATTEMPT:1] and fail instead of passing.
*)
let heartbeat_timeout_retry_activity =
  Temporal.Activity.define_with_context ~name:"smoke.heartbeat_timeout_retry"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string
    (fun context input ->
      let open Temporal.Result_syntax in
      let* () = require_heartbeat_timeout context in
      let attempt =
        Atomic.fetch_and_add heartbeat_timeout_retry_attempts 1 + 1
      in
      match attempt with
      | 1 ->
          Unix.sleepf heartbeat_timeout_retry_first_attempt_sleep_seconds;
          Ok "SMOKE:HEARTBEAT_TIMEOUT:ATTEMPT:1"
      | 2 -> (
          match Temporal.Activity.Context.details context with
          | [] ->
              Ok
                ("SMOKE:HEARTBEAT_TIMEOUT:RETRIED:"
                ^ String.uppercase_ascii input)
          | details ->
              Error
                (Temporal.Error.defect
                   ~message:
                     (Printf.sprintf
                        "heartbeat-timeout retry unexpectedly received %d \
                         details"
                        (List.length details))))
      | attempt ->
          Error
            (Temporal.Error.defect
               ~message:
                 (Printf.sprintf
                    "heartbeat-timeout retry activity received unexpected \
                     attempt %d"
                    attempt)))

(** Schedules [heartbeat_timeout_retry_activity] with a heartbeat lease that
    expires before its start-to-close lease. The first callback sends no
    heartbeat, so the second result can only be produced by Temporal's
    heartbeat-timeout retry state machine. *)
let activity_heartbeat_timeout_retry =
  Temporal.Workflow.define ~name:"smoke.activity_heartbeat_timeout_retry"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match heartbeat_timeout_retry_policy with
      | Error error -> Error error
      | Ok policy ->
          Temporal.Activity.execute ~heartbeat_timeout
            ~start_to_close_timeout:
              heartbeat_timeout_retry_start_to_close_timeout
            ~retry_policy:policy ~do_not_eagerly_execute:true
            heartbeat_timeout_retry_activity seed)

(** Uses Temporal's [non_retryable_error_types] policy matching against the
    public activity error kind. The callback returns a retryable typed error on
    its first attempt and would succeed on a second attempt; the workflow
    catches the first activity future so the live result proves both that the
    server stopped retrying and that the runtime preserved the activity category
    and non-retryable decision. *)
let non_retryable_activity_policy =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 100L)
    ~backoff_coefficient:1.0
    ~maximum_interval:(Temporal.Duration.of_ms 100L)
    ~maximum_attempts:2 ~non_retryable_error_types:[ "activity" ] ()

(** Counts attempts only inside the worker process. A correct policy match
    leaves the second-attempt success branch unreachable; retaining that branch
    makes an accidental retry observable instead of silently passing. *)
let non_retryable_activity_attempts = Atomic.make 0

(** Returns a retryable activity failure whose error type matches the policy's
    non-retryable list. If Temporal retries despite that list, the second call
    returns a different success marker and the workflow reports a defect. *)
let non_retryable_activity =
  Temporal.Activity.define ~name:"smoke.non_retryable_activity"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun _input ->
      let attempt =
        Atomic.fetch_and_add non_retryable_activity_attempts 1 + 1
      in
      match attempt with
      | 1 -> (
          match
            Temporal.Codec.encode Temporal.Codec.string
              "SMOKE:ACTIVITY_NON_RETRYABLE:ATTEMPT:1"
          with
          | Error error -> Error error
          | Ok detail ->
              Error
                (Temporal.Error.make ~category:`Activity ~details:[ detail ]
                   ~message:"SMOKE:ACTIVITY_NON_RETRYABLE:ATTEMPT:1" ()))
      | 2 -> Ok "SMOKE:ACTIVITY_NON_RETRYABLE:RETRIED"
      | attempt ->
          Error
            (Temporal.Error.defect
               ~message:
                 (Printf.sprintf
                    "non-retryable activity received unexpected attempt %d"
                    attempt)))

(** Observes the activity future inside workflow code rather than relying on a
    top-level client failure, whose public category is intentionally always
    [Workflow]. The exact success marker is emitted only after the activity
    error is seen as [Activity] and [non_retryable] by the replay-safe runtime.
    The attempt marker travels in structured failure details because Temporal's
    public activity-failure diagnostic is an outer wrapper whose leading text is
    not the application message. *)
let activity_non_retryable_failure =
  Temporal.Workflow.define ~name:"smoke.activity_non_retryable_failure"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match non_retryable_activity_policy with
      | Error error -> Error error
      | Ok policy -> (
          match
            Temporal.Activity.execute ~retry_policy:policy
              non_retryable_activity seed
          with
          | Error error -> (
              let view = Temporal.Error.view error in
              if view.category <> `Activity || not view.non_retryable then
                Error
                  (Temporal.Error.defect
                     ~message:
                       (Printf.sprintf
                          "activity non-retryable metadata was not preserved \
                           (kind=%s, non_retryable=%b)"
                          (Temporal.Error.kind error)
                          view.non_retryable))
              else
                match view.details with
                | [ detail ] -> (
                    match
                      Temporal.Codec.decode Temporal.Codec.string detail
                    with
                    | Ok "SMOKE:ACTIVITY_NON_RETRYABLE:ATTEMPT:1" ->
                        Ok "SMOKE:ACTIVITY_NON_RETRYABLE:OBSERVED"
                    | Ok marker ->
                        Error
                          (Temporal.Error.defect
                             ~message:
                               (Printf.sprintf
                                  "activity non-retryable marker was \
                                   unexpected: %S"
                                  marker))
                    | Error decode_error -> Error decode_error)
                | details ->
                    Error
                      (Temporal.Error.defect
                         ~message:
                           (Printf.sprintf
                              "activity non-retryable details were not \
                               preserved (count=%d)"
                              (List.length details))))
          | Ok value ->
              Error
                (Temporal.Error.defect
                   ~message:
                     (Printf.sprintf
                        "non-retryable activity unexpectedly retried with %S"
                        value))))

(** Uses a one-attempt activity policy so the first activity failure becomes a
    retryable failure of the child workflow itself. The parent supplies the
    separate child retry policy below; keeping these policies distinct proves
    that Temporal retries the child execution rather than merely retrying its
    activity. *)
let child_activity_no_retry_policy =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 100L)
    ~backoff_coefficient:1.0
    ~maximum_interval:(Temporal.Duration.of_ms 100L)
    ~maximum_attempts:1 ()

(** Counts calls to the transient child activity in the worker process. This is
    deliberately activity-only state: workflow code never reads it, and a fresh
    Compose worker gives each live acceptance run an isolated counter. The
    second callback marker is the server-visible proof that a child retry
    created another execution attempt. *)
let child_retry_attempts = Atomic.make 0

(** Fails once so the enclosing child workflow can return a retryable workflow
    failure, then succeeds on the next child execution attempt. The activity
    itself has a one-attempt policy, preventing its own retry state machine from
    masking the child retry under test. *)
let child_retry_activity =
  Temporal.Activity.define ~name:"smoke.child_retry_once"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      let attempt = Atomic.fetch_and_add child_retry_attempts 1 + 1 in
      match attempt with
      | 1 ->
          Error
            (Temporal.Error.make ~category:`Activity
               ~message:"intentional transient child failure" ())
      | 2 -> Ok (Printf.sprintf "SMOKE:CHILD_RETRY:ATTEMPT:%d" attempt)
      | attempt ->
          Error
            (Temporal.Error.defect
               ~message:
                 (Printf.sprintf
                    "child retry activity received unexpected attempt %d for %S"
                    attempt input)))

(** Converts the first activity failure into a retryable workflow failure. The
    conversion makes the child retry policy's boundary explicit while the second
    activity marker remains the observable evidence that the child was retried
    by Temporal Core and Server. *)
let child_retryable_failure =
  Temporal.Workflow.define ~name:"smoke.child_retryable_failure"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match child_activity_no_retry_policy with
      | Error error -> Error error
      | Ok policy -> (
          match
            Temporal.Activity.execute ~retry_policy:policy child_retry_activity
              seed
          with
          | Ok marker -> Ok marker
          | Error _ ->
              Error
                (Temporal.Error.make ~category:`Workflow
                   ~message:"intentional retryable child workflow failure" ())))

(** Bounds the child retry to exactly two executions. Keeping this policy
    separate from the activity's one-attempt policy makes the ownership of the
    retry state machine visible in the fixture. *)
let child_retry_policy =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 100L)
    ~backoff_coefficient:1.0
    ~maximum_interval:(Temporal.Duration.of_ms 100L)
    ~maximum_attempts:2 ()

(** Starts the transient child with an explicit two-attempt child retry policy.
    The policy is handed to Core through [Child_workflow.execute], so this live
    acceptance path tests durable child retry rather than a replay-sensitive
    OCaml loop. *)
let parent_retries_child =
  Temporal.Workflow.define ~name:"smoke.parent_retries_child"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match child_retry_policy with
      | Error error -> Error error
      | Ok policy ->
          Temporal.Child_workflow.execute ~retry_policy:policy
            ~id:("two-binary-parent-retries-child-" ^ seed)
            child_retryable_failure seed)

(** A child workflow that waits on a short durable timer before deriving its
    result entirely from its input. The timer is deliberately inside the child
    rather than the parent so the live scenario exercises child start and
    terminal resolution as well as a child-owned timer activation. *)
let child_after_timer =
  Temporal.Workflow.define ~name:"smoke.child_after_timer"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match Temporal.Workflow.sleep (Temporal.Duration.of_ms 10L) with
      | Error error -> Error error
      | Ok () -> Ok (String.uppercase_ascii (seed ^ ":child")))

(** Reuses the already-running top-level cancellation execution as the child ID
    in the live start-failure scenario. Temporal IDs are namespace-wide, so the
    parent can only observe a child-start rejection if this conflicting
    execution has been accepted before the parent asks Core to start its child.
    The value is shared by the workflow and driver contract to keep the conflict
    deterministic and visible in source review. *)
let child_start_conflict_id = "two-binary-long-running-cancellation"

(** Starts [child_after_timer] with an identity derived only from the parent
    input, then waits through the public direct-style child helper. The fixed
    prefix keeps this fixture's child execution distinct from the driver's
    top-level workflow IDs, while deriving the suffix deterministically keeps
    the child command stable when Temporal replays the parent. *)
let parent_awaits_child =
  Temporal.Workflow.define ~name:"smoke.parent_awaits_child"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      Temporal.Child_workflow.execute
        ~id:("two-binary-parent-child-" ^ seed)
        child_after_timer seed)

(** The fixed top-level identity used by the dedicated parent/child restart
    fixture. Keeping it in this shared definition module prevents the client
    driver, worker observer, and future history controller from independently
    inventing IDs for what must be one exact Temporal execution. *)
let parent_child_restart_parent_id : string = "two-binary-parent-child-restart"

(** The only input accepted by the dedicated restart driver. The workflow
    definitions remain well-typed for arbitrary strings, while this fixed seed
    gives the external controller a stable child execution ID to inspect. *)
let parent_child_restart_input : string = "smoke"

(** Returns the deterministic durable child identity for one parent input. This
    function is intentionally pure and contains no worker-local state: Temporal
    replays the parent from history and must emit the identical child command on
    every replay. *)
let parent_child_restart_child_id (seed : string) : string =
  "two-binary-parent-child-restart-child-" ^ seed

(** The exact terminal payload expected by the dedicated parent/child restart
    driver. It identifies a parent that waited for its child after the child
    survived the worker replacement; it is not derived from worker generation or
    any other process-local observation. *)
let parent_child_restart_result : string = "SMOKE:PARENT:CHILD:RECOVERED"

(** The child-level result consumed by [parent_child_restart_parent]. It is
    separate from the parent result so the fixture can prove that the parent did
    not manufacture its terminal value before child resolution. *)
let parent_child_restart_child_result : string = "SMOKE:CHILD:RECOVERED"

(** The child holds the durable timer across the worker replacement. The long
    interval gives the external controller time to observe [TimerStarted] in the
    child history and stop generation one before [TimerFired] can reach it. The
    timer is a Temporal command, not an OCaml wall-clock sleep, so the
    replacement worker can replay the child deterministically. *)
let parent_child_restart_child : (string, string) Temporal.Workflow.t =
  Temporal.Workflow.define ~name:"smoke.parent_child_restart_child"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun _seed ->
      match Temporal.Workflow.sleep (Temporal.Duration.of_ms 120_000L) with
      | Error error -> Error error
      | Ok () -> Ok parent_child_restart_child_result)

(** Starts the long-running child with a deterministic ID and waits through the
    public direct-style child helper. The exact child marker is checked before
    the parent returns its own stable result, so a future accidental change to
    the child contract cannot make this recovery fixture pass on a merely
    completed but semantically different child. *)
let parent_child_restart_parent : (string, string) Temporal.Workflow.t =
  Temporal.Workflow.define ~name:"smoke.parent_child_restart_parent"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      let open Temporal.Result_syntax in
      let* child_result =
        Temporal.Child_workflow.execute
          ~id:(parent_child_restart_child_id seed)
          parent_child_restart_child seed
      in
      if String.equal child_result parent_child_restart_child_result then
        Ok parent_child_restart_result
      else
        Error
          (Temporal.Error.defect
             ~message:"parent/child restart child returned an unexpected result"))

(** Fixed identity for the failure-recovery parent. It is deliberately
    separate from the success fixture so a stale history or worker cannot make
    one acceptance scenario satisfy the other. *)
let parent_child_failure_replay_parent_id : string =
  "two-binary-parent-child-failure-replay"

(** The only input used by the child-failure replay driver. Keeping the seed
    constant makes the child ID and all child-start commands deterministic
    across the generation-one and generation-two workers. *)
let parent_child_failure_replay_input : string = "smoke"

(** Derives the exact child workflow ID from the parent input. This function is
    pure because replay must reproduce the same child-start command. *)
let parent_child_failure_replay_child_id (seed : string) : string =
  "two-binary-parent-child-failure-replay-child-" ^ seed

(** Stable output returned only after the parent has observed the child's
    non-retryable failure following worker replacement. The driver checks this
    marker instead of parsing Temporal's failure diagnostic text. *)
let parent_child_failure_replay_result : string =
  "SMOKE:PARENT:CHILD:FAILURE_RECOVERED"

(** Stable child workflow type used by the failure-recovery history contract. *)
let parent_child_failure_replay_child_type =
  "smoke.parent_child_failure_replay_child"

(** Stable parent workflow type used by the failure-recovery history contract. *)
let parent_child_failure_replay_parent_type =
  "smoke.parent_child_failure_replay_parent"

(** The child remains on a durable timer while generation one is removed. The
    timer is intentionally long enough for the controller to capture the
    nonterminal prefix and replace the worker; generation two replays the
    prefix, receives [TimerFired], and then emits the typed failure. *)
let parent_child_failure_replay_child : (string, string) Temporal.Workflow.t =
  Temporal.Workflow.define ~name:parent_child_failure_replay_child_type
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun _seed ->
      let open Temporal.Result_syntax in
      let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 60_000L) in
      Error
        (Temporal.Error.make ~non_retryable:true ~category:`Workflow
           ~message:"intentional child failure after replay" ()))

(** Waits for the failing child and converts only the expected typed child
    failure into a stable success marker. Returning a success value here makes
    the parent history prove that the parent resumed, validated the child
    failure metadata, and completed after replacement. Any unexpected success
    or category/ retryability change remains a typed defect. *)
let parent_child_failure_replay_parent : (string, string) Temporal.Workflow.t =
  Temporal.Workflow.define ~name:parent_child_failure_replay_parent_type
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match
        Temporal.Child_workflow.execute
          ~id:(parent_child_failure_replay_child_id seed)
          parent_child_failure_replay_child seed
      with
      | Error error ->
          let view = Temporal.Error.view error in
          if view.category = `Child_workflow && view.non_retryable then
            Ok parent_child_failure_replay_result
          else
            Error
              (Temporal.Error.defect
                 ~message:
                   "child failure replay returned unexpected error metadata")
      | Ok value ->
          Error
            (Temporal.Error.defect
               ~message:
                 (Printf.sprintf
                    "child failure replay unexpectedly succeeded with %S"
                    value)))

(** Attempts to start a child using [child_start_conflict_id], which is held by
    the live top-level cancellation workflow. A successful child result would
    prove that the duplicate-ID guard was lost, so the parent turns that path
    into a defect. The expected [Child_workflow] and non-retryable metadata are
    checked inside the workflow before returning a stable driver marker. *)
let parent_observes_child_start_failure =
  Temporal.Workflow.define ~name:"smoke.parent_observes_child_start_failure"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match
        Temporal.Child_workflow.execute ~id:child_start_conflict_id
          child_after_timer seed
      with
      | Ok value ->
          Error
            (Temporal.Error.defect
               ~message:
                 (Printf.sprintf
                    "duplicate child ID unexpectedly started child with %S"
                    value))
      | Error error ->
          let view = Temporal.Error.view error in
          if view.category = `Child_workflow && view.non_retryable then
            Ok "SMOKE:CHILD:START_FAILED"
          else
            Error
              (Temporal.Error.defect
                 ~message:
                   (Printf.sprintf
                      "duplicate child ID returned unexpected metadata \
                       (category=%s, non_retryable=%b)"
                      (Temporal.Error.kind error)
                      view.non_retryable)))

(** A child workflow that fails with a deterministic, non-retryable workflow
    error. Keeping the failure in the child (rather than failing the parent
    directly) lets the acceptance driver observe the public child-workflow error
    category and verify that the parent future is resolved exactly once. *)
let child_non_retryable_failure =
  Temporal.Workflow.define ~name:"smoke.child_non_retryable_failure"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun _seed ->
      Error
        (Temporal.Error.make ~non_retryable:true ~category:`Workflow
           ~message:"intentional child workflow failure" ()))

(** Propagates the terminal failure from [child_non_retryable_failure] through
    the public direct-style child helper. The parent has no recovery branch on
    purpose: a top-level [Client.Failed] result proves that Core translated the
    child failure into the parent workflow's terminal outcome. *)
let parent_awaits_failed_child =
  Temporal.Workflow.define ~name:"smoke.parent_awaits_failed_child"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      Temporal.Child_workflow.execute
        ~id:("two-binary-parent-failed-child-" ^ seed)
        child_non_retryable_failure seed)

(** Keeps a child execution outstanding until its parent requests cancellation.
    The long timer prevents a broken cancellation command from being hidden by
    natural completion, while the body remains replay-safe because it only uses
    a durable Temporal timer and its input. *)
let child_long_running =
  Temporal.Workflow.define ~name:"smoke.child_long_running"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match Temporal.Workflow.sleep (Temporal.Duration.of_ms 30_000L) with
      | Error error -> Error error
      | Ok () -> Ok (String.uppercase_ascii (seed ^ ":child-finished")))

(** Starts [child_long_running], requests cancellation through the opaque child
    handle, and waits for the typed cancellation result. This deliberately uses
    [Wait_cancellation_requested] so the parent cannot report success until Core
    has delivered the child's cancellation acknowledgement. The exact marker
    returned on success keeps the driver assertion independent of Core's verbose
    failure diagnostic. *)
let parent_cancels_child =
  Temporal.Workflow.define ~name:"smoke.parent_cancels_child"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      let handle =
        Temporal.Child_workflow.start_handle
          ~cancellation_type:Temporal.Child_workflow.Wait_cancellation_requested
          ~id:("two-binary-parent-cancel-child-" ^ seed)
          child_long_running seed
      in
      match
        Temporal.Child_workflow.cancel ~reason:"acceptance child cancelled"
          handle
      with
      | Error error -> Error error
      | Ok () -> (
          match
            Temporal.Future.await (Temporal.Child_workflow.future handle)
          with
          | Ok _ ->
              Error
                (Temporal.Error.defect
                   ~message:"cancelled child unexpectedly completed")
          | Error error ->
              let view = Temporal.Error.view error in
              if view.category = `Cancelled then Ok "SMOKE:CHILD:CANCELLED"
              else
                Error
                  (Temporal.Error.defect
                     ~message:
                       (Printf.sprintf
                          "expected child cancellation, received %s"
                          (Temporal.Error.kind error)))))

(** Deliberately fails the workflow with a typed, non-retryable application
    error. The failure is deterministic and does not inspect its input, so a
    replay observes the same terminal command. The live driver checks the
    category, retry policy, and stable message prefix rather than matching
    Core's full diagnostic text, which may include source and failure-info
    context. *)
let non_retryable_failure =
  Temporal.Workflow.define ~name:"smoke.non_retryable_failure"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun _seed ->
      Error
        (Temporal.Error.make ~non_retryable:true ~category:`Workflow
           ~message:"intentional terminal workflow failure" ()))

(** Keeps a workflow execution open on a durable timer until the driver asks
    Temporal to cancel its exact run. The long interval is intentional: the
    workflow must still be outstanding when [Temporal.Client.cancel] returns,
    while the body remains replay-safe because it uses no wall clock, random
    value, I/O, or process-global state. Cancellation is converted by the native
    worker into a terminal [Cancel_workflow_execution] command before the timer
    fires. *)
let long_running_cancellation =
  Temporal.Workflow.define ~name:"smoke.long_running_cancellation"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun token ->
      let timer =
        Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 30_000L)
      in
      let marker =
        Temporal.Activity.start ~do_not_eagerly_execute:true
          cancellation_ready_activity token
      in
      match Temporal.Future.await (Temporal.Future.both timer marker) with
      | Error error -> Error error
      | Ok ((), ()) -> Ok (String.uppercase_ascii (token ^ ":finished")))

(** Keeps one workflow run outstanding across a worker replacement. The timer is
    long enough for the controller to observe [TimerStarted] and stop the first
    worker before the server delivers [TimerFired]. Once the replacement worker
    replays the pending timer, the workflow schedules the deliberately transient
    activity with the bounded two-attempt policy. The final marker therefore
    proves both replay and a server-delivered retry on generation 2. *)
let worker_restart_replay =
  Temporal.Workflow.define ~name:"smoke.worker_restart_replay"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun _seed ->
      match Temporal.Workflow.sleep (Temporal.Duration.of_ms 60_000L) with
      | Error error -> Error error
      | Ok () ->
          let open Temporal.Result_syntax in
          let* policy = retry_policy in
          let* transformed =
            Temporal.Activity.execute ~retry_policy:policy retry_once_activity
              "after-replay"
          in
          Ok ("SMOKE:" ^ transformed))

(** Keeps a workflow run in Core's sticky cache while a second run is admitted.
    The long deterministic timer gives Core an explicit pending history
    boundary, so the first workflow task is fully acknowledged before cache
    pressure is introduced. It is deliberately longer than the driver's
    eviction budget: if this run reached its terminal timer completion first,
    Core would remove it for normal completion and there would be no
    cache-full transition to observe. The live fixture configures one cache slot; when
    the second workflow is admitted Core must therefore deliver a
    [RemoveFromCache(CacheFull)] activation for the older run. The driver
    observes that marker and cancels both exact executions. *)
let cache_eviction =
  Temporal.Workflow.define ~name:"smoke.cache_eviction"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      let open Temporal.Result_syntax in
      let* () =
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 3_600_000L)
      in
      Ok ("SMOKE:CACHE:" ^ String.uppercase_ascii seed))

(** A read-only cache-settling probe used only by the live eviction fixture.
    The driver invokes it after A has completed its initial timer-scheduling
    activation and before it starts B. Receiving this typed response proves
    that Core has processed a follow-up activation for A, providing a settled
    native-stream boundary after the OCaml initial-completion marker. It does
    not itself claim which cache route Core took. The handler has no mutable
    state and emits no workflow command, so it is replay-safe. *)
let cache_eviction_residency_query =
  Temporal.Query.define ~name:"smoke.cache_eviction_residency"
    ~output:Temporal.Codec.string

(** Answers the cache-settling probe with a fixed, codec-checked value.
    Keeping the response independent of workflow-local state makes this
    fixture a synchronization check rather than a second behavioral contract
    for the timer workflow. *)
let cache_eviction_residency_handler =
  Temporal.Query.Handler.make cache_eviction_residency_query (fun () ->
      Ok "resident")
