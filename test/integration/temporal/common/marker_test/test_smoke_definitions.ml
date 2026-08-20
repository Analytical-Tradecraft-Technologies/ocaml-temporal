(** Focused tests for the filesystem handshake used by the live acceptance
    fixture. These tests stay independent of Docker and Temporal Server: the
    worker activity's marker protocol must be correct before either external
    service is started. *)

(** Raises a useful failure when one marker contract assertion is false. *)
let require condition message = if not condition then failwith message

(** Reads the complete marker file after the writer has atomically published
    it. The helper is intentionally strict so the test catches an extra line
    or an omitted terminating newline. *)
let read_file path =
  let channel = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in_noerr channel)
    (fun () -> really_input_string channel (in_channel_length channel))

(** Removes a path when present, preserving the original assertion failure if
    cleanup itself is unable to find an already-removed file. *)
let remove_if_present path =
  try if Sys.file_exists path then Sys.remove path with _ -> ()

(** Lists staging files created beside the final marker. A successful atomic
    publication must leave no temporary file behind. *)
let staging_files path =
  let directory = Filename.dirname path in
  let prefix = Filename.basename path ^ ".tmp." in
  Sys.readdir directory
  |> Array.to_list
  |> List.filter (fun name -> String.starts_with ~prefix name)

(** Runs the real contextual heartbeat activity twice with an in-memory
    context. The first attempt must publish one progress payload and return a
    retryable failure; the second attempt receives that payload and the
    server-supplied timeout and may return the success value. This is the
    Docker-free counterpart of the live workflow: it exercises the exact
    activity callback used by the worker without pretending that a local fake
    observed Temporal Server retry delivery. *)
let test_heartbeat_retry_activity_contract () =
  let implementation =
    match
      Temporal.Activity.implementation_with_context
        Smoke_definitions.heartbeat_retry_activity
    with
    | Some implementation -> implementation
    | None -> failwith "heartbeat acceptance activity lost its context callback"
  in
  let timeout_ms = 500L in
  (* Keep timeout validation at the public boundary: this catches a
     conversion defect even when the in-memory base context was constructed
     with the value expected by the fixture. *)
  let require_timeout context label =
    match Temporal.Activity.Context.heartbeat_timeout context with
    | Some timeout when Temporal.Duration.to_ms timeout = timeout_ms -> ()
    | Some timeout ->
        failwith
          (Printf.sprintf "%s saw heartbeat timeout %Ldms, expected %Ldms" label
             (Temporal.Duration.to_ms timeout) timeout_ms)
    | None -> failwith (label ^ " saw no heartbeat timeout")
  in
  let first_heartbeats = ref [] in
  let first_context =
    Temporal_base.Activity_context.create
      ~heartbeat:(fun payloads ->
        first_heartbeats := payloads :: !first_heartbeats;
        Ok ())
      ~details:[]
      ~heartbeat_timeout:(Some (Temporal_base.Duration.of_ms timeout_ms))
  in
  require_timeout first_context "first attempt";
  (match implementation first_context "smoke" with
  | Ok _ -> failwith "first heartbeat attempt unexpectedly succeeded"
  | Error error ->
      let view = Temporal.Error.view error in
      require (view.category = `Activity)
        "first heartbeat attempt returned the wrong error category";
      require (not view.non_retryable)
        "first heartbeat attempt was not retryable";
      require
        (String.equal view.message
           "intentional retry after recording an activity heartbeat")
        "first heartbeat attempt returned an unexpected message");
  require (List.length !first_heartbeats = 1)
    "first heartbeat attempt did not submit exactly one heartbeat";
  let heartbeat_details =
    match !first_heartbeats with
    | [ details ] -> details
    | _ -> failwith "heartbeat callback retained an unexpected call shape"
  in
  let heartbeat_detail =
    match heartbeat_details with
    | [ payload ] -> payload
    | _ -> failwith "first heartbeat did not contain exactly one detail"
  in
  require
    (List.mem ("encoding", "json/plain") heartbeat_detail.metadata)
    "heartbeat detail did not retain its codec metadata";
  (match Temporal_base.Codec.decode Temporal_base.Codec.string heartbeat_detail with
  | Ok value ->
      require
        (String.equal value Smoke_definitions.heartbeat_progress_detail)
        "heartbeat detail payload changed before the retry"
  | Error error ->
      failwith
        ("heartbeat detail could not be decoded: " ^ Temporal_base.Error.message error));
  let retained_details = Temporal.Activity.Context.details first_context in
  require (List.length retained_details = 1)
    "context did not retain the successful heartbeat detail";
  Temporal_base.Activity_context.invalidate first_context;
  let callback_calls_after_invalidate = List.length !first_heartbeats in
  (match
     Temporal.Activity.Context.heartbeat first_context Temporal.Codec.string
       "stale-heartbeat"
   with
  | Ok () -> failwith "invalidated heartbeat context accepted progress"
  | Error error ->
      let view = Temporal.Error.view error in
      require (view.category = `Bridge)
        "invalidated heartbeat context returned the wrong error category";
      require
        (String.equal view.message "activity context is no longer active")
        "invalidated heartbeat context returned an unexpected message");
  require (List.length !first_heartbeats = callback_calls_after_invalidate)
    "invalidated heartbeat context entered its native callback";
  let retained_details : Temporal_base.Payload.t list =
    List.map
      (fun ({ Temporal.Payload.metadata; data } : Temporal.Payload.t) ->
        { Temporal_base.Payload.metadata; data = Bytes.copy data })
      retained_details
  in
  let second_heartbeats = ref [] in
  let second_context =
    Temporal_base.Activity_context.create
      ~heartbeat:(fun payloads ->
        second_heartbeats := payloads :: !second_heartbeats;
        Ok ())
      ~details:retained_details
      ~heartbeat_timeout:(Some (Temporal_base.Duration.of_ms timeout_ms))
  in
  require_timeout second_context "retry attempt";
  (match implementation second_context "smoke" with
  | Ok value ->
      require (String.equal value "SMOKE:HEARTBEAT:RETRIED:SMOKE")
        "retry attempt returned an unexpected result"
  | Error error ->
      failwith ("retry attempt unexpectedly failed: " ^ Temporal.Error.message error));
  require (List.length !second_heartbeats = 0)
    "retry attempt emitted a duplicate heartbeat";
  Temporal_base.Activity_context.invalidate second_context

(** Verifies that one publication uses the marker directory, writes the exact
    token, and cleans its unique staging file after the rename. *)
let test_atomic_publication () =
  let directory = Filename.get_temp_dir_name () in
  let path =
    Filename.concat directory
      (Printf.sprintf "ocaml-temporal-marker-%d" (Unix.getpid ()))
  in
  let token = "cancellation-test-token" in
  remove_if_present path;
  Fun.protect
    ~finally:(fun () -> remove_if_present path)
    (fun () ->
      require (staging_files path = []) "staging file existed before test";
      (match Smoke_definitions.publish_cancellation_ready path token with
      | Ok () -> ()
      | Error error ->
          failwith
            (Printf.sprintf "marker publication failed: %s"
               (Temporal.Error.message error)));
      require (Sys.file_exists path) "marker was not published";
      require (String.equal (read_file path) (token ^ "\n"))
        "marker contents were not exact";
      require (staging_files path = []) "staging file remained after rename")

(** Pins the live Core failure shape used by the completed-target signal
    assertion. This keeps a wording mismatch from requiring a full Compose run
    to diagnose while still rejecting the wrong category, retry policy, or
    application failure type. *)
let test_completed_external_signal_not_found_contract () =
  let message =
    "Unable to signal external workflow because it was not found application type=ExternalWorkflowExecutionNotFound non_retryable=false details=0"
  in
  let error category ?(non_retryable = false) message =
    Temporal.Error.make ~category ~non_retryable ~message ()
  in
  require
    (Smoke_definitions.is_completed_external_signal_not_found
       (error `Workflow message))
    "completed-target signal did not accept Core's typed not-found failure";
  require
    (not
       (Smoke_definitions.is_completed_external_signal_not_found
          (error `Bridge message)))
    "completed-target signal accepted the wrong error category";
  require
    (not
       (Smoke_definitions.is_completed_external_signal_not_found
          (error `Workflow ~non_retryable:true message)))
    "completed-target signal accepted a non-retryable failure";
  require
    (not
       (Smoke_definitions.is_completed_external_signal_not_found
          (error `Workflow
             "Unable to signal external workflow because it was not found application type=DifferentFailure")))
    "completed-target signal accepted the wrong application failure type"

(** Verifies the success path and reports a stable test name in Dune output. *)
let () =
  test_atomic_publication ();
  test_heartbeat_retry_activity_contract ();
  test_completed_external_signal_not_found_contract ();
  print_endline "smoke definitions marker test passed"
