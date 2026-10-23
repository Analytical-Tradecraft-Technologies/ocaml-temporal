(** Offline replay through the public [Temporal.Replay] API only.

    The fixtures are the retained live task-failure histories under
    [test/integration/temporal/task_failure/histories]. No Temporal Server is
    involved: each call drives pinned Temporal Core's replay worker through
    the private native bridge and the production workflow adapter. *)

module R = Temporal.Replay

(** Reads one binary fixture declared as a Dune dependency of this test. *)
let read_fixture name =
  In_channel.with_open_bin
    (Filename.concat "../integration/temporal/task_failure/histories"
       (name ^ ".pb"))
    In_channel.input_all

(** Just enough of the protobuf wire format to derive deliberately invalid
    histories from a valid fixture, so each negative case differs from a real
    server export in exactly one documented way. Fields keep their original
    bytes and order; only the edited fields change. Test-only: it trusts its
    input, which is always a checked-in fixture. *)
module Wire = struct
  type field = { number : int; wire_type : int; value : string }
  (** One field. [value] is the field's payload bytes: the encoded varint for
      wire type 0, the content without its length prefix for wire type 2,
      and the fixed-width bytes for wire types 1 and 5. *)

  (** Decodes the varint at [index] in [bytes], returning it and the index
      after it. Values above [max_int] do not occur in the fixtures. *)
  let read_varint bytes index =
    let rec loop index shift acc =
      let byte = Char.code bytes.[index] in
      let acc = acc lor ((byte land 0x7f) lsl shift) in
      if byte < 0x80 then (acc, index + 1) else loop (index + 1) (shift + 7) acc
    in
    loop index 0 0

  (** Encodes a non-negative integer as a varint. *)
  let varint value =
    let buffer = Buffer.create 10 in
    let rec loop value =
      if value < 0x80 then Buffer.add_char buffer (Char.chr value)
      else (
        Buffer.add_char buffer (Char.chr (value land 0x7f lor 0x80));
        loop (value lsr 7))
    in
    loop value;
    Buffer.contents buffer

  (** Splits one serialized message into its fields, in wire order. *)
  let parse bytes =
    let rec loop index acc =
      if index >= String.length bytes then List.rev acc
      else
        let key, index = read_varint bytes index in
        let wire_type = key land 7 in
        let value, next =
          match wire_type with
          | 0 ->
              let _, next = read_varint bytes index in
              (String.sub bytes index (next - index), next)
          | 1 -> (String.sub bytes index 8, index + 8)
          | 2 ->
              let length, start = read_varint bytes index in
              (String.sub bytes start length, start + length)
          | 5 -> (String.sub bytes index 4, index + 4)
          | _ -> failwith "Wire.parse: unsupported wire type"
        in
        loop next ({ number = key lsr 3; wire_type; value } :: acc)
    in
    loop 0 []

  (** Serializes fields back into one message. *)
  let serialize fields =
    String.concat ""
      (List.map
         (fun { number; wire_type; value } ->
           varint ((number lsl 3) lor wire_type)
           ^ (if wire_type = 2 then varint (String.length value) else "")
           ^ value)
         fields)

  (** Removes every occurrence of field [number]. *)
  let drop number fields = List.filter (fun field -> field.number <> number) fields

  (** Applies [edit] to the fields of each embedded message [number]. *)
  let map_message number edit fields =
    List.map
      (fun field ->
        if field.number = number then
          { field with value = serialize (edit (parse field.value)) }
        else field)
      fields
end

(** [temporal.api.history.v1] field numbers used by the edits below. *)
module History_fields = struct
  (** [History.events]. *)
  let events = 1

  (** [HistoryEvent.event_id]. *)
  let event_id = 1

  (** [HistoryEvent.event_time]. *)
  let event_time = 2

  (** [HistoryEvent.workflow_execution_started_event_attributes]. *)
  let started_attributes = 6

  (** [HistoryEvent.workflow_task_scheduled_event_attributes]. *)
  let task_scheduled_attributes = 10

  (** [WorkflowExecutionStartedEventAttributes.task_queue]. *)
  let task_queue = 5

  (** [WorkflowExecutionStartedEventAttributes.workflow_id]. *)
  let workflow_id = 28
end

(** Rewrites a serialized History by applying [edit index fields] to every
    event, where [index] counts events from 0 in history order. *)
let map_events edit history =
  let index = ref 0 in
  Wire.parse history
  |> List.map (fun (field : Wire.field) ->
         if field.number <> History_fields.events then field
         else
           let event_index = !index in
           incr index;
           {
             field with
             value = Wire.serialize (edit event_index (Wire.parse field.value));
           })
  |> Wire.serialize

(** Rewrites only the start event's attributes. *)
let map_started_attributes edit =
  map_events (fun index fields ->
      if index = 0 then
        Wire.map_message History_fields.started_attributes edit fields
      else fields)

(** Builds the history recorded for the live fixture case [name]. *)
let history name =
  match
    R.History.of_protobuf ~workflow_id:("task-failure-" ^ name)
      (read_fixture name)
  with
  | Ok history -> history
  | Error error -> failwith (Temporal.Error.message error)

(** The workflow type used by every task-failure fixture case. *)
let workflow_type name = "task-failure." ^ name

(** Defines a [unit -> string] workflow under a fixture's recorded type. *)
let define name implementation =
  Temporal.Workflow.define ~name:(workflow_type name)
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string implementation

(** The corrected source generation recorded in [body] and [encoder]: one
    100 ms timer, then a result. *)
let compatible name =
  define name (fun () ->
      let open Temporal.Result_syntax in
      let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 100L) in
      Ok "recovered")

(** An incompatible change: the recorded timer was removed, so the first
    workflow task completes the workflow instead of starting the timer. *)
let timer_removed name = define name (fun () -> Ok "recovered")

(** An incompatible change: an activity is scheduled where the history
    recorded a timer. *)
let activity_instead_of_timer name =
  let activity =
    Temporal.Activity.remote ~name:"replay-test.activity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string
  in
  define name (fun () ->
      Temporal.Activity.execute
        ~start_to_close_timeout:(Temporal.Duration.of_ms 5_000L)
        activity ())

(** A deliberate business failure, matching the [business-*] fixtures. *)
let business name non_retryable =
  define name (fun () ->
      Error
        (Temporal.Error.make ~category:`Workflow ~non_retryable
           ~message:"intentional business failure" ()))

(** A workflow whose code fails its task with a defect on replay. *)
let defective name =
  define name (fun () ->
      Error (Temporal.Error.defect ~message:"replay-test defect"))

(** Fails the test with the public diagnostic of an unexpected result. *)
let expect_ok label = function
  | Ok () -> ()
  | Error failure ->
      failwith (label ^ ": expected Ok, got " ^ R.failure_message failure)

(** Describes a result in failure messages. *)
let describe = function
  | Ok () -> "Ok"
  | Error failure -> R.failure_message failure

(** Requires a [Nondeterminism] result with a non-empty Core description and,
    when given, the recorded event and produced command Core named. Every
    nondeterminism result here replays a [task-failure-body] history, so the
    workflow context is checked too. *)
let expect_nondeterminism ?event ?command label result =
  match result with
  | Error (R.Nondeterminism { run_id; message; mismatch }) ->
      if run_id = "" || message = "" || mismatch.reason = "" then
        failwith (label ^ ": nondeterminism diagnostic is empty");
      if mismatch.workflow_id <> "task-failure-body" then
        failwith (label ^ ": wrong workflow ID " ^ mismatch.workflow_id);
      if mismatch.workflow_type <> Some (workflow_type "body") then
        failwith (label ^ ": missing workflow type");
      Option.iter
        (fun (id, event_type) ->
          if
            mismatch.event_id <> Some id
            || mismatch.event_type <> Some event_type
          then failwith (label ^ ": unexpected event in " ^ mismatch.reason))
        event;
      Option.iter
        (fun command ->
          if mismatch.command <> Some command then
            failwith (label ^ ": unexpected command in " ^ mismatch.reason))
        command
  | _ -> failwith (label ^ ": expected nondeterminism, got " ^ describe result)

(** Requires a [Workflow_task_failed] result. *)
let expect_task_failed label result =
  match result with
  | Error (R.Workflow_task_failed { message; _ }) ->
      if message = "" then failwith (label ^ ": task failure message is empty")
  | _ ->
      failwith (label ^ ": expected workflow task failure, got " ^ describe result)

(** Requires an [Invalid_history] result. *)
let expect_invalid label result =
  match result with
  | Error (R.Invalid_history _) -> ()
  | _ -> failwith (label ^ ": expected invalid history, got " ^ describe result)

(** Requires a [Replay_error] result. *)
let expect_replay_error label result =
  match result with
  | Error (R.Replay_error _) -> ()
  | _ -> failwith (label ^ ": expected replay error, got " ^ describe result)

(** Compatible code replays recorded histories, including histories with
    earlier failed workflow tasks and deliberately failed executions. *)
let test_compatible_histories () =
  List.iter
    (fun name ->
      expect_ok name
        (R.replay ~workflows:[ R.workflow (compatible name) ] (history name)))
    [ "body"; "encoder" ];
  expect_ok "missing"
    (R.replay
       ~workflows:[ R.workflow (timer_removed "missing") ]
       (history "missing"));
  expect_ok "business-retryable"
    (R.replay
       ~workflows:[ R.workflow (business "business-retryable" false) ]
       (history "business-retryable"));
  expect_ok "business-permanent"
    (R.replay
       ~workflows:[ R.workflow (business "business-permanent" true) ]
       (history "business-permanent"))

(** An incompatible change: a second timer after the recorded one, so the
    code produces a timer where the history recorded the workflow's
    completion. *)
let extra_timer name =
  define name (fun () ->
      let open Temporal.Result_syntax in
      let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 100L) in
      let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 100L) in
      Ok "recovered")

(** Changed command sequences are reported as nondeterminism that names the
    recorded event and the command the changed code produced instead. *)
let test_incompatible_changes () =
  expect_nondeterminism "timer removed" ~event:(5L, "TimerStarted")
    ~command:"Complete workflow"
    (R.replay ~workflows:[ R.workflow (timer_removed "body") ] (history "body"));
  expect_nondeterminism "activity instead of timer"
    ~event:(5L, "TimerStarted") ~command:"Activity"
    (R.replay
       ~workflows:[ R.workflow (activity_instead_of_timer "body") ]
       (history "body"));
  expect_nondeterminism "extra timer"
    ~event:(16L, "WorkflowExecutionCompleted") ~command:"Timer"
    (R.replay ~workflows:[ R.workflow (extra_timer "body") ] (history "body"))

(** [contains text needle] is true when [needle] occurs in [text]. *)
let contains text needle =
  let text_length = String.length text in
  let needle_length = String.length needle in
  let rec loop index =
    index + needle_length <= text_length
    && (String.sub text index needle_length = needle || loop (index + 1))
  in
  loop 0

(** A workflow ID that [History.of_protobuf] accepts but that must not be
    copied verbatim into a log line: line breaks, a tab, a backslash, an
    escape byte, a multi-byte character, and about 60 KB of padding. The
    rendered [failure_message] stays one bounded line while the [mismatch]
    record keeps the exact ID. The fixture records its real workflow ID, so
    the ID field is removed first, as in a history from a server older than
    1.21; replay then uses the supplied ID as given. *)
let test_hostile_workflow_id () =
  let workflow_id =
    "multi\nline\r\tid\\\027" ^ "\xc3\xa9" ^ String.make 60_000 'w'
  in
  let unrecorded =
    map_started_attributes
      (Wire.drop History_fields.workflow_id)
      (read_fixture "body")
  in
  let history =
    match R.History.of_protobuf ~workflow_id unrecorded with
    | Ok history -> history
    | Error error -> failwith (Temporal.Error.message error)
  in
  match R.replay ~workflows:[ R.workflow (timer_removed "body") ] history with
  | Error (R.Nondeterminism { mismatch; _ } as failure) ->
      if mismatch.workflow_id <> workflow_id then
        failwith "hostile ID: the mismatch record altered the workflow ID";
      let line = R.failure_message failure in
      if String.contains line '\n' || String.contains line '\r' then
        failwith "hostile ID: failure_message spans several lines";
      if String.contains line '\t' || String.contains line '\027' then
        failwith "hostile ID: failure_message contains a raw control byte";
      if String.length line > 3_072 then
        failwith
          (Printf.sprintf "hostile ID: failure_message is %d bytes"
             (String.length line));
      if not (String.is_valid_utf_8 line) then
        failwith "hostile ID: failure_message is not valid UTF-8";
      List.iter
        (fun needle ->
          if not (contains line needle) then
            failwith ("hostile ID: failure_message lacks " ^ needle ^ ": " ^ line))
        [
          "ID multi\\nline\\r\\tid\\\\\\x1b\xc3\xa9www";
          "bytes truncated)";
          "recorded event 5 (TimerStarted)";
        ]
  | result -> failwith ("hostile ID: expected nondeterminism, got " ^ describe result)

(** Failing workflow code and a missing registration are task failures,
    distinct from nondeterminism and from invalid input. *)
let test_task_failures () =
  expect_task_failed "defect"
    (R.replay ~workflows:[ R.workflow (defective "body") ] (history "body"));
  expect_task_failed "unregistered type"
    (R.replay
       ~workflows:[ R.workflow (compatible "encoder") ]
       (history "body"))

(** Malformed input is rejected before or by the bridge, never as a verdict
    about workflow code. *)
let test_invalid_input () =
  let of_protobuf ?(workflow_id = "invalid") bytes =
    R.History.of_protobuf ~workflow_id bytes
  in
  let rejected label = function
    | Error _ -> ()
    | Ok _ -> failwith (label ^ ": invalid history input was accepted")
  in
  rejected "empty history" (of_protobuf "");
  rejected "empty workflow ID" (of_protobuf ~workflow_id:"" "x");
  rejected "NUL workflow ID" (of_protobuf ~workflow_id:"a\000b" "x");
  rejected "non-UTF-8 workflow ID" (of_protobuf ~workflow_id:"\xff" "x");
  rejected "oversized workflow ID"
    (of_protobuf ~workflow_id:(String.make 65_537 'w') "x");
  (* One byte over [max_bytes] (128 MiB + 1) is rejected by the size check
     itself, before any base64 or JSON copy is made (#497). The allocation is
     transient; platforms whose strings cannot be that long skip the case. *)
  if R.History.max_bytes < Sys.max_string_length then (
    match of_protobuf (String.make (R.History.max_bytes + 1) '\000') with
    | Ok _ -> failwith "oversized history: accepted"
    | Error error ->
        let expected =
          "replay history exceeds Temporal.Replay.History.max_bytes"
        in
        if Temporal.Error.message error <> expected then
          failwith
            ("oversized history: unexpected error "
            ^ Temporal.Error.message error));
  let replay_bytes label bytes =
    match of_protobuf bytes with
    | Error error -> failwith (label ^ ": " ^ Temporal.Error.message error)
    | Ok history ->
        R.replay ~workflows:[ R.workflow (compatible "body") ] history
  in
  expect_invalid "not protobuf" (replay_bytes "not protobuf" "\xff\xff\xff\xff");
  let body = read_fixture "body" in
  expect_invalid "truncated"
    (replay_bytes "truncated" (String.sub body 0 (String.length body / 2)));
  (* A well-formed protobuf with no events fails Core's invariant checks. *)
  expect_invalid "no events" (replay_bytes "no events" "\x12\x00")

(** Requires an [Invalid_history] whose rendered line contains [fragment],
    so each negative case fails for its documented reason. *)
let expect_invalid_because label fragment result =
  expect_invalid label result;
  match result with
  | Error failure ->
      let line = R.failure_message failure in
      if not (contains line fragment) then
        failwith (label ^ ": expected " ^ fragment ^ " in " ^ line)
  | Ok () -> ()

(** Replays [bytes] as the [body] fixture's execution against [definition],
    which defaults to the compatible code. *)
let replay_body ?(workflow_id = "task-failure-body")
    ?(definition = compatible "body") bytes =
  match R.History.of_protobuf ~workflow_id bytes with
  | Error error -> failwith (Temporal.Error.message error)
  | Ok history -> R.replay ~workflows:[ R.workflow definition ] history

(** The supplied workflow ID must match the one the history records
    (issue #1046): replaying [body] under [encoder]'s ID is rejected before
    any workflow code runs, while the recorded ID and, for a history that
    records none, any ID are accepted. *)
let test_recorded_workflow_id () =
  let body = read_fixture "body" in
  expect_invalid_because "other execution's ID"
    "records a different workflow ID than the one supplied"
    (replay_body ~workflow_id:"task-failure-encoder" body);
  expect_invalid_because "forged ID"
    "records a different workflow ID than the one supplied"
    (replay_body ~workflow_id:"first\nPASS forged" body);
  expect_ok "recorded ID" (replay_body body);
  let unrecorded =
    map_started_attributes (Wire.drop History_fields.workflow_id) body
  in
  expect_ok "unrecorded ID" (replay_body ~workflow_id:"any-execution" unrecorded)

(** Histories that Core accepts structurally but cannot replay are
    [Invalid_history] promptly, whatever the registered code does
    (issues #1045 and #1047). Each case also replays against incompatible
    code: an invalid history must never look like a passing replay. *)
let test_core_rejected_histories () =
  let body = read_fixture "body" in
  let both label fragment bytes =
    expect_invalid_because label fragment (replay_body bytes);
    expect_invalid_because (label ^ " (changed code)") fragment
      (replay_body ~definition:(timer_removed "body") bytes)
  in
  (* A scrubber that deletes every eventTime (#1045). *)
  both "scrubbed timestamps" "WorkflowTaskStarted event without an event time"
    (map_events (fun _ -> Wire.drop History_fields.event_time) body);
  (* Event 3 is missing, so later IDs are out of order. *)
  both "event ID gap" "event IDs are not contiguous from 1"
    (map_events
       (fun index fields ->
         if index < 2 then fields
         else
           List.map
             (fun (field : Wire.field) ->
               if field.number = History_fields.event_id then
                 { field with value = Wire.varint (index + 2) }
               else field)
             fields)
       body);
  (* Core's replay poll would panic on the missing task queue and stall for
     the 30-second idle bound, ending in [Replay_error] (#1047). *)
  both "missing task queue" "start event has no task queue"
    (map_started_attributes (Wire.drop History_fields.task_queue) body);
  (* Event 2 (WorkflowTaskScheduled) without attributes passes the bridge's
     checks, but Core cannot apply it inside the first workflow task. Core
     auto-fails that task itself and evicts the run with reason Unspecified,
     which must be a refusal rather than housekeeping (#1045). *)
  both "first task auto-failed by Core" "invalid history: "
    (map_events
       (fun index fields ->
         if index = 1 then Wire.drop History_fields.task_scheduled_attributes fields
         else fields)
       body)

(** Registration problems are reported before native resources exist. *)
let test_registration_errors () =
  expect_replay_error "duplicate"
    (R.replay
       ~workflows:[ R.workflow (compatible "body"); R.workflow (compatible "body") ]
       (history "body"));
  expect_replay_error "remote"
    (R.replay
       ~workflows:
         [
           R.workflow
             (Temporal.Workflow.remote ~name:(workflow_type "body")
                ~input:Temporal.Codec.unit ~output:Temporal.Codec.string);
         ]
       (history "body"));
  expect_replay_error "invalid task queue"
    (R.replay ~task_queue:"" ~workflows:[ R.workflow (compatible "body") ]
       (history "body"))

(** [replay_all] keeps every history's own verdict in input order. *)
let test_replay_all () =
  let workflows = [ R.workflow (timer_removed "body"); R.workflow (compatible "encoder") ] in
  match R.replay_all ~workflows [ history "body"; history "encoder" ] with
  | [ (first, first_result); (second, second_result) ] ->
      if R.History.workflow_id first <> "task-failure-body" then
        failwith "replay_all reordered histories";
      if R.History.workflow_id second <> "task-failure-encoder" then
        failwith "replay_all reordered histories";
      expect_nondeterminism "replay_all body" first_result;
      expect_ok "replay_all encoder" second_result
  | _ -> failwith "replay_all returned the wrong number of results"

(** More sequential replays than OCaml permits simultaneous Domains (128).
    Each call spawns a supervisor Domain, so a leaked graph would make a
    later call fail to start; alternating verdicts covers both cleanup
    paths. *)
let test_repeated_cleanup () =
  let compatible_workflows = [ R.workflow (compatible "body") ] in
  let changed_workflows = [ R.workflow (timer_removed "body") ] in
  let body = history "body" in
  for iteration = 1 to 140 do
    if iteration mod 2 = 0 then
      expect_ok "repeated compatible" (R.replay ~workflows:compatible_workflows body)
    else
      expect_nondeterminism "repeated changed"
        (R.replay ~workflows:changed_workflows body)
  done

let () =
  test_compatible_histories ();
  test_incompatible_changes ();
  test_hostile_workflow_id ();
  test_task_failures ();
  test_invalid_input ();
  test_recorded_workflow_id ();
  test_core_rejected_histories ();
  test_registration_errors ();
  test_replay_all ();
  test_repeated_cleanup ()
