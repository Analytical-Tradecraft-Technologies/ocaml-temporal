(** Deterministic malformations of committed corpus histories, used to build
    the [invalid_history] negative controls at test time (issue #503).

    A malformed-history control must fail because its input is broken, not
    because of some unrelated difference, so each control reuses a committed
    [replays_ok] history byte for byte and applies one named, documented edit
    to the bytes in memory just before replay. Nothing is written to disk: the
    corpus keeps no derived binary files whose provenance would have to be
    reviewed separately, and the manifest records the source entry
    ([negative_control_of]) and the edit ([history_mutation]) as the complete
    provenance of the replayed bytes. The same source bytes always produce the
    same output.

    The edits operate on the protobuf wire format directly, because no
    Temporal protobuf decoder is reachable from OCaml test code (see
    {!Corpus_history_identity}). Unlike that read-only walk, this module
    re-serializes messages, so it keeps every field's raw bytes, including
    fixed-width and unknown fields, and only rewrites the fields an edit
    names. Field numbers come from the Temporal API protos pinned by Temporal
    Core ([temporal/api/history/v1/message.proto]):
    - [History.events = 1];
    - [HistoryEvent.event_id = 1] and
      [HistoryEvent.workflow_execution_started_event_attributes = 6] and
      [HistoryEvent.workflow_task_scheduled_event_attributes = 10];
    - [WorkflowExecutionStartedEventAttributes.task_queue = 5].
    They match the edits in [test/bridge/test_public_replay.ml], which pins
    the same rejections against a single fixture. *)

(** One named edit. Each constructor's comment states the replay failure it
    is meant to provoke; the manifest's [expected_message] pins that reason
    for every control. *)
type t =
  | Truncate_protobuf
      (** Keeps only the first half of the serialized bytes, as a partially
          downloaded or cut-off export would. The cut lands inside an event,
          so the bytes are no longer a [History] protobuf. *)
  | Event_id_gap
      (** Renumbers every event from the third onwards one higher, so event
          ID 3 is missing and IDs are no longer contiguous from 1. *)
  | Drop_start_task_queue
      (** Removes [task_queue] from the [WorkflowExecutionStarted]
          attributes. Core's replay poll would panic on such a history, so
          the bridge's admission gate must refuse it first. *)
  | Drop_first_task_scheduled_attributes
      (** Removes the attributes of the second event (the first
          [WorkflowTaskScheduled]). The bridge's structural checks accept the
          history, but Core cannot apply the event inside the run's first
          workflow task, auto-fails that task itself and evicts the run. *)

(** Every edit, in a stable order, for diagnostics. *)
let all =
  [
    Truncate_protobuf; Event_id_gap; Drop_start_task_queue;
    Drop_first_task_scheduled_attributes;
  ]

(** The manifest spelling of an edit. *)
let to_string = function
  | Truncate_protobuf -> "truncate-protobuf"
  | Event_id_gap -> "event-id-gap"
  | Drop_start_task_queue -> "drop-start-task-queue"
  | Drop_first_task_scheduled_attributes ->
      "drop-first-workflow-task-scheduled-attributes"

(** Parses the manifest spelling of an edit. *)
let of_string value = List.find_opt (fun edit -> to_string edit = value) all

(** Raised when a source history does not have the shape an edit needs. The
    runner reports it as a case that could not be run. *)
exception Unsupported_source of string

(** One field with its raw payload: the varint bytes for wire type 0, the
    contents (without the length prefix) for wire type 2, and the fixed bytes
    for wire types 1 and 5. Keeping the raw bytes makes {!serialize} the exact
    inverse of {!parse} for every field an edit leaves alone. *)
type field = { number : int; wire_type : int; value : string }

(** Encodes a non-negative integer as a protobuf varint. *)
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

(** Splits one serialized message into its fields, in wire order. Uses the
    bounds-checked varint reader of {!Corpus_history_identity}, so a short or
    malformed source is reported rather than read past its end. *)
let parse data =
  let length = String.length data in
  let position = ref 0 in
  let take count =
    if !position + count > length then raise (Unsupported_source "truncated field");
    let value = String.sub data !position count in
    position := !position + count;
    value
  in
  let read_varint () =
    try Corpus_history_identity.read_varint data position
    with Corpus_history_identity.Malformed message ->
      raise (Unsupported_source message)
  in
  let rec loop acc =
    if !position >= length then List.rev acc
    else
      let key = read_varint () in
      let wire_type = key land 7 in
      let value =
        match wire_type with
        | 0 ->
            let start = !position in
            ignore (read_varint () : int);
            String.sub data start (!position - start)
        | 1 -> take 8
        | 2 -> take (read_varint ())
        | 5 -> take 4
        | other ->
            raise
              (Unsupported_source (Printf.sprintf "unsupported wire type %d" other))
      in
      loop ({ number = key lsr 3; wire_type; value } :: acc)
  in
  loop []

(** Serializes fields back into one message, recomputing length prefixes. *)
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

(** [History.events]. *)
let history_events = 1

(** [HistoryEvent.event_id]. *)
let event_id = 1

(** [HistoryEvent.workflow_execution_started_event_attributes]. *)
let started_attributes = 6

(** [HistoryEvent.workflow_task_scheduled_event_attributes]. *)
let task_scheduled_attributes = 10

(** [WorkflowExecutionStartedEventAttributes.task_queue]. *)
let task_queue = 5

(** Rewrites a serialized [History] by applying [edit index fields] to every
    event, where [index] counts events from 0 in history order. Fails if the
    history has fewer than [minimum_events] events, so an edit can never
    silently become a no-op on a shorter source. *)
let map_events ~minimum_events edit history =
  let index = ref 0 in
  let rewritten =
    parse history
    |> List.map (fun field ->
           if field.number <> history_events || field.wire_type <> 2 then field
           else
             let event_index = !index in
             incr index;
             { field with value = serialize (edit event_index (parse field.value)) })
    |> serialize
  in
  if !index < minimum_events then
    raise
      (Unsupported_source
         (Printf.sprintf "history has %d event(s), the edit needs %d" !index
            minimum_events));
  rewritten

(** Applies [edit] to the fields of every embedded message [number] of
    [fields], failing when there is none so the edit cannot be a no-op. *)
let map_message ~what number edit fields =
  if not (List.exists (fun field -> field.number = number) fields) then
    raise (Unsupported_source ("missing " ^ what));
  List.map
    (fun field ->
      if field.number = number then
        { field with value = serialize (edit (parse field.value)) }
      else field)
    fields

(** Applies [mutation] to the serialized history [source]. The result always
    differs from [source]; an edit whose target is absent is reported as
    [Error] rather than returning the input unchanged. *)
let apply mutation source =
  match
    match mutation with
    | Truncate_protobuf ->
        if String.length source < 2 then
          raise (Unsupported_source "history is too short to truncate");
        String.sub source 0 (String.length source / 2)
    | Event_id_gap ->
        map_events ~minimum_events:3
          (fun index fields ->
            if index < 2 then fields
            else
              List.map
                (fun field ->
                  if field.number = event_id && field.wire_type = 0 then
                    { field with value = varint (index + 2) }
                  else field)
                fields)
          source
    | Drop_start_task_queue ->
        map_events ~minimum_events:1
          (fun index fields ->
            if index <> 0 then fields
            else
              map_message ~what:"workflow_execution_started_event_attributes"
                started_attributes
                (fun attributes ->
                  if not (List.exists (fun f -> f.number = task_queue) attributes)
                  then raise (Unsupported_source "start event has no task queue");
                  drop task_queue attributes)
                fields)
          source
    | Drop_first_task_scheduled_attributes ->
        map_events ~minimum_events:2
          (fun index fields ->
            if index <> 1 then fields
            else if
              not
                (List.exists
                   (fun field -> field.number = task_scheduled_attributes)
                   fields)
            then
              raise
                (Unsupported_source
                   "second event is not a WorkflowTaskScheduled event")
            else drop task_scheduled_attributes fields)
          source
  with
  | mutated -> Ok mutated
  | exception Unsupported_source message ->
      Error (Printf.sprintf "cannot apply %s: %s" (to_string mutation) message)
