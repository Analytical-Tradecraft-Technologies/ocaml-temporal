(** Reads a corpus history's recorded identity directly from the binary
    protobuf bytes that the runner replays.

    The public [Temporal.Replay.replay] returns only [Ok ()] on success, so the
    workflow type and run ID that a replay is attributed to must be proven
    from the replay input itself. Checking a separate JSON copy is not enough:
    a JSON file may be absent, or come from another run. No locked protobuf
    decoder for the Temporal API is reachable from OCaml test code (the
    project's protobuf handling lives in the Rust bridge), so this module walks
    only the few fields it needs. It does not need or validate the rest of the
    message; Core validates the full history during replay.

    Field numbers come from the Temporal API protos pinned by Temporal Core
    (crates/protos/protos/api_upstream/temporal/api at the locked Core
    revision):
    - [History.events = 1] (repeated [HistoryEvent]);
    - [HistoryEvent.event_type = 3] ([EVENT_TYPE_WORKFLOW_EXECUTION_STARTED]
      is [1]) and [HistoryEvent.workflow_execution_started_event_attributes =
      6];
    - [WorkflowExecutionStartedEventAttributes.workflow_type = 1] and
      [original_execution_run_id = 14];
    - [WorkflowType.name = 1].

    Temporal records a run's own ID as [original_execution_run_id] on its
    start event; [first_execution_run_id] names the first run of a
    continue-as-new chain instead. The manifest's [run_id] is the replayed
    run's own ID, so this module reads field 14. A Core upgrade that renumbers
    these fields would be a wire-format break of the Temporal API, which
    protobuf compatibility rules forbid. *)

(** Raised internally when the bytes do not have the expected shape. *)
exception Malformed of string

(** One decoded field. Only the shapes this walk needs are retained. *)
type field =
  | Varint of int  (** Wire type 0; values above [max_int] are rejected. *)
  | Bytes of string  (** Wire type 2: a nested message or a string. *)
  | Fixed  (** Wire types 1 and 5, skipped. *)

(** Reads a base-128 varint starting at [!position] in [data] and advances
    [position]. Varints longer than ten bytes, or overflowing a native int,
    are malformed. *)
let read_varint data position =
  let rec loop shift acc =
    if !position >= String.length data then raise (Malformed "truncated varint");
    if shift > 63 then raise (Malformed "varint is too long");
    let byte = Char.code data.[!position] in
    incr position;
    let acc = acc lor ((byte land 0x7f) lsl shift) in
    if acc < 0 then raise (Malformed "varint overflows");
    if byte land 0x80 = 0 then acc else loop (shift + 7) acc
  in
  loop 0 0

(** Decodes the top-level fields of one message, in wire order, as
    [(field number, field)] pairs. Groups (wire types 3 and 4) are not used by
    the Temporal API and are rejected. *)
let fields data =
  let position = ref 0 in
  let length = String.length data in
  let skip count =
    if !position + count > length then raise (Malformed "truncated field");
    position := !position + count
  in
  let rec loop acc =
    if !position >= length then List.rev acc
    else
      let key = read_varint data position in
      let number = key lsr 3 in
      if number = 0 then raise (Malformed "field number 0");
      let field =
        match key land 7 with
        | 0 -> Varint (read_varint data position)
        | 1 ->
            skip 8;
            Fixed
        | 2 ->
            let size = read_varint data position in
            let start = !position in
            skip size;
            Bytes (String.sub data start size)
        | 5 ->
            skip 4;
            Fixed
        | wire -> raise (Malformed (Printf.sprintf "unsupported wire type %d" wire))
      in
      loop ((number, field) :: acc)
  in
  loop []

(** Returns the last length-delimited value of field [number], which is the
    protobuf rule for a repeated singular field, or fails naming [what]. *)
let last_bytes what number fields =
  match
    List.filter_map
      (function n, Bytes value when n = number -> Some value | _ -> None)
      fields
    |> List.rev
  with
  | value :: _ -> value
  | [] -> raise (Malformed ("missing " ^ what))

(** Returns the first value of the repeated field [number]. *)
let first_bytes what number fields =
  match
    List.find_map
      (function n, Bytes value when n = number -> Some value | _ -> None)
      fields
  with
  | Some value -> value
  | None -> raise (Malformed ("missing " ^ what))

(** Returns [(workflow_type, run_id)] from the first event of a binary
    [temporal.api.history.v1.History], or an error naming what is missing.
    The first event must be [WorkflowExecutionStarted]. *)
let of_protobuf data =
  match
    let event = fields (first_bytes "History.events" 1 (fields data)) in
    (match List.assoc_opt 3 event with
    | Some (Varint 1) -> ()
    | _ -> raise (Malformed "first event is not WorkflowExecutionStarted"));
    let attributes =
      fields
        (last_bytes "workflow_execution_started_event_attributes" 6 event)
    in
    let workflow_type =
      fields (last_bytes "workflow_type" 1 attributes)
      |> last_bytes "WorkflowType.name" 1
    in
    let run_id = last_bytes "original_execution_run_id" 14 attributes in
    (workflow_type, run_id)
  with
  | identity -> Ok identity
  | exception Malformed message -> Error message
