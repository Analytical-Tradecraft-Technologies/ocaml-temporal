(** A server-free workflow worker for the memory and fan-out benchmarks (#527,
    #528).

    The production native-worker adapter ({!Temporal_runtime.Native_worker_execution})
    is instantiated with an in-memory source in place of the native
    supervisor. The source holds activation documents already encoded as the
    canonical bridge JSON, built before any timed phase, and performs the same
    OCaml-side boundary work as the supervisor: it copies the native bytes to a
    string and strictly decodes the activation on poll, and copies the single
    encoded completion into a fresh byte buffer on completion. Everything
    between those two points (translation, registry lookup, deterministic
    execution, command validation and completion encoding) is the unmodified
    production adapter. Rust, Temporal Core, the C stubs, the supervisor's
    owner Domain and mailbox, polling and the network are excluded. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Encoded = Temporal_protocol.Encoded_workflow_completion
module Worker = Temporal_runtime.Native_worker_execution
module Context = Temporal_runtime.Workflow_context_store
module Future = Temporal_runtime.Future_store
module Codec = Temporal_base.Codec

(** Returns elapsed monotonic nanoseconds since [counter] was started. *)
let elapsed_ns counter = Mtime.Span.to_float_ns (Mtime_clock.count counter)

type counters = {
  mutable activations : int;
  mutable activation_bytes : int;
  mutable decode_ns : float;
  mutable completions : int;
  mutable completion_bytes : int;
  mutable completion_copy_ns : float;
  mutable encode_ns : float;
  mutable poll_ns : float;
}
(** Boundary work observed by the source. [decode_ns] (native-bytes copy plus
    strict JSON decode) and [completion_copy_ns] are always recorded; the
    timer calls are a few tens of nanoseconds per activation. [encode_ns] and
    [poll_ns] are recorded only during an attribution pass (see
    {!attribute}). *)

(** Creates zeroed counters. *)
let fresh_counters () =
  {
    activations = 0;
    activation_bytes = 0;
    decode_ns = 0.;
    completions = 0;
    completion_bytes = 0;
    completion_copy_ns = 0.;
    encode_ns = 0.;
    poll_ns = 0.;
  }

(** The in-memory replacement for the native supervisor. *)
module Source = struct
  type t = {
    queue : bytes Queue.t;
    counters : counters;
    mutable attribute_encode : bool;
    mutable last_completion : Protocol.completion option;
  }
  (** One worker's queued activation documents and boundary counters. The
      benchmark Domain owns it exclusively; the adapter's poll mutex is never
      contended. [last_completion] holds only the most recent completion so a
      benchmark can validate it without retaining history. *)

  type error = string
  (** A decoding failure. Activations are produced by this benchmark, so any
      error is a fixture defect and fails the sample. *)

  (** Leases the next document, copying and decoding it exactly as the
      supervisor does with bytes received from Rust. *)
  let try_poll_workflow source =
    match Queue.take_opt source.queue with
    | None -> Ok None
    | Some wire -> (
        let started = Mtime_clock.counter () in
        let decoded = Protocol.decode_activation (Bytes.to_string wire) in
        source.counters.decode_ns <-
          source.counters.decode_ns +. elapsed_ns started;
        source.counters.activations <- source.counters.activations + 1;
        source.counters.activation_bytes <-
          source.counters.activation_bytes + Bytes.length wire;
        match decoded with
        | Ok activation -> Ok (Some activation)
        | Error error -> Error (Protocol.error_view error).message)

  (** Accepts one completion. The canonical bytes are copied once, as the
      supervisor copies them for the native call. During an attribution pass
      the typed completion is encoded a second time, only to time an
      equivalent encoder pass; that duplicate is never submitted and its time
      is subtracted from the enclosing poll by {!deliver}. *)
  let complete_workflow source ~(completion : Protocol.completion) encoded =
    let started = Mtime_clock.counter () in
    let submitted = Encoded.to_bytes encoded in
    source.counters.completion_copy_ns <-
      source.counters.completion_copy_ns +. elapsed_ns started;
    source.counters.completions <- source.counters.completions + 1;
    source.counters.completion_bytes <-
      source.counters.completion_bytes + Bytes.length submitted;
    if source.attribute_encode then (
      let started = Mtime_clock.counter () in
      ignore (Protocol.encode_completion completion);
      source.counters.encode_ns <- source.counters.encode_ns +. elapsed_ns started);
    source.last_completion <- Some completion;
    Ok ()

  (** One stable code: every source error is a benchmark fixture defect. *)
  let error_code _ = "benchmark_source"

  (** The decoder's bounded, payload-free diagnostic. *)
  let error_message error = error

  (** Completions are accepted unconditionally, so nothing is retried. *)
  let error_is_retryable _ = false

  (** The source never raises from [complete_workflow]. *)
  let exception_is_retryable _ = false
end

module Adapter = Worker.Make (Source)
(** The unmodified production adapter over the in-memory source. *)

type t = { source : Source.t; registry : Adapter.t }
(** One synthetic worker: its source and the production registry. *)

(** Creates a worker for [workflows]. Configuration errors are benchmark
    defects. *)
let create workflows =
  let source =
    {
      Source.queue = Queue.create ();
      counters = fresh_counters ();
      attribute_encode = false;
      last_completion = None;
    }
  in
  match
    Adapter.create ~task_queue:"benchmark" ~supervisor:source ~workflows ()
  with
  | Ok registry -> { source; registry }
  | Error error -> failwith error.message

(** Delivers one pre-encoded activation, polls it through the adapter, and
    returns the commands of the completion it produced. A rejection or
    adapter error fails the sample. During an attribution pass the poll's
    duration, minus the duplicate encoder pass, is accumulated. *)
let deliver worker wire =
  Queue.push wire worker.source.queue;
  worker.source.last_completion <- None;
  let counters = worker.source.counters in
  let encode_before = counters.encode_ns in
  let started = Mtime_clock.counter () in
  let outcome = Adapter.poll worker.registry in
  if worker.source.attribute_encode then
    counters.poll_ns <-
      counters.poll_ns +. elapsed_ns started
      -. (counters.encode_ns -. encode_before);
  match (outcome, worker.source.last_completion) with
  | Ok (Worker.Completed _), Some completion -> completion.commands
  | Ok (Worker.Completed _), None -> failwith "adapter completed without a source call"
  | Ok Worker.Not_ready, _ -> failwith "adapter did not lease the activation"
  | Ok (Worker.Rejected { error; _ }), _ ->
      failwith ("adapter rejected a benchmark activation: " ^ error.message)
  | Error error, _ -> failwith error.message

(** Releases every execution still held by the registry. Callers first evict
    resident runs through ordinary eviction activations where they model
    Core's shutdown; this is the terminal cleanup path. *)
let discard worker = Adapter.discard worker.registry

(** Runs [f] as an attribution pass: counters are reset, the duplicate
    completion encoding is enabled, and the boundary breakdown is returned as
    report fields. [units] names the work [f] performs (for example one
    fan-out sample) so per-unit figures can be derived. *)
let attribute worker ~units f =
  let c = worker.source.counters in
  (* Reset in place: [deliver] and the source share this one record. *)
  c.activations <- 0;
  c.activation_bytes <- 0;
  c.decode_ns <- 0.;
  c.completions <- 0;
  c.completion_bytes <- 0;
  c.completion_copy_ns <- 0.;
  c.encode_ns <- 0.;
  c.poll_ns <- 0.;
  worker.source.attribute_encode <- true;
  Fun.protect
    ~finally:(fun () -> worker.source.attribute_encode <- false)
    (fun () ->
      for _ = 1 to units do
        f ()
      done);
  let bridge = c.decode_ns +. c.encode_ns +. c.completion_copy_ns in
  let share part = if c.poll_ns <= 0. then 0. else part /. c.poll_ns in
  let per_unit value = value /. float_of_int units /. 1_000. in
  [
    ("units", `Int units);
    ("activations", `Int c.activations);
    ("activation_json_bytes", `Int c.activation_bytes);
    ("completions", `Int c.completions);
    ("completion_json_bytes", `Int c.completion_bytes);
    ("adapter_poll_us_per_unit", `Float (per_unit c.poll_ns));
    ("activation_decode_us_per_unit", `Float (per_unit c.decode_ns));
    ("completion_encode_us_per_unit", `Float (per_unit c.encode_ns));
    ("completion_copy_us_per_unit", `Float (per_unit c.completion_copy_ns));
    ( "workflow_and_adapter_us_per_unit",
      `Float (per_unit (c.poll_ns -. bridge)) );
    ("activation_decode_share", `Float (share c.decode_ns));
    ("completion_encode_share", `Float (share c.encode_ns));
    ("json_bridge_share", `Float (share bridge));
  ]

(** Monotonic timestamp used in every ordinary benchmark activation. *)
let timestamp : Protocol.timestamp = { seconds = 1_700_000_000L; nanoseconds = 0 }

(** Converts a base payload to the binary-safe protocol representation. *)
let protocol_payload (payload : Temporal_base.Payload.t) : Protocol.payload =
  {
    Protocol.metadata =
      List.map (fun (key, value) -> (key, Bytes.of_string value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** A JSON string payload whose encoded data is exactly [bytes] long (minimum
    two, the quotes). Workflow code decodes it with [Codec.string]. *)
let string_payload bytes =
  let value = String.make (max 0 (bytes - 2)) 'x' in
  match Codec.encode Codec.string value with
  | Ok payload -> payload
  | Error _ -> failwith "benchmark payload encoding failed"

(** Builds an activation envelope. Eviction activations omit the timestamp,
    as Core does. *)
let activation ~run_id ~replaying ~history_length jobs : Protocol.activation =
  let evicting =
    List.exists (function Protocol.Remove_from_cache _ -> true | _ -> false) jobs
  in
  {
    run_id;
    timestamp = (if evicting then None else Some timestamp);
    is_replaying = replaying;
    history_length;
    jobs;
    metadata = None;
  }

(** A unit-input workflow start job for [workflow_type]. *)
let initialize ~run_id ~workflow_type : Protocol.activation_job =
  Protocol.Initialize_workflow
    {
      workflow_id = "benchmark-" ^ run_id;
      workflow_type;
      arguments = [];
      randomness_seed = "1";
      attempt = 1;
      context = None;
    }

(** A successful activity resolution carrying [payload]. *)
let resolve_activity ~payload seq : Protocol.activation_job =
  Protocol.Resolve_activity { seq; result = Completed (Some payload) }

(** A Core cache-removal job with a fixed message. *)
let remove_from_cache reason : Protocol.activation_job =
  Protocol.Remove_from_cache { message = "benchmark eviction"; reason }

(** Encodes an activation to the canonical bridge bytes before timing. *)
let encode activation =
  match Protocol.encode_activation activation with
  | Ok wire -> Bytes.of_string wire
  | Error error -> failwith (Protocol.error_view error).message

(** Returns the current workflow context or a typed defect. *)
let current_context () =
  match Context.current () with
  | Some context -> Ok context
  | None ->
      Error
        (Temporal_base.Error.defect
           ~message:"benchmark workflow ran outside a workflow execution")

(** Schedules one remote activity with a string argument of [payload_bytes]
    encoded bytes, returning its result future. *)
let schedule_activity context ~payload_bytes =
  let input = string_payload payload_bytes in
  fst
    (Context.schedule_activity context ~name:"benchmark_activity" ~input
       ~decode:(Codec.decode Codec.string) ())

(** Requires a completion to be exactly one activity schedule for [seq]. *)
let expect_schedule seq = function
  | [ Protocol.Schedule_activity { seq = actual; _ } ] when actual = seq -> ()
  | _ -> failwith (Printf.sprintf "expected one activity schedule seq %Ld" seq)

(** Requires a completion to be exactly one timer start for [seq]. *)
let expect_timer seq = function
  | [ Protocol.Start_timer { seq = actual; _ } ] when actual = seq -> ()
  | _ -> failwith (Printf.sprintf "expected one timer start seq %Ld" seq)

(** Requires a successful workflow completion. *)
let expect_complete = function
  | [ Protocol.Complete_workflow _ ] -> ()
  | _ -> failwith "expected the workflow to complete"

(** Requires an empty completion, as for an eviction acknowledgement or an
    activation that only resolves part of a fan-out. *)
let expect_empty = function
  | [] -> ()
  | _ -> failwith "expected an empty completion"

(** Returns reachable OCaml data in bytes. [Gc.stat] forces a full major
    collection, so this is used only outside timed phases. *)
let live_bytes () = (Gc.stat ()).live_words * (Sys.word_size / 8)

(** Reads a bounded positive integer from the environment, so suite-specific
    sizes travel through the shared Makefile command without extending the
    harness's common argument parser. *)
let env_count ~name ~default ~limit =
  match Sys.getenv_opt name with
  | None | Some "" -> default
  | Some text -> (
      match int_of_string_opt text with
      | Some value when value > 0 && value <= limit -> value
      | _ -> failwith (Printf.sprintf "%s must be in 1..%d" name limit))

(** Conservative per-item envelope allowance, in bytes, for one activity
    result job in an encoded activation: job tag, sequence number, result
    wrapper and base64 payload metadata. Measured jobs are well below it, so
    {!encoded_payload_estimate} over-approximates rather than under-counts. *)
let encoded_item_overhead_bytes = 512

(** Over-approximates the canonical activation JSON bytes of one job carrying
    a payload of [payload_bytes] raw bytes. Payload data crosses the bridge as
    padded base64, which expands every started 3-byte group to 4 bytes, plus
    {!encoded_item_overhead_bytes} of envelope. [payload_bytes] must be
    nonnegative and small enough that the expansion fits in [int]. *)
let encoded_payload_estimate payload_bytes =
  (4 * ((payload_bytes + 2) / 3)) + encoded_item_overhead_bytes

(** Checks that [items] fixture entries of [bytes_per_item] bytes each fit in
    [limit] bytes without computing the possibly overflowing product: the
    comparison divides [limit] instead, which is exact for nonnegative
    integers because [items * b <= limit] iff [b <= limit / items]. Returns a
    message naming [what] on rejection so a benchmark fails before it builds
    any fixture. *)
let check_fixture_bytes ~what ~items ~bytes_per_item ~limit =
  if items < 0 || bytes_per_item < 0 || limit < 0 then
    invalid_arg "check_fixture_bytes: negative argument"
  else if items = 0 || bytes_per_item <= limit / items then Ok ()
  else
    Error
      (Printf.sprintf
         "%s would materialize about %d items of %d encoded bytes, more than \
          the %d-byte fixture limit"
         what items bytes_per_item limit)
