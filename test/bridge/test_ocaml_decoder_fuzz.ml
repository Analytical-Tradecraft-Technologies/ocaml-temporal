(** Bounded, reproducible mutation fuzzing of the OCaml protocol decoders
    (issue #506).

    These decoders read every document the Rust bridge returns: control
    envelopes and payloads, workflow activations and completions, activity
    tasks, completions and heartbeats, and every client response and error
    body. Each target owns one or more corpus directories under
    [fixtures/]. The harness mutates their documents with a seeded stream of
    structural operators and requires, for every input:

    - no exception escapes the decoder (including [Stack_overflow] and
      [Out_of_memory]), so every failure is a typed [Error];
    - the bytes allocated by one decode stay below a fixed allowance plus a
      fixed multiple of the input length;
    - an accepted document re-encodes and decodes again, where the module
      provides an encoder.

    The seed and budget come from [DECODER_FUZZ_SEED] and
    [DECODER_FUZZ_OCAML_CASES]; see [docs/reference/bridge-decoder-fuzz.md].
    The Rust-side decoders are covered by
    [rust/core-bridge/tests/decoder_fuzz.rs]. *)

module Control = Temporal_protocol.Control_protocol
module Workflow = Temporal_protocol.Workflow_protocol
module Activity = Temporal_protocol.Activity_protocol
module Client = Temporal_protocol.Client_protocol

(** Default number of generated cases per target in the ordinary test run. *)
let default_cases = 96

(** Upper bound on [DECODER_FUZZ_OCAML_CASES]. *)
let max_cases = 50_000

(** Default seed, shared in spirit with the Rust harness's default. *)
let default_seed = 0x0506d3c0L

(** Largest generated input. The oversized-string operator needs the 64 KiB
    string limit plus a seed document. *)
let max_input_bytes = 128 * 1024

(** Fixed allocation allowance per decode, in bytes. *)
let allocation_allowance = 4 * 1024 * 1024

(** Allowed allocation per input byte. Strict decoding scans the text, builds
    a Yojson tree, validates it, and builds the typed value; each is a small
    multiple of the input, and OCaml counts every short-lived word. *)
let allocation_per_input_byte = 512

(** The protocol's string-length and nesting limits. They mirror the private
    constants of [Control_protocol] (and the Rust [protocol] module) so the
    oversized and deep-nesting operators straddle the real boundaries. *)
let max_string_bytes = 65_536

let max_depth = 128

(** Number of mutation operators; the first cases visit each once. *)
let operators = 15

(** Reads an integer environment setting within [low, high]. *)
let setting name default low high =
  match Sys.getenv_opt name with
  | None | Some "" -> default
  | Some text -> (
      match int_of_string_opt text with
      | Some value when value >= low && value <= high -> value
      | _ -> failwith (Printf.sprintf "%s must be an integer in %d..%d" name low high))

(** Parses the decimal or [0x] hexadecimal seed. *)
let base_seed () =
  match Sys.getenv_opt "DECODER_FUZZ_SEED" with
  | None | Some "" -> default_seed
  | Some text -> (
      match Int64.of_string_opt text with
      | Some seed -> seed
      | None -> failwith "DECODER_FUZZ_SEED must be a decimal or 0x hexadecimal integer")

(** Deterministic xorshift stream over a mutable 64-bit state. *)
type rng = { mutable state : int64 }

(** Derives an independent stream for one target from the base seed. *)
let rng_for seed name =
  let state = ref (Int64.logxor seed 0x9e3779b97f4a7c15L) in
  String.iter
    (fun char ->
      state :=
        Int64.mul (Int64.logxor !state (Int64.of_int (Char.code char))) 0x100000001b3L)
    name;
  { state = (if Int64.equal !state 0L then default_seed else !state) }

(** Advances the stream. *)
let next rng =
  let x = rng.state in
  let x = Int64.logxor x (Int64.shift_left x 13) in
  let x = Int64.logxor x (Int64.shift_right_logical x 7) in
  let x = Int64.logxor x (Int64.shift_left x 17) in
  rng.state <- x;
  x

(** Index in [0, bound); [bound] must be positive. *)
let below rng bound =
  Int64.to_int (Int64.unsigned_rem (next rng) (Int64.of_int bound))

(** Content spans [(start, stop)] of every JSON string literal, found by an
    escape-aware scan that tolerates malformed input. *)
let string_spans input =
  let length = String.length input in
  let rec outside index spans =
    if index >= length then List.rev spans
    else if input.[index] = '"' then inside (index + 1) (index + 1) spans
    else outside (index + 1) spans
  and inside start cursor spans =
    if cursor >= length then List.rev ((start, length) :: spans)
    else if input.[cursor] = '\\' then inside start (cursor + 2) spans
    else if input.[cursor] = '"' then outside (cursor + 1) ((start, cursor) :: spans)
    else inside start (cursor + 1) spans
  in
  Array.of_list (outside 0 [])

(** Spans of JSON number literals outside strings. *)
let number_spans input =
  let strings = string_spans input in
  let inside offset =
    Array.exists (fun (start, stop) -> offset + 1 >= start && offset <= stop) strings
  in
  let length = String.length input in
  let numeric char = (char >= '0' && char <= '9') || String.contains "-+.eE" char in
  let rec scan index spans =
    if index >= length then Array.of_list (List.rev spans)
    else
      let char = input.[index] in
      if ((char >= '0' && char <= '9') || char = '-') && not (inside index) then begin
        let stop = ref index in
        while !stop < length && numeric input.[!stop] do
          incr stop
        done;
        scan !stop ((index, !stop) :: spans)
      end
      else scan (index + 1) spans
  in
  scan 0 []

(** Replaces [start, stop) of [input] with [replacement]. *)
let splice input (start, stop) replacement =
  String.sub input 0 start ^ replacement
  ^ String.sub input stop (String.length input - stop)

(** Inserts [field] as the first member of the first object. *)
let insert_field input field =
  match String.index_opt input '{' with
  | None -> field ^ input
  | Some brace -> splice input (brace + 1, brace + 1) (field ^ ",")

(** Picks one element of a nonempty array. *)
let pick rng values = values.(below rng (Array.length values))

(** Picks one span, or [None] when there is none. *)
let pick_span rng spans =
  if Array.length spans = 0 then None else Some (pick rng spans)

(** Integer, float, and range boundaries. *)
let boundary_numbers =
  [| "0"; "-1"; "-0"; "1.5"; "1e309"; "-1e309"; "4294967296"; "9223372036854775807";
     "9223372036854775808"; "-9223372036854775809"; "18446744073709551616"; "00";
     "4611686018427387903"; "4611686018427387904" |]

(** Values that confuse the expected JSON type. *)
let confused_values = [| "null"; "true"; "[]"; "{}"; "\"\""; "0"; "[null]" |]

(** Valid escapes hostile to UTF-8 conversion or identifier checks. *)
let hostile_escapes = [| "\\ud800"; "\\udfff"; "\\u0000"; "\\udbff\\udfff"; "\\u202e" |]

(** Applies one operator. [donor] is another seed for crossover. Every
    operator is total and changes the input. *)
let rec apply operator input donor rng =
  let length = String.length input in
  if length = 0 then "{"
  else
    let strings = string_spans input in
    let numbers = number_spans input in
    match operator with
    | 0 -> String.sub input 0 (below rng length)
    | 1 ->
        let start = below rng length in
        let stop = min length (start + 1 + below rng 32) in
        splice input (start, stop) ""
    | 2 ->
        let bytes = "{}[]\",:\\0-en \000\127" in
        let offset = below rng length in
        splice input (offset, offset + 1) (String.make 1 bytes.[below rng (String.length bytes)])
    | 3 ->
        let invalid = [| "\xff"; "\xc0\x80"; "\xed\xa0\x80"; "\x80" |] in
        let offset = below rng (length + 1) in
        splice input (offset, offset) (pick rng invalid)
    | 4 -> (
        match pick_span rng (Array.sub strings 0 (min 1 (Array.length strings))) with
        | Some (start, stop) ->
            insert_field input ("\"" ^ String.sub input start (stop - start) ^ "\":null")
        | None -> insert_field input "\"a\":1,\"a\":1")
    | 5 -> (
        let oversized = String.make (max_string_bytes + below rng 3) 'a' in
        match pick_span rng strings with
        | Some span -> splice input span oversized
        | None -> insert_field input ("\"s\":\"" ^ oversized ^ "\""))
    | 6 -> (
        (* Nesting near the limit, and occasionally far beyond it to check
           that the preflight scan rejects it before recursive parsing. *)
        let depth =
          if below rng 4 = 0 then 20_000 else max_depth - 1 + below rng 4
        in
        let nested = String.make depth '[' ^ "0" ^ String.make depth ']' in
        match pick_span rng strings with
        | Some (start, stop) when start > 0 && stop < length ->
            splice input (start - 1, stop + 1) nested
        | _ -> insert_field input ("\"n\":" ^ nested))
    | 7 -> (
        let value = pick rng boundary_numbers in
        match pick_span rng numbers with
        | Some span -> splice input span value
        | None -> insert_field input ("\"n\":" ^ value))
    | 8 -> (
        let value = pick rng confused_values in
        let quoted =
          Array.to_list strings
          |> List.filter (fun (start, stop) -> start > 0 && stop < length)
          |> List.map (fun (start, stop) -> (start - 1, stop + 1))
        in
        match pick_span rng (Array.append (Array.of_list quoted) numbers) with
        | Some span -> splice input span value
        | None -> value)
    | 9 -> (
        let escape = pick rng hostile_escapes in
        match pick_span rng strings with
        | Some (start, _) -> splice input (start, start) escape
        | None -> insert_field input ("\"u\":\"" ^ escape ^ "\""))
    | 10 -> (
        (* Base64-alphabet corruption inside a nonempty string. *)
        let nonempty = Array.of_list (List.filter (fun (a, b) -> b > a) (Array.to_list strings)) in
        match pick_span rng nonempty with
        | Some (start, stop) ->
            let offset = start + below rng (stop - start) in
            let bytes = "=!-_ A/" in
            splice input (offset, offset + 1)
              (String.make 1 bytes.[below rng (String.length bytes)])
        | None -> apply 2 input donor rng)
    | 11 ->
        let head = below rng (length + 1) in
        let tail = below rng (String.length donor + 1) in
        String.sub input 0 head ^ String.sub donor tail (String.length donor - tail)
    | 12 ->
        let start = below rng length in
        let stop = min length (start + 1 + below rng 64) in
        splice input (stop, stop) (String.sub input start (stop - start))
    | 13 -> (
        let words = [| "a"; "default"; "run-2"; "x.y_z"; "0"; "AAEC/v8=" |] in
        let small = [| "1"; "2"; "3"; "10"; "100"; "65536" |] in
        if Array.length numbers = 0 || (Array.length strings > 0 && below rng 2 = 0) then
          match pick_span rng strings with
          | Some span -> splice input span (pick rng words)
          | None -> apply 12 input donor rng
        else splice input (pick rng numbers) (pick rng small))
    | _ ->
        let output = ref input in
        for _ = 1 to 2 + below rng 3 do
          let next = apply (below rng (operators - 1)) !output donor rng in
          output :=
            if String.length next > max_input_bytes then String.sub next 0 max_input_bytes
            else next
        done;
        !output

(** A decoder under test. [decode] returns [true] when the document was
    accepted (and round-tripped where possible) and [false] on a typed
    rejection. *)
type target = {
  name : string;
  corpora : string list;  (** directories whose files seed this target *)
  strict : bool;  (** whether [valid/] must accept and [invalid/] reject *)
  decode : string -> bool;
}

(** Accepts [Ok] after checking the round trip; a re-encoding or re-decoding
    failure is a defect, so it raises and is reported. *)
let accepted ~encode ~decode = function
  | Error _ -> false
  | Ok value -> (
      match encode value with
      | Error _ -> failwith "accepted value does not re-encode"
      | Ok text -> (
          match decode text with
          | Ok _ -> true
          | Error _ -> failwith "re-encoded value does not decode"))

(** Accepts [Ok] for decoders without an encoder. *)
let ok = function Ok _ -> true | Error _ -> false

(** The execution named by every client response seed. *)
let execution : Client.execution =
  { namespace = "default"; workflow_id = "summarize-1"; run_id = "server-assigned-run-id" }

(** The start request that the start response seeds correlate with. *)
let start_request : Client.start_request =
  {
    request_id = "start-summarize-1";
    namespace = "default";
    workflow_id = "summarize-1";
    workflow_type = "summarize_document";
    task_queue = "agents";
    input = [];
    memo = [];
    search_attributes = [];
    id_conflict_policy = Fail;
    id_reuse_policy = Allow_duplicate;
    execution_timeout_ms = None;
    run_timeout_ms = None;
    task_timeout_ms = None;
    retry_policy = None;
    rpc_deadline = None;
  }

(** The reset request that the reset response seeds correlate with. *)
let reset_request : Client.reset_request =
  {
    execution;
    request_id = "reset-summarize-1-4";
    reason = "fuzz";
    workflow_task_finish_event_id = 4L;
    rpc_deadline = None;
  }

(** The update request that the update response seeds correlate with. *)
let update_request : Client.update_request =
  { execution; update_id = "update-1"; update_name = "set_state"; input = []; rpc_deadline = None }

(** Every OCaml decoder target. *)
let targets =
  let client name dirs decode =
    { name; corpora = List.map (fun dir -> "decoder-fuzz/ocaml-client/" ^ dir) dirs; strict = true; decode }
  in
  let errors = [ "client-error" ] in
  [
    {
      name = "control-envelope";
      corpora = [ "protocol" ];
      strict = false;
      decode = (fun input -> accepted ~encode:Control.encode ~decode:Control.decode (Control.decode input));
    };
    {
      name = "control-payload";
      corpora = [ "protocol" ];
      strict = false;
      decode =
        (fun input ->
          accepted ~encode:Control.encode_payload ~decode:Control.decode_payload
            (Control.decode_payload input));
    };
    {
      name = "workflow-activation";
      corpora = [ "workflow-protocol" ];
      strict = false;
      decode =
        (fun input ->
          accepted ~encode:Workflow.encode_activation ~decode:Workflow.decode_activation
            (Workflow.decode_activation input));
    };
    {
      name = "workflow-completion";
      corpora = [ "workflow-protocol" ];
      strict = false;
      decode =
        (fun input ->
          accepted ~encode:Workflow.encode_completion ~decode:Workflow.decode_completion
            (Workflow.decode_completion input));
    };
    {
      name = "activity-task";
      corpora = [ "decoder-fuzz/activity-task" ];
      strict = true;
      decode =
        (fun input ->
          accepted ~encode:Activity.encode_task ~decode:Activity.decode_task
            (Activity.decode_task input));
    };
    {
      name = "activity-completion";
      corpora = [ "decoder-fuzz/activity-completion" ];
      strict = true;
      decode =
        (fun input ->
          accepted ~encode:Activity.encode_completion ~decode:Activity.decode_completion
            (Activity.decode_completion input));
    };
    {
      name = "activity-heartbeat";
      corpora = [ "decoder-fuzz/activity-heartbeat" ];
      strict = true;
      decode =
        (fun input ->
          accepted ~encode:Activity.encode_heartbeat ~decode:Activity.decode_heartbeat
            (Activity.decode_heartbeat input));
    };
    client "start-response" [ "start-response" ] (fun input ->
        ok (Client.decode_start_response ~request:start_request input));
    client "start-outcome" [ "start-outcome" ] (fun input ->
        match Client.decode_start_outcome ~request:start_request input with
        | Error _ -> false
        | Ok outcome -> (
            match Client.encode_start_outcome outcome with
            | Ok text -> ok (Client.decode_start_outcome ~request:start_request text)
            | Error _ -> failwith "accepted start outcome does not re-encode"));
    client "start-ticket" [ "start-ticket" ] (fun input ->
        ok (Client.decode_start_ticket ~request:start_request input));
    client "cancel-response" [ "acknowledgement" ] (fun input ->
        ok (Client.decode_cancel_response input));
    client "terminate-response" [ "acknowledgement" ] (fun input ->
        ok (Client.decode_terminate_response input));
    client "signal-response" [ "acknowledgement" ] (fun input ->
        ok (Client.decode_signal_response input));
    client "reset-response" [ "reset-response" ] (fun input ->
        ok (Client.decode_reset_response ~request:reset_request input));
    client "query-response" [ "query-response" ] (fun input ->
        ok (Client.decode_query_response input));
    client "visibility-response" [ "visibility-response" ] (fun input ->
        ok (Client.decode_visibility_response input));
    client "update-response" [ "update-response" ] (fun input ->
        ok (Client.decode_update_response ~request:update_request input));
    client "poll-update-response" [ "poll-update-response" ] (fun input ->
        ok (Client.decode_poll_update_response input));
    client "wait-response" [ "wait-response" ] (fun input ->
        ok (Client.decode_wait_response ~request:execution input));
    client "client-error" errors (fun input -> ok (Client.decode_client_error input));
    (* The operation-specific error decoders accept different categories,
       so their shared corpus is not strict. *)
    { (client "start-error" errors (fun input ->
           ok (Client.decode_start_error ~request:start_request input))) with strict = false };
    { (client "wait-error" errors (fun input ->
           ok (Client.decode_wait_error ~request:execution input))) with strict = false };
    { (client "query-error" errors (fun input -> ok (Client.decode_query_error input))) with
      strict = false };
    { (client "update-error" errors (fun input -> ok (Client.decode_update_error input))) with
      strict = false };
  ]

(** Reads a whole file. *)
let read_file path = In_channel.with_open_bin path In_channel.input_all

(** Lists the regular files of [dir] in name order, or none if absent. *)
let files dir =
  if Sys.file_exists dir && Sys.is_directory dir then
    Sys.readdir dir |> Array.to_list |> List.sort String.compare
    |> List.map (Filename.concat dir)
    |> List.filter (fun path -> not (Sys.is_directory path))
  else []

(** Seed documents of one target as [(label, kind, text)], where [kind] is
    [`Valid] for files under [valid/], [`Invalid] under [invalid/] or
    [regressions/]. *)
let corpus target =
  List.concat_map
    (fun dir ->
      let root = Filename.concat "fixtures" dir in
      List.concat_map
        (fun (sub, kind) ->
          List.map (fun path -> (path, kind, read_file path)) (files (Filename.concat root sub)))
        [ ("valid", `Valid); ("invalid", `Invalid); ("regressions", `Invalid) ])
    target.corpora

(** Outcome of one decode, or the violated property. *)
let execute target input =
  let before = Gc.allocated_bytes () in
  let result =
    try Ok (target.decode input) with
    | Stack_overflow -> Error "Stack_overflow escaped the decoder"
    | Out_of_memory -> Error "Out_of_memory escaped the decoder"
    | exn -> Error ("exception escaped the decoder: " ^ Printexc.to_string exn)
  in
  let allocated = int_of_float (Gc.allocated_bytes () -. before) in
  let allowance = allocation_allowance + (allocation_per_input_byte * String.length input) in
  match result with
  | Error _ as error -> (error, allocated)
  | Ok _ when allocated > allowance ->
      (Error (Printf.sprintf "allocated %d bytes, allowance %d" allocated allowance), allocated)
  | Ok accepted -> (Ok accepted, allocated)

(** Deletes chunks while [fails] still holds, for at most 512 attempts. *)
let minimize input fails =
  let input = ref input and chunk = ref (String.length input / 2) and attempts = ref 0 in
  while !chunk > 0 && !attempts < 512 do
    let reduced = ref false and start = ref 0 in
    while (not !reduced) && !start < String.length !input && !attempts < 512 do
      let stop = min (String.length !input) (!start + !chunk) in
      let candidate = splice !input (!start, stop) "" in
      incr attempts;
      if fails candidate then begin
        input := candidate;
        reduced := true
      end
      else start := !start + !chunk
    done;
    if not !reduced then chunk := !chunk / 2
  done;
  !input

(** Writes a reproduction next to the test (or to [DECODER_FUZZ_OUTPUT_DIR])
    and fails with its location and an escaped copy of the bytes. *)
let report target label violation input =
  let minimized = minimize input (fun candidate -> Result.is_error (fst (execute target candidate))) in
  let dir =
    match Sys.getenv_opt "DECODER_FUZZ_OUTPUT_DIR" with
    | Some dir when dir <> "" -> dir
    | _ -> Sys.getcwd ()
  in
  let path = Filename.concat dir (Printf.sprintf "ocaml-%s-%s.bin" target.name label) in
  (try Out_channel.with_open_bin path (fun channel -> output_string channel minimized)
   with Sys_error _ -> ());
  failwith
    (Printf.sprintf "%s %s: %s; minimized (%d of %d bytes) written to %s: %S" target.name label
       violation (String.length minimized) (String.length input) path minimized)

(** Runs one target's corpus contract and generated cases. *)
let run_target seed cases target =
  let seeds = corpus target in
  let valid = ref [] in
  List.iter
    (fun (path, kind, text) ->
      match (fst (execute target text), kind) with
      | Error violation, _ -> report target (Filename.basename path) violation text
      | Ok true, `Valid -> valid := text :: !valid
      | Ok false, `Valid when target.strict ->
          failwith (Printf.sprintf "%s: valid seed %s was rejected" target.name path)
      | Ok true, `Invalid when target.strict ->
          failwith (Printf.sprintf "%s: invalid seed %s was accepted" target.name path)
      | Ok true, `Invalid -> valid := text :: !valid
      | Ok false, _ -> ())
    seeds;
  (* Non-strict targets mutate every seed they accept; rejected seeds of a
     shared corpus belong to another decoder. *)
  let valid = Array.of_list (List.rev !valid) in
  if Array.length valid = 0 then failwith (target.name ^ " accepted no seed");
  let rng = rng_for seed target.name in
  let accepted = ref 0 and rejected = ref 0 and peak = ref 0 in
  for case = 0 to cases - 1 do
    let source = pick rng valid and donor = pick rng valid in
    let operator = if case < operators then case else below rng operators in
    let input = apply operator source donor rng in
    let input =
      if String.length input > max_input_bytes then String.sub input 0 max_input_bytes else input
    in
    match execute target input with
    | Error violation, _ -> report target (Printf.sprintf "%Lx-%06d" seed case) violation input
    | Ok outcome, allocated ->
        peak := max !peak allocated;
        if outcome then incr accepted else incr rejected
  done;
  if !rejected = 0 then failwith (target.name ^ ": mutations never reached the rejection path");
  Printf.printf "%s: seed=0x%Lx seeds=%d generated=%d accepted=%d rejected=%d max_alloc=%d\n%!"
    target.name seed (Array.length valid) cases !accepted !rejected !peak

(** Runs every target selected by [DECODER_FUZZ_TARGET] (all when unset). *)
let () =
  let seed = base_seed () in
  let cases = setting "DECODER_FUZZ_OCAML_CASES" default_cases operators max_cases in
  let selected =
    match Sys.getenv_opt "DECODER_FUZZ_TARGET" with
    | None | Some "" -> fun _ -> true
    | Some name -> fun target -> String.equal target.name name
  in
  List.iter (fun target -> if selected target then run_target seed cases target) targets
