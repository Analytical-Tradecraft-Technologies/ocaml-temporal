(** Maximum bytes retained from user-controlled tag strings. The suffix makes
    truncation visible without allowing unbounded diagnostics. *)
let max_tag_length = 256

(** Returns whether a byte is a UTF-8 continuation byte. *)
let is_utf8_continuation byte = Char.code byte land 0xc0 = 0x80

(** Moves a byte offset back to the start of the UTF-8 character crossing it.
    Valid UTF-8 input therefore remains valid when truncated. *)
let rec utf8_boundary value offset =
  if offset = 0 || not (is_utf8_continuation value.[offset]) then offset
  else utf8_boundary value (offset - 1)

(** Bounds strings before handing them to an application reporter without
    splitting a UTF-8 character at the truncation boundary. *)
let bounded value =
  if String.length value <= max_tag_length then value
  else
    let prefix_length = utf8_boundary value (max_tag_length - 3) in
    String.sub value 0 prefix_length ^ "..."

(** Stable log sources let applications enable verbose bridge or workflow
    detail independently from lifecycle information. *)
module Source = struct
  let lifecycle =
    Logs.Src.create ~doc:"Temporal SDK runtime lifecycle"
      "temporal.sdk.lifecycle"

  let bridge =
    Logs.Src.create ~doc:"Temporal SDK private Rust bridge calls"
      "temporal.sdk.bridge"

  let workflow =
    Logs.Src.create ~doc:"Temporal SDK workflow activation processing"
      "temporal.sdk.workflow"
end

(** Typed tag definitions form a common vocabulary across SDK modules and keep
    reporters independent from message prose. *)
module Tag = struct
  let operation =
    Logs.Tag.def ~doc:"Stable SDK operation identifier" "temporal.operation"
      Format.pp_print_string

  let duration_ms =
    Logs.Tag.def ~doc:"Elapsed operation time in milliseconds"
      "temporal.duration_ms" (fun formatter value ->
        Format.fprintf formatter "%.3f" value)

  let workflow_type =
    Logs.Tag.def ~doc:"Registered Temporal workflow type"
      "temporal.workflow_type" Format.pp_print_string

  let job_count =
    Logs.Tag.def ~doc:"Jobs in one workflow activation" "temporal.job_count"
      Format.pp_print_int

  let command_count =
    Logs.Tag.def ~doc:"Commands produced by one workflow activation"
      "temporal.command_count" Format.pp_print_int

  let bridge_status =
    Logs.Tag.def ~doc:"Stable private bridge status" "temporal.bridge_status"
      Format.pp_print_string

  let error_kind =
    Logs.Tag.def ~doc:"Stable Temporal error category" "temporal.error_kind"
      Format.pp_print_string
end

(** Adds an optional tag only when the caller supplied a value. *)
let add_optional definition map value tags =
  match value with
  | None -> tags
  | Some value -> Logs.Tag.add definition (map value) tags

(** Normalizes invalid elapsed time to the documented zero fallback. Reporters
    therefore never need special handling for negative or non-finite values. *)
let non_negative_finite value =
  if Float.is_finite value && value >= 0. then value else 0.

(** Normalizes impossible structural counts to zero at the shared boundary. *)
let non_negative_count value = Int.max 0 value

(** Constructs a bounded tag set in a fixed, reviewable order. *)
let tags ~operation ?duration_ms ?workflow_type ?job_count ?command_count
    ?bridge_status ?error_kind () =
  Logs.Tag.empty
  |> Logs.Tag.add Tag.operation (bounded operation)
  |> add_optional Tag.duration_ms non_negative_finite duration_ms
  |> add_optional Tag.workflow_type bounded workflow_type
  |> add_optional Tag.job_count non_negative_count job_count
  |> add_optional Tag.command_count non_negative_count command_count
  |> add_optional Tag.bridge_status bounded bridge_status
  |> add_optional Tag.error_kind bounded error_kind

(** Measures diagnostic latency with the portable Unix clock. A backwards wall
    clock adjustment is represented as zero rather than a negative duration. *)
let measure_ms action =
  let started = Unix.gettimeofday () in
  let value = action () in
  let elapsed = (Unix.gettimeofday () -. started) *. 1_000. in
  (value, Float.max 0. elapsed)

(** Reports through Logs without allowing application reporter defects to cross
    the SDK boundary. Messages are constants at current call sites. *)
let report ~src level ~tags message =
  try Logs.msg ~src level (fun log -> log ~tags "%s" message)
  with _reporter_or_formatter_defect -> ()
