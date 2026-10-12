module Observability = Temporal_base.Observability

(** Extracts a required tag value from the normalization fixture. *)
let require definition tags =
  match Logs.Tag.find definition tags with
  | Some value -> value
  | None -> failwith "expected observability tag"

(** Numeric metadata exposed to reporters is always finite and non-negative,
    even if a future internal caller supplies an invalid value. *)
let test_invalid_numeric_metadata_becomes_zero () =
  let tags =
    Observability.tags ~operation:"invalid_numeric_metadata"
      ~duration_ms:(-1.) ~job_count:(-2) ~command_count:(-3) ()
  in
  assert (require Observability.Tag.duration_ms tags = 0.);
  assert (require Observability.Tag.job_count tags = 0);
  assert (require Observability.Tag.command_count tags = 0);
  List.iter
    (fun duration_ms ->
      let tags =
        Observability.tags ~operation:"non_finite_duration" ~duration_ms ()
      in
      assert (require Observability.Tag.duration_ms tags = 0.))
    [ Float.nan; Float.infinity; Float.neg_infinity ]

(** Valid values retain their precision and magnitude. *)
let test_valid_numeric_metadata_is_unchanged () =
  let tags =
    Observability.tags ~operation:"valid_numeric_metadata" ~duration_ms:1.25
      ~job_count:2 ~command_count:3 ()
  in
  assert (require Observability.Tag.duration_ms tags = 1.25);
  assert (require Observability.Tag.job_count tags = 2);
  assert (require Observability.Tag.command_count tags = 3)

(** Truncating a valid UTF-8 tag never exposes a partial encoded character to
    application reporters. A raw 253-byte prefix would retain only the first
    byte of the four-byte emoji in this fixture. *)
let test_bounded_tag_preserves_utf8_character_boundaries () =
  let ascii_prefix = String.make 252 'a' in
  let tags =
    Observability.tags ~operation:"unicode_tag"
      ~workflow_type:(ascii_prefix ^ "😀tail") ()
  in
  assert
    (require Observability.Tag.workflow_type tags = ascii_prefix ^ "...")

(** Activity correlation tags (#500) follow the shared normalization: a
    negative attempt becomes zero and an oversized activity ID is bounded. *)
let test_activity_correlation_tags_are_normalized () =
  let tags =
    Observability.tags ~operation:"activity_correlation"
      ~activity_id:(String.make 300 'x') ~activity_type:"charge" ~attempt:(-4)
      ()
  in
  assert (require Observability.Tag.attempt tags = 0);
  assert (require Observability.Tag.activity_type tags = "charge");
  assert
    (require Observability.Tag.activity_id tags = String.make 253 'x' ^ "...");
  let tags =
    Observability.tags ~operation:"activity_correlation" ~attempt:3 ()
  in
  assert (require Observability.Tag.attempt tags = 3)

let () =
  test_activity_correlation_tags_are_normalized ();
  test_invalid_numeric_metadata_becomes_zero ();
  test_valid_numeric_metadata_is_unchanged ();
  test_bounded_tag_preserves_utf8_character_boundaries ()
