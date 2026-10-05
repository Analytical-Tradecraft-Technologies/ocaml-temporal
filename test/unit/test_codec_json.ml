(** Unit tests for the built-in [json/plain] codecs: [int], [int64], [bool],
    [float], [json], and the [json_conv] helper. They pin the exact wire text
    other SDKs read, and check that malformed, out-of-range, and non-finite
    input becomes a typed codec error rather than an exception. *)

module Codec = Temporal.Codec

(** Turns an unexpected structured error into a readable test failure. *)
let fail_error error = failwith (Temporal.Error.message error)

(** Extracts a successful test result or fails with its SDK diagnostic. *)
let unwrap = function Ok value -> value | Error error -> fail_error error

(** Builds a [json/plain] payload whose body is the given JSON text, as another
    SDK would send it. *)
let json_payload text : Temporal.Payload.t =
  { metadata = [ ("encoding", "json/plain") ]; data = Bytes.of_string text }

(** Encodes [value] and checks the payload's encoding name and exact body. *)
let check_encodes codec value expected =
  let payload = unwrap (Codec.encode codec value) in
  assert (payload.metadata = [ ("encoding", "json/plain") ]);
  let actual = Bytes.to_string payload.data in
  if actual <> expected then
    failwith (Printf.sprintf "expected body %S, encoded %S" expected actual)

(** Asserts that decoding [text] fails with a typed codec error. *)
let check_rejects codec text =
  match Codec.decode codec (json_payload text) with
  | Error error -> assert (Temporal.Error.kind error = "codec")
  | Ok _ -> failwith (Printf.sprintf "payload %S was unexpectedly accepted" text)

(** Asserts that encoding [value] fails with a typed codec error. *)
let check_encode_rejects codec value =
  match Codec.encode codec value with
  | Error error -> assert (Temporal.Error.kind error = "codec")
  | Ok _ -> failwith "value was unexpectedly encoded"

(** Checks native ints, including the extremes, and strict integer decoding. *)
let test_int () =
  check_encodes Codec.int 42 "42";
  check_encodes Codec.int (-7) "-7";
  List.iter
    (fun value ->
      let payload = unwrap (Codec.encode Codec.int value) in
      assert (Codec.decode Codec.int payload = Ok value))
    [ 0; 1; -1; max_int; min_int ];
  assert (Codec.decode Codec.int (json_payload " 17 ") = Ok 17);
  List.iter (check_rejects Codec.int)
    [
      "1.0";
      "1e3";
      "\"1\"";
      "true";
      "null";
      "";
      "1 2";
      "01";
      "NaN";
      (* One past the 64-bit range and one past OCaml's 63-bit range. *)
      "9223372036854775808";
      Int64.to_string (Int64.succ (Int64.of_int max_int));
    ]

(** Checks the full signed 64-bit range and its boundaries. *)
let test_int64 () =
  check_encodes Codec.int64 Int64.max_int "9223372036854775807";
  check_encodes Codec.int64 Int64.min_int "-9223372036854775808";
  check_encodes Codec.int64 5L "5";
  List.iter
    (fun value ->
      let payload = unwrap (Codec.encode Codec.int64 value) in
      assert (Codec.decode Codec.int64 payload = Ok value))
    [ 0L; -1L; Int64.max_int; Int64.min_int; Int64.of_int max_int ];
  List.iter (check_rejects Codec.int64)
    [ "9223372036854775808"; "-9223372036854775809"; "1.5"; "1e2"; "\"5\"" ]

(** Checks the two boolean literals and rejection of truthy non-booleans. *)
let test_bool () =
  check_encodes Codec.bool true "true";
  check_encodes Codec.bool false "false";
  assert (Codec.decode Codec.bool (json_payload "true") = Ok true);
  assert (Codec.decode Codec.bool (json_payload "false") = Ok false);
  List.iter (check_rejects Codec.bool) [ "1"; "0"; "\"true\""; "null" ]

(** Checks bit-exact float round trips, non-finite rejection in both
    directions, and acceptance of integer literals written by other SDKs. *)
let test_float () =
  check_encodes Codec.float 1.5 "1.5";
  List.iter
    (fun value ->
      let payload = unwrap (Codec.encode Codec.float value) in
      match Codec.decode Codec.float payload with
      | Ok decoded ->
          assert (Int64.bits_of_float decoded = Int64.bits_of_float value)
      | Error error -> fail_error error)
    [ 0.0; -0.0; 0.1; 1.0; -2.5; 1e300; 5e-324; Float.max_float; Float.pi ];
  List.iter (check_encode_rejects Codec.float)
    [ Float.nan; Float.infinity; Float.neg_infinity ];
  assert (Codec.decode Codec.float (json_payload "3") = Ok 3.0);
  assert (Codec.decode Codec.float (json_payload "-3") = Ok (-3.0));
  assert (
    Codec.decode Codec.float (json_payload "18446744073709551616")
    = Ok 18446744073709551616.0);
  List.iter (check_rejects Codec.float)
    [ "NaN"; "Infinity"; "-Infinity"; "1e400"; "\"1.5\""; "null" ]

(** Checks arbitrary JSON round trips and validation of non-standard values. *)
let test_json () =
  let document =
    `Assoc
      [
        ("name", `String "temporal ✓");
        ("items", `List [ `Int 1; `Float 2.5; `Null; `Bool true ]);
        ("big", `Intlit "123456789012345678901234567890");
      ]
  in
  let payload = unwrap (Codec.encode Codec.json document) in
  assert (Codec.decode Codec.json payload = Ok document);
  check_encode_rejects Codec.json (`List [ `Float Float.nan ]);
  check_encode_rejects Codec.json (`String "\xff");
  check_encode_rejects Codec.json (`Assoc [ ("\xff", `Null) ]);
  check_encode_rejects Codec.json (`Intlit "12abc");
  check_encode_rejects Codec.json (`Intlit "");
  check_encode_rejects Codec.json (`Intlit "-");
  check_encode_rejects Codec.json (`Intlit "007");
  assert (Result.is_ok (Codec.encode Codec.json (`Intlit "-0")));
  List.iter (check_rejects Codec.json)
    [ "{\"a\": NaN}"; "[Infinity]"; "\"\xff\""; "{"; "" ];
  match
    Codec.decode Codec.json
      (json_payload "{\"a\":1,\"b\":true}" |> fun payload ->
       { payload with metadata = [ ("encoding", "binary/plain") ] })
  with
  | Error error -> assert (Temporal.Error.kind error = "codec")
  | Ok _ -> failwith "json codec accepted the wrong encoding name"

(** A small record used to exercise [json_conv] the way an application would. *)
type point = { x : int; y : int }

(** Checks a record codec built with [json_conv], including typed shape errors
    from the application decoder and containment of conversion exceptions. *)
let test_json_conv () =
  let point =
    Codec.json_conv
      ~to_json:(fun { x; y } -> `Assoc [ ("x", `Int x); ("y", `Int y) ])
      ~of_json:(function
        | `Assoc [ ("x", `Int x); ("y", `Int y) ] -> Ok { x; y }
        | _ -> Error (Temporal.Error.codec ~message:"expected a point"))
  in
  check_encodes point { x = 1; y = -2 } "{\"x\":1,\"y\":-2}";
  assert (Codec.decode point (json_payload "{\"x\":3,\"y\":4}") = Ok { x = 3; y = 4 });
  check_rejects point "{\"x\":3}";
  check_rejects point "{\"x\":NaN,\"y\":1}";
  let raising =
    Codec.json_conv
      ~to_json:(fun () -> failwith "secret")
      ~of_json:(fun _ -> failwith "secret")
  in
  (match Codec.encode raising () with
  | Error error ->
      assert (Temporal.Error.kind error = "codec");
      assert (
        Temporal.Error.message error
        = "codec \"json/plain\" encode callback raised an exception")
  | Ok _ -> failwith "raising to_json unexpectedly succeeded");
  check_rejects raising "null";
  (* JSON codecs compose with option: [None] is binary/null, and [Some] keeps
     the interoperable json/plain body. *)
  let some = unwrap (Codec.encode (Codec.option Codec.int) (Some 9)) in
  assert (some.metadata = [ ("encoding", "json/plain") ]);
  assert (Bytes.to_string some.data = "9");
  assert (Codec.decode (Codec.option Codec.int) some = Ok (Some 9))

let () =
  test_int ();
  test_int64 ();
  test_bool ();
  test_float ();
  test_json ();
  test_json_conv ()
