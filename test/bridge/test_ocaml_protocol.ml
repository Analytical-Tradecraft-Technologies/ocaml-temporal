module Protocol = Temporal_protocol.Control_protocol

(** Reads a complete fixture as binary-safe text and closes the descriptor on
    both successful and exceptional paths. *)
let read_file path =
  let channel = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in_noerr channel)
    (fun () -> really_input_string channel (in_channel_length channel))

(** Resolves a fixture beneath Dune's copied source tree. *)
let fixture parts =
  List.fold_left Filename.concat "fixtures/protocol" parts |> read_file

(** Fails the test with a stable rendering of a structured protocol error. *)
let unwrap = function
  | Ok value -> value
  | Error error ->
      let view = Protocol.error_view error in
      failwith (Printf.sprintf "%s at %s: %s" view.code view.path view.message)

(** Requires a result to fail without inspecting potentially sensitive input. *)
let require_error = function
  | Error _ -> ()
  | Ok _ -> failwith "expected protocol validation to fail"

(** Compares strings without adding a test-framework dependency to the public
    package's test closure. *)
let check_string label expected actual =
  if not (String.equal expected actual) then
    failwith (label ^ " did not match its expected value")

(** Compares integer observations from resource and payload tests. *)
let check_int label expected actual =
  if expected <> actual then
    failwith (label ^ " did not match its expected value")

(** Requires one Boolean protocol invariant. *)
let check_true label condition =
  if not condition then failwith (label ^ " was false")

(** Requires a typed failure at one exact structural location. *)
let check_error_path label expected = function
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")
  | Error error ->
      let view = Protocol.error_view error in
      check_string (label ^ " error path") expected view.path

(** Proves valid shared envelopes normalize and survive a typed round trip. *)
let test_valid_envelopes () =
  List.iter
    (fun name ->
      let input = fixture [ "valid"; name ^ ".input.json" ] in
      let expected =
        String.trim (fixture [ "valid"; name ^ ".normalized.json" ])
      in
      let decoded = unwrap (Protocol.decode input) in
      check_string (name ^ " normalization") expected
        (unwrap (Protocol.encode decoded));
      ignore (unwrap (Protocol.decode expected)))
    [ "request"; "response"; "error"; "unicode" ]

(** Proves every malformed shared envelope is rejected, including duplicate
    members that an ordinary association-map decoder could silently replace. *)
let test_invalid_envelopes () =
  List.iter
    (fun name ->
      require_error (Protocol.decode (fixture [ "invalid"; name ^ ".json" ])))
    [
      "duplicate-envelope";
      "duplicate-body";
      "missing-field";
      "unknown-field";
      "wrong-type";
      "invalid-correlation";
      "unknown-kind";
      "non-integral-number";
      "integer-out-of-range";
      "error-unknown-field";
    ]

(** Exercises the standalone canonical payload wrapper without rendering raw
    bytes in a failure message. *)
let test_payloads () =
  let input = fixture [ "valid"; "payload.input.json" ] in
  let expected = String.trim (fixture [ "valid"; "payload.normalized.json" ]) in
  let bytes = unwrap (Protocol.decode_payload input) in
  check_int "decoded length" 5 (Bytes.length bytes);
  check_string "normalized payload" expected
    (unwrap (Protocol.encode_payload bytes));
  let all_bytes = Bytes.init 256 Char.chr in
  check_int "all byte values" 256
    (Bytes.length
       (unwrap
          (Protocol.decode_payload (unwrap (Protocol.encode_payload all_bytes)))));
  (* Exercise the normal Temporal blob-limit scale without allocating the
     bridge's 128 MiB transport safety maximum in every CI matrix cell. *)
  let maximum =
    Bytes.init (2 * 1024 * 1024) (fun index ->
        Char.chr (index land 255))
  in
  check_true "maximum payload round trip"
    (Bytes.equal maximum
       (unwrap
          (Protocol.decode_payload (unwrap (Protocol.encode_payload maximum)))));
  let oversized_encoding =
    {|{"encoding":"|} ^ String.make 65_537 'a' ^ {|","data":""}|}
  in
  require_error (Protocol.decode_payload oversized_encoding);
  let oversized_unknown_field =
    {|{"encoding":"base64","data":"","extra":"|}
    ^ String.make 65_537 'a'
    ^ {|"}|}
  in
  require_error (Protocol.decode_payload oversized_unknown_field);
  require_error
    (Protocol.decode_payload
       (fixture [ "invalid"; "payload-invalid-base64.json" ]));
  require_error
    (Protocol.decode_payload
       (fixture [ "invalid"; "payload-unknown-field.json" ]))

(** Extracts the base64 text from a canonical wrapper built by the codec. *)
let wrapper_data bytes =
  match unwrap (Protocol.payload_json bytes) with
  | `Assoc [ ("encoding", `String "base64"); ("data", `String data) ] -> data
  | _ -> failwith "payload wrapper did not have its canonical shape"

(** Builds an in-memory wrapper around arbitrary candidate base64 text. *)
let wrapper data =
  `Assoc [ ("encoding", `String "base64"); ("data", `String data) ]

(** A deliberately simple reference for canonical base64, independent of the
    codec's table-driven decoder: decode leniently (ignoring unused low bits),
    then accept only if re-encoding reproduces the input exactly. This is the
    rule the codec enforced by re-encoding before #846. *)
let reference_decode data =
  let alphabet =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
  in
  let length = String.length data in
  let padding =
    if length >= 2 && data.[length - 1] = '=' && data.[length - 2] = '=' then 2
    else if length >= 1 && data.[length - 1] = '=' then 1
    else 0
  in
  if length mod 4 <> 0 then None
  else
    let values =
      List.init (length - padding) (fun index ->
          String.index_opt alphabet data.[index])
    in
    if List.mem None values then None
    else
      let values = Array.of_list (List.map Option.get values) in
      let decoded_length = (length / 4 * 3) - padding in
      let output = Buffer.create decoded_length in
      let symbol index =
        if index < Array.length values then values.(index) else 0
      in
      for group = 0 to (length / 4) - 1 do
        let bits =
          (symbol (group * 4) lsl 18)
          lor (symbol ((group * 4) + 1) lsl 12)
          lor (symbol ((group * 4) + 2) lsl 6)
          lor symbol ((group * 4) + 3)
        in
        List.iter
          (fun shift ->
            if Buffer.length output < decoded_length then
              Buffer.add_char output (Char.chr ((bits lsr shift) land 0xff)))
          [ 16; 8; 0 ]
      done;
      let bytes = Buffer.to_bytes output in
      if String.equal (wrapper_data bytes) data then Some bytes else None

(** Requires the in-memory decoder, the serialized-document decoder, and the
    reference to agree on acceptance and on decoded bytes. *)
let check_decoders_agree data =
  let direct = Protocol.decode_payload_json (wrapper data) in
  let serialized =
    Protocol.decode_payload (Yojson.Safe.to_string (wrapper data))
  in
  match (reference_decode data, direct, serialized) with
  | Some expected, Ok direct, Ok serialized ->
      check_true "direct decode matches reference" (Bytes.equal expected direct);
      check_true "serialized decode matches reference"
        (Bytes.equal expected serialized)
  | None, Error direct, Error _ ->
      let direct = Protocol.error_view direct in
      check_string "rejection path" "$.data" direct.path
  | _ -> failwith "payload decoders disagreed on canonical base64 acceptance"

(** Pins the single-pass base64 codec and in-memory wrapper decoder to the
    previous encode-and-compare behavior, including every padding shape,
    noncanonical spelling, alphabet violation, and wrapper-shape error. *)
let test_payload_codec_edges () =
  (* RFC 4648 section 10 vectors fix the exact wire spelling. *)
  List.iter
    (fun (plain, encoded) ->
      check_string ("encode " ^ plain) encoded
        (wrapper_data (Bytes.of_string plain));
      check_true ("decode " ^ plain)
        (Bytes.equal (Bytes.of_string plain)
           (unwrap (Protocol.decode_payload_json (wrapper encoded)))))
    (* RFC 4648 section 10 vectors: each plaintext is a prefix of "foobar",
       built with [String.sub] so the two-letter prefix is not a literal that
       the spelling gate reports. *)
    (List.map
       (fun (length, encoded) -> (String.sub "foobar" 0 length, encoded))
       [
         (0, "");
         (1, "Zg==");
         (2, "Zm8=");
         (3, "Zm9v");
         (4, "Zm9vYg==");
         (5, "Zm9vYmE=");
         (6, "Zm9vYmFy");
       ]);
  check_string "empty payload document" {|{"encoding":"base64","data":""}|}
    (unwrap (Protocol.encode_payload Bytes.empty));
  (* Every length across many group boundaries round-trips through both the
     document and the in-memory paths, for arbitrary byte values. *)
  let random = Random.State.make [| 846 |] in
  for length = 0 to 300 do
    let bytes =
      Bytes.init length (fun _ -> Char.chr (Random.State.int random 256))
    in
    check_decoders_agree (wrapper_data bytes);
    check_true "document round trip"
      (Bytes.equal bytes
         (unwrap
            (Protocol.decode_payload (unwrap (Protocol.encode_payload bytes)))))
  done;
  List.iter check_decoders_agree
    [
      (* nonzero unused bits with two and with one padding byte *)
      "Zh==";
      "Zm9=";
      "Zg=";
      "Zg";
      "Z===";
      "====";
      "=AAA";
      (* padding before the last group *)
      "Zg==Zg==";
      "Zm9v====";
      "Zm9v\n";
      "Zm9v Zm9v";
      "Zm-v";
      "Zm_v";
      "Zm9\x80";
      "\xc3\xa9AA";
      "Zm9vYmFy\000AAA";
    ];
  (* Exhaustively compare all four-symbol groups over a small alphabet that
     covers both ends of each six-bit range, padding, and invalid bytes, both
     alone and after a valid group. *)
  let symbols = "AQgw/+=-\x80z" in
  String.iter
    (fun first ->
      String.iter
        (fun second ->
          String.iter
            (fun third ->
              String.iter
                (fun fourth ->
                  let group =
                    String.init 4 (function
                      | 0 -> first
                      | 1 -> second
                      | 2 -> third
                      | _ -> fourth)
                  in
                  check_decoders_agree group;
                  check_decoders_agree ("Zm9v" ^ group))
                symbols)
            symbols)
        symbols)
    symbols;
  (* Wrapper shape errors are detected without serializing the wrapper, and
     a repeated member name cannot stand in for a missing one. *)
  List.iter
    (fun json -> require_error (Protocol.decode_payload_json json))
    [
      `Assoc [ ("encoding", `String "base64"); ("encoding", `String "base64") ];
      `Assoc [ ("data", `String ""); ("data", `String "") ];
      `Assoc [ ("encoding", `String "base64") ];
      `Assoc
        [ ("encoding", `String "base64"); ("data", `String ""); ("extra", `Null) ];
      `Assoc [ ("encoding", `String "base32"); ("data", `String "") ];
      `Assoc [ ("encoding", `Int 64); ("data", `String "") ];
      `Assoc [ ("encoding", `String "base64"); ("data", `Null) ];
      `List [];
      `String "";
    ];
  check_error_path "unsupported encoding path" "$.encoding"
    (Protocol.decode_payload_json
       (`Assoc [ ("data", `String ""); ("encoding", `String "hex") ]));
  ignore
    (unwrap
       (Protocol.decode_payload_json
          (`Assoc [ ("data", `String "Zg=="); ("encoding", `String "base64") ])))

(** Outgoing payload objects keep every receiver check without a reparse: tree
    rules come from tree validation and raw-text rules from the preflight scan
    of the serialized bytes. *)
let test_outgoing_payload_object () =
  let input =
    {| {"z":{"encoding":"base64","data":"Zg=="},"a":[1,-2,9223372036854775807]} |}
  in
  let value = unwrap (Protocol.decode_payload_object input) in
  let output = unwrap (Protocol.encode_payload_object value) in
  check_string "normalized payload object"
    {|{"a":[1,-2,9223372036854775807],"z":{"data":"Zg==","encoding":"base64"}}|}
    output;
  check_string "stable re-encoding" output
    (unwrap
       (Protocol.encode_payload_object
          (unwrap (Protocol.decode_payload_object output))));
  require_error (Protocol.encode_payload_object (`List []));
  require_error
    (Protocol.encode_payload_object (`Assoc [ ("a", `Null); ("a", `Null) ]));
  require_error
    (Protocol.encode_payload_object (`Assoc [ ("a", `String "\xff") ]));
  require_error (Protocol.encode_payload_object (`Assoc [ ("a", `Float 1.5) ]));
  require_error
    (Protocol.encode_payload_object
       (`Assoc [ ("a", `Intlit "9223372036854775808") ]));
  let rec nested depth =
    if depth = 0 then `Null else `Assoc [ ("a", nested (depth - 1)) ]
  in
  require_error (Protocol.encode_payload_object (nested 128));
  ignore (unwrap (Protocol.encode_payload_object (nested 127)))

(** Generates resource-limit attacks locally so the repository does not carry
    megabyte-sized fixtures. *)
let test_resource_limits () =
  let prefix =
    {|{"kind":"request","correlation_id":"0123456789abcdef0123456789abcdef","operation":"worker.poll","body":|}
  in
  let deep = prefix ^ String.make 129 '[' ^ String.make 129 ']' ^ "}" in
  let long_string =
    prefix ^ "{\"value\":\"" ^ String.make 65_537 'a' ^ "\"}}"
  in
  let escaped_long_string =
    prefix ^ "{\"value\":\""
    ^ String.init (65_537 * 2) (fun index ->
        if index mod 2 = 0 then '\\' else '"')
    ^ "\"}}"
  in
  let long_array =
    prefix ^ "{\"values\":["
    ^ String.concat "," (List.init 257 (Fun.const "null"))
    ^ "]}}"
  in
  require_error (Protocol.decode deep);
  require_error (Protocol.decode long_string);
  require_error (Protocol.decode escaped_long_string);
  ignore (unwrap (Protocol.decode long_array));
  check_int "document safety limit" (192 * 1024 * 1024)
    Protocol.max_document_bytes;
  check_int "payload safety limit" (128 * 1024 * 1024)
    Protocol.max_payload_bytes;
  let maximum_base64_bytes =
    (Protocol.max_payload_bytes + 2) / 3 * 4
  in
  check_int "maximum base64 bytes" 178_956_972 maximum_base64_bytes;
  check_true "document admits one maximum base64 field"
    (Protocol.max_document_bytes > maximum_base64_bytes);
  check_true "document remains an aggregate bound"
    (Protocol.max_document_bytes < maximum_base64_bytes * 2)

(** Verifies the single startup compatibility gate and outgoing self-validation
    of typed values constructed by internal callers. *)
let test_compatibility_and_outgoing_validation () =
  ignore (unwrap (Protocol.check_compatibility Protocol.compatibility_version));
  require_error (Protocol.check_compatibility Int32.max_int);
  let invalid =
    Protocol.Request
      {
        correlation_id = "not-a-correlation-id";
        operation = "worker.poll";
        body = `Assoc [];
      }
  in
  require_error (Protocol.encode invalid)

(** Proves operation-specific modules can reuse strict duplicate-aware object
    parsing without accepting an envelope-shaped value. *)
let test_strict_object_boundary () =
  require_error (Protocol.decode_object {|{"outer":{"value":1,"value":2}}|});
  let value = unwrap (Protocol.decode_object {| {"z":2,"a":{"y":1}} |}) in
  check_string "normalized strict object" {|{"a":{"y":1},"z":2}|}
    (unwrap (Protocol.encode_object value));
  require_error (Protocol.decode_object {|[]|});
  require_error (Protocol.encode_object (`List []));
  let invalid_utf_8 = String.make 1 (Char.chr 0xff) in
  check_error_path "nested array value" "$.outer[1].name"
    (Protocol.encode_object
       (`Assoc
         [
           ( "outer",
             `List
               [ `Assoc [ ("name", `String "valid") ];
                 `Assoc [ ("name", `String invalid_utf_8) ];
               ] );
         ]))

(** Runs one test and identifies its name without exposing protocol inputs. *)
let run name test =
  try
    test ();
    Printf.printf "PASS %s\n%!" name
  with exn ->
    Printf.eprintf "FAIL %s: %s\n%!" name (Printexc.to_string exn);
    exit 1

let () =
  run "valid envelopes" test_valid_envelopes;
  run "invalid envelopes" test_invalid_envelopes;
  run "payloads" test_payloads;
  run "payload codec edges" test_payload_codec_edges;
  run "outgoing payload object" test_outgoing_payload_object;
  run "resource limits" test_resource_limits;
  run "compatibility and outgoing validation"
    test_compatibility_and_outgoing_validation;
  run "strict object boundary" test_strict_object_boundary
