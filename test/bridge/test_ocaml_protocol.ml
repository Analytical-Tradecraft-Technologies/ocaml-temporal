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
  run "resource limits" test_resource_limits;
  run "compatibility and outgoing validation"
    test_compatibility_and_outgoing_validation;
  run "strict object boundary" test_strict_object_boundary
