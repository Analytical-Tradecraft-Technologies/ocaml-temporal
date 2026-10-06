let compatibility_version = 1l
let max_document_bytes = 192 * 1024 * 1024
let max_depth = 128
let max_string_bytes = 65_536
(* A collection member and a parsed node each require at least one byte in the
   already-bounded source document. These derived ceilings therefore retain a
   finite validation invariant without narrowing the document limit. *)
let max_collection_items = max_document_bytes
let max_nodes = max_document_bytes
let max_payload_bytes = 128 * 1024 * 1024

(** Maximum canonical padded base64 bytes for one maximum-sized payload. *)
let max_payload_base64_bytes = (max_payload_bytes + 2) / 3 * 4

type bridge_error_code =
  | Invalid_message
  | Unsupported_message
  | Internal_bridge

type bridge_error = {
  code : bridge_error_code;
  message : string;
  retryable : bool;
}

type request = {
  correlation_id : string;
  operation : string;
  body : Yojson.Safe.t;
}

type response = {
  correlation_id : string;
  operation : string;
  body : Yojson.Safe.t;
}

type error_response = {
  correlation_id : string;
  operation : string;
  error : bridge_error;
}

type t = Request of request | Response of response | Failed of error_response
type error = { code : string; path : string; message : string }
type error_view = { code : string; path : string; message : string }

(** Constructs a safe error without copying untrusted values into it. *)
let invalid ?(path = "$") message : error =
  { code = "invalid_message"; path; message }

(** Copies the immutable error fields for consumers. *)
let error_view (error : error) : error_view =
  { code = error.code; path = error.path; message = error.message }

(** Sequences fallible validation without exceptions. *)
let ( let* ) = Result.bind

(** Checks every byte using OCaml's UTF-8 decoder. ASCII bytes, which make up
    every canonical base64 payload string, take a single-comparison fast path
    so a multi-megabyte payload is not decoded one scalar value at a time. *)
let valid_utf_8 value =
  let length = String.length value in
  let rec loop offset =
    if offset = length then true
    else if Char.code (String.get value offset) < 0x80 then loop (offset + 1)
    else
      let decoded = String.get_utf_8_uchar value offset in
      Uchar.utf_decode_is_valid decoded
      && loop (offset + Uchar.utf_decode_length decoded)
  in
  loop 0

(** Checks the shared number once before runtime creation. *)
let check_compatibility actual =
  if Int32.equal actual compatibility_version then Ok ()
  else
    Error
      ({
         code = "unsupported_compatibility";
         path = "$";
         message = "unsupported bridge compatibility number";
       }
        : error)

(** Scans raw text to reject byte and depth attacks before recursive parsing.

    The scan is a pair of mutually tail-recursive loops over the raw bytes, so
    it allocates nothing and stops at the first violation. [string_limit]
    counts the unescaped source bytes of each JSON string: a backslash and the
    byte it escapes are not counted, matching the Rust bridge's preflight
    byte-for-byte. An unterminated string or unbalanced bracket is left for the
    full parser to reject. *)
let preflight ?(string_limit = max_string_bytes) input =
  let length = String.length input in
  let rec outside index depth =
    if index = length then Ok ()
    else
      match String.get input index with
      | '"' -> inside (index + 1) depth 0
      | '{' | '[' ->
          if depth + 1 > max_depth then
            Error (invalid "JSON nesting limit exceeded")
          else outside (index + 1) (depth + 1)
      | '}' | ']' -> outside (index + 1) (max 0 (depth - 1))
      | _ -> outside (index + 1) depth
  and inside index depth string_bytes =
    if index >= length then Ok ()
    else
      match String.get input index with
      | '\\' -> inside (index + 2) depth string_bytes
      | '"' -> outside (index + 1) depth
      | _ ->
          if string_bytes + 1 > string_limit then
            Error (invalid "JSON string byte limit exceeded")
          else inside (index + 1) depth (string_bytes + 1)
  in
  if length > max_document_bytes then
    Error (invalid "document byte limit exceeded")
  else outside 0 0

(** Validates a parsed JSON tree, including duplicate keys and finite limits. *)
let validate_json ?(depth = 1) ?(string_limit = max_string_bytes) value =
  let nodes = ref 0 in
  let rec loop depth path (value : Yojson.Safe.t) =
    match value with
    | _ when depth > max_depth ->
        Error (invalid ~path "JSON nesting limit exceeded")
    | _ when !nodes >= max_nodes ->
        Error (invalid ~path "JSON node limit exceeded")
    | `Null | `Bool _ | `Int _ ->
        incr nodes;
        Ok ()
    | `Intlit value -> (
        incr nodes;
        try
          ignore (Int64.of_string value);
          Ok ()
        with _ ->
          Error
            (invalid ~path "JSON integer is outside the signed 64-bit range"))
    | `String value ->
        incr nodes;
        if String.length value > string_limit then
          Error (invalid ~path "decoded JSON string limit exceeded")
        else if not (valid_utf_8 value) then
          Error (invalid ~path "JSON string is not valid UTF-8")
        else Ok ()
    | `List values ->
        incr nodes;
        if List.length values > max_collection_items then
          Error (invalid ~path "JSON collection limit exceeded")
        else
          let rec validate_items index = function
            | [] -> Ok ()
            | value :: rest ->
                let item_path = Printf.sprintf "%s[%d]" path index in
                let* () = loop (depth + 1) item_path value in
                validate_items (index + 1) rest
          in
          validate_items 0 values
    | `Assoc entries ->
        incr nodes;
        if List.length entries > max_collection_items then
          Error (invalid ~path "JSON collection limit exceeded")
        else
          (* Object keys come from remote peers; a random seed prevents
             precomputed hash collisions from making this check quadratic. *)
          let seen = Hashtbl.create ~random:true (List.length entries) in
          List.fold_left
            (fun result (key, value) ->
              let* () = result in
              if Hashtbl.mem seen key then
                Error (invalid ~path "duplicate JSON object member")
              else if String.length key > string_limit || not (valid_utf_8 key)
              then Error (invalid ~path "invalid JSON object key")
              else (
                Hashtbl.add seen key ();
                loop (depth + 1) (path ^ "." ^ key) value))
            (Ok ()) entries
    | `Float _ ->
        Error (invalid ~path "non-integral JSON numbers are not allowed")
  in
  loop depth "$" value

(** Parses one complete document with Yojson while containing every exception.
*)
let parse_strict ?(string_limit = max_string_bytes) input =
  let* () = preflight ~string_limit input in
  try
    let value = Yojson.Safe.from_string input in
    let* () = validate_json ~string_limit value in
    Ok value
  with _ -> Error (invalid "invalid strict JSON document")

(** Requires an association-list JSON object. *)
let expect_object path = function
  | `Assoc entries -> Ok entries
  | _ -> Error (invalid ~path "expected JSON object")

(** Requires a JSON string. *)
let expect_string path = function
  | `String value -> Ok value
  | _ -> Error (invalid ~path "expected JSON string")

(** Requires a JSON boolean. *)
let expect_bool path = function
  | `Bool value -> Ok value
  | _ -> Error (invalid ~path "expected JSON boolean")

(** Finds one required object field. *)
let field path name entries =
  match List.assoc_opt name entries with
  | Some value -> Ok value
  | None -> Error (invalid ~path ("missing required field " ^ name))

(** Requires a closed object with exactly the named fields. *)
let require_exact_fields path expected entries =
  if
    List.length entries = List.length expected
    && List.for_all (fun (key, _) -> List.mem key expected) entries
  then Ok ()
  else Error (invalid ~path "object has missing or unknown fields")

(** Checks correlation identifier syntax without echoing it on failure. *)
let valid_correlation_id value =
  String.length value = 32
  && String.for_all
       (function '0' .. '9' | 'a' .. 'f' -> true | _ -> false)
       value

(** Checks a bounded lowercase operation name. *)
let valid_operation value =
  String.length value > 0
  && String.length value <= 64
  && (match value.[0] with 'a' .. 'z' -> true | _ -> false)
  && String.for_all
       (function 'a' .. 'z' | '0' .. '9' | '_' | '.' -> true | _ -> false)
       value

(** Applies typed invariants symmetrically to decoded and outgoing values. *)
let validate_envelope envelope =
  let correlation_id, operation, body, error =
    match envelope with
    | Request value ->
        (value.correlation_id, value.operation, Some value.body, None)
    | Response value ->
        (value.correlation_id, value.operation, Some value.body, None)
    | Failed value ->
        (value.correlation_id, value.operation, None, Some value.error)
  in
  if not (valid_correlation_id correlation_id) then
    Error
      (invalid ~path:"$.correlation_id"
         "correlation identifier must be lowercase hexadecimal")
  else if not (valid_operation operation) then
    Error (invalid ~path:"$.operation" "invalid operation name")
  else
    let* () =
      match body with
      | Some (`Assoc _ as value) -> validate_json ~depth:2 value
      | Some _ -> Error (invalid ~path:"$.body" "body must be a JSON object")
      | None -> Ok ()
    in
    match error with
    | Some value
      when String.length value.message = 0
           || String.length value.message > 1_024 ->
        Error (invalid ~path:"$.error.message" "invalid error message length")
    | Some value when not (valid_utf_8 value.message) ->
        Error
          (invalid ~path:"$.error.message" "error message is not valid UTF-8")
    | _ -> Ok ()

(** Decodes the closed nested error object. *)
let decode_error entries =
  let* () =
    require_exact_fields "$"
      [ "kind"; "correlation_id"; "operation"; "error" ]
      entries
  in
  let* correlation_json = field "$" "correlation_id" entries in
  let* correlation_id = expect_string "$.correlation_id" correlation_json in
  let* operation_json = field "$" "operation" entries in
  let* operation = expect_string "$.operation" operation_json in
  let* error_json = field "$" "error" entries in
  let* error_entries = expect_object "$.error" error_json in
  let* () =
    require_exact_fields "$.error"
      [ "code"; "message"; "retryable" ]
      error_entries
  in
  let* code_json = field "$.error" "code" error_entries in
  let* code_string = expect_string "$.error.code" code_json in
  let* code =
    match code_string with
    | "invalid_message" -> Ok Invalid_message
    | "unsupported_message" -> Ok Unsupported_message
    | "internal_bridge" -> Ok Internal_bridge
    | _ -> Error (invalid ~path:"$.error.code" "unknown bridge error code")
  in
  let* message_json = field "$.error" "message" error_entries in
  let* message = expect_string "$.error.message" message_json in
  let* retryable_json = field "$.error" "retryable" error_entries in
  let* retryable = expect_bool "$.error.retryable" retryable_json in
  let envelope =
    Failed { correlation_id; operation; error = { code; message; retryable } }
  in
  let* () = validate_envelope envelope in
  Ok envelope

(** Converts a strict JSON object into a typed transport envelope. *)
let envelope_from_json value =
  let* entries = expect_object "$" value in
  let* kind_json = field "$" "kind" entries in
  let* kind = expect_string "$.kind" kind_json in
  match kind with
  | "request" | "response" ->
      let* () =
        require_exact_fields "$"
          [ "kind"; "correlation_id"; "operation"; "body" ]
          entries
      in
      let* correlation_json = field "$" "correlation_id" entries in
      let* correlation_id = expect_string "$.correlation_id" correlation_json in
      let* operation_json = field "$" "operation" entries in
      let* operation = expect_string "$.operation" operation_json in
      let* body = field "$" "body" entries in
      let envelope =
        if String.equal kind "request" then
          Request { correlation_id; operation; body }
        else Response { correlation_id; operation; body }
      in
      let* () = validate_envelope envelope in
      Ok envelope
  | "error" -> decode_error entries
  | _ -> Error (invalid ~path:"$.kind" "unknown envelope kind")

(** Strictly decodes one complete envelope. *)
let decode input =
  try
    let* value = parse_strict input in
    envelope_from_json value
  with _ -> Error (invalid "invalid strict JSON document")

(** Recursively sorts object keys and canonicalizes integral literals. *)
let rec normalize_json = function
  | `Assoc entries ->
      `Assoc
        (entries
        |> List.map (fun (key, value) -> (key, normalize_json value))
        |> List.sort (fun (left, _) (right, _) -> String.compare left right))
  | `List values -> `List (List.map normalize_json values)
  | `Intlit value -> `Intlit (Int64.to_string (Int64.of_string value))
  | value -> value

(** Converts a typed envelope into fixed-order normalized Yojson. *)
let envelope_to_json = function
  | Request value ->
      `Assoc
        [
          ("kind", `String "request");
          ("correlation_id", `String value.correlation_id);
          ("operation", `String value.operation);
          ("body", normalize_json value.body);
        ]
  | Response value ->
      `Assoc
        [
          ("kind", `String "response");
          ("correlation_id", `String value.correlation_id);
          ("operation", `String value.operation);
          ("body", normalize_json value.body);
        ]
  | Failed value ->
      let code =
        match value.error.code with
        | Invalid_message -> "invalid_message"
        | Unsupported_message -> "unsupported_message"
        | Internal_bridge -> "internal_bridge"
      in
      `Assoc
        [
          ("kind", `String "error");
          ("correlation_id", `String value.correlation_id);
          ("operation", `String value.operation);
          ( "error",
            `Assoc
              [
                ("code", `String code);
                ("message", `String value.error.message);
                ("retryable", `Bool value.error.retryable);
              ] );
        ]

(** Validates, serializes, and independently reparses an outgoing envelope. *)
let encode envelope =
  try
    let* () = validate_envelope envelope in
    let output = Yojson.Safe.to_string (envelope_to_json envelope) in
    let* reparsed = decode output in
    if String.equal (Yojson.Safe.to_string (envelope_to_json reparsed)) output
    then Ok output
    else Error (invalid "outgoing envelope did not round trip")
  with _ -> Error (invalid "could not encode outgoing envelope")

(** Strictly decodes one closed-operation candidate as an object. Semantic
    modules apply their own exact-field rules before returning a typed value. *)
let decode_object input =
  try
    let* value = parse_strict input in
    match value with
    | `Assoc _ -> Ok value
    | _ -> Error (invalid "operation body must be a JSON object")
  with _ -> Error (invalid "invalid strict JSON document")

(** Normalizes and reparses an operation object so sender-side construction
    receives the same strict checks as peer-supplied JSON. *)
let encode_object value =
  try
    match value with
    | `Assoc _ ->
        let* () = validate_json value in
        let output = Yojson.Safe.to_string (normalize_json value) in
        let* reparsed = decode_object output in
        if
          String.equal
            (Yojson.Safe.to_string (normalize_json reparsed))
            output
        then Ok output
        else Error (invalid "outgoing object did not round trip")
    | _ -> Error (invalid "operation body must be a JSON object")
  with _ -> Error (invalid "could not encode outgoing object")

(** Parses a semantic object with the payload base64 ceiling. Callers retain
    the ordinary string ceiling for all non-payload fields. *)
let decode_payload_object input =
  try
    let* value = parse_strict ~string_limit:max_payload_base64_bytes input in
    match value with
    | `Assoc _ -> Ok value
    | _ -> Error (invalid "operation body must be a JSON object")
  with _ -> Error (invalid "invalid strict JSON document")

(** Validates, normalizes, and serializes a semantic object containing payload
    wrappers.

    The checks are the ones a receiver applies to these bytes, run without a
    second parse: [validate_json] applies the tree rules of [parse_strict]
    (duplicate keys, UTF-8, integer range, depth, node and string limits) to the
    outgoing tree, and [preflight] applies the raw-text rules (document size,
    raw nesting, and escaped string length) to the exact serialized bytes, as
    the Rust bridge does before it parses them. Earlier versions also reparsed,
    renormalized, and reserialized the output to compare it with itself; that
    only re-tested Yojson's printer against its own parser and cost several
    passes over every payload byte (#846). *)
let encode_payload_object value =
  try
    match value with
    | `Assoc _ ->
        let* () = validate_json ~string_limit:max_payload_base64_bytes value in
        let output = Yojson.Safe.to_string (normalize_json value) in
        let* () = preflight ~string_limit:max_payload_base64_bytes output in
        Ok output
    | _ -> Error (invalid "operation body must be a JSON object")
  with _ -> Error (invalid "could not encode outgoing object")

(** Canonical RFC 4648 alphabet used by the private payload codec. *)
let base64_alphabet =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

(** Encodes bytes with canonical padded RFC 4648 base64.

    Whole three-byte groups are encoded in one loop; the one- or two-byte tail
    and its padding are written separately so the hot loop carries no
    per-group length tests. Every alphabet index is masked to six bits. *)
let base64_encode bytes =
  let length = Bytes.length bytes in
  let output = Bytes.make ((length + 2) / 3 * 4) '=' in
  (* Indexes are masked to six bits, so the alphabet lookup is in bounds. *)
  let symbol index = String.unsafe_get base64_alphabet (index land 63) in
  let groups = length / 3 in
  (* [group < length / 3] gives [input_offset + 2 < length] and
     [output_offset + 3 < (length + 2) / 3 * 4], so the unchecked accesses in
     this hot loop stay inside both buffers. *)
  for group = 0 to groups - 1 do
    let input_offset = group * 3 in
    let output_offset = group * 4 in
    let first = Char.code (Bytes.unsafe_get bytes input_offset) in
    let second = Char.code (Bytes.unsafe_get bytes (input_offset + 1)) in
    let third = Char.code (Bytes.unsafe_get bytes (input_offset + 2)) in
    Bytes.unsafe_set output output_offset (symbol (first lsr 2));
    Bytes.unsafe_set output (output_offset + 1)
      (symbol ((first lsl 4) lor (second lsr 4)));
    Bytes.unsafe_set output (output_offset + 2)
      (symbol ((second lsl 2) lor (third lsr 6)));
    Bytes.unsafe_set output (output_offset + 3) (symbol third)
  done;
  let input_offset = groups * 3 in
  let output_offset = groups * 4 in
  (match length - input_offset with
  | 1 ->
      let first = Char.code (Bytes.get bytes input_offset) in
      Bytes.set output output_offset (symbol (first lsr 2));
      Bytes.set output (output_offset + 1) (symbol (first lsl 4))
  | 2 ->
      let first = Char.code (Bytes.get bytes input_offset) in
      let second = Char.code (Bytes.get bytes (input_offset + 1)) in
      Bytes.set output output_offset (symbol (first lsr 2));
      Bytes.set output (output_offset + 1)
        (symbol ((first lsl 4) lor (second lsr 4)));
      Bytes.set output (output_offset + 2) (symbol (second lsl 2))
  | _ -> ());
  Bytes.unsafe_to_string output

(** Maps every byte to its six-bit base64 value, or to [0xff] for a byte
    outside the alphabet. [0xff] has bit 7 set, which no six-bit value has, so
    one OR across a group detects any invalid symbol. The padding byte ['=']
    is invalid here; [base64_decode] admits it only at the end of the input. *)
let base64_decode_table =
  let table = Bytes.make 256 '\xff' in
  String.iteri
    (fun value symbol -> Bytes.set table (Char.code symbol) (Char.chr value))
    base64_alphabet;
  Bytes.unsafe_to_string table

(** Decodes canonical padded base64 in one pass, directly into a buffer of the
    exact decoded size.

    Canonical means exactly what [base64_encode] produces: a multiple of four
    symbols, only alphabet bytes before the padding, one or two ['='] only at
    the end, and zero bits in the unused low bits of the last symbol. Those
    conditions are checked directly instead of by re-encoding the result; they
    accept precisely the strings [base64_encode] can emit, so every payload has
    one wire spelling. The encoded and decoded size limits are checked before
    the output buffer is allocated. Errors never include input bytes. *)
let base64_decode data =
  let not_canonical () =
    Error (invalid ~path:"$.data" "payload is not canonical padded base64")
  in
  let length = String.length data in
  if length mod 4 <> 0 || length > max_payload_base64_bytes then
    not_canonical ()
  else
    let padding =
      if length = 0 then 0
      else if
        length >= 2
        && Char.equal data.[length - 1] '='
        && Char.equal data.[length - 2] '='
      then 2
      else if Char.equal data.[length - 1] '=' then 1
      else 0
    in
    let decoded_length = (length / 4 * 3) - padding in
    if decoded_length > max_payload_bytes then
      Error (invalid ~path:"$.data" "decoded payload limit exceeded")
    else
      let output = Bytes.create decoded_length in
      let value index =
        Char.code (String.get base64_decode_table (Char.code data.[index]))
      in
      (* Stores the high [count] bytes of the padded last group, with bounds
         checks; it runs at most once per payload. *)
      let store output_offset count first second third =
        let bits = (first lsl 18) lor (second lsl 12) lor (third lsl 6) in
        Bytes.set output output_offset (Char.unsafe_chr (bits lsr 16));
        if count > 1 then
          Bytes.set output (output_offset + 1)
            (Char.unsafe_chr ((bits lsr 8) land 0xff))
      in
      (* Every group except a padded last one decodes to three bytes. This
         loop carries nearly all payload bytes, so it is written out without
         helper calls. Its unchecked accesses are in bounds by construction:
         [group < full_groups <= length / 4] gives
         [input_offset + 3 < length] and
         [output_offset + 2 < full_groups * 3 <= decoded_length]; the table
         index is a [Char.code], always below the table's 256 bytes. *)
      let full_groups = if padding = 0 then length / 4 else (length / 4) - 1 in
      let symbol index =
        Char.code
          (String.unsafe_get base64_decode_table
             (Char.code (String.unsafe_get data index)))
      in
      let rec groups group =
        if group = full_groups then true
        else
          let input_offset = group * 4 in
          let first = symbol input_offset in
          let second = symbol (input_offset + 1) in
          let third = symbol (input_offset + 2) in
          let fourth = symbol (input_offset + 3) in
          if (first lor second lor third lor fourth) land 0x80 <> 0 then false
          else
            let bits =
              (first lsl 18) lor (second lsl 12) lor (third lsl 6) lor fourth
            in
            let output_offset = group * 3 in
            Bytes.unsafe_set output output_offset (Char.unsafe_chr (bits lsr 16));
            Bytes.unsafe_set output (output_offset + 1)
              (Char.unsafe_chr ((bits lsr 8) land 0xff));
            Bytes.unsafe_set output (output_offset + 2)
              (Char.unsafe_chr (bits land 0xff));
            groups (group + 1)
      in
      (* A padded last group must also leave its unused low bits zero: two
         bits of the third symbol for one ['='], four bits of the second for
         two. Nonzero bits would give the same bytes a second spelling. *)
      let last_group () =
        let input_offset = full_groups * 4 in
        let output_offset = full_groups * 3 in
        match padding with
        | 1 ->
            let first = value input_offset in
            let second = value (input_offset + 1) in
            let third = value (input_offset + 2) in
            if (first lor second lor third) land 0x80 <> 0 || third land 3 <> 0
            then false
            else (
              store output_offset 2 first second third;
              true)
        | 2 ->
            let first = value input_offset in
            let second = value (input_offset + 1) in
            if (first lor second) land 0x80 <> 0 || second land 15 <> 0 then
              false
            else (
              store output_offset 1 first second 0;
              true)
        | _ -> true
      in
      if groups 0 && last_group () then Ok output else not_canonical ()

(** Decodes an already-parsed closed payload wrapper without serializing it
    again.

    The wrapper must have exactly the members [encoding] and [data];
    [encoding] must be ["base64"] and [data] canonical padded base64 within the
    payload limits. These checks are complete on their own, so a wrapper built
    in memory receives the same validation as one that came from
    [parse_strict]. Errors carry no payload bytes. *)
let decode_payload_json json =
  let* entries = expect_object "$" json in
  let* () = require_exact_fields "$" [ "encoding"; "data" ] entries in
  let* encoding_json = field "$" "encoding" entries in
  let* encoding = expect_string "$.encoding" encoding_json in
  if not (String.equal encoding "base64") then
    Error (invalid ~path:"$.encoding" "unsupported payload encoding")
  else
    (* [require_exact_fields] admits a repeated name, but both names being
       present in a two-member object proves the members are distinct. *)
    let* data_json = field "$" "data" entries in
    let* data = expect_string "$.data" data_json in
    base64_decode data

(** Builds the canonical closed wrapper for opaque bytes. Base64 output is
    ASCII from a fixed alphabet, so the wrapper needs no further JSON
    validation. *)
let payload_json bytes =
  if Bytes.length bytes > max_payload_bytes then
    Error (invalid ~path:"$.data" "decoded payload limit exceeded")
  else
    Ok
      (`Assoc
         [
           ("encoding", `String "base64");
           ("data", `String (base64_encode bytes));
         ])

(** Decodes a closed payload wrapper without exposing its data in errors.
    Parsing temporarily admits base64's larger encoded representation, then
    immediately enforces the exact fields, encoding, canonical form, and
    decoded-byte limit before returning any data. *)
let decode_payload input =
  try
    let* json = parse_strict ~string_limit:max_payload_base64_bytes input in
    decode_payload_json json
  with _ -> Error (invalid "invalid strict payload document")

(** Encodes opaque bytes as one canonical payload wrapper document. The output
    needs no reparse: [payload_json] only emits the two fixed members and an
    ASCII base64 string. *)
let encode_payload bytes =
  try
    let* json = payload_json bytes in
    Ok (Yojson.Safe.to_string json)
  with _ -> Error (invalid "could not encode outgoing payload")
