(** Implements the public codec contract without exposing the private base
    codec record. A codec owns complete payload-level callbacks, which lets
    value-dependent encodings such as [option] preserve their metadata exactly
    while private adapters later convert the callbacks to base codecs. *)
type payload = Payload.t = { metadata : (string * string) list; data : bytes }

type 'a t = {
  encode_payload : 'a -> (payload, Error.t) result;
  decode_payload : payload -> ('a, Error.t) result;
}

(** Finds the optional encoding marker while enforcing the object semantics of
    payload metadata. The public record uses a list so callers can preserve
    metadata they do not recognize, but the wire representation is a JSON
    object and therefore cannot contain duplicate names. *)
let encoding_metadata metadata =
  let seen = Hashtbl.create 16 in
  let rec loop encoding = function
    | [] -> Ok encoding
    | (key, value) :: rest ->
        if Hashtbl.mem seen key then
          Error (Error.codec ~message:"payload metadata contains a duplicate key")
        else (
          Hashtbl.add seen key ();
          loop
            (if String.equal key "encoding" then Some value else encoding)
            rest)
  in
  loop None metadata

(** Encoding name reserved for the envelope that {!option} uses to keep [None]
    distinct from an inner value whose own encoding is [binary/null]. The name
    is deliberately namespaced ([x-ocaml-]) so it cannot clash with a standard
    Temporal encoding, and {!make} refuses to build a user codec that claims it,
    guaranteeing the [option] combinator owns the marker exclusively. *)
let optional_wrapper_encoding = "binary/x-ocaml-optional"

(** Builds a codec that writes one exact encoding name and validates that name
    before invoking the decoder. A callback's returned error is preserved, and
    an ordinary callback exception becomes a typed codec error without exposing
    its possibly sensitive exception message. Private scheduler control
    exceptions still unwind the workflow fiber. Duplicate metadata names are
    rejected before the callback runs, matching the strict bridge protocol.

    Claiming {!optional_wrapper_encoding} is a programmer error: that name is
    reserved for the [option] combinator, so [make] raises [Invalid_argument]
    rather than letting a user payload masquerade as an option envelope. *)
let make ~encoding ~encode ~decode =
  if String.equal encoding optional_wrapper_encoding then
    invalid_arg
      (Printf.sprintf "Codec.make: encoding %S is reserved for Codec.option"
         optional_wrapper_encoding);
  (* Catch only the application callback. A returned [Error] keeps its identity,
     and no payload or exception text enters an SDK diagnostic. Terminal and
     shutdown control exceptions must still unwind their workflow fiber. *)
  let protect_callback operation callback input =
    match callback input with
    | exception (Temporal_sdk_kernel.Scheduler.Workflow_aborted as exception_) ->
        raise exception_
    | exception (Temporal_sdk_kernel.Future_store.Scheduler_shutdown as exception_) ->
        raise exception_
    | exception _ ->
        Error
          (Error.codec
             ~message:
               (Printf.sprintf "codec %S %s callback raised an exception"
                  encoding operation))
    | result -> result
  in
  let encode_payload value =
    Result.map
      (fun data -> { metadata = [ ("encoding", encoding) ]; data })
      (protect_callback "encode" encode value)
  in
  let decode_payload payload =
    match encoding_metadata payload.metadata with
    | Error error -> Error error
    | Ok (Some actual) when String.equal actual encoding ->
        protect_callback "decode" decode payload.data
    | Ok (Some actual) ->
        Error
          (Error.codec
             ~message:
               (Printf.sprintf "expected payload encoding %S, received %S"
                  encoding actual))
    | Ok None -> Error (Error.codec ~message:"payload has no encoding metadata")
  in
  { encode_payload; decode_payload }

(** Serializes a value while preserving the codec's ownership boundary. *)
let encode codec value = codec.encode_payload value

(** Validates metadata and deserializes one payload. *)
let decode codec payload = codec.decode_payload payload

(** Checks every byte of a string as a complete UTF-8 sequence. *)
let valid_utf_8 value =
  let rec loop offset =
    if offset = String.length value then true
    else
      let decoded = String.get_utf_8_uchar value offset in
      Uchar.utf_decode_is_valid decoded
      && loop (offset + Uchar.utf_decode_length decoded)
  in
  loop 0

(** Encodes a string as Temporal's interoperable [json/plain] payload. *)
let encode_json_string value =
  if not (valid_utf_8 value) then
    Error (Error.codec ~message:"OCaml string is not valid UTF-8")
  else Ok (Bytes.of_string (Yojson.Safe.to_string (`String value)))

(** Decodes exactly one JSON string and reports malformed or non-string JSON as
    a typed codec error. Yojson performs the JSON grammar validation. *)
let decode_json_string data =
  try
    match Yojson.Safe.from_string (Bytes.to_string data) with
    | `String value when valid_utf_8 value -> Ok value
    | `String _ -> Error (Error.codec ~message:"JSON string contains invalid UTF-8")
    | _ -> Error (Error.codec ~message:"payload is not a JSON string")
  with Yojson.Json_error message ->
    Error (Error.codec ~message:("invalid JSON string: " ^ message))

let string =
  make ~encoding:"json/plain" ~encode:encode_json_string ~decode:decode_json_string

(** Copies mutable byte buffers on both sides of the codec boundary. *)
let bytes =
  make ~encoding:"binary/plain"
    ~encode:(fun value -> Ok (Bytes.copy value))
    ~decode:(fun value -> Ok (Bytes.copy value))

(** Represents [()] as an empty [binary/null] payload and rejects unexpected
    bytes when decoding. *)
let unit =
  make ~encoding:"binary/null" ~encode:(fun () -> Ok Bytes.empty)
    ~decode:(fun data ->
      if Bytes.length data = 0 then Ok ()
      else Error (Error.codec ~message:"unit payload must be empty"))

(** Serializes a payload into a self-describing byte buffer used by the option
    wrapper. Framing is a [u32] metadata count, then each entry as a [u32]
    length-prefixed key and a [u32] length-prefixed value, then a [u32]
    length-prefixed data segment. All integers are unsigned 32-bit big-endian.
    Capturing the full metadata and data keeps the wrapper injective for
    arbitrarily nested options. *)
let serialize_payload payload =
  let buffer = Buffer.create 64 in
  let add_u32 length =
    let bytes = Bytes.create 4 in
    Bytes.set_int32_be bytes 0 (Int32.of_int length);
    Buffer.add_bytes buffer bytes
  in
  let add_field value =
    add_u32 (String.length value);
    Buffer.add_string buffer value
  in
  add_u32 (List.length payload.metadata);
  List.iter
    (fun (key, value) ->
      add_field key;
      add_field value)
    payload.metadata;
  add_u32 (Bytes.length payload.data);
  Buffer.add_bytes buffer payload.data;
  Buffer.to_bytes buffer

(** Reverses {!serialize_payload}. Any truncation, negative length, or trailing
    bytes is reported as a typed codec error so a corrupted envelope never
    raises to workflow code. *)
let deserialize_payload data =
  let length = Bytes.length data in
  let read_u32 position =
    if position + 4 > length then None
    else Some (Int32.to_int (Bytes.get_int32_be data position), position + 4)
  in
  let read_field position =
    match read_u32 position with
    | None -> None
    | Some (size, position) ->
        if size < 0 || position + size > length then None
        else Some (Bytes.sub_string data position size, position + size)
  in
  let rec read_metadata remaining position acc =
    if remaining = 0 then Some (List.rev acc, position)
    else
      match read_field position with
      | None -> None
      | Some (key, position) -> (
          match read_field position with
          | None -> None
          | Some (value, position) ->
              read_metadata (remaining - 1) position ((key, value) :: acc))
  in
  let malformed = Error (Error.codec ~message:"malformed optional payload") in
  match read_u32 0 with
  | None -> malformed
  | Some (count, position) ->
      if count < 0 then malformed
      else (
        match read_metadata count position [] with
        | None -> malformed
        | Some (metadata, position) -> (
            match read_u32 position with
            | None -> malformed
            | Some (data_length, position) ->
                if data_length < 0 || position + data_length <> length then
                  malformed
                else Ok { metadata; data = Bytes.sub data position data_length }))

(** Uses a null payload for [None] and delegates [Some] values to the supplied
    codec. When the inner codec would encode a value as [binary/null] (the
    marker reserved for [None]) or as the option wrapper itself, the inner
    payload is wrapped in an {!optional_wrapper_encoding} envelope so that
    decoding stays injective. This keeps [Some ()] and [Some None] distinct from
    [None] while leaving ordinary values such as [Some "text"] in their
    interoperable encoding. The complete callback representation is retained
    instead of assuming that one option codec has one fixed encoding name. *)
let option codec =
  {
    encode_payload =
      (function
      | None ->
          Ok { metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }
      | Some value -> (
          match encode codec value with
          | Error _ as error -> error
          | Ok payload -> (
              match encoding_metadata payload.metadata with
              | Error _ as error -> error
              | Ok encoding ->
                  let collides =
                    match encoding with
                    | Some name ->
                        String.equal name "binary/null"
                        || String.equal name optional_wrapper_encoding
                    | None -> false
                  in
                  if collides then
                    Ok
                      {
                        metadata = [ ("encoding", optional_wrapper_encoding) ];
                        data = serialize_payload payload;
                      }
                  else Ok payload)));
    decode_payload =
      (fun payload ->
        match encoding_metadata payload.metadata with
        | Error error -> Error error
        | Ok (Some "binary/null") when Bytes.length payload.data = 0 -> Ok None
        | Ok (Some "binary/null") ->
            Error (Error.codec ~message:"null payload must be empty")
        | Ok (Some name) when String.equal name optional_wrapper_encoding -> (
            match deserialize_payload payload.data with
            | Error error -> Error error
            | Ok inner -> Result.map Option.some (decode codec inner))
        | Ok _ -> Result.map Option.some (decode codec payload));
  }

(** Name used by every built-in JSON codec. It is the standard Temporal
    data-converter encoding, so other SDKs read and write these payloads. *)
let json_encoding = "json/plain"

(** Recognizes the JSON integer grammar: an optional minus sign followed by
    either a lone [0] or a non-zero digit and any further digits. Yojson
    prints [`Intlit] text verbatim, so an application-constructed literal must
    be checked before encoding or the payload could contain arbitrary text. *)
let valid_json_integer_literal literal =
  let length = String.length literal in
  let start = if length > 0 && literal.[0] = '-' then 1 else 0 in
  let is_digit character = character >= '0' && character <= '9' in
  let rec all_digits index =
    index = length || (is_digit literal.[index] && all_digits (index + 1))
  in
  start < length
  && is_digit literal.[start]
  && (literal.[start] <> '0' || start + 1 = length)
  && all_digits start

(** Checks that a Yojson value is representable as standard JSON text: every
    float is finite, every string and object key is valid UTF-8, and every
    integer literal follows the JSON grammar. The same check runs after
    parsing, because Yojson's reader accepts the non-standard [NaN] and
    [Infinity] tokens and overflows large exponents to infinity. Recursion
    depth is bounded by the nesting depth of the value. *)
let rec validate_json : Yojson.Safe.t -> (unit, Error.t) result = function
  | `Null | `Bool _ | `Int _ -> Ok ()
  | `Intlit literal ->
      if valid_json_integer_literal literal then Ok ()
      else Error (Error.codec ~message:"JSON integer literal is malformed")
  | `Float value ->
      if Float.is_finite value then Ok ()
      else
        Error
          (Error.codec
             ~message:"JSON numbers must be finite; NaN and infinities are rejected")
  | `String value ->
      if valid_utf_8 value then Ok ()
      else Error (Error.codec ~message:"JSON string contains invalid UTF-8")
  | `List items -> validate_json_list items
  | `Assoc fields -> validate_json_fields fields

(** Validates list elements in order, stopping at the first failure. *)
and validate_json_list = function
  | [] -> Ok ()
  | item :: rest -> (
      match validate_json item with
      | Error _ as error -> error
      | Ok () -> validate_json_list rest)

(** Validates object keys and values in order, stopping at the first failure. *)
and validate_json_fields = function
  | [] -> Ok ()
  | (key, value) :: rest ->
      if not (valid_utf_8 key) then
        Error (Error.codec ~message:"JSON object key contains invalid UTF-8")
      else (
        match validate_json value with
        | Error _ as error -> error
        | Ok () -> validate_json_fields rest)

(** Serializes a validated JSON value using strict standard syntax. *)
let encode_json_value value =
  match validate_json value with
  | Error _ as error -> error
  | Ok () -> Ok (Bytes.of_string (Yojson.Safe.to_string ~std:true value))

(** Parses exactly one JSON value and applies {!validate_json}, so non-finite
    numbers and invalid UTF-8 never reach an application decoder. *)
let decode_json_value data =
  match Yojson.Safe.from_string (Bytes.to_string data) with
  | exception Yojson.Json_error message ->
      Error (Error.codec ~message:("invalid JSON: " ^ message))
  | value -> Result.map (fun () -> value) (validate_json value)

(** Builds a [json/plain] codec from conversions to and from a Yojson value.
    Validation and parsing happen inside {!make}'s callbacks, so conversion
    exceptions receive the same typed, payload-free codec error as other
    application callbacks. *)
let json_conv ~to_json ~of_json =
  make ~encoding:json_encoding
    ~encode:(fun value -> encode_json_value (to_json value))
    ~decode:(fun data -> Result.bind (decode_json_value data) of_json)

(** Passes validated Yojson values through unchanged. *)
let json = json_conv ~to_json:Fun.id ~of_json:Result.ok

(** Accepts only JSON integer literals within OCaml's native [int] range.
    Yojson parses in-range integers as [`Int] and wider ones as [`Intlit]. *)
let int =
  json_conv
    ~to_json:(fun value -> `Int value)
    ~of_json:(function
      | `Int value -> Ok value
      | `Intlit _ ->
          Error (Error.codec ~message:"JSON integer is outside the OCaml int range")
      | _ -> Error (Error.codec ~message:"payload is not a JSON integer"))

(** Writes the exact decimal text of a signed 64-bit integer and reads back any
    JSON integer literal that fits that range. *)
let int64 =
  json_conv
    ~to_json:(fun value -> `Intlit (Int64.to_string value))
    ~of_json:(function
      | `Int value -> Ok (Int64.of_int value)
      | `Intlit literal -> (
          match Int64.of_string_opt literal with
          | Some value -> Ok value
          | None ->
              Error (Error.codec ~message:"JSON integer is outside the int64 range"))
      | _ -> Error (Error.codec ~message:"payload is not a JSON integer"))

(** Encodes booleans as the JSON literals [true] and [false]. *)
let bool =
  json_conv
    ~to_json:(fun value -> `Bool value)
    ~of_json:(function
      | `Bool value -> Ok value
      | _ -> Error (Error.codec ~message:"payload is not a JSON boolean"))

(** Encodes finite floats as JSON numbers and accepts any finite JSON number on
    decode. Integer literals are accepted because SDKs such as Go and
    TypeScript write integral floats without a fraction; values beyond 2^53 are
    rounded to the nearest float. A literal too large to represent is rejected
    rather than decoded as infinity.

    Go's JSON encoder writes negative zero as the integer literal [-0], which
    Yojson parses as [`Int 0]. The decoder therefore inspects the payload text
    for that one case so negative zero keeps its sign across SDKs. *)
let float =
  make ~encoding:json_encoding
    ~encode:(fun value -> encode_json_value (`Float value))
    ~decode:(fun data ->
      Result.bind (decode_json_value data) (function
      | `Float value -> Ok value
      | `Int 0
        when String.starts_with ~prefix:"-" (String.trim (Bytes.to_string data))
        ->
          Ok (-0.0)
      | `Int value -> Ok (Float.of_int value)
      | `Intlit literal -> (
          match Float.of_string_opt literal with
          | Some value when Float.is_finite value -> Ok value
          | _ ->
              Error
                (Error.codec ~message:"JSON number is outside the float range"))
      | _ -> Error (Error.codec ~message:"payload is not a JSON number")))
