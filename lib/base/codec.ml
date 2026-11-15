type payload = Payload.t = { metadata : (string * string) list; data : bytes }

(** Stores the two operations that every codec must provide. Keeping these
    functions private ensures every decode checks the payload's encoding name. *)
type 'a t = {
  encode_payload : 'a -> (payload, Error.t) result;
  decode_payload : payload -> ('a, Error.t) result;
}

(** Finds the optional encoding marker while enforcing the object semantics of
    payload metadata. The base representation uses a list so unknown entries
    survive translation, but the JSON wire representation still requires every
    metadata name to be unique. *)
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

(** Builds a Temporal payload codec from two functions supplied by the caller:
    one that converts an OCaml value to bytes and one that converts bytes back
    to an OCaml value. On encode, it adds [("encoding", encoding)] to the
    payload metadata. On decode, it calls the byte decoder only when the
    payload contains that exact encoding value; other unique metadata entries
    do not affect decoding. Duplicate metadata names are rejected before the
    callback runs, matching the strict bridge protocol.

    Claiming {!optional_wrapper_encoding} is a programmer error: that name is
    reserved for the [option] combinator, so [make] raises [Invalid_argument]
    rather than letting a user payload masquerade as an option envelope. *)
let make ~encoding ~encode ~decode =
  if String.equal encoding optional_wrapper_encoding then
    invalid_arg
      (Printf.sprintf "Codec.make: encoding %S is reserved for Codec.option"
         optional_wrapper_encoding);
  let encode_payload value =
    Result.map
      (fun data -> { metadata = [ ("encoding", encoding) ]; data })
      (encode value)
  in
  let decode_payload payload =
    match encoding_metadata payload.metadata with
    | Error error -> Error error
    | Ok (Some actual) when String.equal actual encoding -> decode payload.data
    | Ok (Some actual) ->
        Error
          (Error.codec
             ~message:
               (Printf.sprintf "expected payload encoding %S, received %S"
                  encoding actual))
    | Ok None -> Error (Error.codec ~message:"payload has no encoding metadata")
  in
  { encode_payload; decode_payload }

(** Runs a codec's encoding callback without exposing its private record
    representation to callers, retaining payload ownership at the codec
    boundary. *)
let encode codec value = codec.encode_payload value

(* Applies the codec's decoding callback to one payload. *)
let decode codec payload = codec.decode_payload payload

(** Installs already validated payload-level callbacks without adding an
    encoding metadata entry. The public API uses this narrow bridge to turn an
    opaque public codec into the representation consumed by worker adapters;
    keeping it here avoids exposing the record representation. *)
let of_payload ~encode ~decode =
  { encode_payload = encode; decode_payload = decode }

(** Checks every byte of a string with OCaml's UTF-8 decoder. Advancing by the
    length of each decoded character ensures invalid continuation bytes are not
    accidentally skipped. *)
let valid_utf_8 value =
  let rec loop offset =
    if offset = String.length value then true
    else
      let decoded = String.get_utf_8_uchar value offset in
      Uchar.utf_decode_is_valid decoded
      && loop (offset + Uchar.utf_decode_length decoded)
  in
  loop 0

(** Longest accepted cancellation reason, in bytes: the bridge's per-string
    protocol limit, which the Rust command validator enforces again. *)
let max_cancellation_reason_bytes = 65_536

(** Checks the cancellation-reason text contract in a fixed order so the first
    violated rule names the failure. The order and messages match the earlier
    per-caller copies this replaces, which public tests observe. *)
let validate_cancellation_reason ~subject reason =
  let defect rule =
    Error (Error.defect ~message:(subject ^ " cancellation reason " ^ rule))
  in
  if String.equal reason "" then defect "must not be empty"
  else if String.contains reason '\000' then defect "must not contain NUL"
  else if String.length reason > max_cancellation_reason_bytes then
    defect "exceeds 65536 bytes"
  else if not (valid_utf_8 reason) then defect "must be valid UTF-8"
  else Ok ()

(** Converts an OCaml string to the JSON representation used in a Temporal
    [json/plain] payload. Yojson handles quoting and escaping. The explicit
    UTF-8 check prevents malformed strings from being stored by Temporal. *)
let encode_json_string value =
  if not (valid_utf_8 value) then
    Error (Error.codec ~message:"OCaml string is not valid UTF-8")
  else Ok (Bytes.of_string (Yojson.Safe.to_string (`String value)))

(** Reads one complete JSON value with Yojson and accepts it only when it is a
    string. Objects, arrays, numbers, malformed JSON, and trailing input are
    reported as codec errors instead of raising exceptions to workflow code. *)
let decode_json_string data =
  try
    match Yojson.Safe.from_string (Bytes.to_string data) with
    | `String value when valid_utf_8 value -> Ok value
    | `String _ -> Error (Error.codec ~message:"JSON string contains invalid UTF-8")
    | _ -> Error (Error.codec ~message:"payload is not a JSON string")
  with Yojson.Json_error message ->
    Error (Error.codec ~message:("invalid JSON string: " ^ message))

(** Uses the encoding name understood by the standard converters in other
    Temporal SDKs. *)
let string = make ~encoding:"json/plain" ~encode:encode_json_string ~decode:decode_json_string

let bytes =
  make ~encoding:"binary/plain"
    ~encode:(fun value -> Ok (Bytes.copy value))
    ~decode:(fun value -> Ok (Bytes.copy value))

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

(** Handles [None] itself, and uses the supplied codec for [Some value]. When
    the inner codec would encode a value as [binary/null] (the marker reserved
    for [None]) or as the option wrapper itself, the inner payload is wrapped in
    an {!optional_wrapper_encoding} envelope so that decoding stays injective.
    This keeps [Some ()] and [Some None] distinct from [None] while leaving
    ordinary values such as [Some "text"] in their interoperable encoding. A
    null payload containing data is rejected because the [None] marker must be
    empty. *)
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
