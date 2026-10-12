(** The Rust bridge's [telemetry.rs] defines the same bounds; the two must
    change together (see docs/reference/core-bridge.md). *)

type t = {
  bind_address : string;
  global_tags : (string * string) list;
  counters_total_suffix : bool;
  unit_suffix : bool;
  durations_as_seconds : bool;
}

let max_global_tags = 16
let max_tag_name_bytes = 64
let max_tag_value_bytes = 256
let max_bind_address_bytes = 128

let reserved_tag_names =
  [
    "activity_type";
    "eager";
    "failure_reason";
    "le";
    "namespace";
    "operation";
    "poller_type";
    "quantile";
    "service_name";
    "status_code";
    "task_queue";
    "worker_type";
    "workflow_type";
  ]

(** Result-bind notation keeps validation a flat sequence of checks. *)
let ( let* ) = Result.bind

(** Whether [text] is a non-empty run of ASCII decimal digits. *)
let all_digits text =
  text <> "" && String.for_all (fun character -> character >= '0' && character <= '9') text

(** Whether [text] is a canonical decimal number of at most [max_digits]
    digits: no sign, no spaces, and no leading zero except ["0"] itself, so
    no spelling is ambiguous between decimal and octal. *)
let canonical_decimal ~max_digits text =
  all_digits text
  && String.length text <= max_digits
  && (text = "0" || text.[0] <> '0')

(** Whether [port] is a canonical decimal TCP port, 0 (ephemeral) to 65535. *)
let valid_port port =
  canonical_decimal ~max_digits:5 port && int_of_string port <= 65_535

(** Whether [host] is a dotted-quad IPv4 address: exactly four canonical
    decimal octets 0..255. This parser is deliberately platform-independent;
    system parsers such as [Unix.inet_addr_of_string] on Windows accept
    shorthand ([1], [127.1]), octal, and hexadecimal forms. *)
let valid_ipv4 host =
  match String.split_on_char '.' host with
  | [ _; _; _; _ ] as octets ->
      List.for_all
        (fun octet ->
          canonical_decimal ~max_digits:3 octet && int_of_string octet <= 255)
        octets
  | _ -> false

(** Whether [group] is one IPv6 group of 1 to 4 hexadecimal digits. *)
let valid_hex_group group =
  let length = String.length group in
  length >= 1 && length <= 4
  && String.for_all
       (function '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' -> true | _ -> false)
       group

(** Number of 16-bit groups in a colon-separated IPv6 fragment, or [None] if
    it is malformed. [allow_ipv4_tail] permits a final dotted quad, which
    counts as two groups. An empty fragment has zero groups. *)
let ipv6_groups ~allow_ipv4_tail fragment =
  if fragment = "" then Some 0
  else
    let parts = String.split_on_char ':' fragment in
    let count = List.length parts in
    let rec check index = function
      | [] -> Some 0
      | [ last ] when index = count - 1 && allow_ipv4_tail && valid_ipv4 last ->
          Some 2
      | part :: rest ->
          if valid_hex_group part then
            Option.map (fun groups -> groups + 1) (check (index + 1) rest)
          else None
    in
    check 0 parts

(** Whether [host] is an RFC 4291 textual IPv6 address: eight groups, or
    fewer with exactly one [::], optionally ending in a dotted quad. Zone
    identifiers ([%scope]) are rejected. *)
let valid_ipv6 host =
  let find_double_colon () =
    let rec search index =
      if index + 1 >= String.length host then None
      else if host.[index] = ':' && host.[index + 1] = ':' then Some index
      else search (index + 1)
    in
    search 0
  in
  match find_double_colon () with
  | None -> ipv6_groups ~allow_ipv4_tail:true host = Some 8
  | Some index -> (
      let head = String.sub host 0 index in
      let tail = String.sub host (index + 2) (String.length host - index - 2) in
      (* A second [::], or a stray colon next to it, leaves an empty group
         that [valid_hex_group] rejects. *)
      match
        (ipv6_groups ~allow_ipv4_tail:false head, ipv6_groups ~allow_ipv4_tail:true tail)
      with
      | Some head_groups, Some tail_groups -> head_groups + tail_groups <= 7
      | _ -> false)

(** Validates [a.b.c.d:port] or [[ipv6]:port] with the strict grammar above,
    identically on every platform; Rust applies the same grammar. *)
let validate_bind_address address =
  let invalid =
    Error
      "metrics bind_address must be an IP socket address such as \
       127.0.0.1:9464 or [::1]:9464"
  in
  if address = "" || String.length address > max_bind_address_bytes then invalid
  else
    match String.rindex_opt address ':' with
    | None -> invalid
    | Some colon ->
        let host = String.sub address 0 colon in
        let port =
          String.sub address (colon + 1) (String.length address - colon - 1)
        in
        let host_ok =
          let length = String.length host in
          if length >= 2 && host.[0] = '[' && host.[length - 1] = ']' then
            valid_ipv6 (String.sub host 1 (length - 2))
          else valid_ipv4 host
        in
        if host_ok && valid_port port then Ok () else invalid

(** Whether [name] is an accepted Prometheus label name for a global tag. *)
let valid_tag_name name =
  let identifier_start = function
    | 'A' .. 'Z' | 'a' .. 'z' | '_' -> true
    | _ -> false
  in
  let identifier_char character =
    identifier_start character || (character >= '0' && character <= '9')
  in
  name <> ""
  && String.length name <= max_tag_name_bytes
  && identifier_start name.[0]
  && String.for_all identifier_char name
  && not (String.starts_with ~prefix:"__" name)
  && not (List.mem name reserved_tag_names)

(** Whether [value] is a non-empty, bounded, valid UTF-8 tag value without
    control characters. Rust rejects Unicode control characters too, so the
    check decodes rather than inspecting bytes. *)
let valid_tag_value value =
  let length = String.length value in
  let rec scan index =
    if index >= length then true
    else
      let decoded = String.get_utf_8_uchar value index in
      if not (Uchar.utf_decode_is_valid decoded) then false
      else
        let code = Uchar.to_int (Uchar.utf_decode_uchar decoded) in
        if code < 0x20 || (code >= 0x7f && code <= 0x9f) then false
        else scan (index + Uchar.utf_decode_length decoded)
  in
  length > 0 && length <= max_tag_value_bytes && scan 0

(** Applies the count, name, value, and uniqueness rules in caller order. *)
let validate_global_tags tags =
  if List.length tags > max_global_tags then
    Error
      (Printf.sprintf "metrics global_tags accepts at most %d tags"
         max_global_tags)
  else
    let rec check seen = function
      | [] -> Ok ()
      | (name, value) :: rest ->
          if not (valid_tag_name name) then
            Error
              (Printf.sprintf
                 "metrics global tag names must match [A-Za-z_][A-Za-z0-9_]*, \
                  be at most %d bytes, not start with __, and not be a label \
                  Temporal Core uses"
                 max_tag_name_bytes)
          else if not (valid_tag_value value) then
            Error
              (Printf.sprintf
                 "metrics global tag values must be 1 to %d bytes without \
                  control characters"
                 max_tag_value_bytes)
          else if List.mem name seen then
            Error "metrics global tag names must be unique"
          else check (name :: seen) rest
    in
    check [] tags

let prometheus ?(global_tags = []) ?(counters_total_suffix = false)
    ?(unit_suffix = false) ?(durations_as_seconds = false) ~bind_address () =
  let* () = validate_bind_address bind_address in
  let* () = validate_global_tags global_tags in
  Ok
    {
      bind_address;
      global_tags;
      counters_total_suffix;
      unit_suffix;
      durations_as_seconds;
    }

let bind_address config = config.bind_address

let to_json config =
  let tag (name, value) =
    `Assoc [ ("name", `String name); ("value", `String value) ]
  in
  Yojson.Safe.to_string
    (`Assoc
      [
        ( "prometheus",
          `Assoc
            [
              ("bind_address", `String config.bind_address);
              ("global_tags", `List (List.map tag config.global_tags));
              ("counters_total_suffix", `Bool config.counters_total_suffix);
              ("unit_suffix", `Bool config.unit_suffix);
              ("durations_as_seconds", `Bool config.durations_as_seconds);
            ] );
      ])
