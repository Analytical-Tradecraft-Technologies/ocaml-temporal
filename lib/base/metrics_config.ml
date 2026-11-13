(** The Rust bridge's [telemetry.rs] and [otlp.rs] define the same bounds;
    the two must change together (see docs/reference/core-bridge.md). *)

type otlp_protocol = [ `Grpc | `Http_protobuf ]
type temporality = [ `Cumulative | `Delta ]

(** Options of a Prometheus scrape endpoint (#500). *)
type prometheus = {
  bind_address : string;
  global_tags : (string * string) list;
  counters_total_suffix : bool;
  unit_suffix : bool;
  durations_as_seconds : bool;
}

(** Options of an OTLP push exporter (#1161). [headers] hold credentials:
    they are written only into the bridge document and never into a
    message. *)
type otlp = {
  endpoint : string;
  protocol : otlp_protocol;
  headers : (string * string) list;
  period_ms : int64;
  temporality : temporality;
  otlp_global_tags : (string * string) list;
  otlp_durations_as_seconds : bool;
}

(** Exactly one exporter, as the bridge requires. *)
type t = Prometheus of prometheus | Otlp of otlp

let max_global_tags = 16
let max_tag_name_bytes = 64
let max_tag_value_bytes = 256
let max_bind_address_bytes = 128
let max_endpoint_bytes = 2048
let max_headers = 16
let max_header_name_bytes = 64
let max_header_value_bytes = 4096
let min_period_ms = 100L
let max_period_ms = 3_600_000L
let default_period_ms = 1_000L

let reserved_header_names =
  [
    "connection";
    "content-encoding";
    "content-length";
    "content-type";
    "host";
    "keep-alive";
    "proxy-connection";
    "te";
    "trailer";
    "transfer-encoding";
    "upgrade";
    "user-agent";
  ]

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
    (Prometheus
       {
         bind_address;
         global_tags;
         counters_total_suffix;
         unit_suffix;
         durations_as_seconds;
       })

(** The one message for every rejected endpoint. It never echoes the
    endpoint, whose query string might carry a credential. Identical to
    [ENDPOINT_RULE] in the Rust bridge's [otlp.rs]. *)
let endpoint_rule =
  "metrics OTLP endpoint must be an http:// or https:// URL of at most 2048 \
   bytes with a host name, IPv4 or bracketed IPv6 host, an optional port 1 \
   to 65535, no user information, no fragment, and no spaces or control \
   characters"

(** Whether [label] is one DNS label: 1 to 63 ASCII letters, digits or
    hyphens, not starting or ending with a hyphen. *)
let valid_dns_label label =
  let length = String.length label in
  length >= 1 && length <= 63
  && String.for_all
       (function 'A' .. 'Z' | 'a' .. 'z' | '0' .. '9' | '-' -> true | _ -> false)
       label
  && label.[0] <> '-'
  && label.[length - 1] <> '-'

(** Whether [host] is a bracketed IPv6 address without a zone, a dotted-quad
    IPv4 address, or a DNS name of at most 253 bytes. URL parsers read a name
    whose last label is all digits as IPv4 ([127.1] means [127.0.0.1]), so
    such a host must be a canonical dotted quad. *)
let valid_host host =
  let length = String.length host in
  if length >= 2 && host.[0] = '[' && host.[length - 1] = ']' then
    valid_ipv6 (String.sub host 1 (length - 2))
  else
    let labels = String.split_on_char '.' host in
    let last = List.nth labels (List.length labels - 1) in
    if all_digits last then valid_ipv4 host
    else length <= 253 && List.for_all valid_dns_label labels

(** Validates the collector URL with the grammar documented in the
    interface; the Rust bridge applies the same grammar before handing the
    URL to Core. *)
let validate_endpoint endpoint =
  let invalid = Error endpoint_rule in
  let length = String.length endpoint in
  let printable = String.for_all (fun character -> character >= '!' && character <= '~') in
  if length = 0 || length > max_endpoint_bytes || (not (printable endpoint))
     || String.contains endpoint '#'
  then invalid
  else
    let rest =
      if String.starts_with ~prefix:"http://" endpoint then
        Some (String.sub endpoint 7 (length - 7))
      else if String.starts_with ~prefix:"https://" endpoint then
        Some (String.sub endpoint 8 (length - 8))
      else None
    in
    match rest with
    | None -> invalid
    | Some rest ->
        let authority_end =
          let rec find index =
            if index >= String.length rest then index
            else match rest.[index] with '/' | '?' -> index | _ -> find (index + 1)
          in
          find 0
        in
        let authority = String.sub rest 0 authority_end in
        if String.contains authority '@' then invalid
        else
          (* A bracketed IPv6 host contains colons, so the port separator is
             the last colon that has no closing bracket after it. *)
          let host_end =
            match String.rindex_opt authority ':' with
            | Some colon
              when not
                     (String.contains
                        (String.sub authority colon (String.length authority - colon))
                        ']') ->
                colon
            | _ -> String.length authority
          in
          let host = String.sub authority 0 host_end in
          let port_ok =
            host_end = String.length authority
            ||
            let port =
              String.sub authority (host_end + 1)
                (String.length authority - host_end - 1)
            in
            canonical_decimal ~max_digits:5 port
            &&
            let value = int_of_string port in
            value >= 1 && value <= 65_535
          in
          if port_ok && valid_host host then Ok () else invalid

(** Whether [name] is an accepted header name; see the interface. *)
let valid_header_name name =
  let length = String.length name in
  length >= 1
  && length <= max_header_name_bytes
  && (match name.[0] with 'a' .. 'z' | '0' .. '9' -> true | _ -> false)
  && String.for_all
       (function 'a' .. 'z' | '0' .. '9' | '-' | '_' | '.' -> true | _ -> false)
       name
  && (not (String.ends_with ~suffix:"-bin" name))
  && (not (String.starts_with ~prefix:"grpc-" name))
  && not (List.mem name reserved_header_names)

(** Whether [value] is an accepted header value: printable ASCII (space
    through [~]) without a leading or trailing space. *)
let valid_header_value value =
  let length = String.length value in
  length >= 1
  && length <= max_header_value_bytes
  && String.for_all (fun character -> character >= ' ' && character <= '~') value
  && value.[0] <> ' '
  && value.[length - 1] <> ' '

(** Applies the header count, name, value, and uniqueness rules. Messages
    name only the violated rule: a header value is a credential. *)
let validate_headers headers =
  if List.length headers > max_headers then
    Error (Printf.sprintf "metrics OTLP headers accepts at most %d headers" max_headers)
  else
    let rec check seen = function
      | [] -> Ok ()
      | (name, value) :: rest ->
          if not (valid_header_name name) then
            Error
              (Printf.sprintf
                 "metrics OTLP header names must be 1 to %d bytes of \
                  [a-z0-9_.-] starting with a letter or digit, must not end \
                  in -bin or start with grpc-, and must not be a header the \
                  transport sets"
                 max_header_name_bytes)
          else if not (valid_header_value value) then
            Error
              (Printf.sprintf
                 "metrics OTLP header values must be 1 to %d bytes of \
                  printable ASCII without leading or trailing spaces"
                 max_header_value_bytes)
          else if List.mem name seen then
            Error "metrics OTLP header names must be unique"
          else check (name :: seen) rest
    in
    check [] headers

let otlp ?(headers = []) ?(period_ms = default_period_ms) ?(protocol = `Grpc)
    ?(temporality = `Cumulative) ?(global_tags = [])
    ?(durations_as_seconds = false) ~endpoint () =
  let* () = validate_endpoint endpoint in
  let* () =
    if Int64.compare period_ms min_period_ms < 0
       || Int64.compare period_ms max_period_ms > 0
    then
      Error
        (Printf.sprintf "metrics OTLP period must be %Ld ms to %Ld ms"
           min_period_ms max_period_ms)
    else Ok ()
  in
  let* () = validate_headers headers in
  let* () = validate_global_tags global_tags in
  Ok
    (Otlp
       {
         endpoint;
         protocol;
         headers;
         period_ms;
         temporality;
         otlp_global_tags = global_tags;
         otlp_durations_as_seconds = durations_as_seconds;
       })

let bind_address = function
  | Prometheus { bind_address; _ } -> Some bind_address
  | Otlp _ -> None

let endpoint = function
  | Prometheus _ -> None
  | Otlp { endpoint; _ } -> Some endpoint

(** One [{"name": ..., "value": ...}] member of a tag or header list. *)
let name_value (name, value) =
  `Assoc [ ("name", `String name); ("value", `String value) ]

let to_json = function
  | Prometheus config ->
      Yojson.Safe.to_string
        (`Assoc
          [
            ( "prometheus",
              `Assoc
                [
                  ("bind_address", `String config.bind_address);
                  ("global_tags", `List (List.map name_value config.global_tags));
                  ("counters_total_suffix", `Bool config.counters_total_suffix);
                  ("unit_suffix", `Bool config.unit_suffix);
                  ("durations_as_seconds", `Bool config.durations_as_seconds);
                ] );
          ])
  | Otlp config ->
      Yojson.Safe.to_string
        (`Assoc
          [
            ( "otlp",
              `Assoc
                [
                  ("endpoint", `String config.endpoint);
                  ( "protocol",
                    `String
                      (match config.protocol with
                      | `Grpc -> "grpc"
                      | `Http_protobuf -> "http_protobuf") );
                  ("headers", `List (List.map name_value config.headers));
                  (* Bounded to one hour above, so the conversion is exact. *)
                  ("period_ms", `Int (Int64.to_int config.period_ms));
                  ( "temporality",
                    `String
                      (match config.temporality with
                      | `Cumulative -> "cumulative"
                      | `Delta -> "delta") );
                  ( "global_tags",
                    `List (List.map name_value config.otlp_global_tags) );
                  ("durations_as_seconds", `Bool config.otlp_durations_as_seconds);
                ] );
          ])
