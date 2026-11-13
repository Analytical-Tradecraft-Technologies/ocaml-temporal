(** Validated metrics-export options for a shared Core runtime (#500,
    #1161).

    This is the pure half of [Temporal.Runtime.Metrics]: it checks the
    documented bounds and renders the strict telemetry document that the Rust
    bridge accepts. Rust re-applies the same rules, so this module only moves
    the error earlier and into OCaml's typed [result]. Values are immutable
    and safe to share between Domains.

    OTLP header values are credentials. They appear only in the document
    {!to_json} renders for the bridge, never in an error message; callers
    must not log that document. *)

(** One validated exporter configuration: a Prometheus scrape endpoint or an
    OTLP push exporter. *)
type t

(** OTLP transport: gRPC, or HTTP with protobuf bodies. *)
type otlp_protocol = [ `Grpc | `Http_protobuf ]

(** Aggregation temporality of exported sums and histograms. *)
type temporality = [ `Cumulative | `Delta ]

(** Most global tags one exporter accepts. *)
val max_global_tags : int

(** Longest accepted global tag name, in bytes. *)
val max_tag_name_bytes : int

(** Longest accepted global tag value, in bytes. *)
val max_tag_value_bytes : int

(** Longest accepted bind address, in bytes. *)
val max_bind_address_bytes : int

(** Label names Temporal Core already attaches to its series (plus the
    Prometheus histogram and summary labels). A global tag may not reuse one,
    because the collision would make Core's registry drop instruments. *)
val reserved_tag_names : string list

(** [prometheus ~bind_address ()] validates a Prometheus scrape endpoint.

    [bind_address] is an IP socket address: [a.b.c.d:port] with exactly
    four canonical decimal octets (no shorthand such as [127.1], and no
    octal, hexadecimal, or zero-padded octets), or [[ipv6]:port] without a
    zone identifier; host names are rejected so the listening interface is
    explicit. The port is canonical decimal (no leading zeros) from [0] to
    [65535]; port [0] selects an ephemeral port. The grammar is the same on
    every platform and matches the Rust bridge. Each global tag name must
    match [[A-Za-z_][A-Za-z0-9_]*], be at most {!max_tag_name_bytes} bytes,
    not start with [__], not be in {!reserved_tag_names}, and be unique;
    each value must be 1 to {!max_tag_value_bytes} bytes with no control
    characters; at most {!max_global_tags} tags are accepted. The boolean
    options default to [false], matching Temporal Core. Returns a constant
    message naming the first violated rule; the message never echoes the
    rejected value. *)
val prometheus :
  ?global_tags:(string * string) list ->
  ?counters_total_suffix:bool ->
  ?unit_suffix:bool ->
  ?durations_as_seconds:bool ->
  bind_address:string ->
  unit ->
  (t, string) result

(** Longest accepted OTLP collector endpoint, in bytes. *)
val max_endpoint_bytes : int

(** Most OTLP request headers one exporter sends. *)
val max_headers : int

(** Longest accepted OTLP header name, in bytes. *)
val max_header_name_bytes : int

(** Longest accepted OTLP header value, in bytes. *)
val max_header_value_bytes : int

(** Shortest accepted OTLP export period, in milliseconds. *)
val min_period_ms : int64

(** Longest accepted OTLP export period, in milliseconds (one hour). *)
val max_period_ms : int64

(** Temporal Core's default OTLP export period, in milliseconds. *)
val default_period_ms : int64

(** Header names the OTLP transports set themselves or that HTTP/2 forbids;
    every name starting with [grpc-] is reserved as well. *)
val reserved_header_names : string list

(** [otlp ~endpoint ()] validates an OTLP push exporter (#1161).

    [endpoint] is an [http://] or [https://] URL (lowercase scheme) of at
    most {!max_endpoint_bytes} bytes. Its host is a DNS name (labels of 1 to
    63 letters, digits and hyphens, not starting or ending with a hyphen, at
    most 253 bytes, whose last label is not all digits), a canonical
    dotted-quad IPv4 address, or a bracketed IPv6 address without a zone; an
    optional port is canonical decimal 1 to 65535. User information ([@] in
    the authority), a fragment ([#]), and any byte outside printable ASCII
    [!]..[~] are rejected; a path and query are kept. The Rust bridge
    applies the same grammar.

    At most {!max_headers} [headers]; each name is 1 to
    {!max_header_name_bytes} bytes of [[a-z0-9_.-]] starting with a letter
    or digit, does not end in [-bin] or start with [grpc-], is not in
    {!reserved_header_names}, and is unique; each value is 1 to
    {!max_header_value_bytes} bytes of printable ASCII (space through [~])
    without a leading or trailing space. [period_ms] is
    {!min_period_ms} to {!max_period_ms} and defaults to
    {!default_period_ms}; [protocol] defaults to [`Grpc] and [temporality]
    to [`Cumulative], as in Temporal Core. [global_tags] follow the
    {!prometheus} rules. Returns a constant message naming the first
    violated rule; it never echoes the endpoint, a header, or a tag
    value. *)
val otlp :
  ?headers:(string * string) list ->
  ?period_ms:int64 ->
  ?protocol:otlp_protocol ->
  ?temporality:temporality ->
  ?global_tags:(string * string) list ->
  ?durations_as_seconds:bool ->
  endpoint:string ->
  unit ->
  (t, string) result

(** The configured Prometheus bind address, exactly as validated; [None]
    for an OTLP exporter. *)
val bind_address : t -> string option

(** The configured OTLP endpoint, exactly as validated; [None] for a
    Prometheus exporter. *)
val endpoint : t -> string option

(** Renders the strict telemetry document for the Rust bridge. Global tags
    and headers keep caller order, and every option is written explicitly
    so the document does not depend on Rust-side defaults. The document of
    an OTLP exporter contains its header values. *)
val to_json : t -> string
