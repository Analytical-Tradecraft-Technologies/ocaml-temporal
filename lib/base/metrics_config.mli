(** Validated metrics-export options for a shared Core runtime (#500).

    This is the pure half of [Temporal.Runtime.Metrics]: it checks the
    documented bounds and renders the strict telemetry document that the Rust
    bridge accepts. Rust re-applies the same rules, so this module only moves
    the error earlier and into OCaml's typed [result]. Values are immutable
    and safe to share between Domains. *)

(** One validated exporter configuration. *)
type t

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

(** The configured bind address, exactly as validated. *)
val bind_address : t -> string

(** Renders the strict telemetry document for the Rust bridge. Global tags
    keep caller order, and every option is written explicitly so the
    document does not depend on Rust-side defaults. *)
val to_json : t -> string
