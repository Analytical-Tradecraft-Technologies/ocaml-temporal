(** Internal representation of a Temporal payload. The public [Payload] module
    exposes the same two fields so callers can pass raw payloads without seeing
    a codec's private implementation. *)
type t = { metadata : (string * string) list; data : bytes }

(** The exact [Codec.unit] representation: one [encoding] entry naming
    [binary/null] and no data. Every call allocates a fresh record, so callers
    may hand the result to code that retains it. *)
let unit_null () =
  { metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }

(** Exact structural comparison against [unit_null]. Extra metadata or any data
    byte means the payload carries information, so it is not canonical. *)
let is_unit_null payload =
  Bytes.length payload.data = 0
  &&
  match payload.metadata with
  | [ ("encoding", "binary/null") ] -> true
  | _ -> false

(** The outbound half of the zero-argument convention. Temporal's other SDKs,
    the CLI, and the Web UI send no payloads for a no-argument call, and some
    (Python in particular) bind every payload positionally, so the canonical
    unit payload becomes an empty list. Any other payload, including a null
    marker with extra metadata, is sent unchanged. Every inbound OCaml decoder
    maps [[]] back to [unit_null ()], so the rule is lossless between OCaml
    senders and receivers. *)
let input_arguments payload = if is_unit_null payload then [] else [ payload ]
