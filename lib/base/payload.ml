(** Internal representation of a Temporal payload. The public [Payload] module
    exposes the same two fields so callers can pass raw payloads without seeing
    a codec's private implementation. *)
type t = { metadata : (string * string) list; data : bytes }

(** The exact [Codec.unit] representation: one [encoding] entry naming
    [binary/null] and no data. Every call allocates a fresh record, so callers
    may hand the result to code that retains it. *)
let unit_null () =
  { metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }
