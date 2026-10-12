(** Canonical activity-heartbeat bytes produced by one encoder pass. See the
    interface for the ownership and validation contract. *)

(** The encoder's output string. OCaml strings are immutable, so handing this
    value from the activity adapter to the supervisor mailbox needs no
    defensive copy. *)
type t = string

(** Validates and serializes once; the error is the encoder's own. *)
let encode heartbeat = Activity_protocol.encode_heartbeat heartbeat

(** Copies into a buffer owned by the caller, matching the single
    [Bytes.of_string] copy the supervisor made before this module existed. *)
let to_bytes value = Bytes.of_string value

(** Exposes the immutable document without copying. *)
let to_string value = value
