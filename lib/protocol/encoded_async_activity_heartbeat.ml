(** Canonical async-heartbeat bytes produced by one encoder pass. See the
    interface for the ownership and validation contract. *)

(** The encoder's output string; OCaml strings are immutable, so no
    defensive copy is needed between the adapter and the supervisor. *)
type t = string

(** Validates and serializes once; the error is the encoder's own. *)
let encode heartbeat = Activity_protocol.encode_async_heartbeat heartbeat

(** Copies into a buffer owned by the caller. *)
let to_bytes value = Bytes.of_string value

(** Exposes the immutable document without copying. *)
let to_string value = value
