(** Canonical workflow-completion bytes produced by one encoder pass. See the
    interface for the ownership and validation contract. *)

(** The encoder's output string. OCaml strings are immutable, so sharing this
    value between the worker's retained completion and the supervisor needs no
    defensive copy. *)
type t = string

(** Validates and serializes once; the error is the encoder's own. *)
let encode completion = Workflow_protocol.encode_completion completion

(** Copies into a buffer owned by the caller, matching the single
    [Bytes.of_string] copy the supervisor made before this module existed. *)
let to_bytes value = Bytes.of_string value

(** Exposes the immutable document without copying. *)
let to_string value = value
