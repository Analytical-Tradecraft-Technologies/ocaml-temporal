(** The internal payload representation accepted by every codec. *)
type payload = Payload.t = { metadata : (string * string) list; data : bytes }

(** A bidirectional typed payload codec. Implementations must be deterministic:
    the same OCaml value must produce the same payload during replay. *)
type 'a t

(** [make ~encoding ~encode ~decode] builds a codec from two byte-conversion
    functions. Encoding a value first calls [encode], then returns a payload
    whose metadata contains [("encoding", encoding)]. Decoding checks that the
    payload has that exact encoding value before passing its bytes to [decode].
    Both conversion functions report invalid data as [Error.t]. *)
val make :
  encoding:string ->
  encode:('a -> (bytes, Error.t) result) ->
  decode:(bytes -> ('a, Error.t) result) ->
  'a t

(** Serializes a value into an owned payload. *)
val encode : 'a t -> 'a -> (payload, Error.t) result

(** Validates encoding metadata and deserializes a payload. *)
val decode : 'a t -> payload -> ('a, Error.t) result

(** Builds an internal codec from complete payload-level operations. This is
    used only by the public adapter boundary when a public opaque codec must
    be registered with the native worker. Unlike [make], this constructor does
    not rewrite metadata, so codecs such as [option] can preserve their
    value-dependent encoding names exactly. *)
val of_payload :
  encode:('a -> (payload, Error.t) result) ->
  decode:(payload -> ('a, Error.t) result) ->
  'a t

(** Reports whether every byte forms one complete, canonical UTF-8 sequence.
    Internal protocol-facing APIs use this before placing an OCaml string into
    JSON or Protobuf; public payload callers normally rely on [string]. *)
val valid_utf_8 : string -> bool

(** [validate_cancellation_reason ~subject reason] applies the text contract a
    workflow-issued cancellation reason must meet before it is recorded as a
    command in durable history: non-empty, free of NUL bytes, at most 65,536
    bytes, and valid UTF-8. A violation is an [Error.defect] whose message is
    [subject ^ " cancellation reason " ^ rule], for example
    ["child cancellation reason must not be empty"]. The child-workflow and
    external-workflow cancellation paths share it so their limits cannot drift
    apart from each other or from the Rust bridge's command validator. *)
val validate_cancellation_reason : subject:string -> string -> (unit, Error.t) result

(** Encodes OCaml strings as JSON using Temporal's [json/plain] encoding name.
    Yojson performs the JSON parsing and printing. Invalid UTF-8 and JSON values
    that are not strings produce [Error.t]. *)
val string : string t

(** Binary codec that copies on both encode and decode so ownership never
    aliases a caller's mutable [bytes]. *)
val bytes : bytes t

(** Empty-payload codec for [unit], encoded as [binary/null]. *)
val unit : unit t

(** [option codec] maps [None] to an empty [binary/null] payload and delegates
    non-null values to [codec]. Duplicate metadata names are rejected before
    either null handling or nested decoding. *)
val option : 'a t -> 'a option t
