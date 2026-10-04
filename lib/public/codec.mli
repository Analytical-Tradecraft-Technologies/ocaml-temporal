(** The bytes and metadata that Temporal stores for one workflow or activity
    value. Most users work with typed codecs and do not construct this record
    directly. *)
type payload = Payload.t = { metadata : (string * string) list; data : bytes }

(** A codec converts values of type ['a] to and from Temporal payloads. *)
type 'a t

(** [make ~encoding ~encode ~decode] creates a codec from two functions. The
    [encode] function converts a value to bytes, and [decode] converts those
    bytes back to a value. The SDK writes [encoding] into the payload metadata
    and checks it before decoding, preventing the wrong codec from reading a
    payload. Errors returned by either callback are preserved. If a callback
    raises an ordinary exception, [Codec.encode] or [Codec.decode] returns a
    typed codec error without including the exception text, which might contain
    payload data. Internal terminal and shutdown control exceptions still
    unwind their workflow fiber.

    @raise Invalid_argument
      if [encoding] is ["binary/x-ocaml-optional"], which the SDK reserves for
      {!option}'s internal envelope. Choose any other encoding name. *)
val make :
  encoding:string ->
  encode:('a -> (bytes, Error.t) result) ->
  decode:(bytes -> ('a, Error.t) result) ->
  'a t

(** Converts a typed OCaml value into a Temporal payload. *)
val encode : 'a t -> 'a -> (payload, Error.t) result

(** Converts a Temporal payload back into a typed OCaml value after checking
    that its encoding metadata matches the codec. Duplicate metadata names are
    malformed because the bridge represents metadata as a JSON object; they
    produce a typed codec error before the decoder runs. *)
val decode : 'a t -> payload -> ('a, Error.t) result

(** Encodes strings as JSON using the standard Temporal [json/plain] encoding
    name. JSON is a convenient interoperability format, not a Temporal
    requirement; applications may define other codecs with [make]. *)
val string : string t

(** Encodes raw bytes using [binary/plain]. The codec copies mutable byte
    buffers so later changes by a caller cannot alter a stored payload. *)
val bytes : bytes t

(** Encodes [()] as an empty [binary/null] payload. *)
val unit : unit t

(** Encodes [None] as [binary/null]. A [Some value] normally uses the supplied
    codec and keeps that codec's encoding name, preserving cross-SDK
    interoperability. When the inner codec would itself produce a [binary/null]
    payload — as [unit] and a nested [option]'s own [None] do — the [Some]
    value is wrapped in a distinct envelope so it can never be mistaken for
    [None] on decode. This makes the codec injective: [Some ()], [Some None],
    and [None] all round-trip to different values. Duplicate metadata names are
    rejected when decoding any representation.

    The wrapper is used only for the [binary/null]-shaped inner values above; it
    never appears for ordinary payloads such as [string option] or [int option],
    which keep the standard [binary/null]/inner-encoding representation that
    other-language SDKs already understand. If you instead want an option to
    collapse onto a foreign nullable — deliberately letting [Some ()] read as
    absent for a non-OCaml consumer — do not use this combinator; define that
    exact wire representation yourself with {!make}. *)
val option : 'a t -> 'a option t
