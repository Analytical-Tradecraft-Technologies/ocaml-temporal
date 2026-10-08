(** Typed conversion between OCaml values and Temporal payloads.

    Every workflow, activity, signal, query, and update definition carries
    codecs for its input and output. The SDK provides codecs for common
    primitive and JSON values; {!make} builds one from a pair of encode and
    decode functions. *)

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

    Raises [Invalid_argument] if [encoding] is ["binary/x-ocaml-optional"],
    which the SDK reserves for {!option}'s internal envelope. Choose any other
    encoding name. *)
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

(** {1 JSON codecs}

    The following codecs use the standard Temporal [json/plain] encoding with
    one JSON value as the payload body, the representation that the Go, Java,
    Python, TypeScript, and .NET default data converters read and write.
    Encoding emits strict standard JSON. Decoding parses exactly one JSON value
    (surrounding whitespace is permitted) and returns a typed codec error for
    malformed text, a value of the wrong JSON type, a number outside the target
    range, a non-finite number, or invalid UTF-8. No codec raises for bad
    input. *)

(** Encodes a native OCaml integer as a JSON integer. Decoding accepts only a
    JSON integer literal (no fraction or exponent, so [1.0] and [1e3] are
    rejected) within [[min_int, max_int]]; on 64-bit platforms that is the
    63-bit range [[-2{^62}, 2{^62}-1]], narrower than the signed 64-bit range
    other SDKs may produce. Use {!int64} for values that need the full 64-bit
    range. Note that JavaScript-based SDKs represent numbers as doubles and
    lose precision above [2{^53}]. *)
val int : int t

(** Encodes a signed 64-bit integer as its exact decimal JSON integer, the
    representation other SDKs use for [int64]/[long]. Decoding accepts any JSON
    integer literal in the signed 64-bit range and rejects fractions,
    exponents, and out-of-range literals. *)
val int64 : int64 t

(** Encodes a boolean as the JSON literal [true] or [false]. *)
val bool : bool t

(** Encodes a finite float as a JSON number that parses back to the same
    float, including the sign of [-0.0]. Because JSON has no representation for
    [nan], [infinity], or [neg_infinity], encoding them returns a typed codec
    error rather than emitting the non-standard [NaN]/[Infinity] tokens.
    Decoding accepts any finite JSON number, including integer literals written
    without a fraction by other SDKs; integers beyond [2{^53}] are rounded to
    the nearest float. The non-standard [NaN] and [Infinity] tokens, and
    literals too large to represent (such as [1e400]), are rejected. *)
val float : float t

(** Encodes an arbitrary JSON document. The value is validated before encoding:
    floats must be finite, strings and object keys must be valid UTF-8, and
    [`Intlit] text must follow the JSON integer grammar. Decoded values satisfy
    the same invariants. Object fields keep their order, and duplicate object
    keys are preserved as Yojson represents them. *)
val json : Yojson.Safe.t t

(** [json_conv ~to_json ~of_json] creates a [json/plain] codec for an
    application type by converting through [Yojson.Safe.t], for example with
    functions generated by a Yojson deriver for a record type. [to_json] output
    receives the same validation as {!json}; [of_json] receives only a parsed,
    validated value and should return a typed error such as
    [Error.codec ~message] for a shape it does not accept. Exceptions raised by
    either function become a typed codec error, as with {!make}.

    Combine this helper with Yojson when a list, pair, or record should be one
    interoperable JSON payload; payload codecs such as {!bytes} or {!option}
    are not JSON values and so are not nested inside JSON automatically. *)
val json_conv :
  to_json:('a -> Yojson.Safe.t) ->
  of_json:(Yojson.Safe.t -> ('a, Error.t) result) ->
  'a t

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
