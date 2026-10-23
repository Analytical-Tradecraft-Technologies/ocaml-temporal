(** Raw Temporal payloads: the encoded bytes and metadata of one value. *)

(** The serialized form of one value passed through Temporal. Temporal stores
    the byte sequence without interpreting it; metadata tells SDK codecs how
    those bytes were encoded. JSON is one possible encoding, not a requirement. *)
type t = {
  metadata : (string * string) list;
      (** Information used by data converters, normally including an
          ["encoding"] entry such as ["json/plain"] or ["binary/plain"]. *)
  data : bytes;
      (** The exact serialized bytes stored by Temporal. *)
}
