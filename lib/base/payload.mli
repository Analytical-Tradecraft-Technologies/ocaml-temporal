(** One value in the serialized form stored by Temporal. [metadata] describes
    how [data] was encoded. The list keeps entries the SDK does not recognize,
    allowing newer senders to add metadata without losing it. *)
type t = { metadata : (string * string) list; data : bytes }

(** Returns a freshly allocated canonical unit payload: exactly
    [[("encoding", "binary/null")]] metadata and empty data, the value produced
    by [Codec.unit] (and by [Codec.option] for [None]). Inbound decoders use it
    to reconstruct a single input from an empty payload list.

    Outbound, whether an input is sent as zero or one argument is decided by
    its codec, not by comparing a payload with this value: only [Codec.unit]
    is sent as zero arguments, so an option's [None] stays one [null]
    argument (#1012). *)
val unit_null : unit -> t
