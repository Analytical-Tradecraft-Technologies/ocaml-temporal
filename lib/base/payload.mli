(** One value in the serialized form stored by Temporal. [metadata] describes
    how [data] was encoded. The list keeps entries the SDK does not recognize,
    allowing newer senders to add metadata without losing it. *)
type t = { metadata : (string * string) list; data : bytes }

(** Returns a freshly allocated canonical unit payload: exactly
    [[("encoding", "binary/null")]] metadata and empty data, the value produced
    by [Codec.unit] (and by [Codec.option] for [None]). Inbound decoders use it
    to reconstruct a single input from an empty payload list. *)
val unit_null : unit -> t

(** [true] only for a payload structurally equal to [unit_null ()]. *)
val is_unit_null : t -> bool

(** Converts one encoded single-value input into Temporal's repeated payload
    list. The canonical unit payload becomes [[]], matching the zero-argument
    convention of the CLI, Web UI, and other SDKs; any other payload becomes a
    one-element list. This is the single outbound rule shared by client
    start/signal/update, activity scheduling, child workflow start,
    continue-as-new, and external signal. It is replay-safe because Core's
    nondeterminism checks compare command identity and type, not input. *)
val input_arguments : t -> t list
