(** Typed Temporal search attributes (#1154).

    A search attribute is a named, indexed value that Temporal's visibility
    store uses to filter executions, for example
    [Client.list_visibility ~query:"CustomerId = 'c-42'"]. Each name is
    registered in the namespace with one of Temporal's indexed value types,
    and Temporal Server decodes the stored payload itself, without the
    worker's codecs. A typed {!key} pairs the registered name with the OCaml
    type of its values, so {!encode} always produces the [json/plain] shape
    the server expects for that type, and {!decode} reads it back.

    {[
      let customer = Temporal.Search_attribute.keyword "CustomerId"
      let attempts = Temporal.Search_attribute.int64 "Attempts"

      (* In workflow code: *)
      Temporal.Workflow.upsert_typed_search_attributes
        Temporal.Search_attribute.[ set customer "c-42"; set attempts 3L ]

      (* At start, through the raw client parameter: *)
      let open Temporal.Result_syntax in
      let* search_attributes =
        Temporal.Search_attribute.(encode [ set customer "c-42" ])
      in
      Temporal.Client.start client ~search_attributes ...
    ]}

    The raw [(string * Payload.t) list] parameters of {!Client.start} and
    {!Workflow.upsert_search_attributes} remain available as an escape hatch
    for values this module does not model. A key does not register its name
    with Temporal; register it with the namespace (for example with
    [temporal operator search-attribute create]) before it is used. *)

(** Temporal's indexed value types, named as Temporal Server names them in
    payload ["type"] metadata: [`Text] is ["Text"] (full-text, tokenized),
    [`Keyword] is ["Keyword"] (exact match), [`Int] is ["Int"] (signed 64-bit),
    [`Double] is ["Double"], [`Bool] is ["Bool"], [`Datetime] is ["Datetime"],
    and [`Keyword_list] is ["KeywordList"]. *)
type kind =
  [ `Text | `Keyword | `Int | `Double | `Bool | `Datetime | `Keyword_list ]

(** A registered search-attribute name whose values have OCaml type ['a].
    Keys are immutable and may be defined once at module level and shared by
    workflow and client code. *)
type 'a key

(** A [Text] attribute: a UTF-8 string that visibility tokenizes for
    full-text matching. *)
val text : string -> string key

(** A [Keyword] attribute: a UTF-8 string matched exactly. *)
val keyword : string -> string key

(** An [Int] attribute: Temporal's signed 64-bit integer. *)
val int64 : string -> int64 key

(** A [Double] attribute: a finite float. *)
val double : string -> float key

(** A [Bool] attribute. *)
val bool : string -> bool key

(** A [Datetime] attribute. Values are encoded as RFC 3339 UTC timestamps
    with nanosecond precision and must lie between years 1 and 9999. *)
val datetime : string -> Time.t key

(** A [KeywordList] attribute: an ordered list of UTF-8 strings, each matched
    exactly. *)
val keyword_list : string -> string list key

(** Returns the registered name the key was created with. *)
val name : 'a key -> string

(** Returns the indexed value type of the key. *)
val kind : 'a key -> kind

(** One change to a run's search attributes: a value to set, or a name to
    remove. *)
type update

(** [set key value] sets [key] to [value]. *)
val set : 'a key -> 'a -> update

(** [unset key] removes [key] from the run's search attributes. It is
    encoded as the canonical [binary/null] payload, which Temporal treats
    as a removal in an upsert. A start has nothing to remove, so pass only
    {!set} updates to {!Client.start}. *)
val unset : 'a key -> update

(** Encodes [updates] as the raw [(name, payload)] list accepted by
    {!Client.start} and {!Workflow.upsert_search_attributes}, in the order
    given. A set value is a [json/plain] payload whose metadata also names
    the key's type (["type"], for example ["Keyword"]), as the other Temporal
    SDKs write it. Names must be non-empty, valid UTF-8, NUL-free, at most
    65,536 bytes, and unique within [updates]; a violation is a typed defect.
    A value that cannot be encoded — a non-UTF-8 string, a non-finite
    double, or a datetime outside years 1 to 9999 — is a typed codec error
    naming the key. *)
val encode : update list -> ((string * Payload.t) list, Error.t) result

(** [decode key attributes] reads [key] from a raw list such as
    {!Workflow.val-start_metadata}'s [search_attributes] or
    {!Client.execution_description}'s. It returns [Ok None] when the name is
    absent or holds the [binary/null] removal payload, and the first entry
    when the name repeats. A payload that is not [json/plain], whose ["type"]
    metadata names a different known Temporal type, or whose JSON does not
    match the key's type is a typed codec error. Server-added metadata
    entries are accepted. *)
val decode : 'a key -> (string * Payload.t) list -> ('a option, Error.t) result
