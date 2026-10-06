(** Insertion-ordered registration list with constant-time removal.

    Workflow runtime structures such as scheduler teardown ledgers, condition
    waiters, scope cancellation hooks, and future observers register one entry
    per pending operation and later remove that entry when the operation
    settles.  A plain list makes each removal a linear scan, so settling [n]
    entries costs O(n{^ 2}).  This registry is an intrusive doubly linked list:
    {!add} and {!remove} are O(1), and every traversal follows registration
    order.

    Ordering is part of the contract.  Traversals never depend on hashing,
    physical addresses, or allocation order other than the explicit order of
    {!add} calls, so callers that resume continuations or emit commands from a
    snapshot remain deterministic and replay-safe.

    The registry is not thread-safe.  Every operation on one registry and its
    handles must run on the single Domain that owns it (for the workflow
    runtime, the owning scheduler's Domain). *)

(** A mutable registry of values in registration order. *)
type 'value t

(** The removal capability returned by {!add} for one registration.  It is
    owned by the code that registered the value and stays valid after the
    entry has been removed or drained, in which case operations on it are
    no-ops. *)
type 'value handle

(** Creates an empty registry. *)
val create : unit -> 'value t

(** Appends [value] after every current entry and returns its removal handle.
    O(1). *)
val add : 'value t -> 'value -> 'value handle

(** Unlinks the entry and drops the registry's reference to its value, so a
    settled registration does not keep its closure alive.  Idempotent, and a
    no-op after the entry was drained by {!take_all} or {!clear}.  O(1). *)
val remove : 'value handle -> unit

(** Reports whether the handle's entry is still linked into its registry. *)
val is_linked : 'value handle -> bool

(** Returns the number of linked entries.  O(1). *)
val length : 'value t -> int

(** Reports whether no entries are linked.  O(1). *)
val is_empty : 'value t -> bool

(** Returns the linked values in registration order without changing the
    registry.  Later {!add} or {!remove} calls do not affect the returned
    snapshot.  O(n). *)
val to_list : 'value t -> 'value list

(** Detaches every entry and returns their values in registration order.
    Outstanding handles become unlinked, so later {!remove} calls on them are
    no-ops.  Entries added after this call belong to the now-empty registry.
    O(n). *)
val take_all : 'value t -> 'value list

(** Detaches every entry and discards their values.  O(n). *)
val clear : 'value t -> unit
