(** Dynamically scoped values keyed by the calling system thread.

    [Domain.DLS] is shared by every system thread of a Domain, and the OCaml
    runtime may switch between those threads at any allocation or blocking
    call. A value that describes "what this thread is executing right now",
    such as the current workflow context or the scheduler owner, must therefore
    be keyed by the system thread as well as by the Domain. Otherwise a worker
    whose [run] loop is hosted on one system thread could observe or restore
    the binding installed by a sibling worker on another thread of the same
    Domain (#765).

    A binding is held in a Domain-local immutable map from [Thread.id] to its
    value, published through an atomic cell. Reads never lock: they load the
    cell and look up the calling thread, skipping the thread lookup entirely
    when no thread on the Domain has a binding. Writes replace the map with a
    compare-and-set retry, which is enough because only threads of the same
    Domain ever touch that cell and each attempt is a pure computation.

    Each entry exists only inside a {!with_value} extent and is removed when
    that extent ends normally or by an exception, so no entry outlives the
    code that installed it. The binding module owns no native resource. *)

type 'value t
(** A thread-keyed slot holding at most one ['value] per system thread. *)

val create : unit -> 'value t
(** Allocates a slot that is empty on every Domain and thread. Create slots
    once at module initialization; each slot allocates one Domain-local key. *)

val get : 'value t -> 'value option
(** Returns the value bound for the calling system thread, or [None] when that
    thread is outside every {!with_value} extent for this slot. *)

val with_value : 'value t -> 'value option -> (unit -> 'result) -> 'result
(** [with_value slot value action] binds [value] for the calling thread while
    [action] runs, where [None] means explicitly unbound, then restores that
    thread's previous binding even if [action] raises. Nested calls restore in
    LIFO order. The restore always targets the thread and Domain that
    installed the binding, so a continuation that suspended inside [action]
    and later finished elsewhere cannot leave a stale entry on the installing
    thread or corrupt the resuming thread's binding. *)

val bound_count : 'value t -> int
(** Number of system threads on the calling Domain that currently hold a
    binding in this slot. Intended for leak diagnostics and tests: it is zero
    whenever no {!with_value} extent is active on the Domain. *)
