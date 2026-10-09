(** Private owner of one Core runtime shared by several SDK instances (#832).

    Without sharing, each client or worker supervisor owns a complete native
    graph including its own Core runtime and Tokio pool. A shared runtime
    splits that graph at one seam: this value owns the Core runtime, and each
    attached supervisor still owns its own graph (client, worker, cleanup
    thread) on its own owner Domain, plus one native reference to the shared
    Core. Supervisors therefore never share mutable native state; the "one
    supervisor actor per SDK instance owns its complete handle graph" rule
    is unchanged, with Core moved out of the graph into this owner.

    Ownership rules:
    - Every attachment is represented by one {!lease}, acquired here before
      any native graph exists and released exactly once after that graph has
      been closed (or never created). {!release} is idempotent.
    - {!shutdown} succeeds only when no lease is outstanding; otherwise it
      returns [Still_attached] and changes nothing. Clients and workers must
      therefore be shut down before their runtime.
    - After a successful {!shutdown}, Core and its threads have been
      destroyed and {!acquire} returns [None].
    - Rust keeps Core alive with a native reference per attached graph, so
      even a defect in this accounting (for example a GC-abandoned instance)
      cannot free Core under a live graph; it can only delay destruction.

    All functions may be called from any Domain. *)

(** One shareable Core runtime and its attachment count. *)
type t

(** Permission for one SDK instance to hold a graph on a {!t}. *)
type lease

(** Why {!shutdown} did not release the runtime. *)
type shutdown_error =
  | Still_attached of int
      (** That many leases are still outstanding; nothing was released. *)
  | Native of Temporal_core_bridge.Native_bridge.error
      (** Native release failed. The runtime is closed regardless: its
          handle was detached before Rust was called. *)

(** Creates the Core runtime with an optional Tokio worker bound, validated
    as in {!Temporal_core_bridge.Native_bridge.runtime_create}. *)
val create :
  ?worker_threads:int ->
  unit ->
  (t, Temporal_core_bridge.Native_bridge.error) result

(** Reserves one attachment, or returns [None] once the runtime is shut down.
    The caller must eventually pass the lease to {!release}. *)
val acquire : t -> lease option

(** Creates one native graph on the lease's runtime. The lease keeps the
    runtime open for the whole call, so this never races with {!shutdown}.
    The returned graph is closed by its owner exactly like any other graph,
    and the lease must stay held until that close has returned. *)
val attach :
  lease ->
  ( Temporal_core_bridge.Native_bridge.runtime,
    Temporal_core_bridge.Native_bridge.error )
  result

(** Returns one attachment. Repeated calls on the same lease are no-ops. *)
val release : lease -> unit

(** Number of leases currently outstanding. *)
val attached : t -> int

(** Whether {!shutdown} has released the runtime. *)
val is_shut_down : t -> bool

(** Releases the Core runtime if no lease is outstanding, waiting (with the
    OCaml runtime lock released) until Core and its threads are destroyed.
    Repeating a successful call returns [Ok ()]. Concurrent callers are
    serialized, so every [Ok] return happens after destruction. *)
val shutdown : t -> (unit, shutdown_error) result
