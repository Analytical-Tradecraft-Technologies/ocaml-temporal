(** Thread-keyed dynamic bindings; the interface documents the concurrency
    and cleanup contract. *)

(** Immutable per-Domain map from [Thread.id] to the bound value. [Thread.id]
    is not documented as unique across Domains, which is why the map itself is
    Domain-local rather than process-global. *)
module Thread_map = Map.Make (Int)

(** The Domain-local cell for one slot. OCaml 5.2 and later keep the first
    value stored when two threads of one Domain race to initialize a key, so
    every thread of a Domain observes the same cell. *)
type 'value t = 'value Thread_map.t Atomic.t Domain.DLS.key

(** Allocates the Domain-local key; cells are created lazily per Domain. *)
let create () = Domain.DLS.new_key (fun () -> Atomic.make Thread_map.empty)

(** Identifies the calling system thread within its Domain. *)
let self () = Thread.id (Thread.self ())

(** The empty-map check keeps the common "no workflow installed" read from
    querying the thread identity at all. *)
let get slot =
  let bindings = Atomic.get (Domain.DLS.get slot) in
  if Thread_map.is_empty bindings then None
  else Thread_map.find_opt (self ()) bindings

(** Replaces [thread]'s entry in [cell]. A sibling thread of the same Domain
    may publish its own change between the read and the compare-and-set when
    the runtime switches threads during map allocation; retrying recomputes
    the update from the newer map so neither change is lost. *)
let rec publish cell thread value =
  let before = Atomic.get cell in
  let after =
    match value with
    | None -> Thread_map.remove thread before
    | Some value -> Thread_map.add thread value before
  in
  if not (Atomic.compare_and_set cell before after) then
    publish cell thread value

(** The cell and thread are captured before [action] so the restore targets the
    installing thread even if a continuation migrated before finishing. *)
let with_value slot value action =
  let cell = Domain.DLS.get slot in
  let thread = self () in
  let previous = Thread_map.find_opt thread (Atomic.get cell) in
  publish cell thread value;
  Fun.protect ~finally:(fun () -> publish cell thread previous) action

(** Counts bound threads on the calling Domain only. *)
let bound_count slot = Thread_map.cardinal (Atomic.get (Domain.DLS.get slot))
