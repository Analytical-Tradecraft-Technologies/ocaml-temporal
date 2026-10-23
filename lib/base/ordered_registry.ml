(** Intrusive doubly linked list used for O(1) registration removal.  See the
    interface for the ordering and single-Domain ownership contract. *)

(** One registration.  [value] is [None] once the node has been unlinked so a
    retained handle does not keep the registered closure or result alive.
    [linked] is the authoritative membership flag; [prev] and [next] are only
    meaningful while it is true.  [registry] lets a handle update the owning
    registry's endpoints and length without a separate lookup. *)
type 'value node = {
  mutable value : 'value option;
  mutable prev : 'value node option;
  mutable next : 'value node option;
  mutable linked : bool;
  registry : 'value t;
}

(** Registry endpoints and the number of linked nodes.  [first] is the oldest
    linked registration and [last] the newest, so a forward walk from [first]
    visits entries in registration order. *)
and 'value t = {
  mutable first : 'value node option;
  mutable last : 'value node option;
  mutable length : int;
}

(** A handle is the node itself, giving O(1) removal without a lookup. *)
type 'value handle = 'value node

(** Builds a registry with no endpoints. *)
let create () = { first = None; last = None; length = 0 }

(** Links a fresh node after the current [last] node. *)
let add registry value =
  let node =
    { value = Some value; prev = registry.last; next = None; linked = true;
      registry }
  in
  (match registry.last with
  | None -> registry.first <- Some node
  | Some last -> last.next <- Some node);
  registry.last <- Some node;
  registry.length <- registry.length + 1;
  node

(** Clears a node's links and value after it has left its registry.  Clearing
    [prev]/[next] also prevents a retained handle from keeping neighbouring
    registrations reachable. *)
let detach node =
  node.linked <- false;
  node.value <- None;
  node.prev <- None;
  node.next <- None

(** Splices a linked node out by rewiring its neighbours or the registry
    endpoints, then releases its value. *)
let remove node =
  if node.linked then (
    let registry = node.registry in
    (match node.prev with
    | None -> registry.first <- node.next
    | Some prev -> prev.next <- node.next);
    (match node.next with
    | None -> registry.last <- node.prev
    | Some next -> next.prev <- node.prev);
    registry.length <- registry.length - 1;
    detach node)

(** Reads the node's membership flag. *)
let is_linked node = node.linked

(** Reads the maintained linked-node count. *)
let length registry = registry.length

(** Checks the maintained count rather than walking the list. *)
let is_empty registry = registry.length = 0

(** Copies linked values into a fresh list in registration order. *)
let to_list registry =
  (* Walk newest-to-oldest so consing yields registration order without a
     separate reversal. *)
  let rec collect acc = function
    | None -> acc
    | Some node ->
        let acc =
          match node.value with Some value -> value :: acc | None -> acc
        in
        collect acc node.prev
  in
  collect [] registry.last

(** Snapshots the values, empties the registry, then unlinks every node so any
    outstanding handle's [remove] becomes a no-op. *)
let take_all registry =
  let values = to_list registry in
  let rec detach_all = function
    | None -> ()
    | Some node ->
        let next = node.next in
        detach node;
        detach_all next
  in
  let first = registry.first in
  registry.first <- None;
  registry.last <- None;
  registry.length <- 0;
  detach_all first;
  values

(** Drains the registry without returning its values. *)
let clear registry =
  let (_ : _ list) = take_all registry in
  ()
