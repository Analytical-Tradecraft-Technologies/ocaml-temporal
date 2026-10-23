(** Unit tests for [Temporal_base.Ordered_registry], the O(1)-removal
    registration list behind scheduler teardowns, condition waiters, scope
    cancellation hooks, and future observers (#847).  The registry's ordering
    contract is what keeps those runtime structures replay-deterministic, so
    these tests pin registration order across removal, draining, and reuse. *)

module R = Temporal_base.Ordered_registry

(** Fails with the scenario label when [expected] and [actual] differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Adding preserves registration order and maintains the length. *)
let test_add_order () =
  let registry = R.create () in
  expect "new registry empty" true (R.is_empty registry);
  List.iter (fun value -> ignore (R.add registry value)) [ 1; 2; 3; 4 ];
  expect "add order" [ 1; 2; 3; 4 ] (R.to_list registry);
  expect "add length" 4 (R.length registry);
  expect "to_list does not drain" [ 1; 2; 3; 4 ] (R.to_list registry)

(** Removing the head, a middle entry, and the tail keeps the survivors in
    registration order, and a second removal is a no-op. *)
let test_remove_positions () =
  let registry = R.create () in
  let handles = List.map (fun value -> R.add registry value) [ 1; 2; 3; 4; 5 ] in
  let handle index = List.nth handles index in
  R.remove (handle 0);
  R.remove (handle 2);
  R.remove (handle 4);
  expect "after removals" [ 2; 4 ] (R.to_list registry);
  expect "after removals length" 2 (R.length registry);
  expect "removed handle unlinked" false (R.is_linked (handle 2));
  expect "kept handle linked" true (R.is_linked (handle 1));
  R.remove (handle 2);
  expect "idempotent removal" [ 2; 4 ] (R.to_list registry);
  expect "idempotent removal length" 2 (R.length registry);
  ignore (R.add registry 6);
  expect "append after tail removal" [ 2; 4; 6 ] (R.to_list registry);
  R.remove (handle 1);
  R.remove (handle 3);
  expect "only appended entry" [ 6 ] (R.to_list registry)

(** Draining returns registration order, empties the registry, and makes the
    outstanding handles inert, including for later registrations. *)
let test_take_all () =
  let registry = R.create () in
  let first = R.add registry "a" in
  let _ = R.add registry "b" in
  expect "take_all order" [ "a"; "b" ] (R.take_all registry);
  expect "drained empty" true (R.is_empty registry);
  expect "drained handle unlinked" false (R.is_linked first);
  let later = R.add registry "c" in
  R.remove first;
  expect "stale removal ignored" [ "c" ] (R.to_list registry);
  R.clear registry;
  expect "cleared" [] (R.to_list registry);
  expect "cleared handle unlinked" false (R.is_linked later)

(** A removed entry no longer keeps its value reachable through the handle,
    which matters because runtime handles live in long-lived closures. *)
let test_removed_value_released () =
  let registry = R.create () in
  let weak = Weak.create 1 in
  let handle =
    let value = ref 42 in
    Weak.set weak 0 (Some value);
    R.add registry value
  in
  R.remove handle;
  Gc.full_major ();
  expect "removed value collected" false (Weak.check weak 0);
  ignore (Sys.opaque_identity handle)

(** Interleaved add/remove of many entries in FIFO order stays linear: this
    loop would be quadratic over a list with filter-based removal.  The order
    check proves only the newest window survives. *)
let test_fifo_churn () =
  let registry = R.create () in
  let window = Queue.create () in
  for value = 1 to 200_000 do
    Queue.push (R.add registry value) window;
    if Queue.length window > 3 then R.remove (Queue.pop window)
  done;
  expect "churn survivors" [ 199_998; 199_999; 200_000 ] (R.to_list registry)

(** Runs every registry scenario. *)
let () =
  test_add_order ();
  test_remove_positions ();
  test_take_all ();
  test_removed_value_released ();
  test_fifo_churn ()
