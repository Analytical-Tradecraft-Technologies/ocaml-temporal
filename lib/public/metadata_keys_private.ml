(** Memo and search-attribute key validation shared by every workflow-side
    operation that records those maps in history: the memo and
    search-attribute upserts and continue-as-new successor options in
    [Workflow], and child workflow starts in [Child_workflow] (#1135). One
    implementation keeps their accepted keys and diagnostics identical. The
    native protocol encoder and the Rust bridge re-check the same identifier
    rule, so these checks exist to report a programmer error at the call
    site, before a command is buffered, rather than as a later worker
    failure. *)

(** Largest key, in bytes, that the strict bridge JSON protocol accepts. *)
let max_key_bytes = 65_536

(** Checks one search-attribute or memo key and returns the problem, if any;
    [label] names the collection in the message. A key must be non-empty,
    free of NUL, at most {!max_key_bytes} bytes, and valid UTF-8. *)
let key_problem ~label key =
  if String.equal key "" then Some (Printf.sprintf "Temporal %s key is empty" label)
  else if String.contains key '\000' then
    Some (Printf.sprintf "Temporal %s key contains a NUL byte" label)
  else if String.length key > max_key_bytes then
    Some (Printf.sprintf "Temporal %s key exceeds 65536 bytes" label)
  else if not (Temporal_base.Codec.valid_utf_8 key) then
    Some (Printf.sprintf "Temporal %s key must be valid UTF-8" label)
  else None

(** Returns the first problem with the keys of one metadata map, checking
    each key with {!key_problem} and rejecting repeats, which the bridge's
    payload map could not represent; [None] when every key is valid. [label]
    (["search-attribute"] or ["memo"]) names the collection in the message.
    Repeats are found with a hash set seeded randomly, so a large map costs
    linear rather than quadratic time and precomputed colliding keys cannot
    restore the quadratic cost. *)
let keys_problem ~label values =
  let seen = Hashtbl.create ~random:true (List.length values) in
  let rec check = function
    | [] -> None
    | (key, _) :: rest -> (
        match key_problem ~label key with
        | Some _ as problem -> problem
        | None ->
            if Hashtbl.mem seen key then
              Some (Printf.sprintf "Temporal %s keys must be unique" label)
            else (
              Hashtbl.add seen key ();
              check rest))
  in
  check values
