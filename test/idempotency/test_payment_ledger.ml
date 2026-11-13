(** Tests of the example payment ledger's atomic idempotency contract
    (#504): one posting per key however often and however concurrently it
    is requested, no suppression of a distinct key with an identical
    posting, conflicting reuse of a key detected, and records shared by
    every opener of the directory. *)

open Payment_ledger

(** Fails with [label] and the message when [result] is an error. *)
let ok label = function
  | Ok value -> value
  | Error message -> failwith (label ^ ": " ^ message)

(** Fails with [message] unless [condition] holds. *)
let check condition message = if not condition then failwith message

(** Opens a ledger in a fresh temporary directory. Each test uses its own,
    so tests cannot observe each other's records. *)
let fresh_ledger () =
  ok "open" (open_ledger (Filename.temp_dir "payment-ledger-test-" ""))

(** A correlation for [attempt] of activity ["post-line-1"] in run
    ["run-1"], labelled with [worker]. *)
let correlation ?(run_id = "run-1") ?(worker = "worker-a") attempt =
  { workflow_id = "order-workflow"; run_id; activity_id = "post-line-1"; attempt; worker }

(** The posting most tests use. *)
let charge = { account = "acct-ada"; amount_cents = 2500 }

(** A retried request with the same key and posting is a duplicate that
    returns the original record, including the original attempt's
    correlation, and posts nothing new. *)
let test_retry_is_duplicate () =
  let ledger = fresh_ledger () in
  let first = ok "first post" (post ledger ~key:"order-1/line-1" charge (correlation 1)) in
  let original =
    match first with
    | Committed record -> record
    | other -> failwith ("first post was " ^ string_of_outcome_kind (outcome_kind other))
  in
  let second =
    ok "retried post"
      (post ledger ~key:"order-1/line-1" charge (correlation ~worker:"worker-b" 2))
  in
  (match second with
   | Duplicate record ->
       check (record = original) "a duplicate did not return the original record";
       check (record.committed_by.attempt = 1) "the record names the wrong attempt"
   | other -> failwith ("retried post was " ^ string_of_outcome_kind (outcome_kind other)));
  check (ok "records" (records ledger) = [ original ]) "a retry posted a second record";
  match ok "journal" (journal ledger) with
  | [ { answer = Committed_kind; requested_by = { attempt = 1; worker = "worker-a"; _ }; _ };
      { answer = Duplicate_kind; requested_by = { attempt = 2; worker = "worker-b"; _ }; _ } ]
    -> ()
  | entries -> failwith (Printf.sprintf "unexpected journal of %d entries" (List.length entries))

(** Two distinct operations with an identical posting have distinct keys and
    are both posted; the second is not mistaken for a duplicate. *)
let test_identical_posting_distinct_key () =
  let ledger = fresh_ledger () in
  let first = ok "line 1" (post ledger ~key:"order-1/line-1" charge (correlation 1)) in
  let second = ok "line 2" (post ledger ~key:"order-1/line-2" charge (correlation 1)) in
  check (outcome_kind first = Committed_kind) "line 1 was not committed";
  check (outcome_kind second = Committed_kind)
    "an identical posting under a distinct key was suppressed";
  match ok "records" (records ledger) with
  | [ { key = "order-1/line-1"; posting = p1; _ }; { key = "order-1/line-2"; posting = p2; _ } ]
    when p1 = charge && p2 = charge -> ()
  | _ -> failwith "the ledger does not hold both identical postings"

(** Reusing a key for a different posting is a conflict: nothing is posted
    and the existing record is reported. *)
let test_conflicting_reuse () =
  let ledger = fresh_ledger () in
  ignore (ok "original" (post ledger ~key:"order-1/line-1" charge (correlation 1)));
  let other = { charge with amount_cents = 9900 } in
  (match ok "conflicting" (post ledger ~key:"order-1/line-1" other (correlation 1)) with
   | Conflict record -> check (record.posting = charge) "the conflict reported the wrong record"
   | outcome -> failwith ("a conflicting reuse was " ^ string_of_outcome_kind (outcome_kind outcome)));
  match ok "records" (records ledger) with
  | [ { posting; _ } ] when posting = charge -> ()
  | _ -> failwith "a conflicting reuse changed the ledger"

(** Concurrent requests for one key, from several Domains at once, commit
    exactly one posting; every other request is a duplicate of it, and no
    temporary file is left behind. *)
let test_concurrent_requests () =
  let ledger = fresh_ledger () in
  let domains =
    List.init 8 (fun index ->
        Domain.spawn (fun () ->
            post ledger ~key:"order-1/line-1" charge
              (correlation ~worker:(Printf.sprintf "domain-%d" index) 1)))
  in
  let outcomes = List.map (fun domain -> ok "concurrent post" (Domain.join domain)) domains in
  let committed =
    List.filter_map (function Committed record -> Some record | _ -> None) outcomes
  in
  let committed =
    match committed with
    | [ record ] -> record
    | records -> failwith (Printf.sprintf "%d requests committed" (List.length records))
  in
  List.iter
    (function
      | Committed _ -> ()
      | Duplicate record -> check (record = committed) "a duplicate saw a different record"
      | Conflict _ -> failwith "an identical concurrent request conflicted")
    outcomes;
  check (ok "records" (records ledger) = [ committed ]) "concurrent requests posted twice";
  check (List.length (ok "journal" (journal ledger)) = 8) "a request is missing from the journal";
  let leftovers =
    List.filter
      (fun name -> String.starts_with ~prefix:"partial-" name)
      (Array.to_list (Sys.readdir (directory ledger)))
  in
  check (leftovers = []) "a temporary record file was left behind"

(** Another opener of the same directory (as a separate process would be)
    sees the committed records, and its request for an existing key is a
    duplicate. A stray temporary file, as a process killed before its link
    leaves behind, is never read as a record. *)
let test_shared_directory () =
  let ledger = fresh_ledger () in
  ignore (ok "post" (post ledger ~key:"order-1/line-1" charge (correlation 1)));
  Out_channel.with_open_bin
    (Filename.concat (directory ledger) "partial-999999-0")
    (fun channel -> output_string channel "key \"order-1/line-2\"\naccount");
  let reopened = ok "reopen" (open_ledger (directory ledger)) in
  check (List.length (ok "records" (records reopened)) = 1) "a stray temporary file was read";
  (match ok "find" (find reopened ~key:"order-1/line-1") with
   | Some { committed_by = { attempt = 1; _ }; _ } -> ()
   | _ -> failwith "the reopened ledger does not find the record");
  check (ok "find missing" (find reopened ~key:"order-1/line-2") = None)
    "a key that was never posted was found";
  match ok "post again" (post reopened ~key:"order-1/line-1" charge (correlation 2)) with
  | Duplicate _ -> ()
  | outcome -> failwith ("the reopened ledger answered " ^ string_of_outcome_kind (outcome_kind outcome))

(** Keys and labels with separators, quotes and newlines round-trip, and
    invalid keys are rejected before any file is written. *)
let test_keys () =
  let ledger = fresh_ledger () in
  let key = "order \"7\"/line-1\nsecond line" in
  let correlation = { (correlation 1) with worker = "worker \"x\"" } in
  (match ok "post" (post ledger ~key charge correlation) with
   | Committed _ -> ()
   | _ -> failwith "an unusual key was not committed");
  (match ok "find" (find ledger ~key) with
   | Some record ->
       check (record.key = key && record.committed_by = correlation)
         "an unusual key or label did not round-trip"
   | None -> failwith "an unusual key was not found");
  check (Result.is_error (post ledger ~key:"" charge correlation)) "an empty key was accepted";
  check
    (Result.is_error (post ledger ~key:(String.make 121 'k') charge correlation))
    "an over-long key was accepted";
  check (List.length (ok "records" (records ledger)) = 1) "a rejected key wrote a record"

let () =
  test_retry_is_duplicate ();
  test_identical_posting_distinct_key ();
  test_conflicting_reuse ();
  test_concurrent_requests ();
  test_shared_directory ();
  test_keys ()
