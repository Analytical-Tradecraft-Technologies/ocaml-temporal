(** Server-free tests of the idempotent payments example workflow (#504),
    run in-process with [Temporal.Testing] against a real ledger directory:
    key scope across lines, runs and orders; an attempt whose completion is
    lost after its posting committed; and a key reused for a different
    posting. *)

open Temporal

(** Fails with [label] and the error message when [result] is an error. *)
let ok label = function
  | Ok value -> value
  | Error error -> failwith (label ^ ": " ^ Error.message error)

(** Fails with [label] and the message when a ledger call fails. *)
let ledger_ok label = function
  | Ok value -> value
  | Error message -> failwith (label ^ ": " ^ message)

(** Fails with [message] unless [condition] holds. *)
let check condition message = if not condition then failwith message

(** Opens a ledger in a fresh temporary directory. *)
let fresh_ledger () =
  ledger_ok "open"
    (Payment_ledger.open_ledger (Filename.temp_dir "idempotent-payments-test-" ""))

(** An order of two identical lines. *)
let order order_id : Idempotent_payments.order =
  { order_id; account = "acct-ada"; lines = [ 2500; 2500 ] }

(** Creates an environment whose only activity is the real ledger activity,
    built with [after_commit], and calls [use] with it; the environment is
    always released. *)
let with_environment ?after_commit ledger use =
  let environment =
    ok "create environment"
      (Testing.create
         ~workflows:[ Testing.workflow Idempotent_payments.post_order_workflow ]
         ~activities:
           [
             Testing.activity
               (Idempotent_payments.post_line ~ledger ~worker:"testing" ?after_commit ());
           ]
         ())
  in
  Fun.protect ~finally:(fun () -> Testing.shutdown environment) (fun () -> use environment)

(** Posts [order] as workflow [id] and returns the run ID and the result. *)
let post environment ~id order =
  let handle =
    ok "start" (Testing.start ~id environment Idempotent_payments.post_order_workflow order)
  in
  (Testing.run_id handle, Testing.result handle)

(** Returns the keys and committing (run, attempt) of every record. *)
let committed ledger =
  List.map
    (fun (record : Payment_ledger.record) ->
      (record.key, record.committed_by.run_id, record.committed_by.attempt))
    (ledger_ok "records" (Payment_ledger.records ledger))

(** Returns the journal as (key, run, attempt, answer) tuples. *)
let requests ledger =
  List.map
    (fun (entry : Payment_ledger.journal_entry) ->
      (entry.journal_key, entry.requested_by.run_id, entry.requested_by.attempt, entry.answer))
    (ledger_ok "journal" (Payment_ledger.journal ledger))

(** Two identical lines get distinct keys and are both posted; a new run of
    the same order (as a workflow retry or a resubmission would start) finds
    both posted and posts nothing; a different order with identical lines
    posts its own. *)
let test_key_scope () =
  let ledger = fresh_ledger () in
  with_environment ledger (fun environment ->
      let a = order "order-a" in
      let run_1, output = post environment ~id:"order-a" a in
      check
        (ok "first run" output = Idempotent_payments.expected_output a)
        "unexpected receipts for order-a";
      check
        (committed ledger = [ ("order-a/line-1", run_1, 1); ("order-a/line-2", run_1, 1) ])
        "two identical lines were not posted under two keys";
      let run_2, output = post environment ~id:"order-a-resubmitted" a in
      check (run_2 <> run_1) "the resubmission did not start a new run";
      check
        (ok "resubmission" output = Idempotent_payments.expected_output a)
        "the resubmission returned different receipts";
      check
        (committed ledger = [ ("order-a/line-1", run_1, 1); ("order-a/line-2", run_1, 1) ])
        "the resubmission posted again";
      let b = order "order-b" in
      let run_3, output = post environment ~id:"order-b" b in
      check
        (ok "order-b" output = Idempotent_payments.expected_output b)
        "unexpected receipts for order-b";
      check
        (committed ledger
        = [
            ("order-a/line-1", run_1, 1);
            ("order-a/line-2", run_1, 1);
            ("order-b/line-1", run_3, 1);
            ("order-b/line-2", run_3, 1);
          ])
        "an order with identical lines was suppressed";
      check
        (requests ledger
        = [
            ("order-a/line-1", run_1, 1, Payment_ledger.Committed_kind);
            ("order-a/line-2", run_1, 1, Committed_kind);
            ("order-a/line-1", run_2, 1, Duplicate_kind);
            ("order-a/line-2", run_2, 1, Duplicate_kind);
            ("order-b/line-1", run_3, 1, Committed_kind);
            ("order-b/line-2", run_3, 1, Committed_kind);
          ])
        "unexpected ledger journal")

(** The first attempt of line 1 commits its posting and then fails before
    reporting it, as if its completion were lost. The retry sends the same
    key, is answered as a duplicate, and returns the original receipt; the
    line is posted once, by attempt 1, and the run completes normally. *)
let test_lost_completion () =
  let ledger = fresh_ledger () in
  let lost = Atomic.make false in
  let after_commit = function
    | Payment_ledger.Committed _ when not (Atomic.exchange lost true) ->
        Error
          (Error.make ~category:`Activity ~error_type:"CompletionLost"
             ~message:"simulated loss of the completion after the commit" ())
    | _ -> Ok ()
  in
  with_environment ~after_commit ledger (fun environment ->
      let a = order "order-lost" in
      let run_id, output = post environment ~id:"order-lost" a in
      check
        (ok "run" output = Idempotent_payments.expected_output a)
        "a lost completion changed the receipts";
      check
        (committed ledger
        = [ ("order-lost/line-1", run_id, 1); ("order-lost/line-2", run_id, 1) ])
        "a lost completion posted a line twice";
      check
        (requests ledger
        = [
            ("order-lost/line-1", run_id, 1, Payment_ledger.Committed_kind);
            ("order-lost/line-1", run_id, 2, Duplicate_kind);
            ("order-lost/line-2", run_id, 1, Committed_kind);
          ])
        "the redelivered attempt was not answered as a duplicate")

(** A resubmission that reuses an order ID for different amounts is a key
    conflict: the activity fails non-retryably, the workflow fails, and the
    ledger is unchanged. *)
let test_conflict () =
  let ledger = fresh_ledger () in
  with_environment ledger (fun environment ->
      let a = order "order-c" in
      let _run, output = post environment ~id:"order-c" a in
      ignore (ok "original" output);
      let before = committed ledger in
      let _run, output =
        post environment ~id:"order-c-changed" { a with lines = [ 9900 ] }
      in
      (match output with
       | Ok receipts -> failwith ("a conflicting order completed: " ^ receipts)
       | Error _ -> ());
      check (committed ledger = before) "a conflicting order changed the ledger";
      match List.rev (requests ledger) with
      | ("order-c/line-1", _, 1, Payment_ledger.Conflict_kind) :: _ -> ()
      | _ -> failwith "the conflicting request was not answered as a conflict")

(** Decodes [json] with [codec], as a payload another SDK or tool produced
    would be decoded. *)
let decode codec json =
  Codec.decode codec (ok "encode" (Codec.encode Codec.json json))

(** The JSON codecs look fields up by name: members in another order decode
    to the same value, and a missing, duplicate, unknown or mistyped field is
    a typed error rather than a misread value. Each codec also round-trips
    its own encoding. *)
let test_codecs () =
  let order : Idempotent_payments.order =
    { order_id = "order-r"; account = "acct-ada"; lines = [ 2500; 2500 ] }
  in
  check
    (decode Idempotent_payments.order_codec
       (`Assoc
         [
           ("lines", `List [ `Int 2500; `Int 2500 ]);
           ("account", `String "acct-ada");
           ("order_id", `String "order-r");
         ])
    = Ok order)
    "a reordered order was not decoded by field name";
  let request : Idempotent_payments.posting_request =
    { idempotency_key = "order-r/line-1"; account = "acct-ada"; amount_cents = 2500 }
  in
  check
    (decode Idempotent_payments.posting_request_codec
       (`Assoc
         [
           ("amount_cents", `Int 2500);
           ("idempotency_key", `String "order-r/line-1");
           ("account", `String "acct-ada");
         ])
    = Ok request)
    "a reordered posting request was not decoded by field name";
  let round_trip codec value =
    Codec.decode codec (ok "encode" (Codec.encode codec value)) = Ok value
  in
  check (round_trip Idempotent_payments.order_codec order) "an order did not round-trip";
  check
    (round_trip Idempotent_payments.posting_request_codec request)
    "a posting request did not round-trip";
  let key = ("idempotency_key", `String "order-r/line-1") in
  let account = ("account", `String "acct-ada") in
  let amount = ("amount_cents", `Int 2500) in
  List.iter
    (fun (label, fields) ->
      match decode Idempotent_payments.posting_request_codec (`Assoc fields) with
      | Ok _ -> failwith ("a posting request with " ^ label ^ " was accepted")
      | Error error ->
          check
            ((Error.view error).category = `Codec)
            ("a posting request with " ^ label ^ " was not a codec error"))
    [
      ("a missing field", [ key; account ]);
      ("a duplicate field", [ key; account; amount; ("account", `String "acct-eve") ]);
      ("an unknown field", [ key; account; amount; ("currency", `String "EUR") ]);
      ("a mistyped field", [ key; account; ("amount_cents", `String "2500") ]);
    ];
  check
    (Result.is_error
       (decode Idempotent_payments.order_codec
          (`Assoc
            [
              ("order_id", `String "order-r");
              ("account", `String "acct-ada");
              ("lines", `List [ `Int 2500; `String "2500" ]);
            ])))
    "an order with a non-integer line was accepted"

let () =
  test_key_scope ();
  test_codecs ();
  test_lost_completion ();
  test_conflict ()
