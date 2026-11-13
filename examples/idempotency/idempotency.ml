(** Self-checking idempotent side-effect example (#504).

    One process runs a worker for {!Idempotent_payments} against a ledger
    directory it creates, and a client driver that posts orders and checks
    the ledger afterwards:

    + An order with two identical lines (same account, same amount). Each
      line has its own idempotency key, so the ledger must hold two records,
      both committed by the run's first attempts.
    + The same order is submitted again, as a new run under the closed run's
      workflow ID. The keys come from the order ID, not the run, so the new
      run's requests must all be answered as duplicates, the ledger must not
      change, and the run must return the same receipts.
    + A different order with exactly the same lines. Its keys differ, so its
      two lines must be posted as two more records.

    Redelivery after a worker dies between the ledger commit and the
    activity's completion is exercised by the activity idempotency live
    regression, which reuses these definitions across a worker restart.

    It prints one line per scenario and exits 0 only if all three behaved as
    described. It uses the task queue [TEMPORAL_TASK_QUEUE] with an
    [-idempotency] suffix and fresh order and workflow IDs, so it can be
    rerun in the same namespace, and removes its ledger directory when it
    finishes. *)

open Temporal

(** Returns a defect describing a failed example check. *)
let check_failed message =
  Error.defect ~message:("idempotency example: " ^ message)

(** Fails with [message] unless [condition] holds. *)
let require condition message =
  if condition then Ok () else Error (check_failed message)

(** Creates a worker for the example's workflow and activity against
    [ledger], runs it on a new Domain, calls [use], and always shuts the
    worker down afterwards. *)
let with_worker (config : Example_support.Config.t) ~task_queue ~ledger use =
  let open Result_syntax in
  let identity = "ocaml-temporal-example-idempotency-worker" in
  let* worker =
    Worker.create ~target_url:config.target_url ~namespace:config.namespace
      ~identity ~task_queue
      ~workflows:[ Worker.workflow Idempotent_payments.post_order_workflow ]
      ~activities:
        [
          Worker.activity
            (Idempotent_payments.post_line ~ledger ~worker:identity ());
        ]
      ()
  in
  let domain =
    Domain.spawn (fun () ->
        Example_support.Lifecycle.protect_result "worker run" (fun () ->
            Worker.run worker))
  in
  let result =
    Example_support.Lifecycle.protect_result "example scenario" (fun () ->
        use ())
  in
  let shutdown_result =
    Example_support.Lifecycle.protect_result "worker shutdown" (fun () ->
        Worker.shutdown worker)
  in
  let run_result = Domain.join domain in
  let* value = result in
  let* () = shutdown_result in
  let* () = run_result in
  Ok value

(** Posts [order] under the workflow ID [id] and waits for that exact run.
    Returns the run ID and the workflow output. The execution timeout ends
    a run that a broken worker would otherwise leave open. *)
let post_order client ~task_queue ~id order =
  let open Result_syntax in
  let* handle =
    Client.start client ~execution_timeout:(Duration.of_ms 120_000L)
      ~workflow:Idempotent_payments.post_order_workflow ~task_queue ~id
      ~input:order ()
  in
  let* run_id =
    match Client.run_id handle with
    | Some run_id -> Ok run_id
    | None -> Error (check_failed "a started workflow handle has no run ID")
  in
  let* terminal = Client.wait handle in
  match terminal with
  | Client.Completed { output; _ } -> Ok (run_id, output)
  | Failed { error; _ } | Timed_out { error; _ } ->
      Error (check_failed ("workflow failed: " ^ Error.message error))
  | Cancelled error | Terminated error ->
      Error (check_failed ("workflow ended: " ^ Error.message error))
  | Continued_as_new _ -> Error (check_failed "workflow continued as new")

(** Converts a ledger failure into an example failure. *)
let ledger_result = function
  | Ok value -> Ok value
  | Error message -> Error (check_failed ("ledger: " ^ message))

(** Returns the records of [order], ordered by key. *)
let order_records ledger (order : Idempotent_payments.order) =
  Result.map
    (List.filter (fun (record : Payment_ledger.record) ->
         String.starts_with ~prefix:(order.order_id ^ "/") record.key))
    (ledger_result (Payment_ledger.records ledger))

(** Returns the journal entries for [order]'s keys made by run [run_id]. *)
let run_requests ledger (order : Idempotent_payments.order) ~run_id =
  Result.map
    (List.filter (fun (entry : Payment_ledger.journal_entry) ->
         String.starts_with ~prefix:(order.order_id ^ "/") entry.journal_key
         && String.equal entry.requested_by.run_id run_id))
    (ledger_result (Payment_ledger.journal ledger))

(** Checks that [order] has exactly one record per line, each with that
    line's key and amount, all committed by the first attempt of run
    [run_id]. *)
let check_posted_by ledger (order : Idempotent_payments.order) ~run_id =
  let open Result_syntax in
  let* records = order_records ledger order in
  let expected =
    List.mapi
      (fun index amount_cents ->
        ( Idempotent_payments.line_key ~order_id:order.order_id
            ~line:(index + 1),
          amount_cents ))
      order.lines
  in
  let actual =
    List.map
      (fun (record : Payment_ledger.record) ->
        (record.key, record.posting.amount_cents))
      records
  in
  let* () =
    require (actual = expected)
      (Printf.sprintf "order %s has %d ledger records, expected %d"
         order.order_id (List.length records) (List.length expected))
  in
  require
    (List.for_all
       (fun (record : Payment_ledger.record) ->
         String.equal record.posting.account order.account
         && String.equal record.committed_by.run_id run_id
         && record.committed_by.attempt = 1)
       records)
    ("order " ^ order.order_id
   ^ " was not posted by the first attempts of its own run")

(** Checks that run [run_id] asked once per line of [order] and got [answer]
    every time. *)
let check_answers ledger order ~run_id answer =
  let open Result_syntax in
  let* requests = run_requests ledger order ~run_id in
  require
    (List.length requests = List.length order.Idempotent_payments.lines
    && List.for_all
         (fun (entry : Payment_ledger.journal_entry) -> entry.answer = answer)
         requests)
    (Printf.sprintf "run %s did not get one %s answer per line" run_id
       (Payment_ledger.string_of_outcome_kind answer))

(** Scenario 1: an order with two identical lines posts both. Returns the
    run ID. *)
let identical_lines client ledger ~task_queue ~id order =
  let open Result_syntax in
  let* run_id, output = post_order client ~task_queue ~id order in
  let* () =
    require
      (String.equal output (Idempotent_payments.expected_output order))
      ("unexpected receipts: " ^ output)
  in
  let* () = check_posted_by ledger order ~run_id in
  let* () = check_answers ledger order ~run_id Payment_ledger.Committed_kind in
  Ok run_id

(** Scenario 2: a new run of the same order, under the same workflow ID,
    posts nothing new and returns the same receipts. [first_run_id] is the
    run that posted the order. *)
let resubmitted_order client ledger ~task_queue ~id ~first_run_id order =
  let open Result_syntax in
  let* run_id, output = post_order client ~task_queue ~id order in
  let* () =
    require
      (not (String.equal run_id first_run_id))
      "the resubmitted order did not start a new run"
  in
  let* () =
    require
      (String.equal output (Idempotent_payments.expected_output order))
      ("the resubmitted order returned different receipts: " ^ output)
  in
  let* () = check_posted_by ledger order ~run_id:first_run_id in
  check_answers ledger order ~run_id Payment_ledger.Duplicate_kind

(** Removes the example's ledger directory and the files in it. A failure
    to clean up is reported on stderr but does not fail the example, whose
    checks have already run. *)
let remove_ledger ledger =
  let directory = Payment_ledger.directory ledger in
  try
    Array.iter
      (fun name -> Sys.remove (Filename.concat directory name))
      (Sys.readdir directory);
    Sys.rmdir directory
  with Sys_error message ->
    Printf.eprintf "idempotency example: could not remove %s: %s\n%!"
      directory message

(** Runs the three scenarios in order and prints one line for each. *)
let run () =
  let open Result_syntax in
  let* config = Example_support.Config.from_environment () in
  let task_queue = config.task_queue ^ "-idempotency" in
  let suffix = Printf.sprintf "%.0f-%d" (Unix.time ()) (Unix.getpid ()) in
  let* ledger =
    ledger_result
      (Payment_ledger.open_ledger
         (Filename.temp_dir "ocaml-temporal-idempotency-" ""))
  in
  let order : Idempotent_payments.order =
    { order_id = "order-" ^ suffix ^ "-a"; account = "acct-ada"; lines = [ 2500; 2500 ] }
  in
  let other_order = { order with order_id = "order-" ^ suffix ^ "-b" } in
  let id = "ocaml-temporal-example-" ^ order.order_id in
  let result =
    Example_support.Lifecycle.with_client config (fun client ->
        with_worker config ~task_queue ~ledger (fun () ->
            let* first_run_id =
              identical_lines client ledger ~task_queue ~id order
            in
            print_endline
              "Identical lines: each line has its own idempotency key, so \
               both charges were posted.";
            let* () =
              resubmitted_order client ledger ~task_queue ~id ~first_run_id
                order
            in
            print_endline
              "Resubmitted order: the new run found both lines already \
               posted and posted nothing again.";
            let* _run_id =
              identical_lines client ledger ~task_queue
                ~id:("ocaml-temporal-example-" ^ other_order.order_id)
                other_order
            in
            let* () = check_posted_by ledger order ~run_id:first_run_id in
            print_endline
              "Another order with identical lines: it has its own keys, so \
               both of its charges were posted.";
            Ok ()))
  in
  remove_ledger ledger;
  result

(** Reports the outcome after the worker and the client have been shut
    down. *)
let () =
  match run () with
  | Ok () -> print_endline "Idempotent payments example passed."
  | Error error ->
      Example_support.report_error "idempotency example" error;
      exit 1
