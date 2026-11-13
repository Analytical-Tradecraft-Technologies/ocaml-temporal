(** A side-effecting activity and the workflow that calls it, made safe
    under Temporal's at-least-once activity delivery by an idempotency key
    that the external system deduplicates atomically (#504).

    The workflow posts each line of an order to a {!Payment_ledger}. The
    activity may run more than once for the same line: Temporal retries an
    attempt that failed or timed out, and redelivers one whose worker died,
    even if that attempt already posted the line and only its completion was
    lost. The SDK cannot prevent that, so correctness comes from the key:

    - The key names the business operation, "line N of order O", and is
      built from the order ID in the workflow input. It does not contain the
      attempt number, so every retry and redelivery of the line sends the
      same key and the ledger answers with the original posting. It does not
      contain the run ID either, so a workflow retry, a continue-as-new or a
      resubmission of the same order (each a new run) also finds the lines
      already posted instead of posting them again.
    - The key does not depend on the posting's content. Two lines with the
      same account and amount are two charges, so they get two keys and are
      both posted; a key derived from the payload would wrongly suppress the
      second one.
    - The activity returns a receipt computed from the ledger's record, so a
      duplicate request returns exactly what the original attempt would
      have returned, and the workflow cannot tell them apart.

    When a step must instead happen once per run (so that a new run does
    the work again), include the run ID in the key, as the cooperative
    cancellation example does. *)

open Temporal

(** One order: the account to charge and the amount of each line, in cents.
    Lines with equal amounts are separate charges. *)
type order = { order_id : string; account : string; lines : int list }

(** What the workflow asks the activity to post. [idempotency_key] travels in
    the input, so the activity does not have to reconstruct it from
    execution metadata. *)
type posting_request = {
  idempotency_key : string;
  account : string;
  amount_cents : int;
}

(** Returns the error a codec reports for a JSON document of the wrong
    shape. *)
let shape_error what = Error (Error.codec ~message:("expected " ^ what))

(** JSON codec for {!order}: [{"order_id": ..., "account": ..., "lines":
    [...]}]. *)
let order_codec =
  Codec.json_conv
    ~to_json:(fun { order_id; account; lines } ->
      `Assoc
        [
          ("order_id", `String order_id);
          ("account", `String account);
          ("lines", `List (List.map (fun amount -> `Int amount) lines));
        ])
    ~of_json:(function
      | `Assoc
          [
            ("order_id", `String order_id);
            ("account", `String account);
            ("lines", `List lines);
          ] -> (
          match
            List.map
              (function `Int amount -> amount | _ -> raise Exit)
              lines
          with
          | lines -> Ok { order_id; account; lines }
          | exception Exit -> shape_error "integer order line amounts")
      | _ -> shape_error "an order object")

(** JSON codec for {!posting_request}. *)
let posting_request_codec =
  Codec.json_conv
    ~to_json:(fun { idempotency_key; account; amount_cents } ->
      `Assoc
        [
          ("idempotency_key", `String idempotency_key);
          ("account", `String account);
          ("amount_cents", `Int amount_cents);
        ])
    ~of_json:(function
      | `Assoc
          [
            ("idempotency_key", `String idempotency_key);
            ("account", `String account);
            ("amount_cents", `Int amount_cents);
          ] ->
          Ok { idempotency_key; account; amount_cents }
      | _ -> shape_error "a posting request object")

(** Longest accepted order ID, in bytes, so every line key stays within the
    ledger's key limit. *)
let max_order_id_bytes = 100

(** The idempotency key of line [line] (1-based) of [order_id]. It is a pure
    function of business data, so the workflow computes the same key on
    every run and replay. *)
let line_key ~order_id ~line = Printf.sprintf "%s/line-%d" order_id line

(** The receipt for [amount_cents] posted to [account] under [key]. *)
let receipt_text ~key ~amount_cents ~account =
  Printf.sprintf "%s: %d cents to %s" key amount_cents account

(** The receipt the activity returns for a committed ledger record. It
    depends only on the record, so an original and a duplicate request for
    the same key return the same receipt. *)
let receipt (record : Payment_ledger.record) =
  receipt_text ~key:record.key ~amount_cents:record.posting.amount_cents
    ~account:record.posting.account

(** The workflow output for [order] when every line is posted: one receipt
    per line, in order. Drivers compare the real output with it. *)
let expected_output order =
  String.concat "\n"
    (List.mapi
       (fun index amount_cents ->
         receipt_text
           ~key:(line_key ~order_id:order.order_id ~line:(index + 1))
           ~amount_cents ~account:order.account)
       order.lines)

(** The registered activity type name. *)
let post_line_name = "ocaml-temporal-example.post-ledger-line"

(** The activity ID the workflow gives line [line]. An explicit, readable ID
    makes the ledger's correlation records and the workflow history easy to
    match up; it is not part of the idempotency key. *)
let line_activity_id line = Printf.sprintf "post-line-%d" line

(** Builds the correlation recorded with a ledger request from the
    attempt's {!Activity.Info}. *)
let correlation info ~worker : Payment_ledger.correlation =
  let workflow = Activity.Info.workflow info in
  {
    workflow_id = workflow.workflow_id;
    run_id = workflow.run_id;
    activity_id = Activity.Info.activity_id info;
    attempt = Activity.Info.attempt info;
    worker;
  }

(** [post_line ~ledger ~worker ()] is the activity that posts one line to
    [ledger] under the key in its input.

    - A [Committed] or [Duplicate] answer both mean the line is posted, and
      the activity returns the record's {!receipt}. A duplicate is the
      expected answer to a retry or a redelivery after a lost completion.
    - A [Conflict] means the workflow gave one key to two different
      postings. Retrying cannot fix that, so the activity fails
      non-retryably with error type ["IdempotencyKeyConflict"].
    - A ledger I/O failure is retryable: the next attempt sends the same key,
      so it cannot post the line twice.

    [worker] labels this process in the ledger's correlation records.
    [after_commit] runs after the ledger has answered [Committed] or
    [Duplicate] and before the activity returns; the default does nothing
    and returns [Ok ()]. An [Error] from it fails the attempt with that
    error even though the line is posted. It exists so tests can lose an
    attempt exactly between the durable effect and the completion Temporal
    would record: the unit tests return a retryable error there, and the
    live regression kills the worker process. *)
let post_line ~ledger ~worker ?(after_commit = fun _ -> Ok ()) () =
  Activity.define_with_context ~name:post_line_name
    ~input:posting_request_codec ~output:Codec.string
    (fun context request ->
      let open Result_syntax in
      let* info = Activity.Context.info context in
      let posting : Payment_ledger.posting =
        { account = request.account; amount_cents = request.amount_cents }
      in
      match
        Payment_ledger.post ledger ~key:request.idempotency_key posting
          (correlation info ~worker)
      with
      | Error message ->
          Error
            (Error.make ~category:`Activity ~error_type:"LedgerUnavailable"
               ~message ())
      | Ok (Conflict record) ->
          Error
            (Error.make ~category:`Activity ~non_retryable:true
               ~error_type:"IdempotencyKeyConflict"
               ~message:
                 (Printf.sprintf
                    "key %s already posted a different line (%d cents to %s)"
                    record.key record.posting.amount_cents
                    record.posting.account)
               ())
      | Ok ((Committed record | Duplicate record) as outcome) ->
          let* () = after_commit outcome in
          Ok (receipt record))

(** The activity as the workflow schedules it: the same type name and
    codecs, without a callback, so the workflow definition does not depend
    on a ledger. *)
let remote_post_line =
  Activity.remote ~name:post_line_name ~input:posting_request_codec
    ~output:Codec.string

(** Start-to-close timeout of one posting attempt. A worker that dies
    mid-attempt is detected when it expires, so it is short here; a real
    application sizes it to the external call's worst case. *)
let attempt_timeout = Duration.of_ms 5_000L

(** Returns a terminal workflow failure for an invalid order. *)
let invalid_order message =
  Error (Error.make ~category:`Workflow ~non_retryable:true ~message ())

(** Checks an order before any line is posted. *)
let validate order =
  if String.equal order.order_id "" then invalid_order "an order ID is required"
  else if String.length order.order_id > max_order_id_bytes then
    invalid_order "the order ID is too long"
  else if order.lines = [] then invalid_order "an order needs at least one line"
  else if List.exists (fun amount -> amount <= 0) order.lines then
    invalid_order "every line amount must be positive"
  else Ok ()

(** Posts every line of [order], one after another, and returns their
    receipts. Each line is retried every second, at most five attempts, so
    an attempt lost with its worker is redelivered soon after its timeout.
    Lines are posted sequentially so a failure leaves a clear prefix of
    posted lines; posting them concurrently would be equally safe, because
    each has its own key. *)
let post_order order =
  let open Result_syntax in
  let* () = validate order in
  let* retry_policy =
    Retry_policy.make ~initial_interval:(Duration.of_ms 1_000L)
      ~backoff_coefficient:1.0 ~maximum_interval:(Duration.of_ms 1_000L)
      ~maximum_attempts:5 ()
  in
  let rec post_lines line receipts = function
    | [] -> Ok (String.concat "\n" (List.rev receipts))
    | amount_cents :: rest ->
        let request =
          {
            idempotency_key = line_key ~order_id:order.order_id ~line;
            account = order.account;
            amount_cents;
          }
        in
        let* receipt =
          Activity.execute ~activity_id:(line_activity_id line)
            ~start_to_close_timeout:attempt_timeout ~retry_policy
            remote_post_line request
        in
        post_lines (line + 1) (receipt :: receipts) rest
  in
  post_lines 1 [] order.lines

(** The workflow that posts an order. *)
let post_order_workflow =
  Workflow.define ~name:"ocaml-temporal-example.post-order" ~input:order_codec
    ~output:Codec.string post_order
