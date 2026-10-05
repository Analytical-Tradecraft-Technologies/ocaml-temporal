(** Regression tests for issue #789: the shipped example workflow must report
    invalid business input as a terminal [`Workflow] failure. A propagated
    [Error.defect] would instead fail only the workflow task, which Temporal
    retries indefinitely while the run stays open, so [Client.result] would
    never return for a simple empty-name request. *)

(** Requires [result] to be a non-retryable [`Workflow] error carrying
    [message]. Both validations below run before the example starts any
    activity or timer, so no workflow execution context is needed. *)
let expect_business_failure label ~message result =
  match result with
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")
  | Error error ->
      let view = Temporal.Error.view error in
      if view.category <> `Workflow then
        failwith
          (label ^ " returned category " ^ Temporal.Error.kind error
         ^ " instead of workflow");
      if not view.non_retryable then
        failwith (label ^ " must be non-retryable so bad input is not rerun");
      if not (String.equal view.message message) then
        failwith (label ^ " returned an unexpected message: " ^ view.message)

(** An empty or whitespace-only name is the README's flagship validation. *)
let test_empty_name_fails_workflow () =
  expect_business_failure "empty name" ~message:"a name is required"
    (Example_support.Definitions.compose_message "   ")

(** A delimiter in the name is rejected by the activity request builder,
    which the workflow propagates with [let*]. *)
let test_delimiter_fails_workflow () =
  expect_business_failure "delimiter" ~message:"example names must not contain ':'"
    (Example_support.Definitions.render_request "greeting" "a:b")

let () =
  test_empty_name_fails_workflow ();
  test_delimiter_fails_workflow ()
