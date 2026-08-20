(** Smoke-tests the public structured-error view and the result syntax.

    The assertions cover both an ordinary codec failure and a programmer
    defect, including their retryability and stable kind/category projections.
    The final computation also keeps the public [let*]/[let+] operators in the
    same compilation unit as the error API they are commonly used with. *)
let () =
  let error = Temporal.Error.codec ~message:"invalid payload" in
  let view = Temporal.Error.view error in
  assert (view.category = `Codec);
  assert (view.message = "invalid payload");
  assert (not view.non_retryable);
  assert (view.details = []);
  let defect = Temporal.Error.defect ~message:"unexpected exception" in
  assert ((Temporal.Error.view defect).non_retryable);
  assert (Temporal.Error.kind defect = "defect");
  let detail : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "binary/plain") ];
      data = Bytes.of_string "retained";
    }
  in
  let detailed_error =
    Temporal.Error.make ~category:`Activity ~message:"failed" ~details:[ detail ]
      ()
  in
  let first_view = Temporal.Error.view detailed_error in
  let first_detail = List.hd first_view.details in
  Bytes.set first_detail.data 0 'X';
  let second_view = Temporal.Error.view detailed_error in
  let second_detail = List.hd second_view.details in
  assert (Bytes.to_string second_detail.data = "retained");
  let open Temporal.Result_syntax in
  let computation =
    let* x = Ok 20 in
    let+ y = Ok 22 in
    x + y
  in
  assert (computation = Ok 42)
