(** Proves that inspecting an abstract error cannot expose its retained mutable
    detail buffers. A caller may mutate the payload returned by one view, but
    later views must still report the snapshot captured by [Error.make]. *)
let () =
  let detail : Temporal_base.Payload.t =
    {
      metadata = [ ("encoding", "binary/plain") ];
      data = Bytes.of_string "retained";
    }
  in
  let error =
    Temporal_base.Error.make ~category:`Activity ~message:"failed"
      ~details:[ detail ] ()
  in
  let first_view = Temporal_base.Error.view error in
  let first_detail = List.hd first_view.details in
  Bytes.set first_detail.data 0 'X';
  let second_view = Temporal_base.Error.view error in
  let second_detail = List.hd second_view.details in
  assert (Bytes.to_string second_detail.data = "retained")

(** Checks the private error type contract used by the Core translators: an
    explicit type is the wire type, an absent or empty type falls back to the
    category label, and text that could never cross the bridge is rejected as a
    programming error at construction. *)
let () =
  let typed =
    Temporal_base.Error.make ~error_type:"InvalidInput" ~category:`Activity
      ~message:"bad input" ()
  in
  assert (Temporal_base.Error.error_type typed = Some "InvalidInput");
  assert ((Temporal_base.Error.view typed).error_type = Some "InvalidInput");
  assert (Temporal_base.Error.kind typed = "activity");
  assert (Temporal_base.Error.application_failure_type typed = "InvalidInput");
  let untyped =
    Temporal_base.Error.make ~category:`Workflow ~message:"failed" ()
  in
  assert (Temporal_base.Error.error_type untyped = None);
  assert (Temporal_base.Error.application_failure_type untyped = "workflow");
  let empty =
    Temporal_base.Error.make ~error_type:"" ~category:`Timeout ~message:"t" ()
  in
  assert (Temporal_base.Error.error_type empty = None);
  assert (Temporal_base.Error.application_failure_type empty = "timeout");
  let rejects error_type =
    match
      Temporal_base.Error.make ~error_type ~category:`Activity ~message:"m" ()
    with
    | _ -> false
    | exception Invalid_argument _ -> true
  in
  assert (rejects "\xff");
  assert (
    rejects (String.make (Temporal_base.Error.max_error_type_bytes + 1) 'x'));
  assert (not (rejects (String.make Temporal_base.Error.max_error_type_bytes 'x')));
  (* Control characters would each become a six-character JSON escape, and
     quotes or backslashes double, so limits apply to the encoded form. *)
  assert (rejects "Bad\001Type");
  assert (rejects "tab\there");
  assert (rejects (String.make (Temporal_base.Error.max_error_type_bytes / 2 + 1) '"'));
  assert (not (rejects (String.make (Temporal_base.Error.max_error_type_bytes / 2) '"')))
