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
