(** Tests the canonical unit payload in [Temporal_base.Payload]. Inbound
    decoders use it to reconstruct a single input from an empty argument list
    (#819); the outbound argument-count rule is codec-decided and covered by
    [test/runtime/test_input_arguments.ml] (#1012). *)

module Payload = Temporal_base.Payload

(** The canonical unit payload is exactly what [Codec.unit] encodes, so a
    decoder that substitutes it for zero arguments sees the same value an
    OCaml sender would have encoded. *)
let test_unit_null_matches_codec_unit () =
  let codec_unit =
    match Temporal_base.Codec.encode Temporal_base.Codec.unit () with
    | Ok value -> value
    | Error _ -> failwith "Codec.unit failed to encode"
  in
  assert (Payload.unit_null () = codec_unit)

(** Each call returns a fresh record so a caller mutating one result cannot
    affect another. *)
let test_unit_null_is_fresh () =
  let first = Payload.unit_null () in
  let second = Payload.unit_null () in
  assert (first != second);
  assert (first = second)

let () =
  test_unit_null_matches_codec_unit ();
  test_unit_null_is_fresh ()
