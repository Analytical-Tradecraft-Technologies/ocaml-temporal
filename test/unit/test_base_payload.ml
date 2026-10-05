(** Tests the shared zero-argument payload rule in [Temporal_base.Payload]
    (#819). Every outbound input site relies on [input_arguments], so these
    cases pin exactly which payloads are treated as the canonical unit value. *)

module Payload = Temporal_base.Payload

(** Builds a payload with owned bytes for one test case. *)
let payload metadata data = { Payload.metadata; data = Bytes.of_string data }

(** The canonical unit payload, and only it, maps to zero arguments; anything
    carrying data or extra metadata is preserved as one argument so no
    information is dropped on the wire. *)
let test_input_arguments () =
  let unit_null = Payload.unit_null () in
  assert (Payload.is_unit_null unit_null);
  assert (Payload.input_arguments unit_null = []);
  let codec_unit =
    match Temporal_base.Codec.encode Temporal_base.Codec.unit () with
    | Ok value -> value
    | Error _ -> failwith "Codec.unit failed to encode"
  in
  assert (Payload.input_arguments codec_unit = []);
  let keep value = assert (Payload.input_arguments value = [ value ]) in
  keep (payload [ ("encoding", "json/plain") ] "\"\"");
  keep (payload [ ("encoding", "binary/null") ] "x");
  keep (payload [ ("encoding", "binary/null"); ("extra", "1") ] "");
  keep (payload [ ("encoding", "binary/plain") ] "");
  keep (payload [] "")

(** Each call returns a fresh record so a caller mutating one result cannot
    affect another. *)
let test_unit_null_is_fresh () =
  let first = Payload.unit_null () in
  let second = Payload.unit_null () in
  assert (first != second);
  assert (first = second)

let () =
  test_input_arguments ();
  test_unit_null_is_fresh ()
