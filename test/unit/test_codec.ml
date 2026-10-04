(** Turns an unexpected structured codec error into a readable test failure. *)
let fail_error error = failwith (Temporal.Error.message error)

(** Extracts a successful test result or fails with its SDK diagnostic. *)
let unwrap = function Ok value -> value | Error error -> fail_error error

(** Checks that user callback exceptions become typed codec failures while
    callbacks that deliberately return an error retain that exact error. *)
let test_custom_callback_failures () =
  let payload : Temporal.Payload.t =
    { metadata = [ ("encoding", "test/callback") ]; data = Bytes.empty }
  in
  let raising_encoder =
    Temporal.Codec.make ~encoding:"test/callback"
      ~encode:(fun () -> failwith "encoder-boom") ~decode:(fun _ -> Ok ())
  in
  (match Temporal.Codec.encode raising_encoder () with
  | Error error ->
      assert (Temporal.Error.kind error = "codec");
      assert
        (Temporal.Error.message error
        = "codec \"test/callback\" encode callback raised an exception")
  | Ok _ -> failwith "raising encoder unexpectedly succeeded");
  let raising_decoder =
    Temporal.Codec.make ~encoding:"test/callback" ~encode:(fun () -> Ok Bytes.empty)
      ~decode:(fun _ -> failwith "decoder-boom")
  in
  (match Temporal.Codec.decode raising_decoder payload with
  | Error error ->
      assert (Temporal.Error.kind error = "codec");
      assert
        (Temporal.Error.message error
        = "codec \"test/callback\" decode callback raised an exception")
  | Ok _ -> failwith "raising decoder unexpectedly succeeded");
  let original = Temporal.Error.defect ~message:"preserve custom error" in
  let returned_errors =
    Temporal.Codec.make ~encoding:"test/callback"
      ~encode:(fun () -> Error original) ~decode:(fun _ -> Error original)
  in
  (match Temporal.Codec.encode returned_errors () with
  | Error error -> assert (error == original)
  | Ok _ -> failwith "encoder error unexpectedly succeeded");
  match Temporal.Codec.decode returned_errors payload with
  | Error error -> assert (error == original)
  | Ok _ -> failwith "decoder error unexpectedly succeeded"

let () =
  let value = "agent \"one\"\n\t\001" in
  let payload = unwrap (Temporal.Codec.encode Temporal.Codec.string value) in
  assert (List.assoc "encoding" payload.metadata = "json/plain");
  assert (Temporal.Codec.decode Temporal.Codec.string payload = Ok value);
  let wrong : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "binary/plain") ];
      data = Bytes.of_string "agent";
    }
  in
  (match Temporal.Codec.decode Temporal.Codec.string wrong with
  | Error error -> assert (Temporal.Error.kind error = "codec")
  | Ok _ -> failwith "wrong encoding accepted");
  let escaped : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "json/plain") ];
      data = Bytes.of_string "\"quote: \\\" slash: \\\\ newline: \\n\"";
    }
  in
  assert
    (Temporal.Codec.decode Temporal.Codec.string escaped
    = Ok "quote: \" slash: \\ newline: \n");
  let unicode : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "json/plain") ];
      data = Bytes.of_string "\"\\uD83D\\uDE00\"";
    }
  in
  assert (Temporal.Codec.decode Temporal.Codec.string unicode = Ok "😀");
  let object_payload : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "json/plain") ];
      data = Bytes.of_string "{\"value\":\"not a string\"}";
    }
  in
  assert
    (Result.is_error
       (Temporal.Codec.decode Temporal.Codec.string object_payload));
  let trailing_json : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "json/plain") ];
      data = Bytes.of_string "\"value\" false";
    }
  in
  assert
    (Result.is_error
       (Temporal.Codec.decode Temporal.Codec.string trailing_json));
  let duplicate_metadata : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "json/plain"); ("trace", "one"); ("trace", "two") ];
      data = Bytes.of_string "\"value\"";
    }
  in
  assert
    (Result.is_error
       (Temporal.Codec.decode Temporal.Codec.string duplicate_metadata));
  let duplicate_encoding : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "json/plain"); ("encoding", "json/plain") ];
      data = Bytes.of_string "\"value\"";
    }
  in
  assert
    (Result.is_error
       (Temporal.Codec.decode Temporal.Codec.string duplicate_encoding));
  let decoder_called = ref false in
  let guarded_codec =
    Temporal.Codec.make ~encoding:"test/duplicate-guard"
      ~encode:(fun value -> Ok (Bytes.of_string value))
      ~decode:(fun data ->
        decoder_called := true;
        Ok (Bytes.to_string data))
  in
  let duplicate_guarded : Temporal.Payload.t =
    {
      metadata =
        [ ("encoding", "test/duplicate-guard");
          ("encoding", "test/duplicate-guard") ];
      data = Bytes.of_string "value";
    }
  in
  assert (Result.is_error (Temporal.Codec.decode guarded_codec duplicate_guarded));
  assert (not !decoder_called);
  let invalid_utf_8 = String.make 1 (Char.chr 0xff) in
  assert
    (Result.is_error
       (Temporal.Codec.encode Temporal.Codec.string invalid_utf_8));
  let original = Bytes.of_string "payload" in
  let binary = unwrap (Temporal.Codec.encode Temporal.Codec.bytes original) in
  Bytes.set original 0 'X';
  assert (Bytes.to_string binary.data = "payload");
  let optional = Temporal.Codec.option Temporal.Codec.string in
  let none_payload = unwrap (Temporal.Codec.encode optional None) in
  assert (List.assoc "encoding" none_payload.metadata = "binary/null");
  assert (Temporal.Codec.decode optional none_payload = Ok None);
  let duplicate_null : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "binary/null"); ("encoding", "binary/null") ];
      data = Bytes.empty;
    }
  in
  assert (Result.is_error (Temporal.Codec.decode optional duplicate_null));
  let some_payload = unwrap (Temporal.Codec.encode optional (Some "value")) in
  assert (Temporal.Codec.decode optional some_payload = Ok (Some "value"));
  (* Bug 5 regression: [option] must be injective even when the inner codec
     produces the same [binary/null] payload used for the outer [None]. Without
     the [binary/x-ocaml-optional] wrapper, [Some ()] and [Some None] silently
     decoded to [None], corrupting any workflow value typed as [unit option] or
     nested [option]. Each case below asserts a full encode/decode round-trip
     and, crucially, that the [Some] encoding is byte-distinct from [None]. *)
  let unit_option = Temporal.Codec.option Temporal.Codec.unit in
  let none_unit_option = unwrap (Temporal.Codec.encode unit_option None) in
  let some_unit = unwrap (Temporal.Codec.encode unit_option (Some ())) in
  assert (Temporal.Codec.decode unit_option none_unit_option = Ok None);
  assert (Temporal.Codec.decode unit_option some_unit = Ok (Some ()));
  (* [Some ()] is escaped into the wrapper encoding, so it can never share the
     [binary/null] representation reserved for [None]. *)
  assert (List.assoc "encoding" some_unit.metadata = "binary/x-ocaml-optional");
  assert
    (none_unit_option.metadata <> some_unit.metadata
    || none_unit_option.data <> some_unit.data);
  (* Nested options: [None], [Some None], and [Some (Some "x")] are three
     distinct values that previously collapsed toward [None]. *)
  let nested = Temporal.Codec.option (Temporal.Codec.option Temporal.Codec.string) in
  let none_nested = unwrap (Temporal.Codec.encode nested None) in
  let some_none = unwrap (Temporal.Codec.encode nested (Some None)) in
  let some_some = unwrap (Temporal.Codec.encode nested (Some (Some "x"))) in
  assert (Temporal.Codec.decode nested none_nested = Ok None);
  assert (Temporal.Codec.decode nested some_none = Ok (Some None));
  assert (Temporal.Codec.decode nested some_some = Ok (Some (Some "x")));
  (* [None] and [Some None] must not encode to identical payloads. *)
  assert (
    none_nested.metadata <> some_none.metadata
    || none_nested.data <> some_none.data);
  (* Triple nesting exercises repeated wrapping and unwrapping. *)
  let triple =
    Temporal.Codec.option
      (Temporal.Codec.option (Temporal.Codec.option Temporal.Codec.string))
  in
  List.iter
    (fun value ->
      let encoded = unwrap (Temporal.Codec.encode triple value) in
      assert (Temporal.Codec.decode triple encoded = Ok value))
    [ None; Some None; Some (Some None); Some (Some (Some "deep")) ];
  (* A [Some] carrying an ordinary value keeps its interoperable encoding: the
     wrapper is only introduced when the inner payload would collide with the
     [None] marker. *)
  assert (List.assoc "encoding" some_payload.metadata = "json/plain");
  (* A wrapper payload whose body is truncated must fail with a typed codec
     error rather than raising. *)
  let truncated_wrapper : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "binary/x-ocaml-optional") ];
      data = Bytes.of_string "\000";
    }
  in
  assert (Result.is_error (Temporal.Codec.decode unit_option truncated_wrapper));
  (* The wrapper encoding is reserved: [make] must reject a user codec that
     claims it, so no application payload can masquerade as an option envelope
     and no pre-existing history collides with the new representation. *)
  (match
     Temporal.Codec.make ~encoding:"binary/x-ocaml-optional"
       ~encode:(fun value -> Ok (Bytes.of_string value))
       ~decode:(fun data -> Ok (Bytes.to_string data))
   with
  | exception Invalid_argument _ -> ()
  | _ -> failwith "reserved option encoding accepted by Codec.make")

let () = test_custom_callback_failures ()
