(** Exercises typed signal, query, and update registration and dispatch.

    These tests intentionally use the deterministic [Temporal.Interaction]
    dispatcher rather than a Temporal Server. They verify the codec boundary,
    handler ordering, validator short-circuiting, and exception containment
    that the future native activation path must preserve. *)

(** Fails with the SDK's diagnostic when an operation unexpectedly returns an
    error, keeping the assertions concise without hiding the message. *)
let unwrap = function
  | Ok value -> value
  | Error error ->
      failwith ("unexpected SDK error: " ^ Temporal.Error.message error)

(** Requires an error of the requested public category. *)
let expect_error_kind kind = function
  | Error error -> assert (Temporal.Error.kind error = kind)
  | Ok _ -> failwith ("expected " ^ kind ^ " error")

(** Tests substring membership without adding a regular-expression dependency
    to the small unit-test executable. *)
let contains_substring ~needle haystack =
  let needle_length = String.length needle in
  let haystack_length = String.length haystack in
  let rec loop offset =
    if offset + needle_length > haystack_length then false
    else if String.sub haystack offset needle_length = needle then true
    else loop (offset + 1)
  in
  needle_length = 0 || loop 0

(** Requires an error message to retain the useful operation context. *)
let expect_error_message fragment = function
  | Error error ->
      let message = Temporal.Error.message error in
      assert (contains_substring ~needle:fragment message)
  | Ok _ -> failwith ("expected error containing " ^ fragment)

(** Runs the complete typed interaction flow and checks that signals are
    delivered in submission order, queries observe state, and updates run a
    validator before their implementation. *)
let test_dispatch_and_ordering () =
  let values = ref [] in
  let update_calls = ref 0 in
  let signal =
    Temporal.Signal.define ~name:"append-value" ~input:Temporal.Codec.string
  in
  let query =
    Temporal.Query.define ~name:"current-values" ~output:Temporal.Codec.string
  in
  let update =
    Temporal.Update.define ~name:"uppercase" ~input:Temporal.Codec.string
      ~output:Temporal.Codec.string
  in
  let signal_handler =
    Temporal.Signal.Handler.make signal (fun value ->
        values := !values @ [ value ];
        Ok ())
  in
  let query_handler =
    Temporal.Query.Handler.make query (fun () ->
        Ok (String.concat "," !values))
  in
  let update_handler =
    Temporal.Update.Handler.make
      ~validator:(fun value ->
        if String.equal value "" then
          Error
            (Temporal.Error.make ~category:`Update ~non_retryable:true
               ~message:"update input is empty" ())
        else Ok ())
      update (fun value ->
        incr update_calls;
        Ok (String.uppercase_ascii value))
  in
  let dispatcher =
    unwrap
      (Temporal.Interaction.create ~signals:[ signal_handler ]
         ~queries:[ query_handler ] ~updates:[ update_handler ] ())
  in
  unwrap (Temporal.Interaction.signal dispatcher signal "one");
  unwrap (Temporal.Interaction.signal dispatcher signal "two");
  assert (!values = [ "one"; "two" ]);
  assert
    (unwrap (Temporal.Interaction.query dispatcher query) = "one,two");
  expect_error_kind "update"
    (Temporal.Interaction.update dispatcher update "");
  assert (!update_calls = 0);
  assert
    (unwrap (Temporal.Interaction.update dispatcher update "ready") = "READY");
  assert (!update_calls = 1)

(** Typed query arguments are decoded before the callback and re-encoded after
    it.  The local dispatcher also proves that the output-only handler cannot
    accidentally accept a non-empty payload list. *)
let test_typed_query_arguments () =
  let query =
    Temporal.Query.define_with_input ~name:"length-of" ~input:Temporal.Codec.string
      ~output:Temporal.Codec.string
  in
  let handler =
    Temporal.Query.Handler.make_with_input query (fun value ->
        Ok (string_of_int (String.length value)))
  in
  let dispatcher =
    unwrap (Temporal.Interaction.create ~queries:[ handler ] ())
  in
  assert
    (unwrap (Temporal.Interaction.query_with_input dispatcher query "hello") = "5");
  let payload = unwrap (Temporal.Codec.encode Temporal.Codec.string "unexpected") in
  expect_error_kind "defect"
    (Temporal.Query.Handler.dispatch_payloads handler [ payload; payload ])

(** Signal payload arity matches the wider Temporal ecosystem (#813). The
    Temporal CLI, Web UI, and other SDKs send a no-argument signal with zero
    payloads, which must reach a unit handler instead of failing the workflow.
    A non-unit handler reports its codec error for zero payloads, and repeated
    payloads are rejected non-retryably without invoking the callback. *)
let test_signal_payload_arity () =
  let calls = ref 0 in
  let unit_signal = Temporal.Signal.define ~name:"wake" ~input:Temporal.Codec.unit in
  let unit_handler =
    Temporal.Signal.Handler.make unit_signal (fun () ->
        incr calls;
        Ok ())
  in
  unwrap (Temporal.Signal.Handler.dispatch_payloads unit_handler []);
  let null = unwrap (Temporal.Codec.encode Temporal.Codec.unit ()) in
  unwrap (Temporal.Signal.Handler.dispatch_payloads unit_handler [ null ]);
  if !calls <> 2 then failwith "unit signal did not run for zero and one payloads";
  (match Temporal.Signal.Handler.dispatch_payloads unit_handler [ null; null ] with
  | Error error ->
      if not (Temporal.Error.view error).non_retryable then
        failwith "repeated signal payloads were retryable"
  | Ok () -> failwith "repeated signal payloads were accepted");
  if !calls <> 2 then failwith "repeated signal payloads invoked the callback";
  let string_signal =
    Temporal.Signal.define ~name:"rename" ~input:Temporal.Codec.string
  in
  let string_handler =
    Temporal.Signal.Handler.make string_signal (fun _ -> Ok ())
  in
  expect_error_kind "codec"
    (Temporal.Signal.Handler.dispatch_payloads string_handler [])

(** Duplicate names are rejected while building a dispatcher, and unknown
    definitions fail before any callback can run. *)
let test_registration_and_missing_handlers () =
  let signal =
    Temporal.Signal.define ~name:"duplicate" ~input:Temporal.Codec.string
  in
  let handler = Temporal.Signal.Handler.make signal (fun _ -> Ok ()) in
  expect_error_kind "defect"
    (Temporal.Interaction.create ~signals:[ handler; handler ] ());
  let dispatcher = unwrap (Temporal.Interaction.create ()) in
  let missing =
    Temporal.Signal.define ~name:"not-registered" ~input:Temporal.Codec.string
  in
  expect_error_message "unregistered signal handler"
    (Temporal.Interaction.signal dispatcher missing "value")

(** A definition with the same name but a different encoding cannot silently
    cross the handler boundary; the handler's codec reports the mismatch. *)
let test_codec_mismatch () =
  let registered =
    Temporal.Signal.define ~name:"codec-boundary" ~input:Temporal.Codec.string
  in
  let handler = Temporal.Signal.Handler.make registered (fun _ -> Ok ()) in
  let dispatcher =
    unwrap (Temporal.Interaction.create ~signals:[ handler ] ())
  in
  let caller =
    Temporal.Signal.define ~name:"codec-boundary" ~input:Temporal.Codec.bytes
  in
  expect_error_kind "codec"
    (Temporal.Interaction.signal dispatcher caller (Bytes.of_string "raw"))

(** Handler exceptions become typed defects rather than escaping through the
    dispatcher, which protects a future worker poll loop from user mistakes. *)
let test_exception_containment () =
  let query =
    Temporal.Query.define ~name:"raises" ~output:Temporal.Codec.string
  in
  let handler =
    Temporal.Query.Handler.make query (fun () -> failwith "query bug")
  in
  let dispatcher = unwrap (Temporal.Interaction.create ~queries:[ handler ] ()) in
  expect_error_kind "defect" (Temporal.Interaction.query dispatcher query)

(** A codec whose [encode] raises instead of returning [Error] must produce a
    typed codec error at the [Codec.make] boundary. [decode] is never expected
    to run in these tests, so it returns a harmless [Ok ()]. *)
let raising_encode_codec () =
  Temporal.Codec.make ~encoding:"application/x-raising-test-codec-encode"
    ~encode:(fun () -> failwith "codec encode bug")
    ~decode:(fun _ -> Ok ())

(** A codec whose [decode] raises instead of returning [Error]. [encode]
    succeeds so a well-formed payload with matching encoding metadata can
    reach the raising decoder, exercising containment at the decode boundary
    specifically rather than being short-circuited by a metadata mismatch. *)
let raising_decode_codec () =
  Temporal.Codec.make ~encoding:"application/x-raising-test-codec-decode"
    ~encode:(fun () -> Ok Bytes.empty)
    ~decode:(fun _ -> failwith "codec decode bug")

(** [Interaction.signal] propagates a typed codec error when its input encoder
    raises instead of letting the exception escape the call. *)
let test_signal_input_codec_exception_containment () =
  let signal =
    Temporal.Signal.define ~name:"raising-input" ~input:(raising_encode_codec ())
  in
  let handler = Temporal.Signal.Handler.make signal (fun _ -> Ok ()) in
  let dispatcher =
    unwrap (Temporal.Interaction.create ~signals:[ handler ] ())
  in
  expect_error_kind "codec" (Temporal.Interaction.signal dispatcher signal ())

(** [Signal.Handler.dispatch] must contain an input codec that raises on
    decode, matching the same boundary as a raising handler callback. The
    payload is built through the codec's own [encode] so it carries the
    matching encoding metadata and actually reaches the raising decoder. *)
let test_signal_handler_decode_exception_containment () =
  let codec = raising_decode_codec () in
  let signal = Temporal.Signal.define ~name:"raising-decode" ~input:codec in
  let handler = Temporal.Signal.Handler.make signal (fun _ -> Ok ()) in
  let payload = unwrap (Temporal.Codec.encode codec ()) in
  expect_error_kind "codec" (Temporal.Signal.Handler.dispatch handler payload)

(** [Interaction.query] must contain an output codec that raises on decode
    after a successful handler dispatch: [Query.Handler.dispatch] encodes the
    handler's result with the codec's working [encode], and [Interaction.query]
    then decodes that payload with the same codec's raising [decode]. *)
let test_query_output_codec_exception_containment () =
  let query =
    Temporal.Query.define ~name:"raising-output" ~output:(raising_decode_codec ())
  in
  let handler = Temporal.Query.Handler.make query (fun () -> Ok ()) in
  let dispatcher = unwrap (Temporal.Interaction.create ~queries:[ handler ] ()) in
  expect_error_kind "codec" (Temporal.Interaction.query dispatcher query)

(** [Query.Handler.dispatch] must contain an output codec that raises on
    encode, not just the handler callback itself. *)
let test_query_handler_encode_exception_containment () =
  let query =
    Temporal.Query.define ~name:"raising-encode" ~output:(raising_encode_codec ())
  in
  let handler = Temporal.Query.Handler.make query (fun () -> Ok ()) in
  expect_error_kind "codec" (Temporal.Query.Handler.dispatch handler)

(** [Query.Handler.dispatch_payloads] must contain a raising typed-input
    decoder before invoking the user callback.  This protects the worker
    adapter boundary even when a malformed or application-defined codec is
    used directly, without the higher-level [Interaction] wrapper. *)
let test_typed_query_handler_decode_exception_containment () =
  let input_codec = raising_decode_codec () in
  let query =
    Temporal.Query.define_with_input ~name:"raising-query-input"
      ~input:input_codec ~output:Temporal.Codec.unit
  in
  let handler = Temporal.Query.Handler.make_with_input query (fun () -> Ok ()) in
  let payload = unwrap (Temporal.Codec.encode input_codec ()) in
  expect_error_kind "codec"
    (Temporal.Query.Handler.dispatch_payloads handler [ payload ])

(** [Interaction.update] must contain both a raising input encode and a
    raising output decode, since it owns both codec boundaries around the
    handler dispatch it delegates to. *)
let test_update_codec_exception_containment () =
  let update =
    Temporal.Update.define ~name:"raising-update-input"
      ~input:(raising_encode_codec ()) ~output:Temporal.Codec.unit
  in
  let handler = Temporal.Update.Handler.make update (fun _ -> Ok ()) in
  let dispatcher =
    unwrap (Temporal.Interaction.create ~updates:[ handler ] ())
  in
  expect_error_kind "codec" (Temporal.Interaction.update dispatcher update ());
  let update_raising_output =
    Temporal.Update.define ~name:"raising-update-output" ~input:Temporal.Codec.unit
      ~output:(raising_decode_codec ())
  in
  let output_handler =
    Temporal.Update.Handler.make update_raising_output (fun () -> Ok ())
  in
  let output_dispatcher =
    unwrap (Temporal.Interaction.create ~updates:[ output_handler ] ())
  in
  expect_error_kind "codec"
    (Temporal.Interaction.update output_dispatcher update_raising_output ())

(** [Update.Handler.dispatch] must contain a raising input decode and a
    raising output encode around the validator/implementation sequence. *)
let test_update_handler_codec_exception_containment () =
  let input_codec = raising_decode_codec () in
  let update =
    Temporal.Update.define ~name:"raising-update-decode" ~input:input_codec
      ~output:Temporal.Codec.unit
  in
  let handler = Temporal.Update.Handler.make update (fun _ -> Ok ()) in
  let payload = unwrap (Temporal.Codec.encode input_codec ()) in
  expect_error_kind "codec" (Temporal.Update.Handler.dispatch handler payload);
  let update_raising_output =
    Temporal.Update.define ~name:"raising-update-encode" ~input:Temporal.Codec.unit
      ~output:(raising_encode_codec ())
  in
  let output_handler =
    Temporal.Update.Handler.make update_raising_output (fun () -> Ok ())
  in
  let unit_payload = unwrap (Temporal.Codec.encode Temporal.Codec.unit ()) in
  expect_error_kind "codec"
    (Temporal.Update.Handler.dispatch output_handler unit_payload)

(** Runs all interaction assertions. *)
let () =
  test_dispatch_and_ordering ();
  test_typed_query_arguments ();
  test_signal_payload_arity ();
  test_registration_and_missing_handlers ();
  test_codec_mismatch ();
  test_exception_containment ();
  test_signal_input_codec_exception_containment ();
  test_signal_handler_decode_exception_containment ();
  test_query_output_codec_exception_containment ();
  test_query_handler_encode_exception_containment ();
  test_typed_query_handler_decode_exception_containment ();
  test_update_codec_exception_containment ();
  test_update_handler_codec_exception_containment ()
