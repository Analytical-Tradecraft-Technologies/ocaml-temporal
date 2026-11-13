(** Coverage for typed search attributes and memo upserts (#1154):
    [Temporal.Search_attribute]'s encoding of every Temporal indexed type,
    its decoding of client- and server-shaped payloads, its typed errors, and
    the [Temporal.Testing] simulation of memo and search-attribute upserts
    across continue-as-new. Only the public API is used. *)

module T = Temporal
module SA = T.Search_attribute

(** Unwraps a successful result, failing the test with the error message. *)
let ok label = function
  | Ok value -> value
  | Error error -> failwith (label ^ " failed: " ^ T.Error.message error)

(** Fails the test with [label] when two values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Requires [result] to fail with [category] and returns its message. *)
let expect_error label category = function
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")
  | Error error ->
      if (T.Error.view error).category <> category then
        failwith
          (Printf.sprintf "%s: unexpected %s error: %s" label (T.Error.kind error)
             (T.Error.message error));
      T.Error.message error

(** Reports whether [needle] occurs in [haystack]. *)
let contains haystack needle =
  let length = String.length needle in
  let rec search index =
    index + length <= String.length haystack
    && (String.equal (String.sub haystack index length) needle || search (index + 1))
  in
  search 0

(** Builds a public instant, failing the test on invalid components. *)
let time seconds nanoseconds = ok "time" (T.Time.of_unix ~seconds ~nanoseconds)

(** A [json/plain] payload with [type] metadata, as [encode] writes it. *)
let typed data kind =
  { T.Payload.metadata = [ ("encoding", "json/plain"); ("type", kind) ];
    data = Bytes.of_string data }

let description = SA.text "Description"
let customer = SA.keyword "CustomerId"
let count = SA.int64 "Attempts"
let ratio = SA.double "Ratio"
let flag = SA.bool "Escalated"
let deadline = SA.datetime "Deadline"
let tags = SA.keyword_list "Tags"

(** Every indexed type encodes to the [json/plain] shape Temporal Server
    decodes, with the type name other SDKs write, in the order given; an
    unset is the exact [binary/null] removal payload. *)
let test_encode_every_type () =
  expect "key name and kind" ("Tags", `Keyword_list) (SA.name tags, SA.kind tags);
  let encoded =
    ok "encode"
      (SA.encode
         SA.
           [ set description "free text"; set customer "c-42"; set count (-7L);
             set ratio 0.5; set flag true;
             set deadline (time 1_704_067_200L 120_000_000);
             set tags [ "a"; "b" ]; unset (SA.keyword "Retired") ])
  in
  expect "encoded attributes"
    [ ("Description", typed {|"free text"|} "Text");
      ("CustomerId", typed {|"c-42"|} "Keyword");
      ("Attempts", typed "-7" "Int");
      ("Ratio", typed "0.5" "Double");
      ("Escalated", typed "true" "Bool");
      ("Deadline", typed {|"2024-01-01T00:00:00.12Z"|} "Datetime");
      ("Tags", typed {|["a","b"]|} "KeywordList");
      ("Retired", { T.Payload.metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }) ]
    encoded;
  expect "empty list" [] (ok "empty" (SA.encode []))

(** Datetimes are formatted in UTC with the fraction trimmed, including
    instants before 1970 and the bounds of the four-digit year range;
    instants outside it are codec errors naming the key. *)
let test_datetime_formatting () =
  let formatted seconds nanoseconds =
    match ok "datetime" (SA.encode [ SA.set deadline (time seconds nanoseconds) ]) with
    | [ (_, payload) ] -> Bytes.to_string payload.data
    | _ -> failwith "one datetime was not encoded as one entry"
  in
  expect "epoch" {|"1970-01-01T00:00:00Z"|} (formatted 0L 0);
  expect "before 1970" {|"1969-12-31T23:59:59.000000001Z"|} (formatted (-1L) 1);
  expect "leap day" {|"2024-02-29T12:34:56Z"|} (formatted 1_709_210_096L 0);
  expect "first instant" {|"0001-01-01T00:00:00Z"|} (formatted (-62_135_596_800L) 0);
  expect "last instant" {|"9999-12-31T23:59:59.999999999Z"|}
    (formatted 253_402_300_799L 999_999_999);
  let message =
    expect_error "year 10000" `Codec
      (SA.encode [ SA.set deadline (time 253_402_300_800L 0) ])
  in
  if not (contains message "Deadline") then failwith "datetime error does not name the key"

(** Values JSON cannot carry and malformed or repeated names are rejected
    before anything reaches Temporal: a value as a codec error and a name as
    a defect, each naming the attribute. *)
let test_encode_errors () =
  let codec label update =
    let message = expect_error label `Codec (SA.encode [ update ]) in
    if not (contains message "\"") then failwith (label ^ ": message does not name the key")
  in
  codec "nan double" (SA.set ratio Float.nan);
  codec "infinite double" (SA.set ratio Float.infinity);
  codec "invalid UTF-8 keyword" (SA.set customer "\xff");
  codec "invalid UTF-8 list item" (SA.set tags [ "ok"; "\xc3" ]);
  ignore (expect_error "empty name" `Defect (SA.encode [ SA.set (SA.keyword "") "x" ]));
  ignore (expect_error "NUL name" `Defect (SA.encode [ SA.set (SA.keyword "a\000b") "x" ]));
  ignore
    (expect_error "repeated name" `Defect
       (SA.encode [ SA.set customer "a"; SA.unset (SA.text "CustomerId") ]))

(** Every type decodes what [encode] produced, and a payload as Temporal
    Server returns it ([type] metadata added, another field order) decodes
    too. Absent names and removal payloads are [None]. *)
let test_decode () =
  let attributes =
    ok "encode"
      (SA.encode
         SA.
           [ set description "free text"; set customer "c-42"; set count Int64.max_int;
             set ratio (-0.25); set flag false;
             set deadline (time 1_704_067_200L 5); set tags [ "x"; "y"; "z" ];
             unset (SA.bool "Retired") ])
  in
  expect "text" (Some "free text") (ok "text" (SA.decode description attributes));
  expect "keyword" (Some "c-42") (ok "keyword" (SA.decode customer attributes));
  expect "int" (Some Int64.max_int) (ok "int" (SA.decode count attributes));
  expect "double" (Some (-0.25)) (ok "double" (SA.decode ratio attributes));
  expect "bool" (Some false) (ok "bool" (SA.decode flag attributes));
  (match ok "datetime" (SA.decode deadline attributes) with
  | Some value when T.Time.equal value (time 1_704_067_200L 5) -> ()
  | _ -> failwith "datetime did not round-trip");
  expect "keyword list" (Some [ "x"; "y"; "z" ]) (ok "list" (SA.decode tags attributes));
  expect "removal" None (ok "removal" (SA.decode (SA.bool "Retired") attributes));
  expect "absent" None (ok "absent" (SA.decode (SA.bool "Missing") attributes));
  let server =
    [ ( "Deadline",
        { T.Payload.metadata = [ ("type", "Datetime"); ("encoding", "json/plain") ];
          data = Bytes.of_string {|"2024-01-01T02:00:00.5+02:00"|} } );
      ("Unknown", typed "1" "SomeFutureType") ]
  in
  (match ok "offset datetime" (SA.decode deadline server) with
  | Some value when T.Time.equal value (time 1_704_067_200L 500_000_000) -> ()
  | _ -> failwith "an RFC 3339 offset was not applied");
  expect "unknown type metadata is ignored" (Some 1L)
    (ok "unknown type" (SA.decode (SA.int64 "Unknown") server))

(** A payload of another known type, malformed JSON for the key's type, a
    non-JSON encoding, or an invalid RFC 3339 text is a codec error. *)
let test_decode_errors () =
  let decode_error label key payload =
    ignore (expect_error label `Codec (SA.decode key [ (SA.name key, payload) ]))
  in
  decode_error "type mismatch" customer (typed {|"c-42"|} "Text");
  decode_error "wrong JSON" count (typed {|"7"|} "Int");
  decode_error "fractional int" count (typed "7.5" "Int");
  decode_error "list of numbers" tags (typed "[1]" "KeywordList");
  decode_error "binary payload" customer
    { T.Payload.metadata = [ ("encoding", "binary/plain") ]; data = Bytes.of_string "x" };
  List.iter
    (fun text -> decode_error text deadline (typed (Printf.sprintf "%S" text) "Datetime"))
    [ "2024-02-30T00:00:00Z"; "2024-01-01 00:00:00Z"; "2024-01-01T00:00:60Z";
      "2024-01-01T00:00:00"; "2024-01-01T00:00:00.Z"; "2024-01-01T00:00:00.1234567890Z";
      "2024-01-01T00:00:00+2:00" ]

(** A workflow that upserts typed search attributes and a memo, removes a
    key from each, and continues as new once. *)
let rec metadata_workflow =
  lazy
    (T.Workflow.define ~name:"metadata" ~input:T.Codec.bool ~output:T.Codec.string
       (fun continued ->
         let open T.Result_syntax in
         if continued then
           let* metadata = T.Workflow.start_metadata () in
           let memo_keys =
             List.map fst (Option.value metadata.memo ~default:[])
           in
           let* customer =
             SA.decode customer (Option.value metadata.search_attributes ~default:[])
           in
           Ok (String.concat "," memo_keys ^ "|" ^ Option.value customer ~default:"-")
         else (
           T.Workflow.upsert_typed_search_attributes
             SA.[ set customer "c-42"; set count 3L; set flag true ];
           T.Workflow.upsert_typed_search_attributes SA.[ unset flag ];
           let note = ok "note" (T.Codec.encode T.Codec.string "kept") in
           let draft = ok "draft" (T.Codec.encode T.Codec.string "draft") in
           T.Workflow.upsert_memo [ ("note", note); ("draft", draft) ];
           T.Workflow.upsert_memo [ ("draft", ok "none" (T.Codec.encode (T.Codec.option T.Codec.string) None)) ];
           T.Workflow.continue_as_new (Lazy.force metadata_workflow) true)))

(** The test environment merges memo and search-attribute upserts as the
    server does, and a continue-as-new successor starts with the merged
    memo and the current search attributes. *)
let test_testing_simulation () =
  let workflow = Lazy.force metadata_workflow in
  let environment =
    ok "create" (T.Testing.create ~workflows:[ T.Testing.workflow workflow ] ~activities:[] ())
  in
  Fun.protect
    ~finally:(fun () -> T.Testing.shutdown environment)
    (fun () ->
      let handle = ok "start" (T.Testing.start environment workflow false) in
      expect "successor start metadata" "note|c-42" (ok "result" (T.Testing.result handle));
      let memo = T.Testing.memo handle in
      expect "successor memo keys" [ "note" ] (List.map fst memo);
      expect "successor memo value" (Some "kept")
        (Option.map
           (fun payload -> ok "decode memo" (T.Codec.decode T.Codec.string payload))
           (List.assoc_opt "note" memo));
      let attributes = T.Testing.search_attributes handle in
      expect "successor search attributes" [ "Attempts"; "CustomerId" ] (List.map fst attributes);
      expect "typed attribute" (Some 3L) (ok "decode" (SA.decode count attributes)))

let () =
  test_encode_every_type ();
  test_datetime_formatting ();
  test_encode_errors ();
  test_decode ();
  test_decode_errors ();
  test_testing_simulation ()
