(** Boundary tests for the configurable client admission limits (#796).

    The private bridge must reject each out-of-range limit before a native
    call, encode valid limits as the exact document that
    [rust/core-bridge/tests/support/client_calls.rs] decodes, and keep a
    default client's document byte-for-byte equal to the pre-#796 encoding.
    Reaching the refused endpoint's [Connection] error with a limited
    document proves that Rust strictly decoded and validated it. The public
    [Client.create] must report invalid limits as typed defects for every
    target. *)

module Bridge = Temporal_core_bridge.Native_bridge
module Client = Temporal.Client
module Error = Temporal.Error

(** A local port nothing listens on, so connecting fails fast. *)
let refused_target = "http://127.0.0.1:1"

(** Extracts a successful bridge result or fails with its message. *)
let unwrap = function
  | Ok value -> value
  | Error error -> failwith error.Bridge.message

(** Builds limits from optional values. *)
let limits ?max_pending_waits ?max_pending_starts ?max_in_flight_calls () :
    Bridge.client_limits =
  { max_pending_waits; max_pending_starts; max_in_flight_calls }

(** Renders the connect document for [limits]. *)
let document limits =
  Bridge.client_config_document
    (unwrap
       (Bridge.client_config ~limits ~target_url:refused_target
          ~identity:"unit-test" ()))

(** Fails unless [actual] equals [expected]. *)
let expect_document expected actual =
  if not (String.equal expected actual) then
    failwith (Printf.sprintf "expected %s, got %s" expected actual)

(** A default client sends the pre-#796 document; explicit limits are
    appended in a fixed order and only when set. *)
let test_documents () =
  let base = {|{"target_url":"http://127.0.0.1:1","identity":"unit-test"|} in
  expect_document (base ^ "}") (document Bridge.default_client_limits);
  expect_document (base ^ "}")
    (Bridge.client_config_document
       (unwrap
          (Bridge.client_config ~target_url:refused_target ~identity:"unit-test"
             ())));
  expect_document
    (base
   ^ {|,"max_pending_waits":2,"max_pending_starts":3,"max_in_flight_calls":4}|}
    )
    (document
       (limits ~max_pending_waits:2 ~max_pending_starts:3
          ~max_in_flight_calls:4 ()));
  expect_document
    (base ^ {|,"max_pending_starts":4096}|})
    (document (limits ~max_pending_starts:4_096 ()))

(** Each limit accepts [1..maximum] and rejects anything else with a
    [Configuration] error naming the field and the range. *)
let test_bridge_validation () =
  let cases =
    [
      ( "max_pending_waits",
        Bridge.max_configurable_pending_waits,
        fun value -> limits ~max_pending_waits:value () );
      ( "max_pending_starts",
        Bridge.max_configurable_pending_starts,
        fun value -> limits ~max_pending_starts:value () );
      ( "max_in_flight_calls",
        Bridge.max_configurable_in_flight_calls,
        fun value -> limits ~max_in_flight_calls:value () );
    ]
  in
  assert (Bridge.max_configurable_pending_waits = 4_096);
  assert (Bridge.max_configurable_pending_starts = 4_096);
  assert (Bridge.max_configurable_in_flight_calls = 65_536);
  List.iter
    (fun (name, maximum, make) ->
      List.iter
        (fun value -> ignore (document (make value)))
        [ 1; maximum ];
      let expected =
        Printf.sprintf "client %s must be between 1 and %d" name maximum
      in
      List.iter
        (fun value ->
          match
            Bridge.client_config ~limits:(make value)
              ~target_url:refused_target ~identity:"unit-test" ()
          with
          | Error { Bridge.status = Configuration; message } ->
              if not (String.equal message expected) then
                failwith (Printf.sprintf "expected %S, got %S" expected message)
          | Error { message; _ } -> failwith ("unexpected status: " ^ message)
          | Ok _ -> failwith (name ^ " accepted an out-of-range value"))
        [ 0; -1; maximum + 1; min_int ])
    cases

(** Rust decodes and accepts a limited document: connecting to a refused
    endpoint fails with [Connection], not with [Configuration]. *)
let test_rust_accepts_limits () =
  let runtime = unwrap (Bridge.runtime_create ~worker_threads:1 ()) in
  let config =
    unwrap
      (Bridge.client_config
         ~limits:
           (limits ~max_pending_waits:2 ~max_pending_starts:4_096
              ~max_in_flight_calls:65_536 ())
         ~target_url:refused_target ~identity:"unit-test" ())
  in
  (match Bridge.client_connect runtime config with
  | Error { Bridge.status = Connection; _ } -> ()
  | Error { message; _ } -> failwith ("limited document rejected: " ^ message)
  | Ok () -> failwith "refused endpoint accepted a connection");
  unwrap (Bridge.runtime_close runtime)

(** [Client.create] reports an invalid limit as a typed defect for both the
    mock and a native target, before any connection is attempted, and
    accepts valid limits on the mock. *)
let test_public_create () =
  let invalid =
    [
      (fun target ->
        Client.create ~max_pending_waits:0 ~target_url:target
          ~namespace:"default" ());
      (fun target ->
        Client.create ~max_pending_starts:4_097 ~target_url:target
          ~namespace:"default" ());
      (fun target ->
        Client.create ~max_in_flight_calls:65_537 ~target_url:target
          ~namespace:"default" ());
    ]
  in
  List.iter
    (fun target ->
      List.iter
        (fun create ->
          match create target with
          | Error error -> assert ((Error.view error).category = `Defect)
          | Ok _ -> failwith "an invalid client limit was accepted")
        invalid)
    [ "mock://client-limits"; refused_target ];
  match
    Client.create ~max_pending_waits:2 ~max_pending_starts:1
      ~max_in_flight_calls:3 ~target_url:"mock://client-limits"
      ~namespace:"default" ()
  with
  | Ok client -> (
      match Client.shutdown client with
      | Ok () -> ()
      | Error error -> failwith (Error.message error))
  | Error error -> failwith ("valid limits rejected: " ^ Error.message error)

(** Runs the client limit boundary tests. *)
let () =
  test_documents ();
  test_bridge_validation ();
  test_rust_accepts_limits ();
  test_public_create ()
