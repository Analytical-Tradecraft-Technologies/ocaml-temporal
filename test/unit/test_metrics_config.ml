(** Validation and bridge encoding of metrics-export options (#500).

    [Temporal_base.Metrics_config] must accept exactly the documents the Rust
    bridge accepts and must never echo a rejected value in its message. The
    Rust integration test [rust/core-bridge/tests/metrics_exporter.rs] checks
    the same rules from the other side of the boundary. *)

module Config = Temporal_base.Metrics_config

(** Returns whether [needle] occurs in [haystack]. *)
let contains ~haystack ~needle =
  let haystack_length = String.length haystack in
  let needle_length = String.length needle in
  let rec search offset =
    offset + needle_length <= haystack_length
    && (String.sub haystack offset needle_length = needle || search (offset + 1))
  in
  search 0

(** Asserts acceptance, failing with the returned message otherwise. *)
let accepted ?global_tags bind_address =
  match Config.prometheus ?global_tags ~bind_address () with
  | Ok config -> config
  | Error message -> failwith (bind_address ^ ": " ^ message)

(** Asserts rejection and that the message does not echo [secret]. *)
let rejected ?global_tags ?(secret = "never-echoed") bind_address =
  match Config.prometheus ?global_tags ~bind_address () with
  | Ok _ -> failwith ("accepted invalid metrics option for " ^ bind_address)
  | Error message -> assert (not (contains ~haystack:message ~needle:secret))

(** IPv4 and bracketed IPv6 socket addresses are accepted, including the
    ephemeral port; host names, missing ports, and malformed ports are not. *)
let test_bind_addresses () =
  List.iter
    (fun address -> ignore (accepted address))
    [
      "127.0.0.1:9464";
      "0.0.0.0:0";
      "255.255.255.255:1";
      "[::1]:9464";
      "[::]:65535";
      "[fe80::1:2]:80";
      "[2001:db8:0:0:0:0:0:1]:443";
      "[::ffff:192.0.2.1]:9464";
    ];
  (* Shorthand, octal, and hexadecimal IPv4 spellings that some platform
     resolvers (notably Windows) accept must be rejected everywhere. *)
  List.iter rejected
    [
      "";
      "1:9464";
      "127.1:9464";
      "127.0.1:9464";
      "0x7f.0.0.1:9464";
      "0177.0.0.1:9464";
      "127.0.0.01:9464";
      "256.0.0.1:9464";
      "127.0.0.1.1:9464";
      ":9464";
      "[]:9464";
      "127.0.0.1:09464";
      "[fe80::1%1]:80";
      "[1:2:3:4:5:6:7:8:9]:80";
      "[1::2::3]:80";
      "[1:2:3:4:5:6:7::8]:80";
      "[12345::1]:80";
      "localhost:9464";
      "127.0.0.1";
      "127.0.0.1:";
      "127.0.0.1:65536";
      "127.0.0.1:-1";
      "127.0.0.1:+80";
      "127.0.0.1: 80";
      "::1:9464";
      "[127.0.0.1]:9464";
      "1:9464";
      String.make 129 '1';
    ]

(** Tag names, values, count, and uniqueness follow the documented rules. *)
let test_global_tags () =
  let address = "127.0.0.1:0" in
  ignore
    (accepted
       ~global_tags:[ ("deployment", "blue"); ("_region", "ap-southeast-2") ]
       address);
  ignore (accepted ~global_tags:[ ("unicode", "\xc3\xa9t\xc3\xa9") ] address);
  List.iter
    (fun global_tags -> rejected ~global_tags ~secret:"secret-value" address)
    [
      [ ("task_queue", "secret-value") ];
      [ ("service_name", "secret-value") ];
      [ ("__reserved", "secret-value") ];
      [ ("9starts_with_digit", "secret-value") ];
      [ ("has-dash", "secret-value") ];
      [ ("", "secret-value") ];
      [ (String.make 65 'a', "secret-value") ];
      [ ("empty", "") ];
      [ ("newline", "secret-value\n") ];
      [ ("c1_control", "secret-value\xc2\x85") ];
      [ ("invalid_utf8", "secret-value\xff") ];
      [ ("long", "secret-value" ^ String.make 256 'v') ];
      [ ("dup", "secret-value"); ("dup", "other") ];
      List.init (Config.max_global_tags + 1) (fun index ->
          (Printf.sprintf "tag%d" index, "secret-value"));
    ];
  ignore
    (accepted
       ~global_tags:
         (List.init Config.max_global_tags (fun index ->
              (Printf.sprintf "tag%d" index, "v")))
       address)

(** The bridge document is exact, explicit about every option, and keeps tag
    order; JSON escaping is delegated to Yojson. *)
let test_encoding () =
  let config =
    match
      Config.prometheus
        ~global_tags:[ ("b", "quote\"value"); ("a", "1") ]
        ~counters_total_suffix:true ~durations_as_seconds:true
        ~bind_address:"[::1]:0" ()
    with
    | Ok config -> config
    | Error message -> failwith message
  in
  assert (Config.bind_address config = Some "[::1]:0");
  assert (Config.endpoint config = None);
  let expected =
    {|{"prometheus":{"bind_address":"[::1]:0","global_tags":[{"name":"b","value":"quote\"value"},{"name":"a","value":"1"}],"counters_total_suffix":true,"unit_suffix":false,"durations_as_seconds":true}}|}
  in
  let actual = Config.to_json config in
  if actual <> expected then failwith ("unexpected metrics document: " ^ actual);
  let defaults = Config.to_json (accepted "127.0.0.1:9464") in
  assert (
    defaults
    = {|{"prometheus":{"bind_address":"127.0.0.1:9464","global_tags":[],"counters_total_suffix":false,"unit_suffix":false,"durations_as_seconds":false}}|})

(** The reserved list matches the labels Temporal Core attaches. *)
let test_reserved_names () =
  List.iter
    (fun name -> assert (List.mem name Config.reserved_tag_names))
    [ "namespace"; "task_queue"; "workflow_type"; "activity_type"; "service_name" ]

(** A credential-shaped header value no OTLP message may contain (#1161). *)
let secret = "Bearer s3cr3t-token-value"

(** Validates an OTLP exporter with [secret] as its only header unless
    [headers] is given. *)
let otlp ?(headers = [ ("authorization", secret) ]) ?period_ms ?global_tags
    endpoint =
  Config.otlp ~headers ?period_ms ?global_tags ~endpoint ()

(** Asserts an OTLP rejection whose message names [rule] and echoes neither
    the header secret nor the endpoint. *)
let otlp_rejected ~rule result ~endpoint =
  match result with
  | Ok _ -> failwith ("accepted invalid OTLP option for " ^ endpoint)
  | Error message ->
      if not (contains ~haystack:message ~needle:rule) then
        failwith ("unexpected OTLP rejection: " ^ message);
      assert (not (contains ~haystack:message ~needle:"s3cr3t"));
      (* Very short endpoints such as [http://] are fragments of the rule
         text itself. *)
      if String.length endpoint > 8 then
        assert (not (contains ~haystack:message ~needle:endpoint))

(** The endpoint grammar is the Rust bridge's: the same accepted and
    rejected spellings as [rust/core-bridge/tests/support/otlp_validation.rs]. *)
let test_otlp_endpoints () =
  List.iter
    (fun endpoint ->
      match otlp endpoint with
      | Ok config -> assert (Config.endpoint config = Some endpoint)
      | Error message -> failwith (endpoint ^ ": " ^ message))
    [
      "http://localhost:4317";
      "https://otlp.example.com";
      "https://otlp.example.com:443/v1/metrics";
      "http://127.0.0.1:4318/v1/metrics";
      "http://[::1]:4317";
      "http://[2001:db8::1]";
      "https://collector-1.internal.example:4317/";
      "http://collector/v1/metrics?tenant=a";
      "http://Collector.Example.COM:4317";
      "http://localhost/" ^ String.make (Config.max_endpoint_bytes - 17) 'a';
    ];
  List.iter
    (fun endpoint ->
      otlp_rejected ~rule:"OTLP endpoint" (otlp endpoint) ~endpoint)
    [
      "";
      "localhost:4317";
      "grpc://localhost:4317";
      "HTTP://localhost:4317";
      "ftp://localhost";
      "http://";
      "http://:4317";
      "http://user:pass@localhost:4317";
      "http://token@localhost";
      "http://localhost:0";
      "http://localhost:65536";
      "http://localhost:04317";
      "http://localhost:";
      "http://localhost:43a7";
      "http://local host:4317";
      "http://localhost:4317/v1/metrics#frag";
      "http://localhost:4317/\tpath";
      "http://127.1:4317";
      "http://1.2.3.4.5";
      "http://example.123";
      "http://0x7f.0.0.1";
      "http://010.0.0.1";
      "http://-bad.example";
      "http://bad-.example";
      "http://a..b";
      "http://[::1%25eth0]:4317";
      "http://[::zz]";
      "http://[::1";
      "http://exa_mple.com";
      "http://localhost/" ^ String.make Config.max_endpoint_bytes 'a';
    ]

(** Header names, values, count, and uniqueness follow the documented
    rules, and no rejection echoes a header value. *)
let test_otlp_headers () =
  let endpoint = "http://localhost:4317" in
  ignore
    (Result.get_ok
       (otlp
          ~headers:
            [
              ("authorization", secret);
              ("x-tenant.id_2", "a b");
              ("api-key", String.make Config.max_header_value_bytes 'v');
            ]
          endpoint));
  List.iter
    (fun name ->
      otlp_rejected ~rule:"header names"
        (otlp ~headers:[ (name, secret) ] endpoint)
        ~endpoint:"")
    [
      "";
      "Authorization";
      "-leading";
      "x key";
      "x:key";
      "content-type";
      "user-agent";
      "te";
      "grpc-timeout";
      "trace-bin";
      String.make (Config.max_header_name_bytes + 1) 'a';
    ];
  List.iter
    (fun value ->
      otlp_rejected ~rule:"header values"
        (otlp ~headers:[ ("authorization", value) ] endpoint)
        ~endpoint:"")
    [
      "";
      " " ^ secret;
      secret ^ " ";
      secret ^ "\n";
      secret ^ "\xc3\xa9";
      secret ^ "\x7f";
      String.concat "" (List.init Config.max_header_value_bytes (fun _ -> "s3cr3t"));
    ];
  otlp_rejected ~rule:"unique"
    (otlp ~headers:[ ("a", secret); ("a", secret) ] endpoint)
    ~endpoint:"";
  otlp_rejected ~rule:"at most"
    (otlp
       ~headers:
         (List.init (Config.max_headers + 1) (fun index ->
              (Printf.sprintf "h%d" index, secret)))
       endpoint)
    ~endpoint:"";
  assert (List.mem "host" Config.reserved_header_names)

(** The period is bounded inclusively and global tags use the Prometheus
    rules. *)
let test_otlp_period_and_tags () =
  let endpoint = "http://localhost:4317" in
  List.iter
    (fun period_ms -> ignore (Result.get_ok (otlp ~period_ms endpoint)))
    [ Config.min_period_ms; Config.default_period_ms; Config.max_period_ms ];
  List.iter
    (fun period_ms ->
      otlp_rejected ~rule:"period" (otlp ~period_ms endpoint) ~endpoint:"")
    [ 0L; Int64.pred Config.min_period_ms; Int64.succ Config.max_period_ms; -1L ];
  otlp_rejected ~rule:"global tag names"
    (otlp ~global_tags:[ ("task_queue", "x") ] endpoint)
    ~endpoint:""

(** The OTLP bridge document is exact and explicit about every option. *)
let test_otlp_encoding () =
  let config =
    Result.get_ok
      (Config.otlp
         ~headers:[ ("authorization", "Bearer \"q\""); ("x-tenant", "a") ]
         ~period_ms:60_000L ~protocol:`Http_protobuf ~temporality:`Delta
         ~global_tags:[ ("deployment", "blue") ]
         ~durations_as_seconds:true
         ~endpoint:"https://otlp.example.com/v1/metrics" ())
  in
  assert (Config.bind_address config = None);
  let expected =
    {|{"otlp":{"endpoint":"https://otlp.example.com/v1/metrics","protocol":"http_protobuf","headers":[{"name":"authorization","value":"Bearer \"q\""},{"name":"x-tenant","value":"a"}],"period_ms":60000,"temporality":"delta","global_tags":[{"name":"deployment","value":"blue"}],"durations_as_seconds":true}}|}
  in
  let actual = Config.to_json config in
  if actual <> expected then failwith ("unexpected OTLP document: " ^ actual);
  let defaults =
    Config.to_json (Result.get_ok (Config.otlp ~endpoint:"http://[::1]:4317" ()))
  in
  if
    defaults
    <> {|{"otlp":{"endpoint":"http://[::1]:4317","protocol":"grpc","headers":[],"period_ms":1000,"temporality":"cumulative","global_tags":[],"durations_as_seconds":false}}|}
  then failwith ("unexpected default OTLP document: " ^ defaults)

let () =
  test_bind_addresses ();
  test_global_tags ();
  test_encoding ();
  test_reserved_names ();
  test_otlp_endpoints ();
  test_otlp_headers ();
  test_otlp_period_and_tags ();
  test_otlp_encoding ();
  print_endline "metrics config: ok"
