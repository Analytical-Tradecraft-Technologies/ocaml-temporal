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
  assert (Config.bind_address config = "[::1]:0");
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

let () =
  test_bind_addresses ();
  test_global_tags ();
  test_encoding ();
  test_reserved_names ();
  print_endline "metrics config: ok"
