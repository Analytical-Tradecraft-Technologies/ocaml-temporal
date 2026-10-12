(** Public [Temporal.Runtime] metrics export without a Temporal Server (#500).

    A runtime created with [~metrics] must bind its Prometheus endpoint
    before [create] returns, report the resolved address, answer scrapes,
    reject a busy address as a typed [`Bridge] error without leaking a
    runtime, and close the endpoint once [Runtime.shutdown] has returned.
    Invalid options are typed defects raised before any native work. *)
open Temporal

(** Unwraps a public result in fixture setup. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Splits ["host:port"] (IPv4 only, as these tests bind loopback). *)
let socket_of address =
  match String.rindex_opt address ':' with
  | None -> failwith ("not a socket address: " ^ address)
  | Some colon ->
      let host = String.sub address 0 colon in
      let port =
        int_of_string
          (String.sub address (colon + 1) (String.length address - colon - 1))
      in
      Unix.ADDR_INET (Unix.inet_addr_of_string host, port)

(** Upper bound, in seconds, on one scrape. Generous because Windows CI
    runners can stall a loopback round trip for several seconds (#1058). *)
let scrape_timeout_seconds = 30.

(** The [Content-Length] of a response whose header block is [headers], if
    present. Header names are case-insensitive. *)
let content_length headers =
  String.split_on_char '\n' headers
  |> List.find_map (fun line ->
         match String.index_opt line ':' with
         | None -> None
         | Some colon ->
             let name = String.lowercase_ascii (String.sub line 0 colon) in
             if name <> "content-length" then None
             else
               String.sub line (colon + 1) (String.length line - colon - 1)
               |> String.trim |> int_of_string_opt)

(** Whether [response] holds a complete header block and, when it declares
    a [Content-Length], that whole body. Without a length the response ends
    only at EOF. *)
let complete response =
  let text = Buffer.contents response in
  let marker = "\r\n\r\n" in
  let rec find_marker index =
    if index + 4 > String.length text then None
    else if String.sub text index 4 = marker then Some index
    else find_marker (index + 1)
  in
  match find_marker 0 with
  | None -> false
  | Some header_end -> (
      match content_length (String.sub text 0 header_end) with
      | None -> false
      | Some length -> String.length text - (header_end + 4) >= length)

(** Sends one HTTP/1.1 GET and returns the full response.

    The response is complete once its headers and [Content-Length] body have
    arrived; the helper does not wait for the server to close the
    connection, whose timing is not part of the endpoint's contract and
    varies on Windows (#1058). A timeout fails with the number of bytes
    received, which tells a slow close apart from a missing response. *)
let http_get address path =
  let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Fun.protect
    ~finally:(fun () -> Unix.close socket)
    (fun () ->
      Unix.setsockopt_float socket Unix.SO_RCVTIMEO scrape_timeout_seconds;
      Unix.connect socket (socket_of address);
      let request =
        Printf.sprintf
          "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n" path
          address
      in
      ignore (Unix.write_substring socket request 0 (String.length request));
      let buffer = Buffer.create 4096 in
      let chunk = Bytes.create 4096 in
      let rec read () =
        if not (complete buffer) then
          match Unix.read socket chunk 0 (Bytes.length chunk) with
          | 0 -> ()
          | count ->
              Buffer.add_subbytes buffer chunk 0 count;
              read ()
          | exception Unix.Unix_error ((Unix.ETIMEDOUT | Unix.EAGAIN), _, _) ->
              failwith
                (Printf.sprintf
                   "scrape of %s%s timed out after %.0f s with %d byte(s) \
                    received"
                   address path scrape_timeout_seconds (Buffer.length buffer))
      in
      read ();
      Buffer.contents buffer)

(** Whether a TCP connection to [address] is refused. *)
let refused address =
  let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Fun.protect
    ~finally:(fun () -> Unix.close socket)
    (fun () ->
      match Unix.connect socket (socket_of address) with
      | () -> false
      | exception Unix.Unix_error (Unix.ECONNREFUSED, _, _) -> true)

(** The endpoint lives exactly as long as the runtime. *)
let test_endpoint_lifecycle () =
  let metrics =
    get
      (Runtime.Metrics.prometheus ~bind_address:"127.0.0.1:0"
         ~global_tags:[ ("deployment", "test") ] ())
  in
  let runtime = get (Runtime.create ~io_threads:1 ~metrics ()) in
  let address =
    match Runtime.metrics_address runtime with
    | Some address -> address
    | None -> failwith "metrics address missing"
  in
  assert (String.starts_with ~prefix:"127.0.0.1:" address);
  assert (not (String.ends_with ~suffix:":0" address));
  let scrape = http_get address "/metrics" in
  assert (String.starts_with ~prefix:"HTTP/1.1 200" scrape);
  (* A second exporter cannot take the same address; the failed create
     leaves no runtime behind and the first endpoint keeps serving. *)
  let busy = get (Runtime.Metrics.prometheus ~bind_address:address ()) in
  (match Runtime.create ~io_threads:1 ~metrics:busy () with
  | Ok _ -> failwith "a busy metrics address was accepted"
  | Error error ->
      assert ((Error.view error).category = `Bridge);
      assert (String.length (Error.message error) > 0));
  assert (String.starts_with ~prefix:"HTTP/1.1 200" (http_get address "/metrics"));
  get (Runtime.shutdown runtime);
  assert (Runtime.metrics_address runtime = Some address);
  assert (refused address)

(** Without [~metrics] no endpoint exists. *)
let test_no_metrics_by_default () =
  let runtime = get (Runtime.create ~io_threads:1 ()) in
  assert (Runtime.metrics_address runtime = None);
  get (Runtime.shutdown runtime)

(** Invalid options are typed defects. *)
let test_invalid_options () =
  List.iter
    (fun (bind_address, global_tags) ->
      match Runtime.Metrics.prometheus ~bind_address ~global_tags () with
      | Ok _ -> failwith ("accepted invalid metrics option " ^ bind_address)
      | Error error -> assert ((Error.view error).category = `Defect))
    [
      ("localhost:9464", []);
      ("127.0.0.1:0", [ ("task_queue", "x") ]);
      ("127.0.0.1:0", [ ("ok", "") ]);
    ]

let () =
  test_endpoint_lifecycle ();
  test_no_metrics_by_default ();
  test_invalid_options ();
  print_endline "runtime metrics: ok"
