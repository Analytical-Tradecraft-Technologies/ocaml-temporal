(** Public [Temporal.Runtime] metrics export without a Temporal Server (#500).

    A runtime created with [~metrics] must bind its Prometheus endpoint
    before [create] returns, report the resolved address, answer scrapes,
    reject a busy address as a typed [`Bridge] error without leaking a
    runtime, survive scrapers that reset the connection mid-response, and
    close the endpoint once [Runtime.shutdown] has returned.
    Invalid options are typed defects raised before any native work. *)
open Temporal

(** Unwraps a public result in fixture setup. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Whether [needle] occurs in [haystack]. *)
let contains haystack needle =
  let n = String.length needle and h = String.length haystack in
  let rec at i =
    i + n <= h && (String.sub haystack i n = needle || at (i + 1))
  in
  at 0

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

(** Upper bound, in seconds, on one scrape. A loaded CI runner can delay a
    loopback round trip, so the bound is generous; it is enforced with
    [Unix.select] rather than [SO_RCVTIMEO] (see {!wait_readable}). *)
let scrape_timeout_seconds = 30.

(** Raised when a scrape misses its deadline. [stage] names the step that was
    waiting, [elapsed] is in seconds, and [received] counts the response
    bytes that had arrived, so a missing response is told apart from a
    truncated one (#1072). *)
exception
  Scrape_timeout of {
    label : string;
    address : string;
    stage : string;
    elapsed : float;
    received : int;
  }

(** A one-line rendering of {!Scrape_timeout}, so an uncaught timeout names
    the scrape and the stage in the test log. *)
let () =
  Printexc.register_printer (function
    | Scrape_timeout { label; address; stage; elapsed; received } ->
        Some
          (Printf.sprintf
             "%s: scrape of %s/metrics timed out after %.3f s while %s, with \
              %d byte(s) received"
             label address elapsed stage received)
    | _ -> None)

(** Waits until [socket] is readable or [deadline] (an absolute
    [Unix.gettimeofday] time) passes; returns whether it became readable.

    [SO_RCVTIMEO] cannot express this portably from OCaml: on Windows,
    [Unix.setsockopt_float] passes a [struct timeval], while Winsock reads
    the option as a [DWORD] of milliseconds, i.e. the [tv_sec] field. A
    "30 s" timeout was therefore 30 ms there, which made a scrape on a busy
    runner fail with nothing received (#1058, #1072). [Unix.select] takes
    seconds on every platform. An interrupted wait is resumed with the
    remaining time. *)
let rec wait_readable socket deadline =
  let remaining = deadline -. Unix.gettimeofday () in
  if remaining <= 0. then false
  else
    match Unix.select [ socket ] [] [] remaining with
    | [], _, _ -> wait_readable socket deadline
    | _ -> true
    | exception Unix.Unix_error (Unix.EINTR, _, _) ->
        wait_readable socket deadline

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
    varies on Windows (#1058). The whole exchange shares one [timeout]
    (seconds, default {!scrape_timeout_seconds}); missing it raises
    {!Scrape_timeout} naming [label] (which scrape of the test this is) and
    the stage that was waiting. Connecting is a blocking loopback [connect],
    which the listen backlog completes without the server's participation, so
    a server that never accepts shows up as a timeout while waiting for the
    first response byte. *)
let http_get ?(timeout = scrape_timeout_seconds) ~label address path =
  let started = Unix.gettimeofday () in
  let deadline = started +. timeout in
  let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Fun.protect
    ~finally:(fun () -> Unix.close socket)
    (fun () ->
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
          if not (wait_readable socket deadline) then
            raise
              (Scrape_timeout
                 {
                   label;
                   address;
                   stage =
                     (if Buffer.length buffer = 0 then
                        "waiting for the first response byte"
                      else "reading the rest of the response");
                   elapsed = Unix.gettimeofday () -. started;
                   received = Buffer.length buffer;
                 })
          else
            match Unix.read socket chunk 0 (Bytes.length chunk) with
            | 0 -> ()
            | count ->
                Buffer.add_subbytes buffer chunk 0 count;
                read ()
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
  let scrape = http_get ~label:"first scrape after create" address "/metrics" in
  assert (String.starts_with ~prefix:"HTTP/1.1 200" scrape);
  (* A second exporter cannot take the same address; the failed create
     leaves no runtime behind and the first endpoint keeps serving. *)
  let busy = get (Runtime.Metrics.prometheus ~bind_address:address ()) in
  (match Runtime.create ~io_threads:1 ~metrics:busy () with
  | Ok _ -> failwith "a busy metrics address was accepted"
  | Error error ->
      assert ((Error.view error).category = `Bridge);
      assert (String.length (Error.message error) > 0));
  let scrape =
    http_get ~label:"scrape after the busy-address create" address "/metrics"
  in
  assert (String.starts_with ~prefix:"HTTP/1.1 200" scrape);
  get (Runtime.shutdown runtime);
  assert (Runtime.metrics_address runtime = Some address);
  assert (refused address)

(** Scrapers that send a request and then reset the connection make the
    endpoint write into dead sockets on Core's Tokio threads. With SIGPIPE
    at its default, terminating action (as in any OCaml program that does
    not change it), the process must survive and the endpoint must keep
    serving (#1041). On Linux this is the [writev] path that used to kill
    the process with status 141; macOS sockets never raise the signal, and
    Windows has none. *)
let test_aborted_scrapes_do_not_kill_the_process () =
  if not Sys.win32 then Sys.set_signal Sys.sigpipe Sys.Signal_default;
  let metrics = get (Runtime.Metrics.prometheus ~bind_address:"127.0.0.1:0" ()) in
  let runtime = get (Runtime.create ~io_threads:1 ~metrics ()) in
  let address = Option.get (Runtime.metrics_address runtime) in
  let request =
    Printf.sprintf "GET /metrics HTTP/1.1\r\nHost: %s\r\n\r\n" address
  in
  for round = 1 to 64 do
    let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    Unix.connect socket (socket_of address);
    ignore (Unix.write_substring socket request 0 (String.length request));
    (* Let the response start before the reset on half the rounds. *)
    if round mod 2 = 0 then Unix.sleepf 0.001;
    (* A zero linger time makes [close] send RST instead of FIN. *)
    Unix.setsockopt_optint socket Unix.SO_LINGER (Some 0);
    Unix.close socket
  done;
  (* Since #500's connection cap, relays for the reset connections may still
     hold some of the endpoint's slots for a moment, and a scrape arriving
     then is answered with the documented transient busy [503] or, when even
     the rejection slots are taken, closed without a response. The property
     under test is that the process survived and the endpoint serves again
     once those relays end, so the scrape is retried briefly on exactly those
     transient outcomes. *)
  let deadline = Unix.gettimeofday () +. 10. in
  let rec scrape_until_served attempt =
    let outcome : [ `Response of string | `Closed ] =
      (* Each attempt gets only the time left before the shared deadline,
         so a scrape that hangs cannot stretch the bound to 10 s plus a
         full per-scrape timeout. *)
      let timeout = Float.max 0.1 (deadline -. Unix.gettimeofday ()) in
      match
        http_get ~timeout ~label:"scrape after 64 reset connections" address
          "/metrics"
      with
      | response -> `Response response
      | exception Unix.Unix_error ((Unix.ECONNRESET | Unix.EPIPE), _, _) ->
          `Closed
    in
    let transient =
      match outcome with
      | `Closed -> true
      | `Response "" -> true
      | `Response response ->
          String.starts_with ~prefix:"HTTP/1.1 503" response
          && contains response "Too many concurrent metrics scrapes"
    in
    match outcome with
    | `Response response when String.starts_with ~prefix:"HTTP/1.1 200" response
      -> ()
    | _ when transient && Unix.gettimeofday () < deadline ->
        Unix.sleepf 0.05;
        scrape_until_served (attempt + 1)
    | `Response response ->
        failwith
          (Printf.sprintf
             "endpoint did not serve after reset connections (attempt %d): %S"
             attempt
             (String.sub response 0 (min 200 (String.length response))))
    | `Closed ->
        failwith "endpoint kept closing scrapes after reset connections"
  in
  scrape_until_served 1;
  get (Runtime.shutdown runtime)

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

(** The scrape helper's timeout is measured in seconds on every platform
    (#1072). A listener that never accepts completes the TCP handshake from
    its backlog but sends nothing, so a 1 s scrape must fail at the first
    response byte no sooner than about 1 s. The previous [SO_RCVTIMEO]
    helper failed this on Windows after about 1 ms, which is how a "30 s"
    bound became a 30 ms one there. *)
let test_scrape_timeout_is_in_seconds () =
  let listener = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Fun.protect
    ~finally:(fun () -> Unix.close listener)
    (fun () ->
      Unix.bind listener (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
      Unix.listen listener 1;
      let address =
        match Unix.getsockname listener with
        | Unix.ADDR_INET (host, port) ->
            Printf.sprintf "%s:%d" (Unix.string_of_inet_addr host) port
        | Unix.ADDR_UNIX _ -> failwith "loopback listener has no port"
      in
      let timeout = 1. in
      match http_get ~timeout ~label:"silent listener" address "/metrics" with
      | _ -> failwith "a silent listener produced a response"
      | exception Scrape_timeout { stage; elapsed; received; _ } ->
          assert (stage = "waiting for the first response byte");
          assert (received = 0);
          (* [select] may wake one clock tick early; Windows ticks are
             about 16 ms. *)
          if elapsed < timeout -. 0.1 then
            failwith
              (Printf.sprintf "a %.1f s scrape timed out after %.3f s" timeout
                 elapsed))

let () =
  test_scrape_timeout_is_in_seconds ();
  test_endpoint_lifecycle ();
  test_aborted_scrapes_do_not_kill_the_process ();
  test_no_metrics_by_default ();
  test_invalid_options ();
  print_endline "runtime metrics: ok"
