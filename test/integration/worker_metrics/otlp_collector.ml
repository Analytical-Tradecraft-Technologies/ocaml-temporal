(** In-process OTLP/HTTP collector stub for the live metrics regression
    (#1161).

    It listens on an ephemeral loopback port and answers every
    [POST /v1/metrics] the SDK's OTLP exporter sends with [200] and an empty
    [ExportMetricsServiceResponse], as an OpenTelemetry Collector does. Each
    request's path, headers (names lowercased), and protobuf body are kept
    for the regression to inspect. No container image or network beyond
    loopback is involved. Each connection is served on its own thread,
    because the exporter's HTTP client may open a second connection while
    keeping the first one alive. *)

(** One received export request. *)
type export = {
  path : string;
  headers : (string * string) list;
  body : string;
}

(** A running stub. [mutex] guards [received] and [stopped]. *)
type t = {
  listener : Unix.file_descr;
  port : int;
  mutex : Mutex.t;
  mutable received : export list;
  mutable stopped : bool;
}

(** Runs [f] with [collector.mutex] held. *)
let locked collector f =
  Mutex.lock collector.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock collector.mutex) f

(** The index just after the first ["\r\n\r\n"] in [text], if any. *)
let head_end text =
  let rec search index =
    if index + 4 > String.length text then None
    else if String.sub text index 4 = "\r\n\r\n" then Some (index + 4)
    else search (index + 1)
  in
  search 0

(** Splits a request head into its path and lowercased headers. *)
let parse_head head =
  match String.split_on_char '\n' head |> List.map String.trim with
  | [] -> ("", [])
  | request_line :: lines ->
      let path =
        match String.split_on_char ' ' request_line with
        | _ :: path :: _ -> path
        | _ -> ""
      in
      let headers =
        List.filter_map
          (fun line ->
            match String.index_opt line ':' with
            | None -> None
            | Some colon ->
                Some
                  ( String.lowercase_ascii (String.sub line 0 colon),
                    String.trim
                      (String.sub line (colon + 1)
                         (String.length line - colon - 1)) ))
          lines
      in
      (path, headers)

(** Serves requests on one connection until the client closes it. *)
let serve collector connection =
  let chunk = Bytes.create 65_536 in
  let pending = Buffer.create 65_536 in
  let read_more () =
    match Unix.read connection chunk 0 (Bytes.length chunk) with
    | 0 -> false
    | count ->
        Buffer.add_subbytes pending chunk 0 count;
        true
    | exception Unix.Unix_error _ -> false
  in
  let rec next_request () =
    match head_end (Buffer.contents pending) with
    | None -> if read_more () then next_request ()
    | Some stop ->
        let text = Buffer.contents pending in
        let path, headers = parse_head (String.sub text 0 stop) in
        let length =
          List.assoc_opt "content-length" headers
          |> Option.map int_of_string |> Option.value ~default:0
        in
        let rec complete () =
          String.length (Buffer.contents pending) >= stop + length
          || (read_more () && complete ())
        in
        if complete () then (
          let text = Buffer.contents pending in
          let body = String.sub text stop length in
          let rest =
            String.sub text (stop + length) (String.length text - stop - length)
          in
          Buffer.clear pending;
          Buffer.add_string pending rest;
          locked collector (fun () ->
              collector.received <- { path; headers; body } :: collector.received);
          let response =
            "HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\n\
             Content-Length: 0\r\n\r\n"
          in
          match
            Unix.write_substring connection response 0 (String.length response)
          with
          | _ -> next_request ()
          | exception Unix.Unix_error _ -> ())
  in
  Fun.protect ~finally:(fun () -> Unix.close connection) next_request

(** Accepts connections until {!stop}, polling so the loop notices the stop
    without relying on [close] waking a blocked [accept]. *)
let rec accept_loop collector =
  if not (locked collector (fun () -> collector.stopped)) then (
    (match Unix.select [ collector.listener ] [] [] 0.2 with
    | [], _, _ -> ()
    | _ -> (
        match Unix.accept collector.listener with
        | connection, _ -> ignore (Thread.create (serve collector) connection)
        | exception Unix.Unix_error _ -> ())
    | exception Unix.Unix_error (Unix.EINTR, _, _) -> ());
    accept_loop collector)

(** Starts a stub on an ephemeral loopback port. *)
let start () =
  let listener = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Unix.setsockopt listener Unix.SO_REUSEADDR true;
  Unix.bind listener (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
  Unix.listen listener 16;
  let port =
    match Unix.getsockname listener with
    | Unix.ADDR_INET (_, port) -> port
    | Unix.ADDR_UNIX _ -> failwith "loopback listener has no port"
  in
  let collector =
    { listener; port; mutex = Mutex.create (); received = []; stopped = false }
  in
  ignore (Thread.create accept_loop collector);
  collector

(** The OTLP/HTTP metrics URL of the stub. *)
let endpoint collector =
  Printf.sprintf "http://127.0.0.1:%d/v1/metrics" collector.port

(** Every export received so far, oldest first. *)
let exports collector = locked collector (fun () -> List.rev collector.received)

(** Stops accepting new connections and closes the listener. *)
let stop collector =
  locked collector (fun () -> collector.stopped <- true);
  Unix.sleepf 0.3;
  Unix.close collector.listener

(** Reads one protobuf varint at [position]; returns it and the next
    position. Fails on a truncated or over-long varint. *)
let read_varint text position =
  let rec go position shift value =
    if position >= String.length text || shift > 63 then
      failwith "truncated protobuf varint"
    else
      let byte = Char.code text.[position] in
      let value = value lor ((byte land 0x7f) lsl shift) in
      if byte land 0x80 = 0 then (value, position + 1)
      else go (position + 1) (shift + 7) value
  in
  go position 0 0

(** The length-delimited fields numbered [field] of one protobuf message, in
    order. Other wire types are skipped; malformed input fails. *)
let submessages field text =
  let rec go position acc =
    if position >= String.length text then List.rev acc
    else
      let key, position = read_varint text position in
      let number = key lsr 3 in
      match key land 7 with
      | 0 ->
          let _, position = read_varint text position in
          go position acc
      | 1 -> go (position + 8) acc
      | 2 ->
          let length, position = read_varint text position in
          if position + length > String.length text then
            failwith "truncated protobuf field";
          let value = String.sub text position length in
          go (position + length)
            (if number = field then value :: acc else acc)
      | 5 -> go (position + 4) acc
      | wire -> failwith (Printf.sprintf "unsupported protobuf wire type %d" wire)
  in
  go 0 []

(** The metric names of one [ExportMetricsServiceRequest] body:
    [resource_metrics] (1) / [scope_metrics] (2) / [metrics] (2) /
    [name] (1), as numbered in the OTLP protobuf definitions. *)
let metric_names body =
  submessages 1 body
  |> List.concat_map (submessages 2)
  |> List.concat_map (submessages 2)
  |> List.concat_map (submessages 1)
