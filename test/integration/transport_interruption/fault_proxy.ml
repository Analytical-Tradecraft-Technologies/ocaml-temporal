(* See fault_proxy.mli for the contract. *)

type mode = Forward | Refuse | Drop_responses

(** Which way a pump copies bytes; only server-to-client bytes are dropped. *)
type direction = Client_to_server | Server_to_client

(** One proxied connection. [pumps] counts the live pump threads; the last
    one to exit closes both descriptors, so a concurrent [shutdown] from the
    control side never races with a close and a reused descriptor number. *)
type connection = {
  client : Unix.file_descr;
  server : Unix.file_descr;
  pumps : int Atomic.t;
}

type t = {
  name : string;
  listener : Unix.file_descr;
  port : int;
  upstream : Unix.sockaddr;
  mode : mode Atomic.t;
  stopping : bool Atomic.t;
  accepted : int Atomic.t;
  (* Guards [connections] and [next_id]. Held only for table updates and
     socket shutdown calls, never across blocking I/O. *)
  mutex : Mutex.t;
  connections : (int, connection) Hashtbl.t;
  mutable next_id : int;
  mutable domain : unit Domain.t option;
}

(** Process start, so log offsets from all Domains share one origin. *)
let origin = Unix.gettimeofday ()

(** Serializes log lines written from the proxy and test Domains. *)
let log_mutex = Mutex.create ()

let log format =
  Printf.ksprintf
    (fun line ->
      Mutex.protect log_mutex (fun () ->
          Printf.printf "transport-fault +%.3fs %s\n%!"
            (Unix.gettimeofday () -. origin)
            line))
    format

(** Stable lowercase names used in log lines. *)
let mode_name = function
  | Forward -> "forward"
  | Refuse -> "refuse"
  | Drop_responses -> "drop_responses"

(** Shuts a socket down in both directions, ignoring an already closed or
    reset peer. A shutdown wakes a pump blocked in [read] on either side. *)
let shutdown_quietly fd =
  try Unix.shutdown fd Unix.SHUTDOWN_ALL with Unix.Unix_error _ -> ()

(** Closes a descriptor exactly once from its last owning pump. *)
let close_quietly fd = try Unix.close fd with Unix.Unix_error _ -> ()

(** Shuts down every live connection; the pumps then close and unregister. *)
let reset_all proxy =
  Mutex.protect proxy.mutex (fun () ->
      Hashtbl.iter
        (fun _ connection ->
          shutdown_quietly connection.client;
          shutdown_quietly connection.server)
        proxy.connections)

(** Writes the whole buffer prefix, returning [false] when the peer is gone. *)
let rec write_all fd buffer offset length =
  if length = 0 then true
  else
    match Unix.write fd buffer offset length with
    | written -> write_all fd buffer (offset + written) (length - written)
    | exception Unix.Unix_error (Unix.EINTR, _, _) ->
        write_all fd buffer offset length
    | exception Unix.Unix_error _ -> false

(** Copies one direction until either side closes. The mode is read for each
    chunk, so a switch applies to the next bytes the kernel delivers, and a
    dropped chunk is never buffered for later delivery. *)
let pump proxy id connection direction =
  let source, target =
    match direction with
    | Client_to_server -> (connection.client, connection.server)
    | Server_to_client -> (connection.server, connection.client)
  in
  let buffer = Bytes.create 65_536 in
  let rec loop () =
    match Unix.read source buffer 0 (Bytes.length buffer) with
    | 0 -> ()
    | count ->
        let forward =
          match (Atomic.get proxy.mode, direction) with
          | Forward, _ | Drop_responses, Client_to_server -> true
          | Drop_responses, Server_to_client | Refuse, _ -> false
        in
        if (not forward) || write_all target buffer 0 count then loop ()
    | exception Unix.Unix_error (Unix.EINTR, _, _) -> loop ()
    | exception Unix.Unix_error _ -> ()
  in
  loop ();
  (* Either side ending ends the whole connection, as a real TCP proxy would. *)
  shutdown_quietly connection.client;
  shutdown_quietly connection.server;
  if Atomic.fetch_and_add connection.pumps (-1) = 1 then begin
    Mutex.protect proxy.mutex (fun () -> Hashtbl.remove proxy.connections id);
    close_quietly connection.client;
    close_quietly connection.server
  end

(** Connects one accepted client to the upstream and starts its two pumps,
    or closes it at once in [Refuse] mode or when the upstream is down. *)
let admit proxy client =
  Atomic.incr proxy.accepted;
  if Atomic.get proxy.mode = Refuse then begin
    close_quietly client;
    []
  end
  else
    let server = Unix.socket (Unix.domain_of_sockaddr proxy.upstream) Unix.SOCK_STREAM 0 in
    match Unix.connect server proxy.upstream with
    | exception Unix.Unix_error _ ->
        close_quietly server;
        close_quietly client;
        []
    | () ->
        Unix.setsockopt client Unix.TCP_NODELAY true;
        Unix.setsockopt server Unix.TCP_NODELAY true;
        let connection = { client; server; pumps = Atomic.make 2 } in
        let id =
          Mutex.protect proxy.mutex (fun () ->
              let id = proxy.next_id in
              proxy.next_id <- id + 1;
              Hashtbl.replace proxy.connections id connection;
              id)
        in
        [
          Thread.create (fun () -> pump proxy id connection Client_to_server) ();
          Thread.create (fun () -> pump proxy id connection Server_to_client) ();
        ]

(** Accepts until [stop]. The short [select] timeout lets the loop observe
    [stopping] on platforms where closing a listener does not wake [accept].
    Returns after every pump thread it created has exited. *)
let serve proxy =
  let threads = ref [] in
  while not (Atomic.get proxy.stopping) do
    match Unix.select [ proxy.listener ] [] [] 0.05 with
    | [], _, _ -> ()
    | _ -> (
        match Unix.accept ~cloexec:true proxy.listener with
        | client, _ -> threads := admit proxy client @ !threads
        | exception Unix.Unix_error _ -> ())
    | exception Unix.Unix_error (Unix.EINTR, _, _) -> ()
  done;
  reset_all proxy;
  List.iter Thread.join !threads

(** Resolves the upstream once so a fault never depends on DNS timing. *)
let resolve host port =
  match
    Unix.getaddrinfo host (string_of_int port) [ Unix.AI_SOCKTYPE Unix.SOCK_STREAM ]
  with
  | { Unix.ai_addr; _ } :: _ -> ai_addr
  | [] -> failwith ("cannot resolve upstream " ^ host)

let start ~name ~upstream_host ~upstream_port =
  (* A write to a peer reset by a fault must be an EPIPE error the pump
     handles, not a process-terminating signal. *)
  Sys.set_signal Sys.sigpipe Sys.Signal_ignore;
  let listener = Unix.socket ~cloexec:true Unix.PF_INET Unix.SOCK_STREAM 0 in
  Unix.setsockopt listener Unix.SO_REUSEADDR true;
  Unix.bind listener (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
  Unix.listen listener 64;
  let port =
    match Unix.getsockname listener with
    | Unix.ADDR_INET (_, port) -> port
    | Unix.ADDR_UNIX _ -> failwith "proxy listener is not an inet socket"
  in
  let proxy =
    {
      name;
      listener;
      port;
      upstream = resolve upstream_host upstream_port;
      mode = Atomic.make Forward;
      stopping = Atomic.make false;
      accepted = Atomic.make 0;
      mutex = Mutex.create ();
      connections = Hashtbl.create 8;
      next_id = 0;
      domain = None;
    }
  in
  proxy.domain <- Some (Domain.spawn (fun () -> serve proxy));
  log "proxy=%s listening port=%d upstream=%s:%d" name port upstream_host
    upstream_port;
  proxy

let url proxy = Printf.sprintf "http://127.0.0.1:%d" proxy.port

let active_connections proxy =
  Mutex.protect proxy.mutex (fun () -> Hashtbl.length proxy.connections)

let accepted_connections proxy = Atomic.get proxy.accepted

let set_mode proxy mode =
  Atomic.set proxy.mode mode;
  if mode = Refuse then reset_all proxy;
  log "proxy=%s mode=%s active=%d" proxy.name (mode_name mode)
    (active_connections proxy)

let restore proxy =
  (* Close first: a socket whose response bytes were dropped has a corrupt
     HTTP/2 state and must not carry a later request. *)
  Atomic.set proxy.mode Refuse;
  reset_all proxy;
  Atomic.set proxy.mode Forward;
  log "proxy=%s mode=forward restored" proxy.name

let stop proxy =
  match proxy.domain with
  | None -> ()
  | Some domain ->
      proxy.domain <- None;
      Atomic.set proxy.stopping true;
      Domain.join domain;
      close_quietly proxy.listener;
      log "proxy=%s stopped accepted=%d" proxy.name (accepted_connections proxy)
