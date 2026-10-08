(** Deterministic TCP fault injector for the transport interruption live
    regression (#504).

    A proxy listens on an ephemeral loopback port and forwards each accepted
    connection to one upstream Temporal frontend. The test points a client or
    worker at {!url} instead of the server, then switches the proxy's {!mode}
    at exact points in the scenario. Faults therefore happen at a known
    request boundary, not after a container restart whose timing depends on
    the host.

    The proxy works below HTTP/2: it never parses frames, so it cannot forge
    a server answer. A dropped response is discarded byte for byte, which
    corrupts the HTTP/2 connection state of that socket; {!restore} therefore
    always closes every proxied connection so the SDK reconnects from a clean
    state.

    Threading: the proxy owns one dedicated Domain. Its accept loop and the
    two pump threads per connection run there, so a test Domain blocked in a
    native client call never starves the proxy. The control functions only
    write atomics and shut down sockets and may be called from any Domain. *)

(** What the proxy does with traffic. *)
type mode =
  | Forward
      (** Copy bytes unchanged in both directions. *)
  | Refuse
      (** Close every existing connection and close each new connection as
          soon as it is accepted, without contacting the upstream. The SDK
          observes a server that is down. *)
  | Drop_responses
      (** Forward client bytes to the server but discard every byte the
          server sends back. A request reaches Temporal and may be applied,
          while its acknowledgement is lost: the lost-acknowledgement case
          whose outcome the SDK must report as uncertain. *)

(** One running proxy. *)
type t

(** [start ~name ~upstream_host ~upstream_port] binds 127.0.0.1 on an
    ephemeral port and starts forwarding in {!Forward} mode. [name] labels
    the proxy in fault log lines. Raises [Unix.Unix_error] when the socket
    cannot be bound; that is a fixture defect, not an SDK outcome. *)
val start : name:string -> upstream_host:string -> upstream_port:int -> t

(** The [http://127.0.0.1:<port>] target URL that routes through the proxy. *)
val url : t -> string

(** Switches mode. Entering [Refuse] also closes every live connection.
    Every switch prints one fault log line with the elapsed time since the
    process started, so a failure report correlates the injection with the
    SDK operation that observed it. *)
val set_mode : t -> mode -> unit

(** Returns to [Forward] after closing every live connection, so no socket
    whose response bytes were discarded is reused. *)
val restore : t -> unit

(** Number of proxied connections whose sockets are still open. *)
val active_connections : t -> int

(** Total connections accepted since [start], including refused ones. *)
val accepted_connections : t -> int

(** Stops accepting, closes every connection, and joins the proxy Domain.
    Idempotent. *)
val stop : t -> unit

(** Prints one timestamped fixture log line ([transport-fault +<seconds>s
    ...]) shared by the proxy and the regression driver. *)
val log : ('a, unit, string, unit) format4 -> 'a
