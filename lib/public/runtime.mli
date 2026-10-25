(** A background I/O runtime that several clients and workers can share.

    By default every {!Client.t} and {!Worker.t} owns its own background
    threads for network I/O and server communication (bounded by their
    [?io_threads] argument). An application that runs several of them in one
    process can instead create one [Runtime.t] and pass it as [?runtime] to
    {!Client.create} and {!Worker.create}, so they all share a single pool of
    background threads.

    Ownership is explicit and checked:
    - the application creates the runtime with {!create} and releases it
      with {!shutdown};
    - every client and worker created with [~runtime] is attached to it until
      its native teardown has completed (a creation that fails is never
      attached). For a client that is when {!Client.shutdown} returns. A
      worker whose {!Worker.shutdown} or {!Worker.shutdown_with_report}
      returned while its native teardown was still running in the background
      ([`Detached] in {!Worker.Shutdown_report.teardown}) stays attached until
      that teardown finishes. A client or worker that is dropped without
      being shut down, native or [mock://], is detached only after the
      garbage collector has collected it and its background cleanup has
      finished (#1222), so shut instances down rather than relying on this;
    - {!shutdown} returns a typed defect, and releases nothing, while any
      client or worker is still attached, so shut those down first;
    - after a successful {!shutdown}, creating a client or worker on the
      runtime returns a typed defect.

    Each client and worker still has its own connection, its own worker
    state, and its own private owner; only the background I/O threads are
    shared. Workflow and activity code never runs on these threads.

    All functions may be called from any Domain or system thread. The
    representation is private to the SDK; use values only through this
    module. *)
type t = Temporal_sdk_kernel.Shared_runtime.t

(** Opt-in export of Temporal Core's built-in worker and client metrics.

    Temporal Core instruments every worker and client it runs: task polls
    and their outcomes, task slots in use and available, sticky-cache size
    and hits, schedule-to-start, execution and end-to-end latency, task
    failures, and request counts and latency. These are the same instruments,
    with the same [temporal_]-prefixed names and labels, that the official
    Temporal SDKs export, so existing dashboards and alerts apply. See
    [docs/reference/observability.md] for the list, units, and labels.

    Workers also report a few states that only this SDK sees, under
    [temporal_ocaml_] names: activity-lane occupancy and callback duration,
    admitted and refused activity tasks, asynchronous activity operations
    refused for back-pressure, expired asynchronous leases, completion
    retries, detached lanes, and the outcome of every worker shutdown.

    Labels are bounded: namespace, task queue, workflow and activity type,
    and a few closed enumerations. Workflow IDs, run IDs, activity IDs,
    payloads, and credentials are never metric labels.

    Metrics are configured on a shared {!t}: every client and worker created
    with [~runtime] on it reports to the same endpoint. Clients and workers
    that own their own runtime do not export metrics. *)
module Metrics : sig
  (** One validated exporter configuration. *)
  type t

  (** [prometheus ~bind_address ()] serves metrics for Prometheus to scrape
      at [http://<bind_address>/metrics].

      [bind_address] is an IP socket address such as ["0.0.0.0:9464"],
      ["127.0.0.1:9464"], or ["[::1]:9464"]; host names are rejected so the
      listening interface is explicit. IPv4 must be four plain decimal
      octets (shorthand such as ["127.1"], octal, and hexadecimal forms are
      rejected on every platform) and the port has no leading zeros. Port [0] picks a free port, which
      {!val-metrics_address} then reports. [global_tags] adds constant labels
      (for example [("deployment", "blue")]) to every series: at most 16,
      each name matching [[A-Za-z_][A-Za-z0-9_]*] (at most 64 bytes, no
      [__] prefix, unique, and not a label Temporal Core already uses, such
      as [task_queue] or [namespace]) and each value 1 to 256 bytes without
      control characters. [counters_total_suffix] appends [_total] to
      counter names, [unit_suffix] appends the unit (for example
      [_milliseconds]) to histogram names, and [durations_as_seconds]
      records durations in seconds instead of milliseconds; all default to
      [false], as in the official SDKs. An invalid option returns a typed
      defect. *)
  val prometheus :
    ?global_tags:(string * string) list ->
    ?counters_total_suffix:bool ->
    ?unit_suffix:bool ->
    ?durations_as_seconds:bool ->
    bind_address:string ->
    unit ->
    (t, Error.t) result
end

(** Creates a runtime and starts its background threads. [io_threads] is an
    upper bound on the threads it uses for network I/O and server
    communication, shared by every attached client and worker; the SDK may
    use fewer. When omitted it is the host's available parallelism capped at
    4. It must be between 1 and 256; any other value returns a typed defect
    before anything is allocated. A failure to start the threads is a
    [`Bridge] error.

    With [metrics], the exporter's endpoint is bound before [create]
    returns and serves until {!shutdown} has destroyed the runtime. An
    address that cannot be bound (for example, one already in use) is a
    [`Bridge] error naming the address, and nothing is left running. The
    endpoint is served by the runtime's background threads and never runs
    OCaml code, so a slow or failing scraper cannot block or fail workflow
    and activity execution. A failed [accept] (for example under
    file-descriptor pressure) is logged and retried rather than stopping the
    endpoint; in the unlikely case that the underlying exporter stops,
    scrapes receive [503 Service Unavailable] until the runtime is
    recreated.

    Creating a runtime needs no Temporal Server. *)
val create :
  ?io_threads:int -> ?metrics:Metrics.t -> unit -> (t, Error.t) result

(** The socket address the metrics endpoint is listening on, such as
    ["127.0.0.1:9464"], with a requested port [0] resolved; [None] when the
    runtime was created without [metrics]. The value does not change after
    {!shutdown}, but the endpoint is then closed. *)
val metrics_address : t -> string option

(** Number of clients and workers currently attached. *)
val attached : t -> int

(** Stops the runtime's background threads and waits until they have
    exited. While any client or worker is still attached, returns a
    [`Defect] error naming how many, and changes nothing; shut them down
    and call [shutdown] again. This includes a worker whose shutdown
    returned with a [`Detached] native teardown: poll {!attached} or retry
    until that teardown has finished. Repeating a successful call returns [Ok ()].
    One wait is unbounded (#1149): if an attached worker's Temporal Core
    finalizer was abandoned after its timeout because Core's workflow
    processing thread was still running, the final release of Core here
    waits for that thread to exit.
    A native release failure is a [`Bridge] error, and the runtime is
    nevertheless closed. *)
val shutdown : t -> (unit, Error.t) result
