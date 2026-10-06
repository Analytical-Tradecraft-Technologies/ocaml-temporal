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
      its own [shutdown] has returned (a creation that fails is never
      attached);
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

(** Creates a runtime and starts its background threads. [io_threads] is an
    upper bound on the threads it uses for network I/O and server
    communication, shared by every attached client and worker; the SDK may
    use fewer. When omitted it is the host's available parallelism capped at
    4. It must be between 1 and 256; any other value returns a typed defect
    before anything is allocated. A failure to start the threads is a
    [`Bridge] error.

    Creating a runtime needs no Temporal Server. *)
val create : ?io_threads:int -> unit -> (t, Error.t) result

(** Number of clients and workers currently attached. *)
val attached : t -> int

(** Stops the runtime's background threads and waits until they have
    exited. While any client or worker is still attached, returns a
    [`Defect] error naming how many, and changes nothing; shut them down
    and call [shutdown] again. Repeating a successful call returns [Ok ()].
    A native release failure is a [`Bridge] error, and the runtime is
    nevertheless closed. *)
val shutdown : t -> (unit, Error.t) result
