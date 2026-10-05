(** Default Temporal client and worker identities.

    Temporal records the identity of every client request and worker poll in
    history events and task-queue poller listings. Following the official
    Temporal SDKs, the default identity is [<pid>@<hostname>] so concurrent
    processes are distinguishable without configuration.

    These functions read process-global state (the process ID and host name)
    and therefore must only run while constructing a client or worker, never
    from workflow code. *)

(** Upper bound, in bytes, on the host-name component of a derived identity.
    It matches the POSIX [HOST_NAME_MAX] order of magnitude and keeps the
    complete identity far below the 65,536-byte bridge string limit. *)
val max_hostname_bytes : int

(** Host-name component used when the operating system reports no usable host
    name, either because the lookup failed or nothing survived sanitization. *)
val fallback_hostname : string

(** [of_parts ~pid ~hostname] formats [<pid>@<hostname>] after sanitizing
    [hostname]: every byte outside printable ASCII ([0x21]..[0x7e]) is replaced
    by ['_'], the result is truncated to {!max_hostname_bytes}, and an empty
    host name becomes {!fallback_hostname}. The result is therefore always
    non-empty, NUL-free, valid UTF-8, and bounded, satisfying the OCaml and
    Rust identity validation. *)
val of_parts : pid:int -> hostname:string -> string

(** Computes the default identity for the current process from
    [Unix.getpid] and [Unix.gethostname]. A failing host-name lookup falls back
    to {!fallback_hostname} instead of raising. *)
val default : unit -> string

(** [resolve identity] returns an explicitly supplied identity unchanged (it is
    validated by the caller like any other user input) and computes
    {!default} only when [identity] is [None]. *)
val resolve : string option -> string
