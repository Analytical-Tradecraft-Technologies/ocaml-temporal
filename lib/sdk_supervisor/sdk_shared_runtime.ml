module Bridge = Temporal_core_bridge.Native_bridge

(** [mutex] protects [attached] and [closed]. It is held across the native
    close in [shutdown] so that every [Ok] return, including a concurrent
    second caller's, happens after Core has been destroyed. Holding it there
    cannot deadlock: no lease is outstanding, so no [release] waits on it,
    and the C close never calls back into OCaml. [native] is immutable. *)
type t = {
  native : Bridge.shared_runtime;
  mutex : Mutex.t;
  mutable attached : int;
  mutable closed : bool;
}

(** [released] makes [release] idempotent without taking [owner.mutex] for
    the repeated case. *)
type lease = { owner : t; released : bool Atomic.t }

type shutdown_error =
  | Still_attached of int
  | Native of Bridge.error

(** Runs [f] with [mutex] held and always releases it. *)
let with_lock mutex f =
  Mutex.lock mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock mutex) f

let create ?worker_threads () =
  Result.map
    (fun native -> { native; mutex = Mutex.create (); attached = 0; closed = false })
    (Bridge.shared_runtime_create ?worker_threads ())

let acquire runtime =
  with_lock runtime.mutex (fun () ->
      if runtime.closed then None
      else (
        runtime.attached <- runtime.attached + 1;
        Some { owner = runtime; released = Atomic.make false }))

(** The lease guarantees [closed = false] until it is released, so [native]
    is live for the whole call; the C gate is only a defensive backstop. *)
let attach lease = Bridge.runtime_attach lease.owner.native

let release lease =
  if not (Atomic.exchange lease.released true) then
    with_lock lease.owner.mutex (fun () ->
        lease.owner.attached <- lease.owner.attached - 1)

let attached runtime = with_lock runtime.mutex (fun () -> runtime.attached)
let is_shut_down runtime = with_lock runtime.mutex (fun () -> runtime.closed)

let shutdown runtime =
  with_lock runtime.mutex (fun () ->
      if runtime.closed then Ok ()
      else if runtime.attached > 0 then Error (Still_attached runtime.attached)
      else (
        runtime.closed <- true;
        match Bridge.shared_runtime_close runtime.native with
        | Ok () -> Ok ()
        | Error error -> Error (Native error)))
