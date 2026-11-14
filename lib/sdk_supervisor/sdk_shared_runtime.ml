module Bridge = Temporal_core_bridge.Native_bridge

(** [mutex] protects [attached], [detached] and [closed]. [detached] counts
    attachments returned by [release_detached]: graphs whose bounded native
    close gave up waiting while their cleanup thread still held a Core
    reference (#1149), so destroying the shared handle cannot prove that Core
    is gone. It is held across the native
    close in [shutdown] so that every [Ok] return, including a concurrent
    second caller's, happens after Core has been destroyed. Holding it there
    cannot deadlock: no lease is outstanding, so no [release] waits on it,
    and the C close never calls back into OCaml. [native] and
    [metrics_address] are immutable; the latter is the bound address of the
    opt-in Prometheus endpoint (#500), recorded once at creation, and [None]
    for an OTLP exporter (#1161), which listens on nothing. [meter] is the
    SDK-side recorder over the runtime's exporter, present exactly when the
    runtime has one (Prometheus or OTLP); it is released only by the GC, so
    it stays safe to record into after [shutdown]. *)
type t = {
  native : Bridge.shared_runtime;
  metrics_address : string option;
  meter : Bridge.metric_meter option;
  mutex : Mutex.t;
  mutable attached : int;
  mutable detached : int;
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

(** Builds the owner record around a freshly created native handle. *)
let make native metrics_address meter =
  {
    native;
    metrics_address;
    meter;
    mutex = Mutex.create ();
    attached = 0;
    detached = 0;
    closed = false;
  }

let create ?worker_threads ?telemetry () =
  match telemetry with
  | None ->
      Result.map
        (fun native -> make native None None)
        (Bridge.shared_runtime_create ?worker_threads ())
  | Some telemetry -> (
      match
        Bridge.shared_runtime_create_with_telemetry ?worker_threads ~telemetry
          ()
      with
      | Error _ as error -> error
      | Ok (native, address) -> (
          (* Only a Prometheus endpoint reports an address; an OTLP exporter
             (#1161) and an empty document report [""]. The recorder is
             created here, once, so every worker attached to this runtime
             shares one registry of SDK series; the bridge returns [None]
             when the document configured no exporter. A failure here is a
             bridge defect; the fresh runtime is released before it is
             reported so nothing keeps running. *)
          let metrics_address = if address = "" then None else Some address in
          match Bridge.shared_runtime_metric_meter native with
          | Ok meter -> Ok (make native metrics_address meter)
          | Error error ->
              ignore (Bridge.shared_runtime_close native);
              Error error))

let metrics_address runtime = runtime.metrics_address
let metric_meter runtime = runtime.meter

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

let release_detached lease =
  if not (Atomic.exchange lease.released true) then
    with_lock lease.owner.mutex (fun () ->
        lease.owner.attached <- lease.owner.attached - 1;
        lease.owner.detached <- lease.owner.detached + 1)

let attached runtime = with_lock runtime.mutex (fun () -> runtime.attached)
let is_shut_down runtime = with_lock runtime.mutex (fun () -> runtime.closed)

let shutdown runtime =
  with_lock runtime.mutex (fun () ->
      if runtime.closed then Ok ()
      else if runtime.attached > 0 then Error (Still_attached runtime.attached)
      else (
        runtime.closed <- true;
        match Bridge.shared_runtime_close runtime.native with
        | Error error -> Error (Native error)
        | Ok () when runtime.detached = 0 -> Ok ()
        | Ok () ->
            (* The shared handle's reference is gone, but a detached graph's
               cleanup thread may still hold the last one, so Core may still
               be running: report that instead of a destruction that cannot
               be proven. *)
            Error
              (Native
                 {
                   Bridge.status = Bridge.Detached;
                   message =
                     Printf.sprintf
                       "%d attached graph(s) were still finishing their native                         cleanup in the background when they released this                         runtime, so Temporal Core may outlive this shutdown;                         the bridge's cleanup threads finish its release"
                       runtime.detached;
                 })))
