(** Regression for #832: several clients and workers can share one Core
    runtime, with explicit, checked ownership.

    The cases run bottom-up without a Temporal Server: the C/Rust bridge
    (graphs attached to one shared Core outlive the shared handle), the
    private attachment ledger, two real native supervisors on one runtime,
    and the public [Temporal.Runtime] contract with [mock://] and refused
    [http://] targets. A final case repeats the full lifecycle and checks
    that the process thread count returns to its baseline where the platform
    exposes it. *)

module Bridge = Temporal_core_bridge.Native_bridge
module Shared = Sdk_shared_runtime
module Native = Sdk_supervisor.Native

(** Fails the test with [label] unless a bridge [result] is [Ok]. *)
let bridge_ok label = function
  | Ok value -> value
  | Error (error : Bridge.error) ->
      failwith (Printf.sprintf "%s failed: %s" label error.message)

(** Fails the test with [label] unless a public [result] is [Ok]. *)
let public_ok label = function
  | Ok value -> value
  | Error error ->
      failwith
        (Printf.sprintf "%s failed: %s" label (Temporal.Error.message error))

(** Asserts a public [Error] in [category] whose message contains [needle]. *)
let expect_public_error label category needle = function
  | Ok _ -> failwith (label ^ ": unexpectedly succeeded")
  | Error error ->
      let view = Temporal.Error.view error in
      if view.category <> category then
        failwith (Printf.sprintf "%s: wrong category: %s" label view.message);
      let contains =
        let n = String.length needle and m = String.length view.message in
        let rec scan i =
          i + n <= m && (String.sub view.message i n = needle || scan (i + 1))
        in
        scan 0
      in
      if not contains then
        failwith (Printf.sprintf "%s: unexpected message: %s" label view.message)

(** Asserts that [runtime] reports exactly [expected] attachments. *)
let expect_attached label expected runtime =
  let actual = Temporal.Runtime.attached runtime in
  if actual <> expected then
    failwith (Printf.sprintf "%s: %d attached, expected %d" label actual expected)

(** Unreachable endpoint: loopback port 1 is closed on every supported
    platform, so connecting is refused without a Temporal Server. *)
let refused_target = "http://127.0.0.1:1"

(** Graphs attached to a shared Core keep it alive after the shared handle is
    closed, can still drive Core's executor (a refused connect is a
    [Connection] error, not a closed-runtime error), and close normally. A
    closed handle rejects new attaches instead of being reused. *)
let test_bridge_sharing () =
  List.iter
    (fun count ->
      match Bridge.shared_runtime_create ~worker_threads:count () with
      | Error { Bridge.status = Bridge.Invalid_argument; _ } -> ()
      | _ -> failwith "invalid shared runtime thread count was accepted")
    [ 0; -1; Bridge.max_runtime_worker_threads + 1 ];
  let shared =
    bridge_ok "shared create" (Bridge.shared_runtime_create ~worker_threads:1 ())
  in
  let first = bridge_ok "first attach" (Bridge.runtime_attach shared) in
  let second = bridge_ok "second attach" (Bridge.runtime_attach shared) in
  bridge_ok "shared close" (Bridge.shared_runtime_close shared);
  bridge_ok "repeated shared close" (Bridge.shared_runtime_close shared);
  let config =
    bridge_ok "client config"
      (Bridge.client_config ~target_url:refused_target ~identity:"unit-test" ())
  in
  List.iter
    (fun runtime ->
      match Bridge.client_connect runtime config with
      | Error { Bridge.status = Bridge.Connection; _ } -> ()
      | Error { Bridge.message; _ } ->
          failwith ("attached graph lost its Core: " ^ message)
      | Ok () -> failwith "refused endpoint accepted a connection")
    [ first; second ];
  (match Bridge.runtime_attach shared with
  | Error { Bridge.status = Bridge.Invalid_argument; _ } -> ()
  | _ -> failwith "closed shared runtime accepted an attach");
  bridge_ok "first close" (Bridge.runtime_close first);
  bridge_ok "second close" (Bridge.runtime_close second)

(** The private ledger refuses to close while leases are outstanding, makes
    release idempotent, and stops issuing leases once closed. *)
let test_ledger_ordering () =
  let runtime = bridge_ok "ledger create" (Shared.create ~worker_threads:1 ()) in
  let lease = Option.get (Shared.acquire runtime) in
  (match Shared.shutdown runtime with
  | Error (Shared.Still_attached 1) -> ()
  | _ -> failwith "ledger closed with an outstanding lease");
  assert (not (Shared.is_shut_down runtime));
  Shared.release lease;
  Shared.release lease;
  assert (Shared.attached runtime = 0);
  (match Shared.shutdown runtime with
  | Ok () -> ()
  | Error _ -> failwith "ledger shutdown failed");
  assert (Shared.is_shut_down runtime);
  assert (Shared.acquire runtime = None);
  match Shared.shutdown runtime with
  | Ok () -> ()
  | Error _ -> failwith "repeated ledger shutdown failed"

(** Two real native supervisors, each with its own owner Domain and graph,
    run on one shared Core. Each supervisor releases its lease only when its
    own shutdown has closed the graph. *)
let test_two_supervisors_share_one_runtime () =
  let runtime = bridge_ok "runtime create" (Shared.create ~worker_threads:2 ()) in
  let create () =
    match Native.create ?runtime:(Shared.acquire runtime) ~capacity:2 () with
    | Ok supervisor -> supervisor
    | Error _ -> failwith "attached supervisor creation failed"
  in
  let first = create () in
  let second = create () in
  List.iter
    (fun supervisor ->
      match Native.perform supervisor Native.Check_compatibility with
      | Ok () -> ()
      | Error _ -> failwith "attached supervisor is unusable")
    [ first; second ];
  (match Shared.shutdown runtime with
  | Error (Shared.Still_attached 2) -> ()
  | _ -> failwith "runtime closed under two supervisors");
  (match Native.shutdown first with
  | Ok () -> ()
  | Error _ -> failwith "first supervisor shutdown failed");
  assert (Shared.attached runtime = 1);
  (match Native.shutdown second with
  | Ok () -> ()
  | Error _ -> failwith "second supervisor shutdown failed");
  (match Native.shutdown second with
  | Ok () -> ()
  | Error _ -> failwith "repeated supervisor shutdown failed");
  assert (Shared.attached runtime = 0);
  match Shared.shutdown runtime with
  | Ok () -> ()
  | Error _ -> failwith "runtime shutdown after supervisors failed"

(** A trivial workflow so a worker registration list is non-empty. *)
let workflow =
  Temporal.Workflow.define ~name:"unit.shared-runtime"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())

(** Creates a worker on [target_url], optionally attached to [runtime]. *)
let create_worker ?runtime ?io_threads target_url =
  Temporal.Worker.create ?runtime ?io_threads ~target_url
    ~namespace:"unit-test" ~task_queue:"unit-test"
    ~workflows:[ Temporal.Worker.workflow workflow ]
    ~activities:[] ()

(** The public contract: a client and a worker attach to one runtime, the
    runtime refuses to shut down until both are shut down, a creation that
    fails never stays attached, and a shut-down runtime is rejected. *)
let test_public_ordering () =
  (match Temporal.Runtime.create ~io_threads:0 () with
  | Error _ -> ()
  | Ok _ -> failwith "invalid io_threads accepted by Runtime.create");
  let runtime = public_ok "runtime create" (Temporal.Runtime.create ~io_threads:2 ()) in
  expect_attached "fresh runtime" 0 runtime;
  expect_public_error "both io sources" `Defect "mutually exclusive"
    (Temporal.Client.create ~runtime ~io_threads:2 ~target_url:"mock://shared"
       ~namespace:"unit-test" ());
  expect_public_error "worker with both io sources" `Defect "mutually exclusive"
    (create_worker ~runtime ~io_threads:2 "mock://shared");
  expect_attached "rejected sources" 0 runtime;
  let client =
    public_ok "mock client"
      (Temporal.Client.create ~runtime ~target_url:"mock://shared"
         ~namespace:"unit-test" ())
  in
  let worker = public_ok "mock worker" (create_worker ~runtime "mock://shared") in
  expect_attached "client and worker" 2 runtime;
  expect_public_error "early shutdown" `Defect "2 client(s) or worker(s)"
    (Temporal.Runtime.shutdown runtime);
  (* Native creations that fail (refused connection) must not stay attached,
     whether the failure happens in the client or the worker path. *)
  expect_public_error "refused native client" `Bridge "refused"
    (Temporal.Client.create ~runtime ~target_url:refused_target
       ~namespace:"unit-test" ());
  (match create_worker ~runtime refused_target with
  | Error _ -> ()
  | Ok _ -> failwith "worker connected to a refused endpoint");
  expect_attached "after refused creations" 2 runtime;
  public_ok "client shutdown" (Temporal.Client.shutdown client);
  public_ok "repeated client shutdown" (Temporal.Client.shutdown client);
  expect_attached "after client shutdown" 1 runtime;
  expect_public_error "shutdown with worker" `Defect "1 client(s) or worker(s)"
    (Temporal.Runtime.shutdown runtime);
  public_ok "worker shutdown" (Temporal.Worker.shutdown worker);
  public_ok "repeated worker shutdown" (Temporal.Worker.shutdown worker);
  expect_attached "after worker shutdown" 0 runtime;
  public_ok "runtime shutdown" (Temporal.Runtime.shutdown runtime);
  public_ok "repeated runtime shutdown" (Temporal.Runtime.shutdown runtime);
  expect_public_error "client on closed runtime" `Defect "already been shut down"
    (Temporal.Client.create ~runtime ~target_url:"mock://shared"
       ~namespace:"unit-test" ());
  expect_public_error "worker on closed runtime" `Defect "already been shut down"
    (create_worker ~runtime "mock://shared");
  expect_attached "closed runtime" 0 runtime

(** Creates a mock client and a mock worker attached to [runtime] and drops
    both without shutting them down. Kept out of line so neither value
    survives in a caller's frame or register. *)
let[@inline never] abandon_mock_instances runtime =
  ignore
    (Sys.opaque_identity
       (public_ok "abandoned client"
          (Temporal.Client.create ~runtime ~target_url:"mock://abandoned"
             ~namespace:"unit-test" ())));
  ignore
    (Sys.opaque_identity
       (public_ok "abandoned worker" (create_worker ~runtime "mock://abandoned")))

(** Collects until [runtime] reports [expected] attachments, for at most five
    seconds: the finalizer only starts the release thread. *)
let await_attached label expected runtime =
  let deadline = Unix.gettimeofday () +. 5.0 in
  let rec wait () =
    Gc.full_major ();
    if Temporal.Runtime.attached runtime = expected then ()
    else if Unix.gettimeofday () > deadline then
      expect_attached label expected runtime
    else (
      Thread.delay 0.01;
      wait ())
  in
  wait ()

(** A mock client or worker dropped without shutdown releases its runtime
    lease once it is collected, so [Runtime.shutdown] can succeed (#1222).
    Before the fix the runtime stayed attached forever. An instance that was
    shut down and then collected is not released a second time: a live
    sibling keeps the count at one. *)
let test_abandoned_mock_instances_release () =
  let runtime = public_ok "abandon runtime" (Temporal.Runtime.create ~io_threads:1 ()) in
  abandon_mock_instances runtime;
  await_attached "after collecting abandoned mocks" 0 runtime;
  let live =
    public_ok "live client"
      (Temporal.Client.create ~runtime ~target_url:"mock://abandoned"
         ~namespace:"unit-test" ())
  in
  let closed =
    public_ok "closed client"
      (Temporal.Client.create ~runtime ~target_url:"mock://abandoned"
         ~namespace:"unit-test" ())
  in
  public_ok "closed client shutdown" (Temporal.Client.shutdown closed);
  ignore (Sys.opaque_identity closed);
  for _ = 1 to 5 do Gc.full_major () done;
  Thread.delay 0.05;
  expect_attached "shut down then collected" 1 runtime;
  public_ok "live client shutdown" (Temporal.Client.shutdown live);
  public_ok "abandon runtime shutdown" (Temporal.Runtime.shutdown runtime)

(** Number of OS threads in this process, where the platform exposes it
    cheaply ([/proc/self/task] on Linux); [None] elsewhere. *)
let thread_count () =
  match Sys.readdir "/proc/self/task" with
  | entries -> Some (Array.length entries)
  | exception Sys_error _ -> None

(** One full shared lifecycle: runtime, a mock client and worker, a refused
    native client, then ordered shutdown. *)
let lifecycle_round () =
  let runtime = public_ok "round runtime" (Temporal.Runtime.create ~io_threads:1 ()) in
  let client =
    public_ok "round client"
      (Temporal.Client.create ~runtime ~target_url:"mock://shared-round"
         ~namespace:"unit-test" ())
  in
  let worker = public_ok "round worker" (create_worker ~runtime "mock://shared-round") in
  ignore
    (Temporal.Client.create ~runtime ~target_url:refused_target
       ~namespace:"unit-test" ());
  public_ok "round client shutdown" (Temporal.Client.shutdown client);
  public_ok "round worker shutdown" (Temporal.Worker.shutdown worker);
  public_ok "round runtime shutdown" (Temporal.Runtime.shutdown runtime)

(** Repeated lifecycles return every native thread (Tokio workers, cleanup
    threads, supervisor Domains) to the baseline. Exiting threads may linger
    briefly after their join, so the check waits a bounded time. *)
let test_no_thread_leak () =
  lifecycle_round ();
  match thread_count () with
  | None -> for _ = 1 to 5 do lifecycle_round () done
  | Some baseline ->
      for _ = 1 to 10 do lifecycle_round () done;
      let deadline = Unix.gettimeofday () +. 5.0 in
      let rec settle () =
        match thread_count () with
        | Some count when count <= baseline -> ()
        | Some count when Unix.gettimeofday () > deadline ->
            failwith
              (Printf.sprintf "thread count grew from %d to %d" baseline count)
        | _ ->
            Unix.sleepf 0.05;
            settle ()
      in
      settle ()

let () =
  test_bridge_sharing ();
  test_ledger_ordering ();
  test_two_supervisors_share_one_runtime ();
  test_public_ordering ();
  test_abandoned_mock_instances_release ();
  test_no_thread_leak ()
