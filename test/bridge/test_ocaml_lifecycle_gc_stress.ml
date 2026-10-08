(** Seeded OCaml-side lifecycle stress under GC pressure (issue #522).

    The Rust operation-sequence stress in
    [rust/core-bridge/tests/lifecycle_stress.rs] drives the C ABI directly.
    This test covers what it cannot: the OCaml custom block that owns a native
    runtime, its GC finalizer (the [runtime_dispose] fallback), and the C stubs
    that read that movable block across blocking calls. Each cycle creates a
    runtime, optionally starts a replay worker, and then either closes it
    explicitly (sometimes twice and then uses it again) or drops it for the
    finalizer, with minor collections, full major collections, and compactions
    interleaved. Every call must return a documented outcome; a released
    runtime must reject further use with [Invalid_argument].

    The sequence is a pure function of [LIFECYCLE_STRESS_SEED] (decimal or
    [0x] hexadecimal) and [LIFECYCLE_STRESS_OCAML_CYCLES]; native scheduling
    is not, so racy calls accept every documented status. This test observes
    OCaml-visible outcomes only: native cleanup counts are asserted by the
    Rust stress, and the C stubs are sanitizer-instrumented only by
    [test/bridge/test_abi.sh]. *)

module Bridge = Temporal_core_bridge.Native_bridge

(** Default number of runtime cycles: well under a second of native work. *)
let default_cycles = 48

(** Default seed shared with the Rust stress so both streams are recorded
    together. *)
let default_seed = 0x0522_2026

(** Reads an optional integer setting in decimal or [0x] hexadecimal form. *)
let env_int name default =
  match Sys.getenv_opt name with
  | None | Some "" -> default
  | Some text -> (
      match int_of_string_opt text with
      | Some value -> value
      | None -> failwith (name ^ " must be a decimal or 0x-hexadecimal integer"))

(** Fails with the operation label and native diagnostic when a call that
    must succeed did not. *)
let expect_ok label = function
  | Ok value -> value
  | Error error -> failwith (label ^ ": " ^ error.Bridge.message)

(** Requires a call on a released runtime to be refused as a null handle. The
    C stub clears the custom block's pointer on close, so Rust receives null
    and must reject it before touching freed memory. *)
let expect_released label = function
  | Error { Bridge.status = Bridge.Invalid_argument; _ } -> ()
  | Ok _ -> failwith (label ^ " succeeded on a released runtime")
  | Error error ->
      failwith (label ^ " on a released runtime returned: " ^ error.message)

(** Accepts the documented outcomes of one bounded replay readiness wait. *)
let expect_wait label = function
  | Ok () | Error { Bridge.status = Bridge.Not_ready; _ } -> ()
  | Error error -> failwith (label ^ ": " ^ error.Bridge.message)

(** Replay worker settings shared with the runtime-lifetime test: replay needs
    no Temporal client. *)
let replay_config () =
  expect_ok "replay worker config"
    (Bridge.worker_config ~namespace:"temporal-sdk-gc-stress"
       ~task_queue:"ocaml-temporal-gc-stress" ~build_id:"gc-stress-build"
       ~max_cached_workflows:0 ~max_outstanding_workflow_tasks:1
       ~max_concurrent_workflow_task_polls:1
       ~graceful_shutdown_timeout_ms:1_000L ())

(** Applies one randomly chosen collection so custom blocks move (promotion,
    compaction) and unreachable runtimes are finalized between calls. *)
let collect state =
  match Random.State.int state 4 with
  | 0 -> Gc.minor ()
  | 1 -> Gc.full_major ()
  | 2 -> Gc.compact ()
  | _ -> ignore (Sys.opaque_identity (Bytes.create 4096))

(** Runs one runtime cycle. Dropped runtimes are released by the finalizer at
    a later collection; explicitly closed ones are closed again and used once
    more to prove idempotent close and use-after-close rejection. *)
let cycle state config =
  let runtime = expect_ok "runtime create" (Bridge.runtime_create ()) in
  collect state;
  let replay = Random.State.bool state in
  if replay then begin
    expect_ok "replay start" (Bridge.replay_worker_start runtime config);
    collect state;
    if Random.State.bool state then
      expect_ok "replay finish" (Bridge.replay_worker_finish_input runtime);
    if Random.State.int state 4 = 0 then
      expect_wait "replay wait" (Bridge.replay_worker_wait_workflow runtime)
  end;
  collect state;
  match Random.State.int state 3 with
  | 0 ->
      (* GC fallback: the only reference dies here, so a later collection
         runs the custom-block finalizer and its non-blocking dispose. *)
      ()
  | 1 ->
      if replay then
        expect_ok "replay dispose" (Bridge.replay_worker_dispose runtime);
      expect_ok "runtime close" (Bridge.runtime_close runtime);
      collect state;
      expect_ok "repeated runtime close" (Bridge.runtime_close runtime)
  | _ ->
      expect_ok "runtime close with children" (Bridge.runtime_close runtime);
      collect state;
      expect_released "replay wait"
        (Bridge.replay_worker_wait_workflow runtime);
      expect_released "replay start" (Bridge.replay_worker_start runtime config);
      expect_ok "repeated runtime close" (Bridge.runtime_close runtime)

(** Creates and shuts down real Rust-backed supervisors with collections in
    between. Each supervisor owns a Domain and the complete native graph, so
    the bounded count keeps Domain usage far below the runtime limit. *)
let supervisor_cycles state count =
  let module Native = Sdk_supervisor.Native in
  for _ = 1 to count do
    let supervisor =
      match Native.create ~capacity:2 () with
      | Ok supervisor -> supervisor
      | Error _ -> failwith "native supervisor creation failed"
    in
    collect state;
    (match Native.perform supervisor Native.Check_compatibility with
    | Ok () -> ()
    | Error _ -> failwith "native supervisor compatibility check failed");
    collect state;
    (match Native.shutdown supervisor with
    | Ok () -> ()
    | Error _ -> failwith "native supervisor shutdown failed");
    collect state;
    match Native.shutdown supervisor with
    | Ok () -> ()
    | Error _ -> failwith "repeated native supervisor shutdown failed"
  done

(** Runs the configured cycles, then forces the finalizers of every dropped
    runtime to run before the process exits. *)
let () =
  let seed = env_int "LIFECYCLE_STRESS_SEED" default_seed in
  let cycles = env_int "LIFECYCLE_STRESS_OCAML_CYCLES" default_cycles in
  if cycles < 1 || cycles > 100_000 then
    failwith "LIFECYCLE_STRESS_OCAML_CYCLES must be 1..100000";
  let state = Random.State.make [| seed |] in
  let config = replay_config () in
  for _ = 1 to cycles do
    cycle state config
  done;
  supervisor_cycles state (max 1 (cycles / 16));
  Gc.full_major ();
  Gc.compact ();
  Gc.full_major ()
