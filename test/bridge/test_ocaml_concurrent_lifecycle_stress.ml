(** Seeded multi-Domain lifecycle stress through the C stubs (issue #506).

    The single-Domain GC stress ([test_ocaml_lifecycle_gc_stress.ml]) moves
    and finalizes runtime custom blocks, but every call comes from one Domain.
    This test runs several Domains at once and exercises the races the C
    stubs' borrow gates exist to make safe:

    - a runtime closed by another Domain while its owner Domain is inside, or
      about to enter, a blocking native call;
    - the same runtime closed by two Domains at once;
    - runtimes dropped for the custom-block finalizer while other Domains run
      minor and major collections and compactions;
    - graphs attached concurrently to one shared runtime while the main
      Domain closes that shared runtime.

    The ownership rule is preserved: only the owner Domain makes ordinary
    calls on a runtime; another Domain may only close it, which is the
    defensive path the borrow gate admits. Every call must return [Ok] or a
    documented typed error and never [Panic], [Internal], or an unknown
    status. A runtime known to be closed must reject further use with
    [Invalid_argument].

    The seed ([LIFECYCLE_STRESS_SEED]) fixes each Domain's choices, but the
    interleaving between Domains is not reproducible. [CONCURRENT_STRESS_OCAML_ROUNDS]
    (1–10,000, default 6) and [CONCURRENT_STRESS_OCAML_DOMAINS] (2–32,
    default 4) set the budget; see [docs/reference/bridge-lifecycle-stress.md].

    A crash inside the C stubs or the OCaml runtime kills the process
    before it can report anything (#1214). Every Domain therefore writes the
    seed, round, step and the operation it is about to perform to its own
    trace file under [concurrent-stress-trace/] in the working directory
    (Dune runs the test in [_build/default/test/bridge]), flushing each line
    to the kernel before the operation starts. A [SIGSEGV] leaves the last
    line of each file naming the phase each Domain was in. Files are
    truncated at the start of every round, so they hold only the current
    round and stay small at any budget. *)

module Bridge = Temporal_core_bridge.Native_bridge

(** Default rounds; each round runs every Domain to completion. *)
let default_rounds = 6

(** Default worker Domains per round, in addition to the main Domain. *)
let default_domains = 4

(** Runtimes each worker Domain owns per round. *)
let runtimes_per_domain = 3

(** Operations each worker Domain performs per round. *)
let operations_per_round = 24

(** Default seed shared with the other lifecycle stresses. *)
let default_seed = 0x0522_2026L

(** Reads an integer setting within [low, high]. *)
let env_int name default low high =
  match Sys.getenv_opt name with
  | None | Some "" -> default
  | Some text -> (
      match int_of_string_opt text with
      | Some value when value >= low && value <= high -> value
      | _ -> failwith (Printf.sprintf "%s must be an integer in %d..%d" name low high))

(** Reads the seed as a decimal or [0x] hexadecimal 64-bit integer. Values
    above [Int64.max_int] are accepted in their two's-complement form. *)
let env_seed () =
  match Sys.getenv_opt "LIFECYCLE_STRESS_SEED" with
  | None | Some "" -> default_seed
  | Some text -> (
      let unsigned =
        if String.length text > 2 && String.sub text 0 2 = "0x" then text else "0u" ^ text
      in
      match Int64.of_string_opt unsigned with
      | Some seed -> seed
      | None -> failwith "LIFECYCLE_STRESS_SEED must be an unsigned 64-bit integer")

(** Seeds one Domain's generator from the base seed, round, and Domain
    index, keeping all 64 seed bits. *)
let rng seed round domain =
  Random.State.make
    [|
      Int64.to_int (Int64.logand seed 0xFFFF_FFFFL);
      Int64.to_int (Int64.shift_right_logical seed 32);
      round;
      domain;
    |]

(** Directory, relative to the working directory, that holds one trace file
    per Domain (#1214). *)
let trace_directory = "concurrent-stress-trace"

(** Creates [trace_directory] if needed. Called on the main Domain before any
    worker Domain starts, so the Domains never race to create it. *)
let prepare_trace_directory () =
  try Sys.mkdir trace_directory 0o755 with Sys_error _ when Sys.is_directory trace_directory -> ()

(** A per-Domain crash trace. Only the Domain that opened it writes to it. *)
type trace = { channel : out_channel; prefix : string }

(** Opens [name] in [trace_directory], truncating the previous round's
    lines, and records which seed and round the file describes. [prefix]
    starts every later line, so a line identifies its Domain on its own. *)
let open_trace ~seed ~round name =
  let path = Filename.concat trace_directory name in
  let channel = open_out_gen [ Open_wronly; Open_creat; Open_trunc; Open_text ] 0o644 path in
  let prefix = Printf.sprintf "seed=0x%Lx round=%d %s" seed round name in
  Printf.fprintf channel "%s begin\n%!" prefix;
  { channel; prefix }

(** Records [phase] before it runs. The flush hands the line to the kernel,
    so it survives a crash of this process; it is not an fsync, which a
    process crash does not need. *)
let trace_phase trace phase = Printf.fprintf trace.channel "%s %s\n%!" trace.prefix phase

(** Records the end of the round and closes the file. *)
let close_trace trace =
  Printf.fprintf trace.channel "%s end\n%!" trace.prefix;
  close_out trace.channel

(** Statuses that indicate a bridge defect wherever they appear. *)
let forbidden = function
  | Bridge.Panic | Bridge.Internal | Bridge.Abi_mismatch | Bridge.Unknown _ -> true
  | _ -> false

(** Fails unless [result] is [Ok] or an error whose status [allowed] accepts.
    Returns the error status, if any, so callers can update their model. *)
let check label allowed = function
  | Ok _ -> None
  | Error { Bridge.status; message } ->
      if forbidden status || not (allowed status) then
        failwith (Printf.sprintf "%s returned an undocumented error: %s" label message);
      Some status

(** A released runtime refuses every call with [Invalid_argument]. *)
let released = function Bridge.Invalid_argument -> true | _ -> false

(** Outcomes of a replay call on a runtime that may have been closed by
    another Domain, or whose replay worker may be absent. *)
let replay_call = function
  | Bridge.Invalid_argument | Bridge.Invalid_state | Bridge.Not_ready -> true
  | _ -> false

(** Outcomes of replay disposal, which retains a worker Core cannot finish. *)
let replay_dispose = function
  | Bridge.Invalid_argument | Bridge.Invalid_state | Bridge.Outstanding_tasks | Bridge.Worker ->
      true
  | _ -> false

(** Outcomes of attaching to a shared runtime that the main Domain may be
    closing concurrently. *)
let attach_race = function Bridge.Invalid_argument | Bridge.Invalid_state -> true | _ -> false

(** Replay worker settings; replay needs no Temporal client. *)
let replay_config =
  match
    Bridge.worker_config ~namespace:"concurrent-stress" ~task_queue:"concurrent-stress"
      ~build_id:"concurrent-stress" ~max_cached_workflows:0 ~max_outstanding_workflow_tasks:1
      ~max_concurrent_workflow_task_polls:1 ~graceful_shutdown_timeout_ms:1_000L ()
  with
  | Ok config -> config
  | Error error -> failwith ("replay worker config: " ^ error.message)

(** Applies one seeded collection so custom blocks move and dropped runtimes
    are finalized while other Domains are inside native calls. The chosen
    collection is traced first, because a finalizer may run inside it. *)
let collect trace state =
  match Random.State.int state 6 with
  | 0 ->
      trace_phase trace "gc minor";
      Gc.minor ()
  | 1 ->
      trace_phase trace "gc full_major";
      Gc.full_major ()
  | 2 ->
      trace_phase trace "gc compact";
      Gc.compact ()
  | _ -> ignore (Sys.opaque_identity (Bytes.create 4096))

(** One runtime owned by a worker Domain. [closed] is set once this Domain
    knows the runtime is released (by itself or by its neighbour). *)
type owned = { mutable runtime : Bridge.runtime option; mutable closed : bool }

(** Per-round shared state: each Domain publishes one runtime that its
    neighbour may close, and the counters record what happened. *)
type round = {
  published : Bridge.runtime option Atomic.t array;
  cross_closes : int Atomic.t;
  attach_refusals : int Atomic.t;
  finalizer_drops : int Atomic.t;
}

(** Runs one ordinary operation on a runtime this Domain owns, tracing the
    operation and the runtime's slot ([slot]) before the native call. *)
let operate trace state label slot owned =
  match owned.runtime with
  | None -> ()
  | Some runtime -> (
      let refused status = owned.closed && not (released status) in
      let observe operation allowed call =
        let label = label ^ " " ^ operation in
        trace_phase trace (Printf.sprintf "%s runtime=%d closed=%b" label slot owned.closed);
        match check label allowed (call ()) with
        | Some status when refused status ->
            failwith (label ^ " on a released runtime was not refused as Invalid_argument")
        | Some Bridge.Invalid_argument -> owned.closed <- true
        | _ -> ()
      in
      match Random.State.int state 6 with
      | 0 ->
          observe "start" replay_call (fun () -> Bridge.replay_worker_start runtime replay_config)
      | 1 -> observe "finish" replay_call (fun () -> Bridge.replay_worker_finish_input runtime)
      | 2 -> observe "wait" replay_call (fun () -> Bridge.replay_worker_wait_workflow runtime)
      | 3 -> observe "poll" replay_call (fun () -> Bridge.replay_worker_try_poll_workflow runtime)
      | 4 -> observe "dispose" replay_dispose (fun () -> Bridge.replay_worker_dispose runtime)
      | _ ->
          trace_phase trace (Printf.sprintf "%s close runtime=%d" label slot);
          ignore (check (label ^ " close") (fun _ -> false) (Bridge.runtime_close runtime));
          owned.closed <- true)

(** Body of one worker Domain for one round. It owns and closes its own
    trace file, [domain-<index>.trace]. *)
let worker seed round_number index round shared domains =
  let trace = open_trace ~seed ~round:round_number (Printf.sprintf "domain-%d.trace" index) in
  let state = rng seed round_number index in
  (* One graph in three builds its own Core; the rest attach to the shared
     runtime, racing the main Domain's close of it. *)
  let attach_or_create () =
    if Random.State.int state 3 = 0 then begin
      trace_phase trace "runtime_create";
      match Bridge.runtime_create () with
      | Ok runtime -> Some runtime
      | Error error -> failwith ("runtime create: " ^ error.message)
    end
    else
      let () = trace_phase trace "runtime_attach shared" in
      let result = Bridge.runtime_attach shared in
      match check "attach" attach_race result with
      | None -> Result.to_option result
      | Some _ ->
          Atomic.incr round.attach_refusals;
          None
  in
  let runtimes =
    Array.init runtimes_per_domain (fun _ -> { runtime = attach_or_create (); closed = false })
  in
  (* Publish the first runtime so the neighbouring Domain may close it. *)
  Atomic.set round.published.(index) runtimes.(0).runtime;
  let neighbour = (index + 1) mod domains in
  for step = 1 to operations_per_round do
    let slot = Random.State.int state runtimes_per_domain in
    let owned = runtimes.(slot) in
    operate trace state
      (Printf.sprintf "round %d domain %d step %d" round_number index step)
      slot owned;
    (match Random.State.int state 8 with
    | 0 -> (
        (* Close the neighbour's published runtime from this Domain while
           its owner may be inside a blocking call on it. *)
        match Atomic.get round.published.(neighbour) with
        | Some runtime ->
            trace_phase trace (Printf.sprintf "step %d cross-domain close of domain %d" step neighbour);
            ignore (check "cross-domain close" (fun _ -> false) (Bridge.runtime_close runtime));
            Atomic.incr round.cross_closes
        | None -> ())
    | 1 -> collect trace state
    | _ -> ());
    collect trace state
  done;
  (* The published runtime may have been closed by the neighbour without
     this Domain observing it yet; the next call must still be refused or
     succeed, never touch freed memory. *)
  Array.iteri
    (fun position owned ->
      match owned.runtime with
      | None -> ()
      | Some runtime ->
          if position > 0 && Random.State.bool state then begin
            (* Drop for the custom-block finalizer; a later collection on any
               Domain releases it with the non-blocking dispose. *)
            trace_phase trace (Printf.sprintf "finalizer drop runtime=%d" position);
            owned.runtime <- None;
            Atomic.incr round.finalizer_drops
          end
          else begin
            trace_phase trace (Printf.sprintf "final close runtime=%d" position);
            ignore (check "final close" (fun _ -> false) (Bridge.runtime_close runtime))
          end)
    runtimes;
  collect trace state;
  close_trace trace

(** Runs one round: worker Domains attach and operate while the main Domain
    closes the shared runtime at a seeded moment. The main Domain's phases go
    to [trace], which the caller owns. *)
let run_round trace seed domains round_number =
  trace_phase trace "shared_runtime_create";
  let shared =
    match Bridge.shared_runtime_create ~worker_threads:2 () with
    | Ok shared -> shared
    | Error error -> failwith ("shared runtime create: " ^ error.message)
  in
  let round =
    {
      published = Array.init domains (fun _ -> Atomic.make None);
      cross_closes = Atomic.make 0;
      attach_refusals = Atomic.make 0;
      finalizer_drops = Atomic.make 0;
    }
  in
  trace_phase trace (Printf.sprintf "spawn %d worker domains" domains);
  let workers =
    List.init domains (fun index ->
        Domain.spawn (fun () -> worker seed round_number index round shared domains))
  in
  let state = rng seed round_number (-1) in
  for _ = 1 to Random.State.int state 2_000 do
    Domain.cpu_relax ()
  done;
  trace_phase trace "shared close";
  ignore (check "shared close" (fun _ -> false) (Bridge.shared_runtime_close shared));
  trace_phase trace "repeated shared close";
  ignore (check "repeated shared close" (fun _ -> false) (Bridge.shared_runtime_close shared));
  trace_phase trace "attach after shared close";
  (match Bridge.runtime_attach shared with
  | Error { Bridge.status = Bridge.Invalid_argument | Bridge.Invalid_state; _ } -> ()
  | Error error -> failwith ("attach after shared close: " ^ error.message)
  | Ok _ -> failwith "attach after shared close succeeded");
  trace_phase trace "join worker domains";
  List.iter Domain.join workers;
  ( Atomic.get round.cross_closes,
    Atomic.get round.attach_refusals,
    Atomic.get round.finalizer_drops )

(** Runs every round, then forces the remaining finalizers. *)
let () =
  let seed = env_seed () in
  let rounds = env_int "CONCURRENT_STRESS_OCAML_ROUNDS" default_rounds 1 10_000 in
  let domains = env_int "CONCURRENT_STRESS_OCAML_DOMAINS" default_domains 2 32 in
  prepare_trace_directory ();
  (* Printed before any native call so a crash still shows the budget in the
     Dune output; the trace files hold the per-Domain detail. *)
  Printf.printf "concurrent OCaml stress: starting seed=0x%Lx rounds=%d domains=%d trace=%s\n%!"
    seed rounds domains
    (Filename.concat (Sys.getcwd ()) trace_directory);
  let cross = ref 0 and refusals = ref 0 and drops = ref 0 in
  for round_number = 1 to rounds do
    let trace = open_trace ~seed ~round:round_number "main.trace" in
    let c, r, d = run_round trace seed domains round_number in
    cross := !cross + c;
    refusals := !refusals + r;
    drops := !drops + d;
    trace_phase trace "gc full_major after join";
    Gc.full_major ();
    close_trace trace
  done;
  (* The last finalizers run here; round [rounds + 1] marks this phase. *)
  let trace = open_trace ~seed ~round:(rounds + 1) "main.trace" in
  trace_phase trace "final gc full_major, compact, full_major";
  Gc.full_major ();
  Gc.compact ();
  Gc.full_major ();
  close_trace trace;
  Printf.printf
    "concurrent OCaml stress: seed=0x%Lx rounds=%d domains=%d cross_closes=%d \
     attach_refusals=%d finalizer_drops=%d\n%!"
    seed rounds domains !cross !refusals !drops
