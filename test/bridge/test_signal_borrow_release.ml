(* Regression test for #1018: a raising OCaml signal handler that is pending
   when a bridge stub releases the runtime lock must not unwind that stub.

   Before the fix, [caml_enter_blocking_section] ran the pending handler and
   re-raised its exception from inside the stub, after the stub had already
   taken a counted runtime borrow, detached the native pointer, or copied its
   input to [malloc]'d memory. The borrow then leaked, so the next close spun
   forever. The stubs now use [caml_enter_blocking_section_no_pending]; the
   handler still runs, but only at the first OCaml poll point after the stub
   has returned and released everything it owns.

   Each probe below makes SIGUSR1 pending with a C helper that does not run
   OCaml handlers, then calls the production stub directly through its own
   [external] declaration. The public wrappers allocate closures between the
   call site and the stub, which are poll points, so they would run the
   handler too early to exercise the stub. A probe sets [returned] with a
   non-allocating store as soon as the stub returns. If the stub unwound,
   [returned] stays false and the test fails before any close can spin. *)

module Bridge = Temporal_core_bridge.Native_bridge

(** The exception the test's signal handler raises, standing in for
    [Sys.Break] or an application's unwinding SIGINT/SIGTERM handler. *)
exception Interrupted

(** The raw native response custom block, as the production bridge declares
    it privately. Only [response_status] and [response_free] touch it. *)
type response

(** Makes SIGUSR1 pending without running the OCaml handler; see
    [signal_test_stubs.c]. *)
external raise_sigusr1 : unit -> unit = "ocaml_temporal_test_raise_sigusr1"
[@@noalloc]

(** Reads the typed status of a native response. *)
external response_status : response -> int = "ocaml_temporal_response_status"

(** Frees a native response deterministically. *)
external response_free : response -> unit = "ocaml_temporal_response_free"

(** [invoke_runtime] path: borrows the runtime, takes no input. *)
external worker_try_poll_workflow_raw : Bridge.runtime -> response
  = "ocaml_temporal_worker_try_poll_workflow"

(** [invoke_runtime_json] path: borrows the runtime and holds a [malloc]'d
    input copy across the blocking section. *)
external client_start_workflow_json_raw : Bridge.runtime -> bytes -> response
  = "ocaml_temporal_client_start_workflow_json"

(** The submit stub, which has its own borrow and input copy. *)
external client_submit_json_raw : Bridge.runtime -> int -> bytes -> response
  = "ocaml_temporal_client_submit_json"

(** Borrows the shared runtime while Rust attaches a graph to it. *)
external runtime_attach_raw : Bridge.shared_runtime -> Bridge.runtime * response
  = "ocaml_temporal_runtime_attach"

(** Detaches the native pointer, then waits for borrows and frees the graph. *)
external runtime_close_raw : Bridge.runtime -> int
  = "ocaml_temporal_runtime_close"

(** Counts handler runs, so each probe can check that its signal was handled
    exactly once and not lost. *)
let handler_runs = ref 0

(** Set by a probe immediately after the stub under test returns. *)
let returned = ref false

(** The native status of the last probed close. *)
let close_status = ref (-1)

(** The native status of the last probed attach response. *)
let attach_status = ref (-1)

(** Runs allocations until the pending handler has run. Each allocation is a
    poll point; the handler's exception ends the loop. The bound only keeps a
    lost signal from looping forever; the caller reports it. *)
let drain_pending_signal before =
  let remaining = ref 1_000_000 in
  while !handler_runs = before && !remaining > 0 do
    ignore (Sys.opaque_identity (ref !remaining));
    decr remaining
  done

(** Runs one [probe] with SIGUSR1 pending at stub entry and checks that the
    stub returned normally and that the handler then ran exactly once, after
    it. *)
let drive name probe =
  returned := false;
  let before = !handler_runs in
  (try
     probe ();
     drain_pending_signal before
   with Interrupted -> ());
  if not !returned then
    failwith
      (name ^ ": the signal handler's exception unwound the native stub (#1018)");
  if !handler_runs <> before + 1 then
    failwith (name ^ ": the pending signal handler did not run exactly once")

(** Probes [invoke_runtime]. The runtime has no worker, so the stub returns a
    typed error; only that it returns matters. *)
let probe_try_poll runtime () =
  raise_sigusr1 ();
  let response = worker_try_poll_workflow_raw runtime in
  returned := true;
  response_free response

(** Probes [invoke_runtime_json]; [input] is allocated before the signal. *)
let probe_start runtime input () =
  raise_sigusr1 ();
  let response = client_start_workflow_json_raw runtime input in
  returned := true;
  response_free response

(** Probes the submit stub with the signal-call selector; the runtime has no
    client, so Rust refuses the call after the borrow was taken. *)
let probe_submit runtime input () =
  raise_sigusr1 ();
  let response = client_submit_json_raw runtime 6 input in
  returned := true;
  response_free response

(** Probes the attach stub. The attached graph is stored in [slot] with a
    non-allocating write so it can be closed explicitly. *)
let probe_attach shared slot () =
  raise_sigusr1 ();
  let attached, response = runtime_attach_raw shared in
  returned := true;
  attach_status := response_status response;
  response_free response;
  slot.(0) <- attached

(** Probes close, which detaches the native pointer before it releases the
    lock: an unwind here used to leak the whole native graph. *)
let probe_close runtime () =
  raise_sigusr1 ();
  let status = runtime_close_raw runtime in
  returned := true;
  close_status := status

(** Unwraps a bridge result for setup steps that must succeed. *)
let unwrap name = function
  | Ok value -> value
  | Error error -> failwith (name ^ ": " ^ error.Bridge.message)

let () =
  if Sys.win32 then print_endline "skipped: Windows has no SIGUSR1"
  else begin
    Sys.set_signal Sys.sigusr1
      (Sys.Signal_handle
         (fun _ ->
           incr handler_runs;
           raise Interrupted));
    let runtime = unwrap "runtime_create" (Bridge.runtime_create ()) in
    let input = Bytes.of_string "{}" in
    drive "invoke_runtime" (probe_try_poll runtime);
    drive "invoke_runtime_json" (probe_start runtime input);
    drive "client_submit_json" (probe_submit runtime input);

    let shared =
      unwrap "shared_runtime_create" (Bridge.shared_runtime_create ())
    in
    let slot = [| runtime |] in
    drive "runtime_attach" (probe_attach shared slot);
    if !attach_status <> 0 then failwith "runtime_attach did not succeed";
    let attached = slot.(0) in
    if attached == runtime then failwith "runtime_attach stored no graph";
    drive "runtime_close (attached)" (probe_close attached);
    if !close_status <> 0 then failwith "attached runtime close failed";
    (* A leaked attach borrow would make this close spin forever. *)
    unwrap "shared_runtime_close" (Bridge.shared_runtime_close shared);

    (* A borrow leaked by any earlier probe would make this close spin. *)
    drive "runtime_close" (probe_close runtime);
    if !close_status <> 0 then failwith "runtime close failed";
    unwrap "repeated runtime_close" (Bridge.runtime_close runtime);
    Sys.set_signal Sys.sigusr1 Sys.Signal_default;
    print_endline "signal borrow release: ok"
  end
