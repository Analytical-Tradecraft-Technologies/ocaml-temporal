(** Per-worker Domain budget regressions (#1042).

    Dune runs this executable with [OCAMLRUNPARAM=d=1], which leaves the
    runtime its main Domain only. Anything that still spawned a Domain would
    therefore fail here: a workflow-only run loop, its thread-hosted activity
    lane (#1211) and the activation watchdog must all work without one, and a run loop that does need its activity
    Domain must report the spawn failure as a typed error instead of raising.
    OCaml 5.2 ignores the [d] parameter (its limit is fixed at 128), so the
    checks that depend on exhaustion are skipped there. *)

module Loop = Temporal_runtime.Native_worker_loop
module Watchdog = Temporal_runtime.Native_worker_watchdog

(** Whether [Domain.spawn] fails in this process. A Domain that did start is
    joined at once, so the probe leaves nothing running. *)
let domains_exhausted () =
  match Domain.spawn ignore with
  | domain ->
      Domain.join domain;
      false
  | exception Failure _ -> true

(** Waits for [predicate] for at most about five seconds. *)
let await ~what predicate =
  let deadline = Unix.gettimeofday () +. 5. in
  while not (predicate ()) do
    if Unix.gettimeofday () > deadline then failwith ("timed out: " ^ what);
    Thread.delay 0.001
  done

(** A workflow-only run loop spawns no activity Domain, never polls the
    activity lane, and, as the only lane, takes every idle wait natively
    instead of alternating with local yields. *)
let test_workflow_only_loop_spawns_no_domain () =
  let workflow_polls = Atomic.make 0 in
  let native_waits = Atomic.make 0 in
  let closed () = Atomic.get workflow_polls >= 6 in
  let poll_workflow () =
    match Atomic.fetch_and_add workflow_polls 1 with
    | 0 | 2 -> Ok Loop.Progress
    | _ -> Ok Loop.Not_ready
  in
  let wait_for_lane ~workflow_lane ~native_wait =
    if not workflow_lane then failwith "absent activity lane waited";
    if not native_wait then
      failwith "a lone workflow lane yielded locally instead of waiting";
    Atomic.incr native_waits;
    Ok ()
  in
  match
    Loop.run ~activity_lane:Loop.No_activity_lane
      ~lane_unavailable:(fun _ -> failwith "no Domain should be spawned")
      ~detach:None ~closed ~poll_workflow
      ~poll_activity:(fun () -> failwith "absent activity lane was polled")
      ~wait_for_lane
      ~retry_pending:(fun ~workflow_lane:_ -> failwith "nothing to retry")
  with
  | Ok () ->
      if Atomic.get native_waits = 0 then
        failwith "the workflow lane never waited for readiness"
  | Error () -> failwith "workflow-only loop failed"

(** A workflow-only worker's activity lane runs on a system thread (#1211):
    with only the main Domain available it still starts, polls on the
    calling Domain, takes native waits alternately with the workflow lane,
    and is joined when the loop closes. Before the fix such a worker ran no
    activity lane, so a local-activity task its workflow scheduled was never
    polled. *)
let test_workflow_only_activity_lane_uses_a_thread () =
  let self = Domain.self () in
  let activity_polls = Atomic.make 0 in
  let foreign_domain = Atomic.make false in
  let closed () = Atomic.get activity_polls >= 4 in
  let poll_workflow () = Ok Loop.Not_ready in
  let poll_activity () =
    if (Domain.self () :> int) <> (self :> int) then
      Atomic.set foreign_domain true;
    (* The first poll finds the pending local-activity task and rejects it. *)
    if Atomic.fetch_and_add activity_polls 1 = 0 then Ok Loop.Progress
    else Ok Loop.Not_ready
  in
  match
    Loop.run ~activity_lane:Loop.Activity_thread
      ~lane_unavailable:(fun exception_ -> `Unavailable exception_)
      ~detach:None ~closed ~poll_workflow ~poll_activity
      ~wait_for_lane:(fun ~workflow_lane:_ ~native_wait:_ ->
        Thread.yield ();
        Ok ())
      ~retry_pending:(fun ~workflow_lane:_ -> failwith "nothing to retry")
  with
  | Ok () ->
      if Atomic.get foreign_domain then
        failwith "the thread-hosted activity lane ran on another Domain"
  | Error (`Unavailable exception_) ->
      failwith
        ("thread-hosted activity lane did not start: "
        ^ Printexc.to_string exception_)

(** When the activity Domain cannot be spawned, [run] returns the caller's
    typed error carrying the spawn exception and runs neither lane, so the
    worker can simply be run again once a Domain is free. *)
let test_activity_domain_failure_is_typed () =
  let touched = Atomic.make false in
  let touch () =
    Atomic.set touched true;
    Ok Loop.Not_ready
  in
  match
    Loop.run ~activity_lane:Loop.Activity_domain
      ~lane_unavailable:(fun exception_ -> `Domain_unavailable exception_)
      ~detach:None
      ~closed:(fun () -> false)
      ~poll_workflow:touch ~poll_activity:touch
      ~wait_for_lane:(fun ~workflow_lane:_ ~native_wait:_ -> Ok ())
      ~retry_pending:(fun ~workflow_lane:_ -> Ok ())
  with
  | Error (`Domain_unavailable (Failure _)) ->
      if Atomic.get touched then
        failwith "a lane ran although the activity Domain never started"
  | Error (`Domain_unavailable exception_) ->
      failwith ("unexpected spawn exception: " ^ Printexc.to_string exception_)
  | Ok () -> failwith "run reported success without its activity lane"
  | exception exception_ ->
      failwith ("spawn failure escaped as " ^ Printexc.to_string exception_)

(** The watchdog needs no Domain of its own: with only the main Domain left it
    still starts on a system thread, detects an activation that outlives its
    deadline, and stops. *)
let test_watchdog_needs_no_domain () =
  let abandoned = Atomic.make None in
  let watchdog =
    match
      Watchdog.start
        ~spawn:(fun body ->
          match Thread.create body () with
          | _thread -> Ok ()
          | exception exception_ -> Error exception_)
        ~host_closing:(fun () -> false)
        ~deadline_ms:20
        ~running_epoch:(fun () -> Some 3)
        ~abandon:(fun ~epoch ~elapsed_ms:_ -> Atomic.set abandoned (Some epoch))
    with
    | Ok watchdog -> watchdog
    | Error exception_ ->
        failwith ("watchdog did not start: " ^ Printexc.to_string exception_)
  in
  Fun.protect
    ~finally:(fun () -> Watchdog.stop watchdog)
    (fun () ->
      await ~what:"watchdog detection" (fun () ->
          Atomic.get abandoned = Some 3))

(** Runs the budget regressions. *)
let () =
  let exhausted = domains_exhausted () in
  test_workflow_only_loop_spawns_no_domain ();
  test_workflow_only_activity_lane_uses_a_thread ();
  test_watchdog_needs_no_domain ();
  if exhausted then begin
    test_activity_domain_failure_is_typed ();
    print_endline "PASS worker Domain budget"
  end
  else
    print_endline
      "SKIP worker Domain exhaustion: runtime ignores OCAMLRUNPARAM d"
