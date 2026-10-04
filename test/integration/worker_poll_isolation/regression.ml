(** Opt-in live #492 regression. One worker's activity callback is held by an
    atomic gate while an unrelated workflow completes a durable timer and its
    exact run is read through a fresh Temporal client. The gate is always
    released before worker shutdown so a failing assertion does not leave the
    test's own callback parked. Use only a disposable Temporal Server. *)
open Temporal

(** Converts the public result boundary into a short fixture failure. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

let namespace =
  Sys.getenv_opt "TEMPORAL_NAMESPACE"
  |> Option.value ~default:"temporal-sdk-test"

(** The live controller's console filter suppresses a Docker build command's
    remaining output after a sensitive line. Keep only fixed, payload-free
    phase markers in an allowlisted file so a timed-out fixture identifies the
    last completed step without publishing arbitrary callback or Core logs. *)
let record_phase phase status =
  match Sys.getenv_opt "TEMPORAL_POLL_ISOLATION_LOG_FILE" with
  | None -> ()
  | Some path ->
      (try
         let channel =
           open_out_gen [Open_creat; Open_append; Open_text] 0o600 path
         in
         Fun.protect
           ~finally:(fun () -> close_out channel)
           (fun () ->
             Printf.fprintf channel "poll isolation phase=%s status=%s\n"
               phase status)
       with _ -> ())

(** The callback gate belongs to this process and is never read by workflow
    code; replay therefore sees only the durable activity command/result. *)
let activity_entered = Atomic.make false
let release_activity = Atomic.make false

(** Blocks the sole activity slot until the driver has checked an unrelated
    exact-run result. A short sleep releases the OCaml runtime lock. *)
let blocked_activity =
  Activity.define ~name:"poll-isolation.blocked-activity"
    ~input:Codec.string ~output:Codec.string (fun input ->
      Atomic.set activity_entered true;
      while not (Atomic.get release_activity) do Thread.delay 0.01 done;
      Ok ("activity:" ^ input))

(** Produces the blocked activity task on the worker's only task queue. *)
let activity_workflow =
  Workflow.define ~name:"poll-isolation.activity-workflow"
    ~input:Codec.string ~output:Codec.string (fun input ->
      Activity.execute blocked_activity input)

(** Requires a second workflow activation after a server timer fires. A
    completion observed from a fresh client proves more than first-task poll. *)
let timer_workflow =
  Workflow.define ~name:"poll-isolation.timer-workflow"
    ~input:Codec.string ~output:Codec.string (fun input ->
      Result.bind (Workflow.sleep (Duration.of_ms 25L)) (fun () ->
        Ok ("timer:" ^ input)))

(** Waits for a local observation with an explicit fixture deadline. *)
let await label seconds predicate =
  let deadline = Unix.gettimeofday () +. seconds in
  while not (predicate ()) do
    if Unix.gettimeofday () >= deadline then
      failwith ("timed out waiting for " ^ label);
    Thread.delay 0.01
  done

(** Reads the server-backed terminal outcome through an independent native
    client and the exact workflow/run identity returned by [Client.start]. The
    wait runs on another Domain so a broken worker cannot hang the gate; client
    shutdown interrupts it after the test deadline. *)
let wait_exact label address workflow handle =
  record_phase (label ^ "_client_create") "begin";
  let client = get (Client.create ~target_url:address ~namespace ()) in
  record_phase (label ^ "_client_create") "ok";
  let execution : Client.execution =
    { namespace; workflow_id = Client.workflow_id handle;
      run_id = Client.run_id handle }
  in
  let outcome = Atomic.make None in
  let waiting =
    Domain.spawn (fun () ->
      let result =
        try
          let followed = get (Client.follow client ~workflow execution) in
          Ok (Client.wait followed)
        with exception_ -> Error exception_
      in
      Atomic.set outcome (Some result))
  in
  let observed =
    try
      await "exact-run terminal result" 15. (fun () ->
        Option.is_some (Atomic.get outcome));
      Ok ()
    with exception_ ->
      record_phase (label ^ "_wait") "deadline";
      Error exception_
  in
  (* On timeout, close the client first: this interrupts its retained native
     wait before the driver joins the helper Domain. *)
  (match observed with
  | Error _ ->
      record_phase (label ^ "_client_shutdown") "begin";
      ignore (Client.shutdown client);
      record_phase (label ^ "_client_shutdown") "done"
  | Ok () -> ());
  record_phase (label ^ "_wait_join") "begin";
  Domain.join waiting;
  record_phase (label ^ "_wait_join") "done";
  record_phase (label ^ "_client_shutdown") "begin";
  ignore (Client.shutdown client);
  record_phase (label ^ "_client_shutdown") "done";
  (match observed with Ok () -> () | Error exception_ -> raise exception_);
  match Atomic.get outcome with
  | Some (Ok result) -> get result
  | Some (Error exception_) -> raise exception_
  | None -> failwith "exact-run waiter returned without an outcome"

(** A fresh task queue isolates the fixture from application workers sharing
    the disposable namespace. Every execution is terminated on cleanup. *)
let check address =
  record_phase "fixture" "started";
  if not (String.starts_with ~prefix:"http://" address) then
    failwith "poll isolation fixture requires an HTTP development server";
  let queue = Printf.sprintf "poll-isolation-%d-%d" (Unix.getpid ())
    (Random.State.bits (Random.State.make_self_init ())) in
  record_phase "worker_create" "begin";
  let worker = get (Worker.create ~target_url:address ~namespace
    ~task_queue:queue
    ~workflows:[Worker.workflow activity_workflow; Worker.workflow timer_workflow]
    ~activities:[Worker.activity blocked_activity] ()) in
  record_phase "worker_create" "ok";
  let running = Domain.spawn (fun () -> Worker.run worker) in
  record_phase "worker_run" "started";
  let client = ref None in
  let executions = ref [] in
  let shutdown_result = ref None in
  let run_result = ref None in
  let body =
    try
      let connected = get (Client.create ~target_url:address ~namespace ()) in
      client := Some connected;
      record_phase "client_create" "ok";
      let start workflow suffix =
        let handle = get (Client.start connected ~workflow ~task_queue:queue
          ~id:(queue ^ suffix) ~input:suffix ()) in
        executions := (fun () -> ignore (Client.terminate handle)) :: !executions;
        handle
      in
      let activity = start activity_workflow "-activity" in
      record_phase "activity_start" "ok";
      await "blocked activity callback" 15. (fun () ->
        Atomic.get activity_entered);
      record_phase "activity_entered" "ok";
      let timer = start timer_workflow "-timer" in
      record_phase "timer_start" "ok";
      record_phase "timer_wait" "begin";
      let result = wait_exact "timer" address timer_workflow timer in
      record_phase "timer_wait" "ok";
      if Atomic.get release_activity then
        failwith "activity barrier opened before the timer workflow completed";
      (match result with
      | Client.Completed "timer:-timer" -> ()
      | _ -> failwith "unrelated workflow did not complete its exact run");
      (* The first workflow must still be held while the server has recorded
         the second workflow's completed result. *)
      Atomic.set release_activity true;
      record_phase "activity_release" "ok";
      record_phase "activity_wait" "begin";
      (match wait_exact "activity" address activity_workflow activity with
      | Client.Completed "activity:-activity" -> ()
      | _ -> failwith "released activity workflow did not complete");
      record_phase "activity_wait" "ok";
      Printf.printf
        "worker poll isolation live regression: exact run %s completed before activity release\n%!"
        (Client.run_id timer);
      record_phase "body" "ok";
      Ok ()
    with exception_ ->
      record_phase "body" "failed";
      Error exception_
  in
  Atomic.set release_activity true;
  record_phase "cleanup_release" "ok";
  List.iter (fun terminate -> try terminate () with _ -> ()) !executions;
  record_phase "cleanup_terminate" "done";
  Option.iter (fun connected -> ignore (Client.shutdown connected)) !client;
  record_phase "cleanup_client" "done";
  record_phase "cleanup_worker" "begin";
  shutdown_result := Some (Worker.shutdown worker);
  record_phase "cleanup_worker" "done";
  record_phase "cleanup_join" "begin";
  run_result := Some (Domain.join running);
  record_phase "cleanup_join" "done";
  (match body with Ok () -> () | Error exception_ -> raise exception_);
  (match !shutdown_result with Some result -> ignore (get result) | None -> assert false);
  (match !run_result with Some result -> ignore (get result) | None -> assert false)

(** The caller supplies a disposable running server, for example the isolated
    PostgreSQL/Temporal Compose fixture used by the live CI suite. *)
let () = match Array.to_list Sys.argv with
  | [_; "check"; address] -> check address
  | _ -> failwith "usage: regression check http://temporal:7233"
