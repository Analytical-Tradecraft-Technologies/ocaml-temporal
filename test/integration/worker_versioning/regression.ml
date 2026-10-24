(** Live regression for per-workflow versioning behavior (#1159).

    Against a disposable Temporal server this check runs two deployment
    versions of one worker deployment, as separate worker processes on one
    task queue. Both register the same two workflow types:
    [worker-versioning.pinned] with [~versioning_behavior:`Pinned] and
    [worker-versioning.auto] with [~versioning_behavior:`Auto_upgrade]. The
    workers' default behavior is [`Auto_upgrade], so only the pinned type's
    own declaration can keep its run on its first version.

    - Build [v1] is made the deployment's current version and one run of
      each type starts on it. Each records the build that ran each of its
      workflow tasks after a [step] signal ([Workflow.current_deployment_version]).
    - After one step both runs report [v1]. Build [v2] then starts and
      becomes current, replacing [v1] as the target of new and auto-upgrade
      work, while the [v1] worker keeps running.
    - Routing changes reach the task queue asynchronously, so the
      auto-upgrade run is stepped until one of its workflow tasks runs on
      [v2] (within a bound), which shows it moved to the replacement worker.
      The pinned run is given the same number of steps, and every one of its
      tasks must still run on [v1].
    - The runs' histories, read with the official CLI, record
      [VERSIONING_BEHAVIOR_PINNED] and [VERSIONING_BEHAVIOR_AUTO_UPGRADE] on
      their completed workflow tasks.

    The parent owns and reaps its workers, uses a unique task queue and
    deployment name, and terminates every execution it starts. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the regression with a labelled message. *)
let fail label = failwith ("worker versioning: " ^ label)

(** Signal that lets a run record the build of its next workflow task. *)
let step = Signal.define ~name:"step" ~input:Codec.unit

(** Signal that completes a run with the builds it recorded. *)
let finish = Signal.define ~name:"finish" ~input:Codec.unit

(** Query returning the builds recorded so far, comma-separated. *)
let builds_query = Query.define ~name:"builds" ~output:Codec.string

(** Steps delivered so far, builds recorded so far (newest first), and
    whether [finish] arrived. *)
let steps = Workflow_context.Local.create ()
let builds = Workflow_context.Local.create ()
let finished = Workflow_context.Local.create ()

(** Reads a workflow-local value with a default. *)
let read key default =
  match Workflow_context.Local.get key with Ok (Some value) -> value | Ok None | Error _ -> default

(** Records the build of the workflow task that observes each step and
    completes with the builds in order once [finish] arrives. *)
let body () =
  let open Result_syntax in
  let rec loop recorded =
    if read finished false && read steps 0 = recorded then
      Ok (String.concat "," (List.rev (read builds [])))
    else
      let* () =
        Condition.wait_until (fun () -> read steps 0 > recorded || read finished false)
      in
      if read steps 0 = recorded then loop recorded
      else
      let build =
        match Workflow.current_deployment_version () with
        | Some { build_id; _ } -> build_id
        | None -> "unversioned"
      in
      let* () = Workflow_context.Local.set builds (build :: read builds []) in
      loop (recorded + 1)
  in
  loop 0

(** The pinned and the auto-upgrade workflow types. *)
let pinned =
  Workflow.define ~name:"worker-versioning.pinned" ~input:Codec.unit ~output:Codec.string body

let auto =
  Workflow.define ~name:"worker-versioning.auto" ~input:Codec.unit ~output:Codec.string body

(** Handlers shared by both types. *)
let signals =
  [
    Signal.Handler.make step (fun () -> Workflow_context.Local.set steps (read steps 0 + 1));
    Signal.Handler.make finish (fun () -> Workflow_context.Local.set finished true);
  ]

let queries =
  [ Query.Handler.make builds_query (fun () -> Ok (String.concat "," (List.rev (read builds [])))) ]

(** Runs build [build] of [deployment] on [queue] until killed. *)
let worker address queue deployment build =
  let options =
    get
      (Worker.Options.make
         ~versioning:
           (Worker.Options.Deployment_based
              {
                deployment_name = deployment;
                build_id = build;
                use_worker_versioning = true;
                default_versioning_behavior = Some `Auto_upgrade;
              })
         ())
  in
  let worker =
    get
      (Worker.create ~options ~target_url:address ~namespace:"default" ~task_queue:queue
         ~activities:[]
         ~workflows:
           [
             Worker.workflow ~versioning_behavior:`Pinned ~signals ~queries pinned;
             Worker.workflow ~versioning_behavior:`Auto_upgrade ~signals ~queries auto;
           ]
         ())
  in
  get (Worker.run worker)

(** Starts a worker process for [build]. *)
let spawn address queue deployment build =
  Unix.create_process Sys.executable_name
    [| Sys.executable_name; "worker"; address; queue; deployment; build |]
    Unix.stdin Unix.stdout Unix.stderr

(** Kills and reaps a worker process; an already exited one is fine. *)
let stop pid =
  (try Unix.kill pid Sys.sigterm with Unix.Unix_error _ -> ());
  ignore (Unix.waitpid [] pid)

(** Retries [attempt] for up to 90 s, for state that appears asynchronously. *)
let await label attempt =
  let deadline = Unix.gettimeofday () +. 90. in
  let rec poll () =
    match attempt () with
    | Ok value -> value
    | Error _ when Unix.gettimeofday () < deadline ->
        Unix.sleepf 0.5;
        poll ()
    | Error message -> fail (Printf.sprintf "timed out waiting for %s: %s" label message)
  in
  poll ()

(** Runs the official CLI with [args] after the connection flags, without a
    shell, and returns its standard output when it exits 0. *)
let run_cli cli cli_address args =
  let argv =
    Array.append [| cli; "--address"; cli_address; "--namespace"; "default" |] args
  in
  let input = Unix.open_process_args_in cli argv in
  let output = In_channel.input_all input in
  match Unix.close_process_in input with
  | Unix.WEXITED 0 -> Ok output
  | Unix.WEXITED code -> Error (Printf.sprintf "CLI exited with %d" code)
  | Unix.WSIGNALED _ | Unix.WSTOPPED _ -> Error "CLI was killed"

(** Makes [build] the deployment's current version once its worker has
    registered it by polling. *)
let set_current cli cli_address deployment build =
  ignore
    (await ("current version " ^ build) (fun () ->
         run_cli cli cli_address
           [| "worker"; "deployment"; "set-current-version"; "--deployment-name"; deployment;
              "--build-id"; build; "--yes" |]))

(** The [versioningBehavior] of every completed workflow task in the run's
    history, read with the CLI. *)
let recorded_behaviors cli cli_address handle =
  let output =
    await "history" (fun () ->
        run_cli cli cli_address
          [| "workflow"; "show"; "--workflow-id"; Client.workflow_id handle; "--run-id";
             Option.get (Client.run_id handle); "--output"; "json" |])
  in
  let open Yojson.Basic.Util in
  Yojson.Basic.from_string output |> member "events" |> to_list
  |> List.filter_map (fun event ->
         match event |> member "eventType" |> to_string with
         | "EVENT_TYPE_WORKFLOW_TASK_COMPLETED" ->
             Some
               (match
                  event |> member "workflowTaskCompletedEventAttributes"
                  |> member "versioningBehavior"
                with
                | `String behavior -> behavior
                | _ -> "absent")
         | _ -> None)

(** Waits until [handle]'s query reports [expected] builds. *)
let await_builds handle expected =
  await
    (Client.workflow_id handle ^ " builds " ^ expected)
    (fun () ->
      match Client.query handle ~query:builds_query with
      | Ok builds when builds = expected -> Ok ()
      | Ok builds -> Error ("builds " ^ builds)
      | Error error -> Error (Error.message error))

(** Runs the scenario described in the module docs. *)
let check address cli =
  let cli_address =
    if String.starts_with ~prefix:"http://" address then
      String.sub address 7 (String.length address - 7)
    else fail "this fixture requires an http:// development server address"
  in
  let suffix = Temporal_base.Client_request_id.create () in
  let queue = "worker-versioning-" ^ suffix and deployment = "worker-versioning-" ^ suffix in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  let workers = ref [] and cleanups = ref [] in
  Fun.protect
    ~finally:(fun () ->
      List.iter (fun cleanup -> cleanup ()) !cleanups;
      ignore (Client.shutdown client);
      List.iter stop !workers)
    (fun () ->
      workers := [ spawn address queue deployment "v1" ];
      set_current cli cli_address deployment "v1";
      let start workflow id =
        let handle =
          get (Client.start client ~workflow ~task_queue:queue ~id:(queue ^ "-" ^ id) ~input:() ())
        in
        cleanups := (fun () -> ignore (Client.terminate handle)) :: !cleanups;
        handle
      in
      let pinned_run = start pinned "pinned" and auto_run = start auto "auto" in
      List.iter (fun handle -> await_builds handle "") [ pinned_run; auto_run ];
      List.iter
        (fun handle ->
          get (Client.signal handle ~signal:step ~input:());
          await_builds handle "v1")
        [ pinned_run; auto_run ];
      workers := spawn address queue deployment "v2" :: !workers;
      set_current cli cli_address deployment "v2";
      (* Step the auto-upgrade run until the new routing reaches it. *)
      let deadline = Unix.gettimeofday () +. 60. in
      let rec step_until_moved taken =
        get (Client.signal auto_run ~signal:step ~input:());
        let expected = String.concat "," (List.init (taken + 1) (fun _ -> "v1")) in
        let moved = expected ^ ",v2" and stayed = expected ^ ",v1" in
        let builds =
          await "the auto-upgrade run's next step" (fun () ->
              match Client.query auto_run ~query:builds_query with
              | Ok builds when builds = moved || builds = stayed -> Ok builds
              | Ok builds -> Error ("builds " ^ builds)
              | Error error -> Error (Error.message error))
        in
        if builds = moved then taken + 1
        else if Unix.gettimeofday () > deadline then
          fail "the auto-upgrade run never moved to the new current version"
        else (
          Unix.sleepf 0.5;
          step_until_moved (taken + 1))
      in
      let taken = step_until_moved 0 in
      for _ = 1 to taken do
        get (Client.signal pinned_run ~signal:step ~input:())
      done;
      await_builds pinned_run (String.concat "," (List.init (taken + 1) (fun _ -> "v1")));
      List.iter (fun handle -> get (Client.signal handle ~signal:finish ~input:())) [ pinned_run; auto_run ];
      let result handle =
        match get (Client.wait ~timeout:(Duration.of_seconds 60L) handle) with
        | Client.Completed { output; _ } -> output
        | _ -> fail (Client.workflow_id handle ^ " did not complete")
      in
      let pinned_builds = result pinned_run and auto_builds = result auto_run in
      if String.split_on_char ',' pinned_builds |> List.exists (( <> ) "v1") then
        fail ("the pinned run ran on " ^ pinned_builds);
      if not (String.ends_with ~suffix:",v2" auto_builds) then
        fail ("the auto-upgrade run ran on " ^ auto_builds);
      let check_history handle expected =
        let behaviors = recorded_behaviors cli cli_address handle in
        if behaviors = [] || List.exists (fun behavior -> behavior <> expected) behaviors then
          fail
            (Printf.sprintf "%s recorded %s" (Client.workflow_id handle)
               (String.concat "," behaviors))
      in
      check_history pinned_run "VERSIONING_BEHAVIOR_PINNED";
      check_history auto_run "VERSIONING_BEHAVIOR_AUTO_UPGRADE";
      Printf.printf "pinned run: %s; auto-upgrade run: %s\n%!" pinned_builds auto_builds;
      print_endline "worker versioning live regression: ok")

(** The supplied URL must identify a disposable test namespace/server; the
    second argument is the official Temporal CLI. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address; cli ] -> check address cli
  | [ _; "worker"; address; queue; deployment; build ] -> worker address queue deployment build
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"
