(** Public-API regression for queries after completion, both on the original
    worker and after a fresh worker reconstructs the execution from history. *)
open Temporal

(** Reports the public diagnostic when a client or worker operation fails. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Query state belongs to each execution and survives its terminal result. *)
let final_value = Workflow_context.Local.create ()

(** Completes immediately after storing the state inspected by queries. *)
let workflow = Workflow.define ~name:"completed-query-regression"
  ~input:Codec.string ~output:Codec.string (fun value ->
    Result.map (fun () -> value) (Workflow_context.Local.set final_value value))

(** Reads state through the same public context API used by running workflows. *)
let query = Query.define ~name:"final-value" ~output:Codec.string
let read () = Result.bind (Workflow_context.Local.get final_value) (function
  | Some value -> Ok value
  | None -> Error (Error.defect ~message:"completed workflow lost its local state"))

(** Runs the fixture worker on its own unique task queue. *)
let worker address queue =
  let worker = get (Worker.create ~target_url:address ~namespace:"default"
    ~task_queue:queue ~activities:[] ~workflows:[Worker.workflow workflow
      ~queries:[Query.Handler.make query read]] ()) in
  get (Worker.run worker)

(** Spawns a separate worker process so replacement has no shared OCaml state. *)
let spawn address queue = Unix.create_process Sys.executable_name
  [|Sys.executable_name; "worker"; address; queue|]
  Unix.stdin Unix.stdout Unix.stderr

(** Always reaps the stopped process; no fixture worker survives the test. *)
let stop pid = Unix.kill pid Sys.sigterm; ignore (Unix.waitpid [] pid)

(** Verifies repeated queries on a completed run, then replacement-worker replay. *)
let check address =
  let queue = Printf.sprintf "completed-queries-%d-%d" (Unix.getpid ())
    (Random.State.bits (Random.State.make_self_init ())) in
  let worker_pid = ref (Some (spawn address queue)) in
  Fun.protect ~finally:(fun () -> Option.iter stop !worker_pid) (fun () ->
    let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
    Fun.protect ~finally:(fun () -> ignore (Client.shutdown client)) (fun () ->
      let expected = "final workflow state" in
      let handle = get (Client.start client ~workflow ~task_queue:queue ~id:queue
        ~input:expected ()) in
      Fun.protect ~finally:(fun () -> ignore (Client.terminate handle)) (fun () ->
        (match get (Client.wait handle) with
        | Client.Completed value when value = expected -> ()
        | _ -> failwith "workflow result changed");
        for _ = 1 to 2 do
          if get (Client.query handle ~query) <> expected then
            failwith "completed query returned the wrong state"
        done;
        let missing = Query.define ~name:"missing" ~output:Codec.string in
        (match Client.query handle ~query:missing with
        | Error _ -> ()
        | Ok _ -> failwith "missing completed query unexpectedly succeeded");
        if get (Client.query handle ~query) <> expected then
          failwith "failed query invalidated the completed execution";
        Option.iter stop !worker_pid;
        worker_pid := None;
        worker_pid := Some (spawn address queue);
        for _ = 1 to 2 do
          if get (Client.query handle ~query) <> expected then
            failwith "replayed completed query returned the wrong state"
        done;
        print_endline "completed workflow query live regression: ok")))

(** Explicit modes let the driver replace its worker without extra binaries. *)
let () = match Array.to_list Sys.argv with
  | [_; "worker"; address; queue] -> worker address queue
  | [_; "check"; address] -> check address
  | _ -> failwith "usage: regression check http://localhost:7233"
