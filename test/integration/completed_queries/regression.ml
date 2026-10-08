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

(** A registered query whose handler always returns a business error, so the
    client must see Temporal's query failure rather than a malformed request
    (issue #823). *)
let refusing_query = Query.define ~name:"refusing" ~output:Codec.string
let refusal = "this query is refused by its handler"
let refuse () = Error (Error.make ~category:`Workflow ~message:refusal ())

(** Signal sent only after completion, which Temporal rejects as NotFound. *)
let late_signal = Signal.define ~name:"late" ~input:Codec.string

(** Requires the typed, non-retryable query handler failure of issue #823 and
    returns its message. *)
let expect_query_failed label = function
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")
  | Error error ->
      let view = Error.view error in
      if not (Client.is_query_failed error && view.non_retryable
              && view.category = `Workflow
              && view.error_type = Some "QueryFailed") then
        failwith (Printf.sprintf "%s was not a typed query failure: %s %S"
          label (Error.kind error) view.message);
      view.message

(** Runs the fixture worker on its own unique task queue. *)
let worker address queue =
  let worker = get (Worker.create ~target_url:address ~namespace:"default"
    ~task_queue:queue ~activities:[] ~workflows:[Worker.workflow workflow
      ~queries:[Query.Handler.make query read;
                Query.Handler.make refusing_query refuse]] ()) in
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
        | Client.Completed { output = value; _ } when value = expected -> ()
        | _ -> failwith "workflow result changed");
        for _ = 1 to 2 do
          if get (Client.query handle ~query) <> expected then
            failwith "completed query returned the wrong state"
        done;
        let missing = Query.define ~name:"missing" ~output:Codec.string in
        if expect_query_failed "missing completed query"
             (Client.query handle ~query:missing) = "" then
          failwith "missing query failure lost its diagnostic";
        let message = expect_query_failed "refusing completed query"
          (Client.query handle ~query:refusing_query) in
        if not (String.equal message refusal) then
          failwith (Printf.sprintf "query failure lost the handler message: %S"
            message);
        (* A signal to the closed run is a permanent NotFound, not a
           retryable transport failure. *)
        (match Client.signal handle ~signal:late_signal ~input:"late" with
        | Ok () -> failwith "signal to a completed run unexpectedly succeeded"
        | Error error ->
            if not (Client.rpc_status error = Some `Not_found
                    && (Error.view error).non_retryable) then
              failwith ("signal to a completed run was not a typed NotFound: "
                ^ Error.message error));
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
