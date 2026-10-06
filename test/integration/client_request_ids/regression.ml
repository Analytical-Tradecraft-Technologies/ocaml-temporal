(** Live regression for generated IDs across independent native clients and
    fresh processes. The parent owns and reaps its worker and terminates every
    workflow it creates, using an isolated task queue on the supplied server. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** All processes share a numeric wire codec so query/update values are exact. *)
let number =
  Codec.make ~encoding:"json/plain"
    ~encode:(fun value -> Ok (Bytes.of_string (string_of_int value)))
    ~decode:(fun bytes ->
      match int_of_string_opt (Bytes.to_string bytes) with
      | Some value -> Ok value
      | None -> Error (Error.codec ~message:"invalid counter value"))

(** Each execution owns an independent counter retained while it is running. *)
let state = Workflow_context.Local.create ()
let read () = Result.map (Option.value ~default:0) (Workflow_context.Local.get state)
let add amount = Result.bind (read ()) (fun value -> Workflow_context.Local.set state (value + amount))
let workflow = Workflow.define ~name:"request-id-counter" ~input:number ~output:number
  (fun initial -> Result.bind (Workflow_context.Local.set state initial) (fun () ->
    Result.bind (Condition.wait_until (fun () -> false)) read))
let signal = Signal.define ~name:"add" ~input:number
let query = Query.define ~name:"get" ~output:number
let update = Update.define ~name:"add" ~input:number ~output:number

(** Every helper invocation gets a fresh native client and runtime graph. *)
let with_client address action =
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  Fun.protect ~finally:(fun () -> ignore (Client.shutdown client))
    (fun () -> action client)

(** Runs only the fixture definitions on the test's unique queue. *)
let worker address queue =
  let worker = get (Worker.create ~target_url:address ~namespace:"default"
    ~task_queue:queue ~activities:[]
    ~workflows:[Worker.workflow workflow
      ~signals:[Signal.Handler.make signal add]
      ~queries:[Query.Handler.make query read]
      ~updates:[Update.Handler.make update (fun amount -> Result.bind (add amount) read)]] ()) in
  get (Worker.run worker)

(** A new OS process sends exactly one operation, then reports the observed
    state/result. Explicit IDs exercise the intentional deduplication control. *)
let send address operation id run amount request_id =
  with_client address (fun client ->
    let handle = get (Client.follow client ~workflow
      {Client.namespace="default"; workflow_id=id; run_id=run}) in
    let request_id = if request_id = "-" then None else Some request_id in
    let value = match operation with
      | "signal" ->
          get (Client.signal ?request_id handle ~signal ~input:amount);
          get (Client.query handle ~query)
      | "update" ->
          let pending = get (Client.start_update ?update_id:request_id handle ~update ~input:amount ()) in
          get (Client.wait_update pending)
      | _ -> failwith "unknown operation" in
    Printf.printf "%d\n%!" value)

(** Reads a helper's assertion value and always reaps that process. *)
let child address operation handle amount request_id =
  let args = [|Sys.executable_name; operation; address;
    Client.workflow_id handle; Client.run_id handle;
    string_of_int amount; request_id|] in
  let channel = Unix.open_process_args_in Sys.executable_name args in
  let value = try Some (input_line channel) with End_of_file -> None in
  match Unix.close_process_in channel, value with
  | Unix.WEXITED 0, Some value -> int_of_string value
  | _ -> failwith "client subprocess failed"

(** Checks the result rather than depending on the ID's textual format. *)
let expect label expected actual =
  if expected <> actual then
    failwith (Printf.sprintf "%s: expected %d, got %d" label expected actual)

(** Waits for [handle]'s exact run and reports whether it was terminated. *)
let terminated handle =
  match Client.wait handle with
  | Ok (Client.Terminated _) -> true
  | Ok _ | Error _ -> false

(** Exercises the three workflow ID conflict policies (#933) against a running
    execution created by [start]: [`Fail] returns the typed already-started
    error naming the running run, [`Use_existing] attaches to it with
    [started = false], and [`Terminate_existing] terminates it and returns a
    new run. A request-ID retry of the creating start returns that run even
    under [`Terminate_existing]. [track] registers every handle for cleanup. *)
let check_id_conflict_policies ~start ~track client queue =
  let original = start ~request_id:"conflict-policy-original" "-policy" in
  if not (Client.started original) then failwith "new start was not marked started";
  let again ?id_conflict_policy ?request_id input =
    Client.start client ?id_conflict_policy ?request_id ~workflow
      ~task_queue:queue ~id:(Client.workflow_id original) ~input () in
  List.iter (fun id_conflict_policy ->
    match again ?id_conflict_policy 1 with
    | Ok _ -> failwith "fail policy started a duplicate run"
    | Error error ->
        if Error.error_type error <> Some "WorkflowExecutionAlreadyStarted" then
          failwith ("unexpected fail-policy error: " ^ Error.message error);
        match Client.already_started error with
        | Some { Client.run_id; _ } when run_id = Client.run_id original -> ()
        | Some _ | None -> failwith "already-started error lost the running run")
    [None; Some `Fail];
  let attached = track (get (again ~id_conflict_policy:`Use_existing 2)) in
  if Client.started attached then failwith "use_existing reported a new run";
  if Client.run_id attached <> Client.run_id original then
    failwith "use_existing returned a different run";
  expect "use_existing attaches to running state" 0
    (get (Client.query attached ~query));
  let retried = track (get (again ~id_conflict_policy:`Terminate_existing
    ~request_id:"conflict-policy-original" 0)) in
  if Client.run_id retried <> Client.run_id original then
    failwith "request-ID retry was not deduplicated before the conflict policy";
  let replacement = track (get (again ~id_conflict_policy:`Terminate_existing 3)) in
  if not (Client.started replacement) then
    failwith "terminate_existing did not report a new run";
  if Client.run_id replacement = Client.run_id original then
    failwith "terminate_existing reused the running run";
  if not (terminated original) then
    failwith "terminate_existing left the previous run open";
  expect "terminate_existing starts the new run" 3
    (get (Client.query replacement ~query))

(** Verifies starts and both message APIs, plus caller-owned retry IDs. *)
let check address =
  let queue = Temporal_base.Client_request_id.create () in
  let pid = Unix.create_process Sys.executable_name
    [|Sys.executable_name; "worker"; address; queue|]
    Unix.stdin Unix.stdout Unix.stderr in
  Fun.protect ~finally:(fun () ->
    Unix.kill pid Sys.sigterm; ignore (Unix.waitpid [] pid)) (fun () ->
    with_client address (fun a -> with_client address (fun b ->
      let handles = ref [] in
      let start ?request_id client suffix =
        let handle = get (Client.start client ?request_id ~workflow ~task_queue:queue
          ~id:(queue ^ suffix) ~input:0 ()) in
        handles := handle :: !handles;
        handle in
      Fun.protect ~finally:(fun () ->
        List.iter (fun handle -> ignore (Client.terminate handle)) !handles)
        (fun () ->
          let original = start a "-conflict" in
          (match Client.start b ~workflow ~task_queue:queue
            ~id:(Client.workflow_id original) ~input:999 () with
          | Error error when Error.kind error = "workflow" -> ()
          | _ -> failwith "independent client's start was incorrectly deduplicated");
          let retry = start ~request_id:"explicit-start" a "-retry" in
          let same = get (Client.start b ~request_id:"explicit-start" ~workflow
            ~task_queue:queue ~id:(Client.workflow_id retry) ~input:0 ()) in
          if Client.run_id retry <> Client.run_id same then failwith "start retry changed run";
          check_id_conflict_policies
            ~start:(fun ~request_id suffix -> start ~request_id a suffix)
            ~track:(fun handle -> handles := handle :: !handles; handle)
            b queue;
          List.iter (fun operation ->
            let handle = start a ("-" ^ operation) in
            expect (operation ^ " first") 1 (child address operation handle 1 "-");
            expect (operation ^ " second process") 3 (child address operation handle 2 "-");
            expect (operation ^ " explicit") 8 (child address operation handle 5 "explicit-message");
            expect (operation ^ " retry") 8 (child address operation handle 5 "explicit-message"))
            ["signal"; "update"];
          print_endline "client request ID live regression: ok"))))

(** The supplied URL must identify a disposable test namespace/server. *)
let () = match Array.to_list Sys.argv with
  | [_; "check"; address] -> check address
  | [_; "worker"; address; queue] -> worker address queue
  | [_; operation; address; id; run; amount; request_id] ->
      send address operation id run (int_of_string amount) request_id
  | _ -> failwith "usage: regression check http://localhost:7233"
