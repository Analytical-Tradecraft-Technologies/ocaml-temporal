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

(** The continue-as-new target of [chain], named by type so the definition
    can refer to its own workflow type. *)
let chain_target = Workflow.remote ~name:"request-id-chain" ~input:number ~output:number

(** A two-run entity workflow for the by-ID regression (#791): each run waits
    for a signal to raise its fresh counter, then the first run (input 1)
    continues as new and the second (input 0) completes with its counter. *)
let chain = Workflow.define ~name:"request-id-chain" ~input:number ~output:number
  (fun remaining -> Result.bind (Workflow_context.Local.set state 0) (fun () ->
    Result.bind
      (Condition.wait_until (fun () ->
        match read () with Ok value -> value > 0 | Error _ -> false))
      (fun () ->
        if remaining > 0 then Workflow.continue_as_new chain_target (remaining - 1)
        else read ())))
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
      ~updates:[Update.Handler.make update (fun amount -> Result.bind (add amount) read)];
      Worker.workflow chain
      ~signals:[Signal.Handler.make signal add]
      ~queries:[Query.Handler.make query read]] ()) in
  get (Worker.run worker)

(** A new OS process sends exactly one operation, then reports the observed
    state/result. Explicit IDs exercise the intentional deduplication control.
    A run of ["-"] addresses the workflow by ID alone, as a process that never
    saw the run ID would (#791). *)
let send address operation id run amount request_id =
  with_client address (fun client ->
    let handle =
      if run = "-" then get (Client.get_handle client ~workflow ~id ())
      else get (Client.follow client ~workflow
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

(** Reads a helper's assertion value and always reaps that process. The
    helper addresses the exact run unless [by_id] is set. *)
let child ?(by_id = false) address operation handle amount request_id =
  let run = if by_id then "-" else Option.get (Client.run_id handle) in
  let args = [|Sys.executable_name; operation; address;
    Client.workflow_id handle; run;
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
        | Some { Client.run_id; _ } when run_id = Option.get (Client.run_id original) -> ()
        | Some _ | None -> failwith "already-started error lost the running run")
    [None; Some `Fail];
  let attached = track (get (again ~id_conflict_policy:`Use_existing 2)) in
  if Client.started attached then failwith "use_existing reported a new run";
  if Option.get (Client.run_id attached) <> Option.get (Client.run_id original) then
    failwith "use_existing returned a different run";
  expect "use_existing attaches to running state" 0
    (get (Client.query attached ~query));
  let retried = track (get (again ~id_conflict_policy:`Terminate_existing
    ~request_id:"conflict-policy-original" 0)) in
  if Option.get (Client.run_id retried) <> Option.get (Client.run_id original) then
    failwith "request-ID retry was not deduplicated before the conflict policy";
  let replacement = track (get (again ~id_conflict_policy:`Terminate_existing 3)) in
  if not (Client.started replacement) then
    failwith "terminate_existing did not report a new run";
  if Option.get (Client.run_id replacement) = Option.get (Client.run_id original) then
    failwith "terminate_existing reused the running run";
  if not (terminated original) then
    failwith "terminate_existing left the previous run open";
  expect "terminate_existing starts the new run" 3
    (get (Client.query replacement ~query))

(** Addresses workflows by ID alone (#791). Another process signals, updates,
    and queries the running counter workflow without its run ID. Then a
    current-run handle follows the two-run [chain] workflow across its
    continue-as-new: [wait] is started on another Domain while the first run
    is open, the same handle's signal makes that run continue as new, the exact
    start handle reports [Continued_as_new], the current-run handle's query and
    signal reach the second run, and the pending [wait] returns the second
    run's completion. [track] registers every handle for cleanup. *)
let check_by_id ~start ~track address client queue =
  let counter = start "-by-id" in
  expect "signal by ID" 1 (child ~by_id:true address "signal" counter 1 "-");
  expect "update by ID" 3 (child ~by_id:true address "update" counter 2 "-");
  let first = track (get (Client.start client ~workflow:chain ~task_queue:queue
    ~id:(queue ^ "-chain") ~input:1 ())) in
  let by_id = track (get (Client.get_handle client ~workflow:chain
    ~id:(Client.workflow_id first) ())) in
  if Client.run_id by_id <> None then failwith "current-run handle pinned a run";
  expect "query by ID before continue-as-new" 0 (get (Client.query by_id ~query));
  let waiter = Domain.spawn (fun () -> Client.wait by_id) in
  (* Give the wait time to reach Temporal while the first run is open, so the
     result below has to follow the continue-as-new link. *)
  Unix.sleepf 1.0;
  get (Client.signal by_id ~signal ~input:1);
  let successor = match Client.wait first with
    | Ok (Client.Continued_as_new successor) -> successor
    | _ -> failwith "exact start handle did not report continue-as-new" in
  if Some successor.run_id = Client.run_id first then
    failwith "continue-as-new successor reused the first run";
  expect "query by ID reaches the new run" 0 (get (Client.query by_id ~query));
  get (Client.signal by_id ~signal ~input:5);
  (match Domain.join waiter with
  | Ok (Client.Completed { output = 5; successor = None }) -> ()
  | Ok _ -> failwith "current-run wait did not follow to the final run"
  | Error error -> failwith ("current-run wait failed: " ^ Error.message error));
  let followed = get (Client.follow client ~workflow:chain successor) in
  match Client.wait followed with
  | Ok (Client.Completed { output = 5; successor = None }) -> ()
  | _ -> failwith "exact successor handle did not complete"

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
      let chains = ref [] in
      let start ?request_id client suffix =
        let handle = get (Client.start client ?request_id ~workflow ~task_queue:queue
          ~id:(queue ^ suffix) ~input:0 ()) in
        handles := handle :: !handles;
        handle in
      Fun.protect ~finally:(fun () ->
        List.iter (fun handle -> ignore (Client.terminate handle)) !handles;
        List.iter (fun handle -> ignore (Client.terminate handle)) !chains)
        (fun () ->
          let original = start a "-conflict" in
          (match Client.start b ~workflow ~task_queue:queue
            ~id:(Client.workflow_id original) ~input:999 () with
          | Error error when Error.kind error = "workflow" -> ()
          | _ -> failwith "independent client's start was incorrectly deduplicated");
          let retry = start ~request_id:"explicit-start" a "-retry" in
          let same = get (Client.start b ~request_id:"explicit-start" ~workflow
            ~task_queue:queue ~id:(Client.workflow_id retry) ~input:0 ()) in
          if Option.get (Client.run_id retry) <> Option.get (Client.run_id same) then failwith "start retry changed run";
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
          check_by_id ~start:(fun suffix -> start a suffix)
            ~track:(fun handle -> chains := handle :: !chains; handle)
            address b queue;
          print_endline "client request ID live regression: ok"))))

(** The supplied URL must identify a disposable test namespace/server. *)
let () = match Array.to_list Sys.argv with
  | [_; "check"; address] -> check address
  | [_; "worker"; address; queue] -> worker address queue
  | [_; operation; address; id; run; amount; request_id] ->
      send address operation id run (int_of_string amount) request_id
  | _ -> failwith "usage: regression check http://localhost:7233"
