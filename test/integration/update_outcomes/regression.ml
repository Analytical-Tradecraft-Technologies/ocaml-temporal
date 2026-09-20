(** Live client regression for rejected, suspended, and already-completed
    update admission outcomes. Only this fixture's own execution is deleted. *)
open Temporal

(** Reports failures through the public error view. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** The worker deliberately rejects non-numeric bytes during input decoding. *)
let number = Codec.make ~encoding:"json/plain"
  ~encode:(fun value -> Ok (Bytes.of_string (string_of_int value)))
  ~decode:(fun bytes -> match int_of_string_opt (Bytes.to_string bytes) with
    | Some value -> Ok value
    | None -> Error (Error.codec ~message:"invalid counter input"))

(** Workflow-local state separates admission from a signal-controlled result. *)
let state = Workflow_context.Local.create ()
let released = Workflow_context.Local.create ()

(** Reads the counter without creating commands. *)
let read () = Result.map (Option.value ~default:0) (Workflow_context.Local.get state)

(** Keeps the execution open so all client cases use the same worker state. *)
let workflow = Workflow.define ~name:"update-outcome-counter" ~input:number ~output:number
  (fun initial -> Result.bind (Workflow_context.Local.set state initial) (fun () ->
    Result.bind (Condition.wait_until (fun () -> false)) read))
let query = Query.define ~name:"get" ~output:number
let release = Signal.define ~name:"release" ~input:Codec.unit
let update = Update.define ~name:"add" ~input:number ~output:number

(** Rejection details must survive admission without a subsequent lookup. *)
let validator amount =
  if amount < 0 then Error (Error.make ~category:`Update ~non_retryable:true
    ~details:[get (Codec.encode Codec.string "negative amount")]
    ~message:"NEGATIVE_REJECTED" ())
  else Ok ()

(** Suspends after acceptance, proving that the client still supports polling. *)
let handler amount =
  Result.bind (Condition.wait_until (fun () ->
    Option.value ~default:false (get (Workflow_context.Local.get released))))
    (fun () -> Result.bind (read ()) (fun value ->
      Result.bind (Workflow_context.Local.set state (value + amount)) read))

(** A dedicated process serves the uniquely named test queue. *)
let worker address queue =
  let worker = get (Worker.create ~target_url:address ~namespace:"default"
    ~task_queue:queue ~activities:[] ~workflows:[Worker.workflow workflow
      ~queries:[Query.Handler.make query read]
      ~signals:[Signal.Handler.make release (fun () -> Workflow_context.Local.set released true)]
      ~updates:[Update.Handler.make ~validator update handler]] ()) in
  get (Worker.run worker)

(** Requires an admission failure, rather than an unusable accepted handle. *)
let rejected label = function
  | Error error when Error.kind error = "update" -> Error.view error
  | Error error -> failwith (label ^ ": wrong error: " ^ Error.message error)
  | Ok _ -> failwith (label ^ ": rejected update was reported as accepted")

(** Invokes the official CLI only for this fixture's exact execution. *)
let delete_execution cli address handle =
  let address = String.sub address 7 (String.length address - 7) in
  let args = [|cli; "--address"; address; "workflow"; "delete"; "--yes";
    "--workflow-id"; Client.workflow_id handle;
    "--run-id"; Client.run_id handle|] in
  let pid = Unix.create_process cli args Unix.stdin Unix.stdout Unix.stderr in
  match snd (Unix.waitpid [] pid) with
  | Unix.WEXITED 0 -> ()
  | _ -> failwith "fixture execution deletion failed"

(** Waits for asynchronous deletion to invalidate a handle whose admission
    outcome was pending. A separate completed handle must still work locally. *)
let rec await_deleted pending attempts =
  match Client.wait_update pending with
  | Error error when Error.kind error = "bridge"
      && String.ends_with ~suffix:"not_found" (Error.message error) -> ()
  | _ when attempts > 0 -> Unix.sleepf 0.05; await_deleted pending (attempts - 1)
  | _ -> failwith "fixture execution was not deleted within 10 seconds"

(** Covers rejection fields, unknown handlers, input decoding, suspended
    acceptance, and cached successful outcomes after their server record is gone. *)
let check address cli =
  if not (String.starts_with ~prefix:"http://" address) then
    failwith "this fixture requires an HTTP development server";
  let queue = Printf.sprintf "update-outcomes-%d-%d" (Unix.getpid ())
    (Random.State.bits (Random.State.make_self_init ())) in
  let pid = Unix.create_process Sys.executable_name
    [|Sys.executable_name; "worker"; address; queue|]
    Unix.stdin Unix.stdout Unix.stderr in
  Fun.protect ~finally:(fun () -> Unix.kill pid Sys.sigterm; ignore (Unix.waitpid [] pid))
    (fun () ->
      let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
      Fun.protect ~finally:(fun () -> ignore (Client.shutdown client)) (fun () ->
        let handle = get (Client.start client ~workflow ~task_queue:queue ~id:queue ~input:0 ()) in
        Fun.protect ~finally:(fun () -> ignore (Client.terminate handle)) (fun () ->
          let failure = rejected "validator" (Client.start_update ~update_id:"negative"
            handle ~update ~input:(-1) ()) in
          (* The public backend appends bounded source/type diagnostics. *)
          if not (String.starts_with ~prefix:"NEGATIVE_REJECTED " failure.message)
             || not failure.non_retryable then
            failwith (Printf.sprintf "validator failure fields changed: message=%S non_retryable=%b"
              failure.message failure.non_retryable);
          (match failure.details with
          | [payload] when get (Codec.decode Codec.string payload) = "negative amount" -> ()
          | _ -> failwith "validator failure details changed");
          let unknown = Update.define ~name:"missing" ~input:number ~output:number in
          ignore (rejected "unknown handler" (Client.start_update ~update_id:"unknown"
            handle ~update:unknown ~input:1 ()));
          let malformed = Update.define ~name:"add" ~input:Codec.string ~output:number in
          ignore (rejected "input decode" (Client.start_update ~update_id:"malformed"
            handle ~update:malformed ~input:"not-a-number" ()));
          if get (Client.query handle ~query) <> 0 then failwith "rejection mutated state";
          let pending = get (Client.start_update ~update_id:"suspended" handle ~update ~input:7 ()) in
          if get (Client.query handle ~query) <> 0 then failwith "update did not suspend";
          get (Client.signal ~request_id:"release" handle ~signal:release ~input:());
          if get (Client.wait_update pending) <> 7 then failwith "suspended result changed";
          let completed = get (Client.start_update ~update_id:"suspended" handle ~update ~input:7 ()) in
          delete_execution cli address handle;
          await_deleted pending 200;
          for _ = 1 to 2 do
            if get (Client.wait_update completed) <> 7 then failwith "cached outcome changed"
          done;
          print_endline "update admission outcome live regression: ok")))

(** The CLI path is explicit so test execution cannot invoke an unrelated wrapper. *)
let () = match Array.to_list Sys.argv with
  | [_; "worker"; address; queue] -> worker address queue
  | [_; "check"; address; cli] -> check address cli
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"
