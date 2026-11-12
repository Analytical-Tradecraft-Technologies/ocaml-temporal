(** Live regression for [Client.describe] and [Client.count] (#1152).

    Against a disposable Temporal server this check:

    - registers a Keyword search attribute through the official CLI when the
      namespace does not have it yet, then starts a run with a memo and that
      search attribute;
    - describes the open run, exact and through a current-run handle: its
      identity, type, queue, [`Running] status, start time, history length,
      memo and search attribute must match what [start] sent, with no close
      time;
    - releases it and describes it again after it completes: [`Completed]
      with a close time no earlier than its start and a longer history;
    - describes an unknown workflow and an unknown run of a known workflow,
      which are [Not_found] RPC errors;
    - seeds two more runs on its own unique task queue, terminates one, and
      requires [count] to equal the number of rows [list_visibility] pages
      through for the queue, for all three runs and for the running one
      alone, once visibility has caught up.

    The parent owns and reaps its worker, uses a unique task queue, and
    terminates every execution it starts. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the regression with a labelled message. *)
let fail label = failwith ("describe/count: " ^ label)

(** The Keyword search attribute this regression registers when absent. *)
let search_attribute = "DescribeCountKeyword"

(** Signal that lets the fixture complete. *)
let release = Signal.define ~name:"release" ~input:Codec.unit

(** Set by [release]; an unset flag is false. *)
let released = Workflow_context.Local.create ()

(** Reads the [released] flag. *)
let is_released () =
  match Workflow_context.Local.get released with Ok (Some true) -> true | Ok _ | Error _ -> false

(** Waits for [release] and completes with its input, so a run stays open
    until the check releases it. *)
let fixture =
  Workflow.define ~name:"describe-count" ~input:Codec.string ~output:Codec.string
    (fun input ->
      let open Result_syntax in
      let* () = Condition.wait_until is_released in
      Ok input)

(** Signal handler registration of the worker. *)
let signals = [ Signal.Handler.make release (fun () -> Workflow_context.Local.set released true) ]

(** Runs the fixture on the test's unique queue until killed. *)
let worker address queue =
  let worker =
    get
      (Worker.create ~target_url:address ~namespace:"default" ~task_queue:queue
         ~activities:[] ~workflows:[ Worker.workflow fixture ~signals ] ())
  in
  get (Worker.run worker)

(** Runs the official CLI against the server and returns its standard output
    and whether it exited successfully. *)
let cli_run cli address args =
  let address = String.sub address 7 (String.length address - 7) in
  let argv = Array.of_list (cli :: "--address" :: address :: args) in
  let channel = Unix.open_process_args_in cli argv in
  let output = In_channel.input_all channel in
  (output, Unix.close_process_in channel = Unix.WEXITED 0)

(** Reports whether [needle] occurs in [haystack]. *)
let contains haystack needle =
  let length = String.length needle in
  let rec search index =
    index + length <= String.length haystack
    && (String.sub haystack index length = needle || search (index + 1))
  in
  search 0

(** Retries [attempt] for up to 30 s, for state that appears asynchronously
    (search-attribute registration and visibility). *)
let await label attempt =
  let deadline = Unix.gettimeofday () +. 30. in
  let rec poll () =
    if attempt () then ()
    else if Unix.gettimeofday () < deadline then (
      Unix.sleepf 0.25;
      poll ())
    else fail ("timed out waiting for " ^ label)
  in
  poll ()

(** Registers [search_attribute] as a Keyword in the [default] namespace
    unless it already is. Another run may register it concurrently, so a
    failed create is accepted when the attribute is listed afterwards. *)
let ensure_search_attribute cli address =
  let listed () =
    let output, ok =
      cli_run cli address
        [ "operator"; "search-attribute"; "list"; "--namespace"; "default"; "--output"; "json" ]
    in
    ok && contains output search_attribute
  in
  if not (listed ()) then (
    ignore
      (cli_run cli address
         [
           "operator"; "search-attribute"; "create"; "--namespace"; "default"; "--name";
           search_attribute; "--type"; "Keyword";
         ]);
    await "the search attribute to be registered" listed)

(** Encodes [value] with the string codec, the payload a Keyword search
    attribute and a memo entry both accept. *)
let string_payload value = get (Codec.encode Codec.string value)

(** Starts the fixture with [memo] and [search_attributes], retrying while a
    freshly registered search attribute is still unknown to the frontend
    (reported as [`Invalid_argument]); nothing is created by such a refusal,
    so the retry reuses the same workflow ID. *)
let start_with_metadata client ~queue ~id ~memo ~search_attributes =
  let started = ref None in
  await "a start with the new search attribute" (fun () ->
      match
        Client.start client ~workflow:fixture ~task_queue:queue ~id ~input:"done" ~memo
          ~search_attributes ()
      with
      | Ok handle ->
          started := Some handle;
          true
      | Error error when Client.rpc_status error = Some `Invalid_argument -> false
      | Error error -> fail ("start failed: " ^ Error.message error));
  Option.get !started

(** Requires [actual] to hold [key] with the data of [expected]. The server
    adds a [type] metadata entry to search-attribute payloads, so only the
    data is compared for them; memo payloads are compared whole. *)
let expect_data label actual (key, (expected : Payload.t)) =
  match List.assoc_opt key actual with
  | Some (payload : Payload.t) when Bytes.equal payload.data expected.data -> ()
  | Some _ -> fail (label ^ ": " ^ key ^ " has other data")
  | None -> fail (label ^ ": " ^ key ^ " is missing")

(** Pages through every visibility row [query] matches and returns how many
    there are. *)
let list_total client query =
  let rec pages token total =
    let page = get (Client.list_visibility ?page_token:token ~page_size:2 client ~query ()) in
    let total = total + List.length page.executions in
    match page.next_page_token with None -> total | Some _ as token -> pages token total
  in
  pages None 0

(** Waits until [count] reports [expected] for [query] and requires the
    visibility listing to agree. *)
let expect_count client ~label ~query expected =
  await (label ^ " to reach " ^ Int64.to_string expected) (fun () ->
      match Client.count client ~query () with
      | Ok count -> Int64.equal count expected && list_total client query = Int64.to_int expected
      | Error error -> fail (label ^ ": count failed: " ^ Error.message error))

(** A well-formed run ID that no run has. Temporal rejects a malformed run ID
    as [Invalid_argument] rather than [Not_found], so the 32 hex digits of a
    fresh 128-bit request ID are laid out as a UUID. *)
let fresh_run_id () =
  let id = Temporal_base.Client_request_id.create () in
  let hex = String.sub id (String.length id - 32) 32 in
  String.concat "-"
    [
      String.sub hex 0 8; String.sub hex 8 4; String.sub hex 12 4; String.sub hex 16 4;
      String.sub hex 20 12;
    ]

(** Requires [result] to be a [Not_found] RPC error. *)
let expect_not_found label = function
  | Error error when Client.rpc_status error = Some `Not_found -> ()
  | Error error -> fail (label ^ " failed with " ^ Error.message error)
  | Ok _ -> fail (label ^ " was described")

(** Runs every check against [address] with one worker process; [cli] is the
    official Temporal CLI used only to register the search attribute. *)
let check address cli =
  let queue = "describe-count-" ^ Temporal_base.Client_request_id.create () in
  ensure_search_attribute cli address;
  let pid =
    Unix.create_process Sys.executable_name
      [| Sys.executable_name; "worker"; address; queue |]
      Unix.stdin Unix.stdout Unix.stderr
  in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  let cleanups = ref [] in
  let track handle =
    cleanups := (fun () -> ignore (Client.terminate handle)) :: !cleanups;
    handle
  in
  Fun.protect
    ~finally:(fun () ->
      List.iter (fun cleanup -> cleanup ()) !cleanups;
      ignore (Client.shutdown client);
      (try Unix.kill pid Sys.sigterm with Unix.Unix_error _ -> ());
      ignore (Unix.waitpid [] pid))
    (fun () ->
      let memo = [ ("note", string_payload "memo-value") ] in
      let search_attributes = [ (search_attribute, string_payload queue) ] in
      let id = queue ^ "-described" in
      let handle =
        track (start_with_metadata client ~queue ~id ~memo ~search_attributes)
      in
      let run_id = Option.get (Client.run_id handle) in
      let check_open label (described : Client.execution_description) =
        if described.execution <> { namespace = "default"; workflow_id = id; run_id } then
          fail (label ^ ": another execution was described");
        if described.workflow_type <> "describe-count" || described.task_queue <> queue then
          fail (label ^ ": wrong workflow type or task queue");
        if described.status <> `Running then fail (label ^ ": the open run is not running");
        if described.close_time <> None then fail (label ^ ": the open run has a close time");
        if Int64.compare described.history_length 1L < 0 then
          fail (label ^ ": the open run has no history");
        if described.memo <> memo then fail (label ^ ": the memo did not round-trip");
        List.iter (expect_data label described.search_attributes) search_attributes;
        if described.pending_activities <> [] || described.pending_children <> [] then
          fail (label ^ ": the fixture has no pending work")
      in
      let opened = get (Client.describe handle) in
      check_open "open run" opened;
      let current = get (Client.get_handle client ~workflow:fixture ~id ()) in
      check_open "current-run handle" (get (Client.describe current));
      get (Client.signal handle ~signal:release ~input:());
      (match Client.wait ~timeout:(Duration.of_seconds 30L) handle with
      | Ok (Client.Completed { output = "done"; _ }) -> ()
      | Ok _ -> fail "the fixture did not complete with its input"
      | Error error -> fail ("waiting for the fixture failed: " ^ Error.message error));
      let closed = get (Client.describe handle) in
      if closed.status <> `Completed then fail "the completed run is not completed";
      (match closed.close_time with
      | Some close_time when Time.compare opened.start_time close_time <= 0 -> ()
      | Some _ -> fail "the run closed before it started"
      | None -> fail "the completed run has no close time");
      if Int64.compare closed.history_length opened.history_length <= 0 then
        fail "the completed run's history did not grow";
      if closed.memo <> memo then fail "the closed run's memo did not round-trip";
      expect_not_found "an unknown workflow"
        (Client.describe (get (Client.get_handle client ~workflow:fixture ~id:(queue ^ "-missing") ())));
      expect_not_found "an unknown run"
        (Client.describe
           (get
              (Client.get_handle client ~workflow:fixture ~id
                 ~run_id:(fresh_run_id ())
                 ())));
      let seed suffix =
        track
          (get
             (Client.start client ~workflow:fixture ~task_queue:queue ~id:(queue ^ suffix)
                ~input:"seed" ()))
      in
      let _running = seed "-running" in
      get (Client.terminate (seed "-terminated"));
      let by_queue = Printf.sprintf "TaskQueue = '%s'" queue in
      expect_count client ~label:"every seeded run" ~query:by_queue 3L;
      expect_count client ~label:"the running run"
        ~query:(by_queue ^ " AND ExecutionStatus = 'Running'")
        1L;
      print_endline "describe/count live regression: ok")

(** The supplied URL must identify a disposable test namespace/server. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address; cli ] -> check address cli
  | [ _; "worker"; address; queue ] -> worker address queue
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"
