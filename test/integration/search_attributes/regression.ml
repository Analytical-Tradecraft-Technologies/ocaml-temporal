(** Live regression for typed search attributes and [Workflow.upsert_memo]
    (#1154).

    Against a disposable Temporal server this check:

    - registers one search attribute of every Temporal indexed type through
      the official CLI when the [default] namespace does not have them yet;
    - starts a run with a memo and a [Text] attribute encoded by
      [Search_attribute.encode];
    - lets the run upsert typed [Keyword], [Int], [Double], [Datetime],
      [KeywordList] and [Bool] values, then unset the [Bool], and upsert its
      memo, adding a key and removing a start key with [binary/null];
    - requires a [list_visibility] query by the typed [Keyword] and [Int]
      values to find the run, and [count] to agree;
    - describes the open run: every typed value decodes with
      [Search_attribute.decode], the unset [Bool] is gone, and the memo is
      the merged one;
    - signals the run to continue as new, and requires the successor to
      start with the merged memo and the upserted keyword, and to report
      the merged memo through [describe] after it completes.

    The parent owns and reaps its worker, uses a unique task queue, and
    terminates every execution it starts. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the regression with a labelled message. *)
let fail label = failwith ("search attributes: " ^ label)

(** One registered attribute per indexed type, named for this suite. *)
let text = Search_attribute.text "TypedSaText"

let keyword = Search_attribute.keyword "TypedSaKeyword"
let int64 = Search_attribute.int64 "TypedSaInt"
let double = Search_attribute.double "TypedSaDouble"
let bool = Search_attribute.bool "TypedSaBool"
let datetime = Search_attribute.datetime "TypedSaDatetime"
let keyword_list = Search_attribute.keyword_list "TypedSaKeywordList"

(** The [temporal operator search-attribute create] arguments registering
    every attribute above. *)
let registrations =
  [
    ("TypedSaText", "Text"); ("TypedSaKeyword", "Keyword"); ("TypedSaInt", "Int");
    ("TypedSaDouble", "Double"); ("TypedSaBool", "Bool"); ("TypedSaDatetime", "Datetime");
    ("TypedSaKeywordList", "KeywordList");
  ]

(** The datetime the workflow upserts: 2024-01-01T00:00:00.123Z. *)
let instant = get (Time.of_unix ~seconds:1_704_067_200L ~nanoseconds:123_000_000)

(** Lets the first run continue as new. *)
let advance = Signal.define ~name:"advance" ~input:Codec.unit

(** Set by [advance]; an unset flag is false. *)
let advanced = Workflow_context.Local.create ()

(** Reads the [advanced] flag. *)
let is_advanced () =
  match Workflow_context.Local.get advanced with Ok (Some true) -> true | Ok _ | Error _ -> false

(** Encodes [value] with the string codec, the payload a memo entry holds. *)
let string_payload value = get (Codec.encode Codec.string value)

(** The [binary/null] payload that removes a memo key. *)
let removal = get (Codec.encode (Codec.option Codec.string) None)

(** The first run ([false] input) upserts typed search attributes and, when
    [upsert_memo] is set, its memo, waits for [advance], and continues as
    new. The successor ([true]) returns its start memo keys and its
    inherited keyword as ["key,key|keyword"]. The worker runs the
    [upsert_memo] variant; the other one only replays its history, to show
    that the recorded memo upsert is matched on replay. *)
let rec define_fixture ~upsert_memo =
  lazy
    (Workflow.define ~name:"typed-search-attributes" ~input:Codec.bool ~output:Codec.string
       (fun continued ->
         let open Result_syntax in
         if continued then
           let* metadata = Workflow.start_metadata () in
           let memo = Option.value metadata.memo ~default:[] in
           let* inherited =
             Search_attribute.decode keyword
               (Option.value metadata.search_attributes ~default:[])
           in
           Ok
             (String.concat "," (List.sort compare (List.map fst memo))
             ^ "|" ^ Option.value inherited ~default:"-")
         else
           let* info = Workflow.info () in
           let queue = Workflow.Info.task_queue info in
           let set = Search_attribute.set in
           Workflow.upsert_typed_search_attributes
             [
               set keyword queue; set int64 42L; set double 2.5; set datetime instant;
               set keyword_list [ "red"; "green" ]; set bool true;
             ];
           Workflow.upsert_typed_search_attributes [ Search_attribute.unset bool ];
           if upsert_memo then
             Workflow.upsert_memo [ ("note", string_payload "upserted"); ("removed", removal) ];
           let* () = Condition.wait_until is_advanced in
           Workflow.continue_as_new (Lazy.force (define_fixture ~upsert_memo)) true))

(** The workflow the worker runs. *)
let fixture = define_fixture ~upsert_memo:true

(** The same workflow without its memo upsert, used only for replay. *)
let without_memo = define_fixture ~upsert_memo:false

(** Signal handler registration of the worker. *)
let signals = [ Signal.Handler.make advance (fun () -> Workflow_context.Local.set advanced true) ]

(** Runs the fixture on the test's unique queue until killed. *)
let worker address queue =
  let worker =
    get
      (Worker.create ~target_url:address ~namespace:"default" ~task_queue:queue
         ~activities:[] ~workflows:[ Worker.workflow (Lazy.force fixture) ~signals ] ())
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

(** Registers every attribute in the [default] namespace unless all are
    already listed. Another run may register them concurrently, so a failed
    create is accepted when the attributes are listed afterwards. *)
let ensure_search_attributes cli address =
  let listed () =
    let output, ok =
      cli_run cli address
        [ "operator"; "search-attribute"; "list"; "--namespace"; "default"; "--output"; "json" ]
    in
    ok && List.for_all (fun (name, _) -> contains output ("\"" ^ name ^ "\"")) registrations
  in
  if not (listed ()) then (
    let missing =
      let output, _ =
        cli_run cli address
          [ "operator"; "search-attribute"; "list"; "--namespace"; "default"; "--output"; "json" ]
      in
      List.filter (fun (name, _) -> not (contains output ("\"" ^ name ^ "\""))) registrations
    in
    ignore
      (cli_run cli address
         ([ "operator"; "search-attribute"; "create"; "--namespace"; "default" ]
         @ List.concat_map (fun (name, kind) -> [ "--name"; name; "--type"; kind ]) missing));
    await "the search attributes to be registered" listed)

(** Starts the fixture with [memo] and [search_attributes], retrying while a
    freshly registered search attribute is still unknown to the frontend
    (reported as [`Invalid_argument]); nothing is created by such a refusal,
    so the retry reuses the same workflow ID. *)
let start_with_metadata client ~queue ~id ~memo ~search_attributes =
  let started = ref None in
  await "a start with the new search attributes" (fun () ->
      match
        Client.start client ~workflow:(Lazy.force fixture) ~task_queue:queue ~id ~input:false
          ~memo ~search_attributes ()
      with
      | Ok handle ->
          started := Some handle;
          true
      | Error error when Client.rpc_status error = Some `Invalid_argument -> false
      | Error error -> fail ("start failed: " ^ Error.message error));
  Option.get !started

(** Decodes [key] from [attributes], failing the regression on an error. *)
let decoded label key attributes =
  match Search_attribute.decode key attributes with
  | Ok value -> value
  | Error error ->
      fail (label ^ ": " ^ Search_attribute.name key ^ " did not decode: " ^ Error.message error)

(** Requires a memo with exactly [expected] keys and string values. *)
let expect_memo label expected (memo : (string * Payload.t) list) =
  let actual =
    List.map (fun (key, payload) -> (key, get (Codec.decode Codec.string payload))) memo
  in
  if List.sort compare actual <> List.sort compare expected then
    fail (label ^ ": the memo is not the merged memo")

(** Runs every check against [address] with one worker process; [cli] is the
    official Temporal CLI used only to register the search attributes. *)
let check address cli =
  let queue = "typed-search-" ^ Temporal_base.Client_request_id.create () in
  ensure_search_attributes cli address;
  let pid =
    Unix.create_process Sys.executable_name
      [| Sys.executable_name; "worker"; address; queue |]
      Unix.stdin Unix.stdout Unix.stderr
  in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  let cleanups = ref [] in
  Fun.protect
    ~finally:(fun () ->
      List.iter (fun cleanup -> cleanup ()) !cleanups;
      ignore (Client.shutdown client);
      (try Unix.kill pid Sys.sigterm with Unix.Unix_error _ -> ());
      ignore (Unix.waitpid [] pid))
    (fun () ->
      let id = queue ^ "-run" in
      let memo = [ ("kept", string_payload "start"); ("removed", string_payload "gone") ] in
      let search_attributes =
        get (Search_attribute.encode [ Search_attribute.set text "typed search text" ])
      in
      let handle = start_with_metadata client ~queue ~id ~memo ~search_attributes in
      let current = get (Client.get_handle client ~workflow:(Lazy.force fixture) ~id ()) in
      cleanups := (fun () -> ignore (Client.terminate current)) :: !cleanups;
      let query = Printf.sprintf "TypedSaKeyword = '%s' AND TypedSaInt = 42" queue in
      await "a visibility query by the typed keys to find the run" (fun () ->
          match Client.list_visibility client ~query () with
          | Ok { executions = [ row ]; _ } -> row.workflow_id = id
          | Ok _ -> false
          | Error error -> fail ("list_visibility failed: " ^ Error.message error));
      (match Client.count client ~query () with
      | Ok 1L -> ()
      | Ok _ -> fail "count disagrees with the visibility listing"
      | Error error -> fail ("count failed: " ^ Error.message error));
      let opened = get (Client.describe handle) in
      let attributes = opened.search_attributes in
      if decoded "open run" keyword attributes <> Some queue then fail "keyword did not round-trip";
      if decoded "open run" int64 attributes <> Some 42L then fail "int did not round-trip";
      if decoded "open run" double attributes <> Some 2.5 then fail "double did not round-trip";
      (match decoded "open run" datetime attributes with
      | Some value when Time.equal value instant -> ()
      | _ -> fail "datetime did not round-trip");
      if decoded "open run" keyword_list attributes <> Some [ "red"; "green" ] then
        fail "keyword list did not round-trip";
      if decoded "open run" text attributes <> Some "typed search text" then
        fail "the start text attribute did not round-trip";
      if List.mem_assoc (Search_attribute.name bool) attributes then
        fail "the unset bool is still present";
      expect_memo "open run" [ ("kept", "start"); ("note", "upserted") ] opened.memo;
      get (Client.signal handle ~signal:advance ~input:());
      (match Client.wait ~timeout:(Duration.of_seconds 30L) handle with
      | Ok (Client.Continued_as_new _) -> ()
      | Ok _ -> fail "the first run did not continue as new"
      | Error error -> fail ("waiting for the first run failed: " ^ Error.message error));
      (* The closed first run's history records both search-attribute
         upserts and the memo upsert. It replays against the fixture, and a
         version without the memo upsert is a nondeterminism error, so the
         recorded WorkflowPropertiesModified event is matched on replay. *)
      let history = get (Client.fetch_history handle) in
      let replay workflow =
        Replay.replay ~namespace:"default" ~task_queue:queue
          ~workflows:[ Replay.workflow (Lazy.force workflow) ~signals ]
          history
      in
      (match replay fixture with
      | Ok () -> ()
      | Error failure -> fail ("the first run did not replay: " ^ Replay.failure_message failure));
      (match replay without_memo with
      | Error (Replay.Nondeterminism _) -> ()
      | Ok () -> fail "a replay without the memo upsert was accepted"
      | Error failure ->
          fail ("a replay without the memo upsert failed with " ^ Replay.failure_message failure));
      (match Client.wait ~timeout:(Duration.of_seconds 30L) current with
      | Ok (Client.Completed { output; _ }) when output = "kept,note|" ^ queue -> ()
      | Ok (Client.Completed { output; _ }) ->
          fail ("the successor started with other metadata: " ^ output)
      | Ok _ -> fail "the successor did not complete"
      | Error error -> fail ("waiting for the successor failed: " ^ Error.message error));
      let closed = get (Client.describe current) in
      if Some closed.execution.run_id = Client.run_id handle then
        fail "the current-run handle still describes the first run";
      expect_memo "successor" [ ("kept", "start"); ("note", "upserted") ] closed.memo;
      if decoded "successor" keyword closed.search_attributes <> Some queue then
        fail "the successor did not inherit the keyword";
      print_endline "search attributes live regression: ok")

(** The supplied URL must identify a disposable test namespace/server. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address; cli ] -> check address cli
  | [ _; "worker"; address; queue ] -> worker address queue
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"
