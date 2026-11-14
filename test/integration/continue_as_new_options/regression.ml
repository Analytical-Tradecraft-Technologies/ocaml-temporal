(** Live regression for [Workflow.continue_as_new] successor options
    (#1153).

    Against a disposable Temporal server this check:

    - registers the [ContinueAsNewOptionsKeyword] search attribute through
      the official CLI when the [default] namespace does not have it yet;
    - starts a run on queue A with a start memo; it continues as new with a
      task queue B, a run timeout, a task timeout, a retry policy, a
      replacement memo and search attributes;
    - requires the successor to be routed to the worker on queue B and to
      report every option through [Workflow.Info] and
      [Workflow.start_metadata] (read by a query), and [Client.describe] to
      report queue B, the replacement memo and the keyword;
    - continues that successor as new without options and requires the next
      run to inherit the queue, timeouts, retry policy, memo and search
      attributes;
    - continues once more with a 2 s run timeout and a single-attempt retry
      policy, and requires the last run to end as [Timed_out] without a
      retry;
    - replays the recorded histories of the runs that continued with and
      without options.

    The parent owns and reaps both workers, uses unique task queues, and
    terminates the execution it starts. *)
open Temporal

(** Keeps setup failures readable at the public error boundary. *)
let get = function Ok value -> value | Error error -> failwith (Error.message error)

(** Fails the regression with a labelled message. *)
let fail label = failwith ("continue-as-new options: " ^ label)

(** The keyword search attribute the first successor receives. *)
let keyword = Search_attribute.keyword "ContinueAsNewOptionsKeyword"

(** Lets a waiting run continue as new. *)
let advance = Signal.define ~name:"advance" ~input:Codec.unit

(** Reports the current run's routing and start metadata. *)
let observe_query = Query.define ~name:"observe" ~output:Codec.string

(** Set by [advance]; an unset flag is false. *)
let advanced = Workflow_context.Local.create ()

(** Reads the [advanced] flag. *)
let is_advanced () =
  match Workflow_context.Local.get advanced with Ok (Some true) -> true | Ok _ | Error _ -> false

(** Encodes [value] with the string codec, the payload a memo entry holds. *)
let string_payload value = get (Codec.encode Codec.string value)

(** Describes the current run as
    [queue/run timeout ms/task timeout ms/maximum attempts/memo/keyword],
    with [-] for an absent value; the memo is its sorted [key=value]
    entries. *)
let observe () =
  let open Result_syntax in
  let* info = Workflow.info () in
  let* metadata = Workflow.start_metadata () in
  let duration = function
    | None -> "-"
    | Some value -> Int64.to_string (Duration.to_ms value)
  in
  let memo =
    match metadata.memo with
    | None | Some [] -> "-"
    | Some entries ->
        String.concat ","
          (List.sort compare
             (List.map
                (fun (key, payload) ->
                  key ^ "="
                  ^ (match Codec.decode Codec.string payload with
                    | Ok value -> value
                    | Error _ -> "?"))
                entries))
  in
  let* inherited =
    Search_attribute.decode keyword (Option.value metadata.search_attributes ~default:[])
  in
  Ok
    (String.concat "/"
       [ Workflow.Info.task_queue info;
         duration (Workflow.Info.run_timeout info);
         duration (Workflow.Info.task_timeout info);
         (match Workflow.Info.retry_policy info with
         | None -> "-"
         | Some policy -> string_of_int (Retry_policy.maximum_attempts policy));
         memo;
         Option.value inherited ~default:"-" ])

(** A retry policy with [attempts] maximum attempts. *)
let policy attempts =
  get
    (Retry_policy.make ~initial_interval:(Duration.of_seconds 1L) ~backoff_coefficient:2.0
       ~maximum_interval:(Duration.of_seconds 10L) ~maximum_attempts:attempts ())

(** Encodes a fixture input as [stage:successor queue]. The stage is split
    off at the first colon, so the queue name may itself contain colons. *)
let input stage queue = Printf.sprintf "%d:%s" stage queue

(** Splits an [input] back into its stage and successor queue. *)
let parse_input text =
  let colon = String.index text ':' in
  ( int_of_string (String.sub text 0 colon),
    String.sub text (colon + 1) (String.length text - colon - 1) )

(** The input is [input stage successor_queue]. Stage 0 continues as new at
    once with every option; stages 1 and 2 wait for [advance], then stage 1
    continues without options and stage 2 with a 2 s run timeout and a
    single-attempt policy; stage 3 waits forever, so its run times out. *)
let rec fixture =
  lazy
    (Workflow.define ~name:"continue-as-new-options"
       ~input:Codec.string ~output:Codec.unit (fun text ->
         let open Result_syntax in
         let stage, successor_queue = parse_input text in
         let self = Lazy.force fixture in
         match stage with
         | 0 ->
             Workflow.continue_as_new ~task_queue:successor_queue
               ~run_timeout:(Duration.of_seconds 60L) ~task_timeout:(Duration.of_seconds 15L)
               ~retry_policy:(policy 5)
               ~memo:[ ("note", string_payload "replaced") ]
               ~search_attributes:
                 (get (Search_attribute.encode [ Search_attribute.set keyword successor_queue ]))
               self (input 1 successor_queue)
         | 1 ->
             let* () = Condition.wait_until is_advanced in
             Workflow.continue_as_new self (input 2 successor_queue)
         | 2 ->
             let* () = Condition.wait_until is_advanced in
             Workflow.continue_as_new ~run_timeout:(Duration.of_seconds 2L)
               ~retry_policy:(policy 1) self (input 3 successor_queue)
         | _ ->
             let* () = Condition.wait_until (fun () -> false) in
             Ok ()))

(** Signal registration of the workers. *)
let signals = [ Signal.Handler.make advance (fun () -> Workflow_context.Local.set advanced true) ]

(** Query registration of the workers. *)
let queries = [ Query.Handler.make observe_query observe ]

(** Runs the fixture on [queue] until killed. *)
let worker address queue =
  let worker =
    get
      (Worker.create ~target_url:address ~namespace:"default" ~task_queue:queue
         ~activities:[]
         ~workflows:[ Worker.workflow (Lazy.force fixture) ~signals ~queries ]
         ())
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
    (registration, a successor's first workflow task). *)
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

(** Registers the keyword in the [default] namespace unless it is listed.
    Another run may register it concurrently, so a failed create is accepted
    when the attribute is listed afterwards. *)
let ensure_search_attribute cli address =
  let name = Search_attribute.name keyword in
  let listed () =
    let output, ok =
      cli_run cli address
        [ "operator"; "search-attribute"; "list"; "--namespace"; "default"; "--output"; "json" ]
    in
    ok && contains output ("\"" ^ name ^ "\"")
  in
  if not (listed ()) then (
    ignore
      (cli_run cli address
         [ "operator"; "search-attribute"; "create"; "--namespace"; "default"; "--name"; name;
           "--type"; "Keyword" ]);
    await "the search attribute to be registered" listed)

(** Waits for the exact run [handle] to continue as new and returns an
    exact handle on its successor. *)
let successor client handle label =
  match Client.wait ~timeout:(Duration.of_seconds 30L) handle with
  | Ok (Client.Continued_as_new successor) ->
      get
        (Client.get_handle client ~run_id:successor.run_id ~workflow:(Lazy.force fixture)
           ~id:successor.workflow_id ())
  | Ok _ -> fail (label ^ " did not continue as new")
  | Error error -> fail ("waiting for " ^ label ^ " failed: " ^ Error.message error)

(** Queries [handle] until its first workflow task has run and requires the
    observation [expected]. *)
let expect_observation handle label expected =
  let observed = ref "" in
  await (label ^ " to answer a query") (fun () ->
      match Client.query handle ~query:observe_query with
      | Ok value ->
          observed := value;
          true
      | Error _ -> false);
  if !observed <> expected then
    fail (Printf.sprintf "%s observed %s, expected %s" label !observed expected)

(** Requires [Client.describe] to report [queue], the replacement memo and
    the keyword [queue] for the run [handle] addresses. *)
let expect_description handle label queue =
  let description = get (Client.describe handle) in
  if description.task_queue <> queue then fail (label ^ " was described on another task queue");
  (match description.memo with
  | [ ("note", payload) ] when get (Codec.decode Codec.string payload) = "replaced" -> ()
  | _ -> fail (label ^ " was described with another memo"));
  match Search_attribute.decode keyword description.search_attributes with
  | Ok (Some value) when value = queue -> ()
  | Ok _ -> fail (label ^ " was described without the keyword")
  | Error error -> fail (label ^ ": the keyword did not decode: " ^ Error.message error)

(** Requires the closed run [handle] to replay against the fixture. *)
let expect_replay handle label queue =
  let history = get (Client.fetch_history handle) in
  match
    Replay.replay ~namespace:"default" ~task_queue:queue
      ~workflows:[ Replay.workflow (Lazy.force fixture) ~signals ~queries ]
      history
  with
  | Ok () -> ()
  | Error failure -> fail (label ^ " did not replay: " ^ Replay.failure_message failure)

(** Runs every check against [address] with one worker per queue; [cli] is
    the official Temporal CLI used only to register the search attribute. *)
let check address cli =
  let base = "can-options-" ^ Temporal_base.Client_request_id.create () in
  let first_queue = base ^ "-a" and successor_queue = base ^ "-b" in
  ensure_search_attribute cli address;
  let spawn queue =
    Unix.create_process Sys.executable_name
      [| Sys.executable_name; "worker"; address; queue |]
      Unix.stdin Unix.stdout Unix.stderr
  in
  let pids = [ spawn first_queue; spawn successor_queue ] in
  let client = get (Client.create ~target_url:address ~namespace:"default" ()) in
  let id = base ^ "-run" in
  Fun.protect
    ~finally:(fun () ->
      (match Client.get_handle client ~workflow:(Lazy.force fixture) ~id () with
      | Ok current -> ignore (Client.terminate current)
      | Error _ -> ());
      ignore (Client.shutdown client);
      List.iter
        (fun pid ->
          (try Unix.kill pid Sys.sigterm with Unix.Unix_error _ -> ());
          ignore (Unix.waitpid [] pid))
        pids)
    (fun () ->
      let first =
        get
          (Client.start client ~workflow:(Lazy.force fixture) ~task_queue:first_queue ~id
             ~memo:[ ("kept", string_payload "start") ]
             ~input:(input 0 successor_queue) ())
      in
      let configured = successor client first "the first run" in
      let expected =
        Printf.sprintf "%s/60000/15000/5/note=replaced/%s" successor_queue successor_queue
      in
      expect_observation configured "the configured successor" expected;
      expect_description configured "the configured successor" successor_queue;
      get (Client.signal configured ~signal:advance ~input:());
      let inherited = successor client configured "the configured successor" in
      expect_observation inherited "the inheriting successor" expected;
      expect_description inherited "the inheriting successor" successor_queue;
      get (Client.signal inherited ~signal:advance ~input:());
      let bounded = successor client inherited "the inheriting successor" in
      (match Client.wait ~timeout:(Duration.of_seconds 30L) bounded with
      | Ok (Client.Timed_out { successor = None; _ }) -> ()
      | Ok (Client.Timed_out { successor = Some _; _ }) ->
          fail "the bounded run was retried despite its single-attempt policy"
      | Ok _ -> fail "the bounded run did not time out"
      | Error error -> fail ("waiting for the bounded run failed: " ^ Error.message error));
      expect_replay first "the run that continued with options" first_queue;
      expect_replay configured "the run that continued without options" successor_queue;
      expect_replay inherited "the run that set a run timeout" successor_queue;
      print_endline "continue-as-new options live regression: ok")

(** The supplied URL must identify a disposable test namespace/server. *)
let () =
  match Array.to_list Sys.argv with
  | [ _; "check"; address; cli ] -> check address cli
  | [ _; "worker"; address; queue ] -> worker address queue
  | _ -> failwith "usage: regression check http://localhost:7233 /path/to/temporal"
