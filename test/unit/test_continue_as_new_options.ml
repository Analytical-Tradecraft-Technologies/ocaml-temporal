(** [Temporal.Testing] coverage for the continue-as-new successor options of
    #1153, through the public API only: a successor started with options
    reports them through [Workflow.Info] and [Workflow.start_metadata], a
    later successor without options inherits them (search attributes
    including upserts, the memo the run started with), and an explicit empty
    search-attribute list starts a successor with none. *)

module T = Temporal

(** Unwraps a successful result, failing the test with the error message. *)
let ok label = function
  | Ok value -> value
  | Error error -> failwith (label ^ " failed: " ^ T.Error.message error)

(** A keyword search attribute used by every stage. *)
let keyword = T.Search_attribute.keyword "CustomKeywordField"

(** Another keyword the first successor upserts before continuing. *)
let extra = T.Search_attribute.keyword "Extra"

(** Describes the current run as
    [queue/run timeout/task timeout/maximum attempts/memo keys/search-attribute
    keys], with [-] for an absent value and keys sorted. *)
let observe () =
  let info = ok "info" (T.Workflow.info ()) in
  let metadata = ok "start metadata" (T.Workflow.start_metadata ()) in
  let duration = function
    | None -> "-"
    | Some value -> Int64.to_string (T.Duration.to_ms value)
  in
  let keys = function
    | None | Some [] -> "-"
    | Some values -> String.concat "," (List.sort String.compare (List.map fst values))
  in
  String.concat "/"
    [ T.Workflow.Info.task_queue info;
      duration (T.Workflow.Info.run_timeout info);
      duration (T.Workflow.Info.task_timeout info);
      (match T.Workflow.Info.retry_policy info with
      | None -> "-"
      | Some policy -> string_of_int (T.Retry_policy.maximum_attempts policy));
      keys metadata.memo;
      keys metadata.search_attributes ]

(** The retry policy the first successor and its descendants run under. *)
let policy =
  ok "retry policy"
    (T.Retry_policy.make ~initial_interval:(T.Duration.of_ms 1_000L)
       ~backoff_coefficient:2.0 ~maximum_interval:(T.Duration.of_ms 10_000L)
       ~maximum_attempts:4 ())

(** A four-run chain. Its input is the observation log so far, one entry
    per earlier run separated by [;]; the stage is the number of entries. *)
let rec chain =
  lazy
    (T.Workflow.define ~name:"can_options_chain" ~input:T.Codec.string
       ~output:T.Codec.string (fun log ->
         let entries = if log = "" then [] else String.split_on_char ';' log in
         let next = String.concat ";" (entries @ [ observe () ]) in
         let definition = Lazy.force chain in
         match List.length entries with
         | 0 ->
             T.Workflow.continue_as_new ~task_queue:"successor-queue"
               ~run_timeout:(T.Duration.of_ms 60_000L)
               ~task_timeout:(T.Duration.of_ms 15_000L) ~retry_policy:policy
               ~memo:[ ("note", ok "memo" (T.Codec.encode T.Codec.string "first")) ]
               ~search_attributes:
                 (ok "encode" (T.Search_attribute.encode [ T.Search_attribute.set keyword "k" ]))
               definition next
         | 1 ->
             T.Workflow.upsert_typed_search_attributes [ T.Search_attribute.set extra "e" ];
             T.Workflow.continue_as_new definition next
         | 2 -> T.Workflow.continue_as_new ~search_attributes:[] definition next
         | _ -> Ok next))

(** Runs the chain in a fresh environment and checks every observation. *)
let test_options_carried_across_runs () =
  let workflow = Lazy.force chain in
  let environment =
    ok "create" (T.Testing.create ~workflows:[ T.Testing.workflow workflow ] ~activities:[] ())
  in
  Fun.protect
    ~finally:(fun () -> T.Testing.shutdown environment)
    (fun () ->
      let handle = ok "start" (T.Testing.start environment workflow "") in
      let log = ok "result" (T.Testing.result handle) in
      let expected =
        String.concat ";"
          [ "temporal-testing/-/-/-/-/-";
            "successor-queue/60000/15000/4/note/CustomKeywordField";
            "successor-queue/60000/15000/4/note/CustomKeywordField,Extra";
            "successor-queue/60000/15000/4/note/-" ]
      in
      if log <> expected then failwith ("unexpected chain observations: " ^ log))

(** An invalid option fails the workflow task rather than continuing; the
    test environment closes the run with the typed [Defect]. *)
let test_invalid_option_is_defect () =
  let workflow =
    T.Workflow.define ~name:"can_options_invalid" ~input:T.Codec.unit ~output:T.Codec.unit
      (fun () ->
        T.Workflow.continue_as_new ~task_timeout:(T.Duration.of_ms 121_000L)
          (T.Workflow.remote ~name:"can_options_invalid" ~input:T.Codec.unit
             ~output:T.Codec.unit)
          ())
  in
  let environment =
    ok "create" (T.Testing.create ~workflows:[ T.Testing.workflow workflow ] ~activities:[] ())
  in
  Fun.protect
    ~finally:(fun () -> T.Testing.shutdown environment)
    (fun () ->
      let handle = ok "start" (T.Testing.start environment workflow ()) in
      match T.Testing.result handle with
      | Error error when (T.Error.view error).category = `Defect -> ()
      | Error error -> failwith ("unexpected error: " ^ T.Error.message error)
      | Ok () -> failwith "an invalid task timeout continued as new")

(** Runs every check; a failure raises and fails the test. *)
let () =
  test_options_carried_across_runs ();
  test_invalid_option_is_defect ()
