(* Runtime coverage for the continue-as-new successor options of #1153: the
   command emitted without options is exactly the one earlier releases
   emitted, supplied options reach the runtime command in exact form, an
   explicit memo replaces the upserted one, invalid options fail only the
   workflow task, and continue-as-new is rejected in read-only callbacks. *)

module Activation = Temporal_runtime.Activation
module Protocol = Temporal_protocol.Workflow_protocol
module Raw_execution = Temporal_runtime.Execution
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Copies a public payload into the private representation used by the
    low-level execution state machine. *)
let base_payload (payload : Temporal.Payload.t) : Temporal_base.Payload.t =
  { Temporal_base.Payload.metadata = payload.metadata; data = Bytes.copy payload.data }

(** Copies a private payload back into the public representation for codec
    callbacks. *)
let public_payload (payload : Temporal_base.Payload.t) : Temporal.Payload.t =
  { Temporal.Payload.metadata = payload.metadata; data = Bytes.copy payload.data }

(** Converts a public error into the private error type, keeping its
    category and message. *)
let base_error (error : Temporal.Error.t) : Temporal_base.Error.t =
  let view = Temporal.Error.view error in
  Temporal_base.Error.make ~category:view.category ~message:view.message ()

(** Adapts a public codec to the base codec of the private definition. *)
let base_codec (codec : 'a Temporal.Codec.t) : 'a Temporal_base.Codec.t =
  Temporal_base.Codec.of_payload
    ~encode:(fun value ->
      Result.map base_payload (Result.map_error base_error (Temporal.Codec.encode codec value)))
    ~decode:(fun payload ->
      Result.map_error base_error (Temporal.Codec.decode codec (public_payload payload)))

(** Rebuilds a public workflow definition as the base definition accepted by
    [Raw_execution.start]; a test-only conversion. *)
let base_workflow (definition : ('input, 'output) Temporal.Workflow.t) =
  let implementation =
    Option.map
      (fun implementation input -> Result.map_error base_error (implementation input))
      (Temporal.Workflow.implementation definition)
  in
  Temporal_base.Definition.make ~name:(Temporal.Workflow.name definition)
    ~input:(base_codec (Temporal.Workflow.input definition))
    ~output:(base_codec (Temporal.Workflow.output definition)) ~implementation

(** A [json/plain] payload holding [text] as a JSON string. *)
let text_payload text =
  match Temporal.Codec.encode Temporal.Codec.string text with
  | Ok payload -> payload
  | Error error -> failwith (Temporal.Error.message error)

(** The successor every scenario continues into. *)
let successor =
  Temporal.Workflow.remote ~name:"can_options_successor" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.unit

(** Run metadata recorded with an execution timeout of [execution_ms] and a
    run timeout of [run_ms] (both optional), as a start event reports them. *)
let run_info ?execution_ms ?run_ms () =
  let duration milliseconds : Protocol.duration =
    { seconds = Int64.div milliseconds 1000L;
      nanoseconds = Int64.to_int (Int64.rem milliseconds 1000L) * 1_000_000 }
  in
  {
    Workflow_context_store.workflow_id = "can-options";
    run_id = "can-options-run";
    workflow_type = "can_options";
    attempt = 1;
    first_execution_run_id = "can-options-run";
    parent = None;
    start_time = None;
    execution_timeout = Option.map duration execution_ms;
    run_timeout = Option.map duration run_ms;
    task_timeout = None;
    retry_policy = None;
    continued_from = None;
  }

(** Runs [body] as the first activation of a fresh execution with [info]
    installed and returns the emitted commands and any task failure. *)
let run ?info body =
  let source =
    Temporal.Workflow.define ~name:"can_options" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit body
  in
  let execution = Raw_execution.start (base_workflow source) () in
  Raw_execution.set_run_info execution info;
  let commands = Raw_execution.activate execution [ Activation.Start_workflow ] in
  (commands, Raw_execution.task_failure execution)

(** Without options the command carries no successor settings and an empty
    memo, exactly as before #1153, so existing histories replay unchanged. *)
let test_no_options_is_unchanged () =
  match run (fun () -> Temporal.Workflow.continue_as_new successor ()) with
  | [ Activation.Continue_as_new { workflow_type = "can_options_successor"; input = []; memo = []; options } ], None
    when options = Activation.Continue_as_new_options.none ->
      ()
  | _ -> failwith "continue-as-new without options changed its command"

(** Every option reaches the command in exact form: timeouts in
    milliseconds, the retry policy's runtime record, copied payload maps, and
    the versioning behavior. An explicit memo replaces an upserted one, and
    an empty search-attribute list is kept as an explicit clear. *)
let test_options_reach_command () =
  let policy =
    match
      Temporal.Retry_policy.make ~initial_interval:(Temporal.Duration.of_ms 500L)
        ~backoff_coefficient:2.0 ~maximum_interval:(Temporal.Duration.of_ms 10_000L)
        ~maximum_attempts:4 ()
    with
    | Ok policy -> policy
    | Error error -> failwith (Temporal.Error.message error)
  in
  let body () =
    Temporal.Workflow.upsert_memo [ ("upserted", text_payload "u") ];
    Temporal.Workflow.continue_as_new ~task_queue:"successor-queue"
      ~run_timeout:(Temporal.Duration.of_ms 60_000L)
      ~task_timeout:(Temporal.Duration.of_ms 15_000L) ~retry_policy:policy
      ~memo:[ ("explicit", text_payload "e") ]
      ~search_attributes:[ ("CustomKeywordField", text_payload "k") ]
      ~initial_versioning_behavior:`Auto_upgrade successor ()
  in
  (match run ~info:(run_info ~execution_ms:120_000L ()) body with
  | [ Activation.Modify_workflow_properties _;
      Activation.Continue_as_new { memo = [ ("explicit", memo) ]; options; _ } ], None ->
      if public_payload memo <> text_payload "e" then failwith "explicit memo bytes changed";
      if options.task_queue <> Some "successor-queue"
         || options.run_timeout <> Some 60_000L
         || options.task_timeout <> Some 15_000L
         || options.initial_versioning_behavior <> Some Protocol.Successor_auto_upgrade
      then failwith "successor routing, timeout or versioning options were not kept";
      (match options.retry_policy with
      | Some { maximum_attempts = 4; initial_interval = 500L; _ } -> ()
      | _ -> failwith "successor retry policy was not kept");
      (match options.search_attributes with
      | Some [ ("CustomKeywordField", value) ] when public_payload value = text_payload "k" -> ()
      | _ -> failwith "successor search attributes were not kept")
  | _ -> failwith "continue-as-new with options emitted an unexpected command");
  (* An empty memo means "omitted": the upserted memo is still carried. An
     empty search-attribute list is an explicit clear. *)
  match
    run (fun () ->
        Temporal.Workflow.upsert_memo [ ("upserted", text_payload "u") ];
        Temporal.Workflow.continue_as_new ~memo:[] ~search_attributes:[] successor ())
  with
  | [ _; Activation.Continue_as_new { memo = [ ("upserted", _) ]; options; _ } ], None
    when options.search_attributes = Some [] ->
      ()
  | _ -> failwith "empty memo or search-attribute options were misread"

(** An invalid option fails only the workflow task with a [Defect] that
    names it, before any command is emitted. A run timeout is checked
    against the chain's recorded execution timeout and a task timeout
    against the inherited run timeout, because Temporal would silently lower
    either. *)
let test_invalid_options_fail_task () =
  let seconds value = Temporal.Duration.of_ms (Int64.mul value 1000L) in
  let cases =
    [ ("empty task queue", None,
       fun () -> Temporal.Workflow.continue_as_new ~task_queue:"" successor ());
      ("zero run timeout", None,
       fun () -> Temporal.Workflow.continue_as_new ~run_timeout:(seconds 0L) successor ());
      ("task timeout above 120 s", None,
       fun () -> Temporal.Workflow.continue_as_new ~task_timeout:(seconds 121L) successor ());
      ("task timeout above run timeout", None,
       fun () ->
         Temporal.Workflow.continue_as_new ~run_timeout:(seconds 10L)
           ~task_timeout:(seconds 11L) successor ());
      ("run timeout above execution timeout", Some (run_info ~execution_ms:30_000L ()),
       fun () -> Temporal.Workflow.continue_as_new ~run_timeout:(seconds 31L) successor ());
      ("task timeout above inherited run timeout", Some (run_info ~run_ms:5_000L ()),
       fun () -> Temporal.Workflow.continue_as_new ~task_timeout:(seconds 6L) successor ());
      ("repeated memo key", None,
       fun () ->
         Temporal.Workflow.continue_as_new
           ~memo:[ ("k", text_payload "1"); ("k", text_payload "2") ] successor ());
      ("empty search-attribute key", None,
       fun () ->
         Temporal.Workflow.continue_as_new ~search_attributes:[ ("", text_payload "1") ]
           successor ()) ]
  in
  List.iter
    (fun (label, info, body) ->
      match run ?info body with
      | [], Some failure ->
          let view = Temporal_base.Error.view failure in
          if view.category <> `Defect then failwith (label ^ ": not a defect");
          let prefix = "Temporal.Workflow.continue_as_new: " in
          if
            String.length view.message < String.length prefix
            || String.sub view.message 0 (String.length prefix) <> prefix
          then failwith (label ^ ": unexpected message " ^ view.message)
      | _ -> failwith (label ^ ": was not a workflow-task failure without commands"))
    cases;
  (* A run timeout equal to the execution timeout and a task timeout equal
     to the inherited run timeout are accepted. *)
  match
    run ~info:(run_info ~execution_ms:30_000L ~run_ms:5_000L ()) (fun () ->
        Temporal.Workflow.continue_as_new ~task_timeout:(seconds 5L) successor ())
  with
  | [ Activation.Continue_as_new { options; _ } ], None
    when options.task_timeout = Some 5_000L && options.run_timeout = None ->
      ()
  | _ -> failwith "boundary timeouts were rejected or the inherited run timeout was sent"

(** Continue-as-new is rejected inside a query handler, which records no
    commands, before anything is buffered: the successor command and the
    memo it carries would otherwise reach history on the live path only.
    The rejection also precedes option validation and input encoding, whose
    failures terminate the context: an invalid option or a failing encoder
    in a query handler must still only raise [Invalid_argument] naming the
    read-only callback, never buffer [Fail_workflow] or seal the run. *)
let test_rejected_in_read_only_callback () =
  let failing_encoder =
    Temporal.Codec.make ~encoding:"json/plain"
      ~encode:(fun () -> Error (Temporal.Error.codec ~message:"encoder failed"))
      ~decode:(fun _ -> Error (Temporal.Error.codec ~message:"unused"))
  in
  let unencodable_successor =
    Temporal.Workflow.remote ~name:"can_options_unencodable" ~input:failing_encoder
      ~output:Temporal.Codec.unit
  in
  let cases =
    [ ("valid options", fun () -> Temporal.Workflow.continue_as_new successor ());
      ("invalid option", fun () -> Temporal.Workflow.continue_as_new ~task_queue:"" successor ());
      ("failing encoder", fun () -> Temporal.Workflow.continue_as_new unencodable_successor ()) ]
  in
  let read_only_message message =
    let needle = "read-only" in
    let rec scan index =
      index + String.length needle <= String.length message
      && (String.sub message index (String.length needle) = needle || scan (index + 1))
    in
    scan 0
  in
  List.iter
    (fun (label, body) ->
      let context =
        Workflow_context_store.create ~randomness_seed:"1153" (Scheduler.create ())
      in
      let rejected =
        Workflow_context_store.with_read_only_query context (fun () ->
            match (body () : unit) with
            | () -> false
            | exception Invalid_argument message -> read_only_message message)
      in
      if not rejected then
        failwith (label ^ ": continue-as-new was not rejected as a read-only callback");
      match Workflow_context_store.take_commands context with
      | [] -> ()
      | _ :: _ -> failwith (label ^ ": a read-only continue-as-new buffered a command"))
    cases

(** Runs every #1153 runtime check; any failure raises and fails the alias. *)
let () =
  test_no_options_is_unchanged ();
  test_options_reach_command ();
  test_invalid_options_fail_task ();
  test_rejected_in_read_only_callback ()
