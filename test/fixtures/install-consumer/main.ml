module T = Temporal

(* Dune compiles and links only the modules reachable from an executable's
   main module, so an unreferenced [Public_api] would never be type-checked
   by the installed-consumer build.  This reference keeps the compatibility
   witness in that build; the witness has no top-level effects. *)
let (_ : string T.Codec.t) = Public_api._codec_string

(** Converts an expected SDK result into a test value while retaining the
    public error message if the installed package violates its contract. *)
let expect_ok label = function
  | Ok value -> value
  | Error error ->
      failwith (label ^ ": " ^ T.Error.message error)

let () =
  (* Exercise the public payload record and both directions of a custom codec. *)
  let payload : T.Payload.t =
    { metadata = [ ("encoding", "binary/plain") ]; data = Bytes.of_string "x" }
  in
  if payload.metadata = [] || Bytes.length payload.data <> 1 then
    failwith "payload record did not retain its fields";
  let custom =
    T.Codec.make ~encoding:"binary/plain"
      ~encode:(fun value -> Ok (Bytes.of_string value))
      ~decode:(fun bytes -> Ok (Bytes.to_string bytes))
  in
  let encoded = expect_ok "codec encode" (T.Codec.encode custom "hello") in
  ignore (expect_ok "codec decode" (T.Codec.decode custom encoded));
  ignore (expect_ok "codec option some"
            (T.Codec.decode (T.Codec.option custom)
               (expect_ok "option encode" (T.Codec.encode (T.Codec.option custom) (Some "value")))));
  ignore (expect_ok "codec option none"
            (T.Codec.decode (T.Codec.option custom)
               (expect_ok "option none encode" (T.Codec.encode (T.Codec.option custom) None))));

  (* Duration and structured errors are independent public value types. *)
  let duration = T.Duration.of_ms 25L in
  if T.Duration.to_ms duration <> 25L then failwith "duration round trip failed";
  let error =
    T.Error.make ~category:`Workflow ~message:"consumer check" ()
  in
  if T.Error.kind error <> "workflow" || T.Error.message error <> "consumer check"
  then failwith "error inspection failed";
  ignore (T.Error.view error);

  (* Definitions are opaque values with semantic accessors and ordinary
     implementation callbacks. *)
  let workflow =
    T.Workflow.define ~name:"consumer-workflow" ~input:T.Codec.string
      ~output:T.Codec.string (fun input -> Ok input)
  in
  let remote_workflow =
    T.Workflow.remote ~name:"consumer-remote-workflow" ~input:T.Codec.string
      ~output:T.Codec.string
  in
  let activity =
    T.Activity.define ~name:"consumer-activity" ~input:T.Codec.string
      ~output:T.Codec.string (fun input -> Ok input)
  in
  let remote_activity =
    T.Activity.remote ~name:"consumer-remote-activity" ~input:T.Codec.string
      ~output:T.Codec.string
  in
  if T.Workflow.name workflow <> "consumer-workflow"
     || T.Workflow.name remote_workflow <> "consumer-remote-workflow"
     || T.Activity.name activity <> "consumer-activity"
     || T.Activity.name remote_activity <> "consumer-remote-activity"
  then failwith "definition names were not retained";
  ignore (T.Workflow.input workflow, T.Workflow.output workflow,
          T.Workflow.implementation workflow);
  ignore (T.Activity.input activity, T.Activity.output activity,
          T.Activity.implementation activity);

  (* Future values come from SDK operations rather than an application-supplied
     callback constructor. The empty aggregate is a ready, context-free
     operation and exercises the public inspection and await functions. *)
  let ready_future = T.Future.all [] in
  if T.Future.await ready_future <> Ok []
     || not (T.Future.is_ready ready_future)
  then failwith "future helpers failed";
  ignore (T.Future.peek ready_future);
  ignore (T.Workflow.start_sleep (T.Duration.of_ms 0L));
  ignore (T.Child_workflow.start ~id:"consumer-child" remote_workflow "input");

  (* Mock client and worker construction exercise lifecycle types without
     requiring a Temporal server in the installed-consumer test. *)
  let client =
    expect_ok "client create"
      (T.Client.create ~target_url:"mock://consumer" ~namespace:"default" ())
  in
  let handle =
    expect_ok "client start"
      (T.Client.start client ~workflow ~task_queue:"queue" ~id:"consumer-id"
         ~input:"input" ())
  in
  ignore (T.Client.workflow_id handle, Option.get (T.Client.run_id handle));
  (match expect_ok "client wait" (T.Client.wait handle) with
  | T.Client.Completed { output = value; _ } when value = "input" -> ()
  | _ -> failwith "unexpected mock client terminal result");
  ignore (expect_ok "client shutdown" (T.Client.shutdown client));
  let worker =
    expect_ok "worker create"
      (T.Worker.create ~target_url:"mock://consumer" ~namespace:"default"
         ~task_queue:"queue" ~workflows:[ T.Worker.workflow workflow ]
         ~activities:[ T.Worker.activity activity ] ())
  in
  ignore (expect_ok "worker shutdown" (T.Worker.shutdown worker));

  let open T.Result_syntax in
  ignore
    (let* value = Ok 1 in
     let+ value = Ok (value + 1) in
     value);
  if T.Workflow_context.is_active () then failwith "consumer context leaked";
  match T.Runtime_info.native_bridge_abi_version () with
  | Ok 4l -> ()
  | Ok version ->
      failwith (Printf.sprintf "unexpected native ABI version %ld" version)
  | Error error -> failwith (T.Error.message error)
