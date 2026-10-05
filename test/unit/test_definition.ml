(** Remote activity definition used to verify its name and codecs. *)
let greeting =
  Temporal.Activity.remote ~name:"greeting" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Minimal local workflow implementation used by the definition tests. *)
let workflow input = Ok (input ^ "!")

(** Local workflow definition used to verify implementation storage. *)
let greeting_workflow =
  Temporal.Workflow.define ~name:"greeting_workflow"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string workflow

(** Confirms invalid names fail during definition construction rather than
    later during workflow execution. *)
let expect_invalid_name make =
  match make () with
  | exception Invalid_argument _ -> ()
  | _ -> failwith "invalid definition name accepted"

(** Builds a byte string that is not valid UTF-8 without introducing a NUL;
    this exercises the protocol text validator rather than the older NUL check. *)
let invalid_utf_8 = Bytes.to_string (Bytes.init 1 (fun _ -> Char.chr 0xff))

(** Confirms public workflow and activity constructors enforce the complete
    bridge identifier contract before a definition can be registered. *)
let expect_name_boundary_rejection () =
  let oversized = String.make 65_537 'x' in
  expect_invalid_name (fun () ->
      Temporal.Workflow.remote ~name:oversized ~input:Temporal.Codec.unit
        ~output:Temporal.Codec.unit);
  expect_invalid_name (fun () ->
      Temporal.Activity.remote ~name:oversized ~input:Temporal.Codec.unit
        ~output:Temporal.Codec.unit);
  expect_invalid_name (fun () ->
      Temporal.Workflow.remote ~name:invalid_utf_8 ~input:Temporal.Codec.unit
        ~output:Temporal.Codec.unit);
  expect_invalid_name (fun () ->
      Temporal.Activity.remote ~name:invalid_utf_8 ~input:Temporal.Codec.unit
        ~output:Temporal.Codec.unit)

let () =
  assert (Temporal.Activity.name greeting = "greeting");
  assert (Temporal.Workflow.name greeting_workflow = "greeting_workflow");
  expect_invalid_name (fun () ->
      Temporal.Activity.remote ~name:"" ~input:Temporal.Codec.unit
        ~output:Temporal.Codec.unit);
  expect_invalid_name (fun () ->
      Temporal.Workflow.remote ~name:"bad\000name" ~input:Temporal.Codec.unit
        ~output:Temporal.Codec.unit);
  expect_name_boundary_rejection ();
  assert (not (Temporal.Workflow_context.is_active ()));
  (match Temporal.Future.await (Temporal.Activity.start greeting "Ada") with
  | Error error -> assert (Temporal.Error.kind error = "defect")
  | Ok _ -> failwith "activity started outside a workflow");
  (* Zero close timeouts are option errors and are reported before the
     workflow-context check, so they are observable here (#812). *)
  let zero = Temporal.Duration.of_ms 0L in
  let expect_zero_rejected label future =
    match Temporal.Future.await future with
    | Error error ->
        assert (Temporal.Error.kind error = "defect");
        if not (String.ends_with ~suffix:"must be positive" (Temporal.Error.message error))
        then failwith (label ^ " reported the wrong defect")
    | Ok _ -> failwith (label ^ " accepted a zero close timeout")
  in
  expect_zero_rejected "schedule-to-close"
    (Temporal.Activity.start ~schedule_to_close_timeout:zero greeting "Ada");
  expect_zero_rejected "start-to-close"
    (Temporal.Activity.start ~start_to_close_timeout:zero greeting "Ada");
  expect_zero_rejected "local start-to-close"
    (Temporal.Activity.start_local ~start_to_close_timeout:zero greeting "Ada");
  (* protobuf allows the maximum second count plus a fractional second. *)
  (match Temporal.Duration.of_ms 315_576_000_001_000L with
  | _ -> failwith "duration above the protobuf maximum was accepted"
  | exception Invalid_argument _ -> ());
  assert (
    Temporal.Duration.to_ms (Temporal.Duration.of_ms 315_576_000_000_999L)
    = 315_576_000_000_999L);
  match Temporal.Workflow.sleep (Temporal.Duration.of_ms 1L) with
  | Error error -> assert (Temporal.Error.kind error = "defect")
  | Ok () -> failwith "workflow slept outside an execution"
