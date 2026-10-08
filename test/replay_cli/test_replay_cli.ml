(** Process-level tests for the application-linked replay checker in
    [examples/replay] (issue #516).

    Each case runs a real executable, exactly as a CI job would, and checks
    its exit status and standard output. The example executable links the
    unchanged example workflow; [altered_replay_history] links deliberately
    changed versions to produce the incompatible outcomes. Everything is
    offline: the history is the checked-in capture of one example run. *)

(** Makes a dune-supplied relative path absolute. A program path without a
    directory separator would otherwise be searched for in [PATH]. *)
let absolute path =
  if Filename.is_relative path then Filename.concat (Sys.getcwd ()) path
  else path

(** The example's replay executable, the altered copy, and the recorded
    example history, passed by dune as build-relative paths. *)
let example, altered, history =
  match Sys.argv with
  | [| _; example; altered; history |] ->
      (absolute example, absolute altered, history)
  | _ -> failwith "usage: test_replay_cli EXAMPLE ALTERED HISTORY"

(** The workflow ID recorded when the example history was captured. *)
let workflow_id = "compose-message-ada-lovelace"

(** Runs [program] with [arguments] and returns its exit code and complete
    standard output. Standard error is inherited so usage diagnostics appear
    in the test log. *)
let run program arguments =
  let channel =
    Unix.open_process_args_in program (Array.of_list (program :: arguments))
  in
  let output = In_channel.input_all channel in
  match Unix.close_process_in channel with
  | Unix.WEXITED code -> (code, output)
  | Unix.WSIGNALED signal | Unix.WSTOPPED signal ->
      failwith (Printf.sprintf "%s stopped by signal %d" program signal)

(** Returns whether [needle] occurs in [haystack]. *)
let contains haystack needle =
  let length = String.length needle in
  let rec search index =
    index + length <= String.length haystack
    && (String.equal (String.sub haystack index length) needle
       || search (index + 1))
  in
  search 0

(** Runs one case and fails with the full output unless the exit code is
    [expected] and the output contains every string in [expect_output]. *)
let check ~name ?(expect_output = []) ~expected program arguments =
  let code, output = run program arguments in
  let missing =
    List.filter (fun needle -> not (contains output needle)) expect_output
  in
  if code <> expected || missing <> [] then
    failwith
      (Printf.sprintf "%s: expected exit %d, got %d; missing %s; output:\n%s"
         name expected code
         (String.concat ", " missing)
         output);
  Printf.printf "ok %s\n%!" name

(** Writes [contents] to a fresh temporary file and returns its path. *)
let temporary_file contents =
  let path = Filename.temp_file "replay-cli" ".pb" in
  Out_channel.with_open_bin path (fun channel ->
      Out_channel.output_string channel contents);
  path

let () =
  let not_history = temporary_file "this is not a History protobuf" in
  let empty = temporary_file "" in
  let missing = Filename.concat (Filename.get_temp_dir_name ()) "replay-cli-missing.pb" in
  Fun.protect
    ~finally:(fun () ->
      List.iter (fun path -> try Sys.remove path with Sys_error _ -> ())
        [ not_history; empty ])
    (fun () ->
      check ~name:"compatible history" ~expected:0
        ~expect_output:
          [
            "PASS " ^ history ^ " workflow_id=" ^ workflow_id;
            "replayed 1 history: 1 passed, 0 failed";
          ]
        example
        [ "--workflow-id"; workflow_id; history ];
      check ~name:"help" ~expected:0 ~expect_output:[ "usage:"; "64 usage error" ]
        example [ "--help" ];
      check ~name:"no history" ~expected:64 example [ "--workflow-id"; workflow_id ];
      check ~name:"no workflow id" ~expected:64 example [ history ];
      check ~name:"unknown option" ~expected:64 example
        [ "--verbose"; "--workflow-id"; workflow_id; history ];
      check ~name:"missing value" ~expected:64 example [ "--workflow-id" ];
      check ~name:"unreadable file" ~expected:66
        ~expect_output:[ "FAIL " ^ missing; "cannot read history file" ]
        example
        [ "--workflow-id"; workflow_id; missing ];
      check ~name:"not a history" ~expected:3
        ~expect_output:[ "FAIL " ^ not_history; "invalid history" ]
        example
        [ "--workflow-id"; workflow_id; not_history ];
      check ~name:"empty history" ~expected:3
        ~expect_output:[ "invalid history" ]
        example
        [ "--workflow-id=" ^ workflow_id; empty ];
      (* Every history is replayed and reported; the first failure in
         command-line order decides the status. *)
      check ~name:"several histories" ~expected:3
        ~expect_output:
          [
            "PASS " ^ history ^ " workflow_id=" ^ workflow_id;
            "FAIL " ^ not_history;
            "PASS " ^ history ^ " workflow_id=another-execution";
            "replayed 3 histories: 2 passed, 1 failed";
          ]
        example
        [
          "--workflow-id"; workflow_id; history; not_history;
          "--workflow-id"; "another-execution"; history;
        ];
      check ~name:"nondeterminism" ~expected:1
        ~expect_output:[ "FAIL " ^ history; ": nondeterminism (run " ]
        altered
        [ "nondeterministic"; "--workflow-id"; workflow_id; history ];
      check ~name:"first failure decides" ~expected:1
        ~expect_output:[ "replayed 2 histories: 0 passed, 2 failed" ]
        altered
        [ "nondeterministic"; "--workflow-id"; workflow_id; history; not_history ];
      check ~name:"workflow task failed" ~expected:2
        ~expect_output:[ ": workflow task failed" ]
        altered
        [ "failing"; "--workflow-id"; workflow_id; history ];
      check ~name:"replay could not run" ~expected:5
        ~expect_output:[ ": replay error: duplicate workflow registration" ]
        altered
        [ "duplicate"; "--workflow-id"; workflow_id; history ])
