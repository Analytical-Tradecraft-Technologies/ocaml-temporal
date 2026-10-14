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

(** Renders a dynamic value exactly as the command prints it. Paths must go
    through this before being compared with the output: on Windows they
    contain backslashes, which the command escapes as [\\]. *)
let shown = Replay_command.escape

(** The workflow ID recorded when the example history was captured. *)
let workflow_id = "compose-message-ada-lovelace"

(** Runs [program] with [arguments] and returns its exit code and complete
    standard output. Standard error is inherited so usage diagnostics appear
    in the test log. The pipe is read in binary mode so the bytes compared
    are exactly the bytes the command wrote; the command itself writes in
    binary mode, so its lines end in a bare [\n] on every platform. *)
let run program arguments =
  let channel =
    Unix.open_process_args_in program (Array.of_list (program :: arguments))
  in
  set_binary_mode_in channel true;
  let output = In_channel.input_all channel in
  match Unix.close_process_in channel with
  | Unix.WEXITED code -> (code, output)
  | Unix.WSIGNALED signal | Unix.WSTOPPED signal ->
      failwith (Printf.sprintf "%s stopped by signal %d" program signal)

(** Like {!run}, but feeds [input] to the program's standard input through a
    pipe, which cannot seek, exactly like [cat FILE | program ...]. The
    whole input is written and the pipe closed before the output is read;
    this cannot deadlock because the command reads its history completely
    before printing anything, and its output is a few short lines. *)
let run_with_stdin program arguments input =
  let child_stdin, parent_stdin = Unix.pipe ~cloexec:true () in
  let parent_stdout, child_stdout = Unix.pipe ~cloexec:true () in
  let pid =
    Unix.create_process program
      (Array.of_list (program :: arguments))
      child_stdin child_stdout Unix.stderr
  in
  Unix.close child_stdin;
  Unix.close child_stdout;
  let to_child = Unix.out_channel_of_descr parent_stdin in
  set_binary_mode_out to_child true;
  Out_channel.output_string to_child input;
  Out_channel.close to_child;
  let from_child = Unix.in_channel_of_descr parent_stdout in
  set_binary_mode_in from_child true;
  let output = In_channel.input_all from_child in
  In_channel.close from_child;
  match snd (Unix.waitpid [] pid) with
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
    [expected] and the output contains every string in [expect_output].
    With [stdin], the program reads that text from a pipe. *)
let check ~name ?(expect_output = []) ?stdin ~expected program arguments =
  let code, output =
    match stdin with
    | None -> run program arguments
    | Some input -> run_with_stdin program arguments input
  in
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

(** Runs one case and fails unless it exits with [expected] and its output
    is exactly [records] lines, one per history, followed by the summary
    line, with no line that starts as a forged [PASS forged] record and every
    string in [expect_output] present. This guards the one-line-per-history
    format against line breaks in paths, IDs and diagnostics. *)
let check_one_line_per_history ~name ~records ~expect_output ~expected program
    arguments =
  let code, output = run program arguments in
  let lines =
    String.split_on_char '\n' output
    |> List.filter (fun line -> not (String.equal line ""))
  in
  let forged =
    List.exists (String.starts_with ~prefix:"PASS forged") lines
  in
  let missing =
    List.filter (fun needle -> not (contains output needle)) expect_output
  in
  if
    code <> expected
    || List.length lines <> records + 1
    || forged || missing <> []
    (* Binary-mode output never contains a carriage return, so any [\r] is
       one that escaped the escaping, on every platform. *)
    || String.contains output '\r'
  then
    failwith
      (Printf.sprintf
         "%s: expected exit %d and %d record lines, got exit %d and %d lines; \
          missing %s; output:\n\
          %s"
         name expected records code (List.length lines)
         (String.concat ", " missing)
         output);
  Printf.printf "ok %s\n%!" name

(** Checks the escaping function directly, independent of what Core or the
    platform's argument passing does to control characters. *)
let test_escape () =
  let cases =
    [
      ("plain text", "plain text");
      ("na\xc3\xafve", "na\xc3\xafve");
      ("a\nPASS b", "a\\nPASS b");
      ("a\r\nb\tc", "a\\r\\nb\\tc");
      ("back\\slash", "back\\\\slash");
      ("bell\007del\127", "bell\\x07del\\x7f");
    ]
  in
  List.iter
    (fun (input, expected) ->
      let actual = Replay_command.escape input in
      if not (String.equal actual expected) then
        failwith
          (Printf.sprintf "escape %S: expected %S, got %S" input expected
             actual))
    cases;
  print_endline "ok escape"

let () =
  test_escape ();
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
            "PASS " ^ shown history ^ " workflow_id=" ^ workflow_id;
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
        ~expect_output:[ "FAIL " ^ shown missing; "cannot read history file" ]
        example
        [ "--workflow-id"; workflow_id; missing ];
      check ~name:"not a history" ~expected:3
        ~expect_output:[ "FAIL " ^ shown not_history; "invalid history" ]
        example
        [ "--workflow-id"; workflow_id; not_history ];
      check ~name:"empty history" ~expected:3
        ~expect_output:[ "invalid history" ]
        example
        [ "--workflow-id=" ^ workflow_id; empty ];
      (* Every history is replayed and reported; the first failure in
         command-line order decides the status. The history records its
         workflow ID, so replaying it under another execution's ID is an
         invalid history rather than a pass (issue #1046). *)
      check ~name:"several histories" ~expected:3
        ~expect_output:
          [
            "PASS " ^ shown history ^ " workflow_id=" ^ workflow_id;
            "FAIL " ^ shown not_history;
            "FAIL " ^ shown history
            ^ " workflow_id=another-execution: invalid history: Temporal \
               replay history records a different workflow ID than the one \
               supplied";
            "PASS " ^ shown history ^ " workflow_id=" ^ workflow_id;
            "replayed 4 histories: 2 passed, 2 failed";
          ]
        example
        [
          "--workflow-id"; workflow_id; history; not_history;
          "--workflow-id"; "another-execution"; history;
          "--workflow-id"; workflow_id; history;
        ];
      check ~name:"mismatched workflow id with changed code" ~expected:3
        ~expect_output:[ "records a different workflow ID" ]
        altered
        [ "nondeterministic"; "--workflow-id"; "another-execution"; history ];
      (* Non-seekable inputs (issue #1047): [-] is standard input. *)
      let history_bytes = In_channel.with_open_bin history In_channel.input_all in
      check ~name:"history from a pipe" ~expected:0 ~stdin:history_bytes
        ~expect_output:[ "PASS - workflow_id=" ^ workflow_id ]
        example
        [ "--workflow-id"; workflow_id; "-" ];
      check ~name:"changed code from a pipe" ~expected:1 ~stdin:history_bytes
        ~expect_output:[ "FAIL - workflow_id="; ": nondeterminism (run " ]
        altered
        [ "nondeterministic"; "--workflow-id"; workflow_id; "-" ];
      if not Sys.win32 then
        check ~name:"history from /dev/stdin" ~expected:0 ~stdin:history_bytes
          ~expect_output:[ "PASS /dev/stdin workflow_id=" ^ workflow_id ]
          example
          [ "--workflow-id"; workflow_id; "/dev/stdin" ];
      check ~name:"nondeterminism" ~expected:1
        ~expect_output:[ "FAIL " ^ shown history; ": nondeterminism (run " ]
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
      check_one_line_per_history ~name:"multi-line diagnostic" ~records:2
        ~expected:2
        ~expect_output:[ ": workflow task failed"; "\\nPASS forged.pb" ]
        altered
        [ "multiline"; "--workflow-id"; workflow_id; history; history ];
      (* The forged ID differs from the recorded one, so the record is a
         FAIL line, which must still be exactly one line. *)
      check_one_line_per_history ~name:"multi-line workflow id" ~records:1
        ~expected:3
        ~expect_output:
          [ "FAIL "; "workflow_id=first\\nPASS forged: invalid history" ]
        example
        [ "--workflow-id"; "first\nPASS forged"; history ];
      check ~name:"replay could not run" ~expected:5
        ~expect_output:[ ": replay error: duplicate workflow registration" ]
        altered
        [ "duplicate"; "--workflow-id"; workflow_id; history ])
