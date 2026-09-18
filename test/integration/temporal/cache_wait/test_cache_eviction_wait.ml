(** Checks deadline exhaustion, diagnostic observations, and successful markers
    using the same wait functions as the live driver, without a Temporal server. *)
let () =
  if Array.length Sys.argv > 1 && Sys.argv.(1) = "--watchdog-child" then begin
    (* Model startup consuming time before the actual missing-marker wait. *)
    Unix.sleepf 0.3;
    let absent = Filename.temp_file "watchdog-marker" ".json" in
    Sys.remove absent;
    match Cache_eviction_wait.wait_for_eviction_with_second_diagnostic
        ~eviction:absent ~second_ready:absent ~timeout:2. with
    | Ok () -> exit 0
    | Error message -> Printf.eprintf "marker diagnostic: %s\n%!" message; exit 1
  end;
  let path = Filename.temp_file "absent-cache-marker" ".json" in
  let second = Filename.temp_file "second-cache-marker" ".txt" in
  Fun.protect ~finally:(fun () ->
      List.iter (fun p -> if Sys.file_exists p then Sys.remove p) [path; second])
    (fun () ->
      Sys.remove path;
      Sys.remove second;
      Unix.putenv "SMOKE_CACHE_EVICTION_DEADLINE_EPOCH" "1";
      let started = Unix.gettimeofday () in
      let result = Cache_eviction_wait.wait_for_marker
          ~path ~expected:"initial-completion\n" ~timeout:0.3 in
      assert (Result.is_error result);
      if Unix.gettimeofday () -. started >= 0.15 then
        failwith "marker wait restarted its timeout after the process deadline";
      let absent = Cache_eviction_wait.wait_for_eviction_with_second_diagnostic
          ~eviction:path ~second_ready:second ~timeout:0.3 in
      (* Markers are exact byte sequences; text output expands LF to CRLF on Windows. *)
      let channel = open_out_bin second in
      output_string channel "initial-completion\n";
      close_out channel;
      let acknowledged = Cache_eviction_wait.wait_for_eviction_with_second_diagnostic
          ~eviction:path ~second_ready:second ~timeout:0.3 in
      assert (Result.is_error absent && Result.is_error acknowledged);
      assert (absent <> acknowledged);
      assert (Cache_eviction_wait.wait_for_marker ~path:second
          ~expected:"initial-completion\n" ~timeout:0.3 = Ok ());
      print_endline "cache eviction shared deadline and observations: ok")
