(** Marker waits share the absolute deadline installed before compilation by
    the process wrapper. A manual invocation retains a bounded local fallback.
    Invalid deadlines fail closed instead of silently granting more time. *)
let deadline ~timeout =
  match Sys.getenv_opt "SMOKE_CACHE_EVICTION_DEADLINE_EPOCH" with
  | None -> Unix.gettimeofday () +. timeout
  | Some raw ->
      (match float_of_string_opt raw with
      | Some value when Float.is_finite value && value > 0. -> value
      | _ -> 0.)

(** Returns whether a shared marker is non-empty and, when requested, exactly
    matches its expected payload. Atomic publication makes a complete read
    possible, while the payload check prevents a previous run's readiness token
    from satisfying the current run. *)
let marker_matches path expected =
  if not (Sys.file_exists path) || (Unix.stat path).Unix.st_size = 0 then false
  else
    match expected with
    | None -> true
    | Some expected -> (
        try
          let channel = open_in_bin path in
          Fun.protect
            ~finally:(fun () -> close_in_noerr channel)
            (fun () ->
              let contents =
                really_input_string channel (in_channel_length channel)
              in
              String.equal contents expected)
        with _ -> false)

(** Waits for an exact completion marker before admitting the second run. The
    worker publishes this marker after Core acknowledges A's first activation;
    using the marker as the admission barrier keeps the driver independent of
    query-task routing and gives the eviction assertion one deterministic
    starting state. *)
let wait_for_marker ~path ~expected ~timeout =
  let deadline = deadline ~timeout in
  let rec loop () =
    if marker_matches path (Some expected) then Ok ()
    else if Unix.gettimeofday () >= deadline then
      Error ("timed out waiting for marker " ^ path)
    else begin
      Unix.sleepf 0.1;
      loop ()
    end
  in
  loop ()

(** Waits for A's cache-full marker while retaining B's completion marker as a
    diagnostic only. Core buffers B when the one-slot cache is full and only
    releases it after A's cache-removal activation is acknowledged; therefore
    B cannot satisfy this acceptance condition. One deadline covers both
    observations so a late B marker cannot silently grant a second timeout
    budget before the required eviction arrives. *)
let wait_for_eviction_with_second_diagnostic ~eviction ~second_ready ~timeout =
  let deadline = deadline ~timeout in
  let rec loop saw_second_acknowledgement =
    let saw_second_acknowledgement =
      saw_second_acknowledgement
      || marker_matches second_ready (Some "initial-completion\n")
    in
    if marker_matches eviction None then Ok ()
    else if Unix.gettimeofday () >= deadline then
      let message =
        if saw_second_acknowledgement then
          "second workflow was acknowledged but A cache-full eviction marker was not published"
        else
          "neither A cache-full eviction marker nor second workflow acknowledgement was published"
      in
      Error message
    else begin
      Unix.sleepf 0.1;
      loop saw_second_acknowledgement
    end
  in
  loop false
