(** Self-checking Eio integration example (#1141).

    The SDK has no Eio dependency: every [Temporal.Client] call and
    [Temporal.Worker.run] blocks its calling system thread. This program
    shows the two patterns of the "Use the SDK from Eio and Lwt applications"
    section of [docs/guides/workflows.md] inside one Eio program, and checks
    that each behaved as the guide describes:

    + Client calls from Eio fibers. [Client.create], [Client.start],
      [Client.wait] and [Client.shutdown] each run in a system thread through
      [Eio_unix.run_in_systhread]. Three fibers share one client and start
      and await their workflows concurrently, while an unrelated ticker fiber
      keeps running. The check requires that all three calls were in flight at
      once and that the ticker ran while they waited; a blocking call made
      directly on a fiber would have serialized the calls and stalled the
      ticker.
    + A worker hosted on its own Domain. [Worker.run] and then
      [Worker.shutdown] run in [Eio.Domain_manager.run], and a daemon fiber
      turns Eio cancellation into [Worker.request_shutdown]. When the client
      work is done, resolving a promise cancels the hosting fiber. The check
      requires that the worker was drained and released, with a successful
      result, before that fiber finished.

    It prints one line per check and exits 0 only if all of them hold. The
    workflows use the task queue [TEMPORAL_TASK_QUEUE] with an [-eio] suffix,
    so this example never shares a queue with the other example workers, and
    fresh workflow IDs, so it can be rerun in the same namespace.

    The program uses [Eio_posix.run] rather than [Eio_main.run]. They behave
    the same here; [Eio_main] would also select the [io_uring] backend on
    Linux, whose extra packages this example does not need. *)

(** Returns a defect describing a failed example check. *)
let check_failed message = Temporal.Error.defect ~message:("eio example: " ^ message)

(** The stable workflow type name, private to this example. *)
let greet_name = "ocaml-temporal-example.eio-greet"

(** Waits on a one-second durable timer, then greets [name]. The timer makes
    each [Client.wait] last long enough for the checks to observe other
    fibers running meanwhile. Workflow code runs under the SDK's own
    deterministic scheduler on the worker's Domain and never uses Eio. *)
let greet name =
  let open Temporal.Result_syntax in
  let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 1_000L) in
  Ok ("Hello, " ^ name ^ "!")

(** The workflow definition registered by the hosted worker and started by
    the client fibers. *)
let greet_workflow =
  Temporal.Workflow.define ~name:greet_name ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string greet

(** Counts the client calls that are in flight in system threads. Only fibers
    of the main Domain read or write these fields, so plain mutation is safe:
    the system threads that run the calls never touch them. *)
type in_flight = {
  mutable current : int;  (** Calls started and not yet returned. *)
  mutable peak : int;  (** Largest value [current] has reached. *)
  mutable ticks_while_waiting : int;
      (** Ticker iterations that observed [current > 0]. *)
}

(** Starts one [greet] workflow and waits for its result, in a system thread,
    as in the guide's [greet_from_eio]. The fiber that calls it suspends until
    the thread returns, so other fibers keep running. [Client.wait ~timeout]
    bounds the wait: Eio cannot interrupt a system thread, so an Eio timeout
    or cancellation could not. *)
let greet_from_eio client ~task_queue ~suffix counter name =
  counter.current <- counter.current + 1;
  counter.peak <- max counter.peak counter.current;
  let result =
    Eio_unix.run_in_systhread ~label:"temporal-client" (fun () ->
        Example_support.Lifecycle.protect_result "client call" (fun () ->
            let open Temporal.Result_syntax in
            let* handle =
              Temporal.Client.start client
                ~execution_timeout:(Temporal.Duration.of_ms 120_000L)
                ~workflow:greet_workflow ~task_queue
                ~id:(Printf.sprintf "ocaml-temporal-example-eio-%s-%s" name suffix)
                ~input:name ()
            in
            match
              Temporal.Client.wait ~timeout:(Temporal.Duration.of_ms 60_000L)
                handle
            with
            | Ok (Temporal.Client.Completed { output; _ }) -> Ok output
            | Ok _ -> Error (check_failed (name ^ ": greet did not complete"))
            | Error error -> Error error))
  in
  counter.current <- counter.current - 1;
  result

(** Wakes every 20 ms until cancelled and counts the iterations that found a
    client call in flight. It is the "other fiber" whose progress shows that
    the blocking calls did not stall the Eio scheduler. *)
let rec tick clock counter =
  Eio.Time.sleep clock 0.02;
  if counter.current > 0 then
    counter.ticks_while_waiting <- counter.ticks_while_waiting + 1;
  tick clock counter

(** Runs [worker] on a new Domain until the calling fiber is cancelled, then
    drains and releases it, as in the guide. Eio cannot interrupt
    [Worker.run] (it is not Eio code), so a daemon fiber requests the stop
    when the switch is cancelled. [on_stopped] receives the combined run and
    shutdown result on the hosting Domain. It is the only way to observe that
    result after a cancellation, because the cancelled fiber then ends with
    Eio's [Cancelled] exception instead of this function's return value. *)
let serve_worker ~domain_mgr ~on_stopped worker =
  Eio.Switch.run @@ fun sw ->
  Eio.Fiber.fork_daemon ~sw (fun () ->
      Fun.protect
        ~finally:(fun () -> Temporal.Worker.request_shutdown worker)
        Eio.Fiber.await_cancel);
  Eio.Domain_manager.run domain_mgr (fun () ->
      let run_result =
        Example_support.Lifecycle.protect_result "worker run" (fun () ->
            Temporal.Worker.run worker)
      in
      let shutdown_result =
        Example_support.Lifecycle.protect_result "worker shutdown" (fun () ->
            Temporal.Worker.shutdown worker)
      in
      let result = Result.bind run_result (fun () -> shutdown_result) in
      on_stopped result;
      result)

(** Runs a blocking SDK constructor or release call in a system thread. *)
let offload label operation =
  Eio_unix.run_in_systhread ~label (fun () ->
      Example_support.Lifecycle.protect_result label operation)

(** The names greeted by the three concurrent client fibers. *)
let names = [ "Ada"; "Grace"; "Alan" ]

(** Runs the client fibers and the ticker beside the hosted worker, then
    stops the worker, and returns the greetings, the in-flight counter and
    the worker's stop result. The worker and client are created and released
    off the Eio thread, because both connect to (or disconnect from) the
    server. *)
let run_scenario env (config : Example_support.Config.t) =
  let open Temporal.Result_syntax in
  let task_queue = config.task_queue ^ "-eio" in
  let suffix = Printf.sprintf "%.0f-%d" (Unix.time ()) (Unix.getpid ()) in
  let* worker =
    offload "worker create" (fun () ->
        Temporal.Worker.create ~target_url:config.target_url
          ~namespace:config.namespace
          ~identity:"ocaml-temporal-example-eio-worker" ~task_queue
          ~workflows:[ Temporal.Worker.workflow greet_workflow ]
          ~activities:[] ())
  in
  let stopped = Atomic.make None in
  let counter = { current = 0; peak = 0; ticks_while_waiting = 0 } in
  let stop, resolve_stop = Eio.Promise.create () in
  let greetings =
    Eio.Fiber.pair
      (fun () ->
        (* Resolving [stop] cancels the hosting fiber, which requests the
           stop; [Fiber.first] returns only once the worker Domain has shut
           the worker down. A worker that fails on its own also ends here. *)
        Eio.Fiber.first
          (fun () ->
            ignore
              (serve_worker ~domain_mgr:(Eio.Stdenv.domain_mgr env)
                 ~on_stopped:(fun result -> Atomic.set stopped (Some result))
                 worker))
          (fun () -> Eio.Promise.await stop))
      (fun () ->
        Fun.protect
          ~finally:(fun () -> Eio.Promise.resolve resolve_stop ())
          (fun () ->
            let* client =
              offload "client create" (fun () ->
                  Temporal.Client.create ~target_url:config.target_url
                    ~namespace:config.namespace
                    ~identity:"ocaml-temporal-example-eio-client" ())
            in
            let greetings =
              Eio.Fiber.first
                (fun () -> tick (Eio.Stdenv.clock env) counter)
                (fun () ->
                  Eio.Fiber.List.map
                    (greet_from_eio client ~task_queue ~suffix counter)
                    names)
            in
            (* Every client fiber has finished, so no call is in flight. *)
            let* () =
              offload "client shutdown" (fun () ->
                  Temporal.Client.shutdown client)
            in
            List.fold_right
              (fun greeting acc ->
                let* greeting = greeting in
                let* acc = acc in
                Ok (greeting :: acc))
              greetings (Ok [])))
    |> snd
  in
  Ok (greetings, counter, Atomic.get stopped)

(** Checks the scenario's observations against the guide's claims and prints
    one line for each. *)
let run env =
  let open Temporal.Result_syntax in
  let* config = Example_support.Config.from_environment () in
  let* greetings, counter, stopped = run_scenario env config in
  (* The worker's result comes first: a worker that failed explains why the
     client calls then failed or timed out. *)
  let* () =
    match stopped with
    | Some (Ok ()) -> Ok ()
    | Some (Error error) -> Error error
    | None -> Error (check_failed "the worker was not stopped before its fiber ended")
  in
  let* greetings = greetings in
  let expected = List.map (fun name -> "Hello, " ^ name ^ "!") names in
  let* () =
    if greetings = expected then Ok ()
    else
      Error
        (check_failed
           ("unexpected greetings: " ^ String.concat " | " greetings))
  in
  print_endline
    "Eio client: three fibers shared one client and each started and awaited a workflow.";
  let* () =
    if counter.peak = List.length names then Ok ()
    else
      Error
        (check_failed
           (Printf.sprintf "only %d of %d client calls were in flight at once"
              counter.peak (List.length names)))
  in
  print_endline
    "Eio client: all three blocking calls were in flight at once, each in its own system thread.";
  let* () =
    if counter.ticks_while_waiting >= 2 then Ok ()
    else
      Error
        (check_failed
           (Printf.sprintf
              "the ticker fiber ran %d times while client calls were in flight"
              counter.ticks_while_waiting))
  in
  print_endline
    "Eio fibers: another fiber kept running while the client calls waited.";
  print_endline
    "Eio worker: hosted on its own Domain, drained and shut down when its fiber was cancelled.";
  Ok ()

(** Exits 1 with a description of the first failed check. *)
let () =
  match Eio_posix.run run with
  | Ok () -> print_endline "Eio example passed."
  | Error error ->
      Example_support.report_error "eio example" error;
      exit 1
