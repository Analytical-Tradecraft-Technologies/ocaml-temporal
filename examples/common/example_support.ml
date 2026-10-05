(** Shared configuration, definitions, and lifecycle helpers for the example
    applications. The module depends only on the public [Temporal] library so
    the executables demonstrate the same package boundary as an installed
    application. *)

module Config = struct
  (** Connection settings shared by the independently started example
      processes. Every process constructs its own SDK instance from this
      immutable value; no native handle is shared across processes. *)
  type t = {
    target_url : string;
    namespace : string;
    task_queue : string;
  }

  (** Looks up [name], retaining [default] only when the variable is absent.
      An explicitly empty value is rejected so a deployment typo is not
      silently converted into a connection to an unintended target. *)
  let environment_or_default name default =
    match Sys.getenv_opt name with
    | None -> Ok default
    | Some "" ->
        Error
          (Temporal.Error.defect
             ~message:(Printf.sprintf "%s must not be empty" name))
    | Some value -> Ok value

  (** Reads the three connection values used by every example executable.
      The public client and worker constructors perform the full protocol
      validation; this early check gives a command-line user a direct error for
      missing values before allocating a native SDK graph. *)
  let from_environment () =
    let open Temporal.Result_syntax in
    let* target_url =
      environment_or_default "TEMPORAL_ADDRESS" "http://127.0.0.1:7233"
    in
    let* namespace = environment_or_default "TEMPORAL_NAMESPACE" "default" in
    let* task_queue =
      environment_or_default "TEMPORAL_TASK_QUEUE" "ocaml-temporal-example"
    in
    Ok { target_url; namespace; task_queue }
end

module Definitions = struct
  (** The stable activity type name shared by the workflow and activity
      workers. It is private to this example namespace to avoid colliding with
      an application's own activity registrations. *)
  let render_message_name = "ocaml-temporal-example.render-message"

  (** The stable workflow type name used by the workflow worker and client. *)
  let compose_message_name = "ocaml-temporal-example.compose-message"

  (** Creates the terminal business failure returned when workflow input is
      invalid. The [`Workflow] category fails the workflow execution so the
      client observes a failed result. [Temporal.Error.defect] must not be used
      here: a propagated defect fails only the workflow task, which Temporal
      retries indefinitely while the run stays open. [non_retryable] prevents
      a configured workflow retry policy from rerunning the same bad input. *)
  let invalid_input message =
    Temporal.Error.make ~category:`Workflow ~non_retryable:true ~message ()

  (** Builds one activity input without allowing an ambiguous delimiter in the
      name. This function runs in deterministic workflow code, so it only
      performs pure string validation and construction. *)
  let render_request style name =
    if String.contains name ':' then
      Error (invalid_input "example names must not contain ':'")
    else Ok (style ^ ":" ^ name)

  (** Decodes the compact activity input used by this small example. Real
      applications commonly replace this with a dedicated codec for a record
      or variant. *)
  let parse_render_request input =
    match String.split_on_char ':' input with
    | [ style; name ] when not (String.equal name "") -> Ok (style, name)
    | _ ->
        Error
          (Temporal.Error.make ~category:`Activity
             ~message:"expected an example message request in style:name form"
             ())

  (** Implements the activity's nondeterministic-work boundary. This sample
      only formats text to remain runnable without another service, but a real
      activity is the right place to call an API, database, filesystem, or
      process because Temporal records its terminal result for workflow replay.
  *)
  let render_message input =
    let open Temporal.Result_syntax in
    let* style, name = parse_render_request input in
    match style with
    | "greeting" -> Ok ("Hello, " ^ name ^ "!")
    | "next-step" -> Ok ("Next: review the Temporal result for " ^ name ^ ".")
    | _ ->
        Error
          (Temporal.Error.make ~category:`Activity
             ~message:("unknown example message style: " ^ style) ())

  (** Defines the local activity implementation registered only by the
      activity worker executable. *)
  let local_render_message =
    Temporal.Activity.define ~name:render_message_name
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string render_message

  (** Describes the activity scheduled by the workflow worker. It has no
      callback and therefore cannot accidentally be registered by that worker.
  *)
  let remote_render_message =
    Temporal.Activity.remote ~name:render_message_name
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string

  (** Implements a deterministic workflow that starts two independent
      activities and a durable timer before awaiting them together. Starting
      all three operations first lets Temporal make progress independently;
      the final [Future.await] resumes this workflow fiber only after every
      recorded result is available. *)
  let compose_message name =
    let open Temporal.Result_syntax in
    let normalized_name = String.trim name in
    if String.equal normalized_name "" then
      Error (invalid_input "a name is required")
    else
      let* greeting_input = render_request "greeting" normalized_name in
      let* next_step_input = render_request "next-step" normalized_name in
      let greeting =
        Temporal.Activity.start remote_render_message greeting_input
      in
      let next_step =
        Temporal.Activity.start remote_render_message next_step_input
      in
      let pause =
        Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 250L)
      in
      let* messages, () =
        Temporal.Future.await
          (Temporal.Future.both (Temporal.Future.all [ greeting; next_step ])
             pause)
      in
      Ok (String.concat "\n" messages)

  (** Defines the local workflow implementation registered only by the
      workflow worker executable. *)
  let local_compose_message =
    Temporal.Workflow.define ~name:compose_message_name
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string compose_message

  (** Describes the workflow started by the client executable. It retains the
      same type name and codecs while intentionally containing no local
      workflow callback. *)
  let remote_compose_message =
    Temporal.Workflow.remote ~name:compose_message_name
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string
end

module Lifecycle = struct
  (** Converts an unexpected application exception into the SDK's typed error
      channel. The public lifecycle operations already use [result]; this
      guard prevents an exception in command-line glue from bypassing cleanup.
  *)
  let protect_result operation callback =
    try callback ()
    with exception_ ->
      Error
        (Temporal.Error.defect
           ~message:
             (Printf.sprintf "%s raised: %s" operation
                (Printexc.to_string exception_)))

  (** Runs a worker until it is stopped, translating [SIGINT] and [SIGTERM]
      into the public idempotent shutdown call. Signal handlers only set an
      atomic flag; a dedicated Domain performs all SDK work outside signal
      context, then the final shutdown call closes the supervisor-owned native
      graph exactly once. *)
  let run_worker worker =
    let stop_requested = Atomic.make false in
    let watcher_finished = Atomic.make false in
    let request_stop _signal = Atomic.set stop_requested true in
    let previous_term =
      Sys.signal Sys.sigterm (Sys.Signal_handle request_stop)
    in
    let previous_int = Sys.signal Sys.sigint (Sys.Signal_handle request_stop) in
    let watcher =
      Domain.spawn (fun () ->
          while not (Atomic.get watcher_finished) do
            if Atomic.get stop_requested then begin
              ignore (Temporal.Worker.shutdown worker);
              Atomic.set watcher_finished true
            end
            else Unix.sleepf 0.05
          done)
    in
    Fun.protect
      ~finally:(fun () ->
        Atomic.set watcher_finished true;
        Domain.join watcher;
        Sys.set_signal Sys.sigterm previous_term;
        Sys.set_signal Sys.sigint previous_int)
      (fun () ->
        let run_result =
          protect_result "worker run" (fun () -> Temporal.Worker.run worker)
        in
        let shutdown_result =
          protect_result "worker shutdown" (fun () ->
              Temporal.Worker.shutdown worker)
        in
        let open Temporal.Result_syntax in
        let* () = run_result in
        shutdown_result)

  (** Creates a client, runs [use_client], and shuts the client down whether
      the client operation succeeds, returns an expected error, or raises.
      A shutdown failure is returned because it means the owned native graph
      could not be released cleanly. *)
  let with_client config use_client =
    match
      Temporal.Client.create ~target_url:config.Config.target_url
        ~namespace:config.namespace ~identity:"ocaml-temporal-example-client" ()
    with
    | Error error -> Error error
    | Ok client ->
        let result =
          protect_result "client operation" (fun () -> use_client client)
        in
        let shutdown_result =
          protect_result "client shutdown" (fun () ->
              Temporal.Client.shutdown client)
        in
        match shutdown_result with Ok () -> result | Error error -> Error error
end

(** Prints a stable, payload-free description of a typed SDK error. *)
let report_error operation error =
  Printf.eprintf "%s failed (%s): %s\n%!" operation (Temporal.Error.kind error)
    (Temporal.Error.message error)
