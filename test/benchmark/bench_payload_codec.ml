(** A no-server benchmark of the OCaml payload codec on a payload-heavy
    workflow task (#846). Each sample decodes one activation carrying a 2 MiB
    activity result, re-encodes it as the runtime's activation validation does,
    then encodes and decodes one completion that schedules an activity with a
    2 MiB argument. Rust, Core, FFI, and the network are excluded. *)

module Protocol = Temporal_protocol.Workflow_protocol

(** Decoded payload size for both the inbound result and outbound argument. *)
let payload_bytes = 2 * 1024 * 1024

(** Fails the sample with the codec's privacy-safe error message. *)
let unwrap = function
  | Ok value -> value
  | Error error -> failwith (Protocol.error_view error).message

(** A deterministic, incompressible-looking payload covering all byte values. *)
let payload =
  {
    Protocol.metadata = [ ("encoding", Bytes.of_string "binary/plain") ];
    data =
      Bytes.init payload_bytes (fun index ->
          Char.chr (((index * 7919) + (index / 3)) land 255));
  }

(** One activation resolving an activity with [payload] as its result. *)
let activation =
  {
    Protocol.run_id = "benchmark-run";
    timestamp = Some { seconds = 1L; nanoseconds = 0 };
    is_replaying = false;
    history_length = 3L;
    jobs =
      [ Protocol.Resolve_activity { seq = 1L; result = Completed (Some payload) } ];
    metadata = None;
  }

(** One completion scheduling an activity with [payload] as its argument. *)
let completion =
  {
    Protocol.run_id = "benchmark-run";
    task_failure = None;
    commands =
      [
        Protocol.Schedule_activity
          {
            seq = 2L;
            activity_id = "benchmark-activity";
            activity_type = "benchmark";
            task_queue = "benchmark";
            arguments = [ payload ];
            schedule_to_close_timeout = Some { seconds = 10L; nanoseconds = 0 };
            schedule_to_start_timeout = None;
            start_to_close_timeout = None;
            heartbeat_timeout = None;
            retry_policy = None;
            priority = None;
            cancellation_type = Try_cancel;
            do_not_eagerly_execute = false;
          };
      ];
  }

(** The activation document as the bridge would deliver it, built once before
    any timed phase. *)
let activation_wire = unwrap (Protocol.encode_activation activation)

(** Runs one payload-heavy task's codec work and checks the decoded bytes. *)
let run_workload _seed =
  let decoded = unwrap (Protocol.decode_activation activation_wire) in
  ignore (unwrap (Protocol.encode_activation decoded));
  let encoded = unwrap (Protocol.encode_completion completion) in
  let decoded_completion = unwrap (Protocol.decode_completion encoded) in
  match (decoded.jobs, decoded_completion.commands) with
  | ( [ Protocol.Resolve_activity { result = Completed (Some result); _ } ],
      [ Protocol.Schedule_activity { arguments = [ argument ]; _ } ] )
    when Bytes.equal result.data payload.data
         && Bytes.equal argument.data payload.data ->
      ()
  | _ -> failwith "payload codec did not preserve the benchmark payloads"

(** Runs the bounded benchmark and emits a versioned JSON report on stdout. *)
let () =
  Benchmark_harness.run ~suite:"payload-codec"
    ~boundary:
      "OCaml workflow protocol decode and validation re-encode of one \
       activation with a 2 MiB result, plus encode and decode of one \
       completion with a 2 MiB argument; no Core, FFI, polling, server, or \
       network"
    ~server_version:"none"
    ~workload_config:
      [
        ("admitted_concurrency", `Int 1);
        ("admission_model", `String "closed_loop");
        ("pending_attempt_backlog_peak", `Int 0);
        ("saturation_observation", `String "not_exercised");
        ("payload_bytes", `Int payload_bytes);
        ("activation_document_bytes", `Int (String.length activation_wire));
      ]
    ~workload:run_workload ()
