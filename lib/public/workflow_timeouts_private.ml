(** Workflow execution, run, and task timeout validation shared by
    [Client.start] and child workflow starts, so both ends of the SDK reject
    exactly the same values before any request or command exists. The
    protocol encoders and the Rust bridge re-check the same rules. *)

module Client_protocol = Temporal_sdk_kernel.Client_protocol

(** Checks optional timeouts and converts them to exact milliseconds. A zero
    timeout is rejected because Temporal reads zero as "unset" and
    would silently apply its default; a task timeout above 120 seconds or
    above the run timeout (the execution timeout when there is no run
    timeout) and a run timeout above the execution timeout are rejected
    because Temporal would silently lower them. Negative and overflowing
    values cannot be built as [Duration.t]. The result is the exact
    millisecond triple [(execution, run, task)]; a violation is a typed
    defect naming the offending option. *)
let validate ~execution_timeout ~run_timeout ~task_timeout =
  let timeout label maximum = function
    | None -> Ok None
    | Some duration ->
        let milliseconds = Duration.to_ms duration in
        if Int64.compare milliseconds 0L <= 0 then
          Error (Error.defect ~message:(label ^ " must be positive"))
        else if Int64.compare milliseconds maximum > 0 then
          Error (Error.defect ~message:(label ^ " exceeds its maximum"))
        else Ok (Some milliseconds)
  in
  let ( let* ) = Result.bind in
  let* execution_timeout_ms =
    timeout "execution_timeout" Client_protocol.max_workflow_timeout_ms
      execution_timeout
  in
  let* run_timeout_ms =
    timeout "run_timeout" Client_protocol.max_workflow_timeout_ms run_timeout
  in
  let* task_timeout_ms =
    timeout "task_timeout (at most 120 seconds)"
      Client_protocol.max_workflow_task_timeout_ms task_timeout
  in
  let* () =
    match (execution_timeout_ms, run_timeout_ms) with
    | Some execution, Some run when Int64.compare run execution > 0 ->
        Error
          (Error.defect ~message:"run_timeout exceeds execution_timeout")
    | _ -> Ok ()
  in
  let* () =
    (* A run without its own timeout is bounded by the execution timeout. *)
    let run =
      match run_timeout_ms with Some _ -> run_timeout_ms | None -> execution_timeout_ms
    in
    match (task_timeout_ms, run) with
    | Some task, Some run when Int64.compare task run > 0 ->
        Error
          (Error.defect
             ~message:"task_timeout exceeds the run (or execution) timeout")
    | _ -> Ok ()
  in
  Ok (execution_timeout_ms, run_timeout_ms, task_timeout_ms)
