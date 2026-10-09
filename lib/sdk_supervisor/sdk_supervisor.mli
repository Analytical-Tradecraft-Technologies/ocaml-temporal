(** Private owner-Domain supervision for one SDK instance.

    The supervisor serializes a complete native resource graph behind typed
    operations. Its entry points block ordinary producer Domains and therefore
    must be offloaded by any cooperative scheduler adapter. No backend state or
    native handle can be obtained through this interface. *)

(** A native resource graph and the typed operations which may use it. *)
module type Backend = sig
  (** Immutable information required to create one graph. *)
  type config

  (** Owner-confined graph state. The supervisor never returns this value to a
      producer and invokes all functions receiving it on one Domain. *)
  type state

  (** Expected backend failure copied into OCaml-owned memory. *)
  type error

  (** Typed operations over the graph. The parameter selects the successful
      result without exposing [state]. *)
  type _ operation

  (** Creates the complete initial graph on the owner Domain. Returning
      [Error] means no state was created and therefore no shutdown is needed. *)
  val create : config -> (state, error) result

  (** Executes one operation on owner-confined [state]. Expected operational
      failures use [Error]; exceptions denote backend defects. *)
  val perform : state -> 'result operation -> ('result, error) result

  (** Releases every resource in reverse ownership order. It is invoked at
      most once for successfully created state. An [Error] must still mean the
      backend consumed or invalidated the complete graph. *)
  val shutdown : state -> (unit, error) result
end

(** Builds one supervisor implementation for a private backend protocol. *)
module Make (Backend : Backend) : sig
  (** Failure visible to internal callers. Backend errors are expected;
      [Supervisor_failed] contains an unexpected owner-Domain defect. *)
  type error =
    | Backend of Backend.error
    | Closed
        (** The graph has begun or completed shutdown. *)
    | Supervisor_failed of exn
        (** The owner contained an unexpected exception and terminated. *)
    | Owner_unavailable of exn
        (** The owner Domain could not be spawned, for example because the
            runtime's Domain limit is reached. This is an operational failure
            that may succeed once another instance shuts down. *)

  (** An abstract instance containing the mailbox, owner Domain, and cached
      terminal result. It contains no publicly accessible native handle. *)
  type t

  (** [create ~capacity config] starts the owner Domain and creates the backend
      graph on it. [capacity] is the positive number of admitted operations
      which may wait behind the active operation; invalid capacity is a
      programmer error reported by [Invalid_argument]. Failure to spawn the
      owner Domain, for example because the runtime's Domain limit is reached,
      returns [Owner_unavailable] with the spawn exception. *)
  val create : capacity:int -> Backend.config -> (t, error) result

  (** [perform instance operation] blocks an ordinary producer Domain until
      the owner has executed the typed operation. Calls are serialized in the
      mailbox admission order. *)
  val perform : t -> 'result Backend.operation -> ('result, error) result

  (** [initiate_shutdown instance] atomically admits the one terminal request
      and closes operation admission, but does not wait for earlier backend
      work or join the owner Domain. Repeated calls are harmless. This is an
      internal lifecycle seam; application code should call [shutdown]. *)
  val initiate_shutdown : t -> unit

  (** [shutdown instance] waits for earlier admitted operations, releases the
      graph once, stops and joins the owner Domain, and caches the exact
      terminal result for every later call. Abandoning an instance schedules
      this same path on a dedicated system thread; the garbage-collector
      finalizer itself never waits on mailbox locks or native teardown. *)
  val shutdown : t -> (unit, error) result
end

(** Caller-side completion of submitted client calls (#807).

    A client request is submitted by the owner Domain, which returns at once;
    the caller then waits for the outcome on its own Domain. The owner never
    blocks on the network, so a pending wait, query, or update poll cannot
    delay other requests on the same supervisor. *)
module Client_call : sig
  (** A submitted call. [Completed] is an outcome produced without any RPC,
      such as a deadline that expired before dispatch. [In_flight] is a
      completion handle plus the pure decoder of its terminal native result;
      the decoder runs on the awaiting caller's Domain. *)
  type ('value, 'ticket) t =
    | Completed of ('value, Temporal_core_bridge.Native_bridge.error) result
    | In_flight of {
        ticket : 'ticket;
        decode :
          (bytes, Temporal_core_bridge.Native_bridge.error) result ->
          ('value, Temporal_core_bridge.Native_bridge.error) result;
      }

  (** [Closed]: the supervisor shut down while the call was in flight, so it
      will never complete. [Failed]: any other bridge failure. *)
  type failure = Closed | Failed of Temporal_core_bridge.Native_bridge.error

  (** Default length in milliseconds of one bounded completion wait. *)
  val default_slice_ms : int

  (** [await ~poll call] blocks the calling thread until [call] completes.
      [poll ticket ~timeout_ms] performs one bounded wait: [Not_ready]
      repeats it, [Invalid_state] is {!Closed}, and any other result is
      decoded. It must not run on a workflow scheduler Domain or on the
      supervisor owner Domain. *)
  val await :
    poll:
      ('ticket ->
      timeout_ms:int ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result) ->
    ?slice_ms:int ->
    ('value, 'ticket) t ->
    ('value, failure) result
end

(** The production supervisor over one native runtime-client-worker graph. *)
module Native : sig
  (** Pure conversion functions at the native-worker boundary. Keeping these
      functions private but independently testable makes both directions of
      the JSON contract reviewable without constructing a Core worker. *)
  module Protocol_adapter : sig
    val decode_workflow_activation :
      bytes ->
      ( Temporal_protocol.Workflow_protocol.activation,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Strictly validates one workflow activation returned by Rust. *)

    val workflow_poll_result :
      reject:(bytes -> (unit, Temporal_core_bridge.Native_bridge.error) result) ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( Temporal_protocol.Workflow_protocol.activation option,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Live-worker workflow poll. Maps native [Not_ready] to [None]. If
        successful bytes fail semantic validation, [reject] must retire their
        exact native lease first; a successful rejection is local task progress
        and also maps to [None], so one undecodable activation cannot end
        [Worker.run]. A failed rejection keeps the [Protocol] error. *)

    val replay_workflow_poll_result :
      reject:(bytes -> (unit, Temporal_core_bridge.Native_bridge.error) result) ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( Temporal_protocol.Workflow_protocol.activation option,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Replay-worker workflow poll. As {!workflow_poll_result}, except that
        the original [Protocol] error remains the result after a successful
        rejection: a replay must fail rather than skip history it could not
        check. *)

    val decode_activity_task :
      bytes ->
      ( Temporal_protocol.Activity_protocol.task,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Strictly validates one remote activity task returned by Rust. *)

    val activity_poll_result :
      reject:(bytes -> (unit, Temporal_core_bridge.Native_bridge.error) result) ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( Temporal_protocol.Activity_protocol.task option,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Maps native [Not_ready] to [None]. If successful bytes fail semantic
        validation, [reject] must retire their exact native lease first; a
        successful rejection (Rust fails the task back to Core) also maps to
        [None], so one undecodable task cannot end [Worker.run]. A failed
        rejection keeps the [Protocol] error. *)

    val encode_activity_completion :
      Temporal_protocol.Activity_protocol.completion ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes and reparses one remote activity completion. *)

    val encode_activity_heartbeat :
      Temporal_protocol.Activity_protocol.heartbeat ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes and reparses one activity heartbeat. *)

    val expired_rpc_deadline_error : Temporal_core_bridge.Native_bridge.error
    (** The [Connection] failure with the closed [rpc] [deadline_exceeded]
        document that Rust returns when an RPC budget expires. The supervisor
        feeds it to a request's result decoder when the deadline expired
        before dispatch. *)

    val with_rpc_deadline :
      now_ns:int64 ->
      Temporal_protocol.Client_protocol.rpc_deadline option ->
      expired:(unit -> 'a) ->
      live:(Temporal_protocol.Client_protocol.rpc_deadline option -> 'a) ->
      'a
    (** Resolves a client request's caller deadline (#499) at owner-Domain
        dispatch time [now_ns] on the monotonic clock. [live] receives
        [None] when there is no deadline, or the deadline lowered to the
        budget that remains, which is what the request then sends; time the
        request spent queued behind earlier calls therefore counts. When the
        deadline has expired, [expired ()] runs instead and nothing is
        encoded or sent. *)

    val encode_client_start_request :
      Temporal_protocol.Client_protocol.start_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes one typed workflow-start request. *)

    val encode_client_cancel_request :
      Temporal_protocol.Client_protocol.cancel_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes one typed exact-run cancellation request. *)

    val encode_client_reset_request :
      Temporal_protocol.Client_protocol.reset_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes one typed exact-run reset request. *)

    val encode_client_signal_request :
      Temporal_protocol.Client_protocol.signal_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes one typed exact-run signal request. *)

    val encode_client_query_request :
      Temporal_protocol.Client_protocol.query_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes one typed exact-run output-only query request. *)

    val decode_client_start_ticket :
      Temporal_protocol.Client_protocol.start_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( ( Temporal_protocol.Client_protocol.start_ticket,
          Temporal_protocol.Client_protocol.client_error )
        result,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Decodes and binds one native asynchronous-start ticket to its request. *)

    val encode_client_start_ticket :
      Temporal_protocol.Client_protocol.start_ticket ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes one previously validated asynchronous-start
        ticket. *)

    val encode_client_wait_request :
      Temporal_protocol.Client_protocol.wait_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes one typed exact-run wait request. *)

    val decode_client_start_result :
      Temporal_protocol.Client_protocol.start_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( ( Temporal_protocol.Client_protocol.start_response,
          Temporal_protocol.Client_protocol.client_error )
        result,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Decodes a native start result, retaining structured client failures as
        an inner [Error] and reporting malformed bridge data as an outer
        protocol error. *)

    val decode_client_start_outcome :
      Temporal_protocol.Client_protocol.start_ticket ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( Temporal_protocol.Client_protocol.start_outcome option,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Decodes one terminal asynchronous-start outcome. [Not_ready] becomes
        [Ok None] so poll and bounded-wait callers can retry without treating
        an in-flight request as a failure. *)

    val decode_client_submitted_start_outcome :
      Temporal_protocol.Client_protocol.start_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( Temporal_protocol.Client_protocol.start_outcome,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Decodes a submitted start's terminal outcome document, correlated
        with its request (#807). Native failures are preserved; the caller
        treats them as an uncertain start. *)

    val decode_client_wait_result :
      Temporal_protocol.Client_protocol.wait_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( ( Temporal_protocol.Client_protocol.wait_response,
          Temporal_protocol.Client_protocol.client_error )
        result,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Decodes a native exact-run wait result. [Not_ready] remains an outer
        bridge status so callers can retry without a fake terminal outcome. *)

    val decode_client_cancel_result :
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( (unit, Temporal_protocol.Client_protocol.client_error) result,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Decodes the cancellation acknowledgement and preserves structured
        server failures as an inner typed result. *)

    val decode_client_reset_result :
      Temporal_protocol.Client_protocol.reset_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( ( Temporal_protocol.Client_protocol.reset_response,
          Temporal_protocol.Client_protocol.client_error )
        result,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Decodes the new run identity after a reset and preserves structured
        server failures as an inner typed result. *)

    val encode_client_terminate_request :
      Temporal_protocol.Client_protocol.terminate_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes one exact-run termination request. *)

    val decode_client_terminate_result :
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( (unit, Temporal_protocol.Client_protocol.client_error) result,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Decodes the termination acknowledgement and preserves structured
        server failures as an inner typed result. *)

    val decode_client_signal_result :
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( (unit, Temporal_protocol.Client_protocol.client_error) result,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Decodes the signal acknowledgement and preserves structured server
        failures as an inner typed result. *)

    (** Decodes query result payloads and preserves structured server failures
        as an inner typed result. *)
    val decode_client_query_result :
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( ( Temporal_protocol.Client_protocol.payload list,
          Temporal_protocol.Client_protocol.client_error )
        result,
        Temporal_core_bridge.Native_bridge.error )
      result
    val encode_client_visibility_request :
      Temporal_protocol.Client_protocol.visibility_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes one bounded visibility request. *)

    val decode_client_visibility_result :
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      (Temporal_protocol.Client_protocol.visibility_page,
       Temporal_core_bridge.Native_bridge.error) result
    (** Strictly decodes one visibility page returned by Rust. *)

    val encode_client_update_request :
      Temporal_protocol.Client_protocol.update_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes one typed workflow-update admission request. *)

    val encode_client_poll_update_request :
      Temporal_protocol.Client_protocol.poll_update_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes one typed workflow-update completion poll. *)

    val decode_client_update_result :
      Temporal_protocol.Client_protocol.update_request ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( ( Temporal_protocol.Client_protocol.update_response,
          Temporal_protocol.Client_protocol.client_error )
        result,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Decodes update admission and preserves structured server failures. *)

    val decode_client_poll_update_result :
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( ( Temporal_protocol.Client_protocol.poll_update_response,
          Temporal_protocol.Client_protocol.client_error )
        result,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Decodes one update completion poll and preserves structured failures. *)
  end

  (** Validated client settings whose representation remains bridge-private. *)
  type client_config

  (** Validated workflow-only worker settings whose representation is private. *)
  type worker_config

  (** A client request submitted through the owner (#807), indexed by the
      typed value its caller receives: a start yields its closed
      accepted/rejected/unknown outcome, and every other request yields the
      same value as the matching synchronous operation below. *)
  type _ client_rpc =
    | Rpc_start :
        Temporal_protocol.Client_protocol.start_request ->
        Temporal_protocol.Client_protocol.start_outcome client_rpc
    | Rpc_wait :
        Temporal_protocol.Client_protocol.wait_request ->
        ( Temporal_protocol.Client_protocol.wait_response,
          Temporal_protocol.Client_protocol.client_error )
        result
        client_rpc
    | Rpc_cancel :
        Temporal_protocol.Client_protocol.cancel_request ->
        (unit, Temporal_protocol.Client_protocol.client_error) result client_rpc
    | Rpc_terminate :
        Temporal_protocol.Client_protocol.terminate_request ->
        (unit, Temporal_protocol.Client_protocol.client_error) result client_rpc
    | Rpc_reset :
        Temporal_protocol.Client_protocol.reset_request ->
        ( Temporal_protocol.Client_protocol.reset_response,
          Temporal_protocol.Client_protocol.client_error )
        result
        client_rpc
    | Rpc_signal :
        Temporal_protocol.Client_protocol.signal_request ->
        (unit, Temporal_protocol.Client_protocol.client_error) result client_rpc
    | Rpc_query :
        Temporal_protocol.Client_protocol.query_request ->
        ( Temporal_protocol.Client_protocol.payload list,
          Temporal_protocol.Client_protocol.client_error )
        result
        client_rpc
    | Rpc_update :
        Temporal_protocol.Client_protocol.update_request ->
        ( Temporal_protocol.Client_protocol.update_response,
          Temporal_protocol.Client_protocol.client_error )
        result
        client_rpc
    | Rpc_poll_update :
        Temporal_protocol.Client_protocol.poll_update_request ->
        ( Temporal_protocol.Client_protocol.poll_update_response,
          Temporal_protocol.Client_protocol.client_error )
        result
        client_rpc
    | Rpc_visibility :
        Temporal_protocol.Client_protocol.visibility_request ->
        Temporal_protocol.Client_protocol.visibility_page client_rpc

  (** A submitted native call, awaited with {!await_client_call}. *)
  type 'value client_call =
    ('value, Temporal_core_bridge.Native_bridge.client_call) Client_call.t

  (** Typed lifecycle operations serialized by the owner Domain. *)
  type _ operation =
    | Check_compatibility : unit operation
    | Connect_client : client_config -> unit operation
    | Client_start_workflow :
        Temporal_protocol.Client_protocol.start_request ->
        ( Temporal_protocol.Client_protocol.start_response,
          Temporal_protocol.Client_protocol.client_error )
        result operation
        (** Starts one workflow using a typed request and returns either the
            correlated execution or a structured client failure. *)
    | Client_begin_start_workflow :
        Temporal_protocol.Client_protocol.start_request ->
        ( Temporal_protocol.Client_protocol.start_ticket,
          Temporal_protocol.Client_protocol.client_error )
        result operation
        (** Admits one asynchronous workflow start and returns an opaque ticket
            bound to the original request. *)
    | Client_poll_start_workflow :
        Temporal_protocol.Client_protocol.start_ticket ->
        Temporal_protocol.Client_protocol.start_outcome option operation
        (** Polls an asynchronous start without waiting. [None] means the
            Rust task is still in flight. *)
    | Client_wait_start_workflow :
        Temporal_protocol.Client_protocol.start_ticket ->
        Temporal_protocol.Client_protocol.start_outcome option operation
        (** Waits one bounded interval for an asynchronous start. [None] means
            the bounded interval elapsed without a terminal outcome. *)
    | Client_wait_workflow :
        Temporal_protocol.Client_protocol.wait_request ->
        ( Temporal_protocol.Client_protocol.wait_response,
          Temporal_protocol.Client_protocol.client_error )
        result operation
        (** Waits for one exact run using a typed request. [Not_ready] remains
            the outer bridge result and is safe to retry. *)
    | Client_cancel_workflow :
        Temporal_protocol.Client_protocol.cancel_request ->
        (unit, Temporal_protocol.Client_protocol.client_error) result operation
        (** Requests cancellation of one exact run. The acknowledgement does
            not itself prove that the run has reached its cancelled outcome. *)
    | Client_reset_workflow :
        Temporal_protocol.Client_protocol.reset_request ->
        ( Temporal_protocol.Client_protocol.reset_response,
          Temporal_protocol.Client_protocol.client_error )
        result operation
        (** Resets one exact run and returns the new run identity. *)
    | Client_terminate_workflow :
        Temporal_protocol.Client_protocol.terminate_request ->
        (unit, Temporal_protocol.Client_protocol.client_error) result operation
        (** Terminates one exact run immediately. *)
    | Client_signal_workflow :
        Temporal_protocol.Client_protocol.signal_request ->
        (unit, Temporal_protocol.Client_protocol.client_error) result operation
        (** Sends one signal to one exact run. The acknowledgement does not
            itself prove that workflow code has processed the signal. *)
    | Client_list_visibility_workflows :
        Temporal_protocol.Client_protocol.visibility_request ->
        Temporal_protocol.Client_protocol.visibility_page operation
        (** Lists one bounded visibility page. *)
    | Client_query_workflow :
        Temporal_protocol.Client_protocol.query_request ->
        ( Temporal_protocol.Client_protocol.payload list,
          Temporal_protocol.Client_protocol.client_error )
        result operation
        (** Executes one output-only query against one exact run. *)
    | Client_update_workflow :
        Temporal_protocol.Client_protocol.update_request ->
        ( Temporal_protocol.Client_protocol.update_response,
          Temporal_protocol.Client_protocol.client_error )
        result operation
        (** Admits one typed workflow update and returns its durable update ID. *)
    | Client_poll_update_workflow :
        Temporal_protocol.Client_protocol.poll_update_request ->
        ( Temporal_protocol.Client_protocol.poll_update_response,
          Temporal_protocol.Client_protocol.client_error )
        result operation
        (** Waits for one admitted update to reach a terminal outcome. *)
    | Start_worker : worker_config -> unit operation
    | Start_replay_worker : worker_config -> unit operation
        (** Starts a workflow-only replay worker without a Temporal client. *)
    | Feed_replay_history : bytes -> unit operation
        (** Queues one strict replay-history JSON document. *)
    | Finish_replay_input : unit operation
        (** Closes replay input after the final history has been admitted. *)
    | Try_poll_replay_workflow :
        Temporal_protocol.Workflow_protocol.activation option operation
        (** Takes one ready replay activation. *)
    | Wait_replay_workflow : unit operation
        (** Waits for replay readiness without consuming an activation. *)
    | Complete_replay_workflow :
        Temporal_protocol.Encoded_workflow_completion.t -> unit operation
        (** Completes one previously leased replay activation with the
            canonical bytes from the completion's single encoder pass. The
            supervisor does not encode it again. *)
    | Reject_replay_workflow : bytes -> unit operation
        (** Retires a replay activation rejected by OCaml semantic decoding. *)
    | Finalize_replay : unit operation
        (** Finalizes a naturally drained replay, retaining it on failure. *)
    | Dispose_replay : unit operation
        (** Explicitly abandons replay and force-completes native debts. *)
    | Try_poll_workflow :
        Temporal_protocol.Workflow_protocol.activation option operation
        (** Takes and validates one already-ready workflow activation. [None]
            means the native lane was empty at that instant. *)
    | Wait_workflow : unit operation
        (** Waits for workflow readiness through the native event mechanism. The
            bridge releases the OCaml runtime lock while this bounded wait is in
            progress, so it never blocks an OCaml workflow scheduler. *)
    | Complete_workflow :
        Temporal_protocol.Encoded_workflow_completion.t -> unit operation
        (** Submits one workflow completion as the canonical bytes from its
            single encoder pass. The value can only be built by
            [Encoded_workflow_completion.encode], so it is already validated;
            the supervisor copies it into the C call without encoding it
            again (issue #846). *)
    | Try_poll_activity :
        Temporal_protocol.Activity_protocol.task option operation
        (** Takes and validates one already-ready remote activity task. [None]
            means the native lane was empty at that instant. *)
    | Wait_activity : unit operation
        (** Waits for activity readiness with the same runtime-lock-free bridge
            contract as [Wait_workflow]. *)
    | Wait_any : unit operation
        (** Waits until either worker lane is ready, under the same bounded
            runtime-lock-free contract as [Wait_workflow]. The worker loop uses
            it for its single idle native wait so a task on the lane it did not
            expect is not delayed by the bounded timeout. *)
    | Wait_activity_completion_retry_backoff : unit operation
        (** Applies the fixed native delay used only after an explicit
            retryable activity-completion transport outcome. The supervisor
            owner Domain performs it while the C stub releases the OCaml
            runtime lock. *)
    | Complete_activity :
        Temporal_protocol.Activity_protocol.completion -> unit operation
        (** Validates and submits one typed remote activity completion. *)
    | Complete_async_activity :
        Temporal_protocol.Activity_protocol.completion -> unit operation
        (** Completes an activity after a [WillCompleteAsync] handoff through
            the namespace-bound client path. *)
    | Record_activity_heartbeat :
        Temporal_protocol.Activity_protocol.heartbeat -> unit operation
        (** Records progress for a leased activity without retiring it. The
            operation is acknowledgement-only; cancellation, pause, and reset
            flags arrive later as an activity Cancel task from Core. *)
    | Record_async_activity_heartbeat :
        Temporal_protocol.Activity_protocol.heartbeat -> unit operation
        (** Records progress for an admitted asynchronous activity. *)
    | Shutdown_worker : unit operation
    | Disconnect_client : unit operation
    | Client_submit : 'value client_rpc -> 'value client_call operation
        (** Submits one client request without waiting for Temporal (#807).
            The owner resolves the request's RPC deadline, validates and
            encodes it, and spawns native work; a deadline that expired in
            the mailbox completes the call at once and sends nothing. The
            caller then awaits the result with {!await_client_call}. *)

  (** Production lifecycle and bridge failures. *)
  type error =
    | Backend of Temporal_core_bridge.Native_bridge.error
    | Closed
    | Supervisor_failed of exn
    | Owner_unavailable of exn

  (** An abstract SDK instance which owns the complete Rust handle graph. *)
  type t

  (** Validates client settings without network access. *)
  val client_config :
    target_url:string -> identity:string -> (client_config, Temporal_core_bridge.Native_bridge.error) result

  (** Validates explicit worker resource settings without network access.
      [workflow_tasks] and [activity_tasks] select the task kinds Core polls
      and [tuning] carries the optional Core settings of #498; see
      {!Temporal_core_bridge.Native_bridge.val-worker_config}. *)
  val worker_config :
    namespace:string ->
    task_queue:string ->
    build_id:string ->
    ?versioning:Temporal_core_bridge.Native_bridge.worker_versioning ->
    max_cached_workflows:int ->
    max_outstanding_workflow_tasks:int ->
    max_concurrent_workflow_task_polls:int ->
    graceful_shutdown_timeout_ms:int64 ->
    ?tuning:Temporal_core_bridge.Native_bridge.worker_tuning ->
    ?workflow_tasks:bool ->
    ?activity_tasks:bool ->
    unit ->
    (worker_config, Temporal_core_bridge.Native_bridge.error) result

  (** Starts a dedicated owner Domain and creates the real Rust runtime.
      [runtime_threads] bounds that runtime's Tokio worker pool; see
      {!Temporal_core_bridge.Native_bridge.runtime_create}. An out-of-range
      count is a [Backend] error with status [Invalid_argument].

      With [runtime], the graph is instead attached to that shared Core
      runtime (#832) and [runtime_threads] is ignored. The supervisor takes
      ownership of the lease: it releases it after closing its graph during
      [shutdown], or before returning if creation fails. *)
  val create :
    ?runtime_threads:int ->
    ?runtime:Sdk_shared_runtime.lease ->
    capacity:int ->
    unit ->
    (t, error) result

  (** Runs one typed bridge operation on the sole owner Domain. Network waits
      enter Rust through C stubs which release the OCaml runtime lock. *)
  val perform : t -> 'result operation -> ('result, error) result

  (** Awaits one submitted call on the calling Domain with the OCaml runtime
      lock released during each bounded wait. A call closed because the
      supervisor shut down while it was in flight is the typed [Closed]
      error; any other failure is a [Backend] error. Must not be called on a
      workflow scheduler Domain. [slice_ms] defaults to
      {!Client_call.default_slice_ms}. *)
  val await_client_call :
    ?slice_ms:int -> 'value client_call -> ('value, error) result

  (** [call supervisor rpc] submits [rpc] with [perform] and then awaits it
      with {!await_client_call}. Only submission occupies the owner Domain,
      so concurrent calls from other Domains proceed independently. *)
  val call : t -> 'value client_rpc -> ('value, error) result

  (** Closes operation admission without waiting for native teardown. This is
      an internal lifecycle seam; application code should call [shutdown]. *)
  val initiate_shutdown : t -> unit

  (** Deterministically closes worker, client, runtime, then owner Domain. *)
  val shutdown : t -> (unit, error) result
end
