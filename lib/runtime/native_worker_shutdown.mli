(** Bounded shutdown orchestration for the private native worker (#495).

    [Temporal.Worker.shutdown] must return within a documented bound even when
    an activity callback ignores cancellation, a workflow activation never
    yields, a retained completion cannot be delivered, or the Temporal server
    is unreachable. OCaml cannot preempt a running callback, so the bound is
    achieved by never waiting on one indefinitely:

    + {b Lanes.} The caller first closes admission (so both execution lanes
      stop polling and activity contexts report a worker shutdown), then waits
      until [lanes_deadline] plus {!lanes_slack_s} for the run loop to release
      its lifecycle lock. A lane that is still inside user code, or inside a
      native call, at that point is {e abandoned}: shutdown proceeds without
      it and never waits for it again.
    + {b Drain.} Each adapter's retained completions are retried while the
      adapter is idle, until the same lanes deadline (at least one attempt).
      An adapter whose lock is still held by abandoned code is skipped.
    + {b Release.} The native graph is released. Temporal Core's bridge
      completes every task still leased to OCaml as a retryable failure (an
      abandoned activity) or a failed workflow task, so the server retries it
      on another worker.

    The whole sequence runs on one dedicated system thread, the {e shutdown
    thread}, which is the sole owner of the lifecycle lock it acquires, of
    the drains, and of the native release. The caller only waits for that
    thread's published outcome, until [teardown_timeout_s] after the release
    began and never past the bound stated on {!run}. If the release has not
    finished by then, the caller returns
    a report whose teardown is {!Detached}: the shutdown thread still owns
    and completes the release (the bridge bounds its own Core waits), and the
    caller never releases anything itself. If no thread can be created, the
    sequence runs on the caller instead and is bounded only by the bridge.

    The module is generic over the error type and receives every native or
    adapter effect as a closure, so the policy is testable with fake
    backends and no Temporal server. *)

(** Time source and sleep used by every bounded wait, in seconds. Tests
    inject a controllable clock; production uses {!system_clock}. *)
type clock = { now : unit -> float; sleep : float -> unit }

(** [Unix.gettimeofday] and [Thread.delay]. [Thread.delay] releases the
    calling Domain's runtime lock while sleeping. A backwards wall-clock
    adjustment can only lengthen a wait by the size of the adjustment. *)
val system_clock : clock

(** How long past [lanes_deadline] the lanes phase keeps waiting before it
    abandons a lane: 250 ms. The run loop detaches a stuck activity lane at
    [lanes_deadline] itself and an idle lane observes the stop within one
    100 ms native readiness wait, so the slack lets both hand the lifecycle
    lock back before shutdown gives up on them. *)
val lanes_slack_s : float

(** Whether the native release finished before the caller's deadline. *)
type teardown =
  | Completed
      (** The release returned, successfully or with the error reported
          alongside, before the caller stopped waiting. *)
  | Detached
      (** The caller's deadline passed first. The shutdown thread still owns
          the release and completes it in the background. *)

(** What a bounded shutdown left behind. Counts are taken when the lanes
    phase ended. *)
type report = {
  elapsed_s : float;  (** Seconds from {!run} being called to its return. *)
  lanes_stopped : bool;
      (** [true] when every execution lane returned within the lanes phase.
          [false] when a lane was abandoned: still running user code (counted
          below) or blocked in a native call (counted in neither field). *)
  abandoned_activity_callbacks : int;
      (** Activity callbacks still running when the lanes phase ended. *)
  abandoned_workflow_activations : int;
      (** Workflow activations still in flight when the lanes phase ended. *)
  teardown : teardown;
}

(** Result of one non-blocking adapter drain attempt. *)
type 'error drain =
  | Drained  (** The adapter holds no retained completion. *)
  | Busy
      (** The adapter lock is held by a lane that has not returned, so the
          drain was skipped without waiting. *)
  | Drain_failed of { error : 'error; retryable : bool }
      (** A retained completion was not accepted. [retryable] repeats the
          adapter's own classification: only [true] permits another attempt
          of the exact same completion. *)

(** Result of the native release, classified by the caller's closure. Every
    constructor except an exception means the native graph was consumed. *)
type 'error release =
  | Released  (** The graph was closed cleanly. *)
  | Released_retiring_leases of 'error
      (** The graph was closed, and the bridge had to complete tasks still
          leased to OCaml on its behalf. The error is the bridge's
          diagnostic, reported only when no lane was abandoned. *)
  | Release_failed of 'error
      (** The graph was closed, but teardown reported this failure. *)

(** Effects the orchestration needs from one worker. Every closure is called
    only on the shutdown thread, except that [activity_callback_running] and
    [workflow_activation_in_flight] may also be called by the caller if the
    shutdown thread has not published the lanes outcome in time; both must
    therefore be non-blocking and safe from any thread. *)
type 'error operations = {
  try_acquire_lanes : unit -> bool;
      (** Takes the run loop's lifecycle lock without waiting. [true] proves
          that no [run] is executing workflow code or joining the activity
          lane, and keeps it so until [release_lanes]. *)
  release_lanes : unit -> unit;
      (** Releases the lock taken by a successful [try_acquire_lanes]. Called
          on the same thread, exactly once per successful acquisition. *)
  activity_lane_detached : unit -> bool;
      (** Whether a [run] returned without joining its activity lane because a
          callback outlived the deadline. *)
  activity_callback_running : unit -> bool;
      (** Whether a synchronous activity callback is executing now. *)
  workflow_activation_in_flight : unit -> bool;
      (** Whether a workflow activation is between poll and completion now. *)
  drain_workflow : unit -> 'error drain;
  drain_activity : unit -> 'error drain;
  outstanding_async_leases : unit -> int;
      (** Admitted asynchronous activity leases, read without the activity
          adapter lock. Consulted only when the activity drain reported
          [Busy]: such a drain cannot see these leases, and they belong to
          external code, not to the abandoned callback. Must never wait. *)
  async_leases_error : int -> 'error;
      (** The error reporting this many async leases still admitted when the
          grace period ended. *)
  release : unit -> 'error release;
      (** Releases the native graph and discards adapter state it proved
          retired. An exception means the release outcome is unknown; the
          closure itself must then have arranged a retry. *)
  exception_error : exn -> 'error;
      (** Converts an exception raised by [drain_*] into a typed defect. *)
}

(** Final outcome. The graph was released (or its release is still owned by
    the shutdown thread, for {!Detached}) in every case. *)
type 'error outcome =
  | Shut_down of report
      (** Every retained completion was delivered or belonged to abandoned
          work, and the release succeeded or retired only abandoned work. *)
  | Completion_lost of { error : 'error; report : report }
      (** A retained completion could not be delivered before the deadline,
          or failed permanently, or an asynchronous activity lease admitted
          before shutdown was still outstanding at the deadline while the
          activity adapter was held by an abandoned callback. The graph was
          still force-released, so such a handle can no longer complete;
          the leases the bridge retired are therefore never attributed to
          the abandoned callback alone. *)
  | Release_error of { error : 'error; report : report }
      (** The release reported a failure, or it retired leases although no
          lane was abandoned (which would otherwise be a false success). *)
  | Release_unproven of report
      (** The release raised before proving that the graph was consumed. *)

(** [run ~lanes_deadline ~teardown_timeout_s operations] performs the
    sequence described above. With [started] the time of the call, it
    returns by [max started lanes_deadline +. lanes_slack_s
    +. teardown_timeout_s] plus scheduling delay, unless no shutdown thread
    could be created. The caller
    must already have closed admission so the lanes are stopping. It is not
    re-entrant for one worker; the native worker's admission flag guarantees
    a single call. *)
val run :
  ?clock:clock ->
  lanes_deadline:float ->
  teardown_timeout_s:float ->
  'error operations ->
  'error outcome

(** Deferred adapter discard (#495). After the native release, the copied
    adapter state (retained completions, run maps, async handles) must be
    discarded, but an abandoned callback or activation may still hold an
    adapter lock. The release then leaves the discard pending, and whichever
    thread next releases an adapter lock retries it: the detached activity
    lane after its callback returns, or [run] after its workflow lane
    returns. The flag is raised {e before} each attempt and cleared only by a
    successful one, so a lane that unlocks while the release is failing its
    attempt still observes the flag afterwards and no retry is lost.
    [try_discard] must be idempotent and non-blocking. *)
module Deferred_discard : sig
  (** The pending flag of one worker. *)
  type t

  (** No discard pending. *)
  val create : unit -> t

  (** Called once the native release has returned: raises the flag and
      attempts [try_discard] at once. *)
  val request : t -> try_discard:(unit -> bool) -> unit

  (** Called by a lane after it released an adapter lock: attempts
      [try_discard] only if a discard is pending. *)
  val retry : t -> try_discard:(unit -> bool) -> unit

  (** Whether a discard is still pending. *)
  val pending : t -> bool
end
