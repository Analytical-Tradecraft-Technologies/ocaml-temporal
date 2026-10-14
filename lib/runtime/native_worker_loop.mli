(** Private scheduler for two independent native worker lanes.

    The workflow adapter executes on the caller Domain; one activity Domain
    executes at most one callback at a time. The adapters still serialize their
    own task leases, and every native operation uses the same supervisor. *)

(** Scheduling summary returned by one lane poll. *)
type progress =
  | Progress
      (** A task was processed or a task-level failure was acknowledged. *)
  | Not_ready
      (** The lane had no work at this instant. *)
  | Retry_pending
      (** The exact retained completion needs a bounded backoff before retry. *)

(** Bounds how long [run] waits for the activity lane once the workflow lane
    has returned (#495). [deadline ()] is called once, when the workflow lane
    returns, and yields an absolute time on the [now] clock. If
    [callback_running ()] reports an activity callback still executing at or
    after that time, [run] calls [on_detached] and returns without joining
    it. Any other lane (idle in a readiness wait, between polls, or in a
    short native call) is joined instead for up to
    {!idle_detach_allowance_s} past the deadline, so a short grace period
    never detaches a lane that has no callback (#1023). [callback_running]
    must be non-blocking; it is sampled every join poll, so a callback that
    starts after the deadline is still detached.

    [on_stop] is started once, when the workflow lane has returned because
    [closed] holds, before the activity lane is joined. The native worker
    uses it to stop Temporal Core polling at once rather than after the grace
    period (#1028). It runs fire-and-forget on its own system thread, so it
    may block (for example on a supervisor mailbox that a stuck activity-lane
    native call occupies) without delaying the bounded join: [run] still
    returns by the deadline. If no thread can be created it is skipped; the
    caller must then stop polling another way. It is not called after a lane
    failure without a stop. An exception it raises is ignored. *)
type activity_detach = {
  now : unit -> float;
  deadline : unit -> float;
  on_detached : unit -> unit;
  on_stop : unit -> unit;
  callback_running : unit -> bool;
}

(** How long past its deadline the bounded join keeps waiting for an idle
    activity lane before detaching it anyway: 150 ms. An idle lane exits
    within one bounded readiness wait (at most 100 ms, and immediately once
    native polling has stopped), so the allowance is not normally reached.
    It is smaller than the bounded-shutdown lanes slack, so a joined [run]
    still returns before shutdown stops waiting for its lifecycle lock. *)
val idle_detach_allowance_s : float

(** Runs both lanes until [closed] or a fatal lane result. With [detach = None]
    the activity Domain is always joined before returning, so the caller may
    then safely drain adapters and release the native graph. With [Some detach]
    the join is bounded as described on {!activity_detach}: a detached
    activity Domain keeps running its callback, still holding the activity
    adapter's lock, and exits on its own once the callback returns, because
    the stop signal is already set. The caller must then treat the activity
    adapter as possibly busy. An error in either lane stops the other through
    a separate per-run signal; it never changes the worker shutdown flag. A
    stuck workflow lane cannot be detached: it is the calling thread.

    [wait_for_lane] receives [native_wait = true] only when both lanes are idle
    and this lane holds the single native-wait token. That bounded native wait
    must end as soon as {e either} lane has work, not only the token holder's:
    the token rotates between lanes, so a lane-specific wait would sleep
    through the sibling's task for the whole bound on every other step of a
    sequential workflow (#806). Otherwise it must perform a short bounded local
    yield without entering the supervisor mailbox. That yield may be zero only
    when a bounded event justifies an immediate repoll, as
    {!Native_worker_idle} does, so the loop never busy-spins (#994).
    [retry_pending] must apply a real bounded backoff even if unrelated work is
    ready, so an uncertain completion cannot spin or rerun its callback. *)
val run :
  detach:activity_detach option ->
  closed:(unit -> bool) ->
  poll_workflow:(unit -> (progress, 'error) result) ->
  poll_activity:(unit -> (progress, 'error) result) ->
  wait_for_lane:(workflow_lane:bool -> native_wait:bool -> (unit, 'error) result) ->
  retry_pending:(workflow_lane:bool -> (unit, 'error) result) ->
  (unit, 'error) result
