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

(** Runs both lanes until [closed] or a fatal lane result. The activity Domain
    is joined before returning, so the caller may then safely drain adapters
    and release the native graph. An error in either lane stops the other through
    a separate per-run signal; it never changes the worker shutdown flag.

    Both [wait_for_lane] and [retry_pending] must be bounded. The latter must
    apply a real backoff even if unrelated work is ready, so an uncertain
    completion cannot spin or trigger a second callback invocation. *)
val run :
  closed:(unit -> bool) ->
  poll_workflow:(unit -> (progress, 'error) result) ->
  poll_activity:(unit -> (progress, 'error) result) ->
  wait_for_lane:(workflow_lane:bool -> (unit, 'error) result) ->
  retry_pending:(workflow_lane:bool -> (unit, 'error) result) ->
  (unit, 'error) result
