(** Private idle pacing for the two native worker lanes (#994).

    At most one idle lane holds the supervisor in the native readiness wait
    ({!Native_worker_loop}); the other idle lane used to sleep a fixed 10 ms
    between polls. In a serial activity workflow every step crosses lanes, so
    that sleep, not the server, dominated latency: a 1,000-activity fan-out
    spent about two thirds of its time asleep.

    This module replaces the fixed sleep with two rules, both decided from
    per-run state and never from workflow data:

    {ol
    {- {b Join an in-flight native wait.} While the sibling lane is inside the
       native readiness wait, the supervisor is occupied by that wait, so a
       poll from this lane queues behind it and runs as soon as the wait ends,
       which happens when {e either} lane has work. The lane therefore polls
       without sleeping, and the poll itself is the readiness wait. To rule
       out a spin if a poll is ever served before the wait (or by a fast path
       that never enters the supervisor), a lane joins each native wait at
       most once.

       The wait is marked in flight just before its request is admitted to
       the supervisor mailbox, so the joining poll can win that race and
       return empty while the wait is still in flight. A poll queued behind
       the wait cannot return before the wait ends, so a lane that finds the
       wait it joined still in flight knows its poll was early. It then
       restarts its backoff at [first_delay_s] for that wait instead of
       resuming a sleep of up to [max_delay_s]: its next poll, a fraction of
       a millisecond later, queues behind the now admitted wait.}
    {- {b Adaptive backoff otherwise.} When no native wait is in flight (the
       sibling is busy running a task), the lane sleeps 0, then
       [first_delay_s], doubling up to [max_delay_s]. The sequence restarts at
       0 after a work event: a lane completed a task ({!note_work}), or a
       native wait reported readiness ({!note_ready}). Work is therefore
       noticed within roughly the time since the last event rather than a fixed 10 ms, while a worker that
       stays idle settles at [max_delay_s], the former fixed interval.}}

    Neither rule ever busy-spins: each zero-delay poll consumes one work event
    or one native wait, both of which are bounded by real readiness or by the
    native wait's own timeout.

    {b Domain ownership.} A {!t} is created per worker run and shared by both
    lanes; its fields are atomics. Each {!type-lane} belongs to exactly one
    execution lane and must only be used from that lane's thread. *)

(** Shared per-run idle state. *)
type t

(** One lane's private backoff state. *)
type lane

(** The default cap, in seconds, equal to the former fixed local yield. A
    stopping lane observes its stop flag within this bound plus one poll. *)
val default_max_delay_s : float

(** The default first non-zero delay, in seconds. *)
val default_first_delay_s : float

(** [create ?first_delay_s ?max_delay_s ()] makes fresh run state. Both
    delays are seconds; [first_delay_s] must be positive and no larger than
    [max_delay_s]. Raises [Invalid_argument] otherwise, which is a programmer
    defect. *)
val create : ?first_delay_s:float -> ?max_delay_s:float -> unit -> t

(** [lane t] makes the backoff state for one execution lane. *)
val lane : t -> lane

(** [note_work t] records that some lane completed a task, so Core is likely
    to deliver a follow-up task soon. Every lane's next idle delay restarts at
    0. Safe from any Domain; it is one atomic increment. *)
val note_work : t -> unit

(** [note_ready lane] records that the native wait run by [lane] ended
    because some lane has work. The sibling's backoff restarts at 0, but not
    [lane]'s own: the wait does not consume the task, so when the work belongs
    to the sibling, an immediate repoll by [lane] would only re-enter a wait
    that returns at once. Call it only from [lane]'s thread. *)
val note_ready : lane -> unit

(** [native_wait lane f] runs [f], the bounded native readiness wait, on
    [lane]'s behalf and marks it in flight for its duration so the sibling
    lane can join it. The mark is cleared even if [f] raises. The caller must
    hold the loop's single native-wait token, so at most one call is in flight
    per run. *)
val native_wait : lane -> (unit -> 'a) -> 'a

(** [next_delay lane] returns how long, in seconds, this idle lane should
    sleep before its next poll and advances its state. [0.] means poll now.
    Pure apart from that state, so tests can check the schedule without
    sleeping. *)
val next_delay : lane -> float

(** [local_yield lane] sleeps for [next_delay lane] seconds, if positive,
    through {!Temporal_core_bridge.Native_bridge.pacing_sleep}. That releases
    the Domain's runtime lock like [Thread.delay], and the supervisor mailbox
    is not entered. Unlike [Thread.delay] it keeps sub-tick precision on
    Windows, where [Thread.delay] rounds a backoff step to an immediate return
    or a whole 15.6 ms timer tick, so the backoff schedule means the same on
    every platform (#1008). *)
val local_yield : lane -> unit

(** [wait lane ~native_wait ready] is the {!Native_worker_loop} [wait_for_lane]
    behaviour for one lane. With [native_wait = false] it is {!local_yield}.
    Otherwise it runs [ready] under {!native_wait}; [ready] performs the
    bounded native wait and returns [Ok true] when it ended because some lane
    has work, [Ok false] when it timed out. Readiness is recorded with
    {!note_ready}; an error is returned unchanged. Call it only from [lane]'s
    thread. *)
val wait :
  lane ->
  native_wait:bool ->
  (unit -> (bool, 'error) result) ->
  (unit, 'error) result
