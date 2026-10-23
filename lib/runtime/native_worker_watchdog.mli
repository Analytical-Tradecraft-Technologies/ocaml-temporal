(** Private watchdog for workflow activations that do not yield (#493).

    Workflow code runs synchronously on the workflow lane until it returns or
    performs a supported workflow effect. A CPU loop or a blocking call stops
    that lane, and OCaml offers no safe way to interrupt it. The watchdog
    therefore only detects and reports: it runs on its own system thread,
    hosted on a Domain the workflow lane does not run on, samples the
    adapter's running-activation epoch once per tick, and asks the adapter to
    fail the workflow task once one epoch has been observed for at least the
    deadline. It never touches workflow state, never signals or cancels a
    thread, and its timing never reaches a workflow command, so it cannot
    change deterministic workflow decisions.

    Elapsed time is counted in watchdog ticks rather than read from a clock,
    so wall-clock adjustments cannot trigger or suppress detection. Because
    the first observation of an epoch counts as zero, the reported elapsed
    time is a lower bound, and detection happens at most one tick (plus
    scheduling delay) after the deadline. *)

(** Watchdog sampling state between ticks. *)
type state

(** No activation observed and nothing reported. *)
val initial : state

(** [tick_ms ~deadline_ms] is the sampling period used for [deadline_ms]: a
    quarter of the deadline, clamped to between 5 ms and 250 ms. *)
val tick_ms : deadline_ms:int -> int

(** [step state ~running ~tick_ms ~deadline_ms] advances the watchdog by one
    tick after sampling [running], the epoch currently executing workflow code
    ([None] when the lane is not in workflow code). It returns the next state
    and [Some (epoch, elapsed_ms)] exactly once for an epoch that has been
    observed continuously for at least [deadline_ms]. This function is pure so
    the detection rule is tested without real time. *)
val step :
  state ->
  running:int option ->
  tick_ms:int ->
  deadline_ms:int ->
  state * (int * int) option

(** Where the sampling thread runs (#1042). It used to own a Domain, which
    cost one of the runtime's limited Domains per running worker. A system
    thread on the workflow lane's own Domain is not enough: it can run only
    when the lane hands over that Domain's runtime lock, which OCaml code does
    at its periodic safepoints (measured at about one 50 ms tick) but C code
    that holds the lock never does, so a lane stuck in such a call would also
    stop detection. The native worker therefore hosts the thread on its
    supervisor's owner Domain, which runs only SDK code and waits for Core
    with the runtime lock released. *)

(** A running watchdog. *)
type t

(** [start ~spawn ~host_closing ~deadline_ms ~running_epoch ~abandon] starts
    the sampling thread by calling [spawn body] once; [spawn] must run [body]
    on a new system thread and return [Error] (passed through unchanged) when
    it cannot. [running_epoch] and [abandon] are called only from that thread;
    both must be safe to call while the workflow lane holds its adapter
    mutex. An exception raised by either is contained and the watchdog keeps
    sampling.

    The thread also exits, without reporting anything further, once
    [host_closing ()] holds (or raises). The host Domain cannot be joined
    while the thread lives, so the native worker passes a predicate that
    holds from the start of the native release: a stuck lane that will never
    return cannot then block the supervisor's shutdown. *)
val start :
  spawn:((unit -> unit) -> (unit, 'error) result) ->
  host_closing:(unit -> bool) ->
  deadline_ms:int ->
  running_epoch:(unit -> int option) ->
  abandon:(epoch:int -> elapsed_ms:int -> unit) ->
  (t, 'error) result

(** Longest single sleep of the sampling thread: 20 ms. A tick is slept in
    slices of at most this length so {!stop} never waits for a whole tick. *)
val stop_poll_s : float

(** Stops the sampling thread and waits for it to exit. It returns within
    about {!stop_poll_s}, whatever the tick (at once if [host_closing]
    already ended the thread), so the run loop that owns the watchdog exits
    promptly enough for bounded shutdown's lanes slack (#1023). *)
val stop : t -> unit
