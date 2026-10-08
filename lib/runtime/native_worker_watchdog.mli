(** Private watchdog for workflow activations that do not yield (#493).

    Workflow code runs synchronously on the workflow lane until it returns or
    performs a supported workflow effect. A CPU loop or a blocking call stops
    that lane, and OCaml offers no safe way to interrupt it. The watchdog
    therefore only detects and reports: it runs on its own Domain, samples the
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

(** A running watchdog Domain. *)
type t

(** [start ~deadline_ms ~running_epoch ~abandon] spawns the watchdog Domain.
    [running_epoch] and [abandon] are called only from that Domain; both must
    be safe to call while the workflow lane holds its adapter mutex. An
    exception raised by either is contained and the watchdog keeps sampling.
    Returns [Error] with the spawn exception when the Domain cannot be
    created. *)
val start :
  deadline_ms:int ->
  running_epoch:(unit -> int option) ->
  abandon:(epoch:int -> elapsed_ms:int -> unit) ->
  (t, exn) result

(** Stops and joins the watchdog Domain. It returns within about one tick. *)
val stop : t -> unit
