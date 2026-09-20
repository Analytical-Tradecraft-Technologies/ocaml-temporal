(** Non-installed acceptance observers. Environment parsing, scenario policy
    and all artifact I/O belong to this fixture library. *)

(** Builds independent callbacks; the optional environment reader is a pure
    test seam and defaults to the fixture process environment. *)
val create :
  ?getenv:(string -> string option) ->
  unit ->
  (Temporal_runtime.Native_worker_observer.t, Temporal_base.Error.t) result

(** Creates one worker with fresh acceptance callbacks on the current Domain.
    Always restores the prior private injection scope, including when the
    supplied constructor raises or returns an error. Native worker ownership
    after successful construction remains with the calling fixture. *)
val with_worker :
  (unit -> ('worker, Temporal.Error.t) result) ->
  ('worker, Temporal.Error.t) result
