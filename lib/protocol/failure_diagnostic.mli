(** Deterministic, bounded diagnostics for structured Temporal failures.

    A Temporal failure can wrap another failure in [cause], for example an
    activity or child-workflow timeout around the application error that caused
    it. The public error type intentionally stays small, so this private
    protocol helper renders every bounded layer into one reviewable message. *)

(** Summarizes one structured failure-info variant without copying payload
    bytes into the diagnostic string. *)
val failure_info_summary : Workflow_protocol.failure_info -> string

(** Renders a failure and its bounded [cause] chain in outer-to-inner order.
    The result is deterministic and includes a depth marker if a malformed
    in-memory value exceeds the protocol nesting bound. *)
val failure_diagnostic : Workflow_protocol.failure -> string

(** Returns the [type] of the first application failure found while walking
    [cause] from the outer wrapper inward, so an application error nested under
    an activity or child-workflow wrapper is still reported. [None] when no
    layer within the protocol depth bound is an application failure, or when
    that failure's type is empty. *)
val application_failure_type : Workflow_protocol.failure -> string option
