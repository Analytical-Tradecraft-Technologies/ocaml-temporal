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
    that failure's type is empty. The returned type has been passed through
    {!sanitize_inbound_error_type}, so it is always accepted by
    [Temporal_base.Error.make]. *)
val application_failure_type : Workflow_protocol.failure -> string option

(** Classifies where a failure originated (#1143): the first layer of the
    bounded [cause] chain that is not an activity, child-workflow or Nexus
    operation wrapper, or an [Absent] layer that has a cause. The result is
    the polymorphic variant [Temporal_base.Error.origin] uses; a chain made
    only of wrappers is [`Other]. *)
val failure_origin :
  Workflow_protocol.failure ->
  [ `Application
  | `Timeout of
    [ `Start_to_close | `Schedule_to_start | `Schedule_to_close | `Heartbeat | `Unspecified ]
  | `Server
  | `Cancelled
  | `Terminated
  | `Other ]

(** Collects the application and cancellation detail payloads of every layer
    of the bounded [cause] chain, outer to inner. Heartbeat details of timeout
    and reset layers are excluded; see {!last_heartbeat_details}. *)
val failure_details : Workflow_protocol.failure -> Workflow_protocol.payload list

(** Collects the [last_heartbeat_details] of every timeout and reset layer of
    the bounded [cause] chain, outer to inner (#1143). *)
val last_heartbeat_details : Workflow_protocol.failure -> Workflow_protocol.payload list

(** Bound on the JSON-encoded length of a sanitized inbound type, equal to
    [Temporal_base.Error.max_error_type_bytes]. *)
val max_inbound_error_type_encoded_length : int

(** Deterministically rewrites an application failure type received from
    Temporal (written by another SDK, worker or history) into text that
    [Temporal_base.Error.make] accepts: malformed UTF-8 becomes U+FFFD, each
    ASCII control character becomes a visible [\\uXXXX] escape, and text whose
    encoded length exceeds {!max_inbound_error_type_encoded_length} is cut at a
    character boundary and suffixed with ["..."]. Text that already satisfies
    those rules is returned unchanged. Inbound types must never raise, because
    the same history is replayed on every workflow-task retry (#1011). *)
val sanitize_inbound_error_type : string -> string
