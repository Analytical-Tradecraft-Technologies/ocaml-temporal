(** Private, deterministic state for the parent/child worker-restart acceptance
    diagnostic.

    This module deliberately owns only pure state transitions. It does not read
    an environment variable, decode JSON, write a file, poll Temporal, or retain
    a workflow continuation. The public worker adapter builds a candidate state
    with [observe], atomically publishes the returned complete document, and
    only then replaces its local state. Keeping the transition separate from
    publication makes partial diagnostic files and half-applied role transitions
    impossible. *)

(** The two fixed workflow roles in the parent/child recovery fixture. These
    roles are not a general workflow-tree model: accepting any extra role would
    weaken the acceptance evidence. *)
type role = Parent | Child

(** The only two checkpoints a role can contribute. Initial checkpoints are
    observed by generation one; replay checkpoints are observed by generation
    two after it has loaded the complete initial document. *)
type phase = Initial | Replay

type identity = { workflow_id : string; run_id : string }
(** The exact Temporal identity retained in a published checkpoint document.
    Both fields are identifiers only; no workflow input, payload, token, or user
    value is retained by this diagnostic protocol. *)

type activation = {
  workflow_id : string option;
  run_id : string;
  is_replaying : bool;
  history_length : int64;
}
(** One payload-free observation made after the private worker adapter has
    strictly translated an activation. [workflow_id] is absent on some later
    activations; a generation-two observer may then identify a role by its
    already-bound exact run ID. *)

type record = {
  role : role;
  phase : phase;
  generation : int;
  is_replaying : bool;
  history_length : int64;
}
(** One canonical record in the diagnostic document. The enclosing document
    always orders initial records parent then child, followed by replay records
    parent then child, independent of the order in which generation two polls
    its two workflow tasks. *)

type document = { parent : identity; child : identity; records : record list }
(** The complete payload-free checkpoint document. Generation one publishes it
    only after both initial roles have been observed. Generation two reads
    exactly that form and publishes the four-record form only after both roles
    have replayed. *)

type role_configuration = { workflow_id : string; run_id : string option }
(** Immutable configuration for one role. Generation one supplies no [run_id]
    because the worker cannot know a Temporal run before it processes the
    corresponding start. Generation two must supply it and is checked against
    the generation-one document. *)

type error = { code : string; message : string }
(** Stable, bounded failure detail for rejected configuration, persisted
    documents, or observed transitions. The native-worker hook turns this
    private error into its existing typed activation/configuration path. *)

type t
(** Opaque pure state. Its construction and transitions enforce the fixed
    two-role, two-generation contract. *)

(** The result of considering one translated activation.

    [Ignored] is an activation for another workflow. [Accepted] changes only
    in-memory partial state; it has no publishable checkpoint yet. [Duplicate]
    repeats a role checkpoint already recorded for the same generation. A
    [Checkpoint] carries the next state and a complete document which the caller
    must atomically publish before retaining [state]. *)
type observation =
  | Ignored
  | Accepted of t
  | Duplicate
  | Checkpoint of { state : t; document : document }

val create :
  generation:int ->
  parent:role_configuration ->
  child:role_configuration ->
  previous:document option ->
  (t, error) result
(** Constructs a fixed parent/child state machine.

    [generation] must be exactly one or two. Generation one requires two
    workflow IDs, absent run IDs, and no prior document. Generation two requires
    exact run IDs and the complete canonical generation-one document whose
    identities match the supplied configuration. *)

val observe : t -> activation -> (observation, error) result
(** Validates and classifies one activation without mutating [state]. A target
    workflow/run mismatch always returns [Error]. For a role's first observation
    in the current generation, a wrong replay bit, non-positive history length,
    or child-before-parent initial observation also returns [Error]. Once that
    exact role and run have supplied their checkpoint, later replay or live
    activations return [Duplicate] without revalidating phase metadata. The
    caller can therefore leave its retained state untouched when atomic
    publication fails. *)

val role_name : role -> string
(** Returns a lowercase closed spelling used by the JSON adapter. *)

val phase_name : phase -> string
(** Returns a lowercase closed spelling used by the JSON adapter. *)

val role_of_string : string -> (role, error) result
(** Parses one closed role spelling from a persisted JSON document. *)

val phase_of_string : string -> (phase, error) result
(** Parses one closed phase spelling from a persisted JSON document. *)

val history_length_of_string : string -> (int64, error) result
(** Parses one canonical, non-negative signed-64 decimal history length.
    Alternate OCaml integer spellings such as a leading sign, underscore, or
    hexadecimal prefix are rejected so persisted JSON has one representation. *)
