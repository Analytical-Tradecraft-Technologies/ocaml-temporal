(** Private OCaml kernel boundary between the public facade and native/Core
    integration.

    Public implementation modules depend on this allow-list instead of naming
    the JSON protocol, C/Rust bridge, deterministic runtime, or supervisor
    libraries directly. The aliases preserve type identity across those
    implementation libraries without making any of them part of the installed
    [Temporal] API. *)

(** Versioned C/Rust ABI and owned native resource graph operations. *)
module Bridge = Temporal_core_bridge.Native_bridge

(** One-owner-Domain supervisor for the complete native resource graph. *)
module Supervisor = Sdk_supervisor.Native

(** Strict client control-protocol documents exchanged with the Rust bridge. *)
module Client_protocol = Temporal_protocol.Client_protocol

(** Strict semantic workflow protocol shared by the runtime and Rust bridge. *)
module Workflow_protocol = Temporal_protocol.Workflow_protocol

(** Bounded, payload-safe diagnostics for protocol failures. *)
module Failure_diagnostic = Temporal_protocol.Failure_diagnostic

(** Deterministic commands and jobs used inside one workflow activation. *)
module Activation = Temporal_runtime.Activation

(** Execution-local workflow state and durable-operation scheduling. *)
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Scheduler-owned future state used by workflow operations. *)
module Future_store = Temporal_runtime.Future_store

(** Semantic workflow activation adapter over the private native source. *)
module Native_worker_execution = Temporal_runtime.Native_worker_execution

(** Semantic activity task adapter over the private native source. *)
module Native_activity_execution = Temporal_runtime.Native_activity_execution

(** Coordinated native workflow/activity poll and completion loop. *)
module Native_worker_loop = Temporal_runtime.Native_worker_loop

(** Closed retry and shutdown classification rules for the native loop. *)
module Native_worker_policy = Temporal_runtime.Native_worker_policy

(** Generic private observer selection scoped to one worker constructor. *)
module Native_worker_observer = Temporal_runtime.Native_worker_observer

(** Callback representation underlying the public abstract future type. *)
module Future = Temporal_future_kernel
