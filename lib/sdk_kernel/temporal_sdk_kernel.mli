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

(** Shareable Core runtime and its attachment leases (#832). *)
module Shared_runtime = Sdk_shared_runtime

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

(** Private workflow control exceptions preserved by public callback wrappers. *)
module Scheduler = Temporal_runtime.Scheduler

(** Semantic workflow activation adapter over the private native source. *)
module Native_worker_execution = Temporal_runtime.Native_worker_execution

(** Semantic activity task adapter over the private native source. *)
module Native_activity_execution = Temporal_runtime.Native_activity_execution

(** Coordinated native workflow/activity poll and completion loop. *)
module Native_worker_loop = Temporal_runtime.Native_worker_loop

(** Closed retry and shutdown classification rules for the native loop. *)
module Native_worker_policy = Temporal_runtime.Native_worker_policy

(** Thread-granular execution-lane identity used by shutdown re-entrancy
    checks. *)
module Native_worker_owner = Temporal_runtime.Native_worker_owner

(** Generic private observer selection scoped to one worker constructor. *)
module Native_worker_observer = Temporal_runtime.Native_worker_observer

(** Detection-only watchdog Domain for workflow activations that do not
    yield. *)
module Native_worker_watchdog = Temporal_runtime.Native_worker_watchdog

(** Bounded worker shutdown orchestration over injected lane, drain, and
    native-release operations (#495). *)
module Native_worker_shutdown = Temporal_runtime.Native_worker_shutdown

(** Callback representation underlying the public abstract future type. *)
module Future = Temporal_future_kernel

(** Per-run workflow runtime; exposed for its private interaction-handler
    constructors, which the in-process test environment registers. *)
module Execution = Temporal_runtime.Execution

(** Deterministic in-process engine behind [Temporal.Testing]. *)
module Test_environment = Temporal_runtime.Test_environment

(** Shared strict JSON limits and canonical binary-payload wrappers, used by
    [Temporal.Replay] to build the private replay-history document. *)
module Control_protocol = Temporal_protocol.Control_protocol
