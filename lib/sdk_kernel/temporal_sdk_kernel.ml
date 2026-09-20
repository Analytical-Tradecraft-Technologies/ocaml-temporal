(** This module is the private dependency allow-list for the public OCaml
    facade. Module aliases deliberately add no runtime forwarding or state;
    they make the architectural boundary reviewable while preserving exact
    type identities required by the runtime adapters. *)

module Bridge = Temporal_core_bridge.Native_bridge
module Supervisor = Sdk_supervisor.Native
module Client_protocol = Temporal_protocol.Client_protocol
module Workflow_protocol = Temporal_protocol.Workflow_protocol
module Failure_diagnostic = Temporal_protocol.Failure_diagnostic
module Activation = Temporal_runtime.Activation
module Workflow_context_store = Temporal_runtime.Workflow_context_store
module Future_store = Temporal_runtime.Future_store
module Native_worker_execution = Temporal_runtime.Native_worker_execution
module Native_activity_execution = Temporal_runtime.Native_activity_execution
module Native_worker_loop = Temporal_runtime.Native_worker_loop
module Native_worker_policy = Temporal_runtime.Native_worker_policy
module Native_worker_observer = Temporal_runtime.Native_worker_observer
module Future = Temporal_future_kernel
