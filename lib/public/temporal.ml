(** The public [Temporal] root is an explicit allow-list of supported modules.

    Dune normally generates aliases for every implementation file in a wrapped
    library, which would make private adapters reachable as
    [Temporal.Backend] or [Temporal.Native_worker].  Keeping this root module
    in source exposes only the supported public surface while implementation
    files remain available to one another inside the library. *)
module Activity = Activity
module Child_workflow = Child_workflow
module Client = Client
module Codec = Codec
module Condition = Condition
module Duration = Duration
module Error = Error
module Future = Future
module Interaction = Interaction
module Payload = Payload
module Query = Query
module Result_syntax = Result_syntax
module Runtime = Runtime
module Runtime_info = Runtime_info
module Scope = Scope
module Time = Time
module Signal = Signal
module Testing = Testing
module Update = Update
module Worker = Worker
module Workflow = Workflow
module Workflow_context = Workflow_context
