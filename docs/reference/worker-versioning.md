# Worker versioning

`Temporal.Worker.Options` exposes the first worker-routing versioning mode in
the public OCaml SDK. It is deliberately separate from
`Temporal.Workflow.patched`: patching keeps old workflow histories compatible,
while worker versioning controls which worker build receives new workflow
tasks.

## Legacy build-ID routing

Use `Options.make` when a deployment publishes a new whole-worker build ID:

```ocaml
let options =
  match
    Temporal.Worker.Options.make
      ~versioning:(Temporal.Worker.Options.Legacy_build_id "agent-worker-2026-07-16") ()
  with
  | Ok options -> options
  | Error error -> failwith (Temporal.Error.message error)

let worker =
  Temporal.Worker.create ~options ~target_url ~namespace ~task_queue
    ~workflows ~activities ()
```

`Legacy_build_id` maps to Temporal Core's `LegacyBuildIdBased` strategy. The
server's build-ID routing and compatibility-set rules remain authoritative;
this library does not silently register or migrate a build ID. Roll out a
worker only after the corresponding build ID has been made eligible through
the normal Temporal deployment process.

`No_versioning` is the default and preserves existing behavior. It still sends
the SDK's build ID as worker metadata, but it does not ask Core to route by a
compatibility set. The mock backend accepts the option for API parity but has
no server-side routing to apply.

## Deployment-based routing

For Temporal deployments that use the modern deployment/version model, select
`Deployment_based` and identify both the deployment and the concrete worker
build. The nested build ID must be the same value as the worker's top-level
build ID; this prevents the worker from advertising one identity while asking
Core to route it as another.

```ocaml
let options =
  match
    Temporal.Worker.Options.make
      ~versioning:(Temporal.Worker.Options.Deployment_based {
        deployment_name = "agents";
        build_id = "agent-worker-2026-07-16";
        use_worker_versioning = true;
        default_versioning_behavior = Some `Pinned;
      }) ()
  with
  | Ok options -> options
  | Error error -> failwith (Temporal.Error.message error)
```

`use_worker_versioning` controls whether Core applies deployment routing. When
it is `false`, `default_versioning_behavior` must be `None` and workflow-task
completions are unversioned. When it is `true`, a default behavior is
required: `Some `Auto_upgrade` allows a workflow to move to a newer compatible
deployment, and `Some `Pinned` keeps it on the selected deployment version.
These choices are passed to Temporal Core's `WorkerDeploymentBased` strategy;
the SDK does not perform deployment registration or rollout automation.

The default is mandatory because this SDK does not yet offer a per-workflow
versioning behavior. Every workflow-task completion therefore leaves the
behavior `UNSPECIFIED`, and the pinned Temporal Core substitutes only a
configured worker default. Temporal defines `UNSPECIFIED` as "versioning is not
enabled in the worker" and requires each workflow type on a versioned worker to
choose between pinned and auto-upgrade, so a versioned worker without a default
would advertise a deployment while declaring every workflow unversioned.
`Options.make` rejects that combination with a defect, and the private bridge
rejects it again as a configuration error.

## Reading the selected deployment in a workflow

When a versioned worker receives a task, workflow code can inspect the exact
deployment and build that Core selected:

```ocaml
match Temporal.Workflow.current_deployment_version () with
| None -> () (* unversioned or synthetic activation *)
| Some { deployment_name; build_id } ->
    Logs.debug (fun log -> log "running %s/%s" deployment_name build_id)
```

The value is replaced before every activation and is therefore safe to log or
attach to diagnostics without retaining stale state. It is metadata, not a
replacement for `Temporal.Workflow.patched`: using it to choose commands is
replay-safe only when the deployment routing policy is stable for the run.

## OCaml/Rust protocol

The private worker document contains a closed `versioning` object in addition
to the top-level `build_id`:

```json
{"kind":"none"}
```

or:

```json
{"kind":"legacy_build_id","build_id":"agent-worker-2026-07-16"}
```

or, for modern deployment-based routing:

```json
{
  "kind": "deployment_based",
  "deployment_name": "agents",
  "build_id": "agent-worker-2026-07-16",
  "use_worker_versioning": true,
  "default_versioning_behavior": "pinned"
}
```

The JSON schema is
[`worker-config.schema.json`](../schemas/bridge/worker-config.schema.json).
OCaml validates the option before creating a native graph and Rust validates
the decoded document again. Unknown keys and modes are rejected, and the
nested build ID in either routing form must exactly equal the top-level
`build_id`. For deployment-based routing, `default_versioning_behavior` may be
`"auto_upgrade"`, `"pinned"`, or `null`; it must be `null` when
`use_worker_versioning` is `false` and non-`null` when it is `true`. This redundancy is intentional: either
side must fail closed if a stale or hand-authored document reaches the ABI
boundary.

## Scope and evidence

This slice covers legacy and deployment-based worker routing plus their
bilateral configuration contracts and task-local deployment metadata
inspection. It does not implement workflow-code history compatibility,
migration automation, or a live routing
acceptance test. Those require server-side compatibility-set orchestration and
are tracked separately in the [feature coverage](feature-coverage.md) and
[implementation roadmap](../implementation-roadmap.md).
