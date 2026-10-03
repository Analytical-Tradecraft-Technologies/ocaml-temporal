# Workflow start metadata

`Client.start` accepts memo and registered search attributes independently or
in combination. The worker retains those maps from `InitializeWorkflow` and
workflow code can inspect an owned historical snapshot:

```ocaml
let open Temporal.Result_syntax in
let* metadata = Temporal.Workflow.start_metadata () in
match Option.bind metadata.memo (List.assoc_opt "note") with
| None -> Ok None
| Some payload -> Result.map Option.some (Temporal.Codec.decode Temporal.Codec.string payload)
```

The snapshot describes this run's start event. Later
`Workflow.upsert_search_attributes` commands do not change it. Every access
copies payload bodies; mutating returned bytes cannot change a later
observation. Replay reconstructs the snapshot from history, and a successor
run receives its own inherited metadata. `None` preserves an absent protobuf
map, while `Some []` preserves an explicitly empty map. Temporal may normalize
empty client collections when writing history; code that only needs values
can use `Option.value ~default:[]`.

The private OCaml/Rust representation also retains an exact nullable
`workflow_execution_expiration_time`. The public snapshot exposes this as
`execution_expiration_time : Time.t option`, preserving nanoseconds and an
explicit zero timestamp. The server owns deadline enforcement; the language
runtime must not emit a new timer or recompute it from a local clock. Existing
execution/run/task timeout fields remain validated and preserved, including
absent and explicit-zero defaults. Public client timeout/start-policy options
and their full policy matrix remain tracked by #499.

Cron schedules and cron-initiated runs remain unsupported. Nonzero root
start delay also remains unsupported, while absent and explicit-zero backoff
are ordinary defaults. Temporal can insert a nonzero first-task backoff for
rapid continue-as-new and retry runs without any cron policy: the bridge
retains and validates that exact duration as `first_workflow_task_backoff` for
known workflow/retry continuations. The server has already applied it before
polling; the runtime must not delay the workflow again. An arbitrary previous
run ID never bypasses validation. The public client exposes no cron/start-delay
options, and its start documentation warns against sending them through
another SDK. Payload metadata retains the SDK's existing UTF-8
string boundary; unsupported binary metadata fails explicitly before workflow
code runs.

## Validation

The shared OCaml/Rust protocol fixtures and Rust conversion tests cover
absent, empty, and nonempty maps; each field individually and together; root
and continued runs; exact expiration timestamps; and malformed/unsupported
fields. Native execution tests reconstruct the same workflow under replay and
check ownership across a durable timer and subsequent activation. The
installed consumer witness checks the public accessor's package boundary.

`make test-temporal-start-metadata-live` builds an independent OCaml
client/worker executable, starts the pinned Temporal/PostgreSQL Compose stack,
and runs five exact active histories: memo, search attributes, both,
continued-both, and an official Temporal CLI start using the Go SDK with both
metadata fields and execution/run/task timeouts. It registers the keyword
attribute, checks workflow snapshots, replaces the worker and discards its entire cache, checks snapshots again, signals completion, and retains initial and
terminal histories plus visibility descriptions under
`_build/start-metadata-evidence`. Each invocation uses fresh workflow IDs on an isolated Compose project;
the caller owns server cleanup. CI runs this gate before the existing live
controllers on its own project, cleans that project, and uploads the synthetic
JSON/log/identity evidence for seven days, including on failure. `TEMPORAL_METADATA_IMAGE` can select an already
built development image for the application processes.

This is exact-history worker replacement/replay coverage. It does not qualify
cache-full eviction, whose polling-capacity issue is tracked separately in
#501/PR #555.

These retained histories are inputs for the shared replay corpus (#503) and
feature conformance gate (#505); this change does not claim to implement that
corpus or all future #499 start policies.
