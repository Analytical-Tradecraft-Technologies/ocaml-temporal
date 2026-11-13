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
`Workflow.upsert_search_attributes` and `Workflow.upsert_memo` commands do
not change it; `Temporal.Search_attribute.decode` reads a typed search
attribute from it (#1154). A continue-as-new successor starts with its
predecessor's current search attributes and, once the predecessor upserted
its memo, with the merged memo, unless `Workflow.continue_as_new` replaces
them with `~memo` or `~search_attributes` (#1153; `~search_attributes:[]`
starts the successor with none). Every access
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
runtime must not emit a new timer or recompute it from a local clock.

## Start policy in `Workflow.Info`

The remaining start policies `Client.start` can set (#499) are read through
the abstract `Workflow.Info.t` rather than the `start_metadata` record, so
later fields can be added without breaking record patterns (#512):

```ocaml
let open Temporal.Result_syntax in
let* info = Temporal.Workflow.info () in
let module Info = Temporal.Workflow.Info in
match Info.continuation_initiator info with
| Some `Retry -> Ok (Printf.sprintf "retry attempt %d" (Info.attempt info))
| Some (`Continue_as_new | `Cron_schedule) | None -> Ok "not a retry"
```

- `execution_timeout`, `run_timeout`, and `task_timeout` return the values
  recorded on the start event as `Duration.t option`, rounded up to whole
  milliseconds and clamped to the public maximum. `None` means no limit was
  recorded; Temporal records its 10 second task-timeout default when the
  starter supplied none. They are metadata only: the server enforces them and
  reading one schedules nothing.
- `retry_policy` returns the effective policy as a `Retry_policy.t`
  (Temporal fills omitted fields before recording it), or `None` when the run
  is not retried. The backoff coefficient keeps its exact IEEE-754 bits.
- `continued_from_run_id` and `continuation_initiator` name the predecessor
  of a retry run (`` `Retry ``) or a continue-as-new successor
  (`` `Continue_as_new ``). Both are `None` for the first run of a chain; an
  initiator Temporal did not specify is `None` rather than a guess.
  `` `Cron_schedule `` names a run that a cron schedule started after its
  predecessor in the chain closed (#1158).
- `cron_schedule` returns the cron schedule recorded on the start event
  (`?cron_schedule` on `Client.start`), the same for every run of a cron
  chain, or `None`.
- `first_workflow_task_backoff` returns how long Temporal held the run's
  first workflow task, rounded up to whole milliseconds: a `?start_delay`,
  the wait until a cron schedule's next time, or the throttle Temporal
  applies to a rapid retry or continue-as-new. An absent or explicit-zero
  value (Temporal records zero for an ordinary start) is `None`.
  `start_time` is when the run was created, before the backoff, so the first
  activation's `Workflow.now` is at least the backoff later.

Every value comes from the run's `InitializeWorkflow` job, is installed
before workflow code runs, and is unchanged by later activations, so a
replaying worker observes the same values. The predecessor's failure and
completion payloads are retained by the private protocol but are not yet
public. `Temporal.Testing` reports the predecessor and `` `Continue_as_new ``
for its continue-as-new successors. It records no execution timeout, and
run and task timeouts only once a continue-as-new set them (#1153); it
records a retry policy for a child started with one and for a successor
given one by `continue_as_new ~retry_policy`. A successor reports the task
queue, timeouts and retry policy its predecessor set or inherited, as the
server does, but the environment does not enforce the timeouts or route by
task queue. It simulates neither cron schedules nor start delays, so
`cron_schedule` and `first_workflow_task_backoff` are always `None` there.

## Cron schedules and delayed starts

`Client.start` and `Client.signal_with_start` take `?cron_schedule` and
`?start_delay` (#1158); `Client.update_with_start` takes neither, because
Temporal refuses both on that operation. The two are mutually exclusive, a
delay must be positive, and an empty or NUL-bearing schedule is a typed
defect; Temporal itself parses the cron expression and answers an invalid
one with `` `Invalid_argument ``. OCaml workers accept every run these
create: a root run whose first task was delayed, the first run of a cron
chain (delayed to the schedule's first time), and each cron-initiated run.
The bridge carries the recorded schedule and the exact first-task backoff
through the private protocol as `cron_schedule` and
`first_workflow_task_backoff`, for root and continued runs alike. The server
has already applied the backoff before the task is polled; the runtime never
delays the workflow again. A run of a cron chain started by another SDK or
the official CLI is served the same way.

A cron chain never ends on its own: when a run closes, Temporal starts the
next one on schedule with the same workflow ID, input and options. An
exact-run `Client.wait` reports the run's own close with the next run as its
`successor`; a current-run wait follows the chain and therefore does not
return while the schedule keeps starting runs, so bound it with `?timeout`
or terminate the chain. A cron workflow reads its previous run's result
nowhere in the public API yet: the private protocol retains the
predecessor's completion payloads, but no accessor exposes them. Child
workflows cannot be given a cron schedule, and Temporal Schedules (the newer
replacement for cron workflows) are not exposed.

Payload metadata values are arbitrary bytes and reach workflow code
losslessly (#1048).

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
`_build/start-metadata-evidence`.

A second phase of the same gate qualifies cache-full eviction (#512). A fresh
worker, built with `Worker.Options.make ~max_cached_workflows:1`, polls its own
task queue (`ocaml-start-metadata-cache`) so no default-cache worker can take
its tasks. The client starts three runs: memo, search attribute and execution
deadline together; memo only; and search attribute with a deadline. Each run
must report that it has parked on its signal (a read-only phase query) before
the next one starts, so B's first task evicts A and C's evicts B. The client
reads each run's exact snapshot while it is cached: the summary plus the
expiration timestamp to the nanosecond. It then queries, signals and waits
for the runs in start order. Querying A evicts C, and each run is rebuilt
from history before it answers. Every replayed snapshot must equal the one
read before eviction, and every run must complete with its exact expected
result. The worker records one payload-free line per activation and per
acknowledged completion in `cache-events.tsv`: run ID, replay flag, history
length and cache-removal reason, with the workflow ID when Core supplies it. The
verifier requires each cache run to have a `cache_full` completion followed
by a replaying activation of that run. It applies the same history and
visibility checks as the replacement scenario, with every workflow task
completed by `metadata-worker-cache`. It also requires each snapshot's
expiration to equal the start event's `workflowExecutionExpirationTime`. Each invocation uses fresh workflow IDs on an isolated Compose project;
the caller owns server cleanup. CI runs this gate before the existing live
controllers on its own project, cleans that project, and uploads the synthetic
JSON/log/identity evidence for seven days, including on failure. `TEMPORAL_METADATA_IMAGE` can select an already
built development image for the application processes.

The first phase is exact-history worker replacement/replay coverage; the
second is cache-full eviction/replay coverage on a live worker. The
polling-capacity defect that delayed cache-full eviction with a one-slot
cache was fixed in PR #555 (#501).

These retained histories are inputs for the shared replay corpus (#503) and
feature conformance gate (#505); this change does not claim to implement that
corpus.

The start-policy accessors are covered by runtime tests that feed
`InitializeWorkflow` jobs carrying every field through the native adapter,
live and replaying, for each continuation initiator
([`test_execution_info.ml`](../../test/runtime/test_execution_info.ml)). The
[start info live regression](../../test/integration/workflow_start_info/README.md),
run by `make test-temporal-live-regressions`, starts a root run without
options and two chains with every timeout, a retry policy, and a memo. The
retry run and the continue-as-new successor each report exactly what the
client sent, before and after the worker is replaced and the run replayed.

Cron schedules and delayed starts are covered by Rust conversion tests
(root delay, every initiator with and without a backoff, cron schedule and
cron initiator on root and continued runs), the shared protocol fixtures,
runtime tests reading `cron_schedule` and `first_workflow_task_backoff`
live and under replay, client validation tests on both backends, and the
[cron/start delay live regression](../../test/integration/cron_start_delay/README.md):
a 3 s `start_delay` on `start` and on `signal_with_start`, a client and an
official-CLI cron chain whose second run reports `` `Cron_schedule `` and its
predecessor, a current-run wait that does not end, server rejection of an
invalid expression, and replay of the recorded histories. The
[history corpus](history-corpus.md) replays the captured histories of a
delayed root run and of the first two runs of a cron chain (capture
`live-2026-11-13-cron`).
