# Live-test diagnostic artifacts

The PR/merge-queue and master/nightly Temporal jobs run the same seven existing
controllers through `make test-temporal-live-ci`. Each controller takes snapshots
before deleting worker generations, containers or coordination files. The outer
wrapper retains partial console output and the original exit status. Upload and
job-summary steps use `always()`, with a 40-minute scenario-step budget inside
the existing 45-minute job budget to leave time for publication.

Artifacts are named `live-diagnostics-<Actions run ID>-<attempt>` and retained for
seven days. The job summary links the immutable downloadable artifact and reports
the process outcome and collection warnings separately. Upload failure is visible
in its own step and summary; it does not replace the scenario's failure. The
sanitized rolling `controller.log` lives in the artifact directory immediately,
so killing the wrapper before its finalizer still leaves partial evidence for
the upload step. Hard runner loss, global job timeout, forced process-group
termination or cancellation can still prevent final collection/upload. A
snapshot is best-effort evidence, not a guarantee that a lost runner can be
recovered.

## What the bundle proves

Each scenario directory contains the rolling `controller.log`, `result.json`
when the wrapper finalizes, and numbered `snapshot-<sequence>-<phase>/`
directories. The snapshot includes:

- `manifest.json`: UTC capture time, exact checked-out SDK commit, locked Core
  commit, Compose project, collection limits and warning count. `images.txt`
  records configured Temporal/PostgreSQL image versions and digests;
  `containers.tsv` records the actual containers and image identities.
- Filtered controller/driver and Temporal/PostgreSQL/worker logs. The driver's
  exact workflow/run phase records distinguish an accepted start, a control
  acknowledgement and a durable terminal result. Cache-eviction logs now bind
  both starts and verified cancellations to their exact client handles.
  `*.log.executions.json` independently projects only complete, closed-format
  machine phase lines, retaining up to 256 observations with an explicit
  `truncated` flag. This preserves run identity and terminal outcomes even if a
  preceding free-text record caused conservative log suppression.
- Existing named controller records, normalized histories, bounded SDK observer
  records and accepted/result/shutdown markers, with known fields projected
  through a publication allowlist. Event IDs, parent-initiated event IDs and run
  IDs remain available for cross-history correlation. Their original filenames
  are preserved without the leading dot so the upload action includes them.
- Container running/exit/OOM status and up to four process snapshots containing
  PID, parent PID, state and executable name. Process arguments and Docker
  environment/config inspection are excluded.
- `collection.tsv`: each retained, truncated or omitted input, its source and
  retained byte count, and failed/timed-out collection commands. Missing or
  malformed partial JSON is never presented as a complete valid history.

A passing process exit is insufficient to claim workflow completion or replay.
For restart/patch/parent-child scenarios, inspect the validated controller and
normalized terminal histories together with exact-run observer evidence. The
ordinary smoke and cache controller use the driver's typed terminal assertions
and exact-run phase records. A failure bundle can have only an accepted run,
initial history or partial log; absence of terminal evidence must remain visible.
The diagnostic projection is not a replacement input for the acceptance
validators or a raw replay history.

## Bounds and privacy

Collection is opt-in through `TEMPORAL_DIAGNOSTICS_DIR` and supports only the
checked-in synthetic fixture. It never accepts another history directory or
production server address. Scenario and filename allowlists exclude raw histories,
workflow-describe responses, arbitrary neighbours, symlinks, temporary files,
payloads, headers, memo, search attributes and arbitrary JSON failure messages.

Each snapshot retains at most 32 evidence files of 64 KiB each, plus a small
manifest and collection index. Each scenario admits at most 12 snapshots; a
`collection-limit.txt` marks omitted later snapshots. The rolling console retains
the last 64 safe lines of at most 768 bytes each. Other logs are projected to
64 lines of 768 characters, with the same 64 KiB file cap. A local log larger than 4 MiB
or JSON larger than 64 KiB is omitted with a warning; JSON arrays over 256 records
are rejected instead of silently truncating a history. Docker reads have a
five-second watchdog and a 128-block output ceiling. They read from container
creation before filtering, rather than starting inside an unknown multiline
record. The seven live scenarios are bounded below 180 MiB uncompressed; the
small collector regression bundles are uploaded alongside them.

Log filtering removes credential keys, bearer credentials, URL user information,
quoted/spaced input/result/output fields, payload and private-key/certificate
records, and values of secret-named environment variables. After the first such
record, the rest of that source is omitted: unlabeled multiline continuations
cannot be classified safely. Filtering happens before console or file tail
truncation. This is deliberately conservative and can remove a useful later log
line. The structured controller/history projection remains independently usable.

These rules are for synthetic acceptance data, not a general-purpose production
sanitizer. Production history investigation requires a separate, explicit
sanitization review: export into a private location, remove payloads/headers/memo,
search attributes and credentials, replace business identifiers consistently,
review every retained text field and obtain approval before publishing. Do not
put a production export in the live fixture or this artifact directory.

## Reconstructing a failure timeline

1. Download the job-summary artifact link before its seven-day retention ends,
   or run `gh run download <run-id> --name live-diagnostics-<run-id>-<attempt>`.
2. Read `result.json`, then the snapshot manifests in numeric order. Confirm the
   SDK/Core commits and service images, and inspect collection warnings before
   treating a missing event as a product failure.
3. Find the accepted workflow/run IDs in a marker, controller or driver phase.
   Join parent/child histories using their recorded initiated/started event IDs.
   Do not query the latest run of a workflow and assume it is this execution.
4. Compare driver phases and SDK generation/replay observations with normalized
   history event IDs. Use UTC snapshot times and retained timestamped service
   logs to locate the last progress boundary. Check running/exit/OOM/process
   status to distinguish a build, exited producer, daemon error or stalled poll.
5. Require the terminal history or typed driver terminal assertion before
   calling the execution complete. Preserve a missing replay/terminal boundary
   as the investigation result rather than converting it into a passing phase.

## Local verification and remaining gates

```sh
make test-temporal-diagnostics-contract
OCAML_VERSION=5.5 DUNE_JOBS=1 \
  TEMPORAL_COMPOSE_PROJECT=ocaml-temporal-diagnostics \
  TEMPORAL_FRONTEND_PORT=7490 \
  TEMPORAL_DIAGNOSTICS_DIR="$PWD/_build/live-diagnostics" \
  make test-temporal-live-ci
```

Use a fresh artifact directory for a new rehearsal. Collection does not disable
the controllers' normal project-scoped cleanup or retain PostgreSQL volumes.

The Docker-free regression runs real shell controllers against a Docker command
fixture: a pass, an intentional exit-23 failure and a producer killed by a
one-second watchdog all retain useful evidence after source deletion. A killed
wrapper retains its sanitized rolling log without a finalizer. The contract also
tests quoted/multiline secret canaries, exact normalized event-ID preservation,
closed driver phases, malformed and oversized inputs, symlink refusal, console
filter failure, command failure/timeouts and the snapshot cap. CI uploads these
sanitized examples beneath `collector-contract/{pass,fail,timeout,killed-wrapper}`.
They qualify collection/cleanup behavior;
they do not demonstrate a real Temporal product failure or prove hosted upload.
The corresponding Actions run and downloadable artifact are the publication
gate; the full live scenarios remain the SDK behavior gate.

New live controllers must register their scenario and exact filename allowlist
in the collector, add pre-removal/validated snapshots, and run through the same
wrapper before they can claim equivalent retained evidence. This change covers
the current seven controllers; it does not add transport-fault scenarios or fix
the cache-eviction timeout.
