# Workflow-task failure regression histories

These five unmodified Temporal histories contain only synthetic fixture data.
They were captured by `make test-temporal-task-failure-live` on 2026-09-20:
OCaml 5.5.1, Rust 1.94.1, Core `95e97686a079dcfe6c42e3254b2f3f5e3d97408f`,
Temporal 1.32.0 and PostgreSQL 18.6. `manifest.json` retains the source baseline
and patch hash, exact run IDs, binary hashes, image digests, and teardown result.
The full initial/terminal histories, describe responses and process logs remain
in the corresponding live artifact directory named in the PR evidence.

`body` and `encoder` commit a timer before defective code causes workflow-task
failures. `missing` starts on a worker without its registration. All three
complete on their original runs after an independently compiled corrected
worker replaces the broken process; no speculative second timer is committed.
Both `business-*` runs fail intentionally, with distinct retryability flags and
no workflow retry policy. The existing `replay_abi` integration test replays
these retained histories through pinned Core using the expected corrected
command sequence and deliberate application failures. The live gate separately
executes both OCaml generations and checks original public client handles.

The `.pb` files encode the same JSON events using the Cargo-generated
`temporalio-protos` descriptor set from the pinned Core revision. This avoids
adding a JSON-history converter to production: this Core revision derives
Rust-specific history JSON instead of accepting Temporal CLI protobuf JSON.
The manifest records both encodings' SHA-256 hashes and the export tool version.
Tests decode the protobuf with Core's generated types and feed the existing
private replay ABI. Export with Python `google.protobuf.json_format.Parse`
using a dynamic `History` class loaded from that descriptor set; no fields are
removed, rewritten, or synthesized.

Regenerate with the live Make target against a fresh Compose project; replace
all five histories and the matching manifest together only after its complete
same-run, command-discard, shutdown and teardown assertions pass.
