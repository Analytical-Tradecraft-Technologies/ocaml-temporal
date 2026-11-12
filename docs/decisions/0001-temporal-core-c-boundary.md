# ADR 0001: Project-owned C boundary over Temporal Core

- Status: accepted
- Date: 2026-07-11
- Decision owners: OCaml Temporal maintainers

## Context

The SDK needs Temporal's production worker state machines without exposing
Rust ownership, Tokio futures, callbacks from foreign threads into OCaml, or
an additional service to workflow authors. The desired artifact is one native
worker executable built by the OCaml application.

The first inspected Core revision is immutable commit
[`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`](https://github.com/temporalio/sdk-core/tree/95e97686a079dcfe6c42e3254b2f3f5e3d97408f).
At that revision:

- Core is [MIT licensed](https://github.com/temporalio/sdk-core/blob/95e97686a079dcfe6c42e3254b2f3f5e3d97408f/LICENSE.txt).
- The upstream
  [`temporalio-sdk-core-c-bridge` manifest](https://github.com/temporalio/sdk-core/blob/95e97686a079dcfe6c42e3254b2f3f5e3d97408f/crates/sdk-core-c-bridge/Cargo.toml)
  declares version 0.1.0 and a `cdylib` artifact.
- Its [worker header](https://github.com/temporalio/sdk-core/blob/95e97686a079dcfe6c42e3254b2f3f5e3d97408f/crates/sdk-core-c-bridge/include/temporal-sdk-core-c-bridge.h#L1029-L1082)
  exposes callback-based asynchronous polling and completion operations.
- Its [worker implementation](https://github.com/temporalio/sdk-core/blob/95e97686a079dcfe6c42e3254b2f3f5e3d97408f/crates/sdk-core-c-bridge/src/worker.rs#L690-L813)
  serializes `WorkflowActivation` values for polling and decodes
  `WorkflowActivationCompletion` values on completion.

The upstream C bridge is useful reference material, but its callback-oriented
dynamic-library surface is not the ideal ownership or threading boundary for
the OCaml runtime.

## Decision

The project will pin Core at an immutable Git commit and build a smaller,
project-owned Rust `staticlib`. The first implementation starts from commit
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`; updates require the complete
compatibility and replay suite plus an updated license inventory.

The `95e97686` citations in this record are historical: they are the revision
inspected when the decision was made. The bridge now links Core
`8fe478ee179ed754614d230f39586f3edb57da71` (#824, PR #989), with the ABI
unchanged. The
[Core bridge reference](../reference/core-bridge.md#temporal-core-revision)
records the current pin and its upgrade evidence, and `rust/Cargo.toml` is
authoritative.

The bridge exports a versioned C ABI containing opaque runtime/client/worker
handles and owned byte-buffer results. Its worker operations are synchronous
from C's point of view:

1. An OCaml C stub releases the OCaml runtime lock.
2. The Rust entry point submits the async Core operation to its Tokio runtime
   and blocks the calling worker thread until the result is owned by the
   bridge.
3. The C stub reacquires the OCaml runtime lock, copies or transfers the owned
   bytes into an OCaml value, and explicitly frees the bridge allocation.

Rust/Tokio threads never invoke arbitrary OCaml closures. Panics are caught at
the ABI boundary and converted to explicit error results. No Rust layout,
pointer lifetime, or Core crate type is public OCaml API.

The compatibility number is checked once when an SDK instance starts. It also
covers the strict internal adapter schema described by ADR 0002, so a stale or
partially rebuilt Rust archive cannot silently exchange the wrong document
shape with OCaml.

The owned byte-buffer mechanism carries a project-defined strict JSON control
document, not Temporal protobuf. ADR 0002 records that correction. Rust remains
solely responsible for decoding and constructing Temporal/Core protobuf.

## Why not use the upstream bridge unchanged?

The upstream bridge is a `cdylib` with callback-based async operations. This
project needs a library linked into the final OCaml-built executable and a
polling model where an OCaml-owned worker thread can safely release its runtime
lock while waiting. A focused static bridge minimizes callback lifetime rules,
symbols, and ABI surface while retaining Core as the implementation.

## Licensing and provenance

No upstream bridge source will be copied without file-level provenance and
MIT notice review. Calling or adapting the upstream API does not remove the
requirement to audit every locked Cargo dependency. Project-owned bridge code
remains Apache-2.0; redistributed MIT notices will be included where required.

## Consequences

- The final executable can be driven and linked by Dune/OCaml.
- Core networking and worker state machines remain upstream-maintained.
- The project owns a deliberately small ABI and must test it across supported
  OCaml, Rust, libc, and target-platform versions.
- Static linking increases the importance of Cargo license, crypto, and native
  library audits.
- The synthetic interpreter remains the fast deterministic unit-test layer;
  recorded Core activations become the compatibility layer.
