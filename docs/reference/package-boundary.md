# Installed package boundary

`temporal-sdk` has one supported OCaml API: the wrapped `Temporal` library.
The native worker implementation, runtime, JSON protocol, mailbox, supervisor,
and C/Rust bridge are implementation details of that library. An application
links `temporal-sdk` and writes ordinary `Temporal.Client`, `Temporal.Worker`,
workflow, activity, and codec code; it does not depend on the implementation
libraries directly.

The [public API map](public-api-map.md) groups every supported `Temporal`
module by execution context and explains which source modules are deliberately
private. Use it alongside this boundary reference when deciding where a new
public helper belongs.

## Three implementation layers

The package dependency graph has three reviewable layers:

1. The native transport layer contains the strict JSON protocol, project-owned
   C ABI, Rust bridge, and Temporal Core integration. It owns foreign memory,
   opaque native handles, Tokio, protobuf conversion, and server I/O.
2. The private OCaml kernel contains the deterministic activation scheduler,
   execution-local state, future kernel, mailbox, and one-owner-Domain SDK
   supervisor. `temporal_sdk_kernel` is the explicit module allow-list through
   which the public implementation reaches that layer and the native layer
   beneath it.
3. The public facade is the installed `Temporal` library. It owns typed codecs,
   labelled arguments, abstract handles, `result`-based failures, and the
   direct-style workflow helpers application authors use.

`lib/public` must not name `Temporal_core_bridge`, `Temporal_protocol`,
`Temporal_runtime`, `Temporal_future_kernel`, or `Sdk_supervisor` directly.
That rule keeps the facade independently readable and prevents a convenient
new helper from quietly acquiring a JSON, native-handle, scheduler-state, or
mailbox dependency. The private `Backend` and `Native_worker` modules translate
between public values and the kernel allow-list; Dune excludes both from the
generated public signature.

## Dune invariant

Every implementation library linked by `lib/public/dune` is declared with:

```lisp
(package temporal-sdk)
```

and has no `public_name`. This applies to:

- `temporal_base`;
- `temporal_protocol`;
- `temporal_core_bridge`;
- `temporal_runtime`;
- `temporal_future_kernel`;
- `temporal_mailbox_processor`;
- `temporal_sdk_kernel`; and
- `temporal_sdk_supervisor`.

Dune therefore installs their archives and interfaces below the package's
reserved `__private__/` directory. They are package-private dependencies of
`temporal-sdk`, not separately installable public Findlib libraries. The public
library remains the only top-level package name an application should request.
The private modules may still be present in an installed artifact because the
public archive needs them at link time; their location and package metadata
prevent normal consumers from importing them by module or library name.

`private_modules` in `lib/public/dune` is complementary: it keeps selected
modules out of the generated `Temporal` signature. It is not a substitute for
the package-private dependency declaration because it cannot hide a separate
library's archive or interface.

The explicit `lib/public/temporal.ml` root is the allow-list for modules
re-exported through `Temporal`; `private_modules` complements that list by
keeping selected implementation files out of the generated signature.

The mailbox and supervisor are not public actor APIs. One supervisor owner
Domain per SDK instance serializes operations on the complete Rust
runtime/client/worker graph; individual client or worker handles do not receive
separate actors. Producer Domains submit typed operations to that owner, and
the blocking supervisor and mailbox entry points must stay off cooperative
workflow or Eio scheduler fibers.

The native bridge follows the same rule. The installed artifact contains the
static archive needed to link an OCaml-owned executable, but does not install
the C header or Rust source. No public type exposes a Rust handle, protobuf,
JSON bridge record, mailbox, supervisor state, or an implementation-library
record. `Payload.t` is intentionally a public record containing codec metadata
and data bytes, so application codecs can construct and inspect payloads
without a private CMI. `Duration.t`, `Error.t`, `Codec.t`, `Workflow.t`, and
`Activity.t` are abstract in the installed `Temporal` signature. Worker
adapters use the documented `name`, `input`, `output`, and `implementation`
accessors; they do not rely on record layout.

Four public types deliberately keep a type equality with a package-private
library so private runtime adapters can pass SDK-owned values through the
facade without unsafe casts:

- `Future.t` equals `Temporal_sdk_kernel.Future.t`, an alias of the generic
  `temporal_future_kernel` future;
- `Activity.context` equals `Temporal_base.Activity_context.t`;
- `Activity.async_handle` and `Activity.async_context` equal
  `Temporal_base.Async_activity.handle` and `.context`.

Each target type is abstract in its private interface. The public `Temporal`
root does not re-export those modules, and neither `Future.mli` nor
`Activity.mli` exposes a constructor or record field for these types, so an
application cannot build one directly. `Future.mli` also exposes no callback or
scheduler lifecycle operation. `Activity.mli` deliberately exposes the
supported activity operations on values the SDK supplies: the context
operations, `Async_context.handle`, and the `Async_handle` completion,
failure, cancellation, and heartbeat functions. [#793](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/793)
tracks auditing these equalities before the v1 freeze. The private libraries
are unavailable through the supported `temporal-sdk` dependency and are not
part of the public API. Public
future values originate from SDK operations such as timers, activities, and
child workflows, or from public combinators over those values. An application
cannot fabricate an arbitrary scheduler-owned future or access its callbacks
or continuations.

## Vendored source builds

An application may build the SDK from source inside its own Dune workspace
instead of installing it, for example as a Git submodule, in a monorepo, or
through `opam monorepo`:

```lisp
; consumer/dune
(vendored_dirs vendor)
```

with this repository at `vendor/temporal/` and executables depending on
`(libraries temporal-sdk)`. In that layout Dune's workspace root is the
consumer's project, which contains neither the bridge scripts nor the Rust
workspace. The bridge rule in `lib/core_bridge/dune` therefore resolves every
path relative to its own directory (`../..` is always the SDK root), and its
writable Cargo fallback is `../../rust-target`, inside the SDK's own build
directory rather than the consumer's build root. An explicit `CARGO_TARGET_DIR`
remains authoritative exactly as in the in-repository build, and
`TEMPORAL_RUST_BRIDGE_DIR`/`TEMPORAL_RUST_BRIDGE_KEY` still select a verified
prebuilt bundle. Building from source requires the same Rust toolchain and
`protoc` as any other source build. The bridge rule forwards Dune's
`%{profile}`: the release profile builds Cargo's optimized `[profile.release]`
and every other profile, including the default dev profile, builds Cargo's dev
profile. Consumers that ship a vendored worker should therefore build it with
`--profile release` (or `-p`).

A vendored SDK's `dune-workspace` is ignored, so the consumer does not inherit
its `(disable_dynamically_linked_foreign_archives true)` context setting. On
Linux and macOS the default is harmless: the bridge rule also produces the
dynamic archive Dune expects, and every Temporal executable links the bridge
statically. On Windows, FlexDLL cannot consume Rust's GNU-style native linker
flags while building that unneeded DLL stub, so Windows consumers must add the
setting to their own `dune-workspace`:

```lisp
(lang dune 3.18)
(context
 (default
  (disable_dynamically_linked_foreign_archives true)))
```

`make test-vendored` (or `make native-test-vendored` on the host) builds and
runs a scratch consumer that vendors a copy of the current checkout this way,
calls the linked bridge through `Temporal.Runtime_info`, and checks that the
fallback Cargo directory did not escape into the consumer's build root. It
compiles the Rust bridge in a fresh workspace, so it is an opt-in check rather
than part of `make test` or the per-commit CI matrix.

## Regression evidence

The installed-package smoke test is deliberately run against a fresh consumer
directory rather than against the repository source tree:

```sh
make test-install
```

The public signature and module-export policy enforced by that consumer is
described in [Public API compatibility](api-stability.md). The package-boundary
checks below protect installation layout and privacy; the typed witness in
that reference protects the source contract visible to downstream OCaml code.

`test/bridge/test_install.sh` checks both sides of the boundary:

1. it verifies that every implementation archive is below
   `temporal-sdk/__private__/` and that no matching top-level artifact exists;
2. it builds and runs a small consumer that constructs and uses every public
   core value family (payloads, codecs, durations, errors, workflow and
   activity definitions, futures, client operations, and worker lifecycle),
   including `Temporal.Runtime_info`, proving that the public package and
   linked static bridge remain usable; and
3. it separately compiles fixtures that try to name the base, runtime,
   protocol, native-worker, backend, future-kernel, mailbox, supervisor, and
   C/Rust bridge modules through only `(libraries temporal-sdk)`. Each
   compilation must fail because those module paths are not part of the
   consumer's normal include path. The future-kernel fixture specifically
   protects the internal type-identity exception described above.

The repository smoke test also checks each currently listed internal Dune stanza
for the `(package temporal-sdk)` declaration and rejects a future
`public_name`. It rejects direct lower-layer module references or Dune
dependencies from `lib/public`, and it compiles a negative consumer fixture for
`Temporal_sdk_kernel` so the kernel cannot silently become application API.
The normal Dune `runtest` alias, including the OPAM test phase, runs the
install-consumer metadata contract. It parses both OPAM manifests and the Dune
package declaration to require `conf-rust-2024` and `conf-protoc`. This makes
the Cargo/Rust and Protocol Buffers compiler prerequisites explicit during
source-install dependency resolution rather than letting the private bridge
build fail later with a missing executable.
The formatter selects Dune language 3.18 explicitly, matching `dune-project`,
to avoid initializing another workspace and scheduler inside the test action.
Concurrent formatters must not contend over `_build/.lock`. Native tool output
is captured in workspace-local files before CRLF normalization so a failing
parser cannot be hidden by a successful pipeline consumer. Mutation fixtures
cover complete-looking output followed by a tool failure, unavailable project
extensions, CRLF output, and dependency pins attached to the wrong package.
Changes that intentionally publish an implementation component must update
this document, the public API review, and the install regression rather than
silently widening the package surface.
