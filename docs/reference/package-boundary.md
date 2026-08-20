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

`Future.t` is the one deliberate type-identity exception. Its signature keeps
an internal equality with the generic Dune package-private
`temporal_future_kernel` so private runtime adapters can pass scheduler-owned
values through the facade without unsafe casts. The public `Temporal` root does
not re-export the kernel module, and `Future.mli` exposes no constructor, record
field, callback, or lifecycle operation. The kernel is unavailable through the
supported `temporal-sdk` dependency and is not part of the public API. Public
future values originate from SDK operations such as timers, activities, and
child workflows, or from public combinators over those values. An application
cannot fabricate an arbitrary scheduler-owned future or access its callbacks
or continuations.

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
The OPAM test phase also runs the install-consumer metadata contract, which
parses both OPAM manifests and the Dune package declaration to require
`conf-rust-2024`. This makes the Cargo/Rust prerequisite explicit during
source-install dependency resolution rather than letting the private bridge
build fail later with a missing executable.
Changes that intentionally publish an implementation component must update
this document, the public API review, and the install regression rather than
silently widening the package surface.
