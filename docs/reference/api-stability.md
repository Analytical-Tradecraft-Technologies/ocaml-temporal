# Public API compatibility

`temporal-sdk` exposes one supported OCaml library: the wrapped `Temporal`
module. The implementation libraries, JSON protocol, supervisor, mailbox, and
C/Rust bridge are package-private and are not compatibility commitments for an
application. The installed-package boundary and its privacy rules are
documented in [the package-boundary reference](package-boundary.md).

## Current status

The package is experimental and has not reached `0.1.0`. There is therefore no
stable-version compatibility promise yet. Public signatures are nevertheless
treated as a deliberate contract: a breaking change must be intentional,
documented, and reflected in the checked-in consumer witness before it is
merged. Adding a new public value is normally compatible; removing a value,
changing a type, changing a labelled argument, changing a result/error
contract, or exposing an implementation module is a breaking change even when
the compiler can still build the repository itself.

The [MVP v1 prerelease scope](v1-support-policy.md) describes the core
workflow/client/worker candidate, its experimental gaps, and release
qualification gates. Its matrix becomes the approved target only after a
maintainer approves it on [PR #557](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/pull/557)
and that PR merges; [#489](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/489)
tracks the decision. The candidate `v0.1.0-rc.1` remains experimental and
does not establish a stable 1.x source or behavior promise. Approval does not
claim that the package satisfies the release gates. Public export visibility
and compile-time checks remain separate from runtime qualification.

The policy is intentionally conservative at the application boundary. It
protects the source API that a downstream OCaml program sees, not the private
Rust/Core implementation. The native bridge has its own version negotiation
and ABI tests; those do not make private modules or C symbols part of the
supported OCaml API.

## Public API witness

`test/fixtures/install-consumer/public_api.ml` is the typed compatibility
witness. It binds every public value to an explicit type annotation written
against the consumer-visible `Temporal` paths, and aliases every module listed
by `lib/public/temporal.ml`, so a removed value, changed type, changed or added
labelled argument, changed result/error contract, or hidden public module fails
compilation. Type variables stay explicit so an annotation cannot silently
become monomorphic. The witness has no top-level effects and is a compile
check only: it does not contact Temporal Server, execute a workflow, or assert
runtime semantics already covered by the unit and live acceptance suites.

Two gates enforce it.

### In-tree gate (`dune runtest`, every CI lane)

`test/api_witness/dune` copies the witness and compiles it against the in-tree
`temporal-sdk` library on every `dune runtest`. That target runs through
`make test` (and therefore `make verify`) on Linux and through
`make native-test` (`make native-verify`) on the macOS and Windows jobs, so
every PR and `master` CI lane type-checks the witness without Docker or a
package installation. The same rule then runs
`test/api_witness/check_api_witness.ml`, a completeness check that:

1. reads the public module allow-list from the `module X = Y` aliases in
   `lib/public/temporal.ml`;
2. collects every `val` declared in each listed module's `lib/public/*.mli`,
   including values in nested `module M : sig ... end` declarations (for
   example `Activity.Context.heartbeat`); and
3. fails, listing each `Temporal.<path>` it could not find, unless the witness
   pins every collected value with an explicitly annotated top-level binding
   whose body is exactly that value, such as
   `let _scope_cancel : T.Scope.t -> (unit, T.Error.t) result = T.Scope.cancel`.
   A bare use such as `let _ = T.Scope.cancel` does not count, because its
   type would be inferred and a signature change could pass unnoticed.

The check parses source with the compiler's own parser (`compiler-libs`, part
of the approved OCaml compiler distribution); it never links the SDK. It
rejects `include` and named module types in public interfaces instead of
skipping them, so adopting either construct requires extending the check
rather than silently narrowing it. Types, constructors, and record fields are
not enumerated; they are covered where the value annotations and the record
field witnesses mention them.

Run only this gate with:

```sh
opam exec -- dune build @test/api_witness/runtest
```

### Installed-consumer gate (`make test-api`)

`make test-install`, aliased as `make test-api`, installs the package into a
fresh prefix and builds an independent consumer from
`test/fixtures/install-consumer`. Its `main.ml` references `Public_api`
because Dune compiles only the modules an executable reaches from its main
module; without that reference the installed build would skip the witness.
This gate additionally proves the witness compiles against the installed
`.cmi` files rather than the build tree. The corresponding negative fixtures
continue to prove that package-private modules cannot be imported through a
normal `(libraries temporal-sdk)` dependency, and `test/bridge/test_install.sh`
compares the installed root's module names with its expected allow-list, which
catches an accidental export even when no consumer uses the new name.

`make test-install` runs within `make test` and `make native-test`; it uses
the same package installation and private-artifact checks and does not create
a second API-specific dependency set.

## Updating the contract

When a public API change is intentional:

1. update the affected interface and the typed witness together. A new public
   value needs a new annotated binding in `public_api.ml`; the in-tree gate
   names any value that is still missing;
2. explain the source-compatibility and migration impact in the release notes
   for the eventual versioned release; and
3. update this document if the compatibility policy or public module allow-list
   changes.

Do not weaken an annotation merely to make a changed signature compile. If a
new capability requires a new public module, add it to the explicit root
allow-list and document why it belongs in the exported surface. Before the
MVP prerelease is published, maintainers must approve its scope and complete
its candidate qualification checklist. The installed witness and
private-module negative checks remain required even for exported capabilities
labelled experimental.
