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

The [proposed v1 support policy](v1-support-policy.md) selects a narrower
production feature/deployment contract, compatibility commitments and release
qualification gates for maintainer review in #489. It is not an approval or a
claim that the experimental package already satisfies those gates. Public
export visibility and compile-time compatibility remain separate from stable
behavioral support.

The policy is intentionally conservative at the application boundary. It
protects the source API that a downstream OCaml program sees, not the private
Rust/Core implementation. The native bridge has its own version negotiation
and ABI tests; those do not make private modules or C symbols part of the
supported OCaml API.

## Installed-consumer witness

`test/fixtures/install-consumer/public_api.ml` is compiled as part of the
fresh installed consumer created by `make test-install`. Its explicit type
annotations cover the public definition, codec, future, workflow, interaction,
client, worker, and lifecycle operations. The file also aliases every module
listed by `lib/public/temporal.ml`, so a removed or accidentally hidden public
module fails compilation. The corresponding negative fixtures continue to
prove that package-private modules cannot be imported through a normal
`(libraries temporal-sdk)` dependency.

`test/bridge/test_install.sh` additionally compares the checked-in root's
module names with its expected allow-list. This catches an accidental export
even when no consumer happens to use the new name. The witness is a compile
check only: it does not contact Temporal Server, execute a workflow, or assert
runtime semantics already covered by the unit and live acceptance suites.

Run the focused gate with:

```sh
make test-api
```

`make test-api` is an alias for the installed-consumer regression, so it uses
the same package installation and private-artifact checks as `make test`. It
requires the normal Docker/OPAM build environment; it does not create a
second API-specific dependency set.

## Updating the contract

When a public API change is intentional:

1. update the affected interface and the typed witness together;
2. explain the source-compatibility and migration impact in the release notes
   for the eventual versioned release; and
3. update this document if the compatibility policy or public module allow-list
   changes.

Do not weaken an annotation merely to make a changed signature compile. If a
new capability requires a new public module, add it to the explicit root
allow-list and document why it belongs in the supported surface. Before the
first stable release, maintainers must approve the v1 policy and complete its
qualification checklist. The installed witness and private-module negative
checks remain required even for exported capabilities labelled experimental.
