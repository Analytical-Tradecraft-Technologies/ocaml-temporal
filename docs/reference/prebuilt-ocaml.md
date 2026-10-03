# Use a precompiled OCaml SDK

CI publishes an installed `temporal-sdk` for each tested **exact OCaml patch,
operating system, and CPU architecture**. Each bundle includes the OCaml native
archives and interfaces, package metadata, private C stubs, and the Rust static
bridge. A downstream application compiles its own OCaml code and links these
libraries; it does not compile the SDK's OCaml, C, or Rust code.

The Rust bridge remains shared across compiler versions during production.
The complete SDK bundles include that bridge so consumers need only one bundle.
Source builds remain supported through the ordinary OPAM package.

## Available bundles

Actions artifacts are named `ocaml-sdk-PLATFORM-ocaml-VERSION` and retained for
90 days. PR/merge-queue runs publish five combinations, master runs ten, and
release runs all sixteen. See the [matrix](release-preflight.md#build-coverage-and-artifact-reuse)
for the combinations. A successful artifact upload means that the producer
built/tested the SDK and then linked and ran a separate application against its
relocated installation. The Linux consumer container mounts neither SDK source
nor the original Dune build tree. Artifacts contain no SDK `.ml` source files.

Published releases include assets named, for example,
`ocaml-temporal-sdk-v0.1.0-rc.1-linux-arm64-ocaml-5.5.1.tar.gz`.
Each contains `library.tar.gz`, `manifest.json`, and the two installation helpers.
The release's top-level manifest records the source commit and all asset hashes.
The first release must be published before these example assets are available.

## Compatibility requirements

- Use the exact OCaml patch and platform on the bundle. Linux bundles target
  Debian 12/glibc, not Alpine/musl. macOS bundles use the macOS 15 ARM64 runner;
  Windows bundles use x64 GNU/MinGW, not MSVC. The application still needs the
  platform's normal native linker and system libraries.
- Use the producer's Dune version and compatible compiled `stdlib`, `unix`,
  `threads`, `logs`, and `yojson` libraries. `manifest.json` records the compiler
  configuration, Dune version, dependency versions, and interface/implementation
  CRCs. Matching OPAM package versions alone is insufficient if compiler flags
  or dependency builds change those CRCs. The installer rejects any mismatch
  before exposing the installation; it never silently compiles from source.
- Python 3.11 or newer runs the small installation helper. Rust, Cargo, and
  `protoc` are not needed to consume this compiled SDK. Python is a transport
  tool, not a dependency of the resulting application.

CI artifacts use the existing development build settings. Release artifacts
use the optimized Rust profile and the same verified OCaml build settings as
CI. The bundle's `profile` identifies that Rust profile; it does not promise
a separately optimized OCaml build.

## Link an application

The following Linux ARM64 example assumes the compiler and compiled dependencies
above are already installed in the current OPAM switch. Select the SDK matching
that environment. Use a trusted release/tag or tested checkout to obtain the
expected source commit, independently of the downloaded SDK manifest.

```sh
set -eu
repository=Analytical-Tradecraft-Technologies/ocaml-temporal
version=v0.1.0-rc.1
asset="ocaml-temporal-sdk-$version-linux-arm64-ocaml-5.5.1.tar.gz"
expected_commit=$(gh api "repos/$repository/git/ref/tags/$version" --jq '.object.sha')
gh release download "$version" --repo "$repository" \
  --pattern "$asset" --pattern SHA256SUMS --dir downloads
(
  cd downloads
  grep -F "  ./$asset" SHA256SUMS | sha256sum --check -
)
mkdir bundle
tar -xzf "downloads/$asset" -C bundle
opam exec -- sh bundle/ocaml-library-environment.sh > consumer-environment.txt
python3 bundle/ocaml-library-artifact.py unpack \
  --directory bundle --prefix "$PWD/prebuilt-sdk" \
  --environment consumer-environment.txt --commit "$expected_commit" \
  --ocaml 5.5.1 --platform linux-arm64 --profile release
export OCAMLPATH="$PWD/prebuilt-sdk/lib"
opam exec -- dune build
```

The application's Dune stanza continues to use `(libraries temporal-sdk)`.
Keep `OCAMLPATH` set when building it. On native Windows, convert this path with
`cygpath -w`; separate multiple OCaml search paths with `;` rather than `:`.
If `OCAMLPATH` already supplies other libraries, append those paths as well.

This installation supplies Dune/findlib libraries, not an OPAM package database
entry. Install the application's other dependencies separately: ordinary
`opam install` of a package depending on `temporal-sdk` still selects its source
package and builds it. The binary path intentionally requires an explicit
downstream build setup. It does not modify global OPAM state or pretend that
a source package was installed.

For a tested Actions run, use `gh run download RUN_ID --repo "$repository"`
with `--name ocaml-sdk-linux-arm64-ocaml-5.5.1 --dir bundle` instead of the
release download/extraction. Use profile `ci` and that producer checkout's
exact `GITHUB_SHA`; on PR runs this is the tested merge commit. Actions
downloads require GitHub authentication. Public release assets do not.

## Reproduce the consumer check

After `make verify OCAML_VERSION=5.5.1` with a verified Rust bundle in
`_build/rust-bridge`, run `make library-artifact OCAML_VERSION=5.5.1`.
Native CI runs `make native-library-artifact` with `NATIVE_OCAML_VERSION` and
`OCAML_ARTIFACT_PLATFORM` set. These commands package the installed SDK, check
its provenance and bytes, relocate it, and compile/run the independent fixture.
They require fresh `_build/ocaml-library-{stage,artifact,consumer}` directories
so an earlier installation cannot accidentally satisfy the check.
