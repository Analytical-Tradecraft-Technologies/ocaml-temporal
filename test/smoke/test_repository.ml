(** Normalizes source-control line endings before metadata assertions. Git may
    materialize CRLF files on Windows, while repository needles are written
    with OCaml's LF string literals; treating both forms alike keeps these
    source-shape tests about content rather than checkout policy. *)
let normalize_newlines contents =
  let length = String.length contents in
  let normalized = Buffer.create length in
  let rec copy offset =
    if offset < length then
      if
        contents.[offset] = '\r'
        && offset + 1 < length
        && contents.[offset + 1] = '\n'
      then (
        Buffer.add_char normalized '\n';
        copy (offset + 2))
      else (
        Buffer.add_char normalized contents.[offset];
        copy (offset + 1))
  in
  copy 0;
  Buffer.contents normalized

(** Reads a whole repository file in binary mode, normalizes its line endings,
    and always closes it. *)
let read path =
  let channel = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in channel)
    (fun () ->
      really_input_string channel (in_channel_length channel)
      |> normalize_newlines)

(** Reports whether [needle] occurs in [haystack] without adding a test-only
    string dependency. *)
let contains ~needle haystack =
  let needle_length = String.length needle in
  let haystack_length = String.length haystack in
  let rec search offset =
    if offset + needle_length > haystack_length then false
    else if String.sub haystack offset needle_length = needle then true
    else search (offset + 1)
  in
  needle_length = 0 || search 0

(** Fails with the path and missing text when repository metadata drifts. *)
let require_text ~path ~needle =
  let contents = read path in
  if not (contains ~needle contents) then
    failwith (Printf.sprintf "%s does not contain %S" path needle)

(** Exercises the Windows checkout form directly so future changes cannot
    reintroduce platform-sensitive multiline source assertions. *)
let test_line_ending_normalization () =
  if normalize_newlines "first\r\nsecond\r\n" <> "first\nsecond\n" then
    failwith "CRLF source text was not normalized"

(** Verifies that the project license file is the expected Apache license. *)
let test_license () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let license = Filename.concat source_root "LICENSE" in
  if not (String.starts_with ~prefix:"Apache License" (read license)) then
    failwith "LICENSE does not begin with the Apache License marker"

(** Checks package identity, maintainership, experimental status, repository
    links, and exact dependency declarations in both generated build
    ecosystems. *)
let test_package_metadata () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let source path = Filename.concat source_root path in
  let opam = source "temporal-sdk.opam" in
  let locked = source "temporal-sdk.opam.locked" in
  let dune_project = source "dune-project" in
  let cargo_manifest = source "rust/Cargo.toml" in
  let cargo_sbom = source "scripts/generate-cargo-sbom.py" in
  if Sys.file_exists (source "temporal.opam") then
    failwith "the retired temporal.opam package manifest still exists";
  require_text ~path:opam
    ~needle:"synopsis: \"Experimental Temporal workflows in modern OCaml\"";
  require_text ~path:opam
    ~needle:
      "description: \"An experimental typed OCaml 5 workflow SDK backed by Temporal Core\"";
  require_text ~path:opam ~needle:"maintainer: \"Michael Fowlie\"";
  require_text ~path:opam ~needle:"authors: \"Michael Fowlie\"";
  require_text ~path:opam
    ~needle:"tags: [ \"temporal\" \"workflow\" \"sdk\" \"experimental\" ]";
  require_text ~path:opam ~needle:"x-maintenance-intent: [ \"(latest)\" ]";
  require_text ~path:opam
    ~needle:
      "homepage: \"https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal\"";
  require_text ~path:opam
    ~needle:
      "bug-reports: \"https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues\"";
  require_text ~path:opam
    ~needle:
      "dev-repo: \"git+https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal.git\"";
  require_text ~path:opam ~needle:"\"logs\" {>= \"0.10\"}";
  require_text ~path:opam ~needle:"\"yojson\" {>= \"3.0\"}";
  require_text ~path:locked ~needle:"name: \"temporal-sdk\"";
  require_text ~path:locked
    ~needle:"synopsis: \"Experimental Temporal workflows in modern OCaml\"";
  require_text ~path:locked
    ~needle:
      "description: \"An experimental typed OCaml 5 workflow SDK backed by Temporal Core\"";
  require_text ~path:locked ~needle:"maintainer: \"Michael Fowlie\"";
  require_text ~path:locked ~needle:"authors: \"Michael Fowlie\"";
  require_text ~path:locked
    ~needle:"tags: [ \"temporal\" \"workflow\" \"sdk\" \"experimental\" ]";
  require_text ~path:locked ~needle:"x-maintenance-intent: [ \"(latest)\" ]";
  require_text ~path:locked
    ~needle:
      "homepage: \"https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal\"";
  require_text ~path:locked
    ~needle:
      "bug-reports: \"https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues\"";
  require_text ~path:locked
    ~needle:
      "dev-repo: \"git+https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal.git\"";
  require_text ~path:locked ~needle:"\"logs\" {= \"0.10.0\"}";
  require_text ~path:locked ~needle:"\"yojson\" {= \"3.0.0\"}";
  require_text ~path:dune_project
    ~needle:"(source (github Analytical-Tradecraft-Technologies/ocaml-temporal))";
  require_text ~path:cargo_manifest
    ~needle:
      "repository = \"https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal\"";
  require_text ~path:cargo_sbom
    ~needle:
      "NAMESPACE = \"https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/sbom/cargo\"";
  require_text ~path:dune_project
    ~needle:"(synopsis \"Experimental Temporal workflows in modern OCaml\")";
  require_text ~path:dune_project
    ~needle:
      "(description \"An experimental typed OCaml 5 workflow SDK backed by Temporal Core\")";
  require_text ~path:dune_project
    ~needle:"(tags (temporal workflow sdk experimental))";
  require_text ~path:dune_project ~needle:"(authors \"Michael Fowlie\")";
  require_text ~path:dune_project ~needle:"(maintainers \"Michael Fowlie\")";
  require_text ~path:dune_project ~needle:"(maintenance_intent \"(latest)\")";
  require_text ~path:dune_project ~needle:"(name temporal-sdk)";
  require_text ~path:dune_project ~needle:"(logs (>= 0.10))";
  require_text ~path:dune_project ~needle:"(yojson (>= 3.0))";
  if contains ~needle:"alcotest" (read (source "test/bridge/dune")) then
    failwith "bridge protocol tests must not add an Alcotest dependency"

(** Ensures Dune never attempts to turn the statically linked Rust bridge into a
    dynamically loadable OCaml stub library. Rust reports GNU-style native
    linker flags on Windows, but FlexDLL cannot consume those flags while
    constructing a DLL. The SDK intentionally ships an OCaml-owned executable
    with the Rust bridge linked into it, so static foreign archives are the
    portable and architecturally correct mode. *)
let test_static_foreign_archives () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let workspace = Filename.concat source_root "dune-workspace" in
  let bridge = Filename.concat source_root "lib/core_bridge/dune" in
  require_text ~path:workspace
    ~needle:"(disable_dynamically_linked_foreign_archives true)";
  require_text ~path:bridge ~needle:"(foreign_library";
  require_text ~path:bridge ~needle:"(archive_name temporal_native_stubs)";
  require_text ~path:bridge ~needle:"(no_dynlink)";
  require_text
    ~path:(Filename.concat source_root "scripts/build-rust-bridge.sh")
    ~needle:"scripts/render-rust-link-flags.sh";
  require_text ~path:bridge
    ~needle:
      "(setenv\n   OCAML_TEMPORAL_RUST_TARGET_FALLBACK\n   %{workspace_root}/rust-target";
  require_text
    ~path:(Filename.concat source_root "scripts/build-rust-bridge.sh")
    ~needle:"if [ -n \"${CARGO_TARGET_DIR:-}\" ]; then";
  require_text
    ~path:(Filename.concat source_root "scripts/build-rust-bridge.sh")
    ~needle:"export CARGO_TARGET_DIR=$target_root";
  if contains ~needle:"(foreign_stubs" (read bridge) then
    failwith "lib/core_bridge/dune must not build a temporary native-stubs DLL"

(** Ensures the reusable mailbox remains an internal build unit rather than an
    installed sublibrary visible to [temporal-sdk] consumers. *)
let test_mailbox_is_private () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let dune = Filename.concat source_root "lib/mailbox_processor/dune" in
  let interface =
    Filename.concat source_root "lib/mailbox_processor/mailbox_processor.mli"
  in
  require_text ~path:dune ~needle:"(name temporal_mailbox_processor)";
  require_text
    ~path:interface
    ~needle:"must not call [post], [call], or [join] on that same processor";
  require_text ~path:interface ~needle:"Calling [close] from the handler is safe";
  if contains ~needle:"public_name" (read dune) then
    failwith "the mailbox processor must remain a Dune-private library"

(** Ensures native graph ownership remains an internal implementation unit and
    that its interface cannot return the owner-confined backend state. *)
let test_sdk_supervisor_is_private () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let dune = Filename.concat source_root "lib/sdk_supervisor/dune" in
  let interface =
    Filename.concat source_root "lib/sdk_supervisor/sdk_supervisor.mli"
  in
  require_text ~path:dune ~needle:"(name temporal_sdk_supervisor)";
  require_text
    ~path:interface
    ~needle:"No backend state or\n    native handle can be obtained";
  require_text
    ~path:interface
    ~needle:"must be offloaded by any cooperative scheduler adapter";
  if contains ~needle:"public_name" (read dune) then
    failwith "the SDK supervisor must remain a Dune-private library"

(** Ensures every implementation library linked by [Temporal] uses Dune's
    package-private installation namespace. A missing [package] field would
    make an internal archive discoverable as a normal findlib dependency even
    when no public module re-exports it. *)
let test_internal_libraries_are_package_private () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let private_libraries =
    [ "lib/base/dune"; "lib/protocol/dune"; "lib/core_bridge/dune";
      "lib/future_kernel/dune"; "lib/runtime/dune";
      "lib/mailbox_processor/dune"; "lib/sdk_supervisor/dune" ]
  in
  List.iter
    (fun relative_path ->
      let path = Filename.concat source_root relative_path in
      require_text ~path ~needle:"(package temporal-sdk)";
      if contains ~needle:"(public_name" (read path) then
        failwith (path ^ " must not declare a public findlib name"))
    private_libraries

(** Protects the one-shot quality gate from drifting into the per-version
    compiler matrix. The Make targets remain the local interface, while CI
    pins the scanner actions independently of mutable release tags. *)
let test_quality_gate_contract () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let source path = Filename.concat source_root path in
  let makefile = source "Makefile" in
  let deny = source "deny.toml" in
  require_text ~path:makefile ~needle:"quality: quality-rust quality-spelling";
  require_text ~path:makefile ~needle:"cargo deny --manifest-path";
  require_text ~path:makefile ~needle:"cargo machete --with-metadata rust";
  require_text ~path:makefile ~needle:"typos";
  require_text ~path:deny ~needle:"required-git-spec = \"rev\"";
  require_text
    ~path:deny
    ~needle:"https://github.com/temporalio/sdk-core"

let () =
  test_line_ending_normalization ();
  test_license ();
  test_package_metadata ();
  test_static_foreign_archives ();
  test_mailbox_is_private ();
  test_sdk_supervisor_is_private ();
  test_internal_libraries_are_package_private ();
  test_quality_gate_contract ()
