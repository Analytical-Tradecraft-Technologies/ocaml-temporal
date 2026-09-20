#!/bin/sh
set -eu

# Exercise the real Make recipe without Docker, including a failed command
# whose stdout could otherwise be mistaken for a successful compiler probe.
source_root=${1:-.}
temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM

cat >"$temporary_root/compiler.sh" <<'SH'
#!/bin/sh
set -eu
[ "$*" = 'ocamlc -version' ]
printf '%s\n' "$PROBE_OUTPUT"
if [ "$PROBE_STATUS" -ne 0 ]; then
  echo 'compiler probe failed during container build' >&2
fi
exit "$PROBE_STATUS"
SH

# A successful probe still accepts the requested compiler and rejects a
# different version. Leading output must not change the version comparison.
PROBE_OUTPUT="build output
5.4.1" PROBE_STATUS=0 make --no-print-directory \
  -f "$source_root/Makefile" version-check OCAML_VERSION=5.4 \
  RUN="sh '$temporary_root/compiler.sh'"
if PROBE_OUTPUT=5.3.0 PROBE_STATUS=0 make --no-print-directory \
  -f "$source_root/Makefile" version-check OCAML_VERSION=5.4 \
  RUN="sh '$temporary_root/compiler.sh'" >"$temporary_root/log" 2>&1; then
  echo 'version-check accepted the wrong compiler' >&2
  exit 1
fi
grep -Fq 'expected OCaml 5.4.x, got 5.3.0' "$temporary_root/log"

# Preserve failure and its original diagnostic with either empty stdout or
# a valid-looking version; neither case may be reported as a version mismatch.
for output in '' 5.4.1; do
  if PROBE_OUTPUT="$output" PROBE_STATUS=100 make --no-print-directory \
    -f "$source_root/Makefile" version-check OCAML_VERSION=5.4 \
    RUN="sh '$temporary_root/compiler.sh'" >"$temporary_root/log" 2>&1; then
    echo 'version-check accepted a failed compiler probe' >&2
    exit 1
  fi
  grep -Fq 'compiler probe failed during container build' "$temporary_root/log"
  grep -Fq 'Error 100' "$temporary_root/log"
  if grep -Fq 'expected OCaml' "$temporary_root/log"; then
    echo 'version-check obscured a command failure with a version mismatch' >&2
    exit 1
  fi
done

# Compose can write build progress to stdout. Keep it out of machine-readable
# command results and never run a container after an unsuccessful image build.
cat >"$temporary_root/compose.sh" <<'SH'
#!/bin/sh
set -eu
while [ "$#" -gt 0 ]; do
  case "$1" in
    build)
      echo 'Docker build stdout diagnostic'
      echo 'Docker build stderr diagnostic' >&2
      exit "$BUILD_STATUS"
      ;;
    run)
      touch "$RUN_MARKER"
      printf '%s\n' '{"packages":[]}'
      exit 0
      ;;
  esac
  shift
done
echo 'expected a Compose build or run command' >&2
exit 1
SH

BUILD_STATUS=0 RUN_MARKER="$temporary_root/ran" make --silent \
  -f "$source_root/Makefile" cargo-metadata \
  COMPOSE="sh '$temporary_root/compose.sh'" \
  >"$temporary_root/stdout" 2>"$temporary_root/stderr"
[ -f "$temporary_root/ran" ]
[ "$(cat "$temporary_root/stdout")" = '{"packages":[]}' ]
grep -Fq 'Docker build stdout diagnostic' "$temporary_root/stderr"
grep -Fq 'Docker build stderr diagnostic' "$temporary_root/stderr"
rm "$temporary_root/ran"

if BUILD_STATUS=100 RUN_MARKER="$temporary_root/ran" make --silent \
  -f "$source_root/Makefile" version-check OCAML_VERSION=5.4 \
  COMPOSE="sh '$temporary_root/compose.sh'" \
  >"$temporary_root/stdout" 2>"$temporary_root/stderr"; then
  echo 'version-check accepted a failed Docker build' >&2
  exit 1
fi
[ ! -f "$temporary_root/ran" ]
[ ! -s "$temporary_root/stdout" ]
grep -Fq 'Docker build stdout diagnostic' "$temporary_root/stderr"
grep -Fq 'Docker build stderr diagnostic' "$temporary_root/stderr"
grep -Fq 'Error 100' "$temporary_root/stderr"
if grep -Fq 'expected OCaml' "$temporary_root/stderr"; then
  echo 'version-check obscured a build failure with a version mismatch' >&2
  exit 1
fi

echo 'Make Docker command output and error propagation: ok'
