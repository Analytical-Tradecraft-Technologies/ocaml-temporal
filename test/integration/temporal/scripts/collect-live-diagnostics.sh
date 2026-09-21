#!/bin/sh
set -eu

# Opt-in, best-effort snapshots before the live controllers delete evidence.
# Only the checked-in synthetic fixture is supported. No production endpoint,
# arbitrary directory, raw history, Docker environment or process arguments
# are accepted as publication inputs. Callers must ignore collection failure
# and keep the original scenario status authoritative.
[ -n "${TEMPORAL_DIAGNOSTICS_DIR:-}" ] || exit 0
root=$(CDPATH='' cd -- "$(dirname "$0")/../../../.." && pwd)
fixture="$root/test/integration/temporal"
filter="$fixture/scripts/diagnostic-filter.jq"
scenario=${TEMPORAL_DIAGNOSTICS_SCENARIO:?missing diagnostic scenario}
phase=${1:-snapshot}
case "$scenario" in
  integration) pattern='.smoke-*' ;;
  restart|crash) pattern='.restart-replay-*' ;;
  cache-eviction) pattern='.cache-eviction*' ;;
  patching) pattern='.patch-replay-*' ;;
  parent-child-restart) pattern='.parent-child-restart-*' ;;
  child-failure-replay) pattern='.child-failure-replay-*' ;;
  *) echo 'unsupported synthetic diagnostic scenario' >&2; exit 2 ;;
esac
case "$phase" in *[!a-zA-Z0-9_-]*|'') exit 2 ;; esac
project=${TEMPORAL_COMPOSE_PROJECT:-ocaml-temporal-integration}
command -v jq >/dev/null
umask 077
destination="$TEMPORAL_DIAGNOSTICS_DIR/$scenario"
mkdir -p "$destination"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# Twelve snapshots cover the largest patching controller's six generations
# and cleanup. A snapshot owns at most 32 files of 64 KiB plus its small
# manifest: the seven-scenario job is bounded below 180 MiB uncompressed.
count=$(find "$destination" -maxdepth 1 -type d -name 'snapshot-*' | wc -l | tr -d ' ')
if [ "$count" -ge 12 ]; then
  printf '%s\n' 'snapshot limit reached; later snapshot omitted' >"$destination/collection-limit.txt"
  exit 0
fi
snapshot="$destination/snapshot-$(printf '%02d' "$count")-$phase"
mkdir "$snapshot"
files=0
warnings=0
printf 'file\tsource_bytes\tretained_bytes\tresult\n' >"$snapshot/collection.tsv"

# Copies one bounded regular file through the publication filter. Oversized
# JSON and malformed/partial writes get an omission entry, not truncated JSON
# that could be mistaken for a complete SDK/controller record.
publish() {
  source_file=$1
  name=$2
  mode=$3
  [ -f "$source_file" ] && [ ! -L "$source_file" ] || return 0
  if [ "$files" -ge 32 ]; then
    printf '%s\t0\t0\tomitted-file-limit\n' "$name" >>"$snapshot/collection.tsv"
    warnings=$((warnings + 1))
    return 0
  fi
  source_bytes=$(wc -c <"$source_file" | tr -d ' ')
  result=retained
  if [ "$mode" = json ]; then
    if [ "$source_bytes" -gt 65536 ] || ! jq -c --arg mode json -f "$filter" "$source_file" >"$scratch/filtered" 2>/dev/null; then
      printf '%s\t%s\t0\tomitted-invalid-or-oversize-json\n' "$name" "$source_bytes" >>"$snapshot/collection.tsv"
      warnings=$((warnings + 1))
      return 0
    fi
  else
    # Scan from the beginning before bounding the tail, so a byte offset can
    # never expose a continuation whose sensitive key was outside the window.
    # Large build logs are omitted explicitly rather than read without limit.
    if [ "$source_bytes" -gt 4194304 ]; then
      printf '%s\t%s\t0\tomitted-oversize-log\n' "$name" "$source_bytes" >>"$snapshot/collection.tsv"
      warnings=$((warnings + 1))
      return 0
    fi
    jq -Rnr --arg mode text -f "$filter" "$source_file" >"$scratch/filtered"
    result=filtered-tail

  fi
  retained=$(wc -c <"$scratch/filtered" | tr -d ' ')
  if [ "$retained" -gt 65536 ]; then
    # Redaction can increase bytes. Fail closed rather than cut a JSON value.
    printf '%s\t%s\t0\tomitted-oversize-projection\n' "$name" "$source_bytes" >>"$snapshot/collection.tsv"
    warnings=$((warnings + 1))
    return 0
  fi
  cp "$scratch/filtered" "$snapshot/$name"
  files=$((files + 1))
  printf '%s\t%s\t%s\t%s\n' "$name" "$source_bytes" "$retained" "$result" >>"$snapshot/collection.tsv"
}

# A dead Docker daemon must not consume the controller's remaining cleanup
# budget. Each read is bounded to five seconds and 128 filesystem blocks;
# the watchdog and child are reaped before their PIDs can be reused.
capture() {
  name=$1
  shift
  (ulimit -f 128; exec "$@") >"$scratch/command" 2>/dev/null &
  command_pid=$!
  (sleep 5; kill -KILL "$command_pid" 2>/dev/null || true) &
  watchdog=$!
  command_status=0
  wait "$command_pid" 2>/dev/null || command_status=$?
  kill "$watchdog" 2>/dev/null || true
  wait "$watchdog" 2>/dev/null || true
  publish "$scratch/command" "$name" text
  if [ "$command_status" -ne 0 ]; then
    printf '%s\t0\t0\tcommand-exit-%s\n' "$name" "$command_status" >>"$snapshot/collection.tsv"
    warnings=$((warnings + 1))
  fi
}

# Enumerate only known fixture-owned paths; hidden raw/describe JSON, arbitrary
# neighbouring files, symlinks and credentials can never enter the artifact.
for source_file in "$fixture"/.*; do
  name=${source_file##*/}
  # The expansion intentionally selects the current scenario's glob.
  # shellcheck disable=SC2254
  case "$name" in $pattern|.worker-stopped) ;; *) continue ;; esac
  case "$name" in *.raw*|*.describe*|*-describe*|*.tmp*) continue ;; esac
  # Filenames are an allowlist too; a neighbouring custom file is not evidence.
  if ! printf '%s\n' "$name" | LC_ALL=C grep -Eq '^\.(smoke-driver\.log|worker-stopped|cache-eviction(\.json|-ready|-second-ready|-driver\.log)|restart-replay-(accepted|result|driver\.log|controller\.json|diagnostics\.json|history\.(initial|terminal)\.json)|patch-replay-(controller\.json|(legacy|new|removal)-(accepted|result|driver\.log|worker-stopped|diagnostics\.json|history\.(initial|terminal)\.json))|(parent-child-restart|child-failure-replay)-(accepted|result|driver\.log|controller\.json|diagnostics\.json|worker-(one|two)-stopped|(parent|child)\.(initial|post-removal|terminal)\.json))$'; then
    continue
  fi
  case "$name" in
    *.json) publish "$source_file" "${name#.}" json ;;
    *.log)
      publish "$source_file" "${name#.}" text
      if [ -f "$source_file" ] && [ ! -L "$source_file" ] && [ "$(wc -c <"$source_file")" -le 4194304 ]; then
        jq -Rnc --arg mode phases -f "$filter" "$source_file" >"$scratch/phases"
        publish "$scratch/phases" "${name#.}.executions.json" json
      fi ;;
    *-accepted|*-result|*-stopped|*-ready)
      # Markers are not arbitrary text attachments: retain identity lines and
      # the existing payload-free readiness/terminal vocabulary only.
      if [ -f "$source_file" ] && [ ! -L "$source_file" ]; then
        head -c 65536 "$source_file" | LC_ALL=C grep -E '^(workflow_id=two-binary-[a-z0-9-]+|child_workflow_id=two-binary-[a-z0-9-]+|run_id=[a-zA-Z0-9-]+|completed|worker-stopped|initial-completion)$' >"$scratch/marker" || true
        publish "$scratch/marker" "${name#.}" text
      fi ;;
  esac
done
if [ -n "${TEMPORAL_DIAGNOSTICS_CONTROLLER_LOG:-}" ]; then
  publish "$TEMPORAL_DIAGNOSTICS_CONTROLLER_LOG" controller.log text
fi

# Docker selectors are project-scoped and deliberately avoid config/inspect
# output containing environment variables, mounts or full command arguments.
capture containers.tsv docker ps --all --no-trunc --filter "label=com.docker.compose.project=$project" --format '{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}'
# Read from each container's start before enforcing the byte cap. Starting at
# a raw tail could lose the key preceding a multiline sensitive record.
capture services.log docker compose --project-directory "$fixture" --file "$fixture/compose.yaml" --project-name "$project" --profile temporal logs --no-color --timestamps --tail all
capture docker-version.txt docker version --format '{{.Client.Version}} / {{.Server.Version}}'
capture images.txt docker compose --project-directory "$fixture" --file "$fixture/compose.yaml" --project-name "$project" --profile temporal config --images
if [ -f "$snapshot/containers.tsv" ]; then
  # At most four processes snapshots; PID, parent, state and executable name
  # suffice to distinguish an idle poller/build from an exited worker.
  head -n 4 "$snapshot/containers.tsv" | cut -f 1 >"$scratch/container-ids"
  while IFS= read -r container_id; do
    case "$container_id" in *[!a-f0-9]*|'') continue ;; esac
    capture "state-$container_id.txt" docker inspect --format '{{.Id}} running={{.State.Running}} exit_code={{.State.ExitCode}} oom_killed={{.State.OOMKilled}}' "$container_id"
    capture "process-$container_id.txt" docker top "$container_id" -eo pid,ppid,stat,comm
  done <"$scratch/container-ids"
fi
commit=$(git -C "$root" rev-parse HEAD 2>/dev/null || printf unknown)
core=$(sed -n 's/^source = "git+https:\/\/github.com\/temporalio\/sdk-core.git.*#\([a-f0-9]*\)"/\1/p' "$root/rust/Cargo.lock" | head -n 1)
jq -n --arg scenario "$scenario" --arg phase "$phase" --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg sdk_commit "$commit" --arg core_commit "$core" --arg project "$project" --argjson warnings "$warnings" --argjson files "$files" '{schema_version:1,scenario:$scenario,phase:$phase,captured_at:$timestamp,sdk_commit:$sdk_commit,core_commit:$core_commit,compose_project:$project,synthetic_only:true,files:$files,collection_warnings:$warnings,limits:{file_bytes:65536,snapshot_files:32,scenario_snapshots:12},scenario_result:"see result.json; collection does not determine acceptance"}' >"$snapshot/manifest.json"
