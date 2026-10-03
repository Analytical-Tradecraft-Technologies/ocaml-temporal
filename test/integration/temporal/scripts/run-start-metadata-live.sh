#!/bin/sh
set -eu

# Exercises isolated start metadata, their combination, continue-as-new, and an
# official Temporal CLI (Go SDK) start with deadlines. Each exact active run is
# queried before and after replacing the OCaml worker, then
# signaled to completion. Raw histories and visibility descriptions remain in
# the ignored build tree for conformance/corpus work; this is not a new corpus.
root=$(CDPATH='' cd -- "$(dirname "$0")/../../../.." && pwd)
fixture="$root/test/integration/temporal"
project=${TEMPORAL_COMPOSE_PROJECT:-ocaml-temporal-integration}
image=${TEMPORAL_METADATA_IMAGE:-$project-dev}
worker="$project-start-metadata-worker"
client="$project-start-metadata-client"
evidence="$root/_build/start-metadata-evidence"
executable=/workspace/_build/default/test/integration/temporal/driver/start_metadata_driver.exe
container_evidence=/workspace/_build/start-metadata-evidence
generation=one
prefix="metadata-$(date +%s)-$$"
official_id="$prefix-official"
mkdir -p "$evidence"

# Uses only this test's project/network and already built source image.
compose() {
  docker compose --project-directory "$fixture" --file "$fixture/compose.yaml" \
    --project-name "$project" --profile temporal "$@"
}

# The pinned admin-tools image supplies the official Go-client CLI.
admin() {
  compose run --rm --no-deps temporal-admin-tools temporal \
    --address temporal:7233 --namespace temporal-sdk-test "$@" </dev/null
}

# Both application roles use one prebuilt binary and no long-lived Dune lock.
application() {
  docker run "$@" --network "${project}_temporal-network" \
    --user "${HOST_UID:-$(id -u)}:${HOST_GID:-$(id -g)}" \
    --volume "$root:/workspace" --workdir /workspace \
    --env TEMPORAL_METADATA_LIVE=1 --env TEMPORAL_ADDRESS=http://temporal:7233 \
    --env TEMPORAL_NAMESPACE=temporal-sdk-test \
    --env METADATA_PREFIX="$prefix" \
    --env METADATA_RUNS="$container_evidence/runs.tsv" \
    --env METADATA_IDENTITY="metadata-worker-$generation" \
    --entrypoint timeout "$image" --signal=KILL "${TEMPORAL_METADATA_TIMEOUT_SECONDS:-300}" \
    opam exec -- "$executable" "$mode"
}

# Always remove only the two explicitly owned application containers. Server
# teardown belongs to the caller so a failure retains inspectable histories.
cleanup() {
  docker logs "$worker" > "$evidence/worker-$generation.log" 2>&1 || true
  docker rm -f "$worker" "$client" >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

# Register the exact indexed type before Client.start creates any durable run.
if ! admin operator search-attribute list -o json > "$evidence/search-attributes.json"; then
  exit 1
fi
if ! grep -q 'MetadataKeyword' "$evidence/search-attributes.json"; then
  admin operator search-attribute create --name MetadataKeyword --type Keyword
fi

generation=one
mode=worker
application --detach --name "$worker" > "$evidence/worker-one.container"
mode=start
application --rm --name "$client" > "$evidence/start.log" 2>&1

# An official CLI start exercises the Go SDK's metadata encoding and the
# server-generated execution expiration independently of the OCaml start API.
admin workflow start --workflow-id "$official_id" --type metadata.roundtrip \
  --task-queue ocaml-start-metadata --input '"value:value:deadline"' \
  --memo 'note="value"' --search-attribute 'MetadataKeyword="value"' \
  --execution-timeout 1h --run-timeout 30m --task-timeout 10s \
  -o json > "$evidence/official-start.json"
official_run=$(jq -er '.runId' "$evidence/official-start.json")
printf '%s\t%s\tvalue:value:deadline\n' "$official_id" "$official_run" >> "$evidence/runs.tsv"
admin workflow query --workflow-id "$official_id" --run-id "$official_run" \
  --name metadata -o json > "$evidence/official-query.json"

# Keep the original continue-as-new history as well as each active successor.
while IFS="$(printf '\t')" read -r id run _expected; do
  admin workflow show --workflow-id "$id" --run-id "$run" -o json > "$evidence/$id.root.json"
done < "$evidence/runs.tsv.roots"

# Retain exact-run nonterminal histories before terminating generation one.
while IFS="$(printf '\t')" read -r id run _expected; do
  admin workflow show --workflow-id "$id" --run-id "$run" -o json > "$evidence/$id.initial.json"
  admin workflow describe --workflow-id "$id" --run-id "$run" -o json > "$evidence/$id.describe.initial.json"
done < "$evidence/runs.tsv"
docker logs "$worker" > "$evidence/worker-one.log" 2>&1
# Abrupt replacement deliberately discards the entire in-memory cache.
docker rm -f "$worker" >/dev/null
generation=two
mode=worker
application --detach --name "$worker" > "$evidence/worker-two.container"
mode=finish
application --rm --name "$client" > "$evidence/finish.log" 2>&1
while IFS="$(printf '\t')" read -r id run _expected; do
  admin workflow show --workflow-id "$id" --run-id "$run" -o json > "$evidence/$id.terminal.json"
  admin workflow describe --workflow-id "$id" --run-id "$run" -o json > "$evidence/$id.describe.terminal.json"
done < "$evidence/runs.tsv"
sh "$fixture/scripts/verify-start-metadata-history.sh" "$evidence"
cat "$evidence/start.log" "$evidence/finish.log"
printf 'Metadata restart acceptance passed. Raw evidence: %s\n' "$evidence"
