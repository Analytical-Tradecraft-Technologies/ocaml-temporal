#!/bin/sh
# Captures a fresh set of replay-corpus histories from a running Temporal
# stack and stages them for review (issue #518).
#
# The Make target `history-corpus-capture` sets every variable below for the
# Docker Compose stack; maintainers may also run the script directly against
# any disposable server. The worker generations run one after another, so the
# patch workflow type can be served by a different source generation each
# time. Nothing in the committed corpus changes unless
# HISTORY_CORPUS_INSTALL names the corpus directory, and installation is
# append-only (see export-history-corpus.py).
#
# Required environment:
#   HISTORY_CORPUS_CAPTURE_ID   stable capture identifier, e.g. live-2026-10-08
#   HISTORY_CORPUS_RUN          shell command prefix that runs
#                               history_corpus_capture.exe GENERATION OUTPUT
#                               with TEMPORAL_ADDRESS/TEMPORAL_NAMESPACE set
#   HISTORY_CORPUS_TEMPORAL_CLI shell command prefix that runs the Temporal CLI
#                               against the same server
# Optional environment:
#   HISTORY_CORPUS_CAPTURE_DIR  staging directory relative to the repository
#                               root (default .history-corpus-capture)
#   HISTORY_CORPUS_INSTALL      corpus directory to append the staged entries to
#   HISTORY_CORPUS_COMMAND      capture command recorded as provenance
#   HISTORY_CORPUS_CORE_PROTOS  pinned Core protos directory; by default it is
#                               located with `cargo metadata` on the host
#   HISTORY_CORPUS_GENERATIONS  space-separated worker generations to run, in
#                               order (default: every generation below). Use
#                               it to capture only new definition sets.
#   TEMPORAL_NAMESPACE          namespace (default temporal-sdk-test)
set -eu

: "${HISTORY_CORPUS_CAPTURE_ID:?set HISTORY_CORPUS_CAPTURE_ID}"
: "${HISTORY_CORPUS_RUN:?set HISTORY_CORPUS_RUN}"
: "${HISTORY_CORPUS_TEMPORAL_CLI:?set HISTORY_CORPUS_TEMPORAL_CLI}"
capture_dir=${HISTORY_CORPUS_CAPTURE_DIR:-.history-corpus-capture}
namespace=${TEMPORAL_NAMESPACE:-temporal-sdk-test}
command_text=${HISTORY_CORPUS_COMMAND:-"make history-corpus-capture HISTORY_CORPUS_CAPTURE_ID=$HISTORY_CORPUS_CAPTURE_ID"}

# Relative paths keep the staging directory identical inside the Compose
# development container (working directory /workspace) and on the host.
case "$capture_dir" in
  /*|*..*) echo "HISTORY_CORPUS_CAPTURE_DIR must be a relative path inside the repository" >&2; exit 2 ;;
esac
rm -rf "$capture_dir"
mkdir -p "$capture_dir"

# The corpus-v6-child-start-metadata generation (#1135) sets the
# CorpusChildKeyword search attribute on a child, and Temporal fails the
# parent's workflow task for an unknown attribute. Register it (an existing
# registration is fine), wait until the namespace lists it, then allow the
# frontend's search-attribute cache a few seconds to refresh, so the captured
# parent history does not record a failed workflow task.
register_corpus_child_keyword() {
  eval "$HISTORY_CORPUS_TEMPORAL_CLI"' operator search-attribute create --namespace "$namespace" --name CorpusChildKeyword --type Keyword' >/dev/null 2>&1 || true
  attempts=0
  until eval "$HISTORY_CORPUS_TEMPORAL_CLI"' operator search-attribute list --namespace "$namespace"' 2>/dev/null | grep -q CorpusChildKeyword; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 60 ]; then
      echo "CorpusChildKeyword was not registered in $namespace" >&2
      exit 1
    fi
    sleep 1
  done
  sleep 5
}

# Generation order is fixed: the two older patch generations run before the
# current corpus-v1 worker, so each patch history is produced by exactly one
# source generation and the workflow IDs never collide with an open run. The
# patch lifecycle generations follow their stage order; the legacy one leaves
# a parked handover run that the patched one must find and finish.
default_generations="corpus-v1-patch-legacy corpus-v1-patch-deprecated corpus-v1 \
corpus-lifecycle-legacy corpus-lifecycle-patched corpus-lifecycle-deprecated \
corpus-lifecycle-removed"
for generation in ${HISTORY_CORPUS_GENERATIONS:-$default_generations}; do
  echo "history corpus capture: generation=$generation" >&2
  case "$generation" in
    corpus-v6-child-start-metadata) register_corpus_child_keyword ;;
  esac
  # The prefix is shell text (it may carry quoted paths and leading
  # environment assignments, as Make's Compose commands do), so it is
  # evaluated rather than word-split. The arguments stay quoted.
  eval "$HISTORY_CORPUS_RUN"' "$generation" "$capture_dir/executions.$generation.json"'
done

set -- --capture-dir "$capture_dir" --capture-id "$HISTORY_CORPUS_CAPTURE_ID" \
  --temporal-cli "$HISTORY_CORPUS_TEMPORAL_CLI" --namespace "$namespace" \
  --capture-command "$command_text"
if [ -n "${HISTORY_CORPUS_CORE_PROTOS:-}" ]; then
  set -- "$@" --core-protos "$HISTORY_CORPUS_CORE_PROTOS"
fi
if [ -n "${HISTORY_CORPUS_INSTALL:-}" ]; then
  set -- "$@" --install "$HISTORY_CORPUS_INSTALL"
fi
python3 test/history_corpus/scripts/export-history-corpus.py "$@"
