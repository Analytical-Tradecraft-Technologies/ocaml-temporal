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

# Generation order is fixed: the two older patch generations run before the
# current corpus-v1 worker, so each patch history is produced by exactly one
# source generation and the workflow IDs never collide with an open run.
for generation in corpus-v1-patch-legacy corpus-v1-patch-deprecated corpus-v1; do
  echo "history corpus capture: generation=$generation" >&2
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
