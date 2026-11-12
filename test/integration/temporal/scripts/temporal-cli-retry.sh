#!/bin/sh
# Shared transport-failure classification and bounded retry for one-shot
# official Temporal CLI calls in the live tests (#753, #839, #1285).
#
# This file is sourced, never executed: it only defines functions, so it
# neither enables `set -e` nor changes the caller's traps. It is sourced by
# check-temporal-stack.sh inside the admin-tools image (Compose mounts it next
# to that script) and by the host-side live controllers and Make recipes. Every
# variable it assigns starts with `_tcr_`, because POSIX sh has no local
# variables and callers keep their own `attempt`, `status` and similar names.
#
# Only the transport between the CLI and the frontend is treated as transient.
# A failure that the server reported after receiving the request, an
# assertion in the caller, authentication or configuration errors, and Compose
# or container failures are returned on the first attempt.

# temporal_cli_transient_error TEXT
#
# Succeeds when TEXT (the CLI's captured stderr) describes a transient
# transport failure: a gRPC status of Unavailable or DeadlineExceeded, or the
# CLI's own connection deadline. The connection-deadline message is matched
# only on the first line that starts with "Error: ", which is the CLI's
# primary error. Current CLI releases append diagnostic lines after it
# ("Connection checks", TLS, namespace), and Compose can print progress lines
# before it, so neither the first line of the whole text nor an exact match on
# the whole text would classify it correctly. A fatal primary error whose
# diagnostics merely mention the deadline text is not transient.
temporal_cli_transient_error() {
  case "$1" in
    *"code = Unavailable"*|*"code = DeadlineExceeded"*) return 0 ;;
  esac
  _tcr_primary=$(printf '%s\n' "$1" | awk '/^Error: /{print; exit}')
  case "$_tcr_primary" in
    "Error: failed connecting to Temporal server at "*": context deadline exceeded") return 0 ;;
  esac
  return 1
}

# temporal_cli_read_only COMMAND [ARGUMENT...]
#
# Runs one read-only CLI query (workflow describe/show/query/list,
# search-attribute list) and retries it while it fails with a transient
# transport error. COMMAND may be a shell function of the caller, such as a
# Compose wrapper. Use it only for calls that are safe to repeat: a mutating
# call whose first attempt may have committed (workflow start, search-attribute
# create, signal) must not be wrapped unless it is idempotent.
#
# Standard output is buffered and written once, from the final attempt only,
# so a caller's `>file` redirection never receives a partial or failed
# response. The final attempt's stderr is passed through unchanged, which
# keeps each caller's diagnostic reporting as it was. Each retry also prints
# one line naming the attempt and the CLI's primary error to stderr.
#
# TEMPORAL_CLI_RETRY_ATTEMPTS (default 5) bounds the total number of attempts
# and TEMPORAL_CLI_RETRY_SLEEP_SECONDS (default 3) is the pause between them.
# Returns the final attempt's exit status, or 2 for an invalid setting.
temporal_cli_read_only() {
  _tcr_attempts=${TEMPORAL_CLI_RETRY_ATTEMPTS:-5}
  _tcr_sleep=${TEMPORAL_CLI_RETRY_SLEEP_SECONDS:-3}
  case "$_tcr_attempts" in
    ''|*[!0-9]*|0) echo "TEMPORAL_CLI_RETRY_ATTEMPTS must be a positive integer" >&2; return 2 ;;
  esac
  case "$_tcr_sleep" in
    ''|*[!0-9]*) echo "TEMPORAL_CLI_RETRY_SLEEP_SECONDS must be a non-negative integer" >&2; return 2 ;;
  esac
  if [ "$#" -eq 0 ]; then
    echo "temporal_cli_read_only needs a command" >&2
    return 2
  fi
  _tcr_work=$(mktemp -d) || return 2
  _tcr_attempt=1
  while :; do
    _tcr_status=0
    "$@" >"$_tcr_work/stdout" 2>"$_tcr_work/stderr" || _tcr_status=$?
    if [ "$_tcr_status" -ne 0 ] && [ "$_tcr_attempt" -lt "$_tcr_attempts" ] \
      && temporal_cli_transient_error "$(cat "$_tcr_work/stderr")"; then
      _tcr_first=$(awk '/^Error: |code = (Unavailable|DeadlineExceeded)/{print; exit}' "$_tcr_work/stderr")
      printf 'transient Temporal CLI failure (attempt %s of %s), retrying in %ss: %s\n' \
        "$_tcr_attempt" "$_tcr_attempts" "$_tcr_sleep" "$_tcr_first" >&2
      _tcr_attempt=$((_tcr_attempt + 1))
      sleep "$_tcr_sleep"
      continue
    fi
    cat "$_tcr_work/stdout"
    cat "$_tcr_work/stderr" >&2
    rm -rf "$_tcr_work"
    return "$_tcr_status"
  done
}
