# ADR 0007: Activity Retry Policy Boundary

- Status: accepted
- Date: 2026-07-12
- Decision owners: OCaml Temporal maintainers

## Context

Temporal activity commands may carry retry behavior in addition to their
timeouts and cancellation policy. The public SDK needs to make that behavior
easy to compose from a workflow without exposing protobuf records or asking
workflow authors to manage native handles. Retry configuration is also part of
workflow history, so a value that changes while crossing OCaml, JSON, Rust,
or Core would be a replay and correctness defect.

JSON has no lossless, portable floating-point representation for this private
boundary. Printing a coefficient as a JSON number would allow a parser or
formatter to round it, and OCaml's signed `int64` cannot print every
unsigned 64-bit IEEE-754 bit pattern as a positive integer.

## Decision

Add an opaque `Temporal.Activity.Retry_policy.t` value. The constructor
accepts exact millisecond durations, a finite `float` coefficient, a signed
32-bit maximum-attempt count, and a list of non-retryable error type names.
It returns `(t, Error.t) result` rather than raising for invalid
configuration. `maximum_attempts = 0` means unlimited attempts; positive
values include the first attempt.

The public `Activity.start` and `Activity.execute` functions
accept `?retry_policy`. The policy is copied into the private
workflow command, and helper functions can pass it through like any other
labelled option. `None` means that no explicit policy was selected.

The protocol carries the policy as a closed object:

```json
{
  "initial_interval": {"seconds": 1, "nanoseconds": 0},
  "backoff_coefficient_bits": "4609434218613702656",
  "maximum_interval": {"seconds": 60, "nanoseconds": 0},
  "maximum_attempts": 3,
  "non_retryable_error_types": ["InvalidInput"]
}
```

The decimal string is the canonical unsigned representation of the
coefficient's IEEE-754 bits. The required schedule-activity member is JSON
null when no policy is supplied. Rust and OCaml both reject omission, unknown
members, non-canonical decimal text, NaN or infinity, coefficients below
1.0, non-positive initial intervals, maximum intervals below the initial
interval, out-of-range attempt counts, empty or NUL-containing error types,
and oversized text.

Rust converts the validated semantic object to and from Temporal Core's
protobuf retry policy. It uses `f64::from_bits` and `to_bits()`
without formatting the coefficient as a decimal float. Core values are
revalidated before they are exposed to OCaml, so a malformed or unsupported
native value cannot be silently changed into a service default.

## Ownership and replay

The public policy is immutable. Its string list is copied on construction and
when returned by the accessor; the runtime command stores another private
copy. No policy record contains a native pointer or is shared with a Rust
thread. Completion encoding validates the complete command and reparses the
generated JSON before it can cross the C boundary.

A policy is a workflow command input, not mutable worker state. Workflow code
must construct it from deterministic values and must not read clocks, random
sources, or process-global state while choosing a policy. Replay therefore
emits the same policy bytes for the same workflow definition and activation
history.

## Consequences

- Workflow authors get a typed, idiomatic option without seeing protobuf or
  bridge JSON.
- The decimal-bit representation is slightly more verbose than a JSON float,
  but both language implementations can prove exact coefficient preservation.
- `None` and an explicit policy remain distinguishable, which prevents an
  omitted option from being accidentally normalized into a concrete default.
- Retry behavior is validated and translated. The complete
  [PR #279 Actions run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29331237061)
  live-verifies ordinary, heartbeat-detail, start-to-close-timeout,
  heartbeat-timeout, and non-retryable activity retry delivery. Broader
  activity failure/cancellation behavior and replay acceptance remain
  separate follow-up work; focused tests continue to cover policy encoding and
  malformed-input boundaries.

## Evidence

The public constructor and accessors are covered by
[`test/unit/test_activity_retry_policy.ml`](../../test/unit/test_activity_retry_policy.ml).
The runtime command representation is covered by
[`test/runtime/test_activity_retry_policy.ml`](../../test/runtime/test_activity_retry_policy.ml).
OCaml protocol encoding and bilateral validation are covered by
[`test/bridge/test_ocaml_workflow_protocol.ml`](../../test/bridge/test_ocaml_workflow_protocol.ml);
Rust JSON/Core conversion and strict malformed inputs are covered by
[`rust/core-bridge/tests/workflow_retry_policy.rs`](../../rust/core-bridge/tests/workflow_retry_policy.rs).
