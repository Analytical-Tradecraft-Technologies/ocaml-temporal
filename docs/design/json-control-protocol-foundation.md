# JSON Control Protocol Foundation Design

**Status:** Approved for implementation
**Date:** 2026-07-11

## Goal

Establish the private, strict JSON envelope shared by OCaml and the project-owned
Rust Temporal Core bridge without implementing worker polling or Temporal
activation semantics. The foundation must make later message bodies auditable,
bounded, correlated, and independently validated on both sides.

## Chosen approach

The protocol uses a closed outer envelope with three kinds: `request`,
`response`, and `error`. Requests and responses carry a bounded operation name,
a lowercase 128-bit hexadecimal correlation identifier, and a JSON object body.
The body is structurally validated and normalized by this foundation; each
future operation must add a closed semantic validator for its body. Errors use
a closed error object rather than an unconstrained body.

This separates stable transport concerns from worker semantics. Defining the
complete activation and completion variants now would duplicate work before the
Core translation is available. Encoding an entire future message as an opaque
base64 blob would defeat JSON's auditability. A generic envelope with a strictly
bounded object body preserves auditability while keeping this change small.

Opaque Temporal payload bytes use a separate closed `{encoding,data}` value.
Only canonical padded RFC 4648 base64 is accepted. Payload bytes are never
included in error text or logs.

## Compatibility and lifecycle

The existing bridge compatibility number is the protocol compatibility number.
It is checked once before an SDK runtime is created and is not repeated in each
document. Rust and OCaml are compiled and shipped together; mixed protocol
versions are unsupported. Every request receives exactly one terminal response
or error with the same correlation identifier and operation.

## Validation and normalization

Both implementations reject oversized input before parsing, excessive nesting
before recursive allocation, duplicate object keys, unknown envelope or error
fields, missing fields, wrong types, unsupported kinds, invalid identifiers,
invalid operation names, non-integral JSON numbers, invalid base64, and breached
node/string/collection limits. Expected failures are structured `result` values;
parser exceptions and Rust panics do not escape their boundaries.

Normalized output is UTF-8 JSON with no insignificant whitespace. Envelope
fields have a fixed order, body keys are sorted recursively by Unicode code
point order, and integers use their shortest decimal form. Outgoing typed values
are semantically checked, serialized, then passed through the same strict parser
before bytes are returned.

## Verification

Shared valid and invalid fixture manifests drive OCaml and Rust tests. Tests
cover normalization, duplicate keys, missing and unknown fields, wrong types,
invalid base64, document/depth/resource limits, correlation identifiers,
unsupported compatibility, and outgoing self-validation. Draft 2020-12 schemas
document the closed envelope, error, and payload shapes, while runtime behavior
continues to depend on the handwritten validators.

## Scope boundary

This change does not poll a worker, translate Core protobuf, define activation
jobs, define completion commands, or expose protocol modules publicly. Those
features will extend the operation/body layer after the worker boundary exists.
