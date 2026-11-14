<!--
  Pull request template (#1140). See CONTRIBUTING.md for the full workflow.
  Use "Fixes #N" only when this PR satisfies every requirement of the issue;
  otherwise use "Refs #N" and say what remains.
-->

## Summary

<!-- What changed and why. Link the issue and any design note. -->

Fixes #

## Tests run

<!-- The exact commands you ran locally and their result, and what is left to CI. -->

- [ ] Focused tests for the changed area:
- [ ] Broadest practical gate (normally `make verify`):

## Checklist

- [ ] Workflow code stays deterministic and replay-safe; replay coverage added where workflow execution changed.
- [ ] Expected operational failures are typed `result` values; exceptions only for defects.
- [ ] No effect constructors, continuations, Rust futures, Tokio, raw pointers or Core details in the public API.
- [ ] Every new cross-language allocation or handle has one owner, one release path and error/shutdown tests.
- [ ] Tests are in separate files (`test/` for OCaml, `rust/core-bridge/tests/` for Rust integration tests).
- [ ] Every new function, module and data structure is documented; `docs/` and `docs/progress.md` are updated where applicable.
- [ ] New dependencies are permissively licensed and recorded in `docs/dependencies.md`.
- [ ] If an AI model changed source code and is not yet listed under "AI disclosure" in `README.md`, it is added there.
