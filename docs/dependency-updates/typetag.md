# typetag dependency update

This update retains the existing Cargo manifest constraints and Temporal Core
revision. Cargo selected the following stable releases for the locked graph.
The packages are updated together because their resolution changes overlap or
Cargo requires companion updates to satisfy the new versions.

| Package | Previous lock | Updated lock | License |
|---|---|---|---|
| [typetag](https://crates.io/crates/typetag/0.2.23) | 0.2.22 | 0.2.23 | MIT OR Apache-2.0 |
| [typetag-impl](https://crates.io/crates/typetag-impl/0.2.23) | 0.2.22 | 0.2.23 | MIT OR Apache-2.0 |

Validation: the complete locked Cargo license audit and
`cargo check --manifest-path rust/Cargo.toml --locked --all-targets` passed
locally. Runtime, replay, and cross-platform execution remain CI gates.
