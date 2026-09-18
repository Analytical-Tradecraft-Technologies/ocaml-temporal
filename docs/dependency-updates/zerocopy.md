# zerocopy dependency update

This update retains the existing Cargo manifest constraints and Temporal Core
revision. Cargo selected the following stable releases for the locked graph.
The packages are updated together because their resolution changes overlap or
Cargo requires companion updates to satisfy the new versions.

| Package | Previous lock | Updated lock | License |
|---|---|---|---|
| [zerocopy](https://crates.io/crates/zerocopy/0.8.57) | 0.8.54 | 0.8.57 | BSD-2-Clause OR Apache-2.0 OR MIT |
| [zerocopy-derive](https://crates.io/crates/zerocopy-derive/0.8.57) | 0.8.54 | 0.8.57 | BSD-2-Clause OR Apache-2.0 OR MIT |

Validation: the complete locked Cargo license audit and
`cargo check --manifest-path rust/Cargo.toml --locked --all-targets` passed
locally. Runtime, replay, and cross-platform execution remain CI gates.
