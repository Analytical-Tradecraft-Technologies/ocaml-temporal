# bon dependency update

This update retains the existing Cargo manifest constraints and Temporal Core
revision. Cargo selected the following stable releases for the locked graph.
The packages are updated together because their resolution changes overlap or
Cargo requires companion updates to satisfy the new versions.

| Package | Previous lock | Updated lock | License |
|---|---|---|---|
| [bon](https://crates.io/crates/bon/3.10.1) | 3.9.3 | 3.10.1 | MIT OR Apache-2.0 |
| [bon-macros](https://crates.io/crates/bon-macros/3.10.1) | 3.9.3 | 3.10.1 | MIT OR Apache-2.0 |
| [darling](https://crates.io/crates/darling/0.24.1) | 0.23.0 | 0.24.1 | MIT |
| [darling_core](https://crates.io/crates/darling_core/0.24.1) | 0.23.0 | 0.24.1 | MIT |
| [darling_macro](https://crates.io/crates/darling_macro/0.24.1) | 0.23.0 | 0.24.1 | MIT |
| [prettyplease](https://crates.io/crates/prettyplease/0.3.0) | new | 0.3.0 | MIT OR Apache-2.0 |

Validation: the complete locked Cargo license audit and
`cargo check --manifest-path rust/Cargo.toml --locked --all-targets` passed
locally. Runtime, replay, and cross-platform execution remain CI gates.
