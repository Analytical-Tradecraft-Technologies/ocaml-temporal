# Signals before the first workflow task

Regression for [#694](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/issues/694).
The workflow returns the values collected by two signal handlers without
suspending. Both signals occur before the first workflow task, so Core delivers
them in the same activation as initialization. The expected result is
`first,second`; starting the root before the handlers instead returns an empty
string and discards both signals.

`history.json` is a **synthetic** seven-event Temporal history, not a capture
from a live server. It contains workflow start, two ordered signals, one
scheduled/started/completed workflow task, and workflow completion.
`history.replay.json` contains the same history encoded as a Temporal API
`History` protobuf in the private replay envelope. Conversion used the API
descriptors from pinned Core `95e97686a079dcfe6c42e3254b2f3f5e3d97408f` via
`protoc --include_imports --descriptor_set_out`, followed by Python protobuf's
`json_format.ParseDict` and `SerializeToString`. No dependency was added.

`test_initial_signals_replay.ml` feeds the fixture through the real native
supervisor and Core replay worker, checks the actual initialization-plus-signals
activation, executes the public signal handlers through the production worker
adapter, verifies the exact completed result, and requires natural replay
finalization. It rejects failed tasks, failed Core state machines, and incomplete
replay. The Dune test runs in the ordinary native CI suite without a server:

```sh
opam exec -- dune runtest test/integration/temporal/initial_signals
```

The focused `test/runtime/test_initial_handlers.ml` also covers root decisions
before suspension, multiple handlers, suspended handlers, initial updates,
application failures, defects, and initialization teardown in ordinary and
replay activation modes.

This correction changes the order in which initial handlers and the root can
emit commands. Histories produced by the previous ordering may replay
differently; replay existing workflow histories before rolling out the change.
The fixture qualifies the corrected Core contract, not compatibility with every
history produced by the former behavior.
