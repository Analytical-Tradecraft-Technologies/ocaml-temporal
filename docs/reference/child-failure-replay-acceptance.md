# Child failure after worker-replay acceptance

This acceptance fixture complements the successful parent/child replacement
gate. It starts a parent and a child in generation one, stops that worker while
the child is waiting on a durable 60-second timer, and starts a fresh worker
generation. Both workflow activations must report replay before the child is
allowed to finish.

The child then returns a typed, non-retryable workflow failure. Temporal records
that as `WorkflowExecutionFailed` in the child history and
`ChildWorkflowExecutionFailed` in the parent history. The parent handles the
typed child failure and returns the stable result
`SMOKE:PARENT:CHILD:FAILURE_RECOVERED`. An unexpected successful child result,
wrong child identity, or a failure before replay is rejected.

The client-only driver and worker-only binary are the same binaries used by the
successful parent/child fixture. `SMOKE_PARENT_CHILD_REPLAY_SCENARIO=failure`
selects the failure definitions and the external controller passes the exact
parent and child run IDs into every history query. The history validator is
parameterized with the child workflow type and outcome, while the controller
validator requires `child_failure_recovered`. The source-only contract derives
failure snapshots from the checked-in success snapshots and proves that the
success validators reject the failure event sequence.

Run the contract with:

```sh
make test-temporal-parent-child-failure-replay-contract
```

The live gate, which requires Docker, PostgreSQL, and Temporal Server, is:

```sh
make test-temporal-parent-child-failure-replay-live
```

The [complete PR #361 CI run](https://github.com/Analytical-Tradecraft-Technologies/ocaml-temporal/actions/runs/29475615866)
passed the supported OCaml and native-platform matrix and the Temporal /
PostgreSQL live gate. That live smoke verified replay before failure, the
typed child failure, and the recovered parent result. The source-only contract
remains useful for Docker-free validation, but it does not replace the
real-server evidence.
