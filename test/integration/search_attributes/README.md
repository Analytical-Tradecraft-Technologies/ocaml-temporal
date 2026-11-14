# Typed search attributes and memo regression

Start a disposable Temporal server with the `default` namespace, then run:

```sh
_build/default/test/integration/search_attributes/regression.exe check http://127.0.0.1:7233 "$(command -v temporal)"
```

The second argument is the official Temporal CLI, used only to register one
search attribute of every indexed type in `default` when they are not
registered yet: `TypedSaText`, `TypedSaKeyword`, `TypedSaInt`,
`TypedSaDouble`, `TypedSaBool`, `TypedSaDatetime` and `TypedSaKeywordList`.
The test starts and reaps its own worker on a unique task queue and covers
`Temporal.Search_attribute`, `Workflow.upsert_typed_search_attributes` and
`Workflow.upsert_memo` (#1154):

- A run is started with a memo (`kept`, `removed`) and a `Text` attribute
  encoded by `Search_attribute.encode`.
- The run upserts typed `Keyword`, `Int`, `Double`, `Datetime`,
  `KeywordList` and `Bool` values, then unsets the `Bool`, and upserts its
  memo, adding `note` and removing `removed` with a `binary/null` value.
- A `Client.list_visibility` query by the typed keyword and int,
  `TypedSaKeyword = '<queue>' AND TypedSaInt = 42`, finds the run, and
  `Client.count` agrees.
- `Client.describe` on the open run decodes every typed value with
  `Search_attribute.decode`, no longer lists the unset `Bool`, and reports the
  merged memo (`kept`, `note`).
- After a signal the run continues as new. Its history, fetched with
  `Client.fetch_history`, replays against the workflow, and a version
  without the memo upsert is a nondeterminism error. The successor starts
  with the merged memo and the upserted keyword, and `Client.describe`
  reports the merged memo after it completes.

The test terminates its workflows, including after an assertion failure.

CI runs this suite against the Compose Temporal/PostgreSQL stack through
`make test-temporal-live-regressions`, which `make test-temporal-live-ci`
invokes. That target supplies the `default` namespace and the pinned CLI.
