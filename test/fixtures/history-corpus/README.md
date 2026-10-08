# Replay history corpus

Versioned Temporal workflow histories that every candidate SDK must replay
without regeneration. `manifest.json` records each history's provenance,
SHA-256, frozen definition set and expected verdict; `histories/` holds the
binary `History` protobufs (replay input) and the Temporal CLI JSON they were
encoded from.

Do not edit, regenerate or delete committed histories. See
[docs/reference/history-corpus.md](../../../docs/reference/history-corpus.md)
for the manifest format, the Docker-free gate (`make test-history-corpus`), the
capture command (`make history-corpus-capture`) and the addition rules.
