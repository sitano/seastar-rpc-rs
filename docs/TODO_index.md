# TODO index

Sorted by criticality, highest risk first. See each file for details.

| # | Criticality | File | Issue | Status |
|---|---|---|---|---|
| 1 | Low (feature) | [TODO_pluggable_compression.md](TODO_pluggable_compression.md) | User-defined compressors via a trait, and sending out-of-band empty frames. | Deferred |
| 2 | Low (meta) | [TODO_batch_test_gaps.md](TODO_batch_test_gaps.md) | Not a bug — rewrite the batch review stress test (never committed) as an `#[ignore]` test. | Deferred minor |

**Suggested order:** #1 when a stateful (e.g. dictionary) compressor is needed; close out #2 as cleanup alongside whichever fixes land.
