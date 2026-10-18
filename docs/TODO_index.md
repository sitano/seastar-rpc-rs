# TODO index

Sorted by criticality, highest risk first. See each file for details.

| # | Criticality | File | Issue | Status |
|---|---|---|---|---|
| 1 | Low (meta) | [TODO_batch_test_gaps.md](TODO_batch_test_gaps.md) | Not a bug — tracks missing deterministic tests for the state-switch race, a writer error mid-batch, the arena-shrink-after-split path, and reviving a deleted stress test. | Deferred minor |
| 2 | Lowest / not a bug | [TODO_replies_borrow_batch.md](TODO_replies_borrow_batch.md) | `Batch::send`'s `Result` borrows `&mut Batch` even on `Err`. Documented as "not a bug, follows the agreed API"; recommendation is to document it. | Ergonomics note |

**Suggested order:** close out #1-#2 as cleanup/documentation alongside whichever fixes land.
