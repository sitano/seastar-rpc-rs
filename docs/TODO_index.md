# TODO index

Sorted by criticality, highest risk first. See each file for details.

| # | Criticality | File | Issue | Status |
|---|---|---|---|---|
| 1 | Low | [TODO_state_switch_loses_spare.md](TODO_state_switch_loses_spare.md) | Rare state-switch path (fire-and-forget/abandoned send race) drops a spare result vector, costing one extra allocation later. | Deferred minor |
| 2 | Low | [TODO_writer_error_drops_requests_vec.md](TODO_writer_error_drops_requests_vec.md) | Writer socket error mid-`Many` drops the request Vec's capacity and skips recycling. Explicitly harmless — the connection is already dead. | Deferred minor |
| 3 | Low (meta) | [TODO_batch_test_gaps.md](TODO_batch_test_gaps.md) | Not a bug — tracks missing deterministic tests for the state-switch race, a writer error mid-batch, the arena-shrink-after-split path, and reviving a deleted stress test. | Deferred minor |
| 4 | Lowest / not a bug | [TODO_replies_borrow_batch.md](TODO_replies_borrow_batch.md) | `Batch::send`'s `Result` borrows `&mut Batch` even on `Err`. Documented as "not a bug, follows the agreed API"; recommendation is to document it. | Ergonomics note |

**Suggested order:** fix the cheap perf issue #1, then close out #2-#4 as cleanup/documentation alongside whichever fixes land.
