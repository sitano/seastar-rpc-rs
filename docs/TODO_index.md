# TODO index

Sorted by criticality, highest risk first. See each file for details.

| # | Criticality | File | Issue | Status |
|---|---|---|---|---|
| 1 | Critical | [TODO_encode_panic_corrupts_arena.md](TODO_encode_panic_corrupts_arena.md) | A panicking `Encode`, if caught with `catch_unwind` and the batch reused, silently prepends garbage bytes to the next request's payload — no error is raised. | Needs decision |
| 2 | High | [TODO_panic_or_lost_reqs.md](TODO_panic_or_lost_reqs.md) | Leaking a `Batch::send` future permanently steals a queue slot and leaves `Batch` in a state where the next call panics or silently drops pending requests. | Deferred minor, fix proposed |
| 3 | Medium-High | [TODO_user_batch_memory_unbounded.md](TODO_user_batch_memory_unbounded.md) | A long-lived user-owned `Batch` never passes through pool limits, so one large outlier send permanently inflates its arena and vectors with no cap. | Needs decision |
| 4 | Medium | [TODO_arena_hint_never_decays.md](TODO_arena_hint_never_decays.md) | `arena_hint` only grows, never resets/decays on recycle, so small sends over-reserve after one big send. Bounded (≤16 MiB across the pool); no correctness impact. | Deferred minor |
| 5 | Low-Medium | [TODO_push_raw_skips_reservation.md](TODO_push_raw_skips_reservation.md) | A send starting with `push_raw` before any `push` skips the whole-send arena pre-reservation, causing extra allocations. Perf only. | Deferred minor |
| 6 | Low | [TODO_state_switch_loses_spare.md](TODO_state_switch_loses_spare.md) | Rare state-switch path (fire-and-forget/abandoned send race) drops a spare result vector, costing one extra allocation later. | Deferred minor |
| 7 | Low | [TODO_writer_error_drops_requests_vec.md](TODO_writer_error_drops_requests_vec.md) | Writer socket error mid-`Many` drops the request Vec's capacity and skips recycling. Explicitly harmless — the connection is already dead. | Deferred minor |
| 8 | Low (meta) | [TODO_batch_test_gaps.md](TODO_batch_test_gaps.md) | Not a bug — tracks missing deterministic tests for the state-switch race, a writer error mid-batch, the arena-shrink-after-split path, and reviving a deleted stress test. | Deferred minor |
| 9 | Lowest / not a bug | [TODO_replies_borrow_batch.md](TODO_replies_borrow_batch.md) | `Batch::send`'s `Result` borrows `&mut Batch` even on `Err`. Documented as "not a bug, follows the agreed API"; recommendation is to document it. | Ergonomics note |

**Suggested order:** fix #1 and #2 first (correctness/panic risk), decide #3 (resource-growth policy), batch the cheap perf fixes #4-#6, then close out #7-#9 as cleanup/documentation alongside whichever fixes land.
