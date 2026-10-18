# Batch stress test

TL;DR: not a bug. Rewrite the batch review's randomized stress test, which
was never committed, as an `#[ignore]` test so it stays runnable.

Status: deferred minor from the client batch review (branch `ivan/batching`).

The review ran a stress test against one `Batch`: random 0-3 ms reply
delays, duplicate replies, 1 ms deadlines, sends abandoned mid-flight,
`NO_WAIT` then push and `clear`, interleaved `call_many_with`, 3000 rounds.
It passed 5 of 5 runs, but its file (`tests/zz_batch_review.rs`) was never
committed. Writing it again as an `#[ignore]` test would keep it runnable.

The deterministic gaps the review listed are covered now:

- state switch while the queue holds the state:
  `batch_push_after_no_wait_switches_state` in `tests/fake_server/batch.rs`;
- writer error in the middle of a batch:
  `batch_writer_error_fails_unsent_items` in `tests/fake_server/batch.rs`;
- an outlier `call_many` arena not kept by the pool: the end of
  `tests/batch_allocs.rs`.
