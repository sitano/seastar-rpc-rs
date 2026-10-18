use futures_util::task::noop_waker_ref;

use super::*;

fn cx() -> Context<'static> {
    Context::from_waker(noop_waker_ref())
}

fn armed(n: usize) -> Arc<BatchState> {
    let mut state = Arc::new(BatchState::default());
    Arc::get_mut(&mut state).unwrap().arm(n);
    state
}

#[test]
fn batch_completes_once_every_slot_is_done() {
    let shared = Shared::default();
    let batch = armed(3);
    shared.register_many(10, 3, &batch).unwrap();
    assert!(batch.poll_done(&mut cx()).is_pending());

    shared.complete(12, Ok(Bytes::from_static(b"c")));
    shared.complete(10, Ok(Bytes::from_static(b"a")));
    assert!(batch.poll_done(&mut cx()).is_pending());

    // A duplicate reply finds no slot and must not count twice.
    shared.complete(10, Ok(Bytes::from_static(b"dup")));
    assert!(batch.poll_done(&mut cx()).is_pending());

    shared.complete(11, Err(Error::Timeout));
    assert!(batch.poll_done(&mut cx()).is_ready());

    let results = batch.results.lock().unwrap();
    assert!(matches!(&results[0], Some(Ok(b)) if &b[..] == b"a"));
    assert!(matches!(results[1], Some(Err(Error::Timeout))));
    assert!(matches!(&results[2], Some(Ok(b)) if &b[..] == b"c"));
    drop(results);
    assert!(shared.state.lock().unwrap().pending.is_empty());
    // Slots released their references: the batch can reuse its state.
    assert_eq!(Arc::strong_count(&batch), 1);
}

#[test]
fn forget_many_fences_late_replies() {
    let shared = Shared::default();
    let batch = armed(2);

    shared.register_many(1, 2, &batch).unwrap();
    shared.complete(1, Ok(Bytes::from_static(b"x")));
    shared.forget_many(1, 2);
    shared.complete(2, Ok(Bytes::from_static(b"late")));

    let results = batch.results.lock().unwrap();
    assert!(results[0].is_some());
    assert!(results[1].is_none());
    drop(results);
    assert_eq!(Arc::strong_count(&batch), 1);
}

#[test]
fn close_completes_batch_and_rejects_new_ones() {
    let shared = Shared::default();
    let batch = armed(2);

    shared.register_many(1, 2, &batch).unwrap();
    shared.close(Error::Closed);

    assert!(batch.poll_done(&mut cx()).is_ready());
    assert!(
        batch
            .results
            .lock()
            .unwrap()
            .iter()
            .all(|r| matches!(r, Some(Err(Error::Closed))))
    );
    assert!(matches!(
        shared.register_many(3, 1, &armed(1)),
        Err(Error::Closed)
    ));
}

#[test]
fn pool_keeps_only_unique_states() {
    let shared = Shared::default();
    let state = shared.take_batch_state();
    let held = state.clone();

    shared.recycle(state);
    assert_eq!(shared.pool.lock().unwrap().len(), 0);
    shared.recycle(held);
    assert_eq!(shared.pool.lock().unwrap().len(), 1);

    let _reused = shared.take_batch_state();
    assert_eq!(shared.pool.lock().unwrap().len(), 0);

    for _ in 0..POOL_MAX + 1 {
        shared.recycle(Arc::default());
    }
    assert_eq!(shared.pool.lock().unwrap().len(), POOL_MAX);
}

#[test]
fn recycle_drops_oversized_arena() {
    let shared = Shared::default();
    let mut state = Arc::new(BatchState::default());

    Arc::get_mut(&mut state)
        .unwrap()
        .arena
        .reserve(2 * POOL_ARENA_MAX);

    shared.recycle(state);

    assert!(shared.take_batch_state().arena.capacity() <= POOL_ARENA_MAX);
}

#[test]
fn recycle_keeps_arena_hint_with_arena() {
    let shared = Shared::default();
    let mut state = Arc::new(BatchState::default());

    let s = Arc::get_mut(&mut state).unwrap();
    s.arena.reserve(8 * 1024);
    s.arena_hint = 8 * 1024;
    shared.recycle(state);

    assert_eq!(shared.take_batch_state().arena_hint, 8 * 1024);
}

#[test]
fn recycle_forgets_arena_hint_with_dropped_arena() {
    let shared = Shared::default();
    let mut state = Arc::new(BatchState::default());

    let s = Arc::get_mut(&mut state).unwrap();
    s.arena.reserve(2 * POOL_ARENA_MAX);
    s.arena_hint = POOL_ARENA_MAX;
    shared.recycle(state);

    assert_eq!(shared.take_batch_state().arena_hint, 0);
}

#[test]
fn shrink_arena_drops_larger_arena() {
    let mut state = BatchState::default();

    state.arena.reserve(64 * 1024);
    state.shrink_arena(64 * 1024);
    assert!(state.arena.capacity() >= 64 * 1024, "kept within the limit");

    state.shrink_arena(1024);
    assert_eq!(state.arena.capacity(), 0);
}

#[test]
fn recycle_drops_oversized_arena_after_split() {
    let shared = Shared::default();
    let mut state = Arc::new(BatchState::default());
    let arena = &mut Arc::get_mut(&mut state).unwrap().arena;

    // A large payload, already written: only the buffer behind it remains.
    arena.resize(2 * POOL_ARENA_MAX, 0);
    drop(arena.split().freeze());
    shared.recycle(state);

    let mut arena =
        std::mem::take(&mut Arc::get_mut(&mut shared.take_batch_state()).unwrap().arena);
    arena.reserve(1);

    assert!(arena.capacity() <= POOL_ARENA_MAX, "{}", arena.capacity());
}
