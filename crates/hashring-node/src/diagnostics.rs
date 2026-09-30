use super::*;
use std::ops::{Deref, DerefMut};

pub(super) fn enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    cfg!(test)
        || *ENABLED.get_or_init(|| {
            std::env::var_os("HASHRING_PROFILE_WRITES").is_some_and(|value| value == "1")
        })
}

#[derive(Default)]
pub(super) struct StateWriteTiming {
    acquisitions: AtomicU64,
    wait_nanos: AtomicU64,
    hold_nanos: AtomicU64,
    max_wait_nanos: AtomicU64,
    max_hold_nanos: AtomicU64,
}

impl StateWriteTiming {
    pub(super) fn snapshot(&self) -> proto::StateWriteTiming {
        proto::StateWriteTiming {
            acquisitions: self.acquisitions.load(AtomicOrdering::Relaxed),
            wait_nanos: self.wait_nanos.load(AtomicOrdering::Relaxed),
            hold_nanos: self.hold_nanos.load(AtomicOrdering::Relaxed),
            max_wait_nanos: self.max_wait_nanos.load(AtomicOrdering::Relaxed),
            max_hold_nanos: self.max_hold_nanos.load(AtomicOrdering::Relaxed),
        }
    }
}

pub(super) fn nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

pub(super) struct TimedWriteGuard<'a> {
    guard: tokio::sync::RwLockWriteGuard<'a, NodeState>,
    timing: &'a StateWriteTiming,
    acquired: Option<Instant>,
}

pub(super) async fn write<'a>(
    state: &'a RwLock<NodeState>,
    timing: &'a StateWriteTiming,
) -> TimedWriteGuard<'a> {
    let started = enabled().then(Instant::now);
    let guard = state.write().await;
    let acquired = started.map(|started| {
        let acquired = Instant::now();
        let wait = nanos(acquired.duration_since(started));
        timing.acquisitions.fetch_add(1, AtomicOrdering::Relaxed);
        timing.wait_nanos.fetch_add(wait, AtomicOrdering::Relaxed);
        timing
            .max_wait_nanos
            .fetch_max(wait, AtomicOrdering::Relaxed);
        acquired
    });
    TimedWriteGuard {
        guard,
        timing,
        acquired,
    }
}

impl Deref for TimedWriteGuard<'_> {
    type Target = NodeState;
    fn deref(&self) -> &NodeState {
        &self.guard
    }
}

impl DerefMut for TimedWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut NodeState {
        &mut self.guard
    }
}

impl Drop for TimedWriteGuard<'_> {
    fn drop(&mut self) {
        let Some(acquired) = self.acquired else {
            return;
        };
        let held = nanos(acquired.elapsed());
        self.timing
            .hold_nanos
            .fetch_add(held, AtomicOrdering::Relaxed);
        self.timing
            .max_hold_nanos
            .fetch_max(held, AtomicOrdering::Relaxed);
    }
}
