//! Per-key expiry.
//!
//! A record carries an absolute deadline that every replica of it enforces on
//! read, so an expired key is absent without a delete travelling through the
//! replication stream and a migration carrying the same value to a new owner.
//! What is left is memory: the sweep in this module is the only thing that
//! removes a record once its deadline has passed.

use super::*;

/// How often expired records are reclaimed. Reads already hide them, so the
/// period only sets how long a value that nobody asks for stays resident.
pub const EXPIRY_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Records removed per sweep pass. The sweep holds the write lock, so a pass
/// over a node holding millions of expired keys cannot stall writers; the
/// remainder is picked up by the next pass.
pub const EXPIRY_SWEEP_LIMIT: usize = 65_536;

/// The instant an absolute Unix millisecond deadline falls on, for a heap that
/// orders deadlines on the node's own monotonic clock. A deadline already in
/// the past becomes `Instant::now()`, which the next sweep pops.
pub(super) fn deadline_instant(expires_at_unix_millis: u64) -> Instant {
    let now_unix_millis = now_unix_millis();
    Instant::now()
        + std::time::Duration::from_millis(expires_at_unix_millis.saturating_sub(now_unix_millis))
}

/// Removes up to `limit` records whose deadline has passed and returns how
/// many it removed.
///
/// The heap can hold an entry for a key that was later written with a further
/// deadline, or removed outright, so every popped key is confirmed against the
/// record the map currently holds rather than trusted.
pub(super) fn sweep_expired_records(state: &mut NodeState, now: Instant, limit: usize) -> usize {
    let now_unix_millis = now_unix_millis();
    let mut removed = 0;
    while removed < limit
        && state
            .record_expirations
            .peek()
            .is_some_and(|Reverse((deadline, _))| *deadline <= now)
    {
        let Reverse((_, key)) = state
            .record_expirations
            .pop()
            .expect("expiration was checked above");
        if !state
            .records
            .get(key.as_ref())
            .is_some_and(|record| record.is_expired(now_unix_millis))
        {
            continue;
        }
        let token = state.token_of(&key);
        state.remove_record_at(token, key.as_ref());
        removed += 1;
    }
    removed
}

pub(super) fn start_expiry_sweep(state: Arc<RwLock<NodeState>>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(EXPIRY_SWEEP_INTERVAL);
        loop {
            interval.tick().await;
            let mut guard = state.write().await;
            sweep_expired_records(&mut guard, Instant::now(), EXPIRY_SWEEP_LIMIT);
        }
    });
}
