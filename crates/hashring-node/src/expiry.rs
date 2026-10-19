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

/// Expiry entries examined per sweep pass, whether they are removed or
/// deferred. The sweep holds the write lock, so a pass over a node holding
/// millions of expired keys cannot stall writers; the remainder is picked up by
/// the next pass.
pub const EXPIRY_SWEEP_LIMIT: usize = 65_536;

/// Examines up to `limit` records whose deadline has passed at
/// `now_unix_millis` and returns how many it removed.
///
/// A record in a range this node is migrating out is deferred instead of
/// removed (see `SourceMigration::deferred_expirations`); it stays hidden from reads
/// either way.
pub(super) fn sweep_expired_records(
    state: &mut NodeState,
    now_unix_millis: u64,
    limit: usize,
) -> usize {
    let mut removed = 0;
    for _ in 0..limit {
        let Some((deadline, key)) = state
            .record_expirations
            .first()
            .filter(|(deadline, _)| *deadline <= now_unix_millis)
            .cloned()
        else {
            break;
        };
        state.record_expirations.remove(&(deadline, key.clone()));
        let token = state.token_of(&key);
        if let Some(source) = state
            .sources
            .values_mut()
            .find(|source| source.range.contains(token))
        {
            source.deferred_expirations.insert((deadline, key));
            continue;
        }
        let record = state.remove_record_at(token, &key);
        debug_assert!(
            record.is_some_and(|record| record.expires_at_unix_millis == Some(deadline)),
            "the expiry index named a record that does not carry its deadline",
        );
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
            sweep_expired_records(&mut guard, now_unix_millis(), EXPIRY_SWEEP_LIMIT);
        }
    });
}
