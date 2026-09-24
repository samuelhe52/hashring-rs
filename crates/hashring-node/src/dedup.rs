use super::*;

pub(super) fn mutation_fingerprint(key: &[u8], value: &[u8], deleted: bool) -> [u8; 32] {
    let mut digest = blake3::Hasher::new();
    digest.update(b"hashring-rs:mutation:v1\0");
    digest.update(&(key.len() as u64).to_be_bytes());
    digest.update(key);
    digest.update(&[u8::from(deleted)]);
    digest.update(&(value.len() as u64).to_be_bytes());
    digest.update(value);
    *digest.finalize().as_bytes()
}

pub(super) fn dedup_retained_bytes(mutation_id: &str, key_len: usize, owner_len: usize) -> usize {
    mutation_id.len().saturating_mul(2)
        + key_len
        + owner_len
        + std::mem::size_of::<DedupEntry>()
        + std::mem::size_of::<(Instant, String)>()
        + std::mem::size_of::<DeduplicationRecord>()
        + std::mem::size_of::<StagedDedup>()
        + 128 // hash-table buckets and allocator metadata
}

pub(super) fn required_ack_retained_bytes(followers: &[String]) -> usize {
    followers
        .iter()
        .map(|node_id| 2 * std::mem::size_of::<RequiredAck>() + node_id.len() + 64)
        .sum()
}

pub(super) fn dedup_record_size(record: &DeduplicationRecord) -> usize {
    record.mutation_id.len()
        + record.key.len()
        + record.fingerprint.len()
        + record
            .version
            .as_ref()
            .map_or(0, |version| version.owner_node_id.len())
        + 128
}

pub(super) fn stage_dedup(
    destination: &mut DestinationMigration,
    record: DeduplicationRecord,
    max_dedup_bytes: usize,
) -> Result<(), Status> {
    if record.remaining_window_millis == 0 {
        return Ok(());
    }
    if record.mutation_id.is_empty()
        || record.mutation_id.len() > MAX_MUTATION_ID_BYTES
        || record.fingerprint.len() != 32
        || record.version.is_none()
    {
        return Err(Status::invalid_argument("invalid deduplication record"));
    }
    let expires_at = Instant::now()
        + std::time::Duration::from_millis(record.remaining_window_millis.min(60_000));
    if let Some(existing) = destination.dedup.get_mut(&record.mutation_id) {
        let same = existing.record.key == record.key
            && existing.record.fingerprint == record.fingerprint
            && existing.record.version == record.version
            && existing.record.deleted == record.deleted;
        if !same {
            return Err(Status::failed_precondition(
                "conflicting mutation ID in destination migration",
            ));
        }
        existing.expires_at = existing.expires_at.max(expires_at);
        return Ok(());
    }
    let version = record.version.as_ref().expect("version was checked above");
    let cost = dedup_retained_bytes(
        &record.mutation_id,
        record.key.len(),
        version.owner_node_id.len(),
    );
    if destination.dedup_bytes.saturating_add(cost) > max_dedup_bytes {
        return Err(Status::resource_exhausted(
            "staged mutation retry window is full",
        ));
    }
    destination.dedup_bytes += cost;
    destination.dedup.insert(
        record.mutation_id.clone(),
        StagedDedup {
            record,
            expires_at,
            retained_bytes: cost,
        },
    );
    Ok(())
}

pub(super) fn purge_staged_dedup(destination: &mut DestinationMigration, now: Instant) {
    destination.dedup.retain(|_, entry| entry.expires_at > now);
    destination.dedup_bytes = destination
        .dedup
        .values()
        .map(|entry| entry.retained_bytes)
        .sum();
}

pub(super) fn remaining_window_millis(expires_at: Instant, now: Instant) -> u64 {
    let remaining = expires_at.saturating_duration_since(now);
    if remaining.is_zero() {
        return 0;
    }
    u64::try_from(remaining.as_nanos().div_ceil(1_000_000))
        .unwrap_or(u64::MAX)
        .min(60_000)
}

pub(super) fn purge_expired_dedup(state: &mut NodeState, now: Instant) {
    while state
        .dedup_expirations
        .peek()
        .is_some_and(|Reverse((expires_at, _))| *expires_at <= now)
    {
        let Reverse((_, mutation_id)) = state
            .dedup_expirations
            .pop()
            .expect("expiration was checked above");
        if state
            .dedup
            .get(&mutation_id)
            .is_some_and(|entry| entry.expires_at <= now)
            && let Some(entry) = state.dedup.remove(&mutation_id)
        {
            state.dedup_bytes -= entry.retained_bytes;
        }
    }
    prune_ack_progress(state);
}

pub(super) fn insert_dedup(
    state: &mut NodeState,
    mutation_id: String,
    key: Arc<[u8]>,
    fingerprint: [u8; 32],
    version: RecordVersion,
    deleted: bool,
    now: Instant,
) {
    let expires_at = now + IDEMPOTENCY_WINDOW;
    insert_dedup_until(
        state,
        mutation_id,
        key,
        fingerprint,
        version,
        deleted,
        expires_at,
    );
}

pub(super) fn insert_dedup_until(
    state: &mut NodeState,
    mutation_id: String,
    key: Arc<[u8]>,
    fingerprint: [u8; 32],
    version: RecordVersion,
    deleted: bool,
    expires_at: Instant,
) {
    let retained_bytes = dedup_retained_bytes(&mutation_id, key.len(), version.owner_node_id.len());
    state
        .dedup_expirations
        .push(Reverse((expires_at, mutation_id.clone())));
    state.dedup.insert(
        mutation_id,
        DedupEntry {
            key,
            fingerprint,
            version,
            deleted,
            retained_bytes,
            expires_at,
            required_acks: Vec::new(),
        },
    );
    state.dedup_bytes += retained_bytes;
}

pub(super) fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
