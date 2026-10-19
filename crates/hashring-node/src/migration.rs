use super::*;

pub(super) fn page_limit(requested: u64, max_value_bytes: usize) -> usize {
    // Never smaller than one of this node's own largest values, which is the
    // smallest page that can carry a record it holds; the ceiling is what the
    // transport is sized for.
    usize::try_from(requested)
        .unwrap_or(MAX_MIGRATION_PAGE_BYTES)
        .clamp(1, MAX_MIGRATION_PAGE_BYTES)
        .max(max_value_bytes)
}

pub(super) fn migration_record_size(record: &MigrationRecord) -> usize {
    record.key.len() + record.value.len() + record.mutation_id.len() + 128
}

pub(super) fn apply_record(
    records: &mut HashMap<Vec<u8>, Record>,
    record: MigrationRecord,
) -> Result<(), Status> {
    let version = record
        .version
        .ok_or_else(|| Status::invalid_argument("migration record omitted version"))?;
    if record.deleted && !record.value.is_empty() {
        return Err(Status::invalid_argument(
            "deleted migration record must omit its value",
        ));
    }
    apply_internal_record(
        records,
        record.key,
        Record {
            value: record.value.into(),
            version,
            deleted: record.deleted,
            expires_at_unix_millis: stored_deadline(record.expires_at_unix_millis),
        },
    );
    Ok(())
}

pub(super) fn apply_internal_record(
    records: &mut HashMap<Vec<u8>, Record>,
    key: Vec<u8>,
    record: Record,
) {
    if records
        .get(&key)
        .is_none_or(|current| compare_versions(&record.version, &current.version).is_gt())
    {
        records.insert(key, record);
    }
}

pub(super) fn compare_versions(left: &RecordVersion, right: &RecordVersion) -> Ordering {
    (
        left.topology_epoch,
        left.owner_sequence,
        &left.owner_node_id,
    )
        .cmp(&(
            right.topology_epoch,
            right.owner_sequence,
            &right.owner_node_id,
        ))
}

/// Sorts `(token, key, _)` entries into the order [`range_digest`] requires.
///
/// The key comparison runs only when two tokens are equal, which for distinct
/// keys means a 64-bit hash collision, so the sort still compares integers.
pub(super) fn sort_into_token_order<T>(entries: &mut [(u64, &[u8], T)]) {
    entries.sort_unstable_by(|(left_token, left_key, _), (right_token, right_key, _)| {
        left_token
            .cmp(right_token)
            .then_with(|| left_key.cmp(right_key))
    });
}

/// Digest of one range's records, taken in the order the caller supplies.
///
/// The caller must supply **token order, ties broken by key bytes**, and that
/// is part of this function's contract rather than an implementation detail:
/// `RangeDigestResponse` is compared between a source and a destination, so
/// both sides have to agree on the byte stream. Token order is the one ordering
/// both sides can produce without re-sorting keys, because a token depends only
/// on the key bytes and the committed seed, which the two nodes share. The tie
/// break is what the source's `TokenIndex` yields for keys whose tokens
/// collide, and [`sort_into_token_order`] reproduces it on the destination.
///
/// The previous version took a `HashMap`, sorted it by key bytes, and hashed
/// the result. Two costs are gone now. `source_range_digest` no longer builds a
/// `HashMap` of cloned keys and records just to iterate it, and neither side
/// sorts byte slices. In a 1,000,000-key migration that sort and clone were the
/// two largest items under `source_range_digest` and
/// `destination_range_digest` (21.3% and 8.2% of the migration phase in
/// results/flamegraph-correctness-20261015-005607).
///
/// The digest bytes differ from v2 because the order differs, so the domain
/// separator is bumped: a digest from a node running the old code must never be
/// compared against one from a node running this.
///
/// Records now carry an expiry, so the digest covers it as well: a source and a
/// destination that disagreed about a deadline would hand over keys that stop
/// being readable at different times. The digest bytes change again, hence the
/// new domain separator.
pub(super) fn range_digest<'a>(
    records: impl IntoIterator<Item = (&'a [u8], &'a Record)>,
    watermark: u64,
) -> RangeDigestResponse {
    let mut digest = blake3::Hasher::new();
    digest.update(b"hashring-rs:records:v4\0");
    let mut count = 0u64;
    for (key, record) in records {
        count += 1;
        digest.update(&(key.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update(&record.version.topology_epoch.to_be_bytes());
        digest.update(&record.version.owner_sequence.to_be_bytes());
        digest.update(&(record.version.owner_node_id.len() as u64).to_be_bytes());
        digest.update(record.version.owner_node_id.as_bytes());
        digest.update(&(record.value.len() as u64).to_be_bytes());
        digest.update(record.value.as_ref());
        digest.update(&[u8::from(record.deleted)]);
        match record.expires_at_unix_millis {
            Some(expires_at) => {
                digest.update(&[1]);
                let deadline = expires_at.to_be_bytes();
                digest.update(&deadline);
            }
            None => {
                digest.update(&[0]);
            }
        }
    }
    RangeDigestResponse {
        record_count: count,
        digest: digest.finalize().to_hex().to_string(),
        changelog_watermark: watermark,
    }
}
