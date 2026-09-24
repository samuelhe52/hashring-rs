use super::*;

pub(super) fn page_limit(requested: u64) -> usize {
    usize::try_from(requested)
        .unwrap_or(MAX_MIGRATION_PAGE_BYTES)
        .clamp(1, MAX_MIGRATION_PAGE_BYTES)
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

pub(super) fn range_digest(
    records: &HashMap<Vec<u8>, Record>,
    watermark: u64,
) -> RangeDigestResponse {
    let mut ordered: Vec<_> = records.iter().collect();
    ordered.sort_by_key(|(key, _)| *key);
    let mut digest = blake3::Hasher::new();
    digest.update(b"hashring-rs:records:v2\0");
    for (key, record) in &ordered {
        digest.update(&(key.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update(&record.version.topology_epoch.to_be_bytes());
        digest.update(&record.version.owner_sequence.to_be_bytes());
        digest.update(&(record.version.owner_node_id.len() as u64).to_be_bytes());
        digest.update(record.version.owner_node_id.as_bytes());
        digest.update(&(record.value.len() as u64).to_be_bytes());
        digest.update(record.value.as_ref());
        digest.update(&[u8::from(record.deleted)]);
    }
    RangeDigestResponse {
        record_count: ordered.len() as u64,
        digest: digest.finalize().to_hex().to_string(),
        changelog_watermark: watermark,
    }
}
