use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::*;

/// The previous implementation, kept here so the change is measured rather than
/// asserted: collect the map, sort it by key bytes, hash the result.
fn range_digest_sorted_by_key(
    records: &HashMap<Vec<u8>, Record>,
    watermark: u64,
) -> RangeDigestResponse {
    let mut ordered: Vec<_> = records.iter().collect();
    ordered.sort_by_key(|(key, _)| *key);
    let mut digest = blake3::Hasher::new();
    digest.update(b"hashring-rs:records:v3\0");
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

/// Records whose token order is not their key-byte order, which is the case
/// where the removed sort was real work rather than a no-op. `xxh3` of a counter
/// is not monotonic, so the two orders genuinely differ.
fn bench_records(count: usize, seed: u64) -> (HashMap<Vec<u8>, Record>, Vec<(Vec<u8>, Record)>) {
    let mut map = HashMap::with_capacity(count);
    let mut by_token = Vec::with_capacity(count);
    for index in 0..count {
        let key = index.to_be_bytes().to_vec();
        let value = Record {
            value: Arc::from(format!("value-{index}").as_bytes()),
            version: RecordVersion {
                topology_epoch: 1,
                owner_sequence: index as u64,
                owner_node_id: format!("node-{}", index % 4),
            },
            deleted: index % 17 == 0,
        };
        map.insert(key.clone(), value.clone());
        by_token.push((key, value));
    }
    by_token.sort_by_key(|(key, _)| seed ^ token_of(key, seed));
    (map, by_token)
}

fn token_of(key: &[u8], seed: u64) -> u64 {
    TopologySnapshot::new_with_config(
        1,
        seed,
        16,
        vec![Member {
            node_id: "node-1".into(),
            endpoint: "http://127.0.0.1:5001".into(),
        }],
        TopologyConfig::default(),
    )
    .map(|topology| topology.key_token(key))
    .expect("single-member topology builds")
}

fn time(rounds: u32, mut body: impl FnMut()) -> Duration {
    body();
    let started = Instant::now();
    for _ in 0..rounds {
        body();
    }
    started.elapsed() / rounds
}

/// The digest has to stop paying for a sort and a clone.
///
/// The end-to-end experiment cannot resolve this: four repetitions of one
/// unchanged binary spanned 79s to 109s in a single session, and
/// `source_range_digest` was 21.3% of the migration phase in
/// `results/flamegraph-correctness-20261015-005607`. So the change is measured
/// on the operation itself, where the removed work is not competing with noise.
#[test]
#[ignore = "micro-benchmark; run with --ignored --nocapture"]
fn range_digest_orders_by_token_without_sorting_or_cloning() {
    const RECORDS: usize = 20_000;
    const ROUNDS: u32 = 30;
    let seed = 0x5eed_u64;

    let (by_key, by_token) = bench_records(RECORDS, seed);

    // The two must agree, or this is a behaviour change rather than a refactor.
    let sorted = range_digest_sorted_by_key(&by_key, 7);
    let streamed = range_digest(
        by_token.iter().map(|(key, record)| (key.as_slice(), record)),
        7,
    );
    assert_eq!(sorted.record_count, streamed.record_count);
    assert_eq!(
        sorted.digest, streamed.digest,
        "token order and key-byte order must digest the same records identically"
    );
    assert_eq!(streamed.record_count as usize, RECORDS);

    let old = time(ROUNDS, || {
        std::hint::black_box(range_digest_sorted_by_key(&by_key, 7));
    });
    let new = time(ROUNDS, || {
        std::hint::black_box(range_digest(
            by_token.iter().map(|(key, record)| (key.as_slice(), record)),
            7,
        ));
    });

    println!("range_digest over {RECORDS} records, {ROUNDS} rounds:");
    println!("  sort by key bytes (old): {:.3} ms", old.as_secs_f64() * 1e3);
    println!("  token order       (new): {:.3} ms", new.as_secs_f64() * 1e3);
    println!(
        "  speedup: {:.2}x",
        old.as_secs_f64() / new.as_secs_f64().max(f64::MIN_POSITIVE)
    );
}
