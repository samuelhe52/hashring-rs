//! Timing for the migration range digest, old paths versus new ones.
//!
//! The end-to-end experiment cannot resolve this change. Four repetitions of one
//! unchanged binary spanned 79s to 109s in a single session, and the negative
//! control makes the reason plain: `initial_put` moved 75.7% between runs even
//! though `range_digest` never executes during the put phase. Against that, the
//! digest is at most ~3% of a run, so only a measurement of the operation itself
//! says anything.
//!
//! Four paths are timed over one set of records, because the change did not only
//! remove work:
//!
//! | | before | now |
//! |---|---|---|
//! | source | copy the range into a `HashMap`, sort key bytes, hash | copy handles off the token index, hash |
//! | destination | clone the map, sort key bytes, hash | copy the records, token every key, sort `u64`s, hash |
//!
//! The destination gained a pass of `xxh3`, so timing it separately is the only
//! way to see whether the bytes sorted as integers give back more than that pass
//! costs. The copies on both sides are what the handlers take under the state
//! lock before releasing it; `digest_stall_bench.rs` measures that lock hold.
//!
//! ```sh
//! cargo test --release -p hashring-node range_digest -- --ignored --nocapture
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::*;

/// What both sides did before: order the records by key byte and hash. Kept as
/// it was, except for the `v3` domain separator, so the comparison measures the
/// code it replaced rather than a paraphrase of it and the two orders can be
/// compared digest for digest.
fn sorted_by_key(records: &HashMap<Vec<u8>, Record>, watermark: u64) -> RangeDigestResponse {
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

/// The source path before: copy every key and record of the range into a fresh
/// map, then sort it, then hash.
fn source_before(index: &[Arc<[u8]>], records: &HashMap<Vec<u8>, Record>) -> RangeDigestResponse {
    let copied: HashMap<Vec<u8>, Record> = index
        .iter()
        .filter_map(|key| {
            records
                .get(key.as_ref())
                .map(|record| (key.to_vec(), record.clone()))
        })
        .collect();
    sorted_by_key(&copied, 0)
}

/// The source path now: `record_keys_in_range` already yields token order, so
/// the handler copies `Arc` handles off the index under the lock and hashes them
/// after releasing it.
fn source_now(index: &[Arc<[u8]>], records: &HashMap<Vec<u8>, Record>) -> RangeDigestResponse {
    let collected: Vec<_> = index
        .iter()
        .filter_map(|key| {
            let record = records.get(key.as_ref())?.clone();
            Some((key.clone(), record))
        })
        .collect();
    range_digest(
        collected.iter().map(|(key, record)| (key.as_ref(), record)),
        0,
    )
}

/// The destination path before: clone the map and sort the copy by key byte.
fn destination_before(records: &HashMap<Vec<u8>, Record>) -> RangeDigestResponse {
    sorted_by_key(&records.clone(), 0)
}

/// The destination path now: the records live in a plain map, so the handler
/// copies them under the lock, then outside it computes one `xxh3` per key and
/// sorts `u64`s.
fn destination_now(
    records: &HashMap<Vec<u8>, Record>,
    topology: &TopologySnapshot,
) -> RangeDigestResponse {
    let copied: Vec<_> = records
        .iter()
        .map(|(key, record)| (key.clone(), record.clone()))
        .collect();
    let mut by_token: Vec<_> = copied
        .iter()
        .map(|(key, record)| (topology.key_token(key), key.as_slice(), record))
        .collect();
    sort_into_token_order(&mut by_token);
    range_digest(by_token.iter().map(|(_, key, record)| (*key, *record)), 0)
}

/// Same shape as the test helper in `mod.rs`: one member, replication factor 1,
/// so the seed is the only thing that distinguishes tokens from key bytes.
fn bench_topology() -> TopologySnapshot {
    TopologySnapshot::new_with_config(
        1,
        0x5e3d_1234,
        8,
        vec![Member {
            node_id: "node-1".into(),
            endpoint: "http://127.0.0.1:5001".into(),
        }],
        TopologyConfig {
            desired_replication_factor: 1,
            write_availability_guard: WriteAvailabilityGuard {
                minimum_admitted_copies: 1,
                minimum_healthy_followers: 0,
                ..WriteAvailabilityGuard::default()
            },
            ..TopologyConfig::default()
        },
    )
    .unwrap()
}

/// The source's view of a range: its keys, in token order.
type TokenOrderedKeys = Vec<Arc<[u8]>>;

/// Counter keys, whose byte order is their numeric order, plus their token
/// order. `xxh3` is not monotonic, so the two are genuinely different
/// permutations — without that the sort being removed would not have been work.
fn bench_records(
    count: usize,
    topology: &TopologySnapshot,
) -> (TokenOrderedKeys, HashMap<Vec<u8>, Record>) {
    let mut records = HashMap::with_capacity(count);
    let mut index = Vec::with_capacity(count);
    for counter in 0..count {
        let key: Arc<[u8]> = counter.to_be_bytes().to_vec().into();
        let deleted = counter % 17 == 0;
        records.insert(
            key.to_vec(),
            Record {
                value: if deleted {
                    Vec::new().into()
                } else {
                    format!("value-{counter}").into_bytes().into()
                },
                version: RecordVersion {
                    topology_epoch: 1,
                    owner_sequence: counter as u64,
                    owner_node_id: format!("node-{}", counter % 4),
                },
                deleted,
                expires_at_unix_millis: None,
            },
        );
        index.push(key);
    }
    // `records_by_token` is a `BTreeMap`, so the source's view of a range is
    // already in token order. Leaving `index` in key order would have made
    // `source_now` hash the wrong stream and quietly measured the old path.
    index.sort_by_key(|key| topology.key_token(key));
    (index, records)
}

/// Generic over the closure's return so the `black_box` value stays live through
/// the call instead of being discarded before it is produced.
fn time<T>(rounds: u32, mut body: impl FnMut() -> T) -> Duration {
    body();
    let started = Instant::now();
    for _ in 0..rounds {
        drop(body());
    }
    started.elapsed() / rounds
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e3
}

#[test]
#[ignore = "micro-benchmark; run with --ignored --nocapture"]
fn range_digest_pays_for_the_order_it_is_given() {
    const RECORDS: usize = 20_000;
    const ROUNDS: u32 = 30;

    let topology = bench_topology();
    let (index, records) = bench_records(RECORDS, &topology);

    // The setup has to be non-trivial, or a timing result proves nothing about
    // the sort that was removed.
    let mut by_key = index.clone();
    by_key.sort();
    let mut by_token = index.clone();
    by_token.sort_by_key(|key| topology.key_token(key));
    assert_ne!(
        by_key, by_token,
        "test keys are already in token order, so no sort was ever needed"
    );

    let old_src = source_before(&index, &records);
    let old_dst = destination_before(&records);
    let new_src = source_now(&index, &records);
    let new_dst = destination_now(&records, &topology);

    for response in [&old_src, &old_dst, &new_src, &new_dst] {
        assert_eq!(response.record_count as usize, RECORDS);
    }
    // Both contracts, each over the order its side is supposed to use.
    assert_eq!(
        old_src.digest, old_dst.digest,
        "key-byte order agreed on both sides before"
    );
    assert_eq!(
        new_src.digest, new_dst.digest,
        "token order must agree on both sides now, or a range would fail verification"
    );
    assert_ne!(
        new_src.digest, old_src.digest,
        "the orders differ, so the inputs really did exercise the removed sort"
    );

    let old_source = time(ROUNDS, || {
        std::hint::black_box(source_before(&index, &records))
    });
    let new_source = time(ROUNDS, || {
        std::hint::black_box(source_now(&index, &records))
    });
    let old_dest = time(ROUNDS, || {
        std::hint::black_box(destination_before(&records))
    });
    let new_dest = time(ROUNDS, || {
        std::hint::black_box(destination_now(&records, &topology))
    });

    println!("range_digest over {RECORDS} records, {ROUNDS} rounds each:");
    println!(
        "  source      before {ms1:.3} ms   now {ms2:.3} ms   ({speed:.2}x)",
        ms1 = milliseconds(old_source),
        ms2 = milliseconds(new_source),
        speed = old_source.as_secs_f64() / new_source.as_secs_f64()
    );
    println!(
        "  destination before {ms1:.3} ms   now {ms2:.3} ms   ({speed:.2}x)",
        ms1 = milliseconds(old_dest),
        ms2 = milliseconds(new_dest),
        speed = old_dest.as_secs_f64() / new_dest.as_secs_f64()
    );
}
