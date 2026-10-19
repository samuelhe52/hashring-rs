//! How long a range digest stalls writers to the rest of the node.
//!
//! A digest reads the node's state under its `RwLock`, and every `put` takes
//! that lock for writing, so whatever a digest does before releasing the lock
//! is added to the latency of writes to every other range. This times the real
//! `source_range_digest` and `destination_range_digest` handlers against a
//! concurrent writer and reports the writer's latency next to a run with no
//! digest at all. It uses only the RPC surface and test helpers, so it can be
//! dropped into an older tree to compare against the code it replaced.
//!
//! ```sh
//! cargo test --release -p hashring-node range_digest_writer_stall -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use super::*;

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e3
}

/// Half the token ring: the migrating range is `(0, HALF]`, and the writer's keys
/// sit above it, so the writer is never fenced by the paused range and only waits
/// on the node's state lock.
const HALF: u64 = 1 << 63;

fn bench_record(counter: usize, value_bytes: usize) -> Record {
    Record {
        value: vec![(counter % 251) as u8; value_bytes].into(),
        version: RecordVersion {
            topology_epoch: 1,
            owner_sequence: counter as u64,
            owner_node_id: "node-1".into(),
        },
        deleted: false,
        expires_at_unix_millis: None,
    }
}

/// `count` counter keys whose tokens fall on the chosen side of `HALF`.
fn keys_on_side(topology: &TopologySnapshot, count: usize, in_range: bool) -> Vec<Vec<u8>> {
    (0u64..)
        .map(|counter| counter.to_be_bytes().to_vec())
        .filter(|key| {
            let token = topology.key_token(key);
            (token != 0 && token <= HALF) == in_range
        })
        .take(count)
        .collect()
}

fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    sorted[((sorted.len() - 1) as f64 * fraction).round() as usize]
}

/// Writer latency, sorted ascending. `stall` is the mean of the `stalls`
/// slowest puts: each digest delays the one put waiting when it takes the lock,
/// so with one digest per round that is the per-digest stall.
fn summary(sorted: &[Duration], stalls: usize) -> String {
    let slowest = &sorted[sorted.len().saturating_sub(stalls)..];
    let stall = slowest.iter().sum::<Duration>() / slowest.len() as u32;
    format!(
        "puts {:>5}  p50 {:>7.3} ms  p99 {:>7.3} ms  stall {:>7.3} ms  max {:>7.3} ms",
        sorted.len(),
        milliseconds(percentile(sorted, 0.50)),
        milliseconds(percentile(sorted, 0.99)),
        milliseconds(stall),
        milliseconds(*sorted.last().unwrap()),
    )
}

/// Issues one put per millisecond to keys outside the migrating range until
/// `stop`, and returns
/// each put's latency. A put takes the node's state lock for writing, so its
/// latency is exactly what a digest holding the read lock costs other ranges.
fn spawn_writer(
    service: DataNodeService,
    keys: Vec<Vec<u8>>,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<Vec<Duration>> {
    tokio::spawn(async move {
        let mut latencies = Vec::new();
        let mut sequence = 0usize;
        while !stop.load(AtomicOrdering::Relaxed) {
            let key = keys[sequence % keys.len()].clone();
            let started = Instant::now();
            let response = service
                .put(Request::new(PutRequest {
                    key,
                    value: vec![7; 128],
                    topology_epoch: 1,
                    request_id: format!("writer-{sequence}"),
                    expires_at_unix_millis: None,
                    preserve_value: false,
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(response.error.is_none(), "writer put failed");
            latencies.push(started.elapsed());
            sequence += 1;
            // Paced so the receipts it leaves stay far inside the dedup budget;
            // a stall still lands on whichever put is waiting when it starts.
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        latencies
    })
}

/// Runs `digest` `rounds` times against a concurrent writer and prints the
/// digest's own time next to the writer's latency while it ran.
async fn measure_writer_stall<F, Fut>(
    label: &str,
    service: &DataNodeService,
    writer_keys: Vec<Vec<u8>>,
    rounds: u32,
    mut digest: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = RangeDigestResponse>,
{
    let stop = Arc::new(AtomicBool::new(false));
    let writer = spawn_writer(service.clone(), writer_keys, stop.clone());
    // Let the writer reach a steady state before the first digest.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let started = Instant::now();
    let mut count = 0;
    for _ in 0..rounds {
        count = std::hint::black_box(digest().await).record_count;
    }
    let digest_time = started.elapsed() / rounds;
    stop.store(true, AtomicOrdering::Relaxed);
    let mut latencies = writer.await.unwrap();
    latencies.sort();
    println!(
        "  {label:<12} {count:>6} records  digest {:>8.3} ms   {}",
        milliseconds(digest_time),
        summary(&latencies, rounds as usize),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "micro-benchmark; run with --ignored --nocapture"]
async fn range_digest_writer_stall() {
    const RECORDS: usize = 50_000;
    const ROUNDS: u32 = 20;

    for value_bytes in [128, 1024, 4096] {
        println!("value_bytes = {value_bytes}:");

        let source = service();
        let topology = source.state.read().await.topology.clone();
        let range_keys = keys_on_side(&topology, RECORDS, true);
        let writer_keys = keys_on_side(&topology, 64, false);
        {
            let mut state = source.state.write().await;
            for (counter, key) in range_keys.iter().enumerate() {
                seed_record(&mut state, key, bench_record(counter, value_bytes));
            }
        }
        source
            .prepare_source_range(Request::new(PrepareRangeRequest {
                range: Some(range(HALF)),
            }))
            .await
            .unwrap();
        let control = RangeControlRequest {
            change_id: "change-1".into(),
            range_id: "range-1".into(),
        };
        measure_writer_stall("source", &source, writer_keys.clone(), ROUNDS, || {
            let source = source.clone();
            let control = control.clone();
            async move {
                source
                    .source_range_digest(Request::new(control))
                    .await
                    .unwrap()
                    .into_inner()
            }
        })
        .await;

        // `prepare_destination_range` accepts only the range's destination, and
        // `put` only the owner, so the node id is switched between the two.
        let mut destination = service();
        destination.node_id = "node-2".into();
        destination
            .prepare_destination_range(Request::new(PrepareRangeRequest {
                range: Some(range(HALF)),
            }))
            .await
            .unwrap();
        destination.node_id = "node-1".into();
        {
            let mut state = destination.state.write().await;
            let staged = state
                .destinations
                .get_mut(&("change-1".to_string(), "range-1".to_string()))
                .unwrap();
            for (counter, key) in range_keys.iter().enumerate() {
                staged
                    .records
                    .insert(key.clone(), bench_record(counter, value_bytes));
            }
        }
        measure_writer_stall("destination", &destination, writer_keys, ROUNDS, || {
            let destination = destination.clone();
            let control = control.clone();
            async move {
                destination
                    .destination_range_digest(Request::new(control))
                    .await
                    .unwrap()
                    .into_inner()
            }
        })
        .await;

        // The same writer with no digest running, as the floor to compare against.
        let stop = Arc::new(AtomicBool::new(false));
        let writer = spawn_writer(service(), keys_on_side(&topology, 64, false), stop.clone());
        tokio::time::sleep(Duration::from_millis(1000)).await;
        stop.store(true, AtomicOrdering::Relaxed);
        let mut latencies = writer.await.unwrap();
        latencies.sort();
        println!(
            "  {:<12} {:>6}          {:>18}   {}",
            "no digest",
            "",
            "",
            summary(&latencies, ROUNDS as usize),
        );
    }
}
