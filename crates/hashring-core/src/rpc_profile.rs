//! Diagnostic RPC latency histograms, enabled with `HASHRING_PROFILE_RPC=1`.
//!
//! Each process that calls [`start_reporter`] prints every non-empty histogram
//! to stderr twice a second as one line:
//!
//! ```text
//! rpc_profile <unix_ms> <role> <name> <count> <sum_ns> <bucket>:<count>,...
//! ```
//!
//! The values are cumulative, so a phase is measured by subtracting the last
//! line before it from the last line inside it. Bucket `i` holds samples in
//! `[2^(i/4), 2^((i+1)/4))` microseconds; bucket 0 also holds everything below
//! 1 µs and the last bucket everything above its lower bound.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const BUCKETS: usize = 112;

pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("HASHRING_PROFILE_RPC").is_some_and(|v| v == "1"))
}

pub struct Histogram {
    name: &'static str,
    count: AtomicU64,
    sum_ns: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

impl Histogram {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            buckets: [const { AtomicU64::new(0) }; BUCKETS],
        }
    }

    pub fn record(&self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let micros = nanos as f64 / 1000.0;
        let bucket = if micros < 1.0 {
            0
        } else {
            ((micros.log2() * 4.0) as usize).min(BUCKETS - 1)
        };
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(nanos, Ordering::Relaxed);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    /// Records a plain count (such as entries per batch) in the microsecond scale.
    pub fn record_value(&self, value: u64) {
        if enabled() {
            self.record(Duration::from_micros(value));
        }
    }

    /// Returns a guard that records the time until it is dropped, or `None`
    /// when profiling is off.
    pub fn start(&'static self) -> Option<Timer> {
        enabled().then(|| Timer {
            histogram: self,
            started: Instant::now(),
        })
    }
}

pub struct Timer {
    histogram: &'static Histogram,
    started: Instant,
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.histogram.record(self.started.elapsed());
    }
}

/// Owner handler time for a client `Put`, from request decode to response.
pub static NODE_PUT: Histogram = Histogram::new("node_put_handler");
/// Owner handler time for a client `Get`.
pub static NODE_GET: Histogram = Histogram::new("node_get_handler");
/// Follower handler time for one `ReplicateMutations` batch.
pub static NODE_REPLICATE_BATCH: Histogram = Histogram::new("node_replicate_batch_handler");
/// Owner-observed round trip of one `ReplicateMutations` RPC.
pub static OWNER_REPLICATION_RPC: Histogram = Histogram::new("owner_replication_rpc");
/// Entries per replication batch, recorded as microseconds.
pub static REPLICATION_BATCH_ENTRIES: Histogram = Histogram::new("replication_batch_entries");
/// Client-observed time of one `Put` RPC attempt.
pub static CLIENT_PUT_RPC: Histogram = Histogram::new("client_put_rpc");
/// Client-observed time of one `Get` RPC attempt.
pub static CLIENT_GET_RPC: Histogram = Histogram::new("client_get_rpc");
/// One logical client `put`, including routing and retries.
pub static CLIENT_PUT_OPERATION: Histogram = Histogram::new("client_put_operation");

static ALL: [&Histogram; 8] = [
    &NODE_PUT,
    &NODE_GET,
    &NODE_REPLICATE_BATCH,
    &OWNER_REPLICATION_RPC,
    &REPLICATION_BATCH_ENTRIES,
    &CLIENT_PUT_RPC,
    &CLIENT_GET_RPC,
    &CLIENT_PUT_OPERATION,
];

/// Starts the stderr reporter thread when profiling is on.
pub fn start_reporter(role: &str) {
    if !enabled() {
        return;
    }
    let role = role.to_owned();
    std::thread::Builder::new()
        .name("rpc-profile".into())
        .spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(500));
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis());
                for histogram in ALL {
                    let count = histogram.count.load(Ordering::Relaxed);
                    if count == 0 {
                        continue;
                    }
                    let buckets: Vec<String> = histogram
                        .buckets
                        .iter()
                        .enumerate()
                        .filter_map(|(i, b)| {
                            let n = b.load(Ordering::Relaxed);
                            (n > 0).then(|| format!("{i}:{n}"))
                        })
                        .collect();
                    eprintln!(
                        "rpc_profile {now} {role} {} {count} {} {}",
                        histogram.name,
                        histogram.sum_ns.load(Ordering::Relaxed),
                        buckets.join(",")
                    );
                }
            }
        })
        .expect("spawning the rpc profile reporter");
}
