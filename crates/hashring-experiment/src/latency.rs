//! The open-loop load driver of the `latency` experiment mode.
//!
//! The driver sends requests on a fixed schedule: request `n` of a stage is
//! due at `stage start + n / rate`. A request that cannot be sent because too
//! many requests are still in flight is dropped and counted, never queued, so
//! a cluster that cannot keep up shows up as a lower achieved rate instead of
//! a longer queue inside the driver. Every request's latency is measured from
//! the moment it was due, not from the moment it was sent, so a slow cluster
//! cannot hide its tail behind a delayed send.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use hashring_client::{ClientError, HashringClient};
use hashring_core::proto::ErrorCode;
use hdrhistogram::Histogram;
use rand::Rng;
use serde_json::json;

use crate::{EventLog, LatencySummary, deterministic_value};

/// The operations the load issues.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Get,
    Put,
    Delete,
}

impl Operation {
    const ALL: [Operation; 3] = [Operation::Get, Operation::Put, Operation::Delete];

    fn index(self) -> usize {
        match self {
            Operation::Get => 0,
            Operation::Put => 1,
            Operation::Delete => 2,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Operation::Get => "get",
            Operation::Put => "put",
            Operation::Delete => "delete",
        }
    }
}

/// The share of the requests that are reads, writes, and deletes.
#[derive(Clone, Copy, Debug)]
pub struct OperationMix {
    weights: [u64; 3],
    total: u64,
}

impl OperationMix {
    /// Parses a `GET:PUT:DELETE` ratio such as `90:9:1`.
    pub fn parse(text: &str) -> Result<Self> {
        let parts: Vec<&str> = text.split(':').map(str::trim).collect();
        ensure!(
            parts.len() == 3,
            "a mix is GET:PUT:DELETE, like '90:9:1', not '{text}'"
        );
        let mut weights = [0_u64; 3];
        for (index, part) in parts.iter().enumerate() {
            weights[index] = part
                .parse::<u64>()
                .with_context(|| format!("the mix weight '{part}' is not a number"))?;
        }
        let total: u64 = weights.iter().sum();
        ensure!(total > 0, "a mix must allow at least one operation");
        Ok(Self { weights, total })
    }

    /// The share of the requests that delete a key.
    pub fn delete_fraction(&self) -> f64 {
        self.weights[2] as f64 / self.total as f64
    }

    pub fn pick(&self, rng: &mut impl Rng) -> Operation {
        let mut draw = rng.gen_range(0..self.total);
        for (index, weight) in self.weights.iter().enumerate() {
            if draw < *weight {
                return Operation::ALL[index];
            }
            draw -= *weight;
        }
        Operation::ALL[0]
    }
}

/// How the load picks the key of a request.
#[derive(Clone, Debug)]
pub enum KeyDistribution {
    /// Every key is equally likely.
    Uniform,
    /// The keys follow a Zipf distribution, so a few keys take most of the
    /// traffic.
    Zipf {
        theta: f64,
        /// The cumulative share of the traffic up to and including each key.
        cumulative: Vec<f64>,
        total: f64,
    },
    /// A share of the keys takes most of the traffic.
    Hotspot { hot_keys: u64, traffic_ratio: f64 },
}

impl KeyDistribution {
    /// Parses `uniform`, `zipf:<theta>`, or `hotspot:<key share>:<traffic share>`.
    pub fn parse(spec: &str, key_space: u64) -> Result<Self> {
        let spec = spec.trim();
        if spec.eq_ignore_ascii_case("uniform") {
            return Ok(Self::Uniform);
        }
        if let Some(rest) = spec.strip_prefix("zipf:") {
            let theta: f64 = rest
                .trim()
                .parse()
                .with_context(|| format!("the Zipf exponent '{rest}' is not a number"))?;
            ensure!(
                theta > 0.0 && theta.is_finite(),
                "the Zipf exponent must be a positive number"
            );
            ensure!(
                key_space > 0,
                "a key space of zero keys has no distribution"
            );
            // The cumulative share of every key. The weights are
            // 1/(k+1)^theta, so the table is built once and sampled by a
            // binary search.
            let mut cumulative = Vec::with_capacity(key_space as usize);
            let mut total = 0.0_f64;
            for key in 0..key_space {
                total += 1.0 / ((key + 1) as f64).powf(theta);
                cumulative.push(total);
            }
            return Ok(Self::Zipf {
                theta,
                cumulative,
                total,
            });
        }
        if let Some(rest) = spec.strip_prefix("hotspot:") {
            let parts: Vec<&str> = rest.split(':').map(str::trim).collect();
            ensure!(
                parts.len() == 2,
                "a hotspot distribution is hotspot:<key share>:<traffic share>"
            );
            let key_share: f64 = parts[0]
                .parse()
                .with_context(|| format!("the hotspot key share '{}' is not a number", parts[0]))?;
            let traffic_share: f64 = parts[1].parse().with_context(|| {
                format!("the hotspot traffic share '{}' is not a number", parts[1])
            })?;
            ensure!(
                key_share > 0.0 && key_share < 1.0,
                "the hotspot key share must be between 0 and 1"
            );
            ensure!(
                traffic_share > 0.0 && traffic_share < 1.0,
                "the hotspot traffic share must be between 0 and 1"
            );
            let hot_keys = ((key_space as f64) * key_share).round() as u64;
            ensure!(
                hot_keys > 0 && hot_keys < key_space,
                "the hotspot key share leaves no keys on one side of the split"
            );
            return Ok(Self::Hotspot {
                hot_keys,
                traffic_ratio: traffic_share,
            });
        }
        bail!("unknown key distribution '{spec}'")
    }

    /// The distribution as it is written in a configuration file.
    pub fn name(&self) -> String {
        match self {
            Self::Uniform => "uniform".to_owned(),
            Self::Zipf { theta, .. } => format!("zipf:{theta}"),
            Self::Hotspot {
                hot_keys,
                traffic_ratio,
            } => format!("hotspot:{hot_keys}:{traffic_ratio}"),
        }
    }

    /// Picks one of the `key_space` keys.
    pub fn sample(&self, rng: &mut impl Rng, key_space: u64) -> u64 {
        match self {
            Self::Uniform => rng.gen_range(0..key_space),
            Self::Zipf {
                cumulative, total, ..
            } => {
                let draw = rng.gen_range(0.0..1.0) * total;
                let index = cumulative.partition_point(|share| *share <= draw) as u64;
                // The table is built for the whole key space, so a draw at its
                // very top can land on its last key.
                index.min(key_space.saturating_sub(1))
            }
            Self::Hotspot {
                hot_keys,
                traffic_ratio,
            } => {
                if rng.gen_range(0.0..1.0) < *traffic_ratio {
                    rng.gen_range(0..*hot_keys)
                } else {
                    *hot_keys + rng.gen_range(0..(key_space - hot_keys))
                }
            }
        }
    }
}

/// A latency histogram over microseconds.
///
/// The histogram keeps every sample, so the percentiles of a stage are the
/// percentiles of all its requests and not of a sample of them.
///
/// A sample longer than the histogram's range is recorded in the top bucket,
/// so it is counted but not measured: the histogram's `max` is then the upper
/// bound of that bucket and not the length of the slowest request. The
/// longest sample and the number of the samples that were too long are kept
/// beside it, so a reader can tell a truncated sample from one that was near
/// the bound.
pub struct LatencyHistogram {
    inner: Histogram<u64>,
    highest_micros: u64,
    /// The samples that were longer than the histogram's range.
    over_range: u64,
    /// The longest sample, whether or not it was in the range.
    max_observed: u64,
}

impl LatencyHistogram {
    pub fn new(highest_micros: u64) -> Result<Self> {
        let highest_micros = highest_micros.max(1);
        let inner = Histogram::new_with_bounds(1, highest_micros, 3)
            .context("creating a latency histogram")?;
        Ok(Self {
            inner,
            highest_micros,
            over_range: 0,
            max_observed: 0,
        })
    }

    pub fn record(&mut self, micros: u64) {
        self.max_observed = self.max_observed.max(micros);
        if micros > self.highest_micros {
            self.over_range += 1;
        }
        // A value inside the histogram's range is always recorded. A value
        // above it is recorded in the top bucket, and counted above, so it
        // is not lost.
        let _ = self.inner.record(micros.clamp(1, self.highest_micros));
    }

    pub fn merge(&mut self, other: &Self) {
        let _ = self.inner.add(&other.inner);
        self.over_range += other.over_range;
        self.max_observed = self.max_observed.max(other.max_observed);
    }

    pub fn reset(&mut self) {
        self.inner.reset();
        self.over_range = 0;
        self.max_observed = 0;
    }

    pub fn summary(&self) -> LatencySummary {
        if self.inner.is_empty() {
            return LatencySummary::default();
        }
        LatencySummary {
            p50: self.inner.value_at_quantile(0.50),
            p95: self.inner.value_at_quantile(0.95),
            p99: self.inner.value_at_quantile(0.99),
            p999: self.inner.value_at_quantile(0.999),
            max: self.inner.max(),
            max_observed: self.max_observed,
            over_range: self.over_range,
            samples: self.inner.len(),
        }
    }
}

/// What one second of the load produced.
struct WindowState {
    /// The samples of the window that is running.
    current: Vec<LatencyHistogram>,
    /// The samples of the whole stage.
    total: Vec<LatencyHistogram>,
    /// The operations of the window that is running.
    completed: [u64; 3],
    /// The operations of the whole stage.
    total_completed: [u64; 3],
    /// The errors of the window that is running.
    errors: BTreeMap<String, u64>,
    /// The errors of the whole stage.
    total_errors: BTreeMap<String, u64>,
}

impl WindowState {
    /// Moves the open window into the totals of the stage.
    ///
    /// The stage reports the totals, so a window whose counters are left
    /// behind reports those requests as never having happened. The sampler
    /// folds every second of a phase, and the last fold of a phase is this
    /// one: the requests that were still in flight when the sampler stopped
    /// finish before it runs, and belong to the stage all the same.
    fn fold(&mut self) {
        for index in 0..self.current.len() {
            self.total[index].merge(&self.current[index]);
            self.current[index].reset();
        }
        for operation in Operation::ALL {
            let index = operation.index();
            self.total_completed[index] += self.completed[index];
            self.completed[index] = 0;
        }
        for (reason, count) in std::mem::take(&mut self.errors) {
            *self.total_errors.entry(reason).or_insert(0) += count;
        }
    }
}

/// The counters of the running stage.
struct Metrics {
    state: Mutex<WindowState>,
    in_flight: AtomicUsize,
    issued: AtomicU64,
    late: AtomicU64,
    /// How far behind its own schedule the sender is, over the whole phase.
    ///
    /// This is the sender's lag, and it is not the lag of a window: it is
    /// what the stage's `sender_saturated` is decided by, and the sampler
    /// publishes it beside the lag of the window it reports.
    sender_lag: AtomicU64,
    /// Set when the stage is over, so a sender that is still running stops.
    stopped: AtomicBool,
}

impl Metrics {
    fn new(highest_micros: u64) -> Result<Self> {
        let mut current = Vec::with_capacity(3);
        let mut total = Vec::with_capacity(3);
        for _ in 0..3 {
            current.push(LatencyHistogram::new(highest_micros)?);
            total.push(LatencyHistogram::new(highest_micros)?);
        }
        Ok(Self {
            state: Mutex::new(WindowState {
                current,
                total,
                completed: [0; 3],
                total_completed: [0; 3],
                errors: BTreeMap::new(),
                total_errors: BTreeMap::new(),
            }),
            in_flight: AtomicUsize::new(0),
            issued: AtomicU64::new(0),
            late: AtomicU64::new(0),
            sender_lag: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        })
    }

    fn record_success(&self, operation: Operation, latency_micros: u64) {
        let mut state = self.state.lock().expect("the metrics lock is not poisoned");
        state.current[operation.index()].record(latency_micros);
        state.completed[operation.index()] += 1;
    }

    fn record_error(&self, operation: Operation, reason: &'static str, latency_micros: u64) {
        let mut state = self.state.lock().expect("the metrics lock is not poisoned");
        state.current[operation.index()].record(latency_micros);
        *state.errors.entry(reason.to_owned()).or_insert(0) += 1;
    }
}

/// One stage of the load: one target rate.
#[derive(Clone, Debug)]
pub struct StageSettings {
    pub name: String,
    pub target_rate: u64,
    pub warmup_seconds: u64,
    pub duration_seconds: u64,
}

/// Everything the load needs that does not change between stages.
#[derive(Clone, Debug)]
pub struct LoadPlan {
    pub key_space: u64,
    pub value_bytes: usize,
    pub mix: OperationMix,
    pub distribution: KeyDistribution,
    pub max_in_flight: usize,
    /// The keys from here on are the ones the load deletes. They are disjoint
    /// from the keys it reads and writes, so a delete never races another
    /// operation on the same key.
    pub delete_pool_start: u64,
    /// The keys the load has deleted, so the key space can be checked
    /// afterwards.
    pub deleted: Arc<Vec<AtomicBool>>,
    pub highest_latency_micros: u64,
    /// How long the stage waits for the requests that are still in flight.
    /// A request cannot take longer than the operation deadline, so this is
    /// the deadline plus a margin.
    pub drain_timeout: Duration,
}

impl LoadPlan {
    /// Whether the load deleted the key.
    pub fn is_deleted(&self, key: u64) -> bool {
        if key < self.delete_pool_start {
            return false;
        }
        self.deleted
            .get((key - self.delete_pool_start) as usize)
            .is_some_and(|deleted| deleted.load(Ordering::Acquire))
    }

    /// Records that the load deleted the key.
    pub fn mark_deleted(&self, key: u64) {
        if key < self.delete_pool_start {
            return;
        }
        if let Some(deleted) = self.deleted.get((key - self.delete_pool_start) as usize) {
            deleted.store(true, Ordering::Release);
        }
    }

    fn sample_key(&self, operation: Operation, rng: &mut impl Rng) -> u64 {
        match operation {
            Operation::Delete => {
                let pool = self.key_space - self.delete_pool_start;
                if pool == 0 {
                    return 0;
                }
                self.delete_pool_start + rng.gen_range(0..pool)
            }
            _ => self.distribution.sample(rng, self.delete_pool_start),
        }
    }

    fn value(&self, key: u64) -> Vec<u8> {
        deterministic_value(key, 0, self.value_bytes)
    }
}

/// What one stage measured.
#[derive(Clone, Debug)]
pub struct StageOutcome {
    pub name: String,
    pub target_rate: u64,
    pub achieved_rate: f64,
    pub seconds: f64,
    pub sender_saturated: bool,
    pub issued: u64,
    pub late: u64,
    pub ops: BTreeMap<String, u64>,
    pub errors: BTreeMap<String, u64>,
    /// The internal retries the client made while the stage ran.
    pub retries: u64,
    pub latency_us: BTreeMap<String, LatencySummary>,
    /// The windows in which the driver did not keep up with its own schedule.
    pub saturated_windows: u64,
}

/// The load of one stage, warmup first and then the measured window.
pub struct LoadRunner {
    client: HashringClient,
    plan: Arc<LoadPlan>,
    metrics: Arc<Metrics>,
}

impl LoadRunner {
    pub fn new(client: HashringClient, plan: LoadPlan) -> Result<Self> {
        let metrics = Arc::new(Metrics::new(plan.highest_latency_micros)?);
        Ok(Self {
            client,
            plan: Arc::new(plan),
            metrics,
        })
    }

    /// Runs the warmup of a stage. Its samples are thrown away, so only the
    /// window that follows describes the stage.
    pub async fn run_warmup(&self, stage: &StageSettings, events: &EventLog) -> Result<()> {
        if stage.warmup_seconds == 0 {
            return Ok(());
        }
        self.run_phase(stage, events, stage.warmup_seconds, true)
            .await?;
        Ok(())
    }

    /// Runs the window of a stage that its statistics come from.
    pub async fn run_measured(
        &self,
        stage: &StageSettings,
        events: &EventLog,
    ) -> Result<StageOutcome> {
        self.run_phase(stage, events, stage.duration_seconds, false)
            .await
    }

    /// Forgets everything the previous phase counted, so a phase reports only
    /// what it ran itself.
    ///
    /// The totals accumulate over the samples the phase recorded, and a phase
    /// that started with the totals of the phase before it would report both.
    /// The warmup's samples are discarded by the same call: the measured
    /// phase begins by forgetting them.
    fn forget(&self) {
        let mut state = self
            .metrics
            .state
            .lock()
            .expect("the metrics lock is not poisoned");
        for histogram in state.current.iter_mut() {
            histogram.reset();
        }
        for histogram in state.total.iter_mut() {
            histogram.reset();
        }
        state.completed = [0; 3];
        state.total_completed = [0; 3];
        state.errors.clear();
        state.total_errors.clear();
        self.metrics.issued.store(0, Ordering::Relaxed);
        self.metrics.late.store(0, Ordering::Relaxed);
    }

    async fn run_phase(
        &self,
        stage: &StageSettings,
        events: &EventLog,
        seconds: u64,
        warmup: bool,
    ) -> Result<StageOutcome> {
        let start = Instant::now();
        let end = start + Duration::from_secs(seconds);
        // The sender of the previous phase has stopped; this one starts fresh.
        self.metrics.stopped.store(false, Ordering::Relaxed);
        // Every phase counts only its own requests. The measured phase picks
        // up the warmup's counters here, and the warmup of the next stage
        // picks up the measured phase's.
        self.forget();
        // The client's retries are cumulative, so the phase starts from the
        // count it has now.
        let retries_before = self.client.retry_count();
        let sampler = tokio::spawn(sample_windows(
            Arc::clone(&self.metrics),
            events.clone(),
            self.client.clone(),
            stage.clone(),
            warmup,
            start,
            end,
            retries_before,
        ));
        let (issued, late, saturated_windows) = self.drive(stage, start, end).await?;
        // A measured stage that sent nothing is not a measurement of the
        // cluster. The accounting below would accept it, because no request
        // was issued and none came back, so a stage that offered no load
        // would be published as a stage that offered nothing. A warmup is
        // not measured, and a warmup that sent nothing does not fail the
        // run.
        if !warmup {
            ensure!(
                issued > 0,
                "the stage {} issued no requests at all",
                stage.name
            );
        }
        // The sampler writes the window that ends with the phase, so it is
        // waited for rather than stopped: a window that is dropped would leave
        // its samples out of the stage.
        let _ = sampler.await;
        // The requests that were still in flight when the window closed
        // belong to the stage, even though no window reports them.
        self.merge_pending();
        // The totals are read here and not in the sender, because the last
        // window is folded in only now. A stage that read them earlier
        // would miss it, and its rate would be short by a second's worth of
        // requests.
        let (ops, errors, latency_us) = self.totals();
        let completed: u64 = ops.values().sum();
        // Every request the sender sent came back, and every request that
        // came back was recorded. A stage that does not add up is reported as
        // a failure instead of a rate that is quietly short.
        let failed: u64 = errors.values().sum();
        if completed + failed != issued {
            bail!(
                "the stage issued {} requests, but {} of them came back",
                issued,
                completed + failed
            );
        }
        let seconds = end.duration_since(start).as_secs_f64();
        Ok(StageOutcome {
            name: stage.name.clone(),
            target_rate: stage.target_rate,
            achieved_rate: completed as f64 / seconds.max(f64::EPSILON),
            seconds,
            sender_saturated: saturated_windows > 0,
            issued,
            late,
            ops,
            // Every reason, the ones that did not occur at zero.
            errors: complete_errors(&errors),
            retries: self.client.retry_count().saturating_sub(retries_before),
            latency_us,
            saturated_windows,
        })
    }

    /// The open loop: every request is due at a fixed offset from the start
    /// of the phase, and is sent then or not at all.
    ///
    /// Returns how many requests it sent, how many it had to drop, and how
    /// many windows it did not keep up with. The samples are read by the
    /// caller, after the sampler has folded its last window in.
    async fn drive(
        &self,
        stage: &StageSettings,
        start: Instant,
        end: Instant,
    ) -> Result<(u64, u64, u64)> {
        let handle = tokio::runtime::Handle::current();
        let client = self.client.clone();
        let plan = Arc::clone(&self.plan);
        let metrics = Arc::clone(&self.metrics);
        let settings = stage.clone();
        let (sender, sent) = tokio::sync::oneshot::channel();
        let mut thread = Some(
            std::thread::Builder::new()
                .name("latency-sender".to_owned())
                .spawn(move || {
                    let result = send_requests(handle, client, plan, metrics, settings, start, end);
                    let _ = sender.send(result);
                })?,
        );
        let reported = sent.await;
        let (issued, late, saturated_windows) = match reported {
            Ok(counts) => counts,
            Err(error) => {
                // The sender thread ended without reporting how many requests
                // it sent, because it panicked. A stage that reported zero
                // would look like a stage that offered no load, and the
                // accounting below would let it pass, so the run fails here
                // instead of publishing a rate of nothing.
                let message = thread
                    .take()
                    .and_then(|thread| thread.join().err())
                    .map(panic_message)
                    .unwrap_or_else(|| {
                        format!("the sender thread ended without reporting its counts: {error}")
                    });
                bail!("{message}");
            }
        };
        // Let the requests that are still in flight finish, so the stage
        // reports every request it sent. A request cannot take longer than
        // the operation deadline, so the wait is bounded.
        let deadline = Instant::now() + self.plan.drain_timeout;
        while self.metrics.in_flight.load(Ordering::Acquire) > 0 {
            if Instant::now() >= deadline {
                bail!(
                    "{} requests were still in flight {:?} after the stage ended",
                    self.metrics.in_flight.load(Ordering::Relaxed),
                    self.plan.drain_timeout
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.metrics.stopped.store(true, Ordering::Relaxed);
        // The thread has sent its counts, so it is about to end. A panic
        // after that would still be a panic, and a stage that does not
        // describe the load it offered is not a measurement.
        if let Some(thread) = thread.take()
            && let Some(message) = thread.join().err().map(panic_message)
        {
            bail!("the sender thread panicked: {message}");
        }
        Ok((issued, late, saturated_windows))
    }

    /// Moves the samples the stage still holds in its open window into its
    /// totals. The sampler has stopped, so this is the phase's last fold.
    fn merge_pending(&self) {
        let mut state = self
            .metrics
            .state
            .lock()
            .expect("the metrics lock is not poisoned");
        state.fold();
    }

    /// The operations, errors, and latencies the stage recorded.
    fn totals(
        &self,
    ) -> (
        BTreeMap<String, u64>,
        BTreeMap<String, u64>,
        BTreeMap<String, LatencySummary>,
    ) {
        let state = self
            .metrics
            .state
            .lock()
            .expect("the metrics lock is not poisoned");
        let mut ops = BTreeMap::new();
        let mut latency_us = BTreeMap::new();
        for operation in Operation::ALL {
            let index = operation.index();
            ops.insert(operation.name().to_owned(), state.total_completed[index]);
            latency_us.insert(operation.name().to_owned(), state.total[index].summary());
        }
        (ops, state.total_errors.clone(), latency_us)
    }
}

/// The margin the sender leaves between the end of its sleep and the moment
/// a request was due. The sleep of the kernel overshoots by about this much,
/// so the spin that finishes the wait is short.
const SEND_MARGIN: Duration = Duration::from_micros(20);

/// The message of a panic, if the panic carries one.
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "the thread panicked".to_owned()
    }
}

/// Waits until `due` as closely as the host's timer allows.
///
/// The kernel's timer is coarse, so the wait has two parts: a sleep that ends
/// a little before the deadline, and a spin for the rest. The margin is the
/// overshoot the last sleep showed, so the spin stays short and the request
/// goes out on time.
///
/// A runtime timer is not used here. It wakes up about a millisecond late,
/// which is longer than the latencies this mode measures, so every request
/// would carry a millisecond of the sender's own delay.
fn wait_until(due: Instant, margin: &mut Duration) {
    let coarse = due.checked_sub(*margin).unwrap_or(due);
    let now = Instant::now();
    if coarse > now {
        std::thread::sleep(coarse - now);
        let overshoot = Instant::now().saturating_duration_since(coarse);
        *margin = (overshoot + SEND_MARGIN).clamp(SEND_MARGIN, Duration::from_millis(5));
    }
    while Instant::now() < due {
        std::hint::spin_loop();
    }
}

/// Sends the requests of a phase, and returns how many it sent, how many it
/// had to drop, and how many windows it did not keep up with.
fn send_requests(
    handle: tokio::runtime::Handle,
    client: HashringClient,
    plan: Arc<LoadPlan>,
    metrics: Arc<Metrics>,
    stage: StageSettings,
    start: Instant,
    end: Instant,
) -> (u64, u64, u64) {
    let rate = stage.target_rate as f64;
    let mut margin = Duration::from_micros(100);
    let mut issued = 0_u64;
    let mut late = 0_u64;
    let mut saturated_windows = 0_u64;
    let mut index = 0_u64;
    let mut window_end = start + Duration::from_secs(1);
    while index < u64::MAX {
        let due = start + Duration::from_secs_f64(index as f64 / rate);
        if due >= end {
            break;
        }
        if metrics.stopped.load(Ordering::Relaxed) {
            break;
        }
        wait_until(due, &mut margin);
        // The limit is enforced by the one sender, so the check and the
        // reservation cannot race with each other.
        if metrics.in_flight.load(Ordering::Acquire) >= plan.max_in_flight {
            late += 1;
            metrics.late.fetch_add(1, Ordering::Relaxed);
        } else {
            metrics.in_flight.fetch_add(1, Ordering::AcqRel);
            issued += 1;
            metrics.issued.fetch_add(1, Ordering::Relaxed);
            let request = Request {
                client: client.clone(),
                plan: Arc::clone(&plan),
                metrics: Arc::clone(&metrics),
                due,
            };
            handle.spawn(async move { request.run().await });
        }
        index += 1;
        let now = Instant::now();
        if now >= window_end {
            // The schedule says how many requests were due by now. A sender
            // that is behind it did not keep up, and the stage's numbers are
            // not usable. The lag is the sender's own, over the whole phase:
            // it is not the lag of the window the sampler reports, which is
            // a different number with a different meaning.
            let expected = ((now.duration_since(start).as_secs_f64()) * rate).floor() as u64 + 1;
            let sender_lag = expected.saturating_sub(issued + late);
            metrics.sender_lag.store(sender_lag, Ordering::Relaxed);
            if sender_lag > 2 + stage.target_rate / 20 {
                saturated_windows += 1;
            }
            window_end += Duration::from_secs(1);
        }
    }
    (issued, late, saturated_windows)
}

/// One request of the load.
struct Request {
    client: HashringClient,
    plan: Arc<LoadPlan>,
    metrics: Arc<Metrics>,
    /// The moment the request was due, which is where its latency starts.
    due: Instant,
}

impl Request {
    async fn run(self) {
        let (operation, key) = {
            let mut rng = rand::thread_rng();
            let operation = self.plan.mix.pick(&mut rng);
            (operation, self.plan.sample_key(operation, &mut rng))
        };
        let encoded = key.to_be_bytes().to_vec();
        let outcome = match operation {
            Operation::Get => self.client.get(encoded).await.map(|_| ()),
            Operation::Put => {
                let value = self.plan.value(key);
                self.client.put(encoded, value).await.map(|_| ())
            }
            Operation::Delete => self.client.delete(encoded).await.map(|_| ()),
        };
        let latency = Instant::now().saturating_duration_since(self.due);
        let latency_micros = latency.as_micros().min(u64::MAX as u128) as u64;
        match classify(operation, outcome) {
            Ok(()) => {
                if operation == Operation::Delete {
                    self.plan.mark_deleted(key);
                }
                self.metrics.record_success(operation, latency_micros)
            }
            Err(reason) => self.metrics.record_error(operation, reason, latency_micros),
        }
        self.metrics.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Turns a failed operation into the reason it failed.
///
/// A delete of a key that is already gone is not a failure: the key is
/// absent, which is what the delete asked for.
fn classify(operation: Operation, outcome: Result<(), ClientError>) -> Result<(), &'static str> {
    match outcome {
        Ok(()) => Ok(()),
        Err(ClientError::MissingVersion) if operation == Operation::Delete => Ok(()),
        Err(ClientError::DeadlineExceeded { .. }) => Err("deadline"),
        Err(ClientError::Operation(failure)) => {
            if failure.code == ErrorCode::NotFound && operation == Operation::Get {
                Err("not_found")
            } else {
                Err("operation")
            }
        }
        Err(ClientError::Rpc { .. }) => Err("rpc"),
        Err(ClientError::Topology(_) | ClientError::TopologyRefresh { .. }) => Err("topology"),
        Err(ClientError::MissingVersion) => Err("protocol"),
    }
}

/// Every reason an operation can be reported under.
///
/// A run reports all of them in its `errors` maps, with a zero for the ones
/// that did not occur, so a reader compares the same fields in every window
/// of every run instead of guessing which ones a map is missing.
pub const ERROR_REASONS: [&str; 6] = [
    "deadline",
    "not_found",
    "operation",
    "protocol",
    "rpc",
    "topology",
];

/// The errors with every reason in them, the ones that did not occur at zero.
fn complete_errors(errors: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
    let mut complete: BTreeMap<String, u64> = ERROR_REASONS
        .iter()
        .map(|reason| ((*reason).to_owned(), 0))
        .collect();
    for (reason, count) in errors {
        *complete.entry(reason.clone()).or_insert(0) += count;
    }
    complete
}

/// Writes one `metrics_sampled` event per second of the phase.
///
/// The arguments are the phase's own inputs, one per line, because the
/// sampler has to name all of them to write a window: grouping them into a
/// struct would hide which of them a window is built from.
#[allow(clippy::too_many_arguments)]
async fn sample_windows(
    metrics: Arc<Metrics>,
    events: EventLog,
    client: HashringClient,
    stage: StageSettings,
    warmup: bool,
    start: Instant,
    end: Instant,
    retries_before: u64,
) {
    let mut window_start = start;
    let mut retries_seen = retries_before;
    while window_start < end {
        let window_end = window_start + Duration::from_secs(1);
        tokio::time::sleep_until(window_end.into()).await;
        let (issued, late, completed, errors, latency_us) = {
            let mut state = metrics
                .state
                .lock()
                .expect("the metrics lock is not poisoned");
            let mut latency_us = serde_json::Map::new();
            for operation in Operation::ALL {
                latency_us.insert(
                    operation.name().to_owned(),
                    serde_json::to_value(state.current[operation.index()].summary())
                        .expect("a latency summary is serializable"),
                );
            }
            // What the window produced is read before the fold takes it.
            let completed = state.completed;
            let errors = state.errors.clone();
            state.fold();
            let issued = metrics.issued.swap(0, Ordering::AcqRel);
            let late = metrics.late.swap(0, Ordering::AcqRel);
            (issued, late, completed, errors, latency_us)
        };
        let mut ops = serde_json::Map::new();
        for operation in Operation::ALL {
            ops.insert(
                operation.name().to_owned(),
                serde_json::Value::from(completed[operation.index()]),
            );
        }
        // The window's share of the schedule. A sender that is behind it did
        // not keep up, and the window's numbers do not describe the cluster.
        // A request the in-flight limit dropped is not behind: the sender
        // dealt with it, and the cluster is the one that could not.
        let length = window_end.min(end).saturating_duration_since(window_start);
        let expected = (length.as_secs_f64() * stage.target_rate as f64).floor() as u64;
        let behind = expected.saturating_sub(issued + late);
        // The sender's own lag, over the phase so far. It is not the lag of
        // this window: it is what the stage's `sender_saturated` is decided
        // by, and it is published here so a reader can see why.
        let sender_lag = metrics.sender_lag.load(Ordering::Relaxed);
        // The retries the client made in this window, and the epoch it routed
        // with. A retry is a request the client had to try again, so a window
        // with retries is a window the cluster was not answering at once.
        let retries_now = client.retry_count();
        let retries = retries_now.saturating_sub(retries_seen);
        retries_seen = retries_now;
        let epoch = client.epoch().await;
        let fields = json!({
            "stage": stage.name,
            "warmup": warmup,
            "window_ms": 1000,
            "target_rate": stage.target_rate,
            "issued": issued,
            "late": late,
            "behind": behind,
            "sender_lag": sender_lag,
            "ops": ops,
            // Every reason, the ones that did not occur at zero, so a reader
            // finds the same keys in every window of every run.
            "errors": complete_errors(&errors),
            "retries": retries,
            "epoch": epoch,
            "latency_us": latency_us,
        });
        if events.record("metrics_sampled", fields).is_err() {
            return;
        }
        window_start = window_end;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn a_mix_picks_the_operations_it_names() {
        let mix = OperationMix::parse("90:9:1").unwrap();
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let mut counts = [0_u64; 3];
        for _ in 0..10_000 {
            counts[mix.pick(&mut rng).index()] += 1;
        }
        assert!(counts[0] > counts[1] && counts[1] > counts[2]);
        assert!(counts[2] > 50 && counts[2] < 200);
    }

    #[test]
    fn a_mix_rejects_a_ratio_it_cannot_use() {
        assert!(OperationMix::parse("90:9").is_err());
        assert!(OperationMix::parse("0:0:0").is_err());
        assert!(OperationMix::parse("a:b:c").is_err());
    }

    #[test]
    fn a_uniform_distribution_covers_the_whole_key_space() {
        let distribution = KeyDistribution::parse("uniform", 1000).unwrap();
        let mut rng = rand::rngs::StdRng::seed_from_u64(3);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..5_000 {
            seen.insert(distribution.sample(&mut rng, 1000));
        }
        // Five draws per key leave a few of the thousand untouched.
        assert!(
            seen.len() > 990,
            "only {} of 1000 keys were drawn",
            seen.len()
        );
    }

    #[test]
    fn a_zipf_distribution_favours_the_first_keys() {
        let distribution = KeyDistribution::parse("zipf:0.99", 10_000).unwrap();
        let mut rng = rand::rngs::StdRng::seed_from_u64(11);
        let mut first = 0_u64;
        for _ in 0..20_000 {
            if distribution.sample(&mut rng, 10_000) < 100 {
                first += 1;
            }
        }
        // The hottest hundred keys of a Zipf 0.99 distribution take a tenth of
        // the traffic, far more than the one percent a uniform draw gives.
        assert!(
            first > 1_000,
            "only {first} of 20000 draws hit the hot keys"
        );
    }

    #[test]
    fn a_hotspot_distribution_sends_its_traffic_to_a_few_keys() {
        let distribution = KeyDistribution::parse("hotspot:0.01:0.5", 10_000).unwrap();
        let mut rng = rand::rngs::StdRng::seed_from_u64(5);
        let mut hot = 0_u64;
        for _ in 0..10_000 {
            if distribution.sample(&mut rng, 10_000) < 100 {
                hot += 1;
            }
        }
        assert!(hot > 4_000 && hot < 6_000, "{hot} of 10000 draws were hot");
    }

    #[test]
    fn a_distribution_stays_inside_the_range_it_is_sampled_for() {
        for spec in ["uniform", "zipf:0.99", "hotspot:0.01:0.5"] {
            let distribution = KeyDistribution::parse(spec, 1000).unwrap();
            let mut rng = rand::rngs::StdRng::seed_from_u64(17);
            for _ in 0..20_000 {
                assert!(distribution.sample(&mut rng, 900) < 900);
            }
        }
    }

    #[test]
    fn a_delete_never_touches_a_key_the_load_reads() {
        let plan = LoadPlan {
            key_space: 1000,
            value_bytes: 8,
            mix: OperationMix::parse("90:9:1").unwrap(),
            distribution: KeyDistribution::parse("zipf:0.99", 900).unwrap(),
            max_in_flight: 16,
            delete_pool_start: 900,
            deleted: Arc::new((0..100).map(|_| AtomicBool::new(false)).collect()),
            highest_latency_micros: 1_000_000,
            drain_timeout: Duration::from_secs(1),
        };
        let mut rng = rand::rngs::StdRng::seed_from_u64(23);
        for _ in 0..20_000 {
            assert!(plan.sample_key(Operation::Get, &mut rng) < 900);
            assert!(plan.sample_key(Operation::Put, &mut rng) < 900);
            assert!(plan.sample_key(Operation::Delete, &mut rng) >= 900);
        }
    }

    #[test]
    fn a_histogram_keeps_every_sample_and_finds_its_percentiles() {
        let mut histogram = LatencyHistogram::new(60_000_000).unwrap();
        for value in 1..=10_000_u64 {
            histogram.record(value);
        }
        let summary = histogram.summary();
        assert_eq!(summary.samples, 10_000);
        // A histogram reports the upper bound of the bucket a value falls in,
        // so its maximum is a little above the largest value recorded.
        assert!(summary.max >= 10_000 && summary.max <= 10_010);
        assert!(summary.p50 >= 4_900 && summary.p50 <= 5_100);
        assert!(summary.p99 >= 9_800 && summary.p99 <= 10_010);
    }

    #[test]
    fn a_histogram_merges_the_windows_of_a_stage() {
        let mut first = LatencyHistogram::new(60_000_000).unwrap();
        let mut second = LatencyHistogram::new(60_000_000).unwrap();
        for value in 1..=100_u64 {
            first.record(value);
            second.record(value);
        }
        first.merge(&second);
        assert_eq!(first.summary().samples, 200);
    }

    #[test]
    fn a_histogram_counts_the_samples_that_are_longer_than_its_range() {
        let mut histogram = LatencyHistogram::new(1_000).unwrap();
        histogram.record(10);
        histogram.record(5_000);
        histogram.record(2_000);
        let summary = histogram.summary();
        assert_eq!(summary.samples, 3);
        // A sample above the range is recorded in the top bucket, so the
        // histogram's own maximum is its bound and not the longest sample.
        assert_eq!(summary.max, 1_000);
        // The longest sample and the number of the samples that were too long
        // are kept beside it, so a reader can tell a truncated sample from
        // one that was near the bound.
        assert_eq!(summary.max_observed, 5_000);
        assert_eq!(summary.over_range, 2);

        // A window that is merged into the stage brings its own count and
        // its own longest sample with it.
        let mut other = LatencyHistogram::new(1_000).unwrap();
        other.record(9_000);
        histogram.merge(&other);
        let summary = histogram.summary();
        assert_eq!(summary.samples, 4);
        assert_eq!(summary.max_observed, 9_000);
        assert_eq!(summary.over_range, 3);
    }

    #[test]
    fn a_panic_carries_its_message() {
        let payload: Box<dyn std::any::Any + Send> = Box::new("the sender gave up");
        assert_eq!(panic_message(payload), "the sender gave up");
        let payload: Box<dyn std::any::Any + Send> = Box::new(String::from("the sender gave up"));
        assert_eq!(panic_message(payload), "the sender gave up");
    }

    #[test]
    fn folding_a_window_moves_everything_it_counted_into_the_totals() {
        let metrics = Metrics::new(1_000_000).unwrap();
        metrics.record_success(Operation::Get, 10);
        metrics.record_success(Operation::Get, 20);
        metrics.record_error(Operation::Put, "deadline", 30);
        {
            let mut state = metrics.state.lock().unwrap();
            // The fold that closes a phase runs after the sampler stopped, so
            // a fold that took the latencies and left the counters would
            // report the phase as short by the requests of its last second.
            state.fold();
            // A failed request is in the errors and in the latencies, and the
            // two of them are what it takes to account for every request the
            // phase issued.
            assert_eq!(state.total_completed, [2, 0, 0]);
            assert_eq!(state.total_errors.get("deadline"), Some(&1));
            assert_eq!(state.total[0].summary().samples, 2);
            assert_eq!(state.total[1].summary().samples, 1);
            // The window is empty once it is folded, so folding it again
            // reports the same totals instead of counting them twice.
            state.fold();
        }
        let state = metrics.state.lock().unwrap();
        assert_eq!(state.total_completed, [2, 0, 0]);
        assert_eq!(state.total_errors.get("deadline"), Some(&1));
        assert_eq!(state.total[0].summary().samples, 2);
    }
}
