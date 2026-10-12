# Coalescing the routing-token read lock: no established speedup

Removing the extra `NodeState` read-lock acquisition from Put and Delete produced
about 5% higher median PUT throughput on this host, but the paired runs were too
variable to establish a repeatable improvement. The change is below what this
benchmark can resolve. It was later applied as a code-clarity cleanup, not as a
throughput optimization; the measured binaries remain the baseline and the
benchmark patch described below.

## Comparison

Baseline: `1a586254`, including the memory-accounting, partial-acknowledgement and
worker-count corrections. The [candidate patch](data/read-lock-ab/candidate.patch)
only moves the token calculation into the existing initial read-lock block in Put
and Delete. Both binaries were built with `cargo build --release` using Rust
1.98.1 on an Apple M1 Pro (8 logical CPUs, 16 GiB RAM).

Each existing performance experiment used 10 nodes, RF=3, OwnerOnly, two Tokio
workers per process, 100,000 keys, 128-byte values, seed 1 and 128 virtual nodes.
Lock profiling was disabled. At each concurrency, one warm-up pair was excluded
in advance, followed by six measured pairs. Baseline/candidate order reversed
between pairs. All 28 runs completed successfully and verified all keys.

## Results

Throughput is the instrumented initial PUT phase, excluding startup, replica
admission, GET verification and subsequent sequential latency probes.

| Concurrency | Baseline median PUT/s | Candidate median PUT/s | Change in medians |
| --- | ---: | ---: | ---: |
| 64 | 28,269 | 29,683 | +5.0% |
| 256 | 40,570 | 42,542 | +4.9% |

Paired throughput changes, in execution order:

- Concurrency 64: -7.87%, +29.57%, -5.61%, +95.32%, +4.90%, -0.03%.
- Concurrency 256: -1.48%, -9.21%, +27.85%, +18.70%, -5.42%, +12.10%.

The geometric mean of paired ratios was +15.0% and +6.3%, respectively.
Exploratory paired-bootstrap 95% intervals (20,000 resamples, seed 5, on log
ratios) were [-3.4%, +45.1%] and [-3.4%, +17.5%]. Both include no improvement.
Six pairs on one shared host are limited evidence; these intervals do not remove
systematic environmental effects. The candidate was faster in three of six pairs
at each concurrency, so a sign test gives no evidence either way.

At concurrency 64, run order explains more than the variant does. The first run
of each pair averaged 25,625 PUT/s and the second 29,808 PUT/s. Both large
candidate gains (+29.57% and +95.32%) came from pairs where the baseline ran
first; where the candidate ran first the changes were -7.87%, -5.61% and +4.90%.
The +15.0% geometric mean at 64 is therefore an order and outlier effect, not a
candidate effect. Concurrency 256 shows no order effect (42,686 versus 42,622
PUT/s).

The run is also far too small for the effect in question. The standard deviation
of paired log ratios was 0.287 at 64 and 0.137 at 256; detecting a true 5% change
with 80% power at a 5% two-sided level would take roughly 270 and 60 pairs. The
expected effect is much smaller than 5%: the patch removes one uncontended Tokio
`RwLock` read acquisition per Put or Delete, an estimated sub-microsecond cost
against requests of about 140 microseconds. The earlier routing-token and
fingerprint change in [write-path-saturation.md](write-path-saturation.md),
which removed more work from the same path, moved mean owner hold by only
0.27 microseconds.

Median sequential direct-owner p50 latency was unchanged at concurrency 64
(138.5 microseconds for both) and fell from 138 to 134 microseconds at 256.
These 128-request probes run after the load phase, so they do not measure loaded
request latency. DELETE performance was not separately measured.

Replication queue backpressure engaged in some runs. Median reservation rejection
counts were 2.5 versus 0 at concurrency 64 and 90.5 versus 133.5 at 256; all RPC
retry counters were zero. The comparison measures end-to-end throughput with
client retries, not isolated read-lock cost.

Two early baseline runs had 613/656-second external wall durations while the
experiment's monotonic durations were 21/23 seconds. Their rows were retained.
Idle sleep was inhibited for later runs. This discrepancy and the broad
throughput spread further limit the conclusion: +5% is an observed median change,
not a demonstrated optimization benefit.

## Evidence and reproduction

[Per-run measurements](data/read-lock-ab/measurements.csv) include warm-ups,
latency probes and backpressure counts. [Metadata](data/read-lock-ab/metadata.json)
records binary SHA-256 hashes. Full local manifests, logs, summaries, binaries,
build-source copies and the runner/analysis scripts are retained under
`results/read-lock-ab-20261012/` (ignored by version control).

Build the baseline, save its executable, apply the candidate patch, build and
save the candidate executable, then restore the baseline source. Run each saved
binary with the following parameters, alternating order for six measured pairs
at each concurrency after a warm-up pair:

```sh
BINARY --worker-threads 2 experiment --mode performance --nodes 10 \
  --keys 100000 --value-bytes 128 --concurrency 64 --output UNIQUE_OUTPUT
```

Repeat with concurrency 256. Each executable spawns its own matching child
processes. Build/runtime manifests can report dirty or differing source because
this comparison used saved binaries and restored source; identify the artifacts
by the recorded hashes, baseline commit and exact candidate patch rather than
claiming clean-source reproducibility.
