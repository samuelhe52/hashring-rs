# Redis branch: native-client A/B performance

The 2026-10-19 comparison found no consistent native PUT or verified GET
throughput regression from the Redis branch. All 48 measured runs passed, but
every paired throughput interval includes zero: these results establish neither
a throughput improvement nor performance equivalence. Small regressions remain
unresolved. RF=1 showed a small increase in sampled process RSS.

**Every throughput percentage below compares `redis` with `main`: positive
means the Redis branch was faster.** The comparison covers the branch's shared
client and data-node changes with the Redis gateway stopped.

## Revisions and workload

| Variant | Commit |
| --- | --- |
| Baseline, `main` | `214daee57de65e0658ffed4486807ba865d7dd19` |
| Candidate, `redis` | `2e05385c3c2169837bcc3daefd100e8cd7a54024` |

Both executables used `cargo build --locked --release --bin hashring-rs -j 4`,
Rust 1.98.1, and `aarch64-apple-darwin`. The host was an Apple M1 Pro with eight
logical CPUs and 16 GiB RAM, running macOS on AC power. No thermal warning was
reported before or after the cohort.

Each run started a fresh coordinator and six data nodes, then loaded distinct
keys through the native Rust client and read back every value for verification.
There were six adjacent A/B pairs per case, with `main` first in odd pairs and
`redis` first in even pairs. Case order rotated between repetitions. One
10,000-key RF=3/OwnerOnly warmup per binary was excluded in advance; measured
clusters did not receive a separate warmup load.

| Case | RF | ACK policy | Keys | Value bytes |
| --- | ---: | --- | ---: | ---: |
| `rf1-owner-128` | 1 | OwnerOnly | 300,000 | 128 |
| `rf3-owner-128` | 3 | OwnerOnly | 300,000 | 128 |
| `rf3-first-128` | 3 | FirstSuccessor | 300,000 | 128 |
| `rf3-owner-4096` | 3 | OwnerOnly | 100,000 | 4,096 |

All cases used concurrency 64, seed 1, 128 virtual nodes, two workers each for
the driver, coordinator, and every data node, and a 30-second operation deadline.
Minimum admitted copies remained 1 and minimum healthy followers remained 0.
Other node tuning settings used matching defaults; the
[metadata](data/redis-native-ab-20261019/metadata.json) preserves every resolved
configuration. Application profiling was disabled. No gateway, TTLs,
conditional writes, or keyspace scans were used.

The Redis branch changes shared paths even without the gateway: expiry checks,
conditional-write handling, record indexing, and replication payloads. There
are also offsets: replication shares the stored value's `Arc`, and followers
use the supplied request fingerprint instead of recomputing it. This whole-branch
comparison does not isolate the cost or benefit of any one change. See the
[node RPC handlers](../../crates/hashring-node/src/rpc.rs),
[record storage](../../crates/hashring-node/src/lib.rs), and
[replication preparation](../../crates/hashring-node/src/replication.rs).

## Throughput

PUT measures initial loading; GET measures full read verification, including
the driver's value checks. These phase rates exclude cluster startup and the
subsequent sequential latency probes.

The paired percentage is
`100 * (exp(mean(log(redis_rate_i / main_rate_i))) - 1)` across six pairs.
The descriptive 95% intervals bootstrap those paired log ratios with 20,000
resamples and seed 20261019. They are not equivalence tests and do not account
for systematic host interference.

| Case | Paired PUT change | 95% interval | Paired verified GET change | 95% interval |
| --- | ---: | --- | ---: | --- |
| RF1, OwnerOnly, 128 B | +0.69% | −0.32% to +1.49% | −1.74% | −9.10% to +3.97% |
| RF3, OwnerOnly, 128 B | +4.17% | −0.33% to +10.82% | +0.06% | −5.32% to +4.30% |
| RF3, FirstSuccessor, 128 B | +3.23% | −3.40% to +13.74% | +2.22% | −1.25% to +5.64% |
| RF3, OwnerOnly, 4 KiB | +4.46% | −1.53% to +9.48% | −0.88% | −4.94% to +2.51% |

For example, **+4.17% means `redis` achieved higher PUT throughput than `main`**
in the paired estimate for RF3/OwnerOnly/128 B. Its interval still includes
no difference, so it is not an established speedup.

Absolute rates below are medians of the six runs per variant. Dividing these
medians gives a different statistic from the paired geometric mean above.

| Case | `main` PUT/s | `redis` PUT/s | `main` verified GET/s | `redis` verified GET/s |
| --- | ---: | ---: | ---: | ---: |
| RF1, OwnerOnly, 128 B | 72,995 | 73,943 | 77,311 | 76,690 |
| RF3, OwnerOnly, 128 B | 53,569 | 57,723 | 75,117 | 76,825 |
| RF3, FirstSuccessor, 128 B | 38,338 | 38,834 | 76,586 | 79,187 |
| RF3, OwnerOnly, 4 KiB | 38,570 | 40,500 | 63,401 | 63,834 |

## Latency and resources

After each throughput run, the harness sampled 128 sequential native-client
PUTs and 128 sequential direct-owner PUTs against existing keys. These probes
measure low-concurrency overwrite latency; their p95/p99 values do not measure
tail latency during the concurrent load phase.

| Case | `main` client p50, median (µs) | `redis` client p50, median (µs) | `main` direct-owner p50, median (µs) | `redis` direct-owner p50, median (µs) |
| --- | ---: | ---: | ---: | ---: |
| RF1, OwnerOnly, 128 B | 101 | 100.5 | 97.5 | 96.5 |
| RF3, OwnerOnly, 128 B | 117.5 | 117.5 | 112.5 | 112 |
| RF3, FirstSuccessor, 128 B | 220.5 | 223 | 218.5 | 217.5 |
| RF3, OwnerOnly, 4 KiB | 136.5 | 131 | 129 | 131.5 |

All paired p50 intervals include zero change. There is no clear latency
difference in these probes.

Process CPU time and RSS were sampled every 0.5 seconds across the driver,
coordinator, and six nodes. CPU time was interpolated at phase boundaries.
The 4 KiB case used 8.19% less sampled PUT CPU per operation on `redis`
(paired interval −11.05% to −6.28%). This is an approximate resource result
from short phases, and does not identify its cause or establish a throughput
gain. Other PUT CPU-per-operation intervals included zero change.

RF1's median summed RSS during GET verification was 386.0 MiB on `main` and
399.3 MiB on `redis`. The paired increase was 3.33%, with interval +2.41% to
+4.23%. Replicated RSS was much more variable, especially in the short 4 KiB
case, and supports no general memory-regression percentage. Summed RSS includes
allocator retention and shared mappings; it is neither unique physical memory
nor live heap usage.

## Validation and limits

All 48 measured runs completed every write and verified every loaded value.
Owner and follower receipt-capacity rejections and replication RPC retries were
zero. RF3 runs recorded 1,615 retryable replication queue reservation rejections
in total. The measured rates therefore include the client's handling of
backpressure. Per-run counters are retained in the CSV.

Every manifest confirmed matching clean build/runtime source, the expected
revision, and profiling off. Each variant used one executable digest across
all its runs. The baseline was built in a fresh Cargo target directory. An
intermediate candidate build failed because a shared target reused stale
generated protocol artifacts; it produced no measured executable. The measured
candidate was rebuilt in a separate fresh target directory. Use independent
fresh target directories when reproducing this comparison; see also the
[earlier build-cache caveat](write-path-cleanup-and-routing.md#final-build-validation-and-build-cache-caveat).

This was an active macOS desktop with no CPU isolation. Some early trials
overlapped interim analysis, adding scheduling variation. No application
profiling or concurrent benchmark clusters were used. Six pairs and short
phases cannot resolve every small regression, particularly GET performance.
DELETE, migration, TTL, conditional-write, scan, sustained mixed-workload, and
overwrite throughput were not measured. The positive throughput estimates
should not be attributed to a specific optimization without a separate
comparison that isolates it.

## Evidence and reproduction

The following evidence is included alongside this note:

- [Per-run measurements](data/redis-native-ab-20261019/measurements.csv): all
  48 measured rows, executable digests, latency probes, pressure counters,
  and sampled resources, in execution order.
- [Aggregate analysis](data/redis-native-ab-20261019/analysis.json): medians,
  ranges, individual pair changes, paired estimates, and bootstrap intervals.
- [Metadata](data/redis-native-ab-20261019/metadata.json): revisions, SHA-256
  and BLAKE3 executable identities, toolchain, complete resolved settings,
  execution order, and analysis method.

Full local manifests, summaries, events, process logs, resource samples,
coordinator databases, binaries, source archives, and runner/analysis scripts
remain under `results/redis-ab-20261019T184459/` (Git-ignored). The included
CSV and JSON files can be inspected without that local directory.

Build each pinned revision from clean source into its own fresh target
directory. Run one excluded 10,000-key RF3/OwnerOnly warmup per executable.
Then run six measured pairs for each matrix row, alternating variant order
and rotating case order as described above, with a new output directory each
time. For RF3/OwnerOnly/128 B, the invocation is:

```sh
BINARY experiment --mode performance --nodes 6 \
  --keys 300000 --value-bytes 128 --concurrency 64 \
  --seed 1 --virtual-nodes 128 --worker-threads 2 \
  --coordinator-worker-threads 2 --desired-replication-factor 3 \
  --policy owner-only --minimum-admitted-copies 1 \
  --minimum-healthy-followers 0 --operation-timeout-ms 30000 \
  --require-clean-source --output UNIQUE_OUTPUT
```

Keep both profiling environment variables (`HASHRING_PROFILE_WRITES` and
`HASHRING_PROFILE_RPC`) unset. Change only RF, policy, key count, and value
size for the other matrix rows, using the saved resolved settings if defaults
have changed.
