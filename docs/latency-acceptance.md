# Latency acceptance measurements

The `latency` mode offers requests on a fixed schedule and measures from each
request's scheduled time to completion. These measurements cover an in-memory
cluster on one shared Linux host, using the native Rust client over loopback.
They do not measure the Redis gateway or a multi-host deployment.

## Setup and evidence

| Setting | Value |
| --- | --- |
| Nodes / replication | 10 / RF=3 |
| Acknowledgement | `OwnerOnly` (does not wait for follower acknowledgements) |
| Data | 100,000 keys, 128-byte values |
| Operations | GET:PUT:DELETE = 90:9:1 |
| Distributions | uniform and Zipf 0.99 |
| Virtual nodes | 128, plus comparisons at 16 / 64 / 256 |
| Main stages | 10 s warmup, 60 s measured, 3 repetitions per point |
| Overload ladder | 3 s warmup, 10 s measured, 1 repetition per point |
| In-flight limit | 4,096 requests |
| Host | 24 logical CPUs, 32 GB RAM, Linux 7.2.11-zen1-1-zen |
| Campaign | `latency-acceptance-20261022-172621`, about 39 minutes |
| Source | `52adf85` |

The [host record](data/latency-acceptance/host.txt) identifies the binary and
machine. The [stage table](data/latency-acceptance/latency-acceptance-20261022-172621/stages.tsv)
holds all 36 measured stages: 24 main stages, 4 overload stages and 8 virtual-node
stages, across 11 latency runs. A separate `performance` run measured the
closed-loop write baseline. The [median table](data/latency-acceptance/latency-acceptance-20261022-172621/medians.tsv)
contains medians of each field across repetitions; a median p99 is not the
p99 of pooled request samples. The raw run directories are not checked in;
see the [data README](data/latency-acceptance/README.md).

The write baseline was **84,054 ops/s**. Main rates are 0.25, 0.5, 0.75 and
0.9 times this baseline. It is a reference for choosing rates, not the
capacity of the read-heavy mixed workload. Preload write throughput across
the six main runs ranged from 80,993 to 86,500 ops/s (6.8% spread); the host
was shared with desktop processes and other work.

## Latency under the main load

GET latency in microseconds, medians over three repetitions:

| Distribution | Offered ops/s | p50 | p95 | p99 | p999 |
| --- | --- | --- | --- | --- | --- |
| uniform | 21,013 | 56 | 78 | 95 | 209 |
| uniform | 42,027 | 57 | 82 | 107 | 215 |
| uniform | 63,040 | 58 | 97 | 190 | 673 |
| uniform | 75,648 | 62 | 134 | 548 | 1,892 |
| zipf:0.99 | 21,013 | 57 | 78 | 95 | 234 |
| zipf:0.99 | 42,027 | 57 | 82 | 109 | 233 |
| zipf:0.99 | 63,040 | 57 | 93 | 173 | 576 |
| zipf:0.99 | 75,648 | 62 | 120 | 439 | 2,381 |

All 24 main stages achieved their offered rate without drops, errors or
client retries. Across individual repetitions, p50 stayed between 56 and
63 us, while p99 rose from 92–110 us through half the write baseline to
304–592 us at the highest rate. The largest main-stage GET sample bucket
was 21,247 us; maxima varied more than the lower percentiles.

Uniform and Zipf latencies overlap in this campaign. Three repetitions on a
shared host do not establish equal latency or a general advantage for either
distribution. PUT and DELETE percentiles and every repetition are retained
in the stage and median tables.

## Load balance

At 75,648 offered ops/s, medians over three repetitions:

| Distribution | Owned keys max/mean | Owned keys CV | Served ops max/mean | Served ops CV | Maximum node CPU |
| --- | --- | --- | --- | --- | --- |
| uniform | 1.116 | 0.088 | 1.144 | 0.089 | 36.3% |
| zipf:0.99 | 1.116 | 0.088 | 1.859 | 0.338 | 48.8% |

CPU percentages are fractions of one core for the entire node process.
Key ownership follows the fixed ring; request counts follow the workload's
key distribution. Zipf traffic therefore concentrates on the owners of hot
keys even when the key counts are relatively even.

The virtual-node comparison uses uniform traffic at 21,013 and 63,040 ops/s.
These are the lower-rate rows; request max/mean differs by at most 0.008
between rates:

| Virtual nodes | Owned keys max/mean | Owned keys CV | Served ops max/mean | Served ops CV |
| --- | --- | --- | --- | --- |
| 16 | 1.200 | 0.177 | 1.206 | 0.183 |
| 64 | 1.255 | 0.139 | 1.242 | 0.134 |
| 128 | 1.116 | 0.088 | 1.147 | 0.089 |
| 256 | 1.087 | 0.048 | 1.066 | 0.049 |

More virtual nodes reduced CV for this seed. The worst-node share is not
monotonic: the 64-vnode ring is worse than the 16-vnode ring by that measure.
One seed cannot establish a generally sufficient vnode count.

## Overload ladder

The ladder used uniform traffic and the same in-flight limit:

| Offered ops/s | Achieved ops/s | GET p50 | GET p99 | Dropped requests |
| --- | --- | --- | --- | --- |
| 84,054 | 84,054 | 64 us | 341 us | 0 |
| 126,081 | 103,522 | 35,391 us | 86,463 us | 225,590 |
| 168,108 | 100,937 | 39,199 us | 86,143 us | 671,713 |
| 252,162 | 100,619 | 39,359 us | 86,591 us | 1,515,428 |

The transition to substantial queueing and drops occurred between 1 and 1.5
times the write baseline. Above it, achieved throughput stayed around
100,000 ops/s and GET median latency reached 35–39 ms. These results describe
this host and configuration. They do not establish that increasing the
in-flight limit would increase throughput; that needs a separate experiment.

Dropped requests were never issued and are absent from the latency histogram.
An overload percentile must therefore be read together with drops and errors.
Zero errors among issued requests does not mean every offered request succeeded.

## Accounting and measurement limits

All 36 stages satisfy `sum(ops) + sum(errors) == issued`: 96,686,119 issued
requests completed with zero recorded errors and zero client retries,
including topology-refresh retries. The main stages had no drops; the
ladder dropped 2,412,731 requests. After each latency run, the driver checked
all keys against their expected value or absence.

The sender reported no saturated stage. The largest recorded `sender_lag`
in a measured window was one request. The per-window `behind` reached 67
in main/vnode runs and 3,589 in the ladder. These fields check whether the
sender processed its schedule; they do not isolate runtime, transport and
server queues from one another.

The histogram uses three significant figures. `max` is a bucket upper bound;
`max_observed` retains the actual largest sample, and `over_range` counts
samples clipped at the configured range. This campaign's largest GET bucket
was 125.1 ms, below its 31 s range. The [sender timing note](optimizations/latency-sender-timing.md)
explains why a dedicated thread replaces a runtime timer.

The workload reserves 50,000 keys for DELETE, disjoint from GET/PUT, so its
reads do not miss due to deletes. PUT always writes the preloaded value for
a key. This simplifies final-state checking but does not test arbitrary
read/write/delete races or detect every dropped write. Workload draws are
random, not a replayable request trace.

Only 128-byte values, the 90:9:1 mix and `OwnerOnly` were measured here.
The 50:50:0 mix, 4 KiB values, follower-waiting ACK policies and cross-host
network costs remain unmeasured. The data fits in memory. Percentiles include
client scheduling and transport delay and are not server-handler timings.

## Source and reproduction

The campaign was built from committed source `52adf85`, with source digest
`97aaff0f9123ef7004cfd55a394adf8e252e93c7cce380cd09914daabf3d3a38`.
Its manifests reported `source_reproducible: false` because the sweep wrote
an untracked `host.txt` into the source tree before running. The script now
writes metadata under the run directory and copies tables only after the
last run. Formal experiments should use committed source and
`--require-clean-source`; manifests do not archive untracked source files.

To reproduce the historical configuration, check out `52adf85`, build with
the recorded toolchain and run `scripts/latency-sweep.sh`. The binary hash is
path-dependent because the absolute build root is compiled into it. To run
the current implementation:

```sh
cargo build --release
./scripts/latency-sweep.sh
```

Each campaign needs a new output directory. For custom rates without a
capacity measurement, set `LADDER=0` or provide explicit `LADDER_RATES`.
