# Worker pool sizing on a shared host

The `experiment` command starts a driver, a coordinator and every data node as
separate processes on one host. Each process built its tokio runtime through
`#[tokio::main]`, which defaults to one worker per logical CPU. On a 24-core host
that is 24 workers in each of 11 processes: **275 threads on 24 cores**.

This records what that cost, and the fix.

## Measurement

Correctness mode, 20,000 keys, concurrency 64, RF=3, `OwnerOnly`, 128-byte values,
9 data nodes on a 24-core AMD Ryzen 9 7845HX. Five runs per variant, medians, with
`HASHRING_PROFILE_WRITES=1`:

| variant | initial put | best run | keys/s | owner hold | owner wait | follower wait |
| --- | --- | --- | --- | --- | --- | --- |
| auto (host budget divided) | **0.336 s** | 0.315 s | **59,567** | **7.4 us** | **17.3 us** | **26.0 us** |
| `--worker-threads 4` | 0.391 s | 0.371 s | 51,132 | 9.6 us | 20.0 us | 31.2 us |
| `--worker-threads 24` (old default) | 0.540 s | 0.432 s | 37,030 | 12.0 us | 23.3 us | 39.1 us |

The automatic budget is **61% faster** than the old default on median initial put.
The comparison survives the 41.7% run-to-run spread recorded in
[write-path-saturation.md](write-path-saturation.md): the two variants' ranges do
not overlap, and even comparing each variant's fastest single run (0.315 s vs
0.432 s) leaves a 37% gap.

Thread counts, measured from `/proc/<pid>/task` across all 11 processes:
275 with `--worker-threads 24`, 99 with 8, **33 with the automatic default**.

## Why oversubscription was expensive here

This is not the usual "too many threads on a mutex" story. The state write lock is
only **5% busy** across this workload, and mean wait has always exceeded mean hold
(17-23 us waiting against 7-12 us holding). A lock that is idle 95% of the time
cannot produce waits two to three times its hold time through contention alone.

The cost is task wakeup. A task that finds the write lock held parks; releasing it
requires waking a task that the scheduler may place on a different worker, on a
different core, with the run queue containing hundreds of runnable threads. That
wakeup, not the critical section, is what `wait_nanos` is measuring. Reducing the
thread count shortened both halves at once -- hold fell 12.0 -> 7.4 us and wait
fell 23.3 -> 17.3 us -- which is what a scheduling-cost fix looks like, and is not
what a lock-partitioning fix looks like.

## Change

`main` builds the runtime explicitly so each role can size its own worker pool.
The original version of this change added one global `--worker-threads` flag
that every subcommand accepted and the experiment passed to every child. A later
per-role A/B (below) showed which roles actually need the cap, and the flag now
lives only where it matters:

| role | runtime | override |
| --- | --- | --- |
| `node` | one worker per available CPU | `node --worker-threads N` |
| `coordinator` | one worker per available CPU | none |
| `experiment` driver | host budget: `cpus / (peak_nodes + 2)`, at least 2 (1 on a single-CPU host) | `experiment --worker-threads N` |
| data nodes started by `experiment` | the same budget, passed as `node --worker-threads` | the same flag |
| client and admin commands | single-threaded (`current_thread`) | none |

"Available CPU" is `std::thread::available_parallelism()`, which honours CPU
affinity and cgroup quotas, so a containerised node with a CPU limit sizes itself
to that limit. A node that shares a host without such a limit is the case for the
manual override.

Splitting the budget evenly beats pinning every process to 4, which the first
table shows: the driver and the nodes do not carry equal load, and 24 cores across
12 processes is not 4 apiece in practice.

## Which roles need the cap

To decide where the flag belongs, a temporary build let the experiment set the
driver, coordinator and node worker counts independently
([patch](data/worker-pool-roles/instrumentation.patch)). Each variant raised one
role from the automatic budget (2 on this host) to one worker per CPU (8).

Correctness mode, same workload as above, 10 peak nodes, Apple M1 Pro with
8 logical CPUs (12 processes, so 96 threads in the all-8 variant against 24
cores' 275 above). Six runs per variant, order rotated each round, medians;
p-values are two-sided permutation tests on the median against the all-2 baseline.
Per-run data: [runs.tsv](data/worker-pool-roles/runs.tsv),
[metadata](data/worker-pool-roles/metadata.json).

| driver / coordinator / node | initial put | initial get | owner wait | follower wait |
| --- | --- | --- | --- | --- |
| 2 / 2 / 2 (budget) | 0.591 s | 0.264 s | 10.4 us | 15.4 us |
| 2 / 2 / **8** | +13% (p=0.07) | 0% (p=0.96) | -35% | -55% |
| 2 / **8** / 2 | -4% (p=0.28) | -5% (p=0.08) | -3% | -15% |
| **8** / 2 / 2 | +17% (p=0.10) | **+23% (p=0.013)** | +22% | 0% |
| 8 / 8 / 8 (old default) | +10% (p=0.06) | +16% (p=0.012) | -33% | -52% |

Replica repair to full RF, the scale-out phase and total elapsed time moved by
5% or less in every variant.

- **Coordinator:** no measurable effect, including the repair and migration
  phases where it does work. It is off the data path and its idle workers park,
  so it keeps the per-CPU default and has no override.
- **Driver:** uncapping it alone is the only change that clearly slows the read
  phase, and it gives the largest put regression. The load generator competes
  with the nodes for the same cores, so the experiment must budget it.
- **Nodes:** uncapping them slows puts by a similar amount, but here node lock
  waits *fall* by a third to a half. That does not match the wakeup-cost
  explanation above, which predicts the opposite. The scheduler and core count
  differ (macOS, 8 heterogeneous cores, versus Linux on 24), so treat the
  mechanism as host-dependent; the throughput direction agrees.
- **Size of the effect:** on this host the old default costs about 10% on puts,
  not the 61% measured on 24 cores, and run-to-run spread (0.54-0.82 s within the
  baseline) is as wide as the per-role effects. Only the read-phase result is
  significant at six runs; a Linux host with more cores would separate the node
  and driver contributions to puts.

Two of the 18 runs with nodes at 2 workers failed at scale-out ("concurrent
mutator finished without a successful mutation"), against none of 12 with nodes
at 8. That is not enough to attribute, but the budget is 2 workers per node on
an 8-CPU host, so it is worth investigating separately.

## What this does not fix

The write lock still serializes all writes to one node, and its duty cycle is
unchanged in kind. This change makes each acquisition cheaper; it does not reduce
how many acquisitions a workload needs. If write throughput on a single node is
still the constraint, the ACK-state relocation described in
[state-write-lock-profile.md](state-write-lock-profile.md) remains the next lever,
now with a cheaper critical section to hide behind.

## Reproducing

```sh
cargo build --release
# automatic budget
target/release/hashring-rs experiment --mode correctness --output results/run-auto
# every driver and node at 24 workers, for comparison (the coordinator always
# keeps its own per-CPU default)
target/release/hashring-rs experiment --mode correctness --worker-threads 24 \
  --output results/run-24
```

Thread count for a running fleet:

```sh
for p in $(pgrep -x hashring-rs); do echo "$(ls /proc/$p/task | wc -l) $p"; done | sort -rn
```
