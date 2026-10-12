# Where the write path saturates

Sweeping `--concurrency` over the correctness run's initial load, with
`HASHRING_PROFILE_WRITES=1` on every node so the state write lock reports its own
wait and hold times. Medians of 3 runs per point, 20,000 keys, RF=3,
`OwnerOnly`, 128-byte values, 9 data nodes.

Reproduce with `./scripts/sweep.sh`; raw rows land in `results/sweep-<ts>/sweep.tsv`.

## Measurement discipline

A single run of this benchmark is not a measurement. Five runs of one unchanged
binary at concurrency 128 spanned 0.362 s to 0.551 s, a **41.7% spread**, which is
wider than any effect worth chasing. Every point below is therefore a median over
three runs, and the conclusion below rests on the one column that is stable
across repeats rather than on the throughput column, which is not.

That distinction matters: throughput does **not** show a clean peak. The medians
are 34.9k, 44.8k, 40.7k, 44.3k, 35.3k keys/s for concurrency 16, 64, 128, 256,
512. The 64 and 256 points are indistinguishable given the spread, and 128 looks
like a dip only because one of its three reps was slow. **This profile does not
support the earlier claim that throughput inflects at concurrency 128.**

## What is stable: the write lock is wait-dominated

Cumulative across all nodes, microseconds, from `StateWriteTiming`:

| conc | owner hold | owner wait | wait share |
| --- | --- | --- | --- |
| 16 | 205,646 | 89,838 | 30% |
| 64 | 211,366 | 428,292 | 67% |
| 128 | 238,365 | 537,382 | 69% |
| 256 | 221,938 | 406,112 | 65% |
| 512 | 256,213 | 401,794 | 61% |

Owner **hold** is nearly flat at ~0.21-0.26 s regardless of offered load: the work
per put does not change. Owner **wait** climbs 6x from concurrency 16 to 128 and
then flattens. A queue that grows and then plateaus is a saturated lock, and the
plateau is the signal.

At concurrency 16 the lock is still mostly uncontended (30% of lock time is
waiting). By 64 it is two-thirds waiting. Past that, more concurrency only adds
queue depth. This is consistent with the CPU profile showing 1.5 of 24 cores busy:
there is spare CPU, and the serialized resource is the single `NodeState` write
lock, not compute.

## What is not the cause

Three counters that would each explain a knee are **zero at every concurrency**:

| counter | value at all points |
| --- | --- |
| `replication_reservation_rejections` | 0 |
| `replication_rpc_retries` | 0 |
| `owner_dedup_rejections` | 0 |

So the replication queue never filled, no RPC was ever retried, and the
deduplication store never rejected an insert. Peak pending per replication stream
stayed at 8-19 against a queue capacity of 32. Backpressure is not engaged, which
also means the batching change in
[batched-replication-rpc.md](batched-replication-rpc.md) is not limited by queue
depth here -- it is limited by arrival rate, as that note records.

## Consequence

The serialized resource is one `Arc<RwLock<NodeState>>` guarding records, dedup,
topology, stream state and migration state together. Owner hold is ~11 us per
acquisition and every other write path queues behind it, so the only lever that
moves throughput is reducing the number of write-lock acquisitions or the time
each holds it.

Shrinking per-request work inside the critical section does not help much. The
`e4156a6` change removed three of four routing-token hashes and one of two
mutation-fingerprint hashes from the owner path, and mean owner hold moved 11.88 us
to 11.61 us: real, but under the noise floor. The 11 us is dominated by the dedup
probe, the record insert and the replication reservation, all of which are real
work rather than waste.

That leaves reducing acquisition count, which is the ACK-relocation direction in
[state-write-lock-profile.md](state-write-lock-profile.md). It is a larger and
riskier change: 90 access sites across 8 files, and `prune_ack_progress` reads
`dedup` while writing the ACK maps, so the lock order has to stay
`NodeState` -> `AckState` or the two can deadlock.

## A caveat on the shape of the curve

The wait curve plateaus rather than rising without bound, and hold stays flat.
That is what a saturated mutex looks like, but it is equally consistent with the
driver itself becoming the bottleneck: past some concurrency the client side
cannot keep 9 nodes fed and the offered load stops rising. Distinguishing the two
needs a per-node arrival rate, which this run does not record. Treat the
"contention or client-side saturation" question as open.

## Reproducing

```sh
cargo build --release
REPS=3 CONCS="16 64 128 256 512" ./scripts/sweep.sh
```
