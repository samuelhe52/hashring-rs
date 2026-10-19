# ACK bookkeeping off the state lock: rejected

Two changes were measured here and neither was merged. The first moved the ACK
maps behind their own lock. The second read the topology epoch through an
atomic in replication delivery. A third candidate,
[lazy-ack-completion.md](lazy-ack-completion.md), was not merged either. Only
the completion timing (`ack_completion_write_timing`, commit `cb71c40`)
survived this investigation. Across all three, the state lock was not what
limits RF=3 throughput on this host.

The candidates are kept as patches against `main` at `30b19dc`:

- [split-candidate.patch](data/ack-state-lock/split-candidate.patch): the
  split, together with the timing and the epoch atomic
- [epoch-candidate.patch](data/ack-state-lock/epoch-candidate.patch): commit
  `3d0a5e5`, the epoch atomic on top of the timing

[state-write-lock-profile.md](state-write-lock-profile.md) found the replication
ACK writeback waiting far longer for the `NodeState` write lock than it held it,
for maps that share no invariant with `records` or `dedup`. This change moves
those maps behind their own lock.

The ACK path's own wait falls 94-99% everywhere. Under `OwnerOnly`, owner lock
wait falls 5-11%, but write throughput does not change measurably. Under
`FirstSuccessor` at concurrency 256, owner and follower lock wait and owner hold
*rise* 15-18% in all eight pairs. The measurement also exposed a larger cost the
old profile missed: under `FirstSuccessor`, every write takes the state write
lock a second time to complete its required ACKs, and that acquisition waits
24 µs (concurrency 64) to 109 µs (256).

## Change

- `owner_stream_unacked`, `ack_progress` and `ack_process_instances` moved from
  `NodeState` into `AckState`, behind an `Arc<std::sync::Mutex<_>>` that
  `NodeState` and the replication dispatcher both hold
  ([lib.rs](../../crates/hashring-node/src/lib.rs)).
- The ACK writeback in `deliver_replication_stream` takes only `AckState`
  ([replication.rs](../../crates/hashring-node/src/replication.rs)). The
  `wait_required_acks` subscription does too.
- Paths that need both locks take `NodeState` first: preparing replication
  entries, recording unacked sends, the readiness lag check, the final required-ACK
  check, `GetReplicationProgress`, topology refresh, and `prune_ack_progress`. No
  holder of `AckState` awaits or takes `NodeState`. A `std` guard held across an
  `.await` would make the future `!Send`, so the compiler rejects that mistake.
- `prune_ack_progress` scans `dedup` without holding `AckState`, so a writeback
  is never stuck behind that scan. `dedup` cannot change during the scan, because
  the caller holds `NodeState`.
- The delivery loop's epoch check reads an `Arc<AtomicU64>` that `set_topology`
  keeps current, instead of taking a `NodeState` read lock on every attempt.
  This was measured separately; see [The epoch atomic](#the-epoch-atomic).
- The final check in `wait_required_acks` took the state write lock untimed. It
  is now timed. This part was kept, as `ack_completion_write_timing`.

The split is not in the tree.

## Performance-mode matrix

The workload is the 6-node series of
[grpc-io-bottleneck.md](grpc-io-bottleneck.md) and
[replication-batch-linger.md](replication-batch-linger.md): performance mode,
6 nodes, seed 1, 300,000 keys, 128-byte values, RF=3, 2 ms linger, 2 workers per
process, with `HASHRING_PROFILE_WRITES=1` and `HASHRING_PROFILE_RPC=1`. The host
is an 8-core M1 Pro.

- **Binaries:** the baseline is `main` at `30b19dc` (sha256 `e4d5ec42…`) and the
  branch is this change (`9f65f031…`).
- **Design:** per cell, one warm-up pair (excluded), then 8 measured pairs. Within
  each pair the variant order alternates, and cell order rotates each rep.
  `caffeinate -i` was on. All 72 runs succeeded and Time Machine was not backing up.

Each value is the branch's change against the baseline: the geometric mean of
per-pair ratios, with a paired-bootstrap 95% interval (20,000 resamples, seed 5)
in brackets. Lock figures are µs per write, summed across nodes.

| cell | PUT ops/s (base median) | PUT change | owner lock wait | owner lock hold | follower lock wait | ACK writeback wait | owner `Put` handler |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `OwnerOnly`, 64 | 61,468 | +2.3% [−2.1, +8.5] | **−11.4%** [−16.0, −6.6] | −0.9% | −5.8% | **−97.6%** | **−8.7%** [−12.1, −5.0] |
| `OwnerOnly`, 256 | 74,665 | +1.4% [−1.2, +3.6] | **−4.5%** [−8.0, −1.8] | +0.3% | −2.9% | **−98.9%** | −2.6% [−5.8, +0.6] |
| `FirstSuccessor`, 64 | 40,481 | −4.2% [−10.6, +0.9] | −2.8% [−5.5, +0.4] | **+9.5%** [+5.1, +15.0] | +2.7% | **−94.2%** | +0.7% |
| `FirstSuccessor`, 256 | 56,777 | −1.5% [−5.3, +1.6] | **+17.6%** [+13.7, +21.9] | **+15.1%** [+12.2, +18.7] | **+14.5%** [+12.4, +16.5] | **−97.3%** | +1.1% |

Absolute owner lock wait per write, baseline → branch, in µs:

| cell | baseline | branch |
| --- | --- | --- |
| `OwnerOnly`, 64 | 9.5 | 8.5 |
| `OwnerOnly`, 256 | 47.6 | 46.1 |
| `FirstSuccessor`, 64 | 11.7 | 11.1 |
| `FirstSuccessor`, 256 | 58.4 | 67.7 |

ACK-completion acquisitions on the branch wait 24.1 µs each at
`FirstSuccessor`, 64 and 108.6 µs at 256, one acquisition per write. The baseline
binary does not time them.

Raw data, all in `data/ack-state-lock/`:

- [perf-runs.tsv](data/ack-state-lock/perf-runs.tsv): the `collect.py` columns
- [perf-ack-completion.tsv](data/ack-state-lock/perf-ack-completion.tsv)
- [perf-host.txt](data/ack-state-lock/perf-host.txt): run order and load
- [perf-ab.sh](data/ack-state-lock/perf-ab.sh): the runner

## Reading

- **Throughput: no change resolved in any cell.** Every interval includes zero.
  - Run order matters about as much as the variant. In `OwnerOnly`, 64 the second
    run of each pair averaged 2% faster than the first. In `FirstSuccessor`, 64 it
    averaged 4% slower.
  - Single pairs still swing from −23% to +23%.
  - Client `Put` RPC latency means and p50s show no resolved change either.
- **`OwnerOnly`: the split works as intended, but it is not the bottleneck.**
  - Owner lock wait falls 11% at concurrency 64, every pair agrees, and the owner
    handler is 9% faster.
  - The handler accounts for under 10% of the 880 µs client RPC, so that saving
    cannot show up in throughput.
  - At 256, owner wait is 47 µs per write and only 4.5% of it goes away. That wait
    is queueing among owner and follower writers, not ACKs.
- **`FirstSuccessor`, 256 regresses on every lock metric, in 8 of 8 pairs.**
  - Owner hold per acquisition rises 0.9 µs, so the critical section itself got
    longer.
  - The hypothesis, not tested here: owner writes now take `AckState` inside the
    state lock, twice per write (registering `ack_progress` and recording the
    unacked send). Under `FirstSuccessor`, ACK-awaited entries skip the linger, so
    writebacks are frequent. An owner holding `NodeState` can block on a
    writeback that holds `AckState`, and on macOS the contended `std` mutex parks
    the thread. Each such stall stretches the hold for every writer queued
    behind it.
  - Under `OwnerOnly`, writebacks are rarer, and the hold does not change.
- **The larger cost is ACK completion.**
  - Under `FirstSuccessor`, each write takes the state write lock twice: once to
    apply, and once in `wait_required_acks` to clear `required_acks` from its
    dedup receipt.
  - The second acquisition waits 24-109 µs, more than the owner's first (11-68 µs).
    It doubles the writer queue that every owner and follower competes in.
  - It was never timed before this change, so no earlier profile shows it.
- **Stored anchors are context only.** This baseline is 13-22% faster than the
  stored phase C figures (50.3k, 62.1k and 35.9k PUT/s), because `main` has
  changed since. Owner lock wait at `OwnerOnly`, 256 (47.6 µs) is close to phase
  E's 43 µs, so the lock picture has not shifted.

## Earlier correctness-mode A/B

The first comparison used the correctness run's 20,000-key initial load. That
load ran on 9 data nodes, `OwnerOnly`, with 6 ABBA pairs at concurrency 64 and 256.
The window is too short for throughput: loads took 0.26-0.48 s, and pairs swung
from −24% to +21%.

It agreed on the lock picture:
- ACK wait fell 13× at concurrency 64 and 23× at 256.
- Owner wait was unresolved.

Raw rows: [correctness-ab.tsv](data/ack-state-lock/correctness-ab.tsv), with
[correctness-ab.sh](data/ack-state-lock/correctness-ab.sh) and
[extract.py](data/ack-state-lock/extract.py).

## The epoch atomic

The delivery loop's epoch check, alone, was also reverted after measurement.

**Setup.** This was a paired A/B of `main` against a build carrying the
completion timing, the epoch atomic and lazy ACK completion. It ran in all
four cells, with 4 pairs each after a warm-up pair.

**Why `OwnerOnly` isolates it.** Under `OwnerOnly`, no write waits for ACKs, so
lazy completion and its timing never run. In those cells, the epoch atomic is
the only change that runs.

| | `OwnerOnly`, 64 | `OwnerOnly`, 256 |
| --- | --- | --- |
| ACK writeback wait | **+70%** [+42, +105], 4/4 pairs | **+46%** [+43, +50], 4/4 |
| owner lock wait | −5.3% [−10.2, +0.2] | −3.5% [−7.3, +0.5] |
| PUT ops/s | −0.1% [−4.0, +4.5] | −4.8% [−10.6, −0.2] |
| client `Put` RPC mean | +0.1% | +6.7% [+1.2, +13.0] |

**Ruling out batch size.** Replication batches shrank only 1-3%, so batching
does not explain the ACK writeback increase.

**Hypothesis, not tested.** tokio's `RwLock` is fair, so the removed read
acquisition queued each delivery attempt behind waiting writers. That spread out
the ACK writebacks that follow each RPC. Without it, writebacks arrive in step
with owner write bursts.

**`FirstSuccessor`.** The cumulative build was also worse than `main` in these
cells: owner wait +11% at concurrency 256 in 4 of 4 pairs.

Raw rows: [epoch-main-vs-lazy-runs.tsv](data/ack-state-lock/epoch-main-vs-lazy-runs.tsv)
and [epoch-main-vs-lazy-host.txt](data/ack-state-lock/epoch-main-vs-lazy-host.txt).

## Tradeoff and verdict

As built, the split trades a small `OwnerOnly` lock improvement for a
`FirstSuccessor` regression, with no measurable throughput change in either.
It also adds a second lock and an ordering rule that every new call site must
follow.

Two directions followed from the data. The second was tried in
[lazy-ack-completion.md](lazy-ack-completion.md) and gave no end-to-end gain:

- **Keep owner writes off `AckState` while they hold `NodeState`.** Registering
  `ack_progress` and the unacked send could move out of the state critical section,
  but only if dispatch order still matches sequence order. Otherwise, do not split
  the lock at all.
- **Remove the second state write lock from ACK-waiting writes.**
  `complete_required_acks` only clears a receipt's `required_acks` to release its
  retained bytes. Doing that lazily, at expiry or on the next write to the
  receipt, would leave one state acquisition per write under `FirstSuccessor`.

The instrumentation part of this change, the timing of the formerly untimed
completion site, is independent of the split and was kept.

Sharding `records` is not supported by this data. Owner hold is 6-7 µs, and under
`OwnerOnly` owner wait barely responds to removing a fifth of the acquisitions.

## Reproducing

```sh
git archive main | tar -x -C "$S/baseline-src"   # build each side in its own target dir
mkdir -p "$S/bin"; cp <baseline>/release/hashring-rs "$S/bin/baseline"
cp target/release/hashring-rs "$S/bin/branch"
S="$S" REPS=8 caffeinate -i docs/optimizations/data/ack-state-lock/perf-ab.sh
```
