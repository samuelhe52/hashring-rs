# Replication batch linger

[batched-replication-rpc.md](batched-replication-rpc.md) let one
`ReplicateMutations` RPC carry up to 64 entries, but a stream only batches what
queued while its previous RPC was in flight. On one host with 6 nodes,
replication batches averaged **2.2 entries**, so at RF=3 every client write cost
about 1.9 RPCs for the nodes to serve. Almost all of an RPC's cost is in the
transport, not the handler ([grpc-io-bottleneck.md](grpc-io-bottleneck.md)), so
that RPC count, not the hashring code, set the RF=3 write ceiling.

## Change

A follower stream that has entries to send now waits up to
`replication_batch_linger_ms` (default **2 ms**) for more before it sends the
batch, unless some write is waiting for that follower's ACK:

- Each queued entry records whether its write waits for this follower's ACK
  (`PreparedReplication::ack_awaited`, set from the owner's ready followers when
  the mutation is prepared). Under `OwnerOnly` no entry does. Under
  `FirstSuccessor` only the first follower's do.
- `collect_replication_batch` drains whatever is already queued, as before. If
  the batch is not full and no entry in it is awaited, it keeps receiving until
  the linger deadline. An awaited entry ends the wait at once, so a write that
  waits for ACKs is never delayed by lingering.
- Count and byte bounds, the carry-over entry, ordering and retry behaviour are
  unchanged. Entries received while lingering leave the stream's queue, so the
  queue capacity keeps applying to entries not yet taken into a batch, as
  before. The in-flight backlog is still at most one batch per stream.

`replication_batch_linger_ms: 0` restores immediate sending. tokio timers have
1 ms resolution, so a 1 ms linger waits 1-2 ms.

## Measurement

Same host and workload as [grpc-io-bottleneck.md](grpc-io-bottleneck.md):
Apple M1 Pro, 8 CPUs, performance mode, 6 nodes, 300,000 keys, 128-byte values,
RF=3, 2 workers per process, `HASHRING_PROFILE_RPC=1`. Three runs per
configuration, rep-major order, medians. The host was quieter than in the
diagnosis runs: three L0 runs at concurrency 64 spanned 35.4k-39.8k PUT/s.
Raw data: [phase-c-linger-runs.tsv](data/grpc-io-bottleneck/phase-c-linger-runs.tsv).

| policy, concurrency | linger | PUT ops/s | change | entries per batch | replication RPCs | client `Put` RPC mean | owner handler mean |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `OwnerOnly`, 64 | 0 | 37,246 | | 2.2 | 276,634 | 1,586 µs | 203 µs |
| | 1 | 47,084 | +26% | 7.2 | 83,135 | 1,172 µs | 85 µs |
| | **2** | **50,273** | **+35%** | 11.1 | 54,198 | 1,089 µs | 87 µs |
| | 5 | 48,478 | +30% | 21.1 | 28,432 | 1,119 µs | 81 µs |
| `OwnerOnly`, 256 | 0 | 47,888 | | 3.7 | 163,759 | 4,099 µs | 454 µs |
| | **2** | **62,111** | **+30%** | 14.7 | 40,832 | 2,985 µs | 250 µs |
| | 5 | 68,505 | +43% | 29.2 | 20,562 | 2,552 µs | 223 µs |
| `FirstSuccessor`, 64 | 0 | 30,079 | | 1.7 | 359,606 | 2,042 µs | 1,039 µs |
| | **2** | **35,928** | **+19%** | 2.5 | 235,311 | 1,688 µs | 704 µs |
| RF=1, 64 (no replication) | | 71,466 | | | | 704 µs | 21 µs |

- **Fewer RPCs.** A 2 ms linger cuts replication RPCs 4-5x under `OwnerOnly`.
- **More throughput, lower client latency.** Followers wait, yet the client's
  `Put` RPC gets faster (1,586 -> 1,089 µs at concurrency 64). With fewer
  replication RPCs, the nodes' two workers spend less time on replication
  traffic and owner handlers finish sooner (203 -> 87 µs). This confirms that
  per-RPC transport cost was the limit.
- **ACK-waiting writes get faster too.** Under `FirstSuccessor` the first
  follower's stream never lingers, but the second follower's does. That removes
  a third of all replication RPCs, and the owner handler, which includes
  waiting for the ACK, drops from 1,039 to 704 µs.
- **Fewer rejections.** Stream-queue reservation rejections fall about 8x
  (706 -> 89 at concurrency 64), because lingering moves entries
  out of the queue sooner.
- **Time to full RF does not change.** It is 6.2 s in every configuration
  (`initial_full_rf_seconds`), since milliseconds of linger are small against
  it.

The default is 2 ms rather than 5. In this sweep 5 ms was no better at
concurrency 64 and 10% better at concurrency 256. Repeats at 256 (below) put the
typical gain at about 6%, so the smaller value, which keeps the added
`OwnerOnly` loss window (below) short, gives up little.

## Why a longer linger adds little

Going from 2 ms to 5 ms halves the replication RPCs again, so a follow-up
tested whether the driver was hiding the gain. Concurrency 256, three runs
each, under heavier background load (one disk-image service and `logd` busy,
so the first repetition was slow in every configuration). A temporary build
gave only the driver 4 workers and spawned writer tasks, while nodes kept
2 workers. Raw data:
[phase-d-driver-runs.tsv](data/grpc-io-bottleneck/phase-d-driver-runs.tsv).

| PUT ops/s, median | linger 0 | 2 ms | 5 ms | RF=1 |
| --- | --- | --- | --- | --- |
| driver 2 workers (default) | 47,398 | 64,202 | 56,062 | 92,005 |
| driver 4 workers, spawned tasks | 46,383 | 69,793 | 65,918 | 116,363 |

Comparing within each repetition, 5 ms against 2 ms was -4%, -29% and +3% with
the default driver, and -27%, +3% and +8% with the faster one: no gain that
stands out from the noise either way. 0 to 2 ms was +29% to +41% with the
default driver. Across all nine paired comparisons of 5 ms against 2 ms at
concurrency 256 with the default driver (this run, the sweep above, and the lock
profile below), the median gain is **+6.5%**, with a range of -29% to +30%.

- **The driver is not the RF=3 limit.** It does cap RF=1: at concurrency 256 the
  default driver runs both workers at 1.94 cores, and giving it more lifts RF=1
  from 92k to 116k. RF=3 stays well below either ceiling, and a faster driver
  does not widen the 2 to 5 ms gap. (Comparing RF=3 at concurrency 256 with RF=1
  at concurrency 64, as an earlier version of this note did, uses a ceiling that
  is itself latency-bound. At equal concurrency and the default 2 ms, RF=3 is
  about 30% below RF=1.)
- **Halving a small share of RPCs gains little.** With about 300k client `Put`
  RPCs per run, replication was 35% of the RPCs that nodes serve at linger 0,
  12% at 2 ms and 6% at 5 ms. By Amdahl's law, halving the replication RPCs at
  2 ms can speed the RPC load up by at most about 6%. Removing them entirely
  would give at most about 14%.
- **The write lock is not the remaining limit.** A third run added
  `HASHRING_PROFILE_WRITES=1`. Same workload and default driver, Time Machine
  busy throughout, so throughput is noisy but the lock counters agree across
  repetitions. Raw data:
  [phase-e-lock-runs.tsv](data/grpc-io-bottleneck/phase-e-lock-runs.tsv).

  | linger | PUT ops/s | owner lock wait per write | follower lock hold, total | per batch | per entry | owner `Put` handler |
  | --- | --- | --- | --- | --- | --- | --- |
  | 0 | 52,777 | 73 µs | 3.13 s | 22 µs | 5.2 µs | 456 µs |
  | 2 ms | 64,565 | 43 µs | 2.75 s | 72 µs | 4.6 µs | 280 µs |
  | 5 ms | 74,943 | 35 µs | 2.34 s | 127 µs | 3.9 µs | 230 µs |
  | RF=1 | 91,704 | 12 µs | | | | 72 µs |

  Each follower batch holds the lock longer as batches grow, but total hold
  time falls, because per-entry cost shrinks with batching, and owner writes
  wait *less* for the lock. At 5 ms the lock accounts for about 23 µs of the
  158 µs gap between the RF=3 and RF=1 owner handlers. The rest is time the
  handler spends outside the write lock, most likely waiting for one of the
  node's two workers, which also serve replication sends and follower applies,
  or for the state read lock. Pinning that down needs a scheduling profile.

## Tradeoffs

- **`OwnerOnly` loss window.** A write acknowledged by the owner alone may now
  wait up to the linger before its first follower copy is sent. An owner crash
  in that interval can lose writes that would otherwise already have been on
  their way to a follower.
  Policies that wait for follower ACKs are unaffected for the followers they
  wait on.
- **Follower freshness.** Followers may lag by up to the linger more, which
  is far below the `max_replica_lag_ms` default of 5,000.
- **A timer per lingering batch.** That is cheap next to the RPCs it saves.

## Verification

- Unit tests `batch_linger_collects_entries_that_arrive_while_waiting` and
  `batch_linger_never_delays_an_acknowledged_write`
  (`crates/hashring-node/src/tests/replication.rs`) run `collect_replication_batch`
  on tokio's paused clock: linger collects late entries and ends at its
  deadline, and an awaited entry, first or arriving late, ends the wait
  immediately.
- The full workspace suite passes, and with the 2 ms default one correctness-mode
  and one availability-mode run each completed successfully.

## Reproducing

```sh
cargo build --release
for linger in 0 2; do for rep in 1 2 3; do
  HASHRING_PROFILE_RPC=1 scripts/perf/run.sh results/linger L$linger-c64 $rep \
    --keys 300000 --concurrency 64 --replication-batch-linger-ms $linger
done; done
python3 scripts/perf/collect.py results/linger
```
