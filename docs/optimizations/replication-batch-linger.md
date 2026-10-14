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

The default is 2 ms rather than 5. At concurrency 64, 2 ms already reaches the
top of the range (5 ms is not better), and the smaller value keeps the
added `OwnerOnly` loss window (below) short. At concurrency 256, 5 ms gains a further 10%
for operators who accept that.

RF=3 under `OwnerOnly` is still about 30% below RF=1 at concurrency 64. The
remaining gap is the replication RPCs that are left plus follower apply work.

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
