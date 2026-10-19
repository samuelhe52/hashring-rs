# State write-lock profile

`HASHRING_PROFILE_WRITES=1` makes every node time its `Arc<RwLock<NodeState>>`
acquisitions. `diagnostics::write` records, per write path, the count of
acquisitions, cumulative wait and hold nanos, and the maximum of each. The
counters are cumulative per process, reset on restart, and surfaced through
`GetProcessInfo` into the experiment's `node_pressure_sampled` event. Timed paths
skip clock reads entirely when the variable is unset, so an unprofiled run pays
only the two extra atomics guarding the `OnceLock` read.

## Measurement

Correctness run, 20,000 keys, concurrency 64, RF=3, `OwnerOnly`, 128-byte values,
9 nodes. Sample taken 2 ms after `initial_put_complete`; the initial load itself
took 478 ms (41.8k keys/s). Aggregated over all 9 nodes:

| write path | acquisitions | mean wait | mean hold | wait/hold | total wait | total hold |
| --- | --- | --- | --- | --- | --- | --- |
| owner (`Put`/`Delete`) | 20,000 | 24.98 us | 11.88 us | 2.10 | 0.500 s | 0.238 s |
| follower (`ReplicateMutations`) | 31,140 | 36.53 us | 10.87 us | 3.36 | 1.138 s | 0.338 s |
| replication ACK writeback | 31,140 | 31.17 us | 1.95 us | **15.95** | 0.971 s | 0.061 s |

Worst single waits were 5.71 ms (owner) and 9.91 ms (follower/ACK), all on the
same node, consistent with a FIFO writer queue draining behind one long hold.

## Reading

Three things follow, and they do not all point the same way.

- **The lock is busy, not saturated.** Owner and follower holds total 0.576 s
  across 9 nodes over a 478 ms window. Per node that is ~13,100 acquisitions at a
  ~7.7 us mean hold, a duty cycle near 31%. Real contention, real headroom, but
  no queue collapse. Little's law on the observed means puts the mean writer
  queue at ~0.6 deep.
- **The ACK writeback is the worst ratio and the cheapest payload.** It holds the
  lock for 1.95 us to mutate three maps — a few dozen instructions of real work —
  yet waits 31.17 us, because it queues behind owner and follower writers it does
  not share any invariant with. It also fires more often than the owner path
  (31,140 acquisitions to 20,000). This is the clearest case in the profile for
  moving state off the shared lock.
- **Summing wait overstates it.** The README's warning that wait totals overlap
  across tasks and nodes is load-bearing: 0.971 s of summed ACK wait is ~0.108 s
  per node against a 478 ms window, and those waits queue rather than run
  concurrently. Per-operation means are the trustworthy quantity here.

## Consequences

`ack_process_instances`, `ack_progress`, and `owner_stream_unacked` have no
invariant shared with `records` or `dedup`, which is what makes them candidates
to relocate. Two caveats found while reading the call sites:

- `prune_ack_progress` reads `dedup` to build the live-receipt set and mutates
  `ack_progress` and `ack_process_instances` together. It is reached from
  `purge_expired_dedup`, which runs under the `NodeState` write lock on the put
  and follower-apply paths. If the ACK maps move behind their own lock, the
  acquisition order there is `NodeState` -> `AckState`, and that order must hold
  everywhere or the two locks can deadlock.
- `prepare_replication_entries` also registers `ack_progress` entries, under the
  same `NodeState` write lock, so it inherits that same order.

Directions 1 and 2 were later implemented, measured and rejected in
[ack-state-lock.md](ack-state-lock.md). The ACK split regressed `FirstSuccessor`.
The epoch atomic made ACK writebacks wait longer. That work also timed a second
write lock per ACK-waiting write. Turning it into a read
([lazy-ack-completion.md](lazy-ack-completion.md)) shortened lock queues but
did not change throughput.

Direction 2 (cache the topology epoch in an `Arc<AtomicU64>` so the delivery loop
stops taking a read lock per retry) is consistent with this order and strictly
removes work; it is worth doing first because it cannot deadlock.

Direction 4 (sharding `records` by token) was not attempted. At a 31% duty cycle
the ceiling on any lock change is bounded, and sharding additionally forces
`next_sequence` per shard and cross-shard consistent reads for migration
snapshots. Revisit only if a profile after directions 1-3 still shows owner
holds dominating.

## Reproducing

```sh
HASHRING_PROFILE_WRITES=1 target/release/hashring-rs experiment \
  --mode correctness --output results/<dir>
```

Read `node_pressure_sampled` from `events.jsonl` and aggregate
`owner_write_timing`, `follower_write_timing`, and `ack_write_timing` across
nodes. Run against a clean tree (`--require-clean-source`) so the manifest
records a reproducible revision.

## Source

- `crates/hashring-node/src/diagnostics.rs`: `write`, `StateWriteTiming`,
  `TimedWriteGuard`.
- `crates/hashring-node/src/rpc.rs`: owner, follower-apply, and ACK call sites.
- `crates/hashring-experiment/src/lib.rs`: `state_write_timing_json` and the
  pressure sample.
- `README.md`: how to enable and how to read the counters.

See also [batched-replication-rpc.md](batched-replication-rpc.md) for why the
follower path's acquisition count is 31,140 rather than 40,000.
