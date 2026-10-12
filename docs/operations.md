# Operations

Details that the [README](../README.md) leaves out: how membership changes
recover from failures, what `replica-status` reports, and the internal timers
and budgets.

## Membership changes in depth

### Cutover

- The source stays authoritative while a point-in-time snapshot is copied.
  Independent ranges move concurrently, 16 at a time by default
  (`coordinator --range-move-concurrency N`, or the same `experiment` flag).
- Puts and deletes that arrive during the copy are replayed from a bounded
  changelog.
- At cutover, affected writes get a retryable `RangeBusy` while reads continue.
  The coordinator checks the live-record count, the contiguous watermark, and a
  BLAKE3 digest, then publishes the new epoch.
- During the read handoff, the frozen source and the committed destination hold
  the same data. Destination writes stay fenced until every source has
  installed the new topology.
- Deleted values are not kept as permanent tombstones.

### Leases and follower admission

- For planned cutovers, the coordinator withholds new-epoch leases until the
  required target followers are verified and admitted.
- Automatic failover may publish a degraded placement that still satisfies the
  write guard, then repair followers in the background.

### Stopping removed nodes

Removed nodes are stopped only after source cleanup, using the process-instance
identity captured during the migration. The sequence is:

1. The node acknowledges a prepared stop.
2. The coordinator persists an instance-specific stop confirmation.
3. The coordinator requests final shutdown.

The confirmation stays durable after the change completes. A node that briefly
loses contact with the coordinator still shuts down when it reconnects. A lost
final response can be recovered, and a transient outage is never mistaken for a
successful stop.

### Failures during a change

- **Coordinator restart.** The persisted transition and repair plan resume
  automatically.
- **Failure before publication.** Cleanup has its own deadline. The change
  stays in a recoverable `Aborting` state until the authoritative sources
  acknowledge cleanup. If a new joiner stays unavailable for 30 seconds before
  publication, the attempt aborts after source cleanup.
- **Failure after publication.** The epoch is never rolled back. If a required
  target node stays unavailable, the published transition can stay pending.
- **Single-node join where the new process dies before activation.** This can
  be recovered by publishing a further epoch with the original members, but
  only if every original owner is still the same live process and the frozen
  source ranges still hold their verified watermarks. The superseding change
  records the previous change ID and re-admits the required followers before it
  grants leases.
- **Loss of an original owner, or a more complex second failure.** The change
  stays blocked because there is no proof that the data is still covered.
  `change-status` and `replica-status` report why recovery is blocked. Restore
  the same live process if you can.

Coordinator consensus and failover are not provided.

## `replica-status` fields

Cluster level:

- committed epoch and topology digest
- desired RF and ACK policy
- active change phase

Per node:

- process identity
- lease expiry and last renewal
- suspicion and fencing state
- whether the node is joining

Per range:

- `current_rf`: the nominal replica count. **This alone does not prove the
  replicas are alive.**
- `live_rf`: the replicas that hold leases
- owner lease
- follower admission, lag, and health
- last known owner ACK sequence
- `writable`, and the reason when it is false
- under-replication, repair phase, retry count, next attempt time, and last
  error

While a cutover fence is in place, affected ranges are conservatively reported
as possibly blocked. A failed required repair stays visible and is never
treated as healthy.

## Timers and budgets

| Setting | Value |
| --- | --- |
| Lease duration / renewal interval | 5 s / 1 s |
| Failure detection interval | 1 s |
| Repair and recovery interval | 5 s |
| Repair groups per pass / concurrently | 32 / 4 |
| Repair retry backoff cap | 60 s (exponential, per group) |
| Pending replication buffer per node | 64 MiB |
| Deduplication receipt store per node | 128 MiB |
| Deduplication receipt retention | 60 s |
| Migration snapshot page | 8 MiB |
| Migration journal per node (default) | 16 MiB |
| Control-plane message limit | 64 MiB |
| Tokio workers: `node`, `coordinator` | one per available CPU (`node --worker-threads N` overrides) |
| Tokio workers: client and admin commands | single-threaded |

The cluster-size limits in the README (1,024 members, 4,096 virtual nodes per
member, 1,048,576 assignments, 16,384 moved ranges per change) keep topology
and change snapshots under the 64 MiB control-plane message limit.

## ACK policies and admission

With the default write guard, owners use stream admissions carried in their
leases and check the required applied ACKs against the admitted follower
process identities. Explicit copy or health guards still query live replica
status. See
[replica ACK performance](optimizations/replica-ack-performance.md).

Each deduplication receipt identifies one mutation and stores its key,
operation fingerprint, assigned version, delete flag, and expiry.
