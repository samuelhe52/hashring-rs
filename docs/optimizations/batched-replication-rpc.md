# Batched replication RPCs

The owner replicates each mutation to its desired followers through separate ordered
streams. A stream is identified by `(topology epoch, owner, follower)` and has one
delivery worker. Before this change that worker sent exactly one `ReplicationEntry`
per `ReplicateMutation` RPC and waited for the follower's ACK before advancing. Each
mutation therefore cost one round trip per follower, and a burst of N mutations to
one follower cost N sequential RPCs.

## Change

Delivery now drains a contiguous run of queued entries for the stream and sends them
in a single `ReplicateMutations(ReplicationBatch)` RPC. The follower applies the
batch under one state write, in order, and reports the highest applied stream
sequence. The batch is bounded by both a count (`REPLICATION_BATCH_MAX_ENTRIES = 64`)
and a byte budget (`REPLICATION_BATCH_MAX_BYTES = MAX_DATA_MESSAGE_BYTES - 64 KiB`),
so one batch always fits the data-node message limit.

Entries that would exceed either bound are carried to the next batch instead of being
dropped, so per-stream ordering — and therefore the follower's existing gap
detection — is preserved.

## Why it is a system optimization

This is a real change to how the write path spends its RPC budget, not a tunable
value:

- **Fewer RPCs and round trips.** A run of K queued mutations becomes one RPC instead
  of K. Round-trip latency no longer accumulates per mutation on a busy stream.
- **Coalesced follower work.** The follower takes a single state write to apply a
  whole batch, amortizing lock acquisition, dedup purge, and ACK-progress bookkeeping
  across many mutations instead of redoing them per entry.
- **Fewer ACK wakeups.** One response advances the stream's ack progress to the last
  sequence in the batch, so required-ack waiters wake once per batch.

Ordering, idempotency, and backpressure semantics are unchanged: the per-entry
validation, fingerprint, dedup, and sequence checks are identical, applied in the
same order. The single-entry `ReplicateMutation` RPC remains for compatibility with
the existing tests and any single-entry callers.

## Tradeoffs

- A batch holds up to one batch-worth of entries off the per-stream channel while it
  is in flight or retrying. The stream's queue depth still bounds queued entries, but
  the effective in-flight backlog grows from one entry to one batch
  (`REPLICATION_BATCH_MAX_ENTRIES`). The backpressure test now asserts this wider but
  still bounded limit.
- A lost response can retry the whole batch. A received partial acknowledgement
  advances the owner and retries only the suffix. The follower's dedup receipts
  and fingerprint checks make re-delivery idempotent.
- A batch is applied under a single write lock. The lock is held for the batch's
  duration rather than one entry, but the total work per entry is unchanged and the
  number of acquisitions drops.

## Measured effect

Instrumenting the follower's `ReplicateMutations` handler to count received entries
and RPCs, over a correctness run of 20,000 keys at concurrency 64, RF=3,
`OwnerOnly`, 128-byte values, 9 nodes:

| | value |
| --- | --- |
| replication entries delivered | 40,000 |
| `ReplicateMutations` RPCs | 31,357 |
| **mean batch size** | **1.28** |
| peak pending per stream | 7–20 (queue capacity 32) |

The bounds are not the limiter: with 128-byte values each entry costs ~450 bytes, so
64 entries is ~29 KB against a 9 MiB budget. The limiter is that `try_recv` usually
finds the queue empty.

Mean batch size is governed by `1 + arrival_rate x loop_time`. This workload offers
~2,300 entries/s per stream and ~274 us per delivery-loop iteration, so ~0.6 entries
arrive during one iteration. Neither `REPLICATION_BATCH_MAX_ENTRIES` nor
`REPLICATION_BATCH_MAX_BYTES` changes that ratio; only a deliberate linger (paid for
in added latency) or more concurrent producers per stream would.

Consequence: the amortizations claimed above are real in mechanism but small in
magnitude here. Follower state-write acquisitions fell only from 40,000 to 31,357
(-21.6%), and ACK writebacks fell by the same 21.6%. At this workload's arrival rate
the change removes roughly one RPC in five, not one in sixty-four. Throughput here is
latency-bound per stream, not batch-bound, so this workload cannot show the
change's ceiling. Re-measure with higher per-stream concurrency before drawing
conclusions about the bound.

## Source

- `crates/hashring-core/proto/hashring.proto`: `ReplicationBatch`,
  `ReplicateMutationsResponse`, and the `ReplicateMutations` RPC.
- `crates/hashring-node/src/rpc.rs`: `apply_replication_entry` (shared per-entry
  logic) and the `replicate_mutations` handler.
- `crates/hashring-node/src/replication.rs`: `deliver_replication_stream` batch drain
  and single-RPC send.
- `crates/hashring-node/src/lib.rs`: `REPLICATION_BATCH_MAX_ENTRIES`,
  `REPLICATION_BATCH_MAX_BYTES`.
- Tests: `follower_applies_a_replication_batch_in_order`,
  `replication_batch_rejects_a_gap_between_entries` in
  `crates/hashring-node/src/tests/replication.rs`.

See also [replication-backpressure.md](replication-backpressure.md) for the queue and
retained-byte limits this change builds on, and
[state-write-lock-profile.md](state-write-lock-profile.md) for the measured wait and
hold times of the three write paths this change touches.

## Payload accounting and partial progress correction

Batch payloads are materialized only after connecting and acquiring the aggregate
RPC byte permits. The owned protobuf request is consumed by the RPC instead of
keeping a second request across retries. Pending entries continue to share their
original mutation and retained-byte reservation. The request future completes or
is dropped on timeout before the active permits are released. Transport and
allocator overhead still make this an accounting bound, not a process RSS cap.

An isolated reproduction on macOS with 32 unreachable follower streams sharing
one 8 MiB mutation measured 256,240 KiB of RSS growth before this correction,
while all 64 MiB of active RPC permits remained available. The same reproduction
afterwards measured 1,664 KiB. These are single-run measurements, not a general
memory-overhead guarantee. Run the process-wide RSS check alone:

```sh
cargo test -p hashring-node unreachable_streams_do_not_copy_unbudgeted_payloads -- --ignored --nocapture
```

A follower now returns the applied prefix if a later entry fails. The owner
publishes that acknowledgement, releases the corresponding pending entries, and
retries only the remaining suffix with a recalculated byte reservation. A failure
on the first remaining entry still follows the normal retryable/permanent status
handling. A batch must name one stream so its single acknowledgement is unambiguous.
The integration regression fills follower receipt capacity partway through a
batch, verifies the prefix reaches owner acknowledgement progress, then frees
capacity and verifies delivery of the suffix.
