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
- A retryable failure retries the whole batch, so a transient follower error can
  resend up to a batch of already-pending entries. The follower's dedup receipts and
  fingerprint checks make re-delivery idempotent.
- A batch is applied under a single write lock. The lock is held for the batch's
  duration rather than one entry, but the total work per entry is unchanged and the
  number of acquisitions drops.

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
retained-byte limits this change builds on.
