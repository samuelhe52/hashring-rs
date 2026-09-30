# Replication backpressure: entry count and byte budget

The owner replicates each mutation to its desired followers through separate ordered streams. A stream is identified by `(topology epoch, owner, follower)` and has one delivery worker. That worker sends one entry at a time and retries retryable failures before advancing to the next entry. This ordering lets the follower reject gaps and apply mutations in the owner's stream order.

## What is the backlog?

A follower's **backlog** is the work the owner has accepted for that follower but has not yet confirmed as applied. It includes entries waiting in the stream queue and the entry currently being sent or retried. The in-flight entry has already left the queue, so a stream can have 32 queued entries plus one in flight. If a follower is slow, unreachable, or repeatedly rejects an RPC with a retryable error, later entries wait behind the stalled entry. The backlog grows when the owner's arrival rate exceeds that stream's delivery rate.

This matters to writes because the owner must reserve replication capacity *before* applying a new PUT or DELETE. It reserves space in every target follower's stream and in the owner's retained-byte budget. If any reservation fails, the owner rolls back the tentative stream sequence and returns retryable replication backpressure without applying the mutation. An ACK still requires the followers specified by the write policy to apply the mutation; placing it in a queue is never an ACK.

## The two limits

| Limit | Scope | What it bounds |
| --- | --- | --- |
| 32 queued entries | One `(epoch, owner, follower)` stream | The number of entries waiting behind that stream's active delivery. |
| 64 MiB of retained mutation data | All replication streams on one owner node | The estimated bytes retained for mutations awaiting follower delivery. |

The owner charges a mutation's key, value, identifiers, and a fixed overhead estimate against the byte budget. Entries sent to multiple followers share that one byte reservation. It stays held until every follower entry using it is delivered or dropped. The byte limit is an accounting bound for this replication work, not an exact cap on the process's total memory. A separate 64 MiB semaphore limits bytes in active replication RPCs; it does not replace the retained-byte or queue limits.

Both reservations must succeed. Many small mutations can fill one stream's 32 slots while the 64 MiB budget remains mostly free. Conversely, large mutations across several streams can exhaust 64 MiB while no individual queue reaches 32. A stalled entry also consumes retained bytes while it is being retried, even though it no longer occupies a queue slot.

## Why keep a per-stream count limit?

Using only the aggregate byte budget would still bound retained data, but it would allow one slow stream to accumulate many small entries. Those entries would increase the amount of ordered work waiting behind its first stalled entry. The per-stream limit bounds that waiting count and applies backpressure to writes targeting that follower sooner. It also limits how much queueing a short follower slowdown can create before the owner asks clients to retry.

The count limit is **not** a fairness guarantee. A stream with large entries can still occupy most or all of the shared byte budget and affect other streams. Nor does a larger queue increase delivery parallelism: the worker still processes its stream in order. Raising the limit from 8 to 32 trades more burst tolerance for a larger possible backlog and more retained work. In the [measured million-write run](../../results/performance-1m-e809ce4/summary.json) after the change, the highest observed per-stream pending count was 26 and no replication reservations were rejected; that is evidence for this workload, not a general sizing guarantee. The 8-to-32 change is a capacity choice, not a system optimization by itself.

These numbers are implementation choices. [RFC 0001](../rfcs/0001-ha-for-consistent-hash-cache.md) specifies ordered owner-to-follower streams, applied-state ACKs, and replication backpressure, but does not prescribe a queue depth or this two-limit rationale.
