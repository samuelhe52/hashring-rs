# Wait for receipt store capacity before retrying

## Change and reason

An owner retains deduplication receipts for the 60-second receipt retention period. When the receipt store reaches its byte budget, another write cannot safely acquire a receipt, so the owner rejects it before applying the mutation. Repeating the request immediately cannot create capacity while the oldest receipts are still live.

When the owner's receipt store is full, [the node](../../crates/hashring-node/src/service.rs) returns `retry_after_millis` based on the earliest receipt expiry ([calculation](../../crates/hashring-node/src/dedup.rs)). [The client](../../crates/hashring-client/src/lib.rs) waits for at least that hint on retryable `ResourceExhausted`, subject to its existing logical operation deadline. This aims to avoid requests that are unlikely to succeed before capacity returns. It does not extend the deadline or change the known-not-applied result when the deadline expires.

The hint is sent for **owner receipt store capacity rejection**. Follower receipt store capacity rejection and replication reservation backpressure do not currently supply this timing hint. Other `ResourceExhausted` errors continue to use the client's ordinary backoff when the hint is zero.

## Evidence and limits

- The [owner capacity test](../../crates/hashring-node/src/tests/migration.rs) checks that a rejected write has a positive hint and remains unapplied. The [client test](../../crates/hashring-client/src/lib.rs) checks that a hint longer than the remaining deadline yields a deadline error with `KnownNotApplied`.
- In the instrumented 1M-key run at `b4f6c41`, one node recorded 73 owner receipt store capacity rejections and the run completed: [summary](../../results/performance-1m-b4f6c41/summary.json). This shows the client can recover from saturation in that workload; the summary does not count hint-driven waits or establish how many retries they saved.
- The final 1M-key run at `e809ce4` recorded zero owner receipt store capacity rejections: [summary](../../results/performance-1m-e809ce4/summary.json). It therefore does not measure the hint's effect.

## Tradeoff

The hint is an estimate based on the earliest retained expiry, not a reservation of future capacity. Other writes can use capacity first; the client may still retry and be rejected. Waiting longer can reduce request pressure but also consume more of the caller's operation deadline, so a short-deadline operation may fail without another attempt. The separate receipt store capacity increase on this branch is a sizing choice, not this optimization.
