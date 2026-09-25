# Prune ACK progress when retry records expire

## Change and reason

`ack_progress` tracks follower acknowledgments by topology epoch and follower. Entries for an older epoch can be removed once no live mutation retry record still needs them. Finding those live references requires scanning the retry-record map and its required ACK lists.

Previously, every call to `purge_expired_dedup` performed that scan, even when no retry record had expired. The mutation paths call expiry cleanup frequently, so sustained writes repeatedly scanned the same live records. [The implementation](../../crates/hashring-node/src/dedup.rs) now marks ACK progress for pruning only when a retry record is removed. It runs the scan at most once per second while that mark is set. Topology refresh still prunes after an epoch change ([replication cleanup](../../crates/hashring-node/src/replication.rs), [topology refresh](../../crates/hashring-node/src/service.rs)).

This changes the usual mutation path from a scan over live retry records to a check of the expiry heap and a flag. It does not change when retry records expire or which acknowledgments are required for a write.

## Evidence and limits

- The [regression test](../../crates/hashring-node/src/tests/replication.rs) verifies that an old epoch's ACK progress remains until the deferred prune and is eventually removed.
- The 1M-key performance run after this change passed at commit `3c58c54`: [summary](../../results/performance-1m-3c58c54/summary.json). The final run at `e809ce4` also passed: [summary](../../results/performance-1m-e809ce4/summary.json).
- Neither run isolates the cost saved by this change. The first commit also raised the retry-window capacity, and the final run includes further changes. These results establish workload completion, not an attributable throughput gain from pruning.

## Tradeoff

After retry records expire, ACK progress for an old epoch can remain for up to roughly one second, and until the next expiry-cleanup call after that interval. This retains some small, stale bookkeeping temporarily in exchange for avoiding repeated scans. The current epoch's ACK progress is retained by the pruning rule regardless of this schedule.
