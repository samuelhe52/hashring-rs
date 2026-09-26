# Receipt cleanup and request routing

## Problem and changes

The repeated one-million-key runs varied across ACK policies. Two concrete sources of avoidable work were identified while investigating that variation.

Receipt expiry invokes ACK-progress pruning up to once per second per node. The old implementation scanned every live receipt to find old-epoch ACK references even when every tracked stream belonged to the current epoch. Current-epoch streams must always survive pruning. The new fast path checks the much smaller ACK-progress and process-identity maps first and skips the receipt scan when no older stream exists. When an older stream exists, pruning still scans receipt requirements and preserves any stream needed by a retry. Receipt lifetime, expiry order, byte accounting, and retry results are unchanged.

Each client GET, PUT, and DELETE attempt previously called `topology()`, cloning the complete snapshot, including all token assignments and their node-ID strings, before selecting an owner. At 10 nodes with 128 virtual nodes each, this copies 1,280 assignments per attempt. Routing now reads the epoch and selected endpoint under one topology read lock, then releases that lock before network I/O. Explicit callers of the public `topology()` API still receive an owned snapshot. Retry handling continues to use the epoch that selected the endpoint. There is no change to ACK policy or stream ordering.

Source: [ACK pruning](../../crates/hashring-node/src/replication.rs), [client routing](../../crates/hashring-client/src/lib.rs), and [receipt expiry](../../crates/hashring-node/src/dedup.rs).

## Profiling

`HASHRING_PROFILE_WRITES=1` enables per-process cumulative wall-time counters, returned by node info and saved in the experiment's post-write pressure sample. With profiling disabled, these optional timing messages are absent and the experiment records null values; hot paths skip clock reads and timing-counter updates. Unit tests enable diagnostics automatically.

The counters cover the state write lock for owner PUT/DELETE application, follower replication application, and sender-side ACK processing. They do not cover read-lock waits, the separate final required-ACK completion lock, client CPU, transport queues, or all migration locks. Counts include successful lock acquisitions, and early-return paths still record hold duration. Wait sums can overlap across tasks and nodes. Hold and cleanup durations include descheduling; they are not CPU-time measurements.

Cleanup time includes expiry-heap work, receipt destruction, and any nested ACK-pruning scan. Report pruning as a component of cleanup, not additional time. These counters reveal avoidable work but cannot alone assign the entire throughput gap to locks or expiry.

## Evidence and provenance

The comparison uses fixed source snapshots based on `fd500a3`, with identical profiling instrumentation in baseline and optimized builds. Uncommitted experimental builds are identified by the build source-tree hash, binary hash, and preserved source archives rather than represented as clean committed builds. The cleanup-only snapshot differs in production behavior only by the pruning fast path. The routing snapshot adds the client endpoint-selection change to that snapshot. The final working tree also gates profiling behind the environment variable; comparison snapshots have profiling always enabled.

Artifacts, exact runner scripts, manifests, logs, source archives, and summaries are under `results/write-path-profile/` (Git-ignored). All performance runs use one million keys, 10 nodes, RF=3, 128-byte values, concurrency 64, seed 1, 128 virtual nodes, and the unchanged 30-second logical deadline. Each verifies all one million keys after loading. Runs execute sequentially, with fresh clusters and no concurrent compilation or test runs.

See [the prior policy comparison](replica-ack-performance.md) for the original throughput variation and [retry-window memory](retry-window-memory.md) for the receipt-layout changes that preceded this work.

### Repeated performance runs

All 14 runs in the initial comparison completed writes and full read verification. The OwnerOnly baseline and cleanup-only variants alternated for three trials each. Routing then ran three trials each for OwnerOnly and FirstSuccessor, alternating policies. FirstSuccessor has one baseline and one cleanup-only observation, so those entries are not repeated estimates.

| Build | Policy | Runs | PUT seconds, median (range) | GET verification seconds, median (range) |
| --- | --- | ---: | --- | --- |
| Instrumented baseline | OwnerOnly | 3 | 78.207 (69.885–83.259) | 55.978 (53.307–58.709) |
| Instrumented baseline | FirstSuccessor | 1 | 79.571 (79.571–79.571) | 52.268 (52.268–52.268) |
| Skip redundant ACK scan | OwnerOnly | 3 | 72.864 (72.544–79.949) | 52.852 (52.351–56.216) |
| Skip redundant ACK scan | FirstSuccessor | 1 | 67.127 (67.127–67.127) | 52.218 (52.218–52.218) |
| Also avoid topology copies | OwnerOnly | 3 | 54.540 (47.567–58.393) | 16.965 (16.586–21.763) |
| Also avoid topology copies | FirstSuccessor | 3 | 59.597 (53.704–64.397) | 19.153 (18.592–22.494) |

The initial OwnerOnly baseline scanned 51,899,055 receipt entries for ACK pruning. Summed across ten nodes, pruning took 1.858 seconds and total receipt cleanup took 4.992 seconds; the largest single cleanup took 213.872 ms. Across the three OwnerOnly baselines, pruning scanned 25.7–51.9 million receipts and took 0.619–1.858 seconds. All cleanup-only and routing runs scanned zero receipts for ACK pruning in this steady-epoch profile. The cleanup-only OwnerOnly median improved, but its range overlaps baseline; eliminating these scans does not establish that they caused the whole slow-run gap.

Avoiding full topology copies reduced OwnerOnly's median PUT duration from 72.864 to 54.540 seconds relative to cleanup-only (25.2% less time, 33.6% more writes/s), and median read-verification duration from 52.852 to 16.965 seconds (3.12 times the read rate). The routing cohort's OwnerOnly write range does not overlap the earlier cleanup-only range. The cohorts ran at different times on one local host, so these measurements establish a local improvement rather than a hardware-independent rate guarantee.

Faster routing exposed more backpressure: OwnerOnly routing runs recorded 275, 551, and 472 queue reservation rejections, and FirstSuccessor recorded 76, 13, and 63. These retryable rejections did not cause an experiment failure. The policy comparison still has overlapping ranges, so it does not establish an inherent throughput ranking. Per-stream replication remains serial and bounded; pipeline changes, queue tuning, and adaptive retry pacing were not part of this optimization.

A closing cleanup-only OwnerOnly control, run after the routing cohort, passed with PUT 74.558 seconds and read verification 52.510 seconds. It remained within the earlier cleanup-only range, supporting a routing benefit beyond a simple change in machine conditions over time. It is reported separately from the three-trial table.


### Final-build validation and build-cache caveat

An initial final-build check reused the shared Cargo target directory used by the frozen comparison worktrees. Although its manifest matched the current source digest, profiling counters remained enabled without the environment variable: Cargo had reused the earlier node library. The retained `final-owner-1m-default` and `final-first-20k-profiled` runs are excluded from final-source validation. The former passed at 51.143 seconds PUT and 17.342 seconds GET, but is not evidence for profiling-disabled behavior. A source digest alone does not prove that every linked dependency was rebuilt from that source.

Final validation therefore uses a fresh, isolated `target/write-path-final` build directory. The build log, final executable, and source archive are preserved alongside the experiment artifacts. For future comparisons across copied worktrees, use separate Cargo target directories per source snapshot, or explicitly invalidate the affected package artifacts before rebuilding.

Build identities (BLAKE3 prefixes; full hashes are retained in each manifest):

| Variant | Source digest | Executable digest |
| --- | --- | --- |
| Baseline | `05d1b0a713975892` | `0a136f78626f4750` |
| Cleanup only | `ec86195a615caf9e` | `e32b4fb3116b08db` |
| Cleanup and routing | `120aa5de8a56a32d` | `43642ab13c3bc1a0` |
| Final isolated build | `77961acb30a180b4` | `270653e911d0ded1` |

The fresh final build, with profiling disabled, passed all one million writes in **50.057 seconds (19,977 writes/s)** and all one million reads in **17.653 seconds**. All four optional timing fields were null for every node, as intended. The post-write sample contained 891 retryable queue reservation rejections, zero receipt-capacity rejections, and zero replication RPC retries. This is a single final-build check, separate from the repeated instrumented comparisons.

The same fresh executable, with `HASHRING_PROFILE_WRITES=1`, passed the FirstSuccessor correctness profile with 10 nodes and 20,000 initial keys. Initial PUT took 1.051 seconds; scale-out with concurrent mutations took **8.872 seconds**, and scale-in with mutations took **0.571 seconds**. Read verification passed after initial load and both transitions. Every node supplied the optional timing messages. This checks the final instrumentation and migration behavior; one run is not a new reliability-rate estimate.

Validation also passed 11 client and 35 node unit tests, including old-epoch ACK-reference retention and orphan identity cleanup; the process tests `online_scale_out_and_scale_in_preserve_concurrent_writes` and `moved_request_retries_a_transient_coordinator_outage_until_deadline`; workspace/all-target Clippy with warnings denied; formatting; and whitespace checks.

## Remaining work

The source audit and measurements support avoidable shared-lock cleanup work and client allocation/copying as actual bottlenecks. They do not prove a single cause for every earlier reversal of OwnerOnly and FirstSuccessor throughput. OwnerOnly still queues the same follower replication at RF3, while earlier responses allow it to apply more pressure. Optimized OwnerOnly and FirstSuccessor ranges still overlap. Queue rejection counts rose after faster routing, so bounded stream pipelining or better retry pacing remain candidates for a separate measured change. No queue size, timeout, ACK policy, receipt lifetime, or failover lease was changed here.
