# Replica ACK throughput and activation

## Targets and previous bottlenecks

For the local 10-node, RF=3, 128-byte-value workload at concurrency 64, the target is at least 10,000 acknowledged writes/s. The 20k-key correctness workload should normally finish scale-out within 10 seconds, with a 30-second hard cap. A longer logical operation deadline does not satisfy these targets.

Previously, every FirstSuccessor or AllReplicas write called `GetReplicaStatus`. That diagnostic endpoint polls owner/follower streams across the cluster and builds a status record for every range. The 10-node topology has 1,280 ranges and up to 90 directed owner/follower pairs. The FirstSuccessor 20k performance run took 48.25 seconds for initial PUTs; the attempted 1M run was interrupted after roughly 23 minutes without finishing initial writes. OwnerOnly skips this query under the default availability guard.

Activation seeded replica streams using per-range coordinator transactions for Copying, Verifying, and Complete. Each transaction cloned and persisted the complete cluster state. The earlier FirstSuccessor scale-out took 72.6 seconds and caused concurrent operations to exceed their 30-second deadlines.

## Changes and safety boundaries

The owner lease now carries admitted follower identities for the current topology epoch. A follower is included only when every interval requiring that follower under the selected ACK policy has a durable admission matching the registered follower process. Owners discard these identities on epoch changes. This permits default-guard writes to select required followers locally, without calling the diagnostic status endpoint.

Replication responses carry the responding process identity. Before reporting write success, the owner checks that each required sequence has been acknowledged by the admitted process and that its own epoch and lease remain valid. Sequence progress is reset to the response's sequence when the responder identity changes, so an old process's high watermark cannot be attributed to a new process. Missing admissions fail before applying a new mutation. Missing or mismatched ACKs after application preserve an uncertain outcome and the retry receipt. A completed receipt still records success for subsequent retries within the existing window.

The local gate also checks the age of the oldest unacknowledged entry against the configured lag bound. Successful writes require an applied ACK from the selected follower; they do not depend on a separate pre-write probe of that follower's lease. Explicit minimum-copy or minimum-healthy-follower guards retain the existing live status check and are outside the optimized performance claim. The policies still require the exact first successor or every desired follower, respectively; they never substitute an arbitrary responding subset.

Repair phase updates and final admissions are persisted atomically per complete owner/follower stream group. Snapshot transfer, final write fences, digest verification, checkpoint identity checks, and durability before admission remain in place. A failed batch changes neither the in-memory state nor the durable state. Incomplete repairs remain resumable after restart.

Coordinator migration and repair RPCs now share reconnecting HTTP/2 channels by endpoint, including channels for pending members. Each RPC retains its original deadline. This avoids repeatedly opening connections for every range and protocol step. The intermediate build without channel reuse reproduced a transport error during activation twice; connection churn is a suspected contributor, not a proven operating-system diagnosis.

## Validation

Focused tests cover policy-required admission coverage, stale process identities, mismatched ACK identities, local lag rejection, and all-or-nothing admission batches. Existing process tests cover policy transitions, follower failure, fencing, coordinator restart, and recovery during migration.

Preliminary runs before the final local lag check:

- `results/firstsucc-local-admission-1m/`: all 1M writes and reads passed; initial PUT 64.045 seconds (15,614 writes/s); no receipt/queue rejections or replication RPC retries at the post-write snapshot.
- `results/firstsucc-shared-channels-20k/`: the 30-second-deadline correctness profile passed; scale-out with concurrent mutations completed in 8.224 seconds.

The final admission rule requires coverage only on intervals where that follower is required by the selected ACK policy. Initially requiring every interval of an owner/follower stream unnecessarily delayed FirstSuccessor scale-in for optional second-follower repairs (14.1 seconds). Requiring every *policy-required* interval reduced the repeated scale-in times to 0.598 and 0.572 seconds without changing the successor requirement. The focused admission test explicitly removes optional admissions, then required coverage, then changes the process identity to check these distinctions.

All runs below use the same 10-node configuration, RF=3, 128-byte values, concurrency 64, 128 virtual nodes per node, and 30-second logical deadlines. Correctness starts with nine nodes, adds the tenth, and removes it again. The 20k runs verify all values and expected deletions after each transition. They include 1,401 concurrent mutations of moving keys and 700 untouched moving sentinels.

| Policy / build | Dataset | Initial PUT | Scale-out with mutations | Scale-in with mutations | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| FirstSuccessor, final policy coverage | 1M | 17,625 writes/s | — | — | All writes and reads passed |
| AllReplicas | 1M | 17,495 writes/s | — | — | All writes and reads passed |
| FirstSuccessor, final policy coverage A | 20k | 18,465 writes/s | 8.320 s | 0.598 s | Pass |
| FirstSuccessor, final policy coverage B | 20k | 19,359 writes/s | 8.337 s | 0.572 s | Pass |
| AllReplicas, final control limit | 20k | 17,945 writes/s | 8.301 s | 7.574 s | Pass |
| OwnerOnly, final code | 20k | 15,566 writes/s | 3.642 s | 0.599 s | Pass |

The 1M artifacts are `results/ack-policy-coverage-first-successor-1m/` and `results/ack-final-all-replicas-1m/`. The preceding FirstSuccessor 1M run with the broader admission rule also passed at 13,691 writes/s (`results/ack-final-first-successor-1m/`). The final transition artifacts are `results/ack-policy-coverage-{first-successor-20k-a,first-successor-20k-b,all-replicas-20k,owner-only-20k}/`. The AllReplicas 1M build predates the FirstSuccessor-only coverage correction and restoration of the existing 64 MiB migration control-message limit; neither difference changes AllReplicas checks or message sizes exercised by this dataset. Two earlier AllReplicas correctness repeats took 8.079 and 8.110 seconds for scale-out. The initial write rates vary between runs; these measurements establish target attainment on this machine, not a ranking between policies or a general hardware-independent guarantee.

The preliminary 1M post-write snapshots above recorded no owner/follower receipt rejections, queue reservation rejections, or replication RPC retries. The optimized path keeps the existing 60-second retry window and required applied ACKs. Full desired RF convergence is a separate metric: after scale-in it took 13.34 seconds for FirstSuccessor and 13.19 seconds for OwnerOnly, while AllReplicas already waits for all its required copies. OwnerOnly scale-out reached full RF after 13.49 seconds even though the membership/mutation stage completed in 3.64 seconds. No 1M-key migration has been measured in this change.

The workspace suite passed its unit tests and 26 of 27 process tests on the first run. The remaining artifact test detected a build/checkout source-hash mismatch caused by editing a test during the run; rebuilding and rerunning it with a stable checkout passed. After the policy-coverage correction, all coordinator/node unit tests and strict workspace Clippy passed again. Focused real-process FirstSuccessor failover and AllReplicas under-replication/policy-abort tests also passed on the final code. The preliminary local artifacts are ignored by Git, and their working-tree manifests do not represent a clean committed source revision.

The existing receipt-layout and completed-ACK cleanup measurements are documented separately in [retry-window memory](retry-window-memory.md).

## Repeated measurements on committed source

Both follow-up series used clean commit `89d23fac83287c3cba49dfb9760c9c3ffda4d353` on `fix/write-pressure-capacity`, source-tree BLAKE3 `9ddb91224737ae3837e3b4f9b0a9ee7f88c9f07ef65e30eb9c142775bf3eaca0`, and release executable BLAKE3 `0d8dee90a939940632d2bf6d1c918f81763053651a497b83c43eba7a5b917473`. Every manifest reports `source_reproducible: true`. Runs were sequential, with a fresh local cluster per run. The raw manifests, event logs, process logs, coordinator databases, exact runner scripts, and machine-readable audits are retained in the Git-ignored result directories named below. The commit makes the source reproducible; the raw results remain local unless separately archived.

### One-million-key write policy comparison

Nine performance runs used 10 nodes, RF=3, one million keys, 128-byte values, concurrency 64, seed 1, 128 virtual nodes, default availability guards, and the 30-second logical operation deadline. The only configuration differences between manifests were ACK policy and output directory. Policy order rotated across rounds: OwnerOnly / FirstSuccessor / AllReplicas; FirstSuccessor / AllReplicas / OwnerOnly; AllReplicas / OwnerOnly / FirstSuccessor. All nine runs passed one million writes and complete read verification.

| Policy | Round 1 writes/s | Round 2 writes/s | Round 3 writes/s | Median writes/s | Queue reservation rejections after PUT, by run |
| --- | ---: | ---: | ---: | ---: | --- |
| OwnerOnly | 12,938 | 18,563 | 14,423 | 14,423 | 83, 0, 1 |
| FirstSuccessor | 18,016 | 18,030 | 13,672 | 18,016 | 5, 0, 0 |
| AllReplicas | 16,995 | 17,990 | 14,255 | 16,995 | 0, 0, 0 |

Every observed write rate exceeded 10,000/s, including the slowest run of each policy. OwnerOnly produced both the slowest and the fastest observation. The ranges overlap, and all three policies slowed relative to round 2 in the final round; these nine runs do not establish a performance ranking or explain the variation. Queue rejections alone cannot explain it: OwnerOnly's third run had only one, while FirstSuccessor and AllReplicas also slowed. No run recorded receipt rejections or replication RPC retries at its post-write snapshot. Host thermal state, scheduler contention, allocator activity, and replication backlog were not profiled. The experiment measured initial writes and read verification, not migration. Full details: `results/policy-comparison-89d23fa/README.md` and `comparison.json`.

### FirstSuccessor 20k scale-out reliability

Twenty consecutive correctness runs used nine initial nodes, scaled out to ten and back, RF=3, FirstSuccessor, 20,000 keys, 128-byte values, concurrency 64, seed 1, 128 virtual nodes, default availability guards, and the 30-second logical operation deadline. Every run passed initial writes and reads, scale-out with concurrent mutations, verification after each membership change, scale-in, and final verification. The prior `Unavailable: transport error (outcome=MayHaveApplied)` did not recur.

| Stage | Minimum | Median | Maximum |
| --- | ---: | ---: | ---: |
| Initial 20k PUT | 1.000 s | 1.063 s | 1.162 s |
| Scale-out with mutations | 7.781 s | 7.958 s | 8.463 s |
| Scale-in with mutations | 0.568 s | 0.595 s | 0.616 s |

All observed scale-outs met the 10-second normal target and 30-second hard cap. The two transport failures were on an intermediate build before coordinator channel reuse; the first build with shared channels passed, and these twenty committed-build runs passed. This sequence supports channel reuse as a likely contributor, but does not identify the exact transport failure or prove it cannot recur. Under an independent, identical-run assumption, zero failures in 20 trials still permits a 13.9% one-sided 95% upper bound on failure probability; sequential runs on one host may be correlated. This is evidence for the 20k migration profile, not a one-million-key migration test. Full details: `results/first-successor-reliability-89d23fa/README.md` and `audit.json`.
