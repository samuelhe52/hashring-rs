# RFC 0001: HA for Consistent-Hash Cache

If you want the overview first, read [How the HA cache fits together](0001-ha-walkthrough.md). It is a 15-minute walkthrough with diagrams. This RFC is the detailed design contract, implementation plan, and acceptance criteria.

## Goal

Add range-level replication, configurable write acknowledgement, automatic node failover, and asynchronous replica repair to the in-memory consistent-hash cache. Keep classic consistent hashing; do not switch to fixed slots.

Clients keep routing requests themselves, and the coordinator stays thin:

- A client sends each data operation directly to the owner it computes from its cached copy of the committed topology.
- The coordinator serializes topology changes, confirms failures, issues leases, persists replica admission, and drives migration and repair.
- The committed ring alone determines each range's owner and its ordered followers.
- A range is the interval between two adjacent tokens in one topology. Ranges split and merge as membership changes.

This design replaces the earlier plan based on fixed ranges, RF=3, and acknowledgement from every copy.

## Baseline

State of `kona` at HEAD `1f7b25b`.

Already implemented:

- Classic consistent hashing, with multiple deterministic vnode tokens per physical node.
- `TopologySnapshot`, which holds the epoch, members, token assignments, hash configuration, and digest.
- Owner lookup, which picks the first token clockwise from the key.
- Membership migration, which works over the union of old and target token boundaries. Its work ranges therefore already split and merge.
- Online owner migration with snapshot, changelog, write pause, digest verification, topology publication, activation, and cleanup stages.
- Client `request_id` and topology epoch on every request, stale-topology refresh, a single deadline that spans all retries, and a distinct result for an unknown write outcome.

Not yet implemented:

- Steady-state replication, replica admission, failure detection, owner leases, promotion, RF repair, and write-ACK policy.
- Node-side `request_id` deduplication.

The existing migration `range_id` values identify tasks within one transition. They can stay, but they must not turn into permanent shard or vnode IDs.

## Architectural decisions

### 1. Ring, ranges, and placement

1. Keep classic consistent hashing with many virtual-node tokens per physical node.
2. Each token owns `(previous_token, token]`, wrapping around the ring.
3. Derived ranges have no permanent identity. Removing tokens merges adjacent ranges, and adding tokens splits them.
4. A range's desired replica set starts with the owner of its ending token. It continues clockwise through distinct physical nodes until it reaches the desired RF or runs out of members.
5. When picking followers, skip any token that belongs to a physical node already in the set.
6. The first successor plays two roles. It is the first desired follower, and it is the node the ring selects as owner if the current owner is removed.
7. There is no per-range placement table and no placement override.
8. The existing vnode layout spreads a failed owner's ranges across the survivors. Promotion never looks for the least-loaded node.

### 2. Committed topology contract

Owner authority comes only from the committed topology. It contains:

- `epoch`
- hash algorithm, seed, encoding version, and vnode count
- canonical members and token assignments
- `desired_replication_factor`, which is configurable
- the cluster-wide `write_ack_policy`
- write-availability guard settings
- a digest over all of the above

Write policies are named enums rather than numeric quorums:

| Policy | A write succeeds after |
| --- | --- |
| `OwnerOnly` (default) | the owner applies it |
| `FirstSuccessor` | the owner and the first successor apply it |
| `AllReplicas` | every node in the desired replica set applies it |

There is no `W=2`. An ACK from an arbitrary second replica does nothing for the node that will actually be promoted.

Changing the policy is a topology change:

- Strengthening the policy (`OwnerOnly` to `FirstSuccessor`, or `FirstSuccessor` to `AllReplicas`) waits until the newly required copies have caught up. Only then is the new epoch committed.
- Weakening the policy can take effect immediately, but only through a committed topology.
- Local node configuration cannot override the committed policy.
- Per-request overrides are out of scope.

### 3. Desired placement versus operational readiness

Where a range should live and whether a copy is ready are tracked separately:

- The **desired** replica set comes only from the committed ring and the desired RF.
- The **admitted** replica set holds copies whose initial data and contiguous mutation history the coordinator has verified.
- A **healthy** follower is admitted, connected, and within the configured replication-lag bound.

The coordinator stores each admission durably, keyed by:

- topology epoch
- exact derived range bounds
- node ID
- the verified snapshot/changelog watermark, or an equivalent proof of coverage

Admission does not advance the topology epoch, and clients never see it. It decides ACK eligibility, promotion eligibility, health reporting, and repair.

When a topology change merges ranges, a successor is admitted for the merged range only if the coordinator can prove it covers every constituent interval. When a range splits, an existing admission carries over only to the subranges it actually covers.

### 4. Replication factor and degraded operation

The desired RF is configurable. RF=3 is the usual example, not a constant in the code.

The desired RF can exceed the number of live physical nodes. In that case the range is under-replicated, and repair stays pending until enough nodes exist.

Guards decide whether a write may proceed at all. By default, an `OwnerOnly` write needs only a leased owner. Operators who care more about losing data than about write availability can configure stricter guards.

Examples with desired RF=3:

| Admitted copies | `OwnerOnly` | `FirstSuccessor` | `AllReplicas` |
| --- | --- | --- | --- |
| 2 | Writes if the guards pass | Writes if the admitted, healthy follower is the first successor and it ACKs | Rejects writes |
| 1 | Writes under the default guards and a valid lease; stricter guards may reject | Rejects writes | Rejects writes |

`desired_rf=1` with `FirstSuccessor` is an invalid configuration. RF=1 with `OwnerOnly` accepts writes under the default guards, but nothing survives the loss of the owner.

Repair always targets the next missing distinct physical node clockwise. If no eligible node exists, the range stays under-replicated and status output shows it.

Initial defaults:

- `minimum_admitted_copies = 1`
- `minimum_healthy_followers = 0`
- `max_replica_lag = 5s` (configurable; process tests must confirm the value is sensible)

### 5. Consistency contract

All data lives in memory. An ACK means a mutation was applied in memory, not written to disk.

**`OwnerOnly`**

- Success means the owner applied the mutation.
- Followers receive it asynchronously.
- If the owner fails, recently acknowledged writes can be lost.
- After failover, clients may see older state.
- Per-key linearizability holds within one owner epoch, not across a failover.

**`FirstSuccessor`**

- The owner orders the mutation and applies it.
- Before the owner ACKs, the first successor checks authority and sequence, then applies the mutation.
- Starting from a healthy state, an acknowledged write survives the failure of any one member.
- The second and later followers are still updated asynchronously.
- After a promotion, reads can resume once the old authority is fenced and the new topology is committed. Writes cannot succeed until the new first successor has caught up and been admitted.

**`AllReplicas`**

- Every node in the full desired replica set must apply the mutation before it succeeds.
- While the range is under-replicated, writes fail with a retryable unavailable error.
- "All" never means "all the copies that are currently admitted."

Followers do not serve normal client reads. A follower ACKs only after it has checked the epoch, owner, stream order, and payload, and applied the mutation to its replica. Receiving or queueing a mutation is not enough.

## Data-plane design

### 1. Mutation ordering and replication streams

Record versions keep their current shape:

```text
(topology_epoch, owner_node_id, owner_sequence)
```

- `owner_sequence` increases monotonically on the physical owner.
- Each `(epoch, owner, follower)` pair has its own contiguous `stream_sequence`.
- A replication entry carries the stream sequence, key/token, operation, value or tombstone, record version, and mutation ID.
- One stream can carry mutations for every derived range in which that follower is a desired replica.
- A follower rejects gaps, stale epochs, wrong owners, conflicting duplicates, and mutations for ranges it does not cover.
- On a gap, the follower catches up from retained history or from a snapshot. It never skips the gap.
- A new topology epoch starts new streams and new readiness barriers. There is no per-range log that outlives the topology.

### 2. Owner write path

For PUT and DELETE, the owner:

1. Checks request size, topology epoch, ownership, node lease, range availability, and the admission and health guards.
2. Looks up the mutation ID in the deduplication table.
3. Rejects the request if that ID was already used with a different key, operation, or payload.
4. Assigns the owner sequence and one stream entry per follower.
5. Applies the mutation locally.
6. Sends the mutation to the desired followers that are admitted.
7. Waits only for the followers the committed ACK policy requires.
8. Returns the record version.
9. Keeps delivering to the remaining followers in the background and tracks their lag.

If a required follower is unavailable before the owner applies the mutation, the owner returns `TemporarilyUnavailable`, and the client knows nothing was applied. If the owner may have applied the mutation but cannot confirm the required ACKs, it returns `OutcomeUnknown`.

### 3. Mutation idempotency

- The client generates a globally unique mutation ID and reuses it on every retry of the same logical mutation.
- The owner keeps a deduplication receipt holding the mutation fingerprint, the assigned version, a delete flag, and an expiry time.
- Receipts replicate with the mutation, so a promoted successor gives the same answer to a retry.
- Receipts are retained for 60 seconds. This period is part of the public retry contract.
- A receipt is never evicted before its retention period ends.
- If the deduplication budget cannot hold receipts for the full period, the owner rejects new writes with retryable backpressure before applying them.
- Reusing an ID with a different fingerprint is a conflict and is not retryable.
- The guarantee is at-most-once execution while the receipt is retained. It is not exactly-once forever.

### 4. Error and retry semantics

Add distinct operation errors for:

- stale topology or wrong owner, with the current epoch and an owner hint
- expired lease or fenced authority
- replica or policy temporarily unavailable (`unknown_write_outcome=false`)
- mutation outcome unknown (`unknown_write_outcome=true`)
- replication gap, or replica not ready
- idempotency conflict
- deduplication or replication backpressure

The client:

- Keeps the same mutation ID across retries.
- Refreshes its topology before retrying if the error reports a newer epoch. For other retryable errors it backs off exponentially with jitter.
- Uses one end-to-end deadline for connecting, refreshing, backing off, and every RPC attempt.
- When the deadline expires, reports `DeadlineExceeded` together with a typed mutation outcome. The outcome is `KnownNotApplied` only if the client knows the mutation was not applied. Otherwise it is `MayHaveApplied`.
- Once an attempt ends with `MayHaveApplied`, keeps that outcome through later retries, refreshes, and backoff. Callers see both the deadline cause and the outcome.
- Stops retrying at the deadline.

The Rust client attaches the mutation outcome as a named type to operation, RPC, refresh, and deadline errors. It derives the type from the protocol's `unknown_write_outcome` flag and keeps the original error as the cause.

## Control plane

### 1. Thin coordinator

This implementation keeps the singleton coordinator backed by Redb. The coordinator:

- Owns the committed membership, tokens, RF, ACK policy, epoch, and topology digest.
- Serializes joins, leaves, policy and RF changes, and automatic removal of failed nodes.
- Confirms failures and fences nodes.
- Issues node leases.
- Persists topology-transition state and replica admissions.
- Starts and resumes migration, catch-up, and repair.
- Publishes health and recovery status.

The coordinator does not:

- Proxy normal GET, PUT, or DELETE traffic.
- Choose range owners or followers by hand.
- Keep permanent range placements.
- Run consensus or replicate itself.

Coordinator failure, failover, and replication are out of scope. If the coordinator is down, node leases expire and nodes stop serving. This plan makes no availability promise for a coordinator outage.

### 2. Owner leases and fencing

- A lease covers `(node_id, topology_epoch)`, not individual ranges.
- Leases last 5 seconds by default. Nodes renew about once per second.
- Nodes track lease validity with a monotonic clock. They never compare wall clocks.
- GET, PUT, and DELETE all require a valid lease for the installed epoch.
- A node that cannot renew stops serving when its lease expires.
- Before activating ownership that conflicts with an older epoch, the coordinator either gets a fence acknowledgement from the old owner or waits until the old lease has certainly expired.
- Fencing includes the process-instance ID, so a restarted process with empty memory cannot inherit authority from the previous instance.

### 3. Failure suspicion and confirmation

Defaults:

- Probe and health interval: 1 second.
- The first failure on a replication stream marks that link degraded right away.
- A failure is confirmed after 5 consecutive seconds.
- A missing or expired coordinator lease is enough to confirm that a node has failed.
- Without that, at least two independent nodes must report the member unreachable before it is removed automatically.

Sometimes two healthy, leased nodes simply cannot reach each other. Elapsed time does not reveal which side is at fault. In that case the coordinator keeps the affected ranges degraded, nodes return retryable errors as the policy requires, and operators get an actionable alert. The coordinator does not pick one endpoint to evict.

A partial failure that persists and is corroborated leads to node-level fencing and removal from membership. There is no per-range exception.

### 4. Failover

A failure is handled as an unplanned scale-in:

1. Detect and confirm that the physical node has failed.
2. Stop renewing or revoke its lease, and fence its old authority.
3. Commit one new topology without the failed node's tokens.
4. Adjacent ranges merge as a result.
5. Derive the new owners and ordered followers from the new ring.
6. Promote a successor only if it is admitted and its coverage proves it holds the entire merged range.
7. Resume reads once the topology is installed and the lease is active.
8. Resume writes when the active policy allows.
9. Repair the missing desired followers in the background.

Promotion is deterministic. The coordinator does not pick the replica with the highest apparent sequence or the lowest load.

When writes resume depends on the policy:

- `OwnerOnly` resumes once the minimum-copy and healthy-follower guards pass. Some acknowledged writes may have been lost.
- `FirstSuccessor` waits until the new first successor has fully caught up.
- `AllReplicas` waits for the full desired RF.

If no surviving admitted copy covers the whole range, the range is unavailable. An incomplete replica is never promoted.

### 5. Returning nodes

A node that failed or was fenced has stale local records:

- It cannot renew its old authority or get admitted from its old files or memory.
- It rejoins through an explicit membership transition.
- Its data is reset, and it is seeded the same way as a new scale-out node.
- Only a verified snapshot and catch-up, followed by a committed topology, can give it ownership or follower admission again.

## Membership and data movement

### 1. Scale-out

Adding a node changes the ring and splits ranges, so data has to move:

1. Build and validate the target topology.
2. Compute the transition work from the union of old and target token boundaries.
3. List the new owner obligations and the new follower obligations.
4. Seed the joining node with a snapshot from admitted, authoritative sources.
5. Stream concurrent mutations, keeping their mutation IDs and versions.
6. Catch up to a recorded watermark.
7. Pause writes to the affected ranges briefly, drain the final changes, and verify record count, digest, and watermark.
8. Make sure every affected target range meets the minimum-copy requirement and the active ACK policy.
9. Commit the target topology and install it on the nodes.
10. Activate the new ownership, record replica admissions, resume operations, and delete copies that are no longer needed.

Proposing a node's tokens does not make it authoritative.

### 2. Graceful scale-in

1. Propose a topology without the departing node.
2. Check that each successor has complete coverage and that the required downstream followers are ready.
3. Fence writes only for ranges that need a final sync.
4. Commit the removal. The node's tokens disappear and ranges merge.
5. Activate the successors as owners.
6. Resume writes when the ACK policy allows.
7. Repair any remaining RF shortfall and delete the old data.

The successors are already replicas, so a graceful removal normally does not need a bulk copy of the owner's data.

### 3. RF and policy changes

- Raising RF adds followers clockwise and seeds them in the background.
- Lowering RF deletes copies only after the new topology commits, and only copies that are no longer desired.
- Strengthening the ACK policy, including switching to `AllReplicas`, waits for every newly required admission before commit.
- Lowering RF must not leave a policy that needs more copies than can exist.
- A change that would leave the topology permanently unwritable is rejected, unless it is an explicit administrative read-only state.

### 4. Repair

- Repair works on the current derived ranges, not on per-node ranges.
- The destination is the next missing distinct physical node clockwise.
- A repair copies a snapshot, streams ordered incremental changes, and verifies the result.
- Admission is recorded only after full coverage and the watermark are verified.
- Repair tasks run with bounded concurrency and per-node and network budgets.
- Each task is idempotent and can resume from coordinator state.
- If a topology change alters a task's epoch or range bounds, the task is cancelled or re-planned.

## Implementation phases

Every phase must leave the workspace building, with its new behavior covered by tests.

### Phase 1: Topology contract and deterministic replica derivation

- Bump the topology encoding version.
- Add desired RF, ACK policy, and write-availability guards to the Rust and protobuf topology and to its digest.
- Add helpers that list derived ranges and return the owner and ordered distinct followers for any token or range.
- Validate policy and RF combinations, and canonical serialization.
- Extend topology-delta planning to report new owner and follower obligations, without adding permanent range IDs.
- Keep today's single-owner behavior as the initial `OwnerOnly` implementation.

Exit criteria:

- Placement and digest tests are deterministic and pass.
- Split, merge, and wraparound tests pass.
- Every node produces byte-identical canonical output for the same topology.
- Existing migration tests still pass.

### Phase 2: Replica storage, streams, admission, and seeding

- Store follower replica state in the node's unified in-memory record store.
- Implement epoch-scoped owner-to-follower streams, gap detection, lag metrics, snapshot catch-up, and replica verification.
- Persist replica admissions and repair tasks in the coordinator.
- Generalize the migration machinery so it can seed followers as well as transfer ownership.
- Add inspection and status APIs for desired RF, current RF, admission, stream cursor, and lag.

Exit criteria:

- Followers can be seeded and kept current under concurrent PUT and DELETE.
- Incomplete or gapped followers are never admitted.
- Admission records survive a coordinator process restart. (This is durable metadata, not coordinator HA.)
- Client routing does not change.

### Phase 3: ACK policies, idempotent retry, and client semantics

- Implement the `OwnerOnly`, `FirstSuccessor`, and `AllReplicas` gates.
- Implement mutation fingerprints, 60-second deduplication, replicated receipts, and budget backpressure.
- Add the retryable and indeterminate errors, reusing the client's existing epoch, deadline, and retry framework.
- Enforce the minimum admitted-copy and healthy-follower guards.
- Stage ACK-policy changes through the committed topology.

Exit criteria:

- `FirstSuccessor` waits for that specific replica and never for some other follower.
- `AllReplicas` never falls back to the admitted subset.
- A retried RPC with an ambiguous result executes once within the receipt retention period.
- `OwnerOnly` latency does not include any follower ACK.
- Tests show that strengthening a policy waits for readiness before the new topology is published.

### Phase 4: Node leases, failure detection, failover, and RF repair

- Implement one-second heartbeats and lease renewal, with five-second leases.
- Check the lease on every client operation.
- Add suspicion, corroborated failure confirmation, process-instance fencing, and automatic membership removal.
- Derive the promotion target and merged-range coverage from the new ring.
- Resume reads and writes using the policy-specific gates.
- Start bounded background repair to restore the desired RF.
- Treat a returning node as a new scale-out participant.

Exit criteria:

- An old owner stops serving once its lease expires or a replacement activates.
- Promoting the first successor needs no bulk copy when its coverage is complete.
- When one physical node fails, its ranges are promoted onto several different successors.
- RF repair picks only the deterministic clockwise destination.
- A partition of a single link does not cause an arbitrary automatic eviction.

### Phase 5: Membership integration and operational hardening

- Add replica obligations to the 3->4->3 and general membership transitions.
- Make transition and repair recovery idempotent across ordinary coordinator restarts. Coordinator HA remains out of scope.
- Add bounded concurrency, memory and network budgets, retry limits, and actionable health and status output.
- Update the CLI and README to cover topology, policies, degraded state, leases, repair, and what is and is not guaranteed.
- Extend the experiments to record replication overhead, owner latency, failover time, unavailable intervals, acknowledged loss under `OwnerOnly`, preservation under `FirstSuccessor`, and time to restore RF.

Exit criteria:

- Online scale-out and graceful scale-in preserve verified state under concurrent traffic.
- Automatic failover and repair complete with real processes.
- Status output explains why each range is writable, blocked, under-replicated, or repairing.
- The documentation states the default window for losing acknowledged writes.

## Test plan

### Topology and placement

- Owner lookup is exact at token boundaries and across ring wraparound.
- Follower lists skip duplicate physical nodes even when their vnode tokens are adjacent.
- Replica sets are deterministic for any configured RF.
- A desired RF larger than the membership shows up as under-replication.
- Removing a node merges ranges and adding one splits them, with no stable range IDs.
- The first successor before a removal is the owner after it.
- The topology digest changes when RF, policy, guards, membership, or tokens change.
- Two topologies with the same epoch and different digests are rejected.

### Node and replication

- Followers apply only valid, contiguous stream entries.
- Wrong owner, stale epoch, gaps, and conflicting duplicates are rejected.
- A follower ACKs only after applying the entry in memory.
- Snapshot plus concurrent log reaches the same verified digest.
- PUT and every DELETE, including a delete of an absent key, replicate consistently.
- `OwnerOnly` returns without waiting for a follower ACK.
- `FirstSuccessor` does not accept an ACK from only the second follower.
- `AllReplicas` requires the full desired RF.
- The lag and admission gates return the correct known-unavailable error.

### Idempotency and retry

- The same mutation ID and fingerprint returns the stored version without applying the mutation again.
- The same ID with different content is rejected.
- Deduplication state survives promotion of the first successor.
- Receipts are kept for the full retention period.
- When the budget runs out, the owner applies backpressure before applying the mutation.
- Client retries keep the mutation ID and the end-to-end deadline.
- A stale topology triggers a refresh. An unchanged but degraded topology triggers jittered backoff.
- Deadline errors keep the distinction between a known and an indeterminate outcome.

### Coordinator, leases, and failure

- No node serves client operations with an expired or wrong-epoch lease.
- No conflicting owner activates before a fence acknowledgement or the expiry of the old lease.
- Five missed one-second intervals confirm the loss of a lease.
- Corroborated peer failure can remove a node.
- A single ambiguous link failure cannot.
- Admission is bound to the exact epoch, range bounds, and node.
- Promoting into a merged range requires coverage of every constituent range.
- Stale data from a returning node is never admitted.

### Real-process scenarios

- RF=3 with the default `OwnerOnly` under sustained PUT, GET, and DELETE.
- Owner kill showing the documented loss of recently acknowledged writes.
- Owner kill under `FirstSuccessor` with every acknowledged mutation preserved.
- First-successor loss, with retryable errors until the topology change and catch-up allow recovery.
- Promotion, then catch-up of the new first successor before writes resume.
- Follower loss, with `OwnerOnly` still serving because another follower satisfies the guard.
- Background repair from RF=2 back to the desired RF=3.
- 3->4 scale-out and 4->3 graceful scale-in under concurrent operations.
- Rejection of a stale client and a fenced old owner after the new topology is published.
- A returning node that is reset and re-seeded like a new scale-out node.
- A failure in a many-vnode ring that spreads new ownership across the surviving nodes.

Client retries can hide a temporary `RangeBusy`, a stale-owner response, or an interrupted cutover. Where that is possible, pair each client-level test with assertions on the raw node RPCs.

## Observability

Expose at least:

- committed epoch and digest
- desired RF and active ACK policy
- per-node lease expiry, renewal state, and process-instance ID
- node states: suspected, fenced, joining, active, recovering
- per range: owner, desired followers, admitted followers, current RF, and the reason it is or is not writable
- per stream: sequence, lag, last ACK, gaps, and repair state
- counters for retryable-unavailable and indeterminate writes
- deduplication occupancy, eviction age, and backpressure
- migration, failover, and repair phase and duration
- time spent below the desired RF

## Out of scope

- fixed hash slots, or permanent vnode or range IDs
- arbitrary numeric write quorums
- client reads served by followers
- per-request ACK-policy overrides
- choosing a failover target by load
- disk durability for cached data
- multi-writer or leaderless conflict resolution
- topology authority through gossip alone
- coordinator replication, consensus, or automatic coordinator failover
- mixed-version rolling upgrades, and compatibility with old Redb or protobuf state

While the project is private and early-stage, start from a fresh coordinator database and upgrade all binaries together.

## Verification

Run these for every phase:

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

The feature is complete when:

1. Placement comes only from one committed topology.
2. The default `OwnerOnly` behavior and its loss window are demonstrated and documented.
3. `FirstSuccessor` keeps every acknowledged write through the failure of one member.
4. After a promotion, no stale or unfenced owner serves requests.
5. Scale-out, graceful scale-in, failure handling, and repair all work with topology-relative ranges.
6. Replica admission prevents incomplete promotion, and idempotent retries prevent duplicate mutations.
7. Tests with real processes confirm failover, repair, routing refresh, and the spread of load across many vnodes.
