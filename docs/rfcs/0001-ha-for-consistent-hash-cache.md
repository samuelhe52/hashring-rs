# RFC 0001: HA for Consistent-Hash Cache

## Goal

Extend the current in-memory consistent-hash cache with range-level replication, configurable write acknowledgement, automatic node failover, and asynchronous replication repair without replacing classic consistent hashing with fixed slots.

The design keeps client-side routing and a thin coordinator:

- clients route data operations directly to the owner derived from a cached committed topology;
- the coordinator serializes topology changes, confirms failures, issues leases, persists replica admission, and orchestrates migration/repair;
- owners and ordered followers are deterministic consequences of the committed ring;
- ranges are intervals derived from adjacent tokens in one topology and may split or merge when membership changes.

This is a full replacement for the previous fixed-range, RF=3, all-copy-ACK plan.

## Baseline

At `kona` HEAD `1f7b25b`:

- classic consistent hashing already assigns multiple deterministic vnode tokens to each physical node;
- `TopologySnapshot` contains the epoch, members, token assignments, hash configuration, and digest;
- owner lookup selects the first token clockwise;
- membership migration already uses the union of old and target token boundaries, so its work ranges naturally split and merge;
- online owner migration already has snapshot, changelog, write pause, digest verification, topology publication, activation, and cleanup stages;
- clients already carry `request_id` and topology epoch, refresh stale topology, preserve one operation deadline across retries, and distinguish an unknown write outcome;
- steady-state replication, replica admission, failure detection, owner leases, promotion, RF repair, and write-ACK policy do not yet exist;
- node-side `request_id` deduplication does not yet exist.

Existing migration `range_id` values are transition-scoped task identifiers. They may remain, but must never become permanent shard or vnode identities.

## Architectural Decisions

### 1. Ring, ranges, and placement

1. Keep classic consistent hashing with many virtual-node tokens per physical node.
2. A token owns `(previous_token, token]`, including wraparound.
3. A derived range has no permanent identity. Removing tokens merges adjacent ranges; adding tokens splits ranges.
4. For each derived range, the desired replica set is:
   - the owner of the ending token;
   - then the next distinct physical nodes clockwise until the desired RF is reached or all members are exhausted.
5. Repeated tokens belonging to the same physical node are skipped when selecting followers.
6. The first successor is both:
   - the first desired follower; and
   - the node the ring naturally selects as owner if the current owner is removed.
7. No explicit per-range placement table or exceptional placement override is introduced.
8. Failed-owner load is distributed by the existing vnode distribution. Do not dynamically choose a `least loaded` promotion target.

### 2. Committed topology contract

The committed topology is the only source of owner authority and contains:

- `epoch`;
- hash algorithm, seed, encoding version, and vnode count;
- canonical members and token assignments;
- configurable `desired_replication_factor`;
- cluster-wide `write_ack_policy`;
- write-availability guard configuration;
- a digest covering every field above.

Supported write policies are semantic enums, not numeric quorum values:

- `OwnerOnly` — default; return after the owner applies the mutation.
- `FirstSuccessor` — return after the owner and the deterministic first successor apply it.
- `AllReplicas` — return only after the complete desired RF applies it.

There is no arbitrary `W=2` because acknowledgement by an arbitrary second replica does not make the deterministic promotion target safe.

Policy changes are topology changes:

- strengthening `OwnerOnly -> FirstSuccessor` or `FirstSuccessor -> AllReplicas` requires the necessary copies to catch up before the new epoch is committed;
- weakening a policy may activate immediately only through an explicit committed topology;
- local node configuration cannot override the committed policy;
- per-request policy overrides are out of scope.

### 3. Desired placement versus operational readiness

Placement and readiness are separate:

- the desired replica set is derived solely from the committed ring and desired RF;
- the admitted replica set contains copies whose initial data and contiguous mutation history have been verified;
- a healthy follower is admitted, connected, and within the configured replication-lag bound.

Replica admission is durable coordinator state keyed by:

- topology epoch;
- exact derived range bounds;
- node ID;
- verified snapshot/changelog watermark or equivalent coverage proof.

Admission does not advance the global topology epoch and is not sent to clients. It affects ACK eligibility, promotion eligibility, health reporting, and repair.

When topology changes merge ranges, a successor is admitted for the merged range only if the coordinator can prove complete coverage of every constituent interval. When ranges split, admission may be projected only to covered subranges.

### 4. Replication factor and degraded operation

- Desired RF is configurable; RF=3 is the normal example, not a hard-coded constant.
- Desired RF may temporarily exceed the number of live physical nodes. The range is then under-replicated and repair remains pending until capacity exists.
- By default, an `OwnerOnly` write requires only a leased owner. Operators can
  configure a stricter admission and health guard when reduced data-loss risk
  matters more than write availability.
- With two admitted copies of desired RF=3:
  - `OwnerOnly` may write if the health guard passes;
  - `FirstSuccessor` may write if the admitted healthy copy is the first successor and acknowledges;
  - `AllReplicas` rejects writes because the complete desired RF is unavailable.
- With only one admitted copy, `OwnerOnly` writes remain available under the
  default guard and a valid owner lease. Stricter guards may reject them.
- `desired_rf=1` with `FirstSuccessor` is invalid. RF=1 with `OwnerOnly` is
  writable under the default guard, with no replica to survive owner failure.
- Repair always selects the next required distinct physical node clockwise. If no eligible node exists, the range remains visibly under-replicated.

Initial defaults:

- `minimum_admitted_copies = 1`;
- `minimum_healthy_followers = 0`;
- `max_replica_lag = 5s`, configurable and subject to validation by process tests.

### 5. Consistency contract

All data is volatile; ACK means applied to memory, not persisted to disk.

`OwnerOnly`:

- success means the owner applied the mutation;
- followers are updated asynchronously;
- a single owner failure can lose recently acknowledged writes;
- failover may expose an older state;
- per-key linearizability is promised only within one owner epoch, not across failover.

`FirstSuccessor`:

- the owner assigns mutation order and applies the mutation;
- the first successor validates authority and sequence and applies the mutation before ACK;
- an acknowledged write survives one member failure from a healthy starting state;
- the second and later followers remain asynchronous;
- after promotion, reads may resume once authority is fenced and committed, but successful writes wait until the new first successor is caught up and admitted.

`AllReplicas`:

- every member of the full desired replica set must apply before success;
- temporary under-replication makes writes retryably unavailable;
- `all` never silently means only the currently admitted subset.

Followers never serve normal client reads. A follower ACK means the mutation passed epoch, owner, stream-order, and payload validation and was applied to its replica state; enqueueing or merely receiving it is insufficient.

## Data-Plane Design

### 1. Mutation ordering and replication streams

Keep record versions compatible with the existing shape:

```text
(topology_epoch, owner_node_id, owner_sequence)
```

- `owner_sequence` increases monotonically on the physical owner.
- Each `(epoch, owner, follower)` pair has a separate contiguous `stream_sequence`.
- A replication entry carries the stream sequence, key/token, operation, value or tombstone, record version, and mutation ID.
- One stream may carry mutations for multiple derived ranges for which that follower is desired.
- Followers reject gaps, stale epochs, wrong owners, conflicting duplicate entries, and mutations outside their desired coverage.
- A stream gap triggers catch-up from retained history or snapshot repair; it never gets skipped.
- A new topology epoch establishes new stream identities and readiness barriers rather than pretending a derived range has a permanent log.

### 2. Owner write path

For PUT and DELETE:

1. Validate request size, topology epoch, current ownership, node lease, range availability, and admission/health gates.
2. Look up the mutation ID in the deduplication table.
3. Reject reuse of the same mutation ID with a different key, operation, or payload.
4. Allocate the owner sequence and per-follower stream entries.
5. Apply the mutation on the owner.
6. Dispatch replication to desired admitted followers.
7. Wait only for the followers required by the committed ACK policy.
8. Return the record version after the policy is satisfied.
9. Continue asynchronous delivery and lag tracking for followers not required for this ACK.

If a required follower is unavailable before the owner applies the mutation, return a known `TemporarilyUnavailable` result. If the owner may have applied the mutation but required acknowledgement is uncertain, return `OutcomeUnknown`.

### 3. Mutation idempotency

- The client generates one globally unique mutation ID and reuses it for every retry of the same logical mutation.
- The owner stores the mutation fingerprint and original result.
- Deduplication records are replicated with the mutation so a promoted successor can answer the retry consistently.
- `idempotency_window` defaults to 60 seconds and is part of the public retry contract.
- Entries must not be silently evicted before the window expires.
- If the deduplication budget cannot retain the guarantee, reject new writes with retryable backpressure before applying them.
- A repeated ID with a different fingerprint is a non-retryable conflict.
- The guarantee is at-most-once within the configured window, not indefinite exactly-once execution.

### 4. Error and retry semantics

Add explicit operation errors for:

- stale topology / wrong owner, including the current epoch and owner hint;
- lease expired or fenced authority;
- replica or policy temporarily unavailable with `unknown_write_outcome=false`;
- indeterminate mutation outcome with `unknown_write_outcome=true`;
- replication gap or replica not ready;
- idempotency conflict;
- deduplication or replication backpressure.

The client:

- keeps the same mutation ID across retries;
- refreshes topology before retrying when a newer epoch is advertised;
- otherwise retries retryable errors with jittered exponential backoff;
- keeps one end-to-end operation deadline across connection, refresh, backoff, and RPC attempts;
- retains `DeadlineExceeded` as the cause when the end-to-end deadline expires, with a typed mutation outcome: `KnownNotApplied` only when the client knows the mutation was not applied, or `MayHaveApplied` when it may have applied;
- preserves `MayHaveApplied` across later retries, topology refreshes, and backoff; callers can distinguish the two outcomes without losing the deadline cause;
- never retries indefinitely.

The Rust client exposes the mutation outcome as a named type on operation, RPC, refresh, and deadline errors. It maps the protocol's `unknown_write_outcome` flag to that type while retaining the original error cause.

## Control Plane

### 1. Thin coordinator

Retain the Redb-backed singleton coordinator for this implementation. It:

- owns committed membership, tokens, RF, ACK policy, epoch, and topology digest;
- serializes joins, leaves, policy/RF changes, and automatic failure removal;
- confirms failures and fences nodes;
- issues node leases;
- persists topology-transition and replica-admission state;
- starts and resumes migration, catch-up, and repair;
- publishes health and recovery status.

It does not:

- proxy normal GET/PUT/DELETE traffic;
- manually choose range owners or followers;
- maintain permanent range placements;
- implement consensus or coordinator HA.

Coordinator failure, failover, and replication are explicitly out of scope for this plan. Nodes still fail closed when their leases expire; no availability guarantee is made for a coordinator outage.

### 2. Owner leases and fencing

- A lease is scoped to `(node_id, topology_epoch)` rather than to every range.
- Default lease duration is 5 seconds; nodes renew approximately every second.
- Nodes measure granted validity with monotonic elapsed time and do not compare wall clocks.
- GET, PUT, and DELETE require a valid lease for the installed epoch.
- A node that cannot renew self-fences at expiry.
- Before activating ownership that conflicts with an old epoch, the coordinator obtains an explicit fence acknowledgement or waits until the old lease must have expired.
- Process-instance identity remains part of fencing so a restarted empty process cannot inherit authority from an older instance.

### 3. Failure suspicion and confirmation

Defaults:

- probe/health interval: 1 second;
- first replication-stream failure: immediately mark that link degraded;
- failure-confirmation threshold: 5 consecutive seconds;
- missing/expired coordinator lease: sufficient evidence to confirm node failure;
- otherwise require at least two independent nodes to report the member unreachable before automatic eviction.

If only one pair of healthy leased nodes cannot communicate, elapsed time alone does not identify which endpoint is faulty. Keep affected ranges degraded, return retryable errors as required by policy, and report an actionable alert rather than arbitrarily evicting one endpoint.

Persistent, corroborated partial failure causes node-level fencing and global membership removal. There is no per-range placement exception.

### 4. Failover

Failure is an ungraceful scale-in:

1. Detect and confirm the failed physical node.
2. Stop or revoke lease renewal and fence the old authority.
3. Commit one new topology without the failed node's tokens.
4. Let adjacent ranges merge naturally.
5. Derive new owners and ordered followers from the new ring.
6. Promote only an admitted successor whose coverage proves it owns the complete merged range.
7. Resume reads after topology installation and lease activation.
8. Resume writes according to the active policy gate.
9. Repair missing desired followers asynchronously.

Promotion is deterministic; do not elect the replica with the largest apparent sequence or lowest load.

Policy-specific recovery:

- `OwnerOnly` may resume once the minimum-copy and healthy-follower guards pass; acknowledged tail loss is allowed.
- `FirstSuccessor` waits for the new first successor to catch up completely.
- `AllReplicas` waits for the complete desired RF.

If no surviving admitted copy proves complete coverage, the range is unavailable. Never manufacture authority from an incomplete replica.

### 5. Returning nodes

A previously failed or fenced node's local records are stale:

- it cannot renew old authority or become admitted from its old files/memory;
- rejoining is an explicit membership transition;
- its data state is reset and seeded exactly like a new scale-out node;
- only verified snapshot/catch-up and a committed topology can restore ownership or follower admission.

## Membership and Data Movement

### 1. Scale-out

Adding a node changes the ring and splits ranges, so genuine migration is required:

1. Build and validate the proposed target topology.
2. Compute transition work from the union of old and target token boundaries.
3. Identify both new-owner obligations and new-follower obligations.
4. Seed the joining node from authoritative admitted sources using a snapshot.
5. Stream concurrent mutations and preserve mutation IDs and versions.
6. Catch up to a recorded watermark.
7. Briefly pause affected writes, drain the final delta, and verify record count/digest/watermark.
8. Satisfy the minimum-copy and active ACK-policy barrier for every affected target range.
9. Commit the target topology and install it on nodes.
10. Activate new ownership, record replica admissions, resume operations, and clean obsolete copies.

The new node is never authoritative merely because its tokens were proposed.

### 2. Graceful scale-in

1. Propose the topology without the departing node.
2. Verify that every natural successor has complete coverage and required downstream followers are ready.
3. Fence writes only for ranges needing a final synchronization.
4. Commit removal; tokens disappear and ranges merge.
5. Activate the deterministic successor owners.
6. Resume according to the ACK-policy gate.
7. Repair any remaining desired RF and clean old data.

Graceful removal should normally avoid bulk owner migration because successors are already replicas.

### 3. RF and policy changes

- Increasing RF derives additional clockwise followers and seeds them asynchronously.
- Decreasing RF removes only no-longer-desired copies after the new topology commits.
- Strengthening ACK policy or enabling `AllReplicas` waits for all newly required admissions before commit.
- Decreasing RF must not strand a policy whose minimum required copies can no longer exist.
- A configuration change that would make the topology permanently unwritable is rejected unless explicitly represented as an administrative read-only state.

### 4. Repair

- Repair is planned per current derived range, not per physical-node range.
- The repair destination is the next missing distinct physical node clockwise.
- Use snapshot plus ordered incremental replication and verification.
- Admission is recorded only after complete coverage and watermark verification.
- Independent repair tasks run with bounded concurrency and per-node/network budgets.
- A repair task is idempotent and resumable from coordinator state.
- Topology changes invalidate or re-plan repair work whose epoch or derived bounds no longer match.

## Implementation Phases

Each phase must keep the workspace buildable and its new behavior testable.

### Phase 1 — Topology contract and deterministic replica derivation

- Bump topology encoding.
- Add desired RF, ACK policy, and write-availability guard fields to Rust/protobuf topology and its digest.
- Add helpers to enumerate derived ranges and return owner plus ordered distinct followers for any token/range.
- Validate policy/RF combinations and canonical serialization.
- Extend topology-delta planning to report new owner and new follower obligations without adding permanent range identities.
- Keep current single-owner behavior behind the initial `OwnerOnly` implementation boundary.

Exit criteria:

- deterministic placement and digest tests pass;
- split/merge and wraparound tests pass;
- the same topology produces byte-identical canonical output on every node;
- existing migration tests remain green.

### Phase 2 — Replica storage, streams, admission, and seeding

- Add follower replica state to the node's unified in-memory record store.
- Implement epoch-scoped owner-to-follower streams, gap detection, lag metrics, snapshot catch-up, and replica verification.
- Persist replica-admission records and repair tasks in the coordinator.
- Generalize the existing migration machinery so it can seed follower obligations as well as transfer ownership.
- Add inspection/status APIs for desired RF, current RF, admission, stream cursor, and lag.

Exit criteria:

- followers can be seeded and kept current under concurrent PUT/DELETE;
- incomplete or gapped followers are never admitted;
- admission survives coordinator process restart as durable metadata, without claiming coordinator HA;
- no client routing change is required.

### Phase 3 — ACK policies, idempotent retry, and client semantics

- Implement `OwnerOnly`, `FirstSuccessor`, and `AllReplicas` execution gates.
- Implement mutation fingerprinting, 60-second deduplication, replicated results, and budget backpressure.
- Add precise retryable/indeterminate errors and reuse the client's existing epoch/deadline/retry framework.
- Enforce minimum admitted-copy and healthy-follower guards.
- Implement staged ACK-policy changes through committed topology.

Exit criteria:

- `FirstSuccessor` waits for that exact replica, never an arbitrary follower;
- `AllReplicas` never degrades to the admitted subset;
- ambiguous RPC retries execute once within the idempotency window;
- `OwnerOnly` latency does not wait for a follower ACK;
- policy-strengthening tests prove the readiness barrier occurs before publication.

### Phase 4 — Node leases, failure detection, failover, and RF repair

- Implement one-second heartbeat/lease renewal and five-second node leases.
- Enforce lease checks for all client operations.
- Add suspicion, corroborated failure confirmation, process-instance fencing, and automatic membership removal.
- Derive deterministic promotion and merged-range coverage from the new ring.
- Resume reads/writes using policy-specific readiness gates.
- Start bounded asynchronous repair to restore desired RF.
- Treat returning nodes as fresh scale-out participants.

Exit criteria:

- an old owner cannot serve after its lease expires or a replacement activates;
- first-successor promotion requires no bulk copy when coverage is complete;
- failure of one physical node distributes promoted ranges across multiple successors;
- RF repair selects only deterministic clockwise destinations;
- ambiguous single-link partitions do not cause arbitrary automatic eviction.

### Phase 5 — Membership integration and operational hardening

- Integrate replica obligations into 3->4->3 and general membership transitions.
- Make transition/repair recovery idempotent across ordinary coordinator restarts, while leaving coordinator HA out of scope.
- Add bounded concurrency, memory/network budgets, retry limits, and actionable health/status output.
- Update CLI and README for topology, policies, degraded state, leases, repair, guarantees, and non-guarantees.
- Update experiments to record replication overhead, owner latency, failover time, unavailable intervals, acknowledged loss under `OwnerOnly`, preservation under `FirstSuccessor`, and time to restore RF.

Exit criteria:

- online scale-out and graceful scale-in preserve verified state under concurrent traffic;
- automatic failover and repair complete in real processes;
- status output explains why each range is writable, blocked, under-replicated, or repairing;
- documentation makes the default acknowledged-write-loss window explicit.

## Test Plan

### Topology and placement

- exact owner lookup at token boundaries and ring wraparound;
- ordered followers skip duplicate physical nodes despite adjacent vnode tokens;
- deterministic replica sets for configurable RF;
- desired RF larger than membership produces visible under-replication;
- removal merges and addition splits ranges without stable range identity;
- first successor before removal becomes the natural owner afterward;
- topology digest changes with RF, policy, guards, membership, or tokens;
- conflicting equal-epoch topology digests are rejected.

### Node and replication

- follower applies only valid contiguous stream entries;
- wrong owner, stale epoch, gap, and conflicting duplicate are rejected;
- follower ACK occurs only after in-memory application;
- snapshot plus concurrent log reaches the same verified digest;
- PUT and every DELETE, including an absent-key delete, replicate consistently;
- `OwnerOnly` returns without follower ACK;
- `FirstSuccessor` rejects ACK from only the second follower;
- `AllReplicas` requires the full desired RF;
- lag and admission gates produce the correct known-unavailable error.

### Idempotency and retry

- the same mutation ID and fingerprint returns the original result;
- the same ID with different content is rejected;
- deduplication state survives first-successor promotion;
- entries remain protected for the full idempotency window;
- budget exhaustion backpressures before mutation application;
- client retries preserve mutation ID and end-to-end deadline;
- stale topology triggers refresh; unchanged degraded topology triggers jittered backoff;
- deadline errors preserve known versus indeterminate outcome.

### Coordinator, leases, and failure

- no node serves client operations with an expired or wrong-epoch lease;
- no conflicting owner activates before fencing acknowledgement or old-lease expiry;
- five missed one-second intervals confirm loss of lease;
- corroborated peer failure may remove a node;
- one ambiguous pairwise link failure does not;
- admission is bound to exact epoch, bounds, and node;
- merged-range promotion requires complete constituent coverage;
- returning stale data is never admitted.

### Real-process scenarios

- RF=3 with default `OwnerOnly` under sustained PUT/GET/DELETE;
- owner kill demonstrating the documented possible acknowledged-tail loss;
- owner kill under `FirstSuccessor` preserving all acknowledged mutations;
- first-successor loss returning retryable errors until topology/catch-up permits recovery;
- promotion followed by new-first-successor catch-up before writes resume;
- follower loss with continued `OwnerOnly` service when another follower satisfies the guard;
- asynchronous repair from current RF=2 back to desired RF=3;
- 3->4 scale-out and 4->3 graceful scale-in under concurrent operations;
- stale client and fenced old owner rejection after topology publication;
- returning node reset and re-seed as a fresh scale-out participant;
- many-vnode failure distributing new ownership across surviving nodes.

Raw node-RPC assertions must accompany client-level tests where client retry could hide a temporary `RangeBusy`, stale-owner response, or interrupted cutover.

## Observability

Expose at minimum:

- committed epoch and digest;
- desired RF and active ACK policy;
- per-node lease expiry/renewal state and process-instance ID;
- suspected, fenced, joining, active, and recovering node states;
- per-range owner, desired followers, admitted followers, current RF, and writability reason;
- per-stream sequence, lag, last ACK, gap, and repair state;
- retryable-unavailable and indeterminate-write counters;
- deduplication occupancy, eviction age, and backpressure;
- migration/failover/repair phase and duration;
- time spent below desired RF.

## Out of Scope

- fixed hash slots or permanent vnode/range identities;
- arbitrary numeric write quorums;
- follower-served client reads;
- per-request ACK-policy overrides;
- dynamic `least loaded` failover placement;
- disk durability for cached data;
- multi-writer or leaderless conflict resolution;
- gossip-only topology authority;
- coordinator replication, consensus, or automatic coordinator failover;
- mixed-version rolling upgrades and old Redb/protobuf state compatibility.

Use a fresh coordinator database and upgrade all binaries together while the project remains private and early-stage.

## Verification

For every phase:

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

The complete feature is accepted only when:

1. placement is derived solely from one committed topology;
2. default `OwnerOnly` behavior and its loss semantics are demonstrated and documented;
3. `FirstSuccessor` preserves acknowledged writes across one member failure;
4. no stale or unfenced authority serves after promotion;
5. scale-out, graceful scale-in, failure, and repair all work with topology-relative ranges;
6. replica admission and idempotent retries prevent incomplete promotion and duplicate mutation;
7. real-process evidence confirms failover, repair, routing refresh, and many-vnode load distribution.
