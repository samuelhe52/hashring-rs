# hashring-rs

`hashring-rs` is a process-distributed, single-owner, replicated in-memory cache. One binary
provides the durable coordinator, data-node, and client/admin roles.
Nodes communicate exclusively through the versioned gRPC contract, so the same
client works with local processes, containers, VMs, or remote hosts.

## Start a cluster

Bootstrap membership is supplied only when creating a coordinator store:

```sh
cargo run -- coordinator \
  --state ./coordinator.redb \
  --member node-1=http://127.0.0.1:50051 \
  --member node-2=http://127.0.0.1:50052 \
  --member node-3=http://127.0.0.1:50053

cargo run -- node --id node-1 --listen 127.0.0.1:50051
cargo run -- node --id node-2 --listen 127.0.0.1:50052
cargo run -- node --id node-3 --listen 127.0.0.1:50053
```

Restart the coordinator with only `--state`; the hash seed, epoch, membership,
token assignments, and any active migration are loaded from the transactional
store.

```sh
cargo run -- put --key example --value payload
cargo run -- get --key example
cargo run -- delete --key example
cargo run -- topology
cargo run -- replica-status
```

`--key-hex`, `--value-hex`, and `get --hex` support arbitrary bytes. `DELETE`
is idempotent: success means the key is absent afterward, even if it was already
absent.

Each virtual-node range has one owner and up to `desired_replication_factor-1`
clockwise, distinct physical followers. The default is RF=3, `OwnerOnly`
acknowledgements, at least two admitted copies, and at least one healthy
follower. A follower is admitted only after a verified snapshot/stream catch-up;
the guard prevents writes while those minimums are not met. It is a readiness
gate, not a durability or zero-loss promise.

## Workspace architecture

The root package contains the operational binary and a compatibility facade for
the original public module paths. Implementation responsibilities are separated
into five workspace crates:

- `hashring-core` owns the protobuf contract, topology and migration domain
  types, shared protocol limits, and coordinator transport configuration;
- `hashring-client` owns routing, deadlines, retries, and public client errors;
- `hashring-coordinator` owns durable cluster state and migration orchestration;
- `hashring-node` owns in-memory records, replication streams, migration staging, and the data-node
  service;
- `hashring-experiment` owns reproducible real-process workload orchestration
  and evidence capture.

The client, coordinator, and node depend on `hashring-core`, but not on one
another. This keeps the wire/domain boundary reusable without coupling clients
to either server implementation.

## Reproducible experiments

The `experiment` command launches a coordinator and ordinary data-node child
processes, drives them through the reusable client, and writes a configuration
manifest, JSON-lines event log, per-process stdout/stderr, coordinator database,
and JSON summary into a new or empty output directory.

```sh
cargo build --release

# Correctness and migration stress: starts with 9 nodes, scales to 10 under
# concurrent rewrites and deletes, preserves a third moving cohort as snapshot
# sentinels, scales back to 9 while restoring/deleting the mutation cohorts, and
# verifies present and absent keys after each stage.
target/release/hashring-rs experiment \
  --mode correctness --require-clean-source \
  --output results/correctness-10-node

# Nominal acceptance profile: 10 data nodes and 1,000,000 logical keys.
target/release/hashring-rs experiment \
  --mode performance --require-clean-source \
  --output results/performance-10-node-1m

# Failure/repair evidence: 3 live nodes plus 1 replacement. Run separately
# with --policy owner-only to sample its possible acknowledged-loss window.
target/release/hashring-rs experiment \
  --mode availability --policy first-successor \
  --output results/availability-first-successor
```

Correctness mode defaults to 20,000 keys; performance mode defaults to
1,000,000; availability mode defaults to 1,000 keys and 4 total nodes. The
other modes default to 10 peak nodes. All default to 128-byte values, concurrency 64,
seed 1, and 128 virtual nodes. Use explicit flags to vary them; the manifest
always records the actual configuration, build-time revision/toolchain/profile,
runtime source status and diff, and a BLAKE3 digest of the executable. The
`results` directory is ignored by Git so raw databases and logs stay local, but
the runner never overwrites a non-empty result directory. Formal runs use
`--require-clean-source`; dirty ad hoc runs remain available but are explicitly
marked `source_reproducible=false` in the manifest.
The summary records initial-put throughput plus sampled end-to-end client `PUT`
and direct owner-RPC `PUT` round-trip latency distributions (microseconds,
p50/p95/p99/max). The latter includes the network hop and owner processing,
not CPU-only service time. Correctness runs record time to
full RF measured from each transition's execution start. Availability runs
record failover publication and first recovered read times, sampled
read-unavailability probes, acknowledged key preservation/loss (including a
just-acknowledged 1 MiB-or-larger tail write), and replacement-to-full-RF time.
These are observed local process measurements, not service-level guarantees.
Compare RF=1 and RF=3 with `--minimum-admitted-copies 1
--minimum-healthy-followers 0` and `OwnerOnly` in both runs to estimate
replication overhead; compare policies at the same
RF to estimate ACK-wait overhead. A single OwnerOnly run with zero observed
loss does not close its loss window.

## Change membership

`begin-change` receives the complete target membership. Existing node IDs must
retain their endpoints because data-node storage is volatile; replacing a process
under the same ID would otherwise imply data that the new process does not have.
Each committed node ID is durably fenced to its registered process instance, so
an empty replacement is rejected instead of serving false cache misses.

```sh
cargo run -- begin-change \
  --member node-1=http://127.0.0.1:50051 \
  --member node-2=http://127.0.0.1:50052 \
  --member node-3=http://127.0.0.1:50053

# A new node may start after it appears in the pending target topology.
cargo run -- node --id node-3 --listen 127.0.0.1:50053
# Use the identity printed by begin-change; this prevents an ambiguous retry
# from executing a later change.
cargo run -- execute-change \
  --change-id CHANGE_ID --base-epoch 1 --target-epoch 2
cargo run -- change-status
cargo run -- replica-status
```

The source remains authoritative while a point-in-time snapshot is copied and
independent ranges are moved concurrently with a default limit of 16. Use
`coordinator --range-move-concurrency N` (or the same `experiment` option) to tune
that bound. Concurrent puts and deletes are replayed from a bounded changelog.
Cutover briefly returns retryable `RangeBusy` responses for affected writes while reads continue,
verifies the live-record count, contiguous watermark, and a BLAKE3 digest, then
publishes the new epoch. During the read handoff, the frozen source and committed
destination contain the same data; destination writes remain fenced until every
source has installed the new topology. Deleted values are not retained as permanent
tombstones. Removed cooperative nodes are stopped only after source cleanup, using
the process-instance identity captured during migration. A node acknowledges a
prepared stop before that acknowledgement is persisted as an instance-specific stop
confirmation and final shutdown is requested. The confirmation remains durable
after the active change completes, so a node that temporarily loses coordinator
connectivity still shuts down when it reconnects; a lost final response is
recoverable without treating a transient outage as a successful stop. Interrupted
coordinator work resumes automatically. Pre-publication cleanup has its own deadline
and remains in a recoverable `Aborting` state until authoritative sources acknowledge
it; a post-publication failure never rolls the epoch back.

For planned membership cutovers, new-epoch leases are withheld until required
target followers are verified and admitted. Automatic failover may publish a
degraded but guard-compliant placement, then repair followers in the background.
A restart of the
ordinary coordinator resumes its persisted transition and repair plan. If a
required target node remains unavailable, the published transition can remain
pending; restore the same live process if possible and inspect `change-status`
and `replica-status`. A second process loss during cutover is not automatically
rolled back or reconfigured. Coordinator consensus/failover is not provided.

`replica-status` reports each range's nominal `current_rf`, leased `live_rf`,
owner lease, follower admission/lag/health, `writable` and its block reason,
under-replication, repair phase, retry count, next attempt time, and last error.
`current_rf` alone does not prove liveness. During a cutover fence, status
conservatively marks affected ranges as potentially blocked. Leases last five
seconds and renew every second. Automatic failure detection runs every second;
repairs/recovery run every five seconds. Repair work is capped at 32
owner/follower groups per pass and four concurrent groups, with per-group
exponential retry backoff capped at 60 seconds. Each node caps its pending
replication buffer at 64 MiB and its dedup receipts at 16 MiB; migration
snapshot pages are capped at 8 MiB. A failed required repair remains visible,
not silently considered healthy.

To strengthen acknowledgements after all followers are admitted:

```sh
cargo run -- begin-policy-change --policy first-successor
cargo run -- execute-change --change-id CHANGE_ID --base-epoch E --target-epoch E_PLUS_1
cargo run -- replica-status
```

`FirstSuccessor` waits for the first clockwise follower's applied-stream ACK
before reporting write success. `AllReplicas` waits for every desired follower,
so writes block if the full desired placement is not healthy. A policy change
briefly fences writes and publishes a new epoch only after its readiness check.
The client reuses one request ID across retries of a logical write within the
in-memory 60-second deduplication window. A new CLI invocation generates a new
ID, and receipts do not survive process loss.

## Request and error semantics

- Each key has one owner. `PUT`, `GET`, and `DELETE` are linearizable per key
  while the coordinator and owner are available. `DELETE` succeeds when the key
  is already absent and does not expose a prior-existence flag.
- Versions are `(topology_epoch, owner_sequence, owner_node_id)` and migration
  preserves them.
- Logical operations use an overall deadline and bounded jittered retry. Clients
  combine periodic epoch polling, mandatory refresh after `Moved`, opportunistic
  refresh after `Unavailable`, and non-blocking refresh after a successful
  response reports a newer epoch.
- `Moved`, `RangeBusy`, retryable `Unavailable`, and retryable
  `ResourceExhausted` may be retried within that deadline.
- `NotFound`, `TooLarge`, `InvalidArgument`, and permanent gRPC statuses return
  immediately.
- If a `PUT`, `DELETE`, or mutating admin RPC was dispatched but its response was
  lost, a final failure reports `unknown_write_outcome=true`. A later successful
  DELETE confirms that the key is absent. Mutation IDs have bounded, in-memory
  deduplication receipts for retries; they are not durable across node loss.
- Key and value limits are 64 KiB and 8 MiB. A node's aggregate migration-journal
  budget defaults to 16 MiB. A cluster supports at most 1,024 members, 4,096
  virtual nodes per member, and 1,048,576 total assignments. A single membership
  change may move at most 16,384 ranges; these bounds keep topology and change
  snapshots within the 64 MiB control-plane message limit.

The default `OwnerOnly` policy acknowledges once the owner applies a write;
followers receive it asynchronously. If that owner dies abruptly, **any
acknowledged writes not yet applied by the promoted follower can be lost**,
including an acknowledged update reverting to an older value even when the
key remains present.
There is no fixed time-based maximum loss window: the configured 5-second lag
threshold is a health check, not an RPO bound. `FirstSuccessor` closes that
single-owner-loss window for acknowledged writes that reached its required
follower, but not for simultaneous/correlated node losses. Data nodes are
in-memory; neither policy provides crash durability. During degraded placement,
the guard or ACK policy may make writes unavailable rather than weaken the
requested guarantee. Transactions, durable data-node storage, and coordinator
consensus remain out of scope.

## Tests

```sh
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```
