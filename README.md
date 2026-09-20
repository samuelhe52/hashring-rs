# hashring-rs

`hashring-rs` is a process-distributed, single-owner in-memory cache. One binary
provides the durable coordinator, data-node, and client/admin roles.
Nodes communicate exclusively through the versioned gRPC contract, so the same
client works with local processes, containers, VMs, or remote hosts.

## Start a cluster

Bootstrap membership is supplied only when creating a coordinator store:

```sh
cargo run -- coordinator \
  --state ./coordinator.redb \
  --member node-1=http://127.0.0.1:50051 \
  --member node-2=http://127.0.0.1:50052

cargo run -- node --id node-1 --listen 127.0.0.1:50051
cargo run -- node --id node-2 --listen 127.0.0.1:50052
```

Restart the coordinator with only `--state`; the hash seed, epoch, membership,
token assignments, and any active migration are loaded from the transactional
store.

```sh
cargo run -- put --key example --value payload
cargo run -- get --key example
cargo run -- delete --key example
cargo run -- topology
```

`--key-hex`, `--value-hex`, and `get --hex` support arbitrary bytes. `DELETE`
is idempotent: success means the key is absent afterward, even if it was already
absent.

## Workspace architecture

The root package contains the operational binary and a compatibility facade for
the original public module paths. Implementation responsibilities are separated
into five workspace crates:

- `hashring-core` owns the protobuf contract, topology and migration domain
  types, shared protocol limits, and coordinator transport configuration;
- `hashring-client` owns routing, deadlines, retries, and public client errors;
- `hashring-coordinator` owns durable cluster state and migration orchestration;
- `hashring-node` owns in-memory records, migration staging, and the data-node
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
```

Correctness mode defaults to 20,000 keys; performance mode defaults to
1,000,000. Both default to 10 peak nodes, 128-byte values, concurrency 64,
seed 1, and 128 virtual nodes. Use explicit flags to vary them; the manifest
always records the actual configuration, build-time revision/toolchain/profile,
runtime source status and diff, and a BLAKE3 digest of the executable. The
`results` directory is ignored by Git so raw databases and logs stay local, but
the runner never overwrites a non-empty result directory. Formal runs use
`--require-clean-source`; dirty ad hoc runs remain available but are explicitly
marked `source_reproducible=false` in the manifest.

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
```

The source remains authoritative while a point-in-time snapshot is copied and
concurrent puts and deletes are replayed from a bounded changelog. Cutover briefly
returns retryable `RangeBusy` responses for affected writes while reads continue,
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
  DELETE confirms that the key is absent. Request IDs are correlation IDs, not
  persistent deduplication receipts; retrying a `PUT` may allocate a newer version
  for the same value.
- Key and value limits are 64 KiB and 8 MiB. A node's aggregate migration-journal
  budget defaults to 16 MiB. A cluster supports at most 1,024 members, 4,096
  virtual nodes per member, and 1,048,576 total assignments. A single membership
  change may move at most 16,384 ranges; these bounds keep topology and change
  snapshots within the 64 MiB control-plane message limit.

All data-node binaries must be upgraded before clients issue DELETE: an older
destination does not understand deletion journal records. Replication, automatic
failure recovery, heartbeat eviction, transactions, durable data-node storage,
and coordinator consensus remain intentionally out of scope. Unexpected owner
loss makes that owner's data unavailable.

## Tests

```sh
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```
