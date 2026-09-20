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
cargo run -- topology
```

`--key-hex`, `--value-hex`, and `get --hex` support arbitrary bytes.

## Workspace architecture

The root package contains the operational binary and a compatibility facade for
the original public module paths. Implementation responsibilities are separated
into four workspace crates:

- `hashring-core` owns the protobuf contract, topology and migration domain
  types, shared protocol limits, and coordinator transport configuration;
- `hashring-client` owns routing, deadlines, retries, and public client errors;
- `hashring-coordinator` owns durable cluster state and migration orchestration;
- `hashring-node` owns in-memory records, migration staging, and the data-node
  service.

The client, coordinator, and node depend on `hashring-core`, but not on one
another. This keeps the wire/domain boundary reusable without coupling clients
to either server implementation.

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
concurrent writes are replayed from a bounded changelog. Cutover briefly returns
retryable `RangeBusy` responses for affected reads and writes, verifies record count,
contiguous watermark, and a BLAKE3 digest, then publishes the new epoch. Removed
cooperative nodes are stopped only after source cleanup, using the process-instance
identity captured during migration. A node acknowledges a prepared stop before
that acknowledgement is persisted as an instance-specific stop confirmation and
final shutdown is requested. The confirmation remains durable after the active
change completes, so a node that temporarily loses coordinator connectivity still
shuts down when it reconnects; a lost final response is recoverable without treating
a transient outage as a successful stop. Interrupted coordinator work resumes
automatically. Pre-publication cleanup has its own deadline and remains in a
recoverable `Aborting` state until authoritative sources acknowledge it; a
post-publication failure never rolls the epoch back.

## Request and error semantics

- Each key has one owner. `PUT` and `GET` are linearizable per key while the
  coordinator and owner are available.
- Versions are `(topology_epoch, owner_sequence, owner_node_id)` and migration
  preserves them.
- Logical operations use an overall deadline and bounded jittered retry.
- `Moved`, `RangeBusy`, retryable `Unavailable`, and retryable
  `ResourceExhausted` may be retried within that deadline.
- `NotFound`, `TooLarge`, `InvalidArgument`, and permanent gRPC statuses return
  immediately.
- If a `PUT` or mutating admin RPC was dispatched but its response was lost, the
  client reports `unknown_write_outcome=true`. Request IDs are correlation IDs,
  not persistent deduplication receipts; retrying a `PUT` may allocate a newer
  version for the same value.
- Key and value limits are 64 KiB and 8 MiB. A node's aggregate migration-journal
  budget defaults to 16 MiB. A cluster supports at most 1,024 members, 4,096
  virtual nodes per member, and 1,048,576 total assignments. A single membership
  change may move at most 16,384 ranges; these bounds keep topology and change
  snapshots within the 64 MiB control-plane message limit.

Replication, automatic failure recovery, heartbeat eviction, transactions,
`DELETE`, durable data-node storage, and coordinator consensus are intentionally
out of scope. Unexpected owner loss makes that owner's data unavailable.

## Tests

```sh
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```
