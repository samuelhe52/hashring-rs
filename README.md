# hashring-rs

A replicated, in-memory key-value cache built on consistent hashing.

- **Single owner per key.** Each virtual-node range on the hash ring has one
  owner and up to `RF-1` followers on distinct physical nodes. `PUT`, `GET`,
  and `DELETE` are linearizable per key.
- **Durable coordinator.** One coordinator stores membership, epochs, token
  assignments, and in-flight migrations in a transactional
  [redb](https://github.com/cberner/redb) file. Data nodes keep everything in
  memory.
- **Online membership changes.** Ranges move while the cluster serves traffic.
  Writes to a moving range pause briefly at cutover. Reads continue throughout.
- **One binary, plain gRPC.** The same executable runs the coordinator, a data
  node, or the client/admin CLI. Processes can run locally, in containers, or
  on separate hosts.

> **Status: experimental.** There are no compatibility guarantees for the CLI,
> wire protocol, or coordinator state file. You may have to delete old state
> after upgrading.

## Quick start

Start a coordinator with three members, then the three data nodes, each in its
own terminal:

```sh
cargo run -- coordinator --state ./coordinator.redb \
  --member node-1=http://127.0.0.1:50051 \
  --member node-2=http://127.0.0.1:50052 \
  --member node-3=http://127.0.0.1:50053

cargo run -- node --id node-1 --listen 127.0.0.1:50051
cargo run -- node --id node-2 --listen 127.0.0.1:50052
cargo run -- node --id node-3 --listen 127.0.0.1:50053
```

The coordinator listens on `127.0.0.1:50050` by default, and every other
command connects there unless you pass `--coordinator`.

Read and write some keys:

```sh
cargo run -- put    --key greeting --value hello
cargo run -- get    --key greeting
cargo run -- delete --key greeting
```

Use `--key-hex`, `--value-hex`, and `get --hex` for binary data.

Inspect the cluster:

```sh
cargo run -- topology        # ring layout as JSON
cargo run -- replica-status  # per-range owner, followers, lag, and repair state
```

`--member` is only read when the state file is first created. To restart the
coordinator, pass just `--state`. Everything else is loaded from the file.
Passing `--member` on a restart compares those flags with the stored topology,
so a cluster whose membership or configuration changed since the store was
created has to be restarted with `--state` alone.

## Redis clients

`redis-gateway` serves the Redis protocol on a port over this cluster, so
`redis-cli`, redis-py, Jedis, Lettuce and go-redis can drive it unmodified:

```sh
cargo run -- redis-gateway --listen 127.0.0.1:6379 \
  --coordinator http://127.0.0.1:50050
```

It answers the string and key commands, the connection handshake, and the
expiry commands, and declines the rest with a clear error rather than a wrong
answer. It is **not** Redis and not Redis Cluster: there are no data structures,
no transactions or scripting, and no atomicity across keys. Keyspace commands
include `KEYS`, `SCAN`, `DBSIZE` and `RANDOMKEY`.
Read-modify-write commands such as `INCR` are conditional writes the
owning node checks, so they stay atomic however many gateways are running.
[docs/redis-compatibility.md](docs/redis-compatibility.md) documents every
command, every difference, and what each limitation would cost to close.

Lua scripting is available separately in the purely experimental
[`hashring-redis-lua-experimental`](docs/experimental-redis.md) crate:

```sh
cargo run --features redis-lua-experimental -- redis-lua-experimental-gateway \
  --config examples/experimental-redis-gateway.yaml
```

The feature enables this additional command; `redis-gateway` continues to run
without scripting, even in a build with the feature enabled.

Expiry itself belongs to the cluster, not the gateway: a deadline is a field on
the record, so it survives failover, migration and restart. `SET key value EX
60` and `EXPIRE` work the same way whether or not a gateway is in front.

How the gateway path compares with a real Redis Cluster of the same shape is
measured in [docs/redis-cluster-comparison.md](docs/redis-cluster-comparison.md).

The same settings can live in a YAML file instead. Keys use the flag names with
`_` for `-`, except that `--member` becomes a `members` list of
`{node_id, endpoint}`. A flag on the command line wins over the file (see
[`examples/coordinator.yaml`](examples/coordinator.yaml)):

```sh
cargo run -- coordinator --config examples/coordinator.yaml
```

A relative `state` path in the file is resolved against the file's directory.
Unknown keys are an error. When the state file already exists, the coordinator
ignores the bootstrap keys in the config file (`members`, `seed`,
`virtual_nodes`, replication factor, and write guard), so the same file works
for every restart. Explicit `--member` flags must still match the stored
topology.

Nodes take a config file too (see [`examples/node.yaml`](examples/node.yaml)).
[docs/configuration.md](docs/configuration.md) lists every flag and key with
its default and scope.

## Commands

| Command                          | Purpose                                                                                              |
| -------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `coordinator`                    | Run the topology coordinator                                                                         |
| `node`                           | Run an in-memory data node                                                                           |
| `put` / `get` / `delete`         | Single-key operations                                                                                |
| `topology`                       | Print the committed topology                                                                         |
| `replica-status`                 | Print per-range replication, liveness, and write readiness                                           |
| `begin-change`                   | Stage a new membership                                                                               |
| `begin-policy-change`            | Stage a new acknowledgement policy                                                                   |
| `begin-config-change`            | Stage a new replication factor or write guard                                                        |
| `execute-change`                 | Run the staged change through to publication                                                         |
| `change-status`                  | Show the active change, if any                                                                       |
| `redis-gateway`                  | Serve the Redis protocol on a port, over this cluster                                                |
| `redis-lua-experimental-gateway` | Serve the shared gateway with experimental Lua support; requires `--features redis-lua-experimental` |
| `experiment`                     | Run a reproducible multi-process experiment                                                          |

Run `cargo run -- <command> --help` for each command's flags.
For the Lua command, use
`cargo run --features redis-lua-experimental -- redis-lua-experimental-gateway --help`.

## Replication and guarantees

### Acknowledgement policies

| Policy                | A write succeeds when…                           | If the owner crashes                                                                                                 |
| --------------------- | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| `OwnerOnly` (default) | the owner has applied it                         | **Acknowledged writes that the promoted follower had not yet applied are lost.** A key can revert to an older value. |
| `FirstSuccessor`      | the first clockwise follower has also applied it | No loss from a single owner failure. Correlated failures can still lose data.                                        |
| `AllReplicas`         | every desired follower has applied it            | Writes block unless the full placement is healthy.                                                                   |

Followers are always updated asynchronously in the background. The policy only
decides how long the client waits.

Data nodes are in-memory under every policy. Losing enough processes loses data.
`OwnerOnly` has no time bound on its loss window: `--max-replica-lag-ms`
affects only the write guard and is not a recovery-point objective.

### Write guard

The write guard is a readiness check that runs before a write is accepted:

- `--minimum-admitted-copies` (default 1, so the owner alone is enough)
- `--minimum-healthy-followers` (default 0)
- `--max-replica-lag-ms` (default 5000), which defines a "healthy" follower

When the guard or the ACK policy cannot be met, writes fail. The cluster does
not silently weaken the guarantee. A follower is admitted only after it has
caught up from a verified snapshot and stream. The guard controls availability
only; it does not make data durable.

### Request semantics

- **Versions** are `(topology_epoch, owner_sequence, owner_node_id)` and
  migrations preserve them.
- **`DELETE` is idempotent.** Success means the key is absent afterward,
  whether or not it existed before.
- **Retries.** Every logical operation has one overall deadline. Within it, the
  client retries `Moved`, `RangeBusy`, and retryable `Unavailable` or
  `ResourceExhausted` responses with jittered backoff, refreshing the topology
  as needed. `NotFound`, `TooLarge`, `InvalidArgument`, and permanent gRPC
  errors return immediately.
- **Unknown outcomes.** If a mutation was sent but its response was lost, the
  final error reports `unknown_write_outcome=true`. A follow-up `DELETE` that
  succeeds confirms the key is absent.
- **Deduplication.** The client keeps one request ID across retries of a
  logical write. Nodes keep in-memory receipts for the cluster's receipt
  retention period (60 seconds by default), so a retry is not applied twice,
  and the client stops retrying a mutation within that period. A new CLI
  invocation uses a new ID, and receipts are lost if the node restarts.

### Limits

| Limit                                | Value     |
| ------------------------------------ | --------- |
| Key size                             | 64 KiB    |
| Value size                           | 8 MiB     |
| Members per cluster                  | 1,024     |
| Virtual nodes per member             | 4,096     |
| Total token assignments              | 1,048,576 |
| Ranges moved per change              | 16,384    |
| Pending replication buffer per node  | 64 MiB    |
| Deduplication receipt store per node | 128 MiB   |
| Migration journal per node (default) | 16 MiB    |

### Not provided

Transactions, durable data-node storage, and coordinator failover or consensus.

## Changing the cluster

All changes follow the same two steps. `begin-*` stages the change and prints
its ID and epochs. `execute-change` then runs it. Passing the ID and both
epochs back means an ambiguous retry can never execute a different change.

### Add or remove nodes

Pass the **complete** target membership. For example, to add `node-4`:

```sh
cargo run -- begin-change \
  --member node-1=http://127.0.0.1:50051 \
  --member node-2=http://127.0.0.1:50052 \
  --member node-3=http://127.0.0.1:50053 \
  --member node-4=http://127.0.0.1:50054

# Start the new node once it appears in the pending topology.
cargo run -- node --id node-4 --listen 127.0.0.1:50054

# Use the change ID and epochs printed by begin-change.
cargo run -- execute-change --change-id CHANGE_ID --base-epoch 1 --target-epoch 2

cargo run -- change-status
cargo run -- replica-status
```

Rules:

- Existing nodes must keep their endpoints. Each node ID is fenced to the
  process instance that registered it, so the coordinator rejects an empty
  replacement under the same ID instead of letting it serve false misses.
- Removed nodes are told to shut down once their ranges have been handed off.
- Interrupted changes resume automatically when the coordinator restarts.

What happens during a move: the source keeps serving while a snapshot is
copied. Up to 16 ranges move in parallel (`--range-move-concurrency`).
Concurrent writes are replayed from a changelog. At cutover, affected writes
get a retryable `RangeBusy`. The coordinator then checks the record count,
watermark, and a BLAKE3 digest before it publishes the new epoch. A failure
before publication aborts cleanly. After publication, the epoch never rolls
back.

See [docs/operations.md](docs/operations.md) for failure recovery during a
change, `replica-status` fields, and internal timers.

### Change the ACK policy, RF, or write guard

```sh
cargo run -- begin-policy-change --policy first-successor
# or: cargo run -- begin-config-change --desired-replication-factor 2
cargo run -- execute-change --change-id CHANGE_ID --base-epoch E --target-epoch E_PLUS_1
```

Writes pause briefly, and the new epoch is published only after its readiness
check passes. See
[replica ACK performance](docs/optimizations/replica-ack-performance.md) for
the cost of each policy.

## Experiments

`experiment` starts a coordinator and data nodes as real child processes,
drives a workload through the client, and writes the results into a new or
empty directory:

```sh
cargo build --release

# Scale 9 → 10 → 9 nodes under concurrent writes and deletes, verifying every key.
target/release/hashring-rs experiment --mode correctness --require-clean-source \
  --output results/correctness

# Throughput and latency with 10 nodes and 1,000,000 keys.
target/release/hashring-rs experiment --mode performance --require-clean-source \
  --output results/performance

# Kill an owner, measure failover, data loss, and repair time.
target/release/hashring-rs experiment --mode availability --policy first-successor \
  --output results/availability

# Kill three owners in a row, replacing each one before the next failure.
target/release/hashring-rs experiment --mode availability --availability-rounds 3 \
  --nodes 9 --output results/availability-consecutive

# Offer a fixed rate and measure the latency of that load, one stage per rate.
target/release/hashring-rs experiment --mode latency --target-rates 5000,10000,20000 \
  --output results/latency

# Keep mixed reads and writes running across 9 -> 10 -> 9 nodes, and verify
# every cohort after each change.
target/release/hashring-rs experiment --mode scaling \
  --output results/scaling
```

Experiment settings can also come from a YAML file whose keys are the flag
names with `_` for `-` (see [`examples/experiment.yaml`](examples/experiment.yaml)).
Flags on the command line win over the file:

```sh
target/release/hashring-rs experiment --config examples/experiment.yaml \
  --output results/availability
```

The output directory holds a manifest of the configuration and build, an
event log, per-process logs, the coordinator database, and a JSON summary. See
[docs/experiments.md](docs/experiments.md) for defaults, summary fields,
profiling, and how to compare runs.

## Repository layout

| Path                          | Contents                                                                          |
| ----------------------------- | --------------------------------------------------------------------------------- |
| `src/`                        | The `hashring-rs` binary (CLI)                                                    |
| `crates/hashring-core`        | Protobuf contract, topology and migration types, shared limits                    |
| `crates/hashring-client`      | Routing, deadlines, retries, client errors                                        |
| `crates/hashring-coordinator` | Durable cluster state and migration orchestration                                 |
| `crates/hashring-node`        | In-memory storage, replication, and the data-node service                         |
| `crates/hashring-redis`       | The Redis-protocol gateway over the cluster                                       |
| `crates/hashring-experiment`  | Multi-process experiment runner                                                   |
| `docs/report/`                | [Preliminary report](docs/report/preliminary-report.md) on the design and results |
| `docs/configuration.md`       | Every flag and config key, with defaults and scopes                               |
| `docs/redis-compatibility.md` | What the Redis gateway answers, and where it differs                              |
| `docs/rfcs/`                  | Design RFCs                                                                       |
| `docs/optimizations/`         | Performance work, with measurements                                               |

The client, coordinator, and node crates depend on `hashring-core`, and none
of them depend on each other.

## Development

```sh
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

The default test run skips the full correctness, latency, and availability
experiments and the 96 MiB migration transport check. Run those four explicitly:

```sh
cargo test --test process_cluster -- --ignored
```

The node crate also has separately ignored microbenchmarks and an isolated RSS
regression check; run those individually as described in `docs/optimizations/`.

## License

Copyright (C) 2026 Samuel He, Mingyuan Chen, and Yang Chen. Licensed under the [GNU Affero General Public License v3.0](LICENSE).
