# Configuration

Every flag of the long-running commands, the YAML key that mirrors it, its
default, and its scope. `--help` lists the same flags; this page adds the scope
and the rules that keep a setting safe. Values that are fixed in the code are
listed under [timers and budgets](operations.md#timers-and-budgets).

## How settings resolve

`coordinator`, `node`, `redis-gateway` and `experiment` take `--config <file>`, a
YAML file whose keys are the flag names with `_` for `-`. For each setting, a flag
on the command line wins over the file, and the file wins over the default.
Unknown keys are an error. A relative path in a file is resolved against the
file's directory. Boolean experiment flags can turn a setting on but not off.

The examples list every key: [`coordinator.yaml`](../examples/coordinator.yaml),
[`node.yaml`](../examples/node.yaml),
[`redis-gateway.yaml`](../examples/redis-gateway.yaml) and
[`experiment.yaml`](../examples/experiment.yaml).

## Scopes

| Scope | Meaning |
| --- | --- |
| process | Affects only the process that reads it. Change it on any restart. |
| per node | A node's own budget or timeout. Nodes in one cluster may differ. |
| bootstrap | Read only when the coordinator creates a new state store, then stored in the topology. For an existing store they are ignored, except that with `--member` flags every bootstrap setting must match the store. |
| bootstrap, fixed | Bootstrap settings with no later change command. Every topology copies them, which is what makes them safe to depend on. |
| experiment | Shapes an experiment run; recorded in its manifest. |

## `coordinator`

| Flag | YAML key | Default | Scope | Notes |
| --- | --- | --- | --- | --- |
| `--config` | — | none | process | |
| `--listen` | `listen` | `127.0.0.1:50050` | process | |
| `--state` | `state` | `./coordinator.redb` | process | The durable store. |
| `--migration-timeout-ms` | `migration_timeout_ms` | 120000 | process | Bounds one membership change. |
| `--range-move-concurrency` | `range_move_concurrency` | 16 | process | Disjoint ranges moved at once in one change. |
| `--replica-repair-concurrency` | `replica_repair_concurrency` | 4 | process | Follower groups seeded, merged, or repaired at once, and ranges copied at once within one group. Independent of `--range-move-concurrency`. |
| `--prepublication-failure-grace-ms` | `prepublication_failure_grace_ms` | 30000 | process | How long a migration participant may stay unreachable before publication before the coordinator recovers the change without it. After publication, the lease duration applies instead. |
| `--worker-threads` | `worker_threads` | one per available CPU | process | |
| `--member` | `members` | none | bootstrap | `NODE_ID=ENDPOINT`; in YAML a list of `{node_id, endpoint}`. Explicit flags must match an existing store. |
| `--seed` | `seed` | 1 | bootstrap | Hash seed. |
| `--virtual-nodes` | `virtual_nodes` | 128 | bootstrap | Per member. |
| `--desired-replication-factor` | `desired_replication_factor` | 3 | bootstrap | Change later with `begin-config-change`. |
| `--minimum-admitted-copies` | `minimum_admitted_copies` | 1 | bootstrap | Write guard; change later with `begin-config-change`. |
| `--minimum-healthy-followers` | `minimum_healthy_followers` | 0 | bootstrap | Write guard; change later with `begin-config-change`. |
| `--max-replica-lag-ms` | `max_replica_lag_ms` | 5000 | bootstrap | Write guard; change later with `begin-config-change`. |
| `--receipt-retention-ms` | `receipt_retention_ms` | 60000 | bootstrap, fixed | How long every replica keeps a deduplication receipt after applying a mutation. Clients stop retrying a mutation within it, so a retry cannot apply twice. Between 1 s and 24 h. |
| `--lease-duration-ms` | `lease_duration_ms` | 5000 | bootstrap, fixed | Node lease grants. Nodes renew every fifth of it, with a renewal timeout of two fifths. It also sets how long peer failures must persist before failover and how long a restarted coordinator waits before acting on leases. Between 1 s and 24 h. |

## `node`

| Flag | YAML key | Default | Scope | Notes |
| --- | --- | --- | --- | --- |
| `--config` | — | none | process | |
| `--id` | `id` | required | process | Must be a member of the committed or pending topology. |
| `--listen` | `listen` | required | process | |
| `--coordinator` | `coordinator` | `http://127.0.0.1:50050` | process | |
| `--worker-threads` | `worker_threads` | one per available CPU | process | Lower it when the node shares its host. |
| `--max-dedup-bytes` | `max_dedup_bytes` | 134217728 (128 MiB) | per node | Retained deduplication receipts, and separately each migration destination's staged receipts. When full, the node rejects writes before applying them, with a retryable error, until receipts expire. |
| `--max-value-bytes` | `max_value_bytes` | 8388608 (8 MiB) | per node | The largest value the node accepts; a larger one is refused before it is written. Nodes may differ, and clients are checked against the largest the cluster allows, not against any one node. Raising it also raises what a replication batch and a journaled write need, so `--max-pending-replication-bytes` and `--max-migration-journal-bytes` must be at least as large or the node will not start. |
| `--max-pending-replication-bytes` | `max_pending_replication_bytes` | 67108864 (64 MiB) | per node | Mutations queued for followers, and separately the payloads held by in-flight replication RPCs. A mutation larger than the budget is always rejected. |
| `--max-migration-journal-bytes` | `max_migration_journal_bytes` | 16777216 (16 MiB) | per node | Writes journaled across the node's active migrations. When full, writes to moving ranges get a retryable rejection. |
| `--replication-stream-queue-capacity` | `replication_stream_queue_capacity` | 32 | per node | Mutations queued per follower stream, whatever their size, so it limits small writes before the byte budget does. |
| `--required-ack-timeout-ms` | `required_ack_timeout_ms` | 2000 | per node | How long a write waits for its required follower ACKs before reporting an unknown outcome. |
| `--replication-rpc-timeout-ms` | `replication_rpc_timeout_ms` | 2000 | per node | Bounds each replication connect and batch RPC; a timed-out batch is retried. |
| `--replication-batch-linger-ms` | `replication_batch_linger_ms` | 2 | per node | How long a follower stream waits to fill a replication batch. It never waits once a write in the batch is waiting for that follower's ACK. Under `owner-only` it also delays the first copy of an acknowledged write by up to this long. 0 sends at once. See [replication-batch-linger.md](optimizations/replication-batch-linger.md). |

## `redis-gateway`

Serves the Redis protocol on `--listen` and reaches the cluster through
`--coordinator`. The port may be fronted by any number of gateway processes;
see [Redis compatibility](redis-compatibility.md) for what that does and does
not guarantee.

| Flag | YAML key | Default | Scope | Notes |
| --- | --- | --- | --- | --- |
| `--config` | — | none | process | |
| `--listen` | `listen` | `127.0.0.1:6379` | process | |
| `--coordinator` | `coordinator` | `http://127.0.0.1:50050` | process | Contacted on the first command, so the gateway may start before the coordinator. |
| `--deadline-ms` | `deadline_ms` | 5000 | process | How long one command may take against the cluster. Redis has no such deadline; a proxy in front of a migrating cluster needs one. |
| `--requirepass` | `requirepass` | none | process | Falls back to the `HASHRING_GATEWAY_PASSWORD` environment variable, which keeps the secret out of both the file and the process list. |
| `--maxclients` | `maxclients` | 10000 | process | Connections open at once. One more is answered `-ERR max number of clients reached` and closed, as Redis does. |
| `--worker-threads` | `worker_threads` | one per available CPU | process | |

## `redis-lua-experimental-gateway`

The [purely experimental Lua gateway](experimental-redis.md) requires the
`redis-lua-experimental` Cargo feature. It uses the same flags, YAML settings,
defaults and password environment variable as [`redis-gateway`](#redis-gateway),
plus this setting:

| Flag | YAML key | Default | Scope | Notes |
| --- | --- | --- | --- | --- |
| `--max-bulk-bytes` | `max_bulk_bytes` | 16777216 (16 MiB) | process | Maximum size of one RESP bulk string, including a value or script source. Must be positive and at most 134217728 (128 MiB). Nodes enforce their own value admission limit. |

```sh
cargo run --features redis-lua-experimental -- redis-lua-experimental-gateway \
  --config examples/experimental-redis-gateway.yaml
```

Flags override the file, which overrides defaults. Enabling the feature does not
add scripting to the ordinary `redis-gateway` command.

## `experiment`

The run starts its own coordinator and nodes. It passes the bootstrap settings to
the coordinator and the node tuning flags to every node, including nodes added or
replaced during the run.

| Flag | YAML key | Default | Scope | Notes |
| --- | --- | --- | --- | --- |
| `--config` | — | none | process | |
| `--mode` | `mode` | `correctness` | experiment | `correctness`, `performance`, `availability`, `latency` or `scaling`. |
| `--output` | `output` | required | experiment | New or empty directory. |
| `--nodes` | `nodes` | 3 + rounds for availability, 10 otherwise | experiment | Peak node count. The `availability` mode starts with the node count minus one node per round, and uses the rest as the replacements it joins after every failure. |
| `--keys` | `keys` | 20,000 / 1,000,000 / 1,000 / 100,000 by mode | experiment | The `latency` mode reads it as the key count of the run, which is its `--preload-keys`. |
| `--value-bytes` | `value_bytes` | 128 | experiment | |
| `--concurrency` | `concurrency` | 64 | experiment | The `latency` mode uses it for the preload and the check, not for the load. The `scaling` mode uses it for its read tasks and verification. |
| `--operation-timeout-ms` | `operation_timeout_ms` | 30000 | experiment | At most `--receipt-retention-ms`, since mutations stop retrying there. |
| `--migration-timeout-ms` | `migration_timeout_ms` | 600000 | experiment | |
| `--pre-publish-delay-ms` | `pre_publish_delay_ms` | 250; 0 for scaling | experiment | Observation window before each publication. It runs after writes are paused, so a scaling run defaults it to 0. |
| `--require-clean-source` | `require_clean_source` | off | experiment | Refuse a run whose source is not one clean commit. |
| `--verbose` | `verbose` | off | experiment | |
| `--policy` | `policy` | `first-successor` for availability, `owner-only` otherwise | experiment | ACK policy. |
| `--preload-keys` | `preload_keys` | 100,000 | experiment | `latency` mode only: the keys written before the load, and the key space the load reads and writes. |
| `--target-rates` | `target_rates` | none | experiment | `latency` mode only: the target rates in operations per second, one stage each. A list in YAML. |
| `--warmup-seconds` | `warmup_seconds` | 10 | experiment | `latency` mode: the seconds at the start of every stage that are not counted. `scaling` mode: the read/write warmup before the first change. |
| `--duration-seconds` | `duration_seconds` | 60 | experiment | `latency` mode only: the seconds of every stage that are counted. |
| `--mix` | `mix` | `90:9:1` | experiment | `latency` mode only: the `GET:PUT:DELETE` share of the requests. |
| `--key-distribution` | `key_distribution` | `zipf:0.99` | experiment | `latency` and `scaling` modes: `uniform`, `zipf:<theta>`, or `hotspot:<key share>:<traffic share>`. |
| `--max-in-flight` | `max_in_flight` | 4096 | experiment | `latency` mode only: how many requests may be in flight at once. A request over the limit is dropped and counted as late. |
| `--availability-rounds` | `availability_rounds` | 1 | experiment | `availability` mode only: how many consecutive owner failures the run injects. Every round kills the current owner of the probe key and then joins a replacement, so the run needs one spare node per round. |
| `--scaling-scenario` | `scaling_scenario` | `round-trip` | experiment | `scaling` mode only: `round-trip` (grow one, then shrink), `grow`, `shrink`, `replace` (swap one member for a spare), `churn` (grow by three, then shrink), or `control` (no membership change). |
| `--scaling-load-mode` | `scaling_load_mode` | `closed` | experiment | `scaling` mode only: `closed` for a fixed number of workers, `open` for requests offered at `--scaling-open-rate`. |
| `--scaling-open-rate` | `scaling_open_rate` | 0 | experiment | `scaling` mode only: total operations per second in open mode. Reads are 80%; writes are 12% rewrite, 5% fresh write, and 3% delete. The pools are sized from this rate. |
| `--initial-nodes` | `initial_nodes` | peak - 1 for grow/round-trip/replace, peak for shrink/control, peak - 3 for churn | experiment | `scaling` mode only: the nodes the run starts with. |
| `--scaling-write-rate` | `scaling_write_rate` | 200 | experiment | `scaling` mode only: closed-loop one-shot write and delete operations per second. Reads are not paced. |
| `--scaling-pool-seconds` | `scaling_pool_seconds` | 120 | experiment | `scaling` mode only: how long the one-shot pools of one transition must last. |
| `--scaling-post-seconds` | `scaling_post_seconds` | 10 | experiment | `scaling` mode only: how long the load keeps running after a change. In `control` it is the measured baseline window. |
| `--worker-threads` | `worker_threads` | CPUs / (nodes + 2), at least 2 | process | For the driver and each node. |
| `--coordinator-worker-threads` | `coordinator_worker_threads` | one per available CPU | process | For the run's coordinator, which is not on the data path. |
| `--seed`, `--virtual-nodes`, `--range-move-concurrency`, `--replica-repair-concurrency`, `--prepublication-failure-grace-ms`, `--desired-replication-factor`, `--minimum-admitted-copies`, `--minimum-healthy-followers`, `--max-replica-lag-ms`, `--receipt-retention-ms`, `--lease-duration-ms` | as for `coordinator` | as for `coordinator` | as for `coordinator` | Passed to the run's coordinator. |
| `--max-dedup-bytes`, `--max-value-bytes`, `--max-pending-replication-bytes`, `--max-migration-journal-bytes`, `--replication-stream-queue-capacity`, `--required-ack-timeout-ms`, `--replication-rpc-timeout-ms`, `--replication-batch-linger-ms` | as for `node` | as for `node` | per node | Passed to every node of the run. |

## Client and admin commands

`put`, `get`, `delete`, `topology`, `change-status`, `replica-status` and the
`begin-*` commands take `--coordinator` (default `http://127.0.0.1:50050`) and
`--deadline-ms` (default 2000). `put` and `delete` stop retrying after the
cluster's receipt retention period, even with a longer deadline, because a
later retry of the same mutation could apply it twice. `execute-change`
defaults to a 120000 ms deadline. These commands have no config file.
