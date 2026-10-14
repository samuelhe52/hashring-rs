# Configuration

Every flag of the long-running commands, the YAML key that mirrors it, its
default, and its scope. `--help` lists the same flags; this page adds the scope
and the rules that keep a setting safe. Values that are fixed in the code are
listed under [timers and budgets](operations.md#timers-and-budgets).

## How settings resolve

`coordinator`, `node` and `experiment` take `--config <file>`, a YAML file whose
keys are the flag names with `_` for `-`. For each setting, a flag on the
command line wins over the file, and the file wins over the default. Unknown
keys are an error. A relative path in a file is resolved against the file's
directory. Boolean experiment flags can turn a setting on but not off.

The examples list every key: [`coordinator.yaml`](../examples/coordinator.yaml),
[`node.yaml`](../examples/node.yaml) and
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
| `--max-pending-replication-bytes` | `max_pending_replication_bytes` | 67108864 (64 MiB) | per node | Mutations queued for followers, and separately the payloads held by in-flight replication RPCs. A mutation larger than the budget is always rejected. |
| `--max-migration-journal-bytes` | `max_migration_journal_bytes` | 16777216 (16 MiB) | per node | Writes journaled across the node's active migrations. When full, writes to moving ranges get a retryable rejection. |
| `--replication-stream-queue-capacity` | `replication_stream_queue_capacity` | 32 | per node | Mutations queued per follower stream, whatever their size, so it limits small writes before the byte budget does. |
| `--required-ack-timeout-ms` | `required_ack_timeout_ms` | 2000 | per node | How long a write waits for its required follower ACKs before reporting an unknown outcome. |
| `--replication-rpc-timeout-ms` | `replication_rpc_timeout_ms` | 2000 | per node | Bounds each replication connect and batch RPC; a timed-out batch is retried. |
| `--replication-batch-linger-ms` | `replication_batch_linger_ms` | 2 | per node | How long a follower stream waits to fill a replication batch. It never waits once a write in the batch is waiting for that follower's ACK. Under `owner-only` it also delays the first copy of an acknowledged write by up to this long. 0 sends at once. See [replication-batch-linger.md](optimizations/replication-batch-linger.md). |

## `experiment`

The run starts its own coordinator and nodes. It passes the bootstrap settings to
the coordinator and the node tuning flags to every node, including nodes added or
replaced during the run.

| Flag | YAML key | Default | Scope | Notes |
| --- | --- | --- | --- | --- |
| `--config` | — | none | process | |
| `--mode` | `mode` | `correctness` | experiment | `correctness`, `performance` or `availability`. |
| `--output` | `output` | required | experiment | New or empty directory. |
| `--nodes` | `nodes` | 4 for availability, 10 otherwise | experiment | Peak node count. |
| `--keys` | `keys` | 20,000 / 1,000,000 / 1,000 by mode | experiment | |
| `--value-bytes` | `value_bytes` | 128 | experiment | |
| `--concurrency` | `concurrency` | 64 | experiment | |
| `--operation-timeout-ms` | `operation_timeout_ms` | 30000 | experiment | At most `--receipt-retention-ms`, since mutations stop retrying there. |
| `--migration-timeout-ms` | `migration_timeout_ms` | 600000 | experiment | |
| `--pre-publish-delay-ms` | `pre_publish_delay_ms` | 250 | experiment | Observation window before each publication. |
| `--require-clean-source` | `require_clean_source` | off | experiment | Refuse a run whose source is not one clean commit. |
| `--verbose` | `verbose` | off | experiment | |
| `--policy` | `policy` | `first-successor` for availability, `owner-only` otherwise | experiment | ACK policy. |
| `--worker-threads` | `worker_threads` | CPUs / (nodes + 2), at least 2 | process | For the driver and each node. |
| `--coordinator-worker-threads` | `coordinator_worker_threads` | one per available CPU | process | For the run's coordinator, which is not on the data path. |
| `--seed`, `--virtual-nodes`, `--range-move-concurrency`, `--replica-repair-concurrency`, `--prepublication-failure-grace-ms`, `--desired-replication-factor`, `--minimum-admitted-copies`, `--minimum-healthy-followers`, `--max-replica-lag-ms`, `--receipt-retention-ms`, `--lease-duration-ms` | as for `coordinator` | as for `coordinator` | as for `coordinator` | Passed to the run's coordinator. |
| `--max-dedup-bytes`, `--max-pending-replication-bytes`, `--max-migration-journal-bytes`, `--replication-stream-queue-capacity`, `--required-ack-timeout-ms`, `--replication-rpc-timeout-ms`, `--replication-batch-linger-ms` | as for `node` | as for `node` | per node | Passed to every node of the run. |

## Client and admin commands

`put`, `get`, `delete`, `topology`, `change-status`, `replica-status` and the
`begin-*` commands take `--coordinator` (default `http://127.0.0.1:50050`) and
`--deadline-ms` (default 2000). `put` and `delete` stop retrying after the
cluster's receipt retention period, even with a longer deadline, because a
later retry of the same mutation could apply it twice. `execute-change`
defaults to a 120000 ms deadline. These commands have no config file.
