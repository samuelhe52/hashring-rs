# Redis Cluster comparison

These scripts start a real Redis Cluster next to a hashring-rs cluster and
measure both with the same client, so the two numbers of a case differ only
in the system under test.

| File | What it does |
| --- | --- |
| [`redis-cluster.sh`](redis-cluster.sh) | Starts, stops and inspects a local Redis Cluster. Every node is a `redis-server` process with its own port, directory and cluster configuration file, so no container runtime is needed. |
| [`hashring-cluster.sh`](hashring-cluster.sh) | Starts, stops and inspects a hashring-rs cluster: one coordinator, N data nodes and one or more Redis gateways. |
| [`compare.py`](compare.py) | Runs the comparison and writes `measurements.csv`, `summary.json` and `metadata.json`. |
| [`paired-summary.py`](paired-summary.py) | Prints the comparison of the variants of a run, with each variant against the control of its own repetition. |
| [`gateway-profile.py`](gateway-profile.py) | Drives writes and reads through the gateway with `HASHRING_PROFILE_RPC=1` and reports what the nodes spent inside their handlers, against what the client waited for. |

## Running it

Build the binaries first, then point the harness at a Redis installation:

```sh
cargo build --release --bin hashring-rs
make -C /path/to/redis MALLOC=jemalloc        # or use a packaged redis-server

python3 scripts/redis-cluster/compare.py \
  --output results/redis-cluster \
  --cases 3m,3m1r,3m-gw3,6m \
  --reps 3 --seconds 15 \
  --redis-bin /path/to/redis/src
```

`--redis-bin` is the directory holding `redis-server`, `redis-cli` and
`redis-benchmark`. The Redis Cluster needs a `redis-server` built with cluster
support, which every standard build has.

The cases are:

| Case | Redis | hashring-rs |
| --- | --- | --- |
| `3m` | 3 masters | 3 nodes, RF=1, 1 gateway |
| `3m1r` | 3 masters, 1 replica each | 3 nodes, RF=2, 1 gateway |
| `3m-gw3` | 3 masters | 3 nodes, RF=1, 3 gateways |
| `6m` | 6 masters | 6 nodes, RF=1, 1 gateway |
| `6m1r` | 6 masters, 1 replica each | 6 nodes, RF=2, 1 gateway |

## How the two systems are driven

Both sides run the same `redis-benchmark` binary with the same command
templates, the same number of client connections, the same key space and the
same value size. The systems run one after the other, never at the same time,
so they never compete for the same CPUs.

Redis Cluster routes by slot, and `redis-benchmark` cannot route by itself, so
one benchmark process per master runs against that master with a key template
whose hash tag falls in that master's slot range. The aggregate of those
processes is the cluster's number, and no client is redirected.

The gateway routes by the ring, so one benchmark process per gateway runs
against the cluster with the same key template, and the aggregate of those
processes is the cluster's number.

A `redis-benchmark` process tops out around 130k requests per second, so the
harness runs `--client-procs` processes per endpoint. Without that, the
measurement would be of the client rather than of the server. The `--clients`
connections are split over all processes, so the total is the same on both
sides whatever the number of endpoints.

`redis-benchmark` expands only `__rand_int__` in a custom command, so the
harness builds the `--value-bytes` value itself rather than using `__data__`
and `-d`.

`redis-benchmark` has no time limit, so every phase runs a pilot first and the
measured phase is then long enough to cover the same wall-clock time on both
systems. The pilot doubles as the warmup. Every process gets a request count
in proportion to its role's pilot rate and its connections, so the processes
of a phase finish together, and the throughput of a phase is all completed
requests over the wall-clock time of the phase. `finish_spread` in
`measurements.csv` is the slowest process's run time over the fastest's.

The harness refuses to start when one of its ports is in use; it does not stop
processes it did not start. `--keep` leaves the clusters of the last case
running.

The node-side handler times come from a separate run:

```sh
python3 scripts/redis-cluster/gateway-profile.py \
  --output results/node-profile --redis-bin /path/to/redis/src
```

## Comparing settings of the hashring side

`--variants` measures the hashring side in more than one setting. Each variant
is `name:connection-pool:batch-entries:linger-ms`:

```sh
python3 scripts/redis-cluster/compare.py \
  --output results/ab-batch \
  --cases 3m1r \
  --variants "b64l2:1:64:2,b256l2:1:256:2,b16l2:1:16:2,b64l0:1:64:0" \
  --reps 3 --seconds 15
```

The variants run in turn inside every repetition, and the Redis control is
measured right next to each of them, so a load that arrives while the run is
going hits every variant instead of only the one that happens to be running.
The hashring cluster is restarted per variant, because these are process
settings, and each variant runs its own pilot, so every measured phase covers
the same wall-clock time.

The number to compare is the ratio of the hashring throughput to the control
of the same repetition: the host's load moves both systems, and the ratio
moves with them. `paired-summary.py` prints that table:

```sh
python3 scripts/redis-cluster/paired-summary.py results/ab-batch
```

Two more flags make the structure of a phase visible:

| Flag | What it does |
| --- | --- |
| `--profile-rpc` | Runs the hashring processes with `HASHRING_PROFILE_RPC=1` and writes `rpc_profile.csv`: the requests, the replication RPCs and the entries per replication batch of every process, per phase. |
| `--settle-seconds` | Waits between the write phase and the read phase. The write phase leaves the replication behind, and without the wait the read phase measures a cluster that is still catching up. |

## Tuning the hashring-rs side

Two budgets are raised for the measurement, through the environment:

| Variable | Default in the project | Value used | Why |
| --- | --- | --- | --- |
| `HASHRING_MAX_DEDUP_BYTES` | 128 MiB per node | 2 GiB | A node keeps one deduplication receipt per write for the receipt retention period, so the default store fills after a few seconds of sustained writes and the node then rejects writes. The measured numbers would describe the receipt store rather than the data path. |
| `HASHRING_GATEWAY_DEADLINE_MS` | 5000 | 60000 | Redis has no deadline. A saturated gateway should not answer `TRYAGAIN` while it is still making progress, so the deadline is raised far enough that a slow phase is slow rather than failed. |
| `HASHRING_MAX_PENDING_REPLICATION_BYTES` | 64 MiB per node | 512 MiB | The same reason, for the queue of mutations waiting for a follower. |

## Running one system by hand

```sh
# A Redis Cluster with three masters on ports 7100-7102.
REDIS_BIN_DIR=/path/to/redis/src \
  bash scripts/redis-cluster/redis-cluster.sh start /tmp/redis 3 0 7100
/tmp/redis/7100/redis.log

# The same cluster, stopped.
bash scripts/redis-cluster/redis-cluster.sh stop /tmp/redis

# A hashring-rs cluster with three nodes and one gateway.
bash scripts/redis-cluster/hashring-cluster.sh start /tmp/hashring 3 1 1
redis-cli -p 6399 set greeting hello
bash scripts/redis-cluster/hashring-cluster.sh stop /tmp/hashring
```

The results of the run in this directory are written to
[docs/redis-cluster-comparison.md](../../docs/redis-cluster-comparison.md).
