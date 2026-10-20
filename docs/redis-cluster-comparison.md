# Redis Cluster comparison

This page answers one question: **how fast is hashring-rs when a Redis client
talks to it, compared with a real Redis Cluster of the same shape?**

The answer measured here: with one Redis gateway in front, the cluster serves
about a fifth of the throughput of a Redis Cluster with the same number of
owners, and the latency of a command is three to nine times higher. The gap is
not in the ring, the storage or the replication: it is in the transport, and
the gateway process is the limit in the single-gateway setup. Three gateways
instead of one recover a sixth of the difference on reads and less on writes.

The numbers come from a real Redis 8.10.2 built from source, driven by
Redis' own `redis-benchmark`, against a hashring-rs cluster of the same shape
driven by the same binary with the same commands. Everything ran on one host,
one system at a time.

- The scripts are in [`scripts/redis-cluster/`](../scripts/redis-cluster).
- The raw measurements are in `results/redis-cluster-20261020/` (ignored by
  Git): [`measurements.csv`](../results/redis-cluster-20261020/measurements.csv),
  [`summary.json`](../results/redis-cluster-20261020/summary.json),
  [`metadata.json`](../results/redis-cluster-20261020/metadata.json) and
  [`run.log`](../results/redis-cluster-20261020/run.log).

## The two systems

| | Redis Cluster | hashring-rs |
| --- | --- | --- |
| Version | Redis 8.10.2, built from `download.redis.io` | commit `8771351`, release build |
| Nodes | `redis-server` processes with `cluster-enabled yes` | one coordinator, N data nodes, one Redis gateway |
| Routing | by slot, computed by the client | by the ring, computed by the client library |
| Replication | one replica per master, asynchronous | RF copies per range, asynchronous followers |
| Client | `redis-benchmark` 8.10.2 | `redis-benchmark` 8.10.2, the same binary |

The host had 24 logical CPUs and 30 GiB of RAM, and every process of both
systems ran on it, as did the benchmark client.

## Method

### The cases

| Case | Redis | hashring-rs |
| --- | --- | --- |
| `3m` | 3 masters | 3 nodes, RF=1, 1 gateway |
| `3m1r` | 3 masters, 1 replica each | 3 nodes, RF=2, 1 gateway |
| `3m-gw3` | 3 masters | 3 nodes, RF=1, 3 gateways |
| `6m` | 6 masters | 6 nodes, RF=1, 1 gateway |

RF=2 with three nodes holds the same number of copies as three masters with
one replica each: one owner and one follower per range, against one master and
one replica per slot range.

### How each side is driven

Redis Cluster routes by slot, and `redis-benchmark` cannot route by itself, so
the harness runs one benchmark process per master. Each process uses a key
template whose hash tag falls inside that master's slot range, so every key
belongs to the master it is sent to and no client is redirected. The aggregate
of those processes is the cluster's number.

The gateway routes by the ring, so the harness runs one benchmark process per
gateway with the same key template, and the aggregate of those processes is the
cluster's number.

Three workloads over a 300,000-key key space, with values of `--value-bytes`
(128 by default):

| Workload | Command | Meaning |
| --- | --- | --- |
| write | `set k:{tag}:__rand_int__ <value>` | a stream of writes over random keys |
| read | `get k:{tag}:__rand_int__` | a stream of reads of keys the write phase wrote |
| mixed | a writer and a reader at the same time | half the clients write, half read |

Reads run after the writes of the same repetition, so they hit keys that
exist. The harness checks the key count in the cluster after every write phase
(`DBSIZE`): 300,000 keys on the Redis side and 298,531 to 300,000 on the
hashring-rs side, so the reads are reads, not misses.

### The client

`redis-benchmark` has no time limit, only a request count, so every phase runs
a pilot first and the measured phase is then long enough to cover the same
wall-clock time on both systems. The pilot doubles as the warmup.

One `redis-benchmark` process tops out around 130,000 requests per second on
this host, which is below what the Redis side reaches. The harness therefore
runs two processes per endpoint, 64 client connections in total on both sides.
Without that, the measurement would be of the client.

The client is never the limit on the hashring-rs side: two, three and four
client processes against one gateway produce the same total throughput
(92.6k, 87.9k and 79.2k requests per second in a check before the run), while
the Redis side gains 34% when a second process is added per master.

### What was raised, and why

Three settings of the hashring-rs side were raised for the measurement, and
all of them matter for anyone running this workload:

| Setting | Project default | Used here | Why |
| --- | --- | --- | --- |
| `--max-dedup-bytes` | 128 MiB per node | 2 GiB | A node keeps one deduplication receipt per write for the receipt retention period (60 s). At the write rate measured here the default store fills after about ten seconds, the node then rejects writes, and the phase ends in `TRYAGAIN` before the deadline. The default is a real limit on sustained writes, not a detail of the benchmark; see [dedup-receipt-store-memory.md](optimizations/dedup-receipt-store-memory.md#the-budget-is-also-a-sustained-write-rate-limit). |
| `--deadline-ms` of the gateway | 5000 | 60000 | Redis has no deadline. A saturated gateway should be slow, not `TRYAGAIN`. |
| `--max-pending-replication-bytes` | 64 MiB per node | 512 MiB | The queue of mutations waiting for a follower, for the same reason. |

## Results

> **These numbers predate four fixes to the harness and are to be replaced by
> a rerun.** They are what was measured, but not under the method above:
>
> - **Every value was 8 bytes, not 128.** `redis-benchmark` expands only
>   `__rand_int__` in a custom command, so every `SET` stored the literal
>   `__data__`. The harness now builds the value itself.
> - **The Redis side had fewer connections in most cases.** The connections
>   were split by integer division: 60 on the Redis side against 64 through a
>   single gateway in `3m`, `3m1r` and `6m`, 48 against 64 in the `6m` mixed
>   phase, and 60 on both sides in `3m-gw3`. The harness now keeps the total
>   at 64 on both sides.
> - **The mixed rows overstate throughput.** The rate of a phase was the sum
>   of the rates of its processes, each over its own run time, and the reads
>   of the mixed phase were sized from the write pilot, so readers and
>   writers did not finish together. The harness now sizes every process by
>   its own role and its connections, and divides all completed requests by
>   the wall-clock time of the phase.
> - **About half of the profiled reads were misses** (see
>   [the node-side profile](#where-the-time-goes)). The profile now writes
>   every key before the read phase.

Medians of three repetitions, 15 seconds each, alternating which system runs
first between repetitions.

### Throughput and latency

| Case | Workload | Redis rps | hashring-rs rps | ratio | Redis p50 | hashring-rs p50 | Redis p99 | hashring-rs p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `3m` | write | 501,837 | 107,163 | 21% | 0.119 ms | 0.535 ms | 0.255 ms | 1.127 ms |
| `3m` | read | 519,605 | 100,365 | 19% | 0.111 ms | 0.615 ms | 0.239 ms | 1.095 ms |
| `3m` | mixed | 461,242 | 109,266 | 24% | 0.127 ms | 0.551 ms | 0.247 ms | 1.127 ms |
| `3m1r` | write | 457,634 | 88,290 | 19% | 0.135 ms | 0.655 ms | 0.263 ms | 1.399 ms |
| `3m1r` | read | 521,575 | 127,410 | 24% | 0.111 ms | 0.471 ms | 0.239 ms | 0.903 ms |
| `3m1r` | mixed | 442,436 | 105,534 | 24% | 0.135 ms | 0.551 ms | 0.255 ms | 1.223 ms |
| `3m-gw3` | write | 503,270 | 131,408 | 26% | 0.111 ms | 0.407 ms | 0.239 ms | 1.007 ms |
| `3m-gw3` | read | 510,949 | 169,462 | 33% | 0.111 ms | 0.319 ms | 0.239 ms | 0.751 ms |
| `3m-gw3` | mixed | 464,564 | 140,310 | 30% | 0.127 ms | 0.367 ms | 0.247 ms | 1.087 ms |
| `6m` | write | 692,359 | 80,197 | 12% | 0.079 ms | 0.727 ms | 0.207 ms | 1.599 ms |
| `6m` | read | 707,419 | 86,453 | 12% | 0.071 ms | 0.687 ms | 0.215 ms | 1.503 ms |
| `6m` | mixed | 635,758 | 81,639 | 13% | 0.071 ms | 0.727 ms | 0.175 ms | 1.567 ms |

The latency columns are the worst of the benchmark processes, so they are the
latency a client in the busiest position saw.

Redis scales with more masters: six masters reach 692,000 writes per second
against 502,000 for three. hashring-rs does not, because one gateway cannot
absorb the traffic of six nodes, and its worker budget shrinks as the node
count grows.

### Where the time goes

Server CPU per operation, sampled from the processes of each system during
the measured phases. The gateway and node columns are the sums over the
gateway processes and over the data nodes, so the client is not counted on
either side:

| Case | Workload | Redis | hashring-rs total | of which the gateway | of which the nodes |
| --- | --- | ---: | ---: | ---: | ---: |
| `3m` | write | 3.8 µs | 71.7 µs | 29.0 µs | 42.7 µs |
| `3m` | read | 3.8 µs | 68.1 µs | 32.5 µs | 35.6 µs |
| `3m1r` | write | 4.9 µs | 93.1 µs | 34.2 µs | 58.9 µs |
| `3m-gw3` | read | 3.7 µs | 82.0 µs | 49.2 µs | 32.8 µs |
| `6m` | read | 5.5 µs | 67.1 µs | 27.4 µs | 40.3 µs |

One operation costs the hashring-rs side about 19 times the CPU of the same
operation on the Redis side, and the client sees three to nine times the
latency.

The node's own work is a small part of that. The nodes report their handler
time with `HASHRING_PROFILE_RPC=1` while the gateway is driven
([`scripts/redis-cluster/gateway-profile.py`](../scripts/redis-cluster/gateway-profile.py)):

| Phase | Client-observed p50 | Node handler p50 | Node handler mean | Node handler p99 |
| --- | ---: | ---: | ---: | ---: |
| write | 0.543 ms | 12.3 µs | 68.9 µs | 558.3 µs |
| read | 0.479 ms | 1.5 µs | 1.8 µs | 6.2 µs |

This profile ran before the fixes noted above: its write phase covered about
49% of the key space, so about half of the reads below were misses.

For a read, 1.8 µs of a 479 µs command is the node's work; the rest is the
transport, the extra hop through the gateway, and the scheduling in between.
The write handler has a long tail, which is the handler waiting to be
scheduled again rather than working.

This is the same conclusion the project reached for its own client in
[grpc-io-bottleneck.md](optimizations/grpc-io-bottleneck.md): 90% or more of a
request's latency is outside the handler, and most of the CPU is in the gRPC
stack and its socket calls.

### The gateway is the limit, not the cluster

In the single-gateway cases the gateway process is the busiest one: in `3m`
it spends 3.1 of its 4 workers on writes, and in `6m` it spends 2.3 of its 3
workers. The nodes behind it have room to spare: in `3m` the three nodes
together use 4.6 of their 12 workers.

Three gateways instead of one, over the same three nodes, raise the read
throughput from 100,365 to 169,462 requests per second (+69%) and the write
throughput from 107,163 to 131,408 (+23%), with the client-observed p50
falling from 0.615 ms to 0.319 ms. The cluster behind the gateways does not
change; the proxy work is spread over three processes.

### The native client path

For reference, the same cluster driven by its own Rust client rather than by
the Redis gateway (`hashring-rs experiment --mode performance`, 3 nodes,
300,000 keys, 128-byte values, concurrency 64):

| Cluster | PUT | GET (with verification) |
| --- | ---: | ---: |
| 3 nodes, RF=1 | 86,858 ops/s | 101,982 ops/s |
| 3 nodes, RF=2 | 72,799 ops/s | 98,499 ops/s |

These come from a different client and a different workload, so they are a
reference and not a controlled comparison. They do show the size of the
effect: the native client path is in the same range as the gateway path, so a
client that skips the gateway does not close the gap to Redis. Both paths
share the gRPC hop to a node; this reference does not measure what the
gateway adds on top of it.

## What the numbers say

1. **The cluster is several times slower than a Redis Cluster.** Through the
   gateway the cluster reaches 80,000 to 170,000 operations per second,
   against 440,000 to 707,000 for a Redis Cluster of the same shape. The
   native client path is in the same range as the gateway (73,000 to 102,000
   operations per second in the reference run), so switching clients does not
   close the gap.
2. **The cost is transport, not the cluster.** The node spends 1.8 µs on a
   read and a median of 12.3 µs on a write, while the client waits half a
   millisecond. The ring, the storage and the follower copy are not what the
   time is spent on.
3. **The gateway is a single point of contention.** One gateway caps the
   cluster at about 100,000 operations per second, and more gateways lift that
   cap. The cluster behind it has more to give: in `3m` the three nodes
   together use 4.6 of their 12 workers.
4. **The default deduplication budget limits sustained writes.** A node keeps
   one receipt per write for 60 seconds, and 128 MiB fills after about ten
   seconds of sustained writes at these rates. A deployment that writes
   continuously needs a larger `--max-dedup-bytes` or a shorter receipt
   retention.
5. **Replication costs both systems.** RF=2 costs the hashring-rs side about
   20% of its write throughput and the Redis side about 9%, and the CPU per
   operation goes up by 30% on both sides.

## Limits

- One host, with the benchmark client on the same host as both systems. The
  client takes CPU that the servers do not get, and it is the same client for
  both sides, but the absolute numbers would be lower on a quieter machine.
- The Redis side gets one key space per master, each inside its own slots,
  because that is what a slot-routed client does. The hashring-rs side has
  one key space over the ring. The key count per owner is the same.
- The Redis side is measured with `redis-benchmark` talking to masters
  directly, which is what a cluster-aware client does after it learns the slot
  map. The hashring-rs side is measured with `redis-benchmark` talking to a
  gateway, because the gateway is what a Redis client has to talk to.
- The node budgets listed above were raised, so these numbers describe the
  data path and not the deduplication store.
- Three repetitions of 15 seconds per phase on a shared host. The spread
  between the three repetitions of one configuration was 0.7% to 7.5% of the
  median for the Redis side, except one mixed phase at 20.7%, and 1.3% to
  19.8% for the hashring-rs side.
- The measurements are of this build of this project on this host. The
  project's own numbers in
  [optimizations/](optimizations/) were taken on other machines and are not
  comparable with these.
- Valkey 9.1.2 is installed on the host as `redis-server`, and is a Redis
  fork with the same protocol and the same cluster commands. The measurements
  here used Redis 8.10.2 built from source.

## Reproducing

```sh
cargo build --release --bin hashring-rs
make -C /path/to/redis MALLOC=jemalloc

python3 scripts/redis-cluster/compare.py \
  --output results/redis-cluster \
  --cases 3m,3m1r,3m-gw3,6m \
  --reps 3 --seconds 15 \
  --redis-bin /path/to/redis/src
```

The harness writes `measurements.csv` with one row per phase, `summary.json`
with the medians, and `metadata.json` with the versions, the checkout's
revision, the digest of the binary that ran, and every resolved setting,
including what `hashring-cluster.sh` resolved for each case. The node-side profile comes from:

```sh
python3 scripts/redis-cluster/gateway-profile.py \
  --output results/node-profile --redis-bin /path/to/redis/src
```
