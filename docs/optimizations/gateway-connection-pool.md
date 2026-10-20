# Gateway connection pool

[grpc-io-bottleneck.md](grpc-io-bottleneck.md) found that 12-19% of the
client's on-CPU time lands in h2's per-connection `Streams` mutex: every
in-flight request to a node shares one HTTP/2 connection and one stream table.
It tried a temporary 4-connections-per-node client pool, watched the mutex
contention fall from 12-19% to 5%, and kept neither the pool nor the result,
because throughput did not move.

This note puts the same idea in the Redis gateway as a setting and measures it
properly: a pool of 1 against a pool of 4, interleaved with a Redis control in
the same repetition, so the comparison survives the host's background load.

**Result: the pool buys nothing here and costs a little.** Throughput is
unchanged on writes and reads and lower on the mixed workload in all three
repetitions, while the gateway's CPU per request and the p50 latency go up.

> **These numbers are being replaced.** They were measured with the harness
> before the fixes of
> [9e65434](https://github.com/samuelhe52/hashring-rs/commit/9e65434c8d44cf6b1503105e4787a5a9352226ed)
> (the values were 8 bytes instead of 128, the two systems had a different
> number of client connections, and the mixed phase's rate was not the
> requests over the phase's wall-clock time). The run below is a first look
> at the question; the numbers in this file come from a rerun with the fixed
> harness.

## Change

`ClientConfig::connection_pool_size` says how many HTTP/2 connections the
client opens to each data node. The default is 1, which is the behaviour
before this change.

- A tonic `Channel` is a single HTTP/2 connection: `Endpoint::connect()`
  performs one `handshake` and keeps that connection. A pool is therefore a
  vector of channels, one per connection.
- `ClientInner::channels` maps an endpoint to a `ChannelPool`. The pool hands
  its connections out round-robin through one `AtomicUsize`, so a pool of one
  always returns the same connection and the behaviour is unchanged.
- The gateway's `--client-connection-pool` (YAML `client_connection_pool`) sets
  the size for the gateway's client.

The failure semantics keep the old behaviour for the request that needs the
connection and stay forgiving for the rest:

- The first connection to a node must succeed. If it does not, the request
  fails exactly as it did before a pool existed.
- The connections after the first are best effort. A connection that fails to
  open, or a deadline that runs out while the pool is growing, leaves the pool
  smaller and logs a warning. A node that answers on one connection is usable,
  and a request that has a working connection should not fail because a
  second one did not open.

## Measurement

24 CPU Linux host (Arch), Valkey 9.1.2 as the control, `scripts/redis-cluster/`
harness. Case `3m`: 3 data nodes, RF=1, 1 gateway, 300,000 keys, 128-byte
values, 64 client connections over 2 benchmark processes per endpoint. Each
phase runs 15 s, 3 repetitions.

The two variants run **inside** every repetition, and the Redis control is
measured right next to each of them, so a load that arrives while the run is
going hits both variants. The number to compare is the ratio of the hashring
throughput to the Redis control of the same repetition: the host's load moves
both systems, and the ratio moves with them. The CPU per request is split
into the gateway and the data nodes.

Raw data:
[ab-pool-20261020.tsv](data/gateway-connection-pool/ab-pool-20261020.tsv).

| workload | pool | hashring rps | Redis rps | ratio | p50 | CPU per request | gateway | node |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write | 1 | 92,956 | 447,886 | 0.2145 | 0.615 ms | 80.0 µs | 32.5 µs | 47.5 µs |
| | 4 | 84,891 | 415,700 | 0.2045 | 0.687 ms | 81.5 µs | 34.9 µs | 46.6 µs |
| read | 1 | 103,259 | 458,807 | 0.2251 | 0.575 ms | 65.8 µs | 30.0 µs | 35.8 µs |
| | 4 | 99,862 | 459,075 | 0.2120 | 0.599 ms | 65.5 µs | 30.6 µs | 34.9 µs |
| mixed | 1 | 96,954 | 392,861 | 0.2581 | 0.607 ms | 76.5 µs | 31.8 µs | 44.7 µs |
| | 4 | 89,360 | 413,769 | 0.2261 | 0.679 ms | 78.9 µs | 34.3 µs | 44.6 µs |

Medians of three repetitions. The per-repetition differences (pool 4 minus
pool 1, in the same repetition) are:

| workload | ratio, per repetition | direction | gateway CPU per request | p50 |
| --- | --- | --- | --- | --- |
| write | +0.021, -0.029, -0.001 | 1 of 3 | -2.0, +5.7, +3.5 µs | -0.04, +0.11, +0.08 ms |
| read | -0.016, -0.043, +0.045 | 1 of 3 | +0.9, +6.0, -5.0 µs | +0.02, +0.13, -0.09 ms |
| mixed | -0.042, -0.005, -0.027 | **0 of 3** | **+3.4, +1.8, +2.1 µs** | **+0.07, +0.02, +0.06 ms** |

- **No gain in any workload.** The write ratio moves by -0.5% and the read
  ratio by -7.2%, both inside the noise of three repetitions. The mixed
  workload is the one that resolves: the pool is lower in all three
  repetitions, by 10.4% in the median.
- **The gateway pays for the pool.** Its CPU per request is 3-11% higher on
  the write and mixed workloads, and the p50 latency is 6-13% higher. The
  data nodes' CPU per request is unchanged (within 2%), so the pool does not
  make the nodes cheaper either.
- **The contention it removes was not the wall.** The pool exists to spread
  in-flight requests over more than one h2 stream table. It does that, and
  the gateway's own CPU per request still goes up.

## Why the pool does not help here

The gateway answers **one command at a time per client connection**
(`crates/hashring-redis/src/server.rs:165` awaits `dispatcher.execute` inside
the frame loop of one connection). The number of requests in flight is
therefore the number of client connections, not the number of cores: 64 in
this benchmark, spread over 3 nodes, so about 21 concurrent requests per
node.

At that concurrency the single connection to a node is not what limits the
throughput. The closed loop is `throughput = in-flight / latency`, and the
latency of a request is set by the node's service time: the p50 is 0.6 ms
against 0.13 ms for Redis, and the gateway and the nodes together spend
about 80 µs of CPU per request. Splitting 21 concurrent requests over 4
connections removes a mutex that was never the wall, and it adds:

- four h2 connection states per node on the client instead of one, each with
  its own HPACK tables, flow control windows and buffers, and
- one shared counter that every request to that node touches.

The node side does gain a little parallelism (4 h2 streams on 4 workers
instead of 1 stream on 1 worker), which is why its CPU per request fell
slightly, but the gateway pays more than the node saves.

## Tradeoffs and when to enable it

- **Leave it at 1** for a gateway that serves about as many client
  connections as this one. It is the default, and it is what the rest of
  these notes were measured with.
- The setting is worth trying when a deployment puts **many more concurrent
  requests on one node** than a client connection count implies, for example a
  gateway with thousands of client connections, several gateways in front of
  one node, or a client that pipelines more than one command per connection.
  This measurement does not cover those cases, so it cannot say what the pool
  is worth there.
- A larger pool costs memory: one h2 connection state per node per gateway,
  and the connections are opened when the node is first used.

## Verification

- Unit tests `a_pool_hands_out_its_connections_in_turn`,
  `a_pool_of_one_always_uses_the_same_connection` and
  `a_pool_size_below_one_is_treated_as_one`
  (`crates/hashring-client/src/lib.rs`), plus
  `the_connection_pool_size_comes_from_the_flag_or_the_file`
  (`src/config/gateway.rs`).
- `cargo test --workspace --lib` passes, and the two cluster suites
  (`cargo test --test process_cluster --test redis_gateway -- --test-threads=1`)
  pass with the default pool size.

## Reproducing

```sh
cargo build --release
python3 scripts/redis-cluster/compare.py \
  --output results/ab-pool-20261020 \
  --cases 3m \
  --variants "pool1:1:64:2,pool4:4:64:2" \
  --reps 3 --seconds 15
python3 scripts/redis-cluster/paired-summary.py results/ab-pool-20261020
```

The control is the system's Valkey 9.1.2 (`/usr/bin`), whose throughput on
this host is within a few percent of the Redis 8.10.2 build that
[redis-cluster-comparison.md](../redis-cluster-comparison.md) used, so the
absolute numbers here are not comparable with that report's table. The ratio
against the control is what the comparison is based on.
