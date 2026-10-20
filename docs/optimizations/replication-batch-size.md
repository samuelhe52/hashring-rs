# Replication batch size

[batched-replication-rpc.md](batched-replication-rpc.md) put a cap of 64
entries on one `ReplicateMutations` RPC, and
[replication-batch-linger.md](replication-batch-linger.md) made a stream wait
2 ms for more entries before it sends. The cap was a constant, so the only way
to change it was to change the code.

This note makes the cap a setting and measures what it is worth. The question
is not whether a bigger batch is cheaper per entry, which it is, but whether
that shows up as throughput.

**Result: the batch size is not worth changing on this host.** 64 entries and
256 entries cost the same CPU per write, and 16 entries costs slightly more.
The one clear effect is the linger: `linger=0` costs the data nodes 22-30 µs of
CPU per write, 20% of the cost of the write, and buys nothing, because this
host has the cores to spend.

## Change

`NodeSettings::replication_batch_max_entries` says how many mutations one
replication RPC may carry. The default is 64, the value that was a constant
before.

- `--replication-batch-max-entries` on the node (YAML
  `replication_batch_max_entries`), and the same setting in an experiment
  configuration.
- A node rejects a setting above 4096 (`MAX_REPLICATION_BATCH_MAX_ENTRIES`).
  A follower applies a batch under one lock, so a huge batch holds that lock
  for a long time.
- The limit stops the collection: the entries that do not fit stay in the
  stream's queue, in order, and the next batch picks them up. Only the byte
  budget (`REPLICATION_BATCH_MAX_BYTES`) keeps its carry-over entry, as
  before.
- The linger is unchanged (`--replication-batch-linger-ms`, default 2 ms) and
  still ends the wait early for a write that waits for that follower's ACK.

## Measurement

24 CPU Linux host (Arch), Valkey 9.1.2 as the control, `scripts/redis-cluster/`
harness. Case `3m1r`: 3 data nodes, RF=2, 1 gateway, 300,000 keys, 128-byte
values, 64 client connections over 2 benchmark processes per endpoint. Each
phase runs 15 s, 3 repetitions.

The four variants run **inside** every repetition, and the Redis control is
measured right next to each of them, so a load that arrives while the run is
going hits every variant. The number to compare is the ratio of the hashring
throughput to the Redis control of the same repetition.

Raw data:
[ab-batch-20261020.tsv](data/replication-batch-size/ab-batch-20261020.tsv).

| workload | variant | hashring rps | Redis rps | ratio | p50 | CPU per request | gateway | node |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write | 64 / 2 ms | 72,855 | 398,134 | 0.1888 | 0.783 ms | 110.9 µs | 40.8 µs | 70.1 µs |
| | 256 / 2 ms | 79,339 | 377,020 | **0.2009** | 0.719 ms | 104.0 µs | 38.0 µs | 65.9 µs |
| | 16 / 2 ms | 77,362 | 389,518 | 0.1975 | 0.759 ms | 114.4 µs | 40.7 µs | 73.7 µs |
| | 64 / 0 ms | 70,552 | 348,909 | 0.1880 | 0.823 ms | 137.2 µs | 42.8 µs | 94.4 µs |
| read | 64 / 2 ms | 113,472 | 436,014 | **0.2602** | 0.511 ms | 69.3 µs | 30.8 µs | 38.5 µs |
| | 256 / 2 ms | 99,850 | 451,529 | 0.2211 | 0.599 ms | 68.2 µs | 31.9 µs | 36.3 µs |
| | 16 / 2 ms | 103,420 | 440,102 | 0.2347 | 0.527 ms | 66.9 µs | 30.3 µs | 36.6 µs |
| | 64 / 0 ms | 113,680 | 446,400 | 0.2547 | 0.511 ms | 63.8 µs | 28.5 µs | 35.4 µs |
| mixed | 64 / 2 ms | 89,926 | 375,362 | 0.2356 | 0.671 ms | 102.5 µs | 38.4 µs | 64.1 µs |
| | 256 / 2 ms | 86,707 | 375,739 | 0.2304 | 0.695 ms | 91.7 µs | 34.9 µs | 56.8 µs |
| | 16 / 2 ms | 88,831 | 370,000 | 0.2512 | 0.623 ms | 91.7 µs | 33.5 µs | 58.2 µs |
| | 64 / 0 ms | 83,745 | 380,015 | 0.2220 | 0.735 ms | 115.8 µs | 36.5 µs | 79.3 µs |

Medians of three repetitions. The per-repetition differences against the
`64 / 2 ms` variant, in the same repetition:

| workload | metric | 256 / 2 ms | 16 / 2 ms | 64 / 0 ms |
| --- | --- | --- | --- | --- |
| write | ratio | +0.056, -0.004, +0.010 | +0.023, -0.007, +0.010 | +0.023, -0.019, +0.026 |
| | node CPU | -12.5, +3.6, -4.2 µs | +0.5, +7.2, +2.5 µs | **+21.6, +29.5, +22.1 µs** |
| read | ratio | **-0.024, -0.049, -0.037** | **-0.006, -0.041, -0.017** | +0.014, -0.051, +0.016 |
| mixed | node CPU | -24.3, -1.6, +2.9 µs | -23.2, +2.0, +2.5 µs | +0.8, +22.0, +22.3 µs |

- **The linger is what costs.** With `linger=0` the data nodes spend 22-30 µs
  more CPU per write, 3 of 3 repetitions, which is 20% of the cost of a
  write, and the mixed workload costs 22-24 µs more per request. The
  throughput does not improve: the write ratio is a wash, and the read ratio
  is a wash.
- **A bigger batch does not cost less, in the end.** 256 entries and 64
  entries cost the same CPU per write. The median difference is -6.8%, but
  that comes from the first repetition of the `64 / 2 ms` variant, which was
  slow for both systems (its Redis control read 332,214 requests per second
  against 457,073 and 436,014 in the other two). In the two warm repetitions
  the difference is -0.9%, a wash.
- **A smaller batch costs more.** 16 entries costs the data nodes 2.5 µs more
  per write than 64 (2 of 3 repetitions), and never less.
- **The read workload is the one that resolves, and it goes against 256.** Its
  ratio is 14% below the `64 / 2 ms` variant in all three repetitions, and
  6% below for 16 entries, in all three. Its p50 is 17% higher for 256.

## Why the CPU saving does not become throughput

The write path of the hashring side is not CPU-bound on this host. During the
write phase the whole hashring side uses about 8 of the 24 cores, and with
`linger=0` it uses about 9.7. The extra 20% of CPU per write that `linger=0`
costs is therefore free: the cores are there.

That is the difference from
[replication-batch-linger.md](replication-batch-linger.md), which measured the
same lever on an 8 CPU host with 6 nodes at RF=3 and found 0 to 2 ms worth
+30-35% PUT/s. There the host was the limit, so every RPC the linger removed
was throughput. Here it is not, so the same lever moves the CPU and not the
throughput.

The batch size follows the same rule: fewer, larger replication RPCs are
cheaper per entry, and on a host with cores to spare that is worth nothing.
The lever matters when the nodes are CPU-bound, which is the case the earlier
note measured.

## The read result is not settled

The read phase follows the write phase, and the write phase leaves the
replication behind: the write path is acknowledged by the owner alone
(`OwnerOnly`), so the copies to the follower are still in flight when the
read phase starts. The read phase therefore measures a cluster that is still
catching up, not a cluster at rest, and the phase's length is set by a pilot
that measures the same state.

That is enough to make the read numbers harder to read, and it is the reason
this note does not claim an explanation for the 14% that 256 entries is below
64 entries there. What would settle it:

- The harness's `--settle-seconds` waits between the write phase and the read
  phase, so the read phase measures a cluster whose replication has caught up.
  A 3 second wait was enough to leave no replication RPC in flight at all in a
  trial run.
- The harness's `--profile-rpc` counts the requests and the replication
  batches of every process per phase, which says how many replication RPCs
  ran during the read phase and how many entries each batch carried.

## Tradeoffs and when to change the setting

- **Keep 64 entries and the 2 ms linger.** The measurement does not support
  any other value: 256 buys nothing and reads worse, 16 costs more, and
  `linger=0` costs 20% of the CPU per write.
- **A larger batch trades a follower's lock time for fewer RPCs.** A follower
  applies a batch under one lock, so a 256 entry batch holds that lock 4x
  longer than a 64 entry batch. On a host where the nodes are CPU-bound this
  is the trade that pays; here it does not.
- **A node with a slow follower** should not raise the batch size: the batch
  is what the follower holds its lock for, and a failed batch is retried as a
  whole.

## Verification

- Unit test `a_batch_stops_at_the_configured_number_of_entries`
  (`crates/hashring-node/src/tests/replication.rs`): the batch stops at the
  configured number of entries and the remaining entries stay queued in
  order.
- `cargo test --workspace --lib` passes, and the two cluster suites
  (`cargo test --test process_cluster --test redis_gateway -- --test-threads=1`)
  pass with the default batch size.

## Reproducing

```sh
cargo build --release
python3 scripts/redis-cluster/compare.py \
  --output results/ab-batch-20261020 \
  --cases 3m1r \
  --variants "b64l2:1:64:2,b256l2:1:256:2,b16l2:1:16:2,b64l0:1:64:0" \
  --reps 3 --seconds 15
python3 scripts/redis-cluster/paired-summary.py results/ab-batch-20261020
```

The control is the system's Valkey 9.1.2 (`/usr/bin`), whose throughput on
this host is within a few percent of the Redis 8.10.2 build that
[redis-cluster-comparison.md](../redis-cluster-comparison.md) used, so the
absolute numbers here are not comparable with that report's table. The ratio
against the control is what the comparison is based on.
