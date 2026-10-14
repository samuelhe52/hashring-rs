# Range migration: a token index instead of four full key scans

A data node stores its records in `NodeState.records`, a `HashMap<Vec<u8>, Record>`
keyed by the raw key bytes. Nothing in that map records the key's position in
the ring, so answering "which of my keys fall in this token range?" means
hashing every key with `TopologySnapshot::key_token` and testing each one. Four
migration RPCs do exactly that, and three of them do it while holding a lock.

This is the dominant cost of a topology change. In the capture below it is 59%
of every cycle the run spends, and it is why concurrent writes collapse during
scale-out.

## The four scans

| Handler | Source | Lock | Work over all keys |
| --- | --- | --- | --- |
| `prepare_source_range` | `rpc.rs` | **write** | clones every key and every dedup `(id, key)` pair, clones the topology snapshot; then outside the lock hashes every key to filter and sort it, and repeats that for every dedup key |
| `source_range_digest` | `rpc.rs` | read | hashes every key, clones every record that matches |
| `commit_destination_range` | `rpc.rs` | **write** | `records.retain(\|key, _\| !range.contains(topology.key_token(key)))`, hashing every key under the write lock |
| `cleanup_source_range` | `rpc.rs` | **write** | clones the topology, then hashes every key and calls `replica_node_ids_for_token` on each before removing the matches |

A range is one vnode slot wide, so a range query should touch a few dozen keys.
Instead it touches every key on the node, and the two write-lock scans hold the
lock for the duration.

`TopologySnapshot::derived_ranges` produces **one range per distinct vnode
token**, which is `members * virtual_nodes`: 1,024 ranges for the 8-node,
128-vnode topology in the capture below. Every range whose owner or follower set
changes when a node joins or leaves goes through all four handlers.

## Evidence

Captured with `scripts/flamegraph.sh correctness` on a 24-core AMD Ryzen 9
7845HX, rustc 1.93.0, frame-pointer build (`target/fp`), `--call-graph fp`.
Config: correctness mode, 8 nodes, 100,000 keys, 128-byte values, RF=3,
`FirstSuccessor`, concurrency 64, seed 114514. 78.74 s, 15,512 samples, 0 lost,
66.8% of folded weight carrying a project frame. Raw artifacts are under
`results/flamegraph-correctness-20261014-111934/` (ignored by Git).

| Phase | Wall | Cycles | Share of run | Cores busy |
| --- | ---: | ---: | ---: | ---: |
| start, full RF, initial put, get verify | 13.9 s | 99 G | 14.7% | 1.5 |
| scale-out transition to full RF | 31.2 s | 294 G | 43.8% | 2.0 |
| scale-in transition to full RF | 33.0 s | 270 G | 40.1% | 1.7 |

Cycles attributed by the innermost project frame, whole run:

| | Share |
| --- | ---: |
| the four range migration handlers | 59.1% |
| no project frame (kernel, libc, tokio, gRPC, allocator) | 25.8% |
| other project code (write path, replication, coordinator) | 12.8% |
| `TopologySnapshot` clone/drop outside migration | 2.3% |

`key_token` is 12.6% of the run on its own. Its callers, inside the scale-out
window, are `commit_destination_range` 6.0%, `source_range_digest` 4.5%,
`cleanup_source_range` 3.3%, `prepare_source_range` 1.5%. `prepare_source_range`
additionally spends 45.1% of that window moving `Vec<u8>` keys around: cloning
them under the write lock, then filtering and sorting them outside it.

The cost is visible from the outside, not just in the profile:

| | Operations | Seconds | Rate |
| --- | ---: | ---: | ---: |
| scale-out with concurrent mutations | 9,095 | 30.457 | **298 ops/s** |
| scale-in with concurrent mutations | 9,095 | 0.563 | **16,153 ops/s** |

The same 9,095 mutations are 54x slower while a scan holds the state write lock.
Scale-in avoids it because that transition took the `direct_merge` path (the run
recorded `direct_merge_cutover_observed`), which does not prepare ranges one by
one.

The scale-out window is also 30.46 s against a 30 s `operation_timeout_ms`.
[dedup-receipt-store-memory.md](dedup-receipt-store-memory.md) records a
`FirstSuccessor` correctness run failing in exactly this phase with
`KnownNotApplied` after exhausting the logical deadline. This run passed, but
only by 0.46 s, so the timeout is not a safety margin here.

## The change

Store the token where the key already is, and keep a token-ordered index beside
each map:

- `records` becomes `HashMap<Arc<[u8]>, Record>`. `Arc<[u8]>` borrows as `[u8]`,
  so `records.get(&request.key)` and friends keep working unchanged, and cloning
  a key is a refcount bump instead of a `Vec` allocation.
- `records_by_token: BTreeMap<u64, Vec<Arc<[u8]>>>` shares those same `Arc`s. A
  `Vec` rather than a one-per-token set because two distinct keys colliding on a
  64-bit xxh3 token is possible in principle and must not lose a key.
- `dedup_by_token: BTreeMap<u64, Vec<Arc<str>>>` holds mutation IDs, which are
  already `Arc<str>` shared with the dedup map, so this index costs no extra key
  allocation either.

Range extraction becomes a `BTreeMap::range` bounded lookup:
`O(log n + k)` for the `k` keys actually in the range, instead of `O(n)` hashes
and clones. `commit_destination_range`'s `retain` and `cleanup_source_range`'s
scan become removals of the `k` keys the index returns. All four handlers stop
touching keys outside their range, and the two that hold the write lock now hold
it for `k` operations instead of `n`.

**The load-bearing invariant: a key's token never changes.** `key_token` is
`xxh3_64_with_seed(key, hash_seed)` and `hash_seed` is a field of the snapshot,
so it would be invalidated by any topology change that reseeded the ring — and
the index would then answer range queries with stale positions.
`crates/hashring-core/src/migration.rs:138` builds every target topology with
`committed.hash_seed`, so a seed change cannot be expressed through a topology
change. Members, virtual nodes and epoch change freely and do not affect a key's
token. `TopologySnapshot::key_token` is not public state a caller can vary
independently of the snapshot it came from, so the index cannot observe a
different seed than the snapshot it was built with.

That invariant is what makes an incrementally maintained index safe here rather
than a correctness hazard, and it is asserted directly in the code and in a
test, so a future seed change fails loudly instead of silently corrupting range
queries.

Every mutation goes through `NodeState` helpers rather than touching `records`
or `dedup` directly, because the failure mode of this change is a map and an
index that disagree — a key invisible to migration, or a migrated key that
reappears. A debug-only consistency check walks both structures and compares
them, and the existing migration and replication tests exercise the same paths.

## Tradeoffs

- **The write path gets more expensive.** Every applied mutation now also
  inserts into a `BTreeMap`: a binary search plus a potential node split, on the
  hot path, under a lock that was already held. The put path already has the
  token in hand from the ownership check, so no extra hashing is needed, but the
  tree insert is new work. This is the cost of moving the scan off the write
  path and onto the migration path, and it has to be measured, not assumed.
- **Memory grows.** Roughly one `BTreeMap` entry per record and per live
  deduplication receipt: tens of bytes per entry on top of the shared `Arc`
  payloads. On the 10-node, 1M-key performance profile that is a few hundred
  thousand entries per node. [dedup-receipt-store-memory.md](dedup-receipt-store-memory.md)
  already bounds the receipt side; this raises the per-node baseline and belongs
  in the same accounting.
- **Two structures to keep consistent.** Handled by funneling mutations through
  helpers and checking the invariant in tests, but it is a real cost and the
  reason the debug assertion exists.
- **`source_range_digest` loses nothing.** It still clones the records it
  digests, so its working set for the range is unchanged; only the discovery of
  which records to clone gets cheaper.

## Measurement

Pending. This section records the same correctness profile and the 10-node,
1M-key performance profile on the same host, before and after, with the sample
loss, node RSS and migration phase times that the change is supposed to move.

## Source

- [the four handlers](../../crates/hashring-node/src/rpc.rs)
- [record application](../../crates/hashring-node/src/migration.rs)
- [`derived_ranges` and `key_token`](../../crates/hashring-core/src/topology.rs)
- [the seed invariant](../../crates/hashring-core/src/migration.rs)

See also [frame-pointer-profiling.md](frame-pointer-profiling.md) for how these
stacks became readable at all, [write-path-saturation.md](write-path-saturation.md)
for the write path's own cost structure, and
[worker-pool-sizing.md](worker-pool-sizing.md) for why the driver and each node
share the host's CPUs, which is why these scans cannot be hidden by parallelism.
