# Range migration: a token index instead of four full key scans

Before this change a data node stored its records in `NodeState.records`, a
`HashMap<Vec<u8>, Record>` keyed by the raw key bytes. Nothing in that map
recorded the key's position in the ring, so answering "which of my keys fall in
this token range?" meant hashing every key with `TopologySnapshot::key_token`
and testing each one. Four migration RPCs did exactly that, and three of them
did it while holding a lock.

This was the dominant cost of a topology change. In the capture below it
accounts for 59% of the cycles the run spends, and it is why concurrent writes
collapse during scale-out.

## The four scans, before the change

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
`FirstSuccessor`, concurrency 64, seed 114514. 78.74 s, 15,512 samples,
672.6 G cycles, 0 lost samples, and 74.1% of cycles carrying a project frame.
Raw artifacts are under `results/flamegraph-correctness-20261014-111934/`
(ignored by Git).

| Phase | Wall | Cycles | Share of run | Gcycles/s |
| --- | ---: | ---: | ---: | ---: |
| start, full RF, initial put, get verify | 13.85 s | 97.7 G | 14.5% | 7.1 |
| scale-out transition to full RF | 31.25 s | 293.6 G | 43.7% | 9.4 |
| scale-in transition to full RF | 33.42 s | 278.1 G | 41.4% | 8.3 |

Even the 31-second scale-out averages 9.4 Gcycles/s, about two cores' worth of
work at this CPU's clock. The cost is serialized work under the state write
lock rather than parallelism, which is why wall time tracks cycles so closely
here.

Cycle shares over the whole run:

| | Share |
| --- | ---: |
| the four range migration handlers, any frame in the stack | 58.8% |
| no project frame at all (kernel, libc, tokio, gRPC, allocator) | 25.9% |
| project code outside the four handlers | 15.4% |
| — of which `TopologySnapshot` clone/drop | 1.0% |

`key_token` is 12.6% of the run on its own. Inside the scale-out window its
cycles land under `commit_destination_range` 6.5%, `source_range_digest` 4.7%,
`cleanup_source_range` 3.7% and `prepare_source_range` 1.5%.
`prepare_source_range` is the exception that proves the rule: 46% of that window
is in the handler's own body — four instruction offsets, the widest 16% —
against 12.9% in `key_token` and 2.5% cloning the topology snapshot. Its cost
is moving keys, not hashing them.

Phase windows come from `run/events.jsonl`; the cycle counts are
`perf script` on `run/perf.data` anchored at `experiment_started`, which puts
the last sample 0.02 s before `experiment_finished`. The two views of the same
capture agree exactly — the sample periods and the `folded.txt` weights both sum
to 672,558,597,308 cycles, and both put the four handlers at 58.8% — which is
what makes the folded medians in [Measurement](#measurement) usable without
re-running `perf script` ten times.

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
  so a lookup by bytes keeps working unchanged — the one read in the migration
  path is `records.get(key.as_ref())` — while a call site that holds a `Vec<u8>`
  now passes `key.as_slice()` instead of `&key`, because `Arc<[u8]>` borrows as
  `[u8]` and not as `Vec<u8>`. Cloning a key becomes a refcount bump instead of
  a `Vec` allocation, and the map, the index and the receipt share one
  allocation instead of two.
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
than a correctness hazard. Both places that swap a node's topology —
`install_topology` and `refresh_topology` — assert that the seed did not change,
so a future reseeding fails in debug builds instead of silently answering range
queries with stale positions, and `tests/index.rs` pins the behaviour across an
epoch bump.

Every mutation goes through `NodeState` helpers rather than touching `records`
or `dedup` directly, because the failure mode of this change is a map and an
index that disagree — a key invisible to migration, or a migrated key that
reappears. A debug-only consistency check walks both structures and compares
them; `tests/index.rs` checks that indexed range answers equal hashing every key
for wrapping, covering and empty ranges, that replacement, stale-version and
removal all leave the two structures equal, and that the answers survive an
epoch bump; and the existing migration and replication tests exercise the same
paths.

## Tradeoffs

- **The write path gets more expensive.** Every applied mutation now also
  inserts into a `BTreeMap`: a binary search plus a potential node split, on the
  hot path, under a lock that was already held. The put path already has the
  token in hand from the ownership check, so no extra hashing is needed, but the
  tree insert is new work. This is the cost of moving the scan off the write
  path and onto the migration path; the measurement below is where it was paid
  for or not.
- **Memory grows in the steady state.** Roughly one `BTreeMap` entry per record
  and per live deduplication receipt: tens of bytes per entry on top of the
  shared `Arc` payloads. On the 20-node, 1M-key performance profile that is a
  few hundred thousand entries per node, measured as +23% peak RSS there — while
  the migration peak it replaces falls 85% in the correctness run.
  [dedup-receipt-store-memory.md](dedup-receipt-store-memory.md)
  already bounds the receipt side; this raises the per-node baseline and belongs
  in the same accounting.
- **Two structures to keep consistent.** Handled by funneling mutations through
  helpers and checking the invariant in tests, but it is a real cost and the
  reason the debug assertion exists.
- **`source_range_digest` loses nothing.** It still clones the records it
  digests, so its working set for the range is unchanged; only the discovery of
  which records to clone gets cheaper.

## Measurement

Everything below is the same frame-pointer build and capture settings as the
Evidence section: `--call-graph fp`, `-F 99`, event `cpu/cycles/P`, and **0 lost
samples in every capture**. Two configurations ran back to back on this shared
desktop, whose load average was between 3.6 and 17.0 while they ran; the pairs
were interleaved (before, after, before, after) so neither side always had the
quiet machine, and the tables give the median with the observed range across
three correctness pairs and two performance pairs. The before binary is the
frame-pointer build of `deb5928`, the after one of `3091ddb`. Later commits add
a guard that de-indexes a replaced receipt id, the two debug-only seed
assertions and test scaffolding; that guard runs once per committed receipt and
is the only one of them that exists in a release build. Captures are in
`results/flamegraph-correctness-{before,after}{,-2,-3}` and
`results/flamegraph-performance-{before,after}{,-2}`, ignored by Git.

### Correctness: 8 nodes, 100,000 keys

| | before (range) | after (range) | after / before |
| --- | ---: | ---: | ---: |
| mutations during scale-out | 174 ops/s (166–203) | 1,694 ops/s (1,618–1,951) | **9.8x** |
| whole experiment | 88.5 s (84.4–88.7) | 31.7 s (26.7–31.7) | 0.36x |
| cycles in the four handlers | 60.7% (57.2–61.5) | 5.6% (5.3–5.7) | 0.09x |
| cycles in `key_token` | 13.5% (13.4–13.8) | 0.0% (0.0–0.1) | — |
| peak per-node RSS | 473 MiB (456–531) | 68.7 MiB (68.6–68.8) | 0.15x |

The three before runs reproduce the capture in the Evidence section: 57.2–61.5%
of cycles in the four handlers against its 58.8%.

Per handler, median of three runs:

| handler | before | after |
| --- | ---: | ---: |
| `prepare_source_range` | 42.3% | 0.27% |
| `commit_destination_range` | 8.1% | 2.4% |
| `source_range_digest` | 6.0% | 2.0% |
| `cleanup_source_range` | 4.4% | 0.9% |

`prepare_source_range` alone falls 157x: it used to clone every key and every
receipt under the write lock and then filter and sort them outside it, and it
now walks the range's own slice of the index. The two sides' observed ranges do
not overlap on the headline row, so the 9.8x on scale-out mutations is not a
load artefact.

Nothing else moved, which is what the write-path tradeoff needed to show.
`initial_put` was 40,335 → 39,593 ops/s (0.98x); the three read measurements
(`initial_get_verify`, `scale_out_verify`, `scale_in_verify`) all landed within
1.14x of their before values; and `initial_full_rf_seconds`, which is
replication rather than migration, was 10.30 s on both sides. Scale-in, which
takes the direct merge path and never scans either way, went 14,506 → 15,306
ops/s at the median, but that phase ranged from 1,535 to 16,322 ops/s across the
six runs, so it shows no consistent movement in either direction.

### Performance: 20 nodes, 1,000,000 keys

| | before (range) | after (range) | after / before |
| --- | ---: | ---: | ---: |
| initial put | 24,496 ops/s (24,142–24,850) | 29,023 ops/s (28,547–29,499) | 1.18x |
| get verify | 96,821 ops/s (89,255–104,387) | 96,066 ops/s (91,807–100,325) | 0.99x |
| time to full RF after the put | 61.93 s | 61.42 s | 0.99x |
| peak per-node RSS | 149.4 MiB (149.0–149.8) | 184.1 MiB (183.9–184.3) | **1.23x** |
| cycles in the four handlers | 0.1% | 0.0% | — |

This mode barely exercises range migration — one topology change after the load
is placed, so 0.1% of cycles went to the handlers on both sides — and its value
here is the put and RSS rows. The put path, which now pays a `BTreeMap` insert
per applied record and per receipt, did not regress: it measured 1.18x faster,
and that number may be real rather than noise, because the record key is now a
single `Arc<[u8]>` shared by the map, the index and the receipt where it used to
be a `Vec` clone plus a separate `Arc` copy. It was measured on the same noisy
desktop as everything else and is not isolated, so the claim to keep is the
weaker one: the index insert cost nothing measurable.

Memory moves in both directions, which is the honest shape of this change. The
migration peak falls 7x because the per-range key and receipt clones are gone:
sampling inside the scale-out window, peak node RSS was 456, 473 and 463 MiB
across the three before runs against 68.7, 68.8 and 68.6 MiB across the three
after runs, and the largest sample anywhere in the before runs, 531 MiB, came
after the migration had already finished. The steady state rises: at 1M keys
and RF=3 each node holds about 150k record replicas plus roughly 180k live
receipts, so about 330k indexed entries, and peak RSS rose 35 MiB for them —
about 110 bytes an entry, which is what a `BTreeMap` entry and its share of a
node cost. The `Vec` → `Arc` key change pushes the other way, one shared
allocation per key instead of two, so the index's own share of that 35 MiB is
below 110 bytes an entry. The correctness run pays the first effect and the
performance run pays the second.

### What is left

Cycles of the three after runs pooled together, innermost project frame: gRPC
dispatch 4.8%, replication stream delivery 2.6%, the per-range
`TopologySnapshot` clone 2.2%, the client's own `get` 1.9%, and two parts of the
migration path that still copy rather than look up — `source_range_digest`'s
records 1.4% and `commit_destination_range` 1.4%. Above project code, the
coordinator's `seed_replica_group` carries 11.9% and `execute_change` 5.5% of
weight at any depth, which is where the migration's coordination now shows up,
and `CoordinatorRepository::store_state` 1.2%.

The snapshot clone is the cheapest of these to remove and the natural
follow-up. `apply_migration_batch`, `apply_dedup_batch` and
`cleanup_source_range` each clone `state.topology` once to keep a snapshot
across the mutations they apply, and they only ever read it —
`topology.key_token` and `topology.epoch`. Holding it as an
`Arc<TopologySnapshot>` in `NodeState` would turn those three deep clones into
reference-count bumps.

### Reproducing both sides

Each side is the same script pointed at a different binary. Build the tree under
test with `RUSTFLAGS='-C force-frame-pointers=yes' cargo build --release
--target-dir target/fp`, then:

```bash
BINARY=$PWD/target/fp/release/hashring-rs BUILD=0 \
  CONFIG=experiments/experiment_cmach.yaml ./scripts/flamegraph.sh
```

The second table needs a copy of that config with `mode: performance`,
`nodes: 20` and `keys: 1000000`, passed the same way through `CONFIG`, plus the
`performance` argument. `BUILD=0` is load-bearing: `BUILD=auto` only builds when
the binary is missing, so a stale `target/fp` measures the wrong tree without
saying so.

## Source

- [the index, its helpers and the consistency check](../../crates/hashring-node/src/lib.rs)
- [the four handlers](../../crates/hashring-node/src/rpc.rs)
- [record application](../../crates/hashring-node/src/migration.rs)
- [index consistency tests](../../crates/hashring-node/src/tests/index.rs)
- [`derived_ranges` and `key_token`](../../crates/hashring-core/src/topology.rs)
- [the seed invariant](../../crates/hashring-core/src/migration.rs)

See also [frame-pointer-profiling.md](frame-pointer-profiling.md) for how these
stacks became readable at all, [write-path-saturation.md](write-path-saturation.md)
for the write path's own cost structure, and
[worker-pool-sizing.md](worker-pool-sizing.md) for why the driver and each node
share the host's CPUs, which is why these scans cannot be hidden by parallelism.
