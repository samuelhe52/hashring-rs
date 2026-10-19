# Range digest in token order, hashed outside the state lock

A migration or replica seed ends with `finalize_range` comparing the source's
and the destination's digest of the range. Both sides used to collect the range
into a `HashMap`, sort it by key bytes and hash it. In a 1,000,000-key migration
that sort and copy were the two largest items under `source_range_digest` and
`destination_range_digest` (21.3% and 8.2% of the migration phase in
`results/flamegraph-correctness-20261015-005607`).

## The change

`range_digest` (`crates/hashring-node/src/migration.rs`) takes records in the
order the caller supplies, and that order is part of its contract: **token order,
ties broken by key bytes**. It is the order the source's token index already
yields (`TokenBucket::Many` is a `BTreeSet`), so the source sorts nothing, and the
destination sorts `u64` tokens with `sort_into_token_order`, which compares key
bytes only when two tokens are equal. The digest bytes changed, so the domain
separator moved from `v2` to `v3`.

Two corrections came out of review:

- **The tie-break is required.** The first version sorted the destination by
  token alone, so keys whose tokens collide came out in `HashMap` order and a
  valid range failed verification with `data_loss`.
  `range_digest_agrees_when_keys_share_a_token`
  (`crates/hashring-node/src/tests/migration.rs`) migrates constructed XXH3
  collisions and failed 3 of 3 runs without it.
- **Hash after releasing the lock.** The first version hashed while holding the
  `NodeState` read lock. Every `put` takes that lock for writing, so writes to
  every other range on the node waited for a pass over every value byte of the
  migrating range. Both handlers now copy cheap handles under the lock (the
  source's `Arc` keys and records; the destination's records and the seed) and
  compute tokens, sort and hash after `drop(state)`.

## Evidence

Apple M1 Pro, 8 logical CPUs, release build. 50,000 records in the migrating
range, 20 digests back to back while a writer puts once per millisecond to keys
outside the range (`crates/hashring-node/src/tests/digest_stall_bench.rs`).
*Stall* is the mean of the 20 slowest puts, one per digest. Medians of three
interleaved runs per tree: `b8deaa1` before the change, `a915f5b` hashing under
the lock, and the current code.

| value | side | before: stall | hashed under lock: stall | now: stall | writer puts (before / under lock / now) |
| ---: | --- | ---: | ---: | ---: | --- |
| 128 B | source | 14.65 ms | 24.10 ms | **7.75 ms** | 242 / 43 / 228 |
| 128 B | destination | 1.91 ms | 20.13 ms | 2.05 ms | 237 / 43 / 248 |
| 1 KiB | source | 15.67 ms | 85.82 ms | **8.65 ms** | 814 / 43 / 776 |
| 1 KiB | destination | 3.02 ms | 85.11 ms | 2.71 ms | 809 / 43 / 817 |
| 4 KiB | source | 16.28 ms | 287.11 ms | **9.03 ms** | 2589 / 43 / 2601 |
| 4 KiB | destination | 3.98 ms | 284.06 ms | 3.69 ms | 2583 / 43 / 2588 |

With no digest running, the same writer's stall is 0.09-0.42 ms. Hashing under the
lock made the stall equal to the whole digest and grow with the value size; the
writer completed one put per digest. Now the source stalls writers about half as
long as before the change, because it copies `Arc` handles where the old code
copied key bytes into a map, and the destination is back to the old clone.

The copy costs CPU the lock-holding version did not pay. `digest_bench.rs`, 20,000
records, total digest time in one binary:

| | before | hashed under lock | now |
| --- | ---: | ---: | ---: |
| source | 8.3 ms | 2.12x faster | 5.4 ms, **1.53x** faster |
| destination | 5.2 ms | 1.35x faster | 5.0 ms, **1.04x** faster |

So the source keeps most of the win, and the destination keeps almost none: its
saving was the map clone, and taking the records out from under the lock needs a
copy again. Making `DestinationMigration::records` share keys and values by `Arc`
would make that copy cheap; it is not done here.

## Reproduce

```sh
cargo test --release -p hashring-node range_digest_writer_stall -- --ignored --nocapture
cargo test --release -p hashring-node range_digest_pays -- --ignored --nocapture
```

`digest_stall_bench.rs` uses only the RPC surface and test helpers, so it can be
copied into an older tree (and named in `tests/mod.rs`) to repeat the comparison.

## See also

- [range-token-index.md](range-token-index.md) -- the token index the source reads
  the range from.
- [replica-repair-convergence-latency.md](replica-repair-convergence-latency.md) --
  the other half of the same PR.
