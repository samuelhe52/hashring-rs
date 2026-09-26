# Deduplication receipt store memory layout and accounting

Each node retains deduplication receipts for 60 seconds so retries can reuse the assigned version without applying the mutation again, and migration can carry live receipts to a new owner. The owner and followers each keep their own receipts. The byte budget is admission accounting for these receipts, not a process-wide RSS limit.

## Changes

- A live receipt's mutation ID is now one `Arc<str>` allocation shared by the lookup map and expiration heap. Migration snapshot preparation also clones the shared ID handle instead of allocating another string for every captured receipt. The external mutation ID remains a string; callers do not need to use UUIDs.
- After the required followers have acknowledged a write and the owner lease is still valid, the owner drops that receipt's follower/sequence requirements and releases their budget charge. An incomplete or timed-out ACK wait keeps them so a retry can wait for the original ACKs. The fingerprint, key, version, delete flag, and expiration remain available for the full receipt retention period.
- Live receipts and staged migration receipts now have separate byte estimates. The former estimate included the sizes of `DeduplicationRecord` and `StagedDedup`, although neither object is stored in a live receipt. With that double charge removed, the default live receipt budget is 128 MiB again, down from 256 MiB. This accounting correction admits more receipts; by itself it does not reduce physical memory.

See [the live receipt and expiration logic](../../crates/hashring-node/src/dedup.rs), [the ACK completion path](../../crates/hashring-node/src/service.rs), and [the migration snapshot path](../../crates/hashring-node/src/rpc.rs).

On the measured 64-bit target, `String` occupies 24 bytes and `Arc<str>` 16 bytes; the expiration tuple shrinks from 40 to 32 bytes. For a 36-byte client UUID, sharing the ID saves a nominal 36 bytes per live receipt plus one allocation, before allocator rounding and map capacity. This is a per-receipt layout saving, not a promise about whole-process RSS.

## Evidence

The node tests check that the lookup map and expiration heap share the same ID allocation, that successful ACKs release their metadata while preserving the receipt, and that a timed-out ACK leaves its requirements available for a later retry. The existing migration tests exercise snapshot transfer and expiry with the new ID handle.

Two local release runs used the 10-node, RF=3, 1M-key performance profile with owner-only ACKs. The old binary was rebuilt from an isolated archive of `e809ce4`; its source-tree hash matches the earlier `e809ce4` build. Each run sampled child-process RSS with `ps` every 0.5 seconds. The full local artifacts are under `results/memory-baseline-isolated-e809ce4/` and `results/memory-optimized-128m/` (ignored by Git).

| Build | Initial PUT time | Highest receipt-budget charge | Receipt cap | Highest sampled node RSS | Receipt rejections |
| --- | ---: | ---: | ---: | ---: | ---: |
| `e809ce4` | 74.4 s | 179.5 MiB | 256 MiB | 249.1 MiB | 0 |
| Optimized working tree | 87.7 s | 71.6 MiB | 128 MiB | 268.8 MiB | 0 |

All 1M writes and reads passed under the smaller receipt cap. The lower budget charge reflects both actual layout changes and corrected accounting; it must not be read as a 107.9 MiB RSS saving. These runs had different write rates and receipt lifetimes relative to sampling, and the optimized run's highest sampled process RSS was higher. They establish capacity for this workload, not a reduction in its whole-process RSS peak or an improvement in throughput. The owner-only profile does not exercise completed follower-ACK cleanup; the focused tests cover that path.

The default 20k-key correctness profile also passed on the optimized binary, including scale-out with concurrent writes/deletes, scale-in, and final key verification. Its local summary is `results/correctness-memory-optimized-128m/summary.json`. This checks the shared IDs through migration at that workload size; it does not establish a 1M-key migration memory bound.

### FirstSuccessor comparison

Three isolated release builds ran the same 10-node, RF=3, 20k-key performance profile with FirstSuccessor ACKs, 128-byte values, concurrency 64, and a 30-second logical operation deadline. `current` contains both changes. `keep_acks` removes only the successful-ACK cleanup call. `string_ids` keeps ACK cleanup but restores separate `String` allocations for the lookup map and expiration heap. Each build used its own Cargo target directory. The local artifacts are under `results/firstsucc-ab-{current,keep_acks,string_ids}-20000/`.

| Build | Result | Initial PUT time | Cluster receipt charge after PUT | Highest sampled node RSS |
| --- | --- | ---: | ---: | ---: |
| Shared IDs, ACK cleanup | Pass | 48.25 s | 20,286,303 bytes | 31.8 MiB |
| Shared IDs, retain ACKs | Pass | 48.48 s | 23,288,506 bytes | 31.7 MiB |
| Separate IDs, ACK cleanup | Pass | 49.21 s | 24,366,303 bytes | 35.0 MiB |

All reads verified; there were no receipt rejections or replication RPC retries. Dropping completed ACK requirements reduced the *admission charge* by 3,002,203 bytes across the ten nodes at this snapshot. It did not reduce the highest sampled process RSS in this small run; freed allocations can remain in the allocator, and the maximum came from different nodes and times. The separate-ID run was 2% slower than the shared-ID run, but one run per variant under a coordinator-limited policy cannot establish a throughput difference. Every FirstSuccessor write requests replica status from the coordinator, which dominated this profile.

To isolate physical ID-layout cost from that write-rate limit, a release-mode harness held one million receipts in a lookup map and expiration heap with the same `DedupEntry` field layout, 36-byte IDs, and no replication or coordinator. Two alternating runs per layout measured RSS after all inserts: shared `Arc<str>` IDs used 472,800 and 472,768 KiB; separate `String` IDs used 544,096 and 544,112 KiB. That is about 69.6 MiB less RSS for the shared layout at this fixed population. Warm insertion times were 0.458 and 0.457 seconds respectively; the first cold shared-ID run took 1.001 seconds. This harness is a layout measurement, not an end-to-end throughput claim. Its source is [receipt-id-layout.rs](receipt-id-layout.rs); compile it with `rustc -O docs/optimizations/receipt-id-layout.rs -o /tmp/receipt-id-layout`. The raw result is `/tmp/hashring-memory-ab/id-layout-results.json`.

An attempted 1M-key FirstSuccessor profile was deliberately interrupted during initial PUT after about 23 minutes. A direct key probe found key 250,000 but not key 500,000, so it could not provide a completed 1M result. Its sampled RSS and event log remain under `results/firstsucc-ab-current-1m/`. This profile is limited by per-write coordinator readiness checks and does not create a large enough live receipt population quickly for an efficient layout comparison.

The 20k-key FirstSuccessor *correctness* run at the standard 30-second logical deadline failed during scale-out: one write exhausted its deadline with `KnownNotApplied` after the topology reached `Published`. The same failure occurred in the `keep_acks` build and the isolated pre-optimization `e809ce4` build, so it is not introduced by either ACK cleanup or shared IDs. A live replica-status sample during the transition showed `activation_pending: true` and `write_block_reason: "topology activation pending"`. With a 120-second logical operation deadline, the current build completed scale-out, scale-in, full-RF convergence, and final 20k-key verification. Scale-out with concurrent mutations took 72.6 seconds; the full run took 238.4 seconds. These results identify a pre-existing policy availability and benchmark-deadline issue; increasing the benchmark deadline is not an optimization of that transition. The local artifacts are `results/firstsucc-correctness-{current,keep-acks,baseline-e809ce4}-20k/` and `results/firstsucc-correctness-current-20k-120s/`.

`cargo test --workspace --all-targets` passed, including 27 real-process cluster tests; strict workspace Clippy and formatting checks passed. The optimized run used an uncommitted working-tree build, so its manifest reports `source_reproducible: false`. The local artifacts and source diff must be kept together to reproduce these exact measurements.

## Tradeoffs and limits

`Arc<str>` adds atomic reference counting when ID handles are cloned, in exchange for fewer string allocations and smaller handles. Successful required-ACK writes take a brief state write lock when releasing ACK metadata; owner-only writes retain the read-lock path. The byte estimates still include fixed allocator and hash-table allowances rather than measuring allocations exactly. Data records, replication buffers, migration staging, and allocator high-water behavior all contribute to RSS outside this receipt budget. The 60-second retry guarantee is unchanged.

## Follow-up

The subsequent [replica ACK performance optimization](replica-ack-performance.md) removes the per-write cluster status query and batches activation persistence. Its measurements supersede the slow FirstSuccessor throughput and activation timings above; the comparisons above remain evidence for the earlier receipt-layout changes.
