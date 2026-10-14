# Replica repair convergence latency: the run is idle, not CPU-bound

`results/flamegraph-correctness-20261014-155718/`, correctness mode, 8 nodes,
100,000 keys, RF=3, `FirstSuccessor`, concurrency 64, `worker_threads` 2,
24-core AMD Ryzen 9 7845HX, `perf record -F 99 --call-graph fp` against
`target/fp/release/hashring-rs`, 26.36 s, 7,829 samples, **0 lost**.

## The headline

**The flame graph does not contain the bottleneck.** Its shape is flat, exactly
as [cpu-profile-write-path.md](cpu-profile-write-path.md) predicts: no user-space
symbol reaches 1% self time (the largest is
`<alloc::string::String as Clone>::clone` at 0.96%), and the cost is transport,
allocator and kernel rather than any application function. Read on its own it
says "there is nothing to fix here".

Read on a time axis, against `run/summary.json`, it says something else:

| observation (`run/summary.json`) | seconds | share of the 26.36 s run |
| --- | --- | --- |
| `initial_full_rf_seconds` | 10.27 | 39.0% |
| `scale_in_transition_to_full_rf_seconds` | 6.48 | 24.6% |
| `initial_put` | 2.09 | 7.9% |
| `initial_get_verify` + `scale_out_verify` + `scale_in_verify` | 2.22 | 8.4% |
| `scale_out_transition_to_full_rf_seconds` | 4.76, but 4.75 of it is the concurrent mutation workload, so its own convergence wait is ~0 | ~0% |
| `scale_in_with_mutations` | 0.54 | 2.1% |

**Both `wait_for_full_rf` calls account for 16.9 s, 64% of the run, and they burn
almost nothing.** The initial one averages **0.10 cores** over its 10.27 s. The
per-second CPU histogram from `perf script` is unambiguous: the three seconds
`t = 22 s .. 25 s` of the post-scale-in wait, which are inside that wait, carry
**0.22% of the whole run's CPU** between them.

This is a latency floor, not a CPU floor. The run has 24 cores and spends most
of its time on under one of them.

## What the flame graph contributes

Self time by layer, aggregated over the whole capture from the sampled leaf IP,
so the flat shape is quantified rather than asserted:

| layer | share of profile |
| --- | --- |
| kernel | 30.68% |
| libc, no symbol and no caller | 17.61% |
| **gRPC/HTTP/2 transport** (h2 11.81, http 3.10, tonic 2.23, prost 1.76, mio 0.69, hyper 0.50) | **20.09%** |
| tokio | 8.50% |
| hashbrown + std hash | 3.00% |
| std allocator | 2.57% |
| serde | 1.17% |
| blake3 | 1.43% |
| redb | 0.09% |
| **hashring-node application frames** | **1.86%** |
| **hashring-coordinator application frames** | **0.06%** |
| **hashring-client / driver frames** | **0.09%** |
| *(residual libc, std and bytes machinery)* | *12.26%* |

Two things follow.

- **The application is 2.01% of CPU.** Coordinator logic is 0.06%, i.e. a
  rounding error. No amount of micro-optimisation inside this project's own
  code moves this run. This is the same conclusion
  [cpu-profile-write-path.md](cpu-profile-write-path.md) reached, now measured
  on a capture with a working call graph rather than inferred from symbol self
  time alone.
- **Transport is 20.09%, an order of magnitude above the application.** Each
  gRPC call on this path costs more CPU than the work it carries, which is a
  statement about round-trip count, not about CPU efficiency. It reinforces the
  direction [batched-replication-rpc.md](batched-replication-rpc.md) took from
  the follower side: on a round-trip-bound path, fewer round trips is the only
  lever that moves anything.

## Why convergence takes 10 s on an empty cluster

Seven nodes, 128 virtual nodes each, RF=3, zero keys. Reaching `live_rf == 3` on
every range should be near-instant work. It takes 10.27 s. Three independent
limits in the code serialise it, and they compound.

**1. No one seeds the initial replicas. A background poll does.**

`load_or_initialize` calls `reconcile_replica_repairs`
(`crates/hashring-coordinator/src/lib.rs:490`), which pushes a `Pending`
`ReplicaRepair` for every follower slot of every derived range. For this
topology that is **1,792 tasks in 42 `(owner, follower)` groups** (896 ranges x
2 followers; 7 owners x 6 possible followers), read directly out of
`run/coordinator.redb`. It only *reconciles*; it seeds nothing. The only seeders
are `seed_repairs_for_activation`, reached from a topology change
(`change.rs:449`, `change.rs:909`), and `resume_replica_repairs`, reached only
from the recovery loop (`src/main.rs:450`).

On a fresh bootstrap there is no topology change. So all 1,792 tasks sit
`Pending` until the recovery loop's first tick — and the recovery task is
spawned at `src/main.rs:437`, immediately before the server starts being
awaited at `src/main.rs:471`. `tokio::time::interval` fires its first tick at
once, so that tick runs within milliseconds of the coordinator process starting.
Nodes 1-7 did not call `register_node` until 40 ms later
(`run/process-logs/node-*.stdout.log`). The first tick is not a race the cluster
can win; it is a guaranteed no-op.

**2. One pass cannot finish: 42 > 32 — and the sort key degenerates.**

```rust
// crates/hashring-coordinator/src/lib.rs:53
const MAX_REPAIR_GROUPS_PER_PASS: usize = 32;

// crates/hashring-coordinator/src/repair.rs:258-266
let mut due = groups.into_values().collect::<Vec<_>>();
due.sort_by_key(|group| group.iter().map(|r| r.retry_count).max().unwrap_or(0));
due.truncate(MAX_REPAIR_GROUPS_PER_PASS);
```

This is **a rate limiter, not a defect on its own**, and the code says so:
`repair.rs:256-257` states that remaining groups "stay durable and are
considered on the next periodic pass". Nothing is lost — the 10 excluded groups
keep their `Pending` phase in redb and are selected on the following tick.

What makes it a problem is the interaction, and the sort key makes it worse than
it first looks. `slice::sort_by_key` is **stable**, and on a fresh topology every
group's `retry_count` is **0** (read straight out of `run/coordinator.redb`:
42 groups, one distinct `retry_count` value). With all keys equal the sort is a
no-op and the order is whatever `BTreeMap<(String, String), _>` yielded —
lexicographic by node id. So the 32-group quota becomes a **quota by node name**:

| deferred on the first pass | groups |
| --- | --- |
| `node-6 ->` | `node-3`, `node-4`, `node-5`, `node-7` |
| `node-7 ->` | `node-1`, `node-2`, `node-3`, `node-4`, `node-5`, `node-6` |

**Every obligation owned by `node-7` is served last, on every convergence.** In a
quiescent cluster this costs one extra 5 s tick and nothing more, because pass 2
has only 10 pending groups and all fit under the cap. Under sustained load —
anything that keeps 32 or more groups pending — the tail is starved
indefinitely, because the throttle has a position, not a fairness rule. The
natural tie-breakers (`oldest obligation first`, `largest group first`) are
absent, and `retry_count` only becomes a meaningful key once something has
actually failed.

**3. Passes only happen every 5 s.**

```rust
// src/main.rs:438
let mut interval = tokio::time::interval(Duration::from_secs(5));
```

`run_failure_pass`, which runs on its own 1 s loop, cannot cover the gap: it
returns early for the first `NODE_LEASE_DURATION` (`repair.rs:63`, 5 s at
`lib.rs:56`), and it does not seed replicas at all — the only three callers of
`seed_replica_group` are `merge.rs:221`, `repair.rs:275` and `repair.rs:489`.
Ticks land at t = 0, 5, 10, 15, 20, 25 s.

The arithmetic matches the measurement to within the driver's own poll
granularity:

- t=0 tick: no-op, as above.
- t=5 tick: `resume_replica_repairs` now has both endpoints registered for all
  42 groups (`repair.rs:238`), takes the first 32 in the degenerate
  lexicographic order, leaves the 10 `node-6`/`node-7`-owned ones.
- t=10 tick: the remaining 10, complete at ~10.27 s. **This is the observed
  `initial_full_rf_seconds` of 10.265811039.**

The same arithmetic explains the scale-in wait. `scale_in_with_mutations` takes
only 0.54 s, node-8 exits at t=19.665 s, and then `wait_for_full_rf` blocks for
**5.94 s** until t=25.6 s — the t=20 tick lands mid-transition, and the t=25 tick
is what finally satisfies it. Nothing is computing during that time.

**4. And each pass is a 4-wide RPC pipeline.**

```rust
// repair.rs:271 (periodic) and repair.rs:488 (activation)
try_map_bounded(groups, self.range_move_concurrency.min(4), ...)
```

`range_move_concurrency` is 16 here, so `min(4)` is 4. Inside a group,
`copy_replica` is also `min(4)` (`repair.rs:726`) while only `pause_range` gets
the full 16 (`repair.rs:732`). `seed_replica_group` then does a further serial
round of `get_replication_progress` -> `install_replication_checkpoint` ->
install/verify RPCs (`repair.rs:749-800`).

So even a pass that *could* finish runs 42 groups 4 at a time. This is the limit
that does **not** produce a round number, so it is not proven by the two timings
above; see the caveats. It is visible as the bulk of the 4.71 s the scale-out
change spends before activation completes, where `seed_repairs_for_activation`
is reached from `change.rs:909` at the same width of 4.

The driver's own poll is a fifth, smaller quantiser: `wait_for_full_rf` sleeps
250 ms between `replica_status` calls
(`crates/hashring-experiment/src/lib.rs:894`), so even a perfect convergence
would be reported up to 250 ms late.

## Cost model

Convergence is bounded below by

```
ceil(groups / MAX_REPAIR_GROUPS_PER_PASS) x 5 s   (the tick quantisation)
  + ceil(groups / 4) x per-group latency           (the pipeline width)
```

With 42 groups the first term alone is `ceil(42/32) x 5 s = 10 s`, regardless of
cluster size, key count, or available CPU. The floor is a property of the
constants, not of the load.

Scaling up makes it worse rather than better. At 8 members the group count grows
toward 8 x 7 = 56, so `ceil(56/32)` is still 2 ticks but the second tick carries
24 groups instead of 10, and the truncation is worse in absolute terms. There is
no member count at which this constant stops being the binding constraint.

## What follows

Ordered by how much of the 64% they recover.

1. **Make the periodic pass able to finish.** With 42 groups and a cap of 32,
   every convergence costs at least two ticks, and each extra tick is a hard
   5 s. Looping until no group remains due inside one tick, or raising
   `MAX_REPAIR_GROUPS_PER_PASS`, removes 5 s from every convergence in the
   system. This is the single largest item and it is a constant plus a loop,
   not a redesign.
2. **Fix the selection order before raising the cap.** If the cap is raised, the
   degenerate lexicographic sort stops mattering; if it is not, the cap actively
   starves `node-7`. `retry_count` is 0 for every healthy group, so it needs a
   second key — oldest obligation, or largest group first. This is a two-line
   change and it is what makes item 1 safe to do repeatedly.
3. **Seed on bootstrap.** `load_or_initialize` already knows the full desired
   follower set and already creates the 1,792 `Pending` tasks. Running
   `seed_repairs_for_activation` once after the server starts would replace two
   5 s ticks with the seeding work itself, and would make the steady-state poll
   a retry mechanism rather than the primary path.
4. **Stop gating the first tick on registration.** Requiring both
   `process_instances` entries is correct for correctness but wrong for
   liveness: it turns the t=0 tick into a guaranteed no-op. Re-ticking on
   registration completion, or making the pass wait for registrations with a
   short timeout, recovers a tick.
5. **Widen the seed pipeline.** `min(4)` is applied twice over the same 42
   groups, while `range_move_concurrency` is already 16 and unused here. This
   does not move the 10 s floor, but it is the whole cost of the scale-out
   activation path.

None of these touches the data path, the failure handling, or the durable state
format.

## What this profile cannot answer

- **Whether the `node-6`/`node-7` ordering would actually starve in practice.**
  The degenerate sort is proven from the state file, and the deferred set is
  proven from the BTreeMap key order. That pass 2 then completes all 10 is a
  prediction from the code, not an observation — this run never sustains 32 or
  more pending groups after the first pass. A soak with a slow or flapping
  follower would show whether it starves or merely delays.
- **How much of the 10.27 s is the 4-wide pipeline rather than the 5 s
  quantisation.** The quantisation is proven: 10.27 s and 5.94 s both land on
  tick boundaries, and 42 > 32 is arithmetic from the state file. The
  pipeline's share is inferred, not measured — it needs per-group timings,
  which the coordinator does not emit. `repair.rs:666` already has a
  `tracing::debug!` on group start; enabling it would settle this directly.
- **Anything about the steady-state data path.** This capture is 64% idle
  convergence. The put phase averages 5.95 cores over 2.09 s and the initial
  verify 5.33 cores over 0.80 s; those are shaped by the write lock described in
  [state-write-lock-profile.md](state-write-lock-profile.md) and
  [write-path-saturation.md](write-path-saturation.md), not by anything here.
- **Kernel attribution.** As [cpu-profile-write-path.md](cpu-profile-write-path.md)
  records, kernel samples carry no user stack, so the 30.68% cannot be split
  between loopback TCP, netfilter, futex and page faults from this data.
- **What the 17.61% of unsymbolised libc samples are.** They carry no symbol
  and, for the frame-pointer walk, no caller. That is a capture limitation, not
  application work, and it is why the layer table above is a floor on libc
  rather than a measurement of it.

## Reproducing

```sh
./scripts/flamegraph.sh correctness
```

The conclusions above need two things the flame graph does not give you:

```sh
# the phase boundaries and the convergence timers
python3 -c 'import json;print(json.load(open("run/summary.json"))["observations"])'

# the repair-task and group counts, straight from durable state
python3 -c 'import json;d=json.load(open("cluster.json"));
print(len(d["replica_repairs"]),
      len({(r["owner_node_id"], r["node_id"]) for r in d["replica_repairs"]}))'
```

(`cluster.json` is the `CLUSTER_STATE_KEY` blob, which is a contiguous
`serde_json::ClusterState` document in the redb file.)

For the CPU split by phase, bucket `perf script -i perf.data --ns` output against
the `unix_ms` fields in `run/events.jsonl`. The graph itself still has to be
read inverted, as [cpu-profile-write-path.md](cpu-profile-write-path.md) warns.

See also [cpu-profile-write-path.md](cpu-profile-write-path.md) for why this
capture had to be taken with `--call-graph fp` and what the flat profile shape
means, [batched-replication-rpc.md](batched-replication-rpc.md) for the
follower-side reduction in round trips that this profile would otherwise have
looked like the answer to, and
[worker-pool-sizing.md](worker-pool-sizing.md) for why `worker_threads` is 2
here and why that is not what makes this run slow.