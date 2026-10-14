# CPU profile: where the time actually goes

Captured with `scripts/flamegraph.sh` (`perf record -F 99 --call-graph dwarf,8192`).
Correctness mode, 20,000 keys, concurrency 64, RF=3, `OwnerOnly`, 128-byte values,
9 data nodes plus a coordinator and a driver on a 24-core AMD Ryzen 9 7845HX.
46.5 s of wall time, 13,507 samples, 1.78% sample loss.

Self time below is `perf report --no-children`, which reads the sampled IP
directly and is therefore independent of call-graph quality. Kernel frames are
grouped by symbol prefix; the two entries marked "unclassified" are spinlocks,
RCU, and list traversal that fall outside the named buckets.

## Where the time goes

| | share |
| --- | --- |
| user space (`hashring-rs` + libc + vdso) | 65.96% |
| kernel | 29.17% |
| *(below the 0.01% report floor)* | ~1.5% |

User space splits further:

| | share of profile |
| --- | --- |
| allocator (`alloc`/`free`/`realloc`, incl. libc) | 11.18% |
| `Vec::from_iter` / `collect` | 5.41% |
| `Clone::clone` | 2.10% |
| memcpy / `put_slice` | 0.44% |

Kernel:

| | share of profile |
| --- | --- |
| scheduler (wakeup, load balance, runqueue) | 4.17% |
| SRSO / retpoline mitigation (`srso_*`, `entry_SYSRETQ`, `entry_SYSCALL_64`) | 3.65% |
| page zeroing (`clear_pages`, `copy_folio_from_iter_atomic`) | 2.44% |
| futex | 1.26% |
| interrupts and timer | 1.20% |
| network stack (TCP/IP/skb) | ~3.1% |
| epoll | 0.36% |
| netfilter (conntrack, nftables) | 0.10% |

## Reading

The shape is flat. No single symbol exceeds 2.4% self time, and the top-30 list
is dominated by allocator and `Vec` machinery rather than any application
function. That is itself the main finding: **there is no hot spot to fix, only
a broad allocation and syscall floor.**

Three things follow that are worth acting on.

- **Allocation is the single largest addressable cost at ~19% of the profile**
  (allocator + `from_iter` + `clone`). `Vec::from_iter` at 5.41% is the largest
  individual symbol at 3.35%. The allocator pressure traces back to repeated
  `Vec`/`String` construction on the RPC path rather than to one caller.
- **`TopologySnapshot::key_token` costs 3.01% self.** It is only
  `xxh3_64_with_seed`, which is fast, but `put` computes it twice: once inside
  `owner_error` (`service.rs:503` calls `topology.owner`, which calls
  `key_token`) and again at `rpc.rs:356`. Computing it once per request and
  threading it through is a small, local change.
- **Kernel time is mostly fixed overhead.** Of 29.17%, SRSO/retpoline (3.65%)
  and page zeroing (2.44%) are environment and allocator costs that no
  application change removes. Scheduler + futex (5.43%) is the only kernel
  bucket that responds to reducing work, and it is small.

`btree::search` at 1.61% is the `partition_point` inside
`replica_node_ids_for_token` and `owner_for_token`. With 128 virtual nodes per
member the `assignments` slice is short, so this is call volume rather than
log-depth.

## What this profile cannot answer

The run used only 1.5 cores of 24 (52.7 s user + 19.2 s system over 47.7 s
wall). The system is latency-bound, not CPU-bound. Per this capture,
throughput work that removes CPU will not show up as wall-clock improvement;
only work that removes a **round trip** or a **lock wait** will.

Kernel-side samples carry no user stack (perf records a DWARF user stack only
when the sampled IP is in user space), so the 29.17% kernel time cannot be
attributed to a Rust caller from this data. In particular the futex total of
1.26% here cannot be split between runtime worker wakeups, the node state
write lock, and gRPC's own parking.

A renderer warning worth repeating: the emitted SVG is an **inverted** flame
graph. `all` is at the bottom and leaves at the top, so a frame's children are
at `y-16`. Reading it as a normal flame graph produces self times that sum to
8x the root.

## Reproducing

```sh
cargo build --release
cat > /tmp/experiment_cpu_profile.yaml <<'YAML'
mode: correctness
nodes: 10
keys: 20000
concurrency: 64
policy: owner-only
YAML
CONFIG=/tmp/experiment_cpu_profile.yaml ./scripts/flamegraph.sh
```

The script profiles whatever `experiment` runs, so it needs a settings file;
`examples/experiment_cmach.yaml` is its default. This capture predates that
default and used the flag defaults listed above, so it needs its own file. The
manifest in `<out>/run/` records what was actually resolved.

Check `perf-record.log` in the output directory for sample loss before trusting
the graph. Aggregate self time with:

```sh
perf report -i <out>/perf.data --stdio --no-children --percent-limit 0.01 \
  --sort dso,symbol -F overhead,dso,symbol
```

See also [state-write-lock-profile.md](state-write-lock-profile.md) for the
`HASHRING_PROFILE_WRITES` lock wait/hold counters, which measure the latency
side that this CPU profile cannot see, and
[batched-replication-rpc.md](batched-replication-rpc.md) for why the follower
path takes fewer state writes than there are replicated entries.
