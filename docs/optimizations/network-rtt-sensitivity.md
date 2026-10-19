# Network RTT sensitivity of the write path

Every CPU profile in `results/flamegraph-*` is a single-host run: the driver, the
coordinator and every data node talk gRPC over `127.0.0.1`. Those profiles show the
transport and kernel path is the largest CPU consumer, which invites the conclusion
that the network is the bottleneck. That conclusion had never been tested, and it
cannot be tested from those profiles, because they contain no network latency at
all: the loopback round trip is ~40 us, so every hop looks free.

This note measures what the write path does when the round trip stops being free.
The result is that on loopback the network is **not** the limiter, that the write
path serializes exactly the round trips the acknowledgement policy implies, and that
the two have a crossover which this harness can locate.

## Method

`scripts/netem-sweep.sh` installs one netem qdisc on `lo` and re-runs the correctness
experiment at a series of delays. Everything is inherited into a
`unshare --user --map-root-user --net` network namespace, so no privilege is needed
and only this run's traffic is shaped.

Two facts make a single `lo` qdisc sufficient and its units correct:

- netem delays a packet on egress, and a loopback packet egresses `lo` once per
  direction, so **RTT is twice the configured delay**. Measured here, not assumed:
  `delay 0 -> RTT 0.037 ms`, `delay 5ms -> RTT 10.036 ms`.
- The sweep pings at every point and records the RTT it actually got. Every table
  below is in measured RTT, never in configured delay.

`DELAYS=off` removes the qdisc and `DELAYS=0` keeps it attached with no delay. The
gap between them is the instrument's own cost -- a shaped loopback queues packets in
softirq where an unshaped one does not -- so the slope is fitted from `0` onward and
never from `off`.

Runs are medians of 2 or 3, which is the limit of what this harness resolves: the
`--concurrency 64` and `128` loopback points spread 33% and 44% between
repetitions of one unchanged binary, the same problem `sweep.sh` documents. The
latency column is far better behaved than the throughput column, for the reason in
the next section, and the conclusions below lean on it accordingly.

## The latency sample is the load-bearing column

`client_put_latency_us` is the experiment's own sample of 128 puts issued **one at a
time** after the initial verify, on an otherwise quiet cluster
(`crates/hashring-experiment/src/lib.rs:613`). It is a single-operation, unloaded
latency, not the latency under `--concurrency`.

That is usually a defect and here it is the point. Its slope against RTT is a
difference, so the queueing that `--concurrency` adds cancels out of it, and what
remains is the number of round trips the write path serializes on. The throughput
column carries that queueing and is correspondingly noisy; the latency slope is
stable to three significant figures across every sweep below.

## What the write path serializes

`first-successor`, 4 nodes, RF=3, 100,000 keys, `--concurrency 64`:

| delay | RTT | put keys/s | p50 us | cores |
| --- | --- | --- | --- | --- |
| off | 64 us | 46,740 | 150 | 2.54 |
| 0 | 42 us | 39,776 | 161 | 2.83 |
| 100us | 234 us | 48,843 | 572 | 2.60 |
| 200us | 420 us | 40,311 | 988 | 2.30 |
| 400us | 834 us | 24,759 | 1,793 | 1.98 |

```
p50_us = 91 + 2.06 * RTT_us      (r = 0.999, 4 points)
```

**2.06 round trips per write.** That is the client-to-owner hop plus one
owner-to-follower hop, which is what `first-successor` should cost: it replicates to
`replicas[1..2]`, one follower, whose ACK returns in that same response
(`crates/hashring-node/src/replication.rs:67`). The second follower is never waited
for.

The prediction is falsifiable, so it was tested against the other policy.
`owner-only` replicates to no follower at all and should cost one hop. Same sweep:

| delay | RTT | put keys/s | p50 us | cores |
| --- | --- | --- | --- | --- |
| 0 | 15 us | 60,308 | 76 | 2.34 |
| 200us | 428 us | 66,555 | 491 | 2.23 |
| 400us | 839 us | 55,365 | 886 | 2.02 |
| 800us | 1614 us | 32,980 | 1,698 | 1.38 |

```
p50_us = 54 + 1.01 * RTT_us      (r = 1.000, 4 points)
```

**1.01**, as predicted, and a lower intercept, because the owner does not wait for
replication at all. A method that could not distinguish these two numbers would not
be measuring round trips. The same run also explains why
[batched-replication-rpc.md](batched-replication-rpc.md) could not make progress:
with one hop to remove, and `first-successor` at two, the write path's serialized
RTT is a small integer, not a term batching can shrink much.

## Throughput: the loopback is not the limiter

Raising RTT tenfold, from 42 us to 420 us, moves single-op latency from 161 us to
988 us and leaves put throughput unchanged inside the noise (39,776 to 40,311
keys/s). At 834 us throughput finally falls, to 24,759 keys/s.

Sweeping offered concurrency at both ends of that range separates the two regimes.
Same binary, same 100,000 keys, medians of 2:

| RTT | conc 64 | conc 128 | conc 256 | conc 512 | cores at 64 |
| --- | --- | --- | --- | --- | --- |
| ~30 us (loopback) | 48,662 | 55,722 | 74,118 | 37,232 | 2.84 |
| ~2 ms | 11,746 | 22,272 | 34,111 | 29,508 | 1.07 |

At 2 ms the path behaves exactly as a latency-bound path must: throughput scales
almost linearly with offered concurrency (11.7k to 34.1k for 64 to 256) at
*lower* CPU than the loopback runs, landing at 0.56 to 0.77 of the
`concurrency / p50` ceiling, the shortfall being the queueing the unloaded p50 does
not contain.

On loopback the same sweep does not scale at all: 48.7k, 55.7k, 74.1k, 37.2k is
non-monotonic, and the ratio to the concurrency-latency ceiling is 0.01 to 0.13, off
by one to two orders of magnitude. Whatever caps the loopback write path is not
offered concurrency divided by network latency. It is local, it lands somewhere in
the 40,000 to 75,000 keys/s band across this concurrency range, and it uses 2.3 to
3.0 of the host's 24 cores.

That band is worth being careful about, because it is not a measured ceiling. The
concurrency sweep is not monotonic and its two lowest points spread 33% and 44%
between repetitions, so the honest statement is that loopback throughput is
*insensitive* to offered concurrency over 64 to 512 and lands in that band, not that
it saturates at any particular number in it. The `first-successor` sweep at
`--concurrency 64` alone, which repeated well enough to medians, put it at 39,776
keys/s, and that is the number the crossover below uses. A single sweep cannot
distinguish "locally limited" from "sharply peaked", and pinning down which is a
separate question about the local resource, not about the network.

CPU falls monotonically as RTT rises in every sweep above, from 2.84 to 1.07 cores.
Injected latency becomes waiting, never work. Nothing here converts a slower network
into more CPU, and the reverse also does not hold: removing CPU from the transport
path, which is what the flame graphs invite, does not remove this loopback plateau,
because the plateau is not made of transport CPU.

## Where the two meet

The crossover is where the network's own ceiling falls below the local one:

```
concurrency / (slope * RTT) = local ceiling
```

For `first-successor` at `--concurrency 64`, with the slope of 2.06 and the
39,776 keys/s measured at loopback, that is **RTT ~ 780 us**. Loopback sits at
~40 us, twenty times below it. A same-datacenter LAN at 0.1 to 0.3 ms RTT is below
it. A cross-rack or WAN link at 1 ms or more is past it, and there the network path
is the limiter.

This number inherits the uncertainty above: it is a ratio of a fitted slope and a
single noisy throughput, so it should be read as an order of magnitude, and it moves
with `--concurrency` and node count, which are the two axes that set the local
ceiling. That is the reason the sweep takes `NODE_COUNTS` and `CONCS` as lists
rather than fixing them: the crossover is the quantity that decides whether this
project should spend effort on round trips or on the local resource, and it is not
a property of the network.

## What this does not answer

- **It is not a model of a cluster network.** One shared loopback link is shaped, so
  every hop is delayed and no bandwidth is charged per hop. There is no per-core link
  limit, no NIC offload, and no receive cost on another machine. A rate limit can be
  added with `RATES=`, but it is a shared link in the same way.
- **It does not name the local ceiling.** That loopback write throughput is insensitive
  to offered concurrency over 64 to 512, and lands in the 40,000 to 75,000 keys/s
  band, is measured here; what causes that is not, and neither is whether it is a
  plateau or a peak. The candidate from
  [write-path-saturation.md](write-path-saturation.md) is the single
  `Arc<RwLock<NodeState>>`, whose owner acquisitions were 67% to 69% waiting at
  concurrency 64 with 1.5 of 24 cores busy, but that note also records that it
  cannot separate lock contention from client-side saturation. Both are still open,
  and neither is answered here.
- **The absolute throughputs are not comparable to `results/flamegraph-*`.** Those
  runs sit in the host netns, where this host's nftables and conntrack rules are in
  the path; a fresh namespace has empty netfilter tables. They also use 1,000,000
  keys against 100,000 here.
- **One node count, one key count, one concurrency per sweep.** See the crossover
  discussion above for why that is the binding limitation on what can be concluded.
- **The driver is on the same host and inside the shaped link.** At 4 nodes a third
  of the profile is the client, so part of what the RTT shapes is the generator's
  own round trip.

## Reproduce

The sweep defaults to the tracked `examples/experiment.yaml` and pins
`--mode correctness`. The numbers above came from a local, untracked config with the
same replication factor, policy and virtual nodes but seed 114514 and the default
byte budgets, so expect the same slope and not the same digits.

```sh
cargo build --release

# latency against RTT, first-successor: the slope is the round trips per write
./scripts/netem-sweep.sh

# throughput against offered concurrency at both ends of the range
DELAYS="0 1ms" CONCS="64 128 256 512" REPS=2 ./scripts/netem-sweep.sh

# the same slope under a policy that waits for no follower
sed 's/^policy: first-successor/policy: owner-only/' \
  examples/experiment.yaml > /tmp/owner-only.yaml
DELAYS="0 200us 400us 800us" CONFIG=/tmp/owner-only.yaml REPS=2 ./scripts/netem-sweep.sh
```

Raw rows land in `results/netem-<timestamp>/netem.tsv`, medians in `medians.tsv`, and
the fitted slope with its reading in `analysis.txt`.

## See also

- [write-path-saturation.md](write-path-saturation.md) -- the write lock as the
  measured serialized resource, and the open question of contention versus client-side
  saturation.
- [cpu-profile-write-path.md](cpu-profile-write-path.md) -- "latency-bound, not
  CPU-bound", and the observation that only removing a round trip or a lock wait
  shows up as wall-clock improvement. This note measures that round-trip term
  directly instead of inferring it.
- [batched-replication-rpc.md](batched-replication-rpc.md) -- batching that removed
  one RPC in five and could not reach its bound, because the per-stream arrival rate
  leaves the queue empty. The 2.06 slope explains why there was so little to batch.
