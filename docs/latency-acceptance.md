# The acceptance run of the `latency` mode

This is the run the `latency` mode was accepted with. It answers the three
questions the requirement asks: does the mode measure what it says it
measures, does it measure it correctly, and what does the cluster do under a
load that is offered at a fixed rate.

The numbers come from 10 nodes, 3 replicas, the `OwnerOnly` acknowledgement
policy, 100,000 keys of 128 bytes, and a `GET:PUT:DELETE` mix of `90:9:1`.
Every stage ran for 60 seconds after a 10 second warmup, and every
configuration was run three times. The raw tables are in
[`docs/data/latency-acceptance/`](data/latency-acceptance/), and
[`scripts/latency-sweep.sh`](../scripts/latency-sweep.sh) is the script that
produced them.

## What was run

| | |
| --- | --- |
| Nodes | 10 |
| Replicas | 3 |
| Acknowledgement | `OwnerOnly` |
| Key space | 100,000 keys of 128 bytes |
| Mix | `90:9:1` |
| Virtual nodes | 128 |
| Stage | 10 s warmup, 60 s measured |
| Repetitions | 3 per point |
| Distributions | `uniform`, `zipf:0.99` |
| Host | 24 cores, 32 GB, Linux 7.2.11-zen1-1-zen |
| Run | `latency-acceptance-20261022-172621`, 39 minutes |
| Binary | `e3cf261f`, commit `52adf85`, clean source |

The hash above is the hash of the binary that ran, recorded by the run
itself in [`host.txt`](data/latency-acceptance/host.txt) and in the
`manifest.json` of every run. The build is deterministic at a fixed path —
two `cargo build --release` runs of the same source at the same path give
the same bytes — and the source of the run is the commit it was built from,
`52adf85`. The source tree digest comes back from any path; the binary's
hash does not, because the absolute build root is compiled into the binary.

The target rates are a share of the capacity the cluster showed in
`performance` mode, which measured **84,054 operations per second** for the
write path on the same host with the same configuration. The rates are 0.25,
0.5, 0.75 and 0.9 of that: **21,013**, **42,027**, **63,040** and **75,648**
operations per second.

A run past the capacity was added, because the capacity is a *write* capacity
and the campaign is mostly reads. The ladder offers 1, 1.5, 2 and 3 times the
capacity and shows where the mode gives up.

The host is a shared machine, so every table carries the speed of the host the
run landed on: the preload writes the same 100,000 keys with the same 64
tasks before the load starts, and it is the run's own measure of how fast the
machine was. During this campaign that speed was **80,993 to 86,500
operations per second, a spread of 6.8%**. The run started at 17:26 with 14.6
of the machine's 32 GB of memory free and a load average of 2.24, so it was
not an empty machine: 6.8% is what that cost, and it is still small enough to
make the comparison between the two distributions below worth reading. An
earlier campaign on a busier machine moved by a factor of 1.8 and could not
support it.

## The latency of a load offered at a rate

Every repetition, with the p50, p95, p99 and p999 of the `GET` latency in
microseconds, and the speed of the host that run landed on.

| Run | Distribution | Rate | Host | p50 | p95 | p99 | p999 | max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| uniform-rep1 | uniform | 21,013 | 81,605 | 56 us | 78 us | 95 us | 240 us | 10,543 us |
| uniform-rep1 | uniform | 42,027 | 81,605 | 57 us | 81 us | 108 us | 280 us | 3,665 us |
| uniform-rep1 | uniform | 63,040 | 81,605 | 57 us | 95 us | 181 us | 645 us | 20,975 us |
| uniform-rep1 | uniform | 75,648 | 81,605 | 62 us | 134 us | 548 us | 1,892 us | 5,711 us |
| uniform-rep2 | uniform | 21,013 | 84,872 | 56 us | 76 us | 92 us | 187 us | 10,695 us |
| uniform-rep2 | uniform | 42,027 | 84,872 | 58 us | 82 us | 106 us | 202 us | 1,622 us |
| uniform-rep2 | uniform | 63,040 | 84,872 | 59 us | 98 us | 196 us | 673 us | 21,247 us |
| uniform-rep2 | uniform | 75,648 | 84,872 | 63 us | 135 us | 592 us | 2,633 us | 8,423 us |
| uniform-rep3 | uniform | 21,013 | 82,558 | 57 us | 78 us | 95 us | 209 us | 10,591 us |
| uniform-rep3 | uniform | 42,027 | 82,558 | 57 us | 82 us | 107 us | 215 us | 3,653 us |
| uniform-rep3 | uniform | 63,040 | 82,558 | 58 us | 97 us | 190 us | 676 us | 19,823 us |
| uniform-rep3 | uniform | 75,648 | 82,558 | 62 us | 123 us | 440 us | 1,652 us | 5,795 us |
| zipf-0-99-rep1 | zipf:0.99 | 21,013 | 86,500 | 56 us | 78 us | 95 us | 243 us | 10,311 us |
| zipf-0-99-rep1 | zipf:0.99 | 42,027 | 86,500 | 57 us | 82 us | 110 us | 266 us | 9,327 us |
| zipf-0-99-rep1 | zipf:0.99 | 63,040 | 86,500 | 57 us | 93 us | 172 us | 510 us | 20,815 us |
| zipf-0-99-rep1 | zipf:0.99 | 75,648 | 86,500 | 62 us | 120 us | 439 us | 2,381 us | 19,503 us |
| zipf-0-99-rep2 | zipf:0.99 | 21,013 | 80,993 | 57 us | 79 us | 95 us | 213 us | 10,767 us |
| zipf-0-99-rep2 | zipf:0.99 | 42,027 | 80,993 | 58 us | 82 us | 109 us | 233 us | 8,151 us |
| zipf-0-99-rep2 | zipf:0.99 | 63,040 | 80,993 | 58 us | 97 us | 188 us | 576 us | 20,671 us |
| zipf-0-99-rep2 | zipf:0.99 | 75,648 | 80,993 | 63 us | 127 us | 524 us | 3,187 us | 20,399 us |
| zipf-0-99-rep3 | zipf:0.99 | 21,013 | 84,556 | 57 us | 78 us | 96 us | 234 us | 10,783 us |
| zipf-0-99-rep3 | zipf:0.99 | 42,027 | 84,556 | 56 us | 77 us | 98 us | 197 us | 9,447 us |
| zipf-0-99-rep3 | zipf:0.99 | 63,040 | 84,556 | 57 us | 92 us | 173 us | 582 us | 20,031 us |
| zipf-0-99-rep3 | zipf:0.99 | 75,648 | 84,556 | 62 us | 112 us | 304 us | 1,157 us | 19,631 us |

**The achieved rate was the target rate in every one of these 24 stages.** Not
one request was dropped for want of an in-flight slot, not one failed, the
client never had to try a request twice, and no window was marked as one the
sender did not keep up with. Every number in this table describes the cluster
and not the driver.

The retry counter of this run counts the retries of the requests and the
retries of the topology refreshes behind them — the counter was widened
before this run — and it read zero in every measured window of every run.
The client polls the coordinator for the topology every five seconds, so it
did make refreshes during the run; none of them needed a retry, and no
window of any run carries a non-zero retry count.

**The median does not move.** The p50 is between 56 and 63 us across a factor
of 3.6 of load. A load generator that reported only a median would have shown
a flat line and called it a stable cluster.

**The tail moves, and the top of the campaign is where it starts.** The p99
holds at 92 to 110 us through half the capacity, reaches 172 to 196 us at
0.75, and is at 304 to 592 us at 0.9. The p999 stays under 3.2 ms
throughout. The maximum does not behave: the stage whose p999 was 673 us
still had a request that waited 21,247 us, which is the tail of a shared
machine rather than a queue — a queue shows up in the p999 as well.

**The two distributions cost the same latency.** The three uniform runs
landed at 81,605, 82,558 and 84,872 operations per second of preload and the
three `zipf:0.99` runs at 80,993, 84,556 and 86,500, so the two medians are
2.4% apart — inside the campaign's own 6.8% spread. Under `zipf:0.99` the p50
was 56 to 63 us against the uniform run's 56 to 63, and the p99 was 95 to
524 us against 92 to 592. An earlier campaign, run on a machine that moved by
a factor of 1.8 during it, suggested the skewed load was faster; at this host
speed the two are the same within the noise of the machine.

**What the skew does cost is the busiest node.** See the next table: the same
top rate puts the busiest node at 36.3% of a core under `uniform` and 48.8%
under `zipf:0.99`. The cluster has the headroom for it, which is why the
latency does not show it.

## The load balance

The two layers of the load balance. These numbers do not depend on the host:
the key space is divided by the ring, and the requests land on the keys. The
table is the median over the three repetitions, whose individual numbers are
in the data directory: the key space columns are identical in all three,
because the ring is built the same way every time, and the operations column
moves by at most 0.014 between them.

| Distribution | Rate | Owned keys, max over mean | Owned keys, cv | Operations, max over mean | Operations, cv | Busiest node's CPU |
| --- | --- | --- | --- | --- | --- | --- |
| uniform | 21,013 | 1.116 | 0.088 | 1.143 | 0.089 | 10.7% |
| uniform | 42,027 | 1.116 | 0.088 | 1.145 | 0.089 | 20.5% |
| uniform | 63,040 | 1.116 | 0.088 | 1.142 | 0.088 | 30.1% |
| uniform | 75,648 | 1.116 | 0.088 | 1.144 | 0.089 | 36.3% |
| zipf:0.99 | 21,013 | 1.116 | 0.088 | 1.861 | 0.339 | 14.6% |
| zipf:0.99 | 42,027 | 1.116 | 0.088 | 1.859 | 0.338 | 28.1% |
| zipf:0.99 | 63,040 | 1.116 | 0.088 | 1.859 | 0.338 | 40.2% |
| zipf:0.99 | 75,648 | 1.116 | 0.088 | 1.859 | 0.338 | 48.8% |

**The key space is divided evenly, and it does not depend on the load.** The
128 virtual nodes give every node 1.116 times the mean number of keys, a
coefficient of variation of 0.088, and the same numbers at every rate and
under both distributions.

**The requests are not divided evenly, and they are not supposed to be.**
Under `uniform` the busiest node serves 1.14 times the mean number of
requests, which is the factor of the key space, because the requests land on
the keys. Under `zipf:0.99` the busiest node serves 1.86 times the mean, a
coefficient of variation of 0.338 against the uniform run's 0.089: the hot
keys concentrate on the nodes that own them, and the load follows the keys.

That is the honest answer to "is the load balanced". The key space is. The
requests are as balanced as the keys they touch, and under a skewed
distribution the requests are skewed. A ring cannot spread a hot key.

## The virtual node count

The same load at four virtual node counts, at the lowest and the third rate
of the campaign; the table shows the lower of the two, which the higher one
agrees with to within 0.008. The key space is what the ring divides, so this
is the measurement of how evenly 100,000 keys fall on 10 nodes.

| Virtual nodes | Owned keys, max over mean | Owned keys, cv | Operations, max over mean | Operations, cv |
| --- | --- | --- | --- | --- |
| 16 | 1.200 | 0.177 | 1.206 | 0.183 |
| 64 | 1.255 | 0.139 | 1.242 | 0.134 |
| 128 | 1.116 | 0.088 | 1.147 | 0.089 |
| 256 | 1.087 | 0.048 | 1.066 | 0.049 |

**More virtual nodes divide the key space more evenly.** The coefficient of
variation falls from 0.177 at 16 virtual nodes to 0.048 at 256, and the
spread of the busiest node's share falls from 1.200 to 1.087. The ring is a
lottery, so the division is a sampling, and a larger ring is a lottery with
more tickets: the spread of the draw shrinks as the number of tokens grows.

**The 64 virtual node row is the one to distrust.** Its coefficient of
variation is better than the 16 virtual node row's, but its worst node owns
more keys than the 16 virtual node run's. The two statistics disagree because
they are two statistics of one draw, and one draw of a lottery says little
about the lottery. What the table supports is the direction, and the design
rule that follows from it: 64 virtual nodes are enough to divide 100,000 keys
evenly on 10 nodes, and the default of 128 costs nothing but a larger ring.

**The request distribution follows the key space and does not move.** It is
1.07 to 1.24 times the mean with a coefficient of variation of 0.049 to 0.183
at every virtual node count, because it is the key space that decides where a
request goes.

## Where the mode gives up

The capacity run measures the write path, so the campaign's top rate is well
below the knee of the read-heavy load. The ladder offers more than the
capacity, in one run so that the host's speed is the same for every row. It
ran at 85,497 operations per second of preload.

| Target rate | Achieved | p50 | p95 | p99 | p999 | Late | Sender saturated |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 84,054 (1x) | 84,054 | 64 us | 111 us | 341 us | 2,341 us | 0 | no |
| 126,081 (1.5x) | 103,522 | 35,391 us | 71,103 us | 86,463 us | 100,031 us | 225,590 | no |
| 168,108 (2x) | 100,937 | 39,199 us | 72,511 us | 86,143 us | 97,727 us | 671,713 | no |
| 252,162 (3x) | 100,619 | 39,359 us | 72,703 us | 86,591 us | 101,951 us | 1,515,428 | no |

**The knee is between 1 and 1.5 times the write capacity.** The campaign's
top rate, 0.9 of the capacity, still has a p50 of 62 to 63 us and a p99 of
304 to 592 us, and one times the capacity is still flat: a p50 of 64 us and a
p99 of 341 us with nothing dropped. At 1.5 times the capacity the median has
grown to 35 ms and 225,590 requests were dropped for want of an in-flight
slot. Past that the mode breaks down, and it breaks down in the way it was
built to break down:

- The requests that could not be sent were **dropped**, 225,590 of them at
  1.5x, 671,713 at 2x and 1,515,428 at 3x, not queued.
- The p50 of 35 to 39 ms is the queueing delay inside the in-flight window.
  4,096 requests in flight at the 100,619 to 103,522 operations per second
  the cluster was doing is 39.6 to 40.7 ms of waiting, and the measured
  median is 35.4 to 39.4 ms: the window, not the cluster, is what sets the
  throughput above the knee, which is why the last three rows report
  achieved rates within 3% of each other while the offered rate doubles. A
  larger `--max-in-flight` would raise it and lengthen the queue all the
  same.
- The sender was **not** the problem. `sender_saturated` is false in every
  window of every run, the `sender_lag` the event log now carries never went
  above 1 request in a measured window, and the per-window `behind` count
  never went above 67 requests in a measured window of the campaign or of
  the virtual node runs — 67 out of the 75,648 the schedule called for that
  second, 0.09% of it. The worst window of the ladder was 3,589 out of
  252,162, 1.4%, in the stage that was already dropping most of its own
  schedule. The mode says the cluster could not keep up, and it says which
  of the two it was.
- Not one request **failed** and not one **retried**, anywhere in these 11
  runs: 96,686,119 operations came back with zero `deadline`, zero
  `rpc`, zero `operation`, zero `topology`, zero `protocol` and zero
  `not_found`, even at three times past the capacity. What the cluster could
  not take was never sent to it.

The ceiling itself is a property of this host at this time, not a constant
of the cluster: the three stages above the knee are limited by the window to
about 100,000 operations per second, and a machine with the nodes to
themselves would put the knee somewhere else.

## How the numbers were checked

A stage that does not account for every request it issued is not a
measurement, so the mode does not report one. Before a stage's numbers are
written, the run checks that

```
ops + errors == issued        (issued excludes the requests the in-flight limit dropped)
```

and it fails with an error rather than publish a rate that is quietly short.
All 36 stages of this run pass it, and the 24 campaign stages also
report an achieved rate equal to their target.

The check earned its place before this run: it found two bugs in the
measurement during the campaign this report replaces, both of which would
have gone unnoticed, and both were fixed before this run:

- The totals of a stage were not reset before the warmup that followed it, so
  a warmup reported the measured phase of the stage before it as its own.
- The fold that closes a phase merged the latency histograms into the stage's
  totals but left the operation and error counters behind, so every request
  that was still in flight when the sampler's last window ticked was counted
  in its window and then thrown away. The run that caught it was short by 2
  requests out of 4,209,840.

Two more things are checked on every run, and both hold for all 11 runs here:

- Every `errors` map, in every window and every stage, carries all six
  reasons — `deadline`, `not_found`, `operation`, `protocol`, `rpc`,
  `topology` — with a zero for the ones that did not occur, so a reader
  compares the same fields in every run.
- The key space is re-read after the load finishes, and all 100,000 keys were
  found in every run.

Two numbers in the tables above are worth naming, because a reader could
misread them:

- The `max` of a stage is the upper bound of the bucket the longest request
  fell in, not the length of that request, and the histogram's range ends at
  the operation deadline plus a second. A request longer than that is
  recorded in the top bucket and counted, so it is not lost, but the
  histogram does not measure it. The largest `max` anywhere in these runs is
  125.1 ms, in the ladder's three-times stage, and the largest of the
  campaign's own stages is 21.2 ms, against a range of 31 s: nothing was
  truncated and every `max` here is a bucket.
- The `sender_lag` of a window is the sender's lag over the phase up to that
  window's boundary, and it is what the stage-level `sender_saturated` is
  decided on. The field was added after the campaign this report replaces,
  so it is in every window event of this run, and a reader can check the
  decision instead of taking it. The per-window `behind` is a different
  number with a different meaning: it is that window's own share of the
  schedule.

## The source of the run

The run was built from a committed tree. The commit is `52adf85`, and no
tracked file differed from it: every `manifest.json` holds an empty
`git diff`, a build recorded `git_dirty: false`, and the same source tree
digest at build time and at run time,
`97aaff0f9123ef7004cfd55a394adf8e252e93c7cce380cd09914daabf3d3a38`.
Reproducing the source is therefore a checkout:

```sh
git checkout 52adf85
```

No patch is needed. The source of this run was committed, but the run was not
reported as `source_reproducible` because the script wrote its `host.txt` into
`docs/data/latency-acceptance/<run>/` before the first run started, so
`git status` of every run saw that one untracked file — 457 bytes, held in
the manifest with its content — and the `git_changed_files: 1` of
`host.txt` counts it. Nothing in the source tree differed from the commit.
The script writes the `host.txt` after the last run now, so the next run of
a clean tree reports `source_reproducible: true`.

The digest is the part of the source that does not depend on where the tree
is built. The binary's bytes do: the absolute build root is compiled into
them, so the hash in `host.txt` comes back only from a build at the same
absolute path, with the same target directory layout.

## What the acceptance asked for

| The requirement | Where it is |
| --- | --- |
| `metrics_sampled`, the topology snapshots and `summary.json` as §5.2 to §5.5 | [`docs/experiments.md`](experiments.md#the-latency-mode) |
| 10 nodes, RF 3, `OwnerOnly`, uniform and `zipf:0.99`, at least 4 rates, 3 repetitions | the tables above |
| The virtual node comparison, 16 / 64 / 128 / 256 | the table above |
| The `latency` mode documented | [`docs/experiments.md`](experiments.md#the-latency-mode) |
| The results and the limits in a document under `docs/` | this file |

## What these numbers are not

**They are not the numbers of a machine that is only doing this.** The
driver, the coordinator and the ten nodes shared 24 cores with a desktop and
with other tenants. The spread was 6.8% during this campaign, which is small
enough to compare the two distributions, and it is still a spread: the
latencies would be lower on a machine with the nodes to themselves. The load
balance is not affected by the host at all.

**Three repetitions is a small sample, and the ladder has one.** The maximum
of a campaign stage moved between 1,622 and 21,247 us while its p999 stayed
under 3.2 ms: the worst case is one or two requests out of the millions in a
stage, and on a machine shared with a desktop three repetitions cannot say
how often the host was doing something else. The ladder is a single run per
point, so its rows are one sample each; they locate the knee, they do not
measure it.

**The key space fits in memory.** 100,000 keys of 128 bytes is 12.8 MB, and
every node held its share of it. Nothing in this run touched a disk, so
nothing here says anything about what the cache does when the data does not
fit.

**The mixes and the value sizes that were not run.** The requirement also
names `50:50:0` and 4 KiB values as things to test. Neither was run here.
The mix changes what the cluster does: `90:9:1` is nine reads and one write,
and a run of writes alone is a different measurement, the one `performance`
mode already makes. The value size changes the bytes that move, and the load
here moves 128 bytes per operation.

**The policies that were not run.** Everything above is `OwnerOnly`. The
`FirstSuccessor` policy acknowledges a write at the first replica that takes
it, and its latency and its load balance are different questions.

**The delete pool.** Half of the key space — 50,000 of the 100,000 keys — is a
pool the load deletes from and nothing else touches, so the deletes never
race the writes. That is what makes the key space checkable, and it also
means the deletes were cheaper and more regular than they would be in a
workload that deletes the keys it writes. The pool is at its bound here: the
run's deletes needed more than half the key space, and the size is in the
`latency_delete_pool_keys` observation of every run.

**The client's own delay is in the numbers.** The sender waits against the
wall clock with a sleep and a short spin, so a request leaves the driver
about 20 us after it was due, and a round trip over loopback costs about
40 us. The p50 of 56 to 63 us is therefore mostly the transport, not the
cache. [`latency-sender-timing.md`](optimizations/latency-sender-timing.md)
has the measurements of the sender's own accuracy.

**The CPU numbers are of the whole process.** The `cpu_percent` of a node
counts the transport, the log, the coordination and the cache, not the cache
alone.

## Reproducing it

```sh
cargo build --release
./scripts/latency-sweep.sh
```

The script measures the capacity, takes the rates from it, runs the campaign
three times per distribution, runs the ladder and the virtual node comparison,
and writes the tables into `docs/data/latency-acceptance/`. It took 39
minutes on the host above.
