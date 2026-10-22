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
| Run | `latency-acceptance-20261022-022215`, 39 minutes |
| Binary | `5c864840`, commit `079b218`, 21 changed files |

The hash above is the hash of the binary that ran, recorded by the run
itself in [`host.txt`](data/latency-acceptance/host.txt) and in the
`manifest.json` of every run. The build is deterministic at a fixed path —
two `cargo build --release` runs of the same source at the same path give
the same bytes — and the source the run was built from is in
[`source.patch`](data/latency-acceptance/source.patch), which reproduces the
source tree digest the run recorded. The digest comes back from any path; the
binary's hash does not, because the absolute build root is compiled into the
binary. The tree has changed since: a doc comment and a clippy attribute
were added to `latency.rs`, and the mode was changed afterwards to fix what
the review of this report found.

The target rates are a share of the capacity the cluster showed in
`performance` mode, which measured **80,615 operations per second** for the
write path on the same host with the same configuration. The rates are 0.25,
0.5, 0.75 and 0.9 of that: **20,153**, **40,307**, **60,461** and **72,553**
operations per second.

A run past the capacity was added, because the capacity is a *write* capacity
and the campaign is mostly reads. The ladder offers 1, 1.5, 2 and 3 times the
capacity and shows where the mode gives up.

The host is a shared machine, so every table carries the speed of the host the
run landed on: the preload writes the same 100,000 keys with the same 64
tasks before the load starts, and it is the run's own measure of how fast the
machine was. During this campaign that speed was **80,749 to 84,775
operations per second, a spread of 5%** — the campaign ran between two and
three in the morning, when the machine had little else to do. That is what
makes the comparison between the two distributions below worth reading; an
earlier campaign on a busier machine moved by a factor of 1.8 and could not
support it.

## The latency of a load offered at a rate

Every repetition, with the p50, p95, p99 and p999 of the `GET` latency in
microseconds, and the speed of the host that run landed on.

| Run | Distribution | Rate | Host | p50 | p95 | p99 | p999 | max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| uniform-rep1 | uniform | 20,153 | 84,775 | 57 us | 78 us | 94 us | 197 us | 11,047 us |
| uniform-rep1 | uniform | 40,307 | 84,775 | 58 us | 82 us | 107 us | 211 us | 1,663 us |
| uniform-rep1 | uniform | 60,461 | 84,775 | 58 us | 92 us | 154 us | 476 us | 21,711 us |
| uniform-rep1 | uniform | 72,553 | 84,775 | 62 us | 114 us | 365 us | 1,247 us | 3,775 us |
| uniform-rep2 | uniform | 20,153 | 84,160 | 57 us | 77 us | 93 us | 220 us | 10,719 us |
| uniform-rep2 | uniform | 40,307 | 84,160 | 58 us | 81 us | 105 us | 185 us | 1,456 us |
| uniform-rep2 | uniform | 60,461 | 84,160 | 59 us | 96 us | 180 us | 655 us | 21,791 us |
| uniform-rep2 | uniform | 72,553 | 84,160 | 63 us | 122 us | 429 us | 1,788 us | 8,359 us |
| uniform-rep3 | uniform | 20,153 | 84,430 | 57 us | 77 us | 93 us | 191 us | 10,671 us |
| uniform-rep3 | uniform | 40,307 | 84,430 | 58 us | 82 us | 107 us | 208 us | 3,689 us |
| uniform-rep3 | uniform | 60,461 | 84,430 | 58 us | 94 us | 166 us | 652 us | 20,239 us |
| uniform-rep3 | uniform | 72,553 | 84,430 | 63 us | 124 us | 434 us | 1,525 us | 5,403 us |
| zipf-0-99-rep1 | zipf:0.99 | 20,153 | 80,748 | 57 us | 79 us | 95 us | 216 us | 10,463 us |
| zipf-0-99-rep1 | zipf:0.99 | 40,307 | 80,748 | 57 us | 79 us | 101 us | 185 us | 8,271 us |
| zipf-0-99-rep1 | zipf:0.99 | 60,461 | 80,748 | 58 us | 92 us | 156 us | 482 us | 19,887 us |
| zipf-0-99-rep1 | zipf:0.99 | 72,553 | 80,748 | 62 us | 112 us | 293 us | 1,050 us | 19,087 us |
| zipf-0-99-rep2 | zipf:0.99 | 20,153 | 83,268 | 56 us | 78 us | 94 us | 201 us | 10,375 us |
| zipf-0-99-rep2 | zipf:0.99 | 40,307 | 83,268 | 58 us | 82 us | 106 us | 198 us | 8,759 us |
| zipf-0-99-rep2 | zipf:0.99 | 60,461 | 83,268 | 58 us | 95 us | 165 us | 506 us | 20,655 us |
| zipf-0-99-rep2 | zipf:0.99 | 72,553 | 83,268 | 62 us | 115 us | 383 us | 1,627 us | 19,423 us |
| zipf-0-99-rep3 | zipf:0.99 | 20,153 | 84,024 | 56 us | 77 us | 93 us | 195 us | 10,303 us |
| zipf-0-99-rep3 | zipf:0.99 | 40,307 | 84,024 | 57 us | 79 us | 101 us | 178 us | 8,567 us |
| zipf-0-99-rep3 | zipf:0.99 | 60,461 | 84,024 | 58 us | 92 us | 155 us | 505 us | 20,255 us |
| zipf-0-99-rep3 | zipf:0.99 | 72,553 | 84,024 | 62 us | 114 us | 346 us | 1,234 us | 19,759 us |

**The achieved rate was the target rate in every one of these 24 stages.** Not
one request was dropped for want of an in-flight slot, not one failed, the
client never had to try a request twice, and no window was marked as one the
sender did not keep up with. Every number in this table describes the cluster
and not the driver.

The retry counter of that run counted the retries of the requests. The
retries of a topology refresh were not counted by it — the counter was
widened after this campaign — so "never had to try a request twice" is what
the counter of this run measured, and the refreshes it made are not in it.
The client polls the coordinator for the topology every five seconds, so it
did make refreshes during the run; none of them is in the counter, and none
of them is in the `errors` of any window either.

**The median does not move.** The p50 is between 56 and 63 us across a factor
of 3.6 of load. A load generator that reported only a median would have shown
a flat line and called it a stable cluster.

**The tail moves, and the top of the campaign is where it starts.** The p99
holds at 93 to 107 us through half the capacity, reaches 154 to 180 us at
0.75, and is at 293 to 434 us at 0.9. The p999 stays under 1.8 ms
throughout. The maximum does not behave: the stage whose p999 was 655 us
still had a request that waited 21,791 us, which is the tail of a shared
machine rather than a queue — a queue shows up in the p999 as well.

**The two distributions cost the same latency.** The second repetition of
each is the matched pair, at 84,160 and 83,269 operations per second of
preload, within 1.1% of each other. Under `zipf:0.99` the p50 was 56 to 63 us
against the uniform run's 57 to 63, and the p99 was 94 to 383 us against 93
to 429. An earlier campaign, run on a machine that moved by a factor of 1.8
during it, suggested the skewed load was faster; at this host speed the two
are the same within the noise of the machine.

**What the skew does cost is the busiest node.** See the next table: the same
top rate puts the busiest node at 35.1% of a core under `uniform` and 47.1%
under `zipf:0.99`. The cluster has the headroom for it, which is why the
latency does not show it.

## The load balance

The two layers of the load balance. These numbers do not depend on the host:
the key space is divided by the ring, and the requests land on the keys. The
table is the median over the three repetitions, whose individual numbers are
in the data directory: the key space columns are identical in all three,
because the ring is built the same way every time, and the operations column
moves by at most 0.008 between them.

| Distribution | Rate | Owned keys, max over mean | Owned keys, cv | Operations, max over mean | Operations, cv | Busiest node's CPU |
| --- | --- | --- | --- | --- | --- | --- |
| uniform | 20,153 | 1.116 | 0.088 | 1.142 | 0.089 | 10.2% |
| uniform | 40,307 | 1.116 | 0.088 | 1.145 | 0.089 | 19.8% |
| uniform | 60,461 | 1.116 | 0.088 | 1.145 | 0.089 | 29.1% |
| uniform | 72,553 | 1.116 | 0.088 | 1.142 | 0.089 | 35.1% |
| zipf:0.99 | 20,153 | 1.116 | 0.088 | 1.863 | 0.339 | 14.0% |
| zipf:0.99 | 40,307 | 1.116 | 0.088 | 1.859 | 0.338 | 26.3% |
| zipf:0.99 | 60,461 | 1.116 | 0.088 | 1.860 | 0.338 | 39.1% |
| zipf:0.99 | 72,553 | 1.116 | 0.088 | 1.860 | 0.338 | 47.1% |

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
agrees with to within 0.006. The key space is what the ring divides, so this
is the measurement of how evenly 100,000 keys fall on 10 nodes.

| Virtual nodes | Owned keys, max over mean | Owned keys, cv | Operations, max over mean | Operations, cv |
| --- | --- | --- | --- | --- |
| 16 | 1.200 | 0.177 | 1.204 | 0.182 |
| 64 | 1.255 | 0.139 | 1.240 | 0.135 |
| 128 | 1.116 | 0.088 | 1.145 | 0.088 |
| 256 | 1.087 | 0.048 | 1.065 | 0.048 |

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
1.07 to 1.25 times the mean with a coefficient of variation of 0.048 to 0.182
at every virtual node count, because it is the key space that decides where a
request goes.

## Where the mode gives up

The capacity run measures the write path, so the campaign's top rate is well
below the knee of the read-heavy load. The ladder offers more than the
capacity, in one run so that the host's speed is the same for every row. It
ran at 82,419 operations per second of preload.

| Target rate | Achieved | p50 | p95 | p99 | p999 | Late | Sender saturated |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 80,615 (1x) | 80,615 | 64 us | 104 us | 359 us | 1,913 us | 0 | no |
| 120,922 (1.5x) | 120,922 | 656 us | 2,361 us | 3,437 us | 5,079 us | 0 | no |
| 161,230 (2x) | 100,590 | 39,007 us | 73,023 us | 87,103 us | 102,207 us | 606,396 | no |
| 241,845 (3x) | 100,606 | 39,103 us | 74,047 us | 88,383 us | 103,423 us | 1,412,393 | no |

**The knee is between 0.9 and 1.5 times the write capacity.** The campaign's
top rate, 0.9 of the capacity, still has a p50 of 62 to 63 us and a p99 of
365 to 434 us. At 1.5 times the capacity the median has grown tenfold to
656 us and the p99 is 3.4 ms, though nothing is dropped. Past that the mode
breaks down, and it breaks down in the way it was built to break down:

- The requests that could not be sent were **dropped**, 606,396 of them at 2x
  and 1,412,393 at 3x, not queued.
- The p50 of 39 ms is the queueing delay inside the in-flight window. 4,096
  requests in flight at the 100,590 operations per second the cluster was
  doing is 40.7 ms of waiting, and the measured median is 39.0 ms: the
  window, not the cluster, is what sets the throughput above the knee, which
  is why the last two rows report the same achieved rate. A larger
  `--max-in-flight` would raise it and lengthen the queue all the same.
- The sender was **not** the problem. `sender_saturated` is false in every
  window of every run, and the per-window `behind` count never went above 223
  requests out of the 241,845 the schedule called for in that second, 0.1% of
  it. The mode says the cluster could not keep up, and it says which of the
  two it was.
- Not one request **failed** and not one **retried**, anywhere in the
  campaign: 93,025,331 operations came back with zero `deadline`, zero
  `rpc`, zero `operation`, zero `topology`, zero `protocol` and zero
  `not_found`, even at three times past the capacity. What the cluster could
  not take was never sent to it.

## How the numbers were checked

A stage that does not account for every request it issued is not a
measurement, so the mode does not report one. Before a stage's numbers are
written, the run checks that

```
ops + errors == issued        (issued excludes the requests the in-flight limit dropped)
```

and it fails with an error rather than publish a rate that is quietly short.
All 36 stages of this campaign pass it, and the 24 campaign stages also
report an achieved rate equal to their target.

The check earned its place. It found two bugs in the measurement while this
report was being written, both of which would have gone unnoticed:

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
  histogram does not measure it. The largest `max` of this campaign is
  21.8 ms against a range of 31 s, so nothing was truncated and every `max`
  here is a bucket.
- The `behind` of a window is that window's lag, and it is not the number
  that decides `sender_saturated`: that is the sender's lag over the phase,
  which the event log calls `sender_lag`. The field was added to the event
  after this campaign, so the events of this run hold only `behind`. The
  stage-level `sender_saturated` of every stage here is the same decision,
  taken on the sender's lag.

## The source of the run

The run was built from a tree that was never committed: 21 files were
changed, and the commit it was built on was `079b218`. The `manifest.json` of
every run holds the diff of the files that were tracked and changed, which is
16 of the 21. The other five paths were not in the index, and `git diff` does
not hold a file that was never added to it, so the manifest of this campaign
does not have their content: the mode itself,
`crates/hashring-experiment/src/latency.rs`, was one of them.

[`docs/data/latency-acceptance/source.patch`](data/latency-acceptance/source.patch)
is the whole change, and applying it to `079b218` reproduces the tree the run
was built from. The run recorded the digest of that tree in every manifest,
`52875185bd69bc52c9a8460da2e94650c134b4de57155fcb5e825475d5daf817`, and the
patched tree has that digest. The patch holds the tracked diff exactly as
the manifest recorded it, and the three of the five that existed when the
run started as new files: the mode, the script that ran it, and the
measurement of the sender's own timing. The report and its tables were
written after the run and are not in it.

The manifest now keeps the files that were not in the index with their
content, so a run of an uncommitted tree carries its own source. That change
came after this campaign, which is why this one needed the patch.

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
with other tenants. The spread was 5% during this campaign, which is small
enough to compare the two distributions, and it is still a spread: the
latencies would be lower on a machine with the nodes to themselves. The load
balance is not affected by the host at all.

**Three repetitions is a small sample, and the ladder has one.** The maximum
of a stage moved between 1,456 and 21,791 us while its p999 stayed under
1.8 ms: the worst case is one or two requests out of the millions in a
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
