# Is gRPC IO the bottleneck on one host?

Question: when every process of a 6-node cluster runs on one machine, is the
data path limited by the gRPC transport (tonic, hyper, h2, the loopback
sockets), by the hashring code, or by something else that serializes it?

Short answer: **the transport.** In every configuration measured, about 90% or
more of a request's latency is spent outside the RPC handler, and 70-80% of the
on-CPU time of the nodes and the driver is in the gRPC stack and its socket
syscalls. Yet no process is CPU-bound for writes at RF=3, so the cost is per-RPC
latency, not throughput, and the lever is the number of RPCs per write, not more
connections. [replication-batch-linger.md](replication-batch-linger.md) acts on
that.

## Setup

Apple M1 Pro, 8 logical CPUs, macOS. Performance mode, 6 data nodes, 300,000
keys, 128-byte values, RF=3, `OwnerOnly`, seed 1, base commit `d9c29e3` plus
the diagnostics below. The automatic worker budget gives every node and the
driver 2 tokio workers ([worker-pool-sizing.md](worker-pool-sizing.md)), so 8
processes share 8 CPUs.

Three repetitions per configuration, medians. Background load during these runs
(Time Machine at 50-70% of a core, other desktop processes) made repeats of one
configuration differ by up to ±30%, so throughput differences below about 25%
are not resolved here. The latency breakdown was stable across every run, and
the conclusions rest on it.

Tools, all in `scripts/perf/`:

- `run.sh` runs one experiment while `cpu-sample.sh` samples each process's
  user and system CPU time every 250 ms; `cpu-window.py` turns that into cores
  used during the `PUT` and `GET` phases.
- `HASHRING_PROFILE_RPC=1` ([experiments.md](../experiments.md#diagnostics-and-profiling))
  records client-side and handler-side RPC latency histograms;
  `rpc-profile.py` summarizes them.
- [samply](https://github.com/mstange/samply) samples every thread of every
  child process without root (`samply record -s --unstable-presymbolicate
  --include-args=3 -r 1000 -- target/release/hashring-rs experiment ...`).
  `samply-buckets.py` weights samples by thread CPU time, restricts them to one
  phase, and attributes each to the innermost frame that belongs to `hashring_*`,
  `h2`, `hyper`, `tonic`, `tower`, `prost` or `redb`, with tokio, mio, std and
  libsystem treated as transparent. So a `writev` reached from h2 counts as h2.
  A sample whose leaf is a blocking wait (`kevent`, `__psynch_cvwait`) is
  excluded, since the CPU it carries was spent before the thread parked, on an
  unknown stack. macOS arm64 binaries keep frame pointers, so the
  [frame-pointer build](frame-pointer-profiling.md) is not needed here.

Raw data: [phase-a-runs.tsv](data/grpc-io-bottleneck/phase-a-runs.tsv),
[phase-b-runs.tsv](data/grpc-io-bottleneck/phase-b-runs.tsv), and the
`samply-*.txt` summaries in the same directory.

## Throughput and CPU

| configuration | PUT ops/s | GET ops/s | cores used in PUT (of 8) | driver cores in PUT | busiest node in PUT |
| --- | --- | --- | --- | --- | --- |
| concurrency 16 | 22,941 | 48,162 | 4.55 | 0.88 | 0.63 |
| concurrency 64 | 34,977 | 70,338 | 4.73 | 0.97 | 0.64 |
| concurrency 256 | 48,179 | 90,046 | 4.61 | 1.01 | 0.62 |
| RF=1, concurrency 64 | 69,904 | 68,853 | 4.16 | **1.84** | 0.43 |
| RF=1, concurrency 256 | 76,230 | 85,871 | 3.77 | 1.67 | 0.39 |
| stream queue capacity 128 | 34,261 | 67,587 | 4.72 | 0.96 | 0.64 |
| 1 KiB values | 28,522 | 64,285 | 4.56 | 0.92 | 0.62 |
| 4 workers per process | 26,384 | 61,368 | 4.18 | 0.80 | 0.58 |

- **RF=3 writes are not CPU-bound.** About 4.7 of 8 cores are busy, no node uses
  more than a third of its two workers, and throughput keeps rising with
  concurrency. That is a latency-bound closed loop. More workers made it
  slower.
- **RF=1 writes and all reads are bound by the driver.** It runs at 1.7-1.9 of
  its 2 workers in those phases.
- **Removing replication doubles write throughput.** RF=1 at the same
  concurrency is 2.0x RF=3.
- **The stream queue is not the limit.** At capacity 32 every node reports a full
  follower stream and 1,300-1,400 reservation rejections per run. At 128 the
  rejections nearly vanish (4-78) and throughput does not change.
- **Cost follows message count, not bytes.** 8x larger values cost about 19%
  of throughput.

## Where the latency goes

Means over all RPCs of a run, concurrency 64, with `HASHRING_PROFILE_RPC=1`
(phase B, medians of three):

| RPC | caller-observed | handler | outside the handler |
| --- | --- | --- | --- |
| client `Put`, RF=3 | 1,958 µs | 198 µs (p50 7 µs) | 90% |
| client `Put`, RF=1 | 893 µs | 20 µs | 98% |
| client `Get` | 758 µs | 2.2 µs | >99% |
| replication batch, owner to follower | 536 µs | 43 µs | 92% |

Everything outside the handler is the client's tonic/hyper/h2 stack, two
loopback socket hops, the server's stack, and the wait for a tokio worker at
each hop. The hashring work itself is a few microseconds at the median.

The owner `Put` handler has a long tail at RF=3 (mean 198 µs against 20 µs at
RF=1), although its measured write-lock wait is only about 24 µs per write
(`HASHRING_PROFILE_WRITES=1`, phase A). The rest is the handler's task waiting
to be scheduled again after an await, on two workers that also run the
replication traffic.

Replication batches averaged **2.2 entries**. Each owner has 5 follower
streams, each carrying about 2,300 entries/s at this load, and a stream sends
whatever queued during the previous round trip of about 0.5 ms, which is one or
two entries. So at RF=3 each client write costs about 1.9 RPCs that the nodes
must serve (its own, plus 2 follower entries / 2.2 per batch), against 1.0 at
RF=1. Since every RPC carries the transport cost above, this accounts for
the 2x gap between RF=1 and RF=3.

## Where the CPU goes

On-CPU time during the `PUT` phase, concurrency 64, RF=3
([samply-p-c64-put.txt](data/grpc-io-bottleneck/samply-p-c64-put.txt)):

| attributed to | nodes | driver |
| --- | --- | --- |
| h2 | 40.5% | 50.8% |
| hyper | 18.3% | 14.8% |
| tonic + tower + prost | 14.1% | 15.2% |
| hashring code | 21.7% | 14.3% |
| tokio runtime, no owner frame | 5.5% | 4.9% |

By leaf, `writev` alone is 20% of node and 25% of driver on-CPU time, and
`recvfrom` another 7-12%. RF=1 and concurrency 256 show the same split.

Two smaller findings:

- **h2 connection lock contention.** 12-19% of the driver's sampled CPU time
  lands in `__psynch_mutexwait`, and practically all of it is h2's per-connection
  `Streams` mutex: every in-flight request to a node shares one connection. With
  a temporary 4-connections-per-node client pool (profiled at RF=1 together with
  spawned driver tasks), that fell to 5%, but write
  and read throughput did not improve beyond run-to-run noise (PUT median 23.9k
  vs 29.7k with the pool alone; 34.2k vs 29.7k combined with spawned driver
  tasks). The pool and a spawned-task driver were therefore not kept.
- **UUID generation.** The client generates a v4 UUID per operation, and the
  resulting `getentropy` syscall is 4.5-7% of driver CPU time.

## What this rules out

- Faster hashring code: it is about a fifth of node CPU time and a few
  microseconds per request.
- More client connections: they remove lock contention but not latency.
- Bigger replication queues: they remove rejections, not RPCs.
- More worker threads: on 8 shared CPUs they make things slower.

What remains is to send fewer RPCs per write, and replication batches are where
there is room: they average about 2 entries when they could hold 64.
