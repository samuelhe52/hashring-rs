# Worker pool sizing on a shared host

The `experiment` command starts a driver, a coordinator and every data node as
separate processes on one host. Each process built its tokio runtime through
`#[tokio::main]`, which defaults to one worker per logical CPU. On a 24-core host
that is 24 workers in each of 11 processes: **275 threads on 24 cores**.

This records what that cost, and the fix.

## Measurement

Correctness mode, 20,000 keys, concurrency 64, RF=3, `OwnerOnly`, 128-byte values,
9 data nodes on a 24-core AMD Ryzen 9 7845HX. Five runs per variant, medians, with
`HASHRING_PROFILE_WRITES=1`:

| variant | initial put | best run | keys/s | owner hold | owner wait | follower wait |
| --- | --- | --- | --- | --- | --- | --- |
| auto (host budget divided) | **0.336 s** | 0.315 s | **59,567** | **7.4 us** | **17.3 us** | **26.0 us** |
| `--worker-threads 4` | 0.391 s | 0.371 s | 51,132 | 9.6 us | 20.0 us | 31.2 us |
| `--worker-threads 24` (old default) | 0.540 s | 0.432 s | 37,030 | 12.0 us | 23.3 us | 39.1 us |

The automatic budget is **61% faster** than the old default on median initial put.
The comparison survives the 41.7% run-to-run spread recorded in
[write-path-saturation.md](write-path-saturation.md): the two variants' ranges do
not overlap, and even comparing each variant's fastest single run (0.315 s vs
0.432 s) leaves a 37% gap.

Thread counts, measured from `/proc/<pid>/task` across all 11 processes:
275 with `--worker-threads 24`, 99 with 8, **33 with the automatic default**.

## Why oversubscription was expensive here

This is not the usual "too many threads on a mutex" story. The state write lock is
only **5% busy** across this workload, and mean wait has always exceeded mean hold
(17-23 us waiting against 7-12 us holding). A lock that is idle 95% of the time
cannot produce waits two to three times its hold time through contention alone.

The cost is task wakeup. A task that finds the write lock held parks; releasing it
requires waking a task that the scheduler may place on a different worker, on a
different core, with the run queue containing hundreds of runnable threads. That
wakeup, not the critical section, is what `wait_nanos` is measuring. Reducing the
thread count shortened both halves at once -- hold fell 12.0 -> 7.4 us and wait
fell 23.3 -> 17.3 us -- which is what a scheduling-cost fix looks like, and is not
what a lock-partitioning fix looks like.

## Change

`main` now builds the runtime explicitly so the worker count can be decided, and
adds a global `--worker-threads` override.

The default is role-dependent. A standalone `node` or `coordinator` keeps tokio's
one-worker-per-CPU, because in a real deployment the process owns its machine.
The `experiment` command divides the host budget across the processes it will
start (`cpus / (peak_nodes + 2)`, clamped to at least 2), because it is the one
case that knowingly colocates them. The experiment passes that value to every
child it spawns, so all 11 processes agree.

Splitting the budget evenly beats pinning every process to 4, which the table
above shows: the driver and the nodes do not carry equal load, and 24 cores across
12 processes is not 4 apiece in practice.

## What this does not fix

The write lock still serializes all writes to one node, and its duty cycle is
unchanged in kind. This change makes each acquisition cheaper; it does not reduce
how many acquisitions a workload needs. If write throughput on a single node is
still the constraint, the ACK-state relocation described in
[state-write-lock-profile.md](state-write-lock-profile.md) remains the next lever,
now with a cheaper critical section to hide behind.

## Reproducing

```sh
cargo build --release
# automatic budget
target/release/hashring-rs experiment --mode correctness --output results/run-auto
# old behaviour, for comparison
target/release/hashring-rs --worker-threads 24 experiment --mode correctness \
  --output results/run-24
```

Thread count for a running fleet:

```sh
for p in $(pgrep -x hashring-rs); do echo "$(ls /proc/$p/task | wc -l) $p"; done | sort -rn
```
