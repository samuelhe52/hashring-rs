# Diagnosing thread wakeup and context switches

This records which counters answer which question, because "thread switching"
is two different costs and they need different instruments:

- **Context switch** — a runnable task is put off its CPU so another can run.
  Cheap when the replacement is already resident and warm; expensive when it
  comes off a cold core.
- **Wakeup latency** — a parked task is made runnable and must wait to be
  scheduled. This is what dominated the write path here, and it is not a
  context switch until it finally runs.

A profile showing high `__schedule` self time can mean either. Only the wakeup
counters below tell them apart.

## Cheapest first: /proc, no tools, no privileges

Read these on a live process while the workload is running:

```sh
grep ctxt_switches /proc/<pid>/status      # voluntary, nonvoluntary
cat /proc/<pid>/schedstat                  # field 2, field 3
awk '{print $1, $12, $13}' /proc/<pid>/stat  # se.sum_exec_runtime, nvcsw, nivcsw
```

How to read them:

| counter | meaning | what a rising value implies |
| --- | --- | --- |
| `voluntary_ctxt_switches` | task blocked (syscall, futex, park) | many voluntary switches with high `wait_nanos` means the task is parking on a contended resource |
| `nonvoluntary_ctxt_switches` | task preempted | high values with many runnable threads means the run queue is oversubscribed |
| `schedstat` field 2 | ns a runnable task spent waiting for a CPU | **the direct measure of run-queue contention**; divide by field 3 for mean latency per timeslice |
| `schedstat` field 3 | timeslices acquired | how often the task got the CPU |

Observed on this host while an experiment was in its initial load:

```
voluntary_ctxt_switches:   1346
nonvoluntary_ctxt_switches:  35
schedstat:                 1278905 1381
```

38 voluntary switches per nonvoluntary one, and 0.93 ms of run-queue wait per
timeslice. The dominant switch was the task yielding, not the scheduler
preempting it, which is the signature of blocking on a resource rather than of
too many runnable threads.

## perf: which symbols to look at

From `perf report --no-children` on a good capture, these are the self-time
symbols that constitute switch and wakeup cost. Measured shares from
[cpu-profile-write-path.md](cpu-profile-write-path.md):

| symbol | share | what it means |
| --- | --- | --- |
| `__schedule` / `schedule` | 0.90% | a task actually going off-CPU |
| `ttwu_queue_wakelist` | 0.87% | placing a woken task on a run queue; a direct wakeup counter |
| `native_smp_send_reschedule` | 0.79% | **sending an IPI to another CPU to wake someone**; the clearest evidence of a cross-core wake |
| `try_to_wake_up` | 0.28% | waking a parked task |
| `__smp_call_single_queue` | 0.37% | cross-CPU function call |
| `dequeue_entities`, `pick_task_fair`, `select_task_rq_fair` | ~1.3% | CFS picking the next task; scales with timeslices |
| `__sched_balance_update_blocked_averages`, `update_load_avg`, `update_se` | ~0.8% | scheduler rebalancing; scales with the number of runnable entities |
| `_raw_spin_lock` family | ~0.37% | contention on scheduler or run-queue locks |

`native_smp_send_reschedule` is the one to watch. A nonzero share means wakeups
are crossing cores, which is the cost that shrinks when the thread count drops.

## What NOT to count

Two symbols in a perf capture are the profiler measuring itself, not the
workload:

- `__perf_event_task_sched_in` / `__perf_event_task_sched_out` (0.21% / 0.09%
  in this capture) are perf's own scheduling hooks. They inflate switch counts
  and must be subtracted or ignored.
- `entry_SYSRETQ_unsafe_stack` and the `srso_*` thunks (1.54% + 1.25%) are
  return-address signing on every syscall return. They are mitigation overhead
  charged to any workload making syscalls, not switch cost.

Neither appears in `/proc` counters, which is a reason to read `/proc` as well.

## Counting wakeups with perf stat

`perf stat` software counters work on this host at
`kernel.perf_event_paranoid = 1` and need no privilege. Measured against a live
experiment's driver process:

```sh
target/release/hashring-rs experiment --mode correctness --output results/cs2 &
pid=$!
sleep 3
perf stat -e context-switches,cpu-migrations,task-clock -p "$pid" -- sleep 5
wait "$pid"
```

```
     405      context-switches
      57      cpu-migrations
   58.90 msec task-clock
```

The ratios matter more than the absolute counts. `cpu-migrations / context-switches`
at 14% says an eighth of all switches moved the task to a different CPU, which
is the population that shrinking the thread pool removes. `task-clock` divided by
elapsed wall time gives the CPU utilisation of that process alone.

Two ways this gets reported wrong:

- Passing both `-p <pid>` and a command to run is contradictory; perf attaches
  to the pid, ignores the command, and reports `<not counted>`. Use `-p` alone
  with `-- sleep <n>`, or a bare command with no `-p`.
- `-a` (system wide) is what actually needs privilege. Per-process counting does
  not.

## Counting wakeups exactly

`sched:sched_wakeup` and `sched:sched_switch` tracepoints give exact per-wakeup
counts rather than per-switch totals, but they are not available at
`perf_event_paranoid = 1` here:

```sh
perf trace -e sched:sched_wakeup -e sched:sched_switch -a -- sleep 10
```

They need root:

```sh
sudo sysctl -w kernel.perf_event_paranoid=0
```

Without tracepoints, `context-switches` plus `cpu-migrations` plus the `/proc`
`schedstat` fields above bracket the same ground.

## Comparing worker-pool sizes

The measurement that established the wakeup hypothesis, and the one to repeat
after any change. With `HASHRING_PROFILE_WRITES=1`, read `StateWriteTiming`
per run and compare the two halves separately:

- `hold_nanos / acquisitions` — real critical-section work
- `wait_nanos / acquisitions` — time blocked acquiring it

When a lock is ~5% busy and `wait` is 2-3x `hold`, the wait is not queueing for
the lock, it is the delay between releasing it and the waiter running. That is
the wakeup cost. Halving total threads cut both halves at once in
[worker-pool-sizing.md](worker-pool-sizing.md), which is the signature of a
scheduling problem rather than a lock-partitioning one.

## Reproducing the /proc reading

```sh
target/release/hashring-rs experiment --mode correctness --output results/cs &
pid=$!
sleep 12
grep ctxt_switches /proc/$pid/status
cat /proc/$pid/schedstat
wait $pid
```
