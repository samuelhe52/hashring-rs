# Frame-pointer profiling: why the flame graph was empty

## What was wrong

`scripts/flamegraph.sh` captured `perf record --call-graph dwarf,8192` against
`target/release/hashring-rs` and rendered the result. The graph looked plausible
— thousands of folded stacks, 46-58 frames deep — and was unreadable as
evidence about this project.

A performance-mode capture on a 24-core AMD Ryzen 9 7845HX, 20 nodes, RF=3,
`FirstSuccessor`, 1M keys, 102.8 s, 57,268 samples, **0 lost**:

| samples with N project frames | share |
| --- | --- |
| 0 | 91.5% |
| 1 | 2.3% |
| 2 | 1.8% |
| 3+ | 0.1% |

The graph was `comm` -> `libc` -> kernel. The heaviest single folded stack:

```
tokio-rt-worker
writev
syscall_cancel
internal_syscall_cancel
[libc.so.6]
entry_SYSCALL_64_after_hwframe
do_syscall_64
...
__ip_local_out
nf_hook_slow
[[nf_conntrack]]
```

Unwinding stopped exactly at the libc boundary. The kernel part still unwound
fine, which is what made the failure easy to miss: the SVG had real depth and no
project code in it.

## Why

Two assumptions, one of them wrong.

**"Rust omits frame pointers, so `--call-graph fp` truncates; keep DWARF."**
This was in the script header and is backwards on this host. A default
`rustc -O` build yields one-frame stacks under *both* methods.

**"The binary has unwind tables, so DWARF can walk it."** It does, and it still
does not work: `.eh_frame` is 1,070,560 bytes for a 10,220,433-byte `.text`, and
a 30-line repro has 551 FDEs for 578 functions. The coverage is fine; perf's
DWARF unwinder does not use it here.

## Measurement

A 30-line busy loop, `rustc 1.93.0 (254b59607 2026-01-19)`, one core, 99 Hz.
The metric is the share of samples carrying at least two Rust frames, which is
what a caller needs to attribute CPU to a code path:

| build | `--call-graph fp` | `--call-graph dwarf,8192` |
| --- | --- | --- |
| `rustc -O` (default) | **0.0%** | **1.1%** |
| `-C force-frame-pointers=yes` | **94.5%** | **93.8%** |
| `-C force-unwind-tables=yes` | n/a | **0.0%** |

Three facts follow.

- **Frame pointers are the only thing that works.** They fix both call-graph
  modes. DWARF is not a workaround for their absence; it fails the same way.
- **`force-unwind-tables` is not the answer.** It changes nothing measurable
  here and `.eh_frame` is byte-identical to the default build.
- **`fp` is then strictly better than `dwarf`.** Once frames are on the stack,
  the frame-pointer walk is cheaper and sidesteps the ring-buffer limit below.

Raw artifacts: `/tmp/opencode/fptest/` on this host (not kept; rerun the table
to reproduce).

## Tradeoffs

- **Frame pointers cost throughput.** RBP becomes a callee-saved register.
  Measurements here are single-digit percent and workload-dependent, so treat
  the number as unmeasured: measure with `target/release`, profile with
  `target/fp`, and never ship the profiling build.
- **A separate build costs disk and a cold rebuild.** `target/fp` is a full
  dependency tree. `scripts/flamegraph.sh` builds it on first use and leaves
  `target/release` alone.
- **FP unwinding is unsound through hand-written assembly and some libc
  frames.** Those frames resolve wrong or stop the walk. That is the same
  failure the old capture had, at a much smaller scale, and it is why the script
  now reports the share of samples that reached project code instead of relying
  on the graph looking reasonable.
- **`fp` records 16 bytes per frame**, so perf's per-CPU ring buffer
  (`perf_event_mlock_kb`, 516 KB shared over 24 CPUs, ~22 KB each) stops being
  the constraint. The DWARF path it replaces lost 12.33% of samples at a
  64000-byte stack and 0.4% at 8192, for the same run length.

## Result

`scripts/flamegraph.sh` now builds `target/fp/release/hashring-rs` with
`-C force-frame-pointers=yes`, records with `--call-graph fp`, and prints the
share of folded weight carrying a project frame. A capture taken that way:

- user-side samples: 99.0% carry at least two project frames, 8-33 frames deep;
- 29.6% of all samples reach project code, the rest being kernel-side IPs, for
  which perf records no user stack at all;
- `perf.data` shrank from 500 MB to 952 KB for a comparable run.

The script warns and exits non-zero-worthy noise above stderr when that share
drops below 5%, which is where a truncated capture lands (1.8%) and where a
working one does not.

## What the profile then said

The 20261014 performance capture, read as symbol self time since its stacks were
unusable, is in [cpu-profile-write-path.md](cpu-profile-write-path.md) for the
correctness profile. Its shape: 52.6% `hashring-rs`, 10.3% libc, 35.3% kernel,
no symbol above 1.63%. On the performance profile the dominant cost is the
transport, not the store — HTTP/2, hyper and tonic 26.68%, loopback TCP 9.13%,
netfilter 1.37%, and redb at 0.01%. A put costs 2.32 Mcycles against 0.35 Mcycles
for a get, and the nodes hold 83% of it while the coordinator holds about 2%.

## Reproducing

```sh
./scripts/flamegraph.sh performance
```

Read the printed `N% of samples carry a hashring frame` line before reading the
graph. Below ~5%, the capture is unusable and `perf report --no-children` on
`perf.data` is the only honest readout.
