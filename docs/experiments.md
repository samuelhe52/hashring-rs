# Experiments

`hashring-rs experiment` launches a coordinator and ordinary data-node child
processes, drives them through the client library, and records the results.
For quick-start commands, see the [README](../README.md#experiments).

## Modes and defaults

| Mode | What it does | Default keys | Default nodes |
| --- | --- | --- | --- |
| `correctness` | Starts with 9 nodes and scales to 10 under concurrent rewrites and deletes, keeping a third moving cohort as snapshot sentinels. It then scales back to 9 while restoring or deleting the mutation cohorts, and verifies present and absent keys after every stage. | 20,000 | 10 (peak) |
| `performance` | Nominal acceptance profile | 1,000,000 | 10 |
| `availability` | Failure and repair: 3 live nodes plus 1 replacement | 1,000 | 4 |

Every mode defaults to 128-byte values, concurrency 64, seed 1, 128 virtual
nodes, and a 30-second operation deadline. The default ACK policy is
`FirstSuccessor` for availability mode and `OwnerOnly` for the others. Run
`hashring-rs experiment --help` for every flag.

`experiment --config <file>` reads the same settings from YAML; see
[`examples/experiment.yaml`](../examples/experiment.yaml). Keys are the flag
names with `_` for `-` and take the flag values. A flag on the command line wins
over the file, except that `--verbose` and `--require-clean-source` can only
turn a setting on. Unknown keys are an error, and a relative `output` in the
file is resolved against the file's directory. The manifest records the resolved
configuration, so a run is reproducible without the file.

All processes run on one host, so the driver and each data node get an equal
share of the CPUs: `cpus / (peak_nodes + 2)` tokio workers, at least 2. Override
it with `experiment --worker-threads N`. The coordinator keeps one worker per
CPU, since it is not on the data path. See
[worker-pool-sizing.md](optimizations/worker-pool-sizing.md) for the evidence.

## Output

The output directory must be new or empty. The runner never overwrites a
non-empty one. It writes:

- `manifest.json`: the actual configuration, the build-time revision, toolchain,
  and profile, the runtime source status and diff, and a BLAKE3 digest of the
  executable
- `events.jsonl`: the event log
- stdout and stderr for each process
- `coordinator.redb`: the coordinator database
- `summary.json`: the results

`results/` is ignored by Git, so raw databases and logs stay local.

### Reproducibility

Formal runs use `--require-clean-source`, which refuses to run unless the build
and the runtime source match the same clean commit. Runs from a dirty tree are
allowed but are marked `source_reproducible=false` in the manifest.

## Summary contents

All modes:

- initial-put throughput
- sampled latency distributions in microseconds (p50, p95, p99, max) for
  end-to-end client `PUT` and for direct owner-RPC `PUT`. The direct figure
  includes the network hop and owner processing, not only CPU time.
- `node_pressure_after_initial_put`, recorded per node:
  - current and peak deduplication receipt store bytes, and its capacity
  - owner and follower budget rejections
  - replication reservation rejections
  - replication RPC retries
  - peak pending entries in one replication stream, not counting the RPC in
    flight

  The same sample is also logged as a `node_pressure_sampled` event, even when
  the initial writes fail. Counters are per process and reset when a node
  restarts.

Correctness mode also records the time to full RF, measured from the start of
each transition.

Availability mode also records:

- failover publication time and first recovered read
- sampled read-unavailability probes
- acknowledged keys preserved or lost, including a just-acknowledged tail write
  of at least 1 MiB
- time from replacement to full RF

All of these are local process measurements, not service-level guarantees.

If a concurrent writer fails, the runner records the error and waits for the
migration outcome before it writes the summary.

## Diagnostics and profiling

`--verbose` prints client retry reasons to the experiment's stderr and adds
coordinator phase and replica-seeding progress to the process logs. It is
recorded in the manifest. To keep a complete diagnostic record, capture stderr
separately.

```sh
target/release/hashring-rs experiment \
  --mode correctness --verbose --operation-timeout-ms 120000 \
  --output results/correctness-diagnostic
```

A longer deadline is only for diagnosis. It does not show that the default
30-second deadline is met.

Set `HASHRING_PROFILE_WRITES=1` to add the following to the node pressure
sample:

- per-node receipt-cleanup timings
- state write-lock wait and hold times for owner mutations, follower
  application, and replication ACK processing

These are cumulative wall-clock counters. Summed wait times can overlap across
tasks and nodes. Without profiling, the timing fields are `null`.

When the owner's receipt store is full, the node returns a retry-delay hint.
The client honors it within the same logical-operation deadline.

## Comparing runs

- **Replication overhead.** Compare RF=1 with RF=3, using `OwnerOnly`,
  `--minimum-admitted-copies 1`, and `--minimum-healthy-followers 0` in both
  runs.
- **ACK-wait overhead.** Compare policies at the same RF.
- **`OwnerOnly` loss window.** Run availability mode with
  `--policy owner-only`. One run with zero observed loss does not show that the
  window is closed.
