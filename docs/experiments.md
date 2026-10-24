# Experiments

`hashring-rs experiment` launches a coordinator and ordinary data-node child
processes, drives them through the client library, and records the results.
For quick-start commands, see the [README](../README.md#experiments).

## Modes and defaults

| Mode | What it does | Default keys | Default nodes |
| --- | --- | --- | --- |
| `correctness` | Starts with 9 nodes and scales to 10 under concurrent rewrites and deletes, keeping a third moving cohort as snapshot sentinels. It then scales back to 9 while restoring or deleting the mutation cohorts, and verifies present and absent keys after every stage. | 20,000 | 10 (peak) |
| `performance` | Nominal acceptance profile | 1,000,000 | 10 |
| `availability` | Failure and repair: 3 live nodes plus 1 replacement, or a run of consecutive failures with `--availability-rounds` | 1,000 | 4 |
| `latency` | Open-loop load at a series of target rates, and the latency of that load. See [the latency mode](#the-latency-mode). | 100,000 | 10 |
| `scaling` | One continuous mixed read/write load across 9→10→9 nodes (or a grow/shrink only), with per-cohort verification. See [the scaling mode](#the-scaling-mode). | 100,000 | 10 |

Every mode defaults to 128-byte values, concurrency 64, seed 1, 128 virtual
nodes, and a 30-second operation deadline. The default ACK policy is
`FirstSuccessor` for availability mode and `OwnerOnly` for the others. Run
`hashring-rs experiment --help` for every flag.

`experiment --config <file>` reads the same settings from YAML; see
[`examples/experiment.yaml`](../examples/experiment.yaml) and every setting's
default and scope in [configuration.md](configuration.md). The node tuning
settings, such as `--max-dedup-bytes`, are passed to every node of the run. Keys are the flag
names with `_` for `-` and take the flag values. A flag on the command line wins
over the file, except that `--verbose` and `--require-clean-source` can only
turn a setting on. Unknown keys are an error, and a relative `output` in the
file is resolved against the file's directory. The manifest records the resolved
configuration, so a run is reproducible without the file.

All processes run on one host, so the driver and each data node get an equal
share of the CPUs: `cpus / (peak_nodes + 2)` tokio workers, at least 2. Override
it with `experiment --worker-threads N`. The coordinator keeps one worker per
CPU, since it is not on the data path (`--coordinator-worker-threads N`
overrides it). See
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

The manifest's `runtime_source` holds the source the run was built from: the
commit, the status of the working tree, the diff of the files that were
changed, and the files that were never added to the index. The last part is
there because `git diff` does not hold them, and a run of a tree that was
never committed is only reproducible if the tree itself is kept. For each of
those files the manifest holds the path, the size, the digest, and the
content of the file when it is text and small enough to keep. A file that
could not be read is held with the error and without a digest, so a file that
is not there is never recorded as an empty one.

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
  includes the network hop and owner processing, not only CPU time. Both are
  measured on an **idle** cluster, after the workload has finished, so they
  are a baseline and not the latency under load. The `latency` mode measures
  the other one.
  - `idle_client_put_latency_us` and `idle_owner_rpc_put_latency_us` are the
    names of these two figures. They were `client_put_latency_us` and
    `owner_rpc_put_latency_us` before the rename that says what the sample
    is; the older results still use the old names.
- `node_pressure_after_initial_put`, recorded per node:
  - current and peak deduplication receipt store bytes, and its capacity
  - owner and follower budget rejections
  - replication reservation rejections
  - replication RPC retries
  - peak pending entries in one replication stream, not counting the RPC in
    flight
  - the operations the node served as the owner of a key, by kind, and their
    sum

  The same sample is also logged as a `node_pressure_sampled` event, even when
  the initial writes fail. Counters are per process and reset when a node
  restarts.

Correctness mode also records the time to full RF, measured from the start of
each transition.

Availability mode also records:

- failover publication time and first recovered read
- sampled read-unavailability probes
- acknowledged keys preserved or lost, including a just-acknowledged tail write
  of `value_bytes`, or of the smaller of 1 MiB and the node's value limit when
  `value_bytes` is below 1 MiB, never larger than the value limit the data
  nodes were given
- time from replacement to full RF

### The availability rounds

`--availability-rounds N` injects N consecutive owner failures. Every round

1. picks the current owner of the probe key, which is the first key the first
   member owns, and samples up to 128 keys that member owns,
2. writes an identifiable value to the last sampled key, which is the last one
   that is not the previous round's tail key so that this round's write cannot
   overwrite the value the previous-tail check looks for, and kills the owner,
3. waits for the automatic failover to publish a topology without it and for the
   read probe to succeed again,
4. writes a key outside the sampled key space and reads it back, which measures
   when the cluster serves writes again,
5. reads the sampled keys back, and the value the previous round wrote,
6. joins a spare node, which is seeded like a scale-out node, and waits for the
   full replication factor,
7. records how evenly the live members, and the members that never failed, hold
   the keys.

The probe key's owner changes with every failover, so every round kills a node
that has not failed yet. The run needs one spare node per round: the node
count is the initial members plus the spares, so `--availability-rounds 3`
alone starts with 3 members and 3 spares.

The per-round records are in `availability_rounds`, one object per round. Each
one holds the victim and its epoch, the time from the kill to the publication
and to the first successful read, the time the first write after the failure
took, the longest streak of failed read probes, the acknowledged keys that
survived, whether the previous round's value survived, the time from the
replacement to the full replication factor, the keys every live member owns,
and the spread of those keys (`max_over_min`, `range_over_mean`,
`max_over_mean_minus_one`, and `cv`).

Two fields are `null` rather than a number when the run cannot name one:
`previous_tail_survived` when the round had to reuse the previous round's tail
key, so its own write replaced the value that check looks for, and
`max_over_min` when a member holds no keys, which makes the ratio unbounded.
`tail_bytes` is the size of the value the round wrote: `value_bytes` when that
is larger than 1 MiB, and otherwise the smaller of 1 MiB and the node's value
limit.

The run's own figures describe its worst round: `failover_seconds` and
`read_recovery_seconds` are the slowest round, `observed_unavailable_max_streak_ms`
the longest streak of any round, and `unavailable_probe_count` the total of all
rounds. `acknowledged_key_survival`, `replacement_to_full_rf_seconds`, and
`failure_to_full_rf_seconds` describe the first round.

`FirstSuccessor` guarantees that an acknowledged write survives the failure of
one member. A run of failures is a stronger claim than that guarantee, so the
runner only fails the run when the *first* round loses an acknowledged key.
Later rounds report what they lost in `availability_acknowledged_lost_total`
and in each round's own record.

The `latency` mode records one measurement per target rate, and its own
observations. See [the latency mode](#the-latency-mode).

All of these are local process measurements, not service-level guarantees.

If a concurrent writer fails, the runner records the error and waits for the
migration outcome before it writes the summary.

## The latency mode

`performance` mode is a closed-loop benchmark: 64 tasks write as fast as they
can, so a slow cluster makes the clients send less, and the latency it
reports comes from 128 serial `PUT`s on an idle cluster afterwards. The
`latency` mode measures the latency of a load that is offered at a fixed rate,
which is the number a service-level objective is written against.

[`latency-acceptance.md`](latency-acceptance.md) is the run this mode was
accepted with: what it measured, what the numbers were, and what they are not.

### The open loop

Requests are due at fixed offsets from the start of a stage: request `n` is
due at `stage start + n / rate`. The sender does not wait for a response
before it sends the next one, and a request that is late is not delayed
further to make room for a later one. Two things follow from that:

- A request's latency is measured from the moment it was **due**, not from the
  moment it was sent. A request that waited in the client counts the waiting,
  which is what keeps a slow cluster from hiding its own tail.
- A request that cannot be sent because `--max-in-flight` requests are
  already in flight is **dropped** and counted as `late`. It is not queued.
  A cluster that cannot keep up therefore shows a lower achieved rate rather
  than a longer queue inside the driver, and the offered rate stays the rate
  the run asked for.

The sender runs on its own thread and waits against the wall clock, with a
sleep and a short spin, because a runtime timer wakes up about a millisecond
late. That is longer than the latencies this mode measures, so a runtime
timer would put a millisecond of the sender's own delay into every request.
[latency-sender-timing.md](optimizations/latency-sender-timing.md) has the
measurements.

The sender is not allowed to be the bottleneck. Every second the run records
how many requests the schedule called for and how many the sender had sent;
if the sender is behind by more than a twentieth of a second's worth of
requests, the stage is marked `sender_saturated` and its numbers do not
describe the cluster.

### Parameters

| Flag | Meaning | Default |
| --- | --- | --- |
| `--preload-keys` | The keys written before the load starts, and the key space the load reads and writes. `--keys` names the same count. | 100,000 |
| `--target-rates` | The target rates in operations per second. Each rate is one stage of the run. | none, so the mode needs at least one |
| `--warmup-seconds` | The seconds at the start of every stage that are not counted. | 10 |
| `--duration-seconds` | The seconds of every stage that are counted. | 60 |
| `--mix` | The `GET:PUT:DELETE` share of the requests. | `90:9:1` |
| `--key-distribution` | `uniform`, `zipf:<theta>`, or `hotspot:<key share>:<traffic share>`. | `zipf:0.99` |
| `--max-in-flight` | How many requests may be in flight at once. | 4096 |

The value size is the run's `--value-bytes`, and every other setting is the
run's own: node count, replication factor, ACK policy, hash seed, virtual
nodes, and the operation deadline.

The target rates are the input, not the output: measure the capacity with
`performance` mode first, then offer a share of it. A stage that cannot reach
its target rate says so.

### What a stage reports

`summary.json` holds one `measurements` entry per target rate, named
`rate_0`, `rate_1`, and so on:

```jsonc
"rate_2": {
  "target_rate": 20000, "achieved_rate": 19998, "seconds": 60,
  "sender_saturated": false,   // the sender did not keep up with its schedule
  "issued": 1199880,           // the requests the sender sent
  "late": 120,                 // the requests it had to drop
  "ops": { "get": 1079892, "put": 107989, "delete": 11999 },
  "errors": { "deadline": 0, "not_found": 0, "operation": 0,
              "protocol": 0, "rpc": 0, "topology": 0 },
  "retries": 120,               // the times the client had to try again
  "latency_us": {
    "get": { "p50": 180, "p95": 600, "p99": 2100, "p999": 8000,
             "max": 21000,        // the upper bound of the largest bucket
             "max_observed": 20980, // the longest request itself
             "over_range": 0,     // the requests longer than the histogram
             "samples": 1079892 },
    "put": { ... }, "delete": { ... }
  },
  "load_balance": {
    "per_node": {
      "node-1": { "owned_keys": 10120, "ops_handled": 120400, "cpu_percent": 61.0 }
    },
    "owned_keys_max_over_mean": 1.08, "owned_keys_cv": 0.04,
    "ops_handled_max_over_mean": 1.31, "ops_handled_cv": 0.12
  }
}
```

The `errors` map counts failures by reason. `deadline` is a request that ran
out of its operation deadline, `operation` is a request the cluster refused,
`rpc` is a request whose transport failed, `topology` is a request the
client could not route, and `protocol` is a response the client could not
read. `not_found` is a read of a key that is not there, which is an outcome
of the workload and not a failure of the cluster. The map holds all six
reasons in every window and every stage, with a zero for the ones that did
not occur, so a reader finds the same keys in every run.

The `ops` map counts the operations that **succeeded**, and the `errors` map
counts the ones that did not. The latency histogram holds every request that
came back, so its `samples` is the sum of the two, and a request that failed
is in the histogram with the latency it took to fail.

The histogram's range ends at the operation deadline plus a second, because a
request cannot take longer than its deadline. A request longer than that is
recorded in the top bucket and counted in `over_range`, so it is not lost, but
the histogram does not measure it: `max` is the upper bound of that bucket
rather than the length of the slowest request. `max_observed` is the longest
request itself, so a reader can tell a request that was truncated from one
that was near the bound.

`issued` and `late` do not overlap: a request the in-flight limit dropped was
never sent, so `ops` and `errors` add up to `issued`. A stage that does not
add up is a bug in the measurement, so the run fails with an error rather
than report a rate that is quietly short.

A request that fails after the client has tried it again is counted under the
reason it failed with, and the tries themselves are in `retries`. A retry is
any attempt the client made after a failure it could try again: the retry of a
request, and the retry of a topology refresh. There is no separate "the
retries ran out" reason, because a client that gives up fails with a
deadline or with a transport error, and counting it twice would make the map
harder to read.

The load balance has two layers. `owned_keys` is how the key space was
divided: the number of keys each node owns, counted over the key space
itself. `ops_handled` and `cpu_percent` are how the requests were divided:
the operations each node served while the stage ran, and the share of one
core it used (`null` when CPU sampling is unavailable). A failed node operations
sample fails the run rather than inventing a zero. `max_over_mean` is the largest value divided by the mean, and
`cv` is the standard deviation divided by the mean.

The measurements of a latency run hold the rate stages and nothing else: the
preload and the read that checks it are in the `observations`, under
`initial_put` and `initial_get_verify`.

### The events

`events.jsonl` holds one `metrics_sampled` event per second of every stage,
the warmup included:

```jsonc
{
  "event": "metrics_sampled",
  "unix_ms": 1791998161278,
  "fields": {
    "stage": "rate_2", "warmup": false, "window_ms": 1000,
    "target_rate": 20000, "issued": 19990, "late": 10,
    "behind": 0, "sender_lag": 0,
    "ops": { "get": 17980, "put": 1810, "delete": 200 },
    "errors": { "deadline": 0, "not_found": 0, "operation": 0,
                "protocol": 0, "rpc": 0, "topology": 0 },
    "retries": 12,
    "epoch": 1,
    "latency_us": { "get": { "p50": 180, "p95": 600, "p99": 2100, "max": 9800 } }
  }
}
```

`behind` is how many requests the schedule had called for in that window and
the sender had not dealt with yet, neither sent nor dropped. It is zero when
the sender keeps up. A request the in-flight limit dropped is not behind: the
sender dealt with it, and the cluster is the one that could not keep up.

`sender_lag` is the sender's own lag, over the phase so far, and it is not the
lag of the window. It is the number that decides the stage's
`sender_saturated`: the sender is behind its own schedule when it is more
than a twentieth of a second's worth of requests behind. The schedule is
absolute, so a sender that fell behind catches up in a burst and its lag
falls again; the published value is the lag at the last window boundary, not
a maximum and not a total. The two numbers answer different
questions — `behind` is about one second of the load, `sender_lag` about the
whole phase — and a reader should not read one as the other.

`retries` is how many times the client had to try again in that window, for a
request or for a topology refresh, and `epoch` is the topology epoch it routed
with. A window with retries is a window the cluster was not answering at once.

Every mode, not only `latency`, writes a `topology_published` event when the
cluster is ready, after every topology change, and at the end of the run. Its
`fields` are the output of `hashring-rs topology`, so a run that has ended can
still be read: the ring of every epoch is in the event log, and a reader does
not have to ask a coordinator that is gone.

### The key space after the load

The load leaves a key space behind, and the run checks it before it writes
its summary. Every key the load did not delete must still hold the value the
preload wrote, and every key it did delete must be gone.

A delete and a write to the same key race each other, and the outcome of the
race is not known, so the load deletes from a **pool of its own**: the last
part of the key space, which no read and no write ever touches. A delete
therefore never races another operation on the same key, and the expected
state of every key is known. The pool is as large as the run's deletes need,
and never more than half of the key space and never less than a twentieth of
it; the acceptance run needed more than half, so its pool was 50,000 of the
100,000 keys. The sizes are in the `latency_delete_pool_keys` observation.

Every write stores the same value for a key, the one the preload wrote, so two
writes to the same key cannot disagree about what the key holds.

The check itself is in the `latency_verified_keys` observation, and a run that
fails it fails.

### Reading the numbers

- A stage whose `sender_saturated` is true does not describe the cluster. The
  sender could not keep up, so the offered rate was not the requested one.
- The `latency` mode's `achieved_rate` is the operations that **succeeded**,
  divided by the length of the stage. The requests the sender dropped are in
  `late`, and the requests that failed are in `errors`.
- The percentiles come from a histogram that keeps every request, so they are
  the percentiles of the whole stage and not of a sample of it. The histogram
  has three significant figures, so its `max` is the upper bound of the
  bucket the largest value falls in, a little above the value itself.
- The histogram's range ends at the operation deadline plus a second, and a
  request longer than that is recorded in its top bucket and counted in
  `over_range`. Its `max` is then the bound and not the length of the
  slowest request, and `max_observed` is the length of the slowest request.
- A window's `behind` and the stage's `sender_saturated` are two different
  numbers. `behind` is the lag of that window, and `sender_lag` is the
  sender's lag over the phase, which is what `sender_saturated` is decided
  by.
- The `idle_*` figures of the other modes and the `latency` mode's figures
  answer different questions. The first is how fast a request goes when
  nothing else is happening; the second is how fast it goes when the cluster
  is at the rate the stage asked for.

## The scaling mode

`scaling` keeps a mixed read/write load running while membership changes. It
exists to answer what happens to reads and writes *during* a scale, which the
`correctness` mode does not: that mode mutates moving keys once and has no
sustained GET load.

The load has four cohorts, so every key's expected final state is known without
a read/write race:

| Cohort | Before | During | Checked after |
| --- | --- | --- | --- |
| `read` | `v0` | GET only | still `v0` |
| `rewrite` | `v0` | one PUT to `v1` | `v1` |
| `write_new` | absent | one PUT to `v1` | `v1` |
| `delete` | `v0` | one DELETE | absent |

Reads are unpaced and use `--key-distribution`. Writes and deletes are paced at
`--scaling-write-rate` operations per second, split 60/25/15 across rewrite,
fresh write, and delete, and the pools are sized from
`--scaling-pool-seconds`. A pool that empties before the last transition
completes makes the run invalid rather than changing the mix halfway through.
The split is whole operations per second, so the cohorts together offer
slightly less than the configured rate when the shares do not divide it:
`scaling_pool_plan.write_rate` is the configured rate and
`scaling_pool_plan.cohort_write_rates` is the rounded rate each cohort
actually got. A rate that rounds the smallest share down to zero leaves a
cohort without a key, so it is rejected when the configuration is checked.

`--scaling-scenario` picks the change:

- `round-trip` (default): grow by one node, then shrink back, in one cluster
  and one uninterrupted load.
- `grow`: one scale-out.
- `shrink`: one scale-in, starting with every peak node.
- `replace`: swap the last initial member for one spare peak member, keeping
  the member count. This always copies ranges and starts one extra process.
- `churn`: grow by three nodes at once, then shrink back, so multiple ranges
  move in each change.
- `control`: change no membership and measure the same load for
  `--scaling-post-seconds` after the warmup. This is the S0 baseline for a
  round-trip run.

`--scaling-load-mode` selects how the load is offered. `closed` (the default)
runs `--concurrency` read workers and paced write workers. `open` offers a
total of `--scaling-open-rate` operations per second: 80% GET, 12% rewrite,
5% fresh write, and 3% delete. A request that cannot be sent within
`--max-in-flight` is dropped and counted as `late`, and its latency starts at
the time it was due. The per-second `metrics_sampled` events report `issued`,
`late`, and `in_flight` alongside the phase, and the summary has their totals.
An open-loop run does not use `--scaling-write-rate`, so
`scaling_pool_plan.write_rate` is `null` there and
`scaling_pool_plan.cohort_write_rates` reports the shares the open rate was
split into.

The runner starts with `--initial-nodes` (default: peak - 1 for grow,
round-trip, and replace; peak for shrink and control; peak - 3 for churn),
warms up for `--warmup-seconds`, stages the change, starts any new node,
executes the change, polls its phases, waits for the target epoch to reach
full replication, and keeps the load running for `--scaling-post-seconds`. It
then stops that transition's write cohorts, drains those writes, and verifies
the cohorts before the next transition. Reads continue across the whole run,
so the local drain waits for the write batches only; the final drain after the
read senders stop waits for everything.
`--pre-publish-delay-ms` defaults to 0 here, because the delay runs after
writes are paused and would be measured as migration latency.

A successful write or delete is followed by a GET of the same key, and keys
changed before publication are read again after it. Those probe GETs have
their own metrics and are not mixed into the main GET histogram.

`summary.json` holds one `<label>_transition` measurement and observation per
change: `published_ms` and `complete_ms` from the start of the change,
`full_rf_seconds` from `Complete` to the target epoch reaching full
replication, `stop_window`, `migration`, retries by reason, pool sizes, touched
strata, the direct-merge flag, and the coordinator's `work_steps_ms`. The work steps are
`snapshot_copy_ms`, `changelog_replay_ms`, `pause_writes_ms`, `verify_ms`,
`publish_ms`, `activation_ms`, `merge_proof_ms`, and `catch_up_ms`.
`snapshot_copy_ms` and `changelog_replay_ms` are sums over ranges, so either
can exceed the wall-clock phase it ran in; they do not overlap, because a
range's snapshot copy stops before its changelog replay starts. The other
fields are wall-clock costs. `publish_ms` covers the publication work (the
destination checks and the state lock) but not the diagnostic
`pre_publish_delay` and not the durable write that carries it, and
`activation_ms` covers the activation barrier and the target installation but
not source cleanup or stopping removed members. It also excludes the later
`wait_for_full_rf` convergence, which `full_rf_seconds` reports separately.
The publication and the activation are written with the phase they belong to:
`publish_ms` is stored with `Published`, and `activation_ms` with `CleaningUp`
and `Complete`. The steps before the publication are measured in their own
phase and stored with the `Published` write, so a change that is aborted keeps
none of them and a change that is published keeps all of them. Every one of
those writes is a single durable write, so a repeated `execute-change`, a
`change-status` read, and a restarted coordinator all see what the first
`execute-change` returned.

`published_ms_source` says where `published_ms` came from. It is `observed`
when the runner saw the change publish, and `completion_fallback` when a short
change went from `Verifying` to `Complete` between two polls: the probe still
runs, but the earliest moment that can be named is the completion, and
`published_ms` then equals `complete_ms`.

Every change also gets `scaling_after_publish_probes`: the number of probes and
the keys each one read. A probe with nothing to read is not evidence, so a
change whose publish window held no successful write to a moved stratum fails
the run rather than passing without the read-after-publish check.

`migration` is the plan's migration volume: `keys` and `keys_moved` compare
the base and target topologies key by key, `rate` is their quotient, `ranges`
is how many ranges the coordinator moved, and `snapshot_records` is how many
records the copy path actually carried. A direct merge moves keys without
creating ranges, so it reports zero ranges and zero records.

`stop_window` is what the client saw rather than the phase timeline. During a
change the runner samples the client's cumulative `RangeBusy` count and
successful write count every 5 ms, and reports `first_range_busy_ms`,
`last_range_busy_ms`, `first_write_ok_ms`, `observed_ms`, and `poll_ms`. A
node only fences a range when it receives the command, so the phase boundary
is not the stop; a transition with no rejection leaves the window empty.

`observations.scaling_metrics` holds the per-phase operation and latency
totals, and `events.jsonl` holds one `metrics_sampled` event per second plus
the phase transitions. The full
protocol, cohorts, and pass criteria are in
[scaling-under-load.md](scaling-under-load.md).

The mode runs every process on one host and is a local measurement, not a
service-level guarantee. `--require-clean-source` marks a formal run.
`scripts/scaling-matrix.sh` runs the formal groups (main, S0 control,
250 ms diagnostic, FirstSuccessor policy, open loop, and optionally replace
and churn) with a fresh output directory per repetition.

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
  application, replication ACK processing, and the owner's final check that a
  write's required ACKs arrived

These are cumulative wall-clock counters. Summed wait times can overlap across
tasks and nodes. Without profiling, the timing fields are `null`.

Set `HASHRING_PROFILE_RPC=1` to have the driver and every node print RPC
latency histograms to their stderr twice a second as `rpc_profile` lines:

- client-observed `Put` and `Get` RPC attempts, and whole logical `put`s
  including retries
- owner handler time for `Put` and `Get`
- owner-observed replication batch round trips, follower handler time per
  batch, and entries per batch

Comparing a client-observed RPC with its handler time separates transport and
scheduling from the node's own work. `scripts/perf/run.sh` runs one
performance-mode experiment with per-process CPU sampling and this
summary, and `scripts/perf/collect.py` tabulates a set of such runs. See
[grpc-io-bottleneck.md](optimizations/grpc-io-bottleneck.md) for an example.

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
