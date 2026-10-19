# Completing required ACKs under the read lock: no established speedup

This change was measured and not merged. It turned a second write-lock
acquisition per ACK-waiting write into a read. At concurrency 256 that
shortened lock queues by about 10%, but throughput and client latency did not
move beyond noise. At concurrency 64 it added a small owner-side cost. Since
it also adds a completion queue and a deferred release, it was not worth
keeping. The candidate is [candidate.patch](data/lazy-ack-completion/candidate.patch),
commit `0c6c9af`, applied on the completion timing in `cb71c40`.

Under `FirstSuccessor` and `AllReplicas`, every write took the `NodeState` write
lock twice: once to apply, and again in `wait_required_acks` after its follower
ACKs arrived. The second acquisition existed only to clear the receipt's
`required_acks` and release their retained bytes. It was untimed until
`ack_completion_write_timing` was added. In the performance matrix of
[ack-state-lock.md](ack-state-lock.md), it waited 24 µs per write at
concurrency 64 and 109 µs at 256. That was longer than the write's own first
acquisition.

## Candidate

`wait_required_acks` made the same final checks under the **read** lock:
lease, epoch, follower admission, follower process instance, and ACK progress. Instead of clearing the receipt
itself, it pushed `(mutation_id, version)` onto a new `NodeState::completed_acks`,
a `std::sync::Mutex<Vec<_>>`.

A new `apply_completed_acks` released the queued ACK lists under write locks
that are already being taken:

- `purge_expired_dedup`, which runs on every owner `Put`, `Delete` and
  follower apply
- the 1-second expiry sweep, for an idle owner

It needed no lock of its own. Pushers hold the read lock, so the write-lock
holder reads the queue through `Mutex::get_mut`.

### Semantics

- A receipt kept its ACK list, and its retained bytes stayed counted against
  `max_dedup_bytes`, until the next write-lock holder ran.
- A retry that reached the receipt in that window waited on ACKs that had
  already arrived. It returned at once unless the follower restarted in that
  window, in which case it reported an unknown outcome. The same race existed before, across
  the wait for the write lock.
- A retry on the write path called `purge_expired_dedup` before its receipt
  lookup, so it never saw a stale list.
- `prune_ack_progress` read the lists after they were applied, so it kept no
  stream it would have released before.

## Evidence

The workload is the 6-node performance series of
[grpc-io-bottleneck.md](grpc-io-bottleneck.md): 300,000 keys, RF=3, 2 ms
linger, 2 workers per process, and both profilers on. The host is an 8-core
M1 Pro.

The binaries are `main` at `30b19dc` (sha256 `e4d5ec42…`) and the candidate at
`0c6c9af` (`d1f9de01…`), which also carried the completion timing. Per cell
there was one warm-up pair, excluded, and then 4 measured pairs in alternating
order. All 20 runs succeeded.

Only `FirstSuccessor` was measured. Under `OwnerOnly`, no write waits for ACKs,
so neither the change nor the timing runs.

The changes below are the geometric mean of per-pair ratios, with a paired
bootstrap 95% interval. Lock figures are µs per write, summed across nodes.

| | `FirstSuccessor`, 64 | `FirstSuccessor`, 256 |
| --- | --- | --- |
| PUT ops/s, `main` → change | 40,494 → 40,281, −0.0% [−1.5, +0.9] | 56,036 → 56,670, +1.9% [+0.3, +3.6] |
| owner lock wait | +1.0% [−9.1, +12.3] | −3.3% [−5.2, −0.1] |
| owner lock hold | **+5.5%** [+1.7, +11.5], 4/4 pairs | +3.4% [−0.3, +6.8] |
| follower lock wait | −2.3% [−8.5, +6.2] | **−10.2%** [−12.9, −7.4], 4/4 |
| ACK writeback wait | −5.1% [−11.7, +2.1] | **−9.9%** [−12.3, −7.4], 4/4 |
| client `Put` RPC mean | +0.4% [−0.5, +2.0] | −1.3% [−2.8, +0.3] |
| completion check wait, per acquisition | 9.5 µs, read | 37.9 µs, read |

Raw data: [main-vs-fix-runs.tsv](data/lazy-ack-completion/main-vs-fix-runs.tsv),
[main-vs-fix-host.txt](data/lazy-ack-completion/main-vs-fix-host.txt), and the
runner [main-vs-fix-ab.sh](data/lazy-ack-completion/main-vs-fix-ab.sh).

An earlier focused run compared the same change against a base that also
carried the rejected epoch atomic
([ack-state-lock.md](ack-state-lock.md#the-epoch-atomic)). There, follower
lock wait fell 7% and 13% in 4 of 4 pairs at each concurrency. Raw rows:
[epoch-head-vs-lazy-runs.tsv](data/lazy-ack-completion/epoch-head-vs-lazy-runs.tsv).

## Reading

- **At concurrency 256, removing 300,000 write acquisitions shortens the
  writer queue.** Follower lock wait and ACK writeback wait each fall about 10%
  in every pair.
- **Throughput and latency are not established.** The +1.9% PUT interval
  excludes zero, but a bootstrap over 4 pairs understates the spread. Larger
  matrices on this host could not resolve changes of a few percent.
  Per-RPC transport cost still sets the RF=3 ceiling
  ([grpc-io-bottleneck.md](grpc-io-bottleneck.md)).
- **The completion check still waits 10-38 µs.** tokio's `RwLock` is fair, so
  a reader queues behind waiting writers. Removing that wait would mean making
  the final check without the state lock at all, for example by publishing the
  lease, epoch and admissions through atomics.
- **Owner hold grows about 0.3 µs.** That is applying a queued completion inside
  an owner's critical section, and it is significant at concurrency 64. It is the
  cost of folding the work into an acquisition that already happens, and it is
  smaller than the separate acquisition it replaces.

## Verdict

The candidate's lock-level effects are real and replicated: follower and ACK
writeback waits fell about 10% at concurrency 256. Owner hold rose about 0.3 µs
at concurrency 64. Neither reaches the client. A few µs of lock wait per write
disappears inside a 1.5-4 ms `Put`, which is dominated by per-RPC transport
cost. With no measured benefit, the deferred release below was not worth
adding.

The tradeoffs it would have brought:

- Released ACK lists and their bytes lag by up to one owner write, or by up to
  the 1-second sweep interval on an idle node. Until then, `dedup_bytes`
  overstates retained bytes slightly, so the receipt-store capacity check is
  slightly conservative.
- The queue grows by one entry per ACK-waiting write between drains. Under load
  that is a handful of entries.
