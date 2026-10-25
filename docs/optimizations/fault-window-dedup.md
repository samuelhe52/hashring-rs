# Constant-time acknowledgment dedup in the fault window

## Change and reason

`experiment --mode fault` keeps, per key, the states it has acknowledged, so a read that matches one of them but not the key's current state can be classified as a permitted rollback instead of as a value the history never held. A re-acknowledged state moves to the back of the window rather than taking a second slot: a duplicate would push a distinct state out early, and the window exists to keep the recent ones.

`KeyModel::remember` found out whether the state was already there by scanning the window on every write, so the load's write path cost `writes × window` digest comparisons per key. [`--fault-acknowledged-window`](../configuration.md) is sized as a multiple of the writes one key takes, so a long run grows that product quadratically: 10,000 writes per key with a 40,000-state window is about 400 million comparisons per key.

The model now keeps an index of the window's states as 64-bit fingerprints with a count each ([fault.rs](../../crates/hashring-experiment/src/fault.rs)). Acknowledging a state the window has never held is one lookup plus a `push_back`. Only a state the window already holds — a re-acknowledgement, rare in the fault load, whose values are derived from the request — scans for its position and moves it to the back.

## Evidence and limits

- The [unit test](../../crates/hashring-experiment/src/fault.rs) drives new writes, re-acknowledgements of states still in the window, and re-acknowledgements of states the window already dropped. It asserts after every step that the counts add up to the window's length and that every state in the order is indexed.
- Nothing changes about which reads are verifiable: the window's contents, order, and dropped count are the same, so the classification of a read is unaffected.
- No throughput difference is measurable at the documented sizes. The window fills only to the writes one key takes (about 200 in the default owner run), and the audit runs on `177387b` measured 6,736 against 6,678 requests per second when the limit rose from 256 to 4,096, inside the run-to-run variation. The gain is asymptotic, for runs that fill a window much larger than a few hundred states.
- A [white-box test](../../crates/hashring-experiment/src/fault.rs) injects a fingerprint that another state already has and acknowledges a state under it. The state has to enter the window (a read of it is a rollback, not an impossible value) and has to leave the other state's count in place when the window evicts it, which is why the index counts fingerprints instead of storing a set of them.

## Tradeoff

The fingerprint index adds about 25 B per state on top of the 66 B digest pair (an 8-byte key, an 8-byte count, and hash-table overhead), so the driver's bound is roughly 95 B per state and `keys × window × ~95 B` overall (see the [fault document](../fault-under-load.md#历史模型)). Hashing the state per write is cheaper than the scan it replaces but not free; it is the price of not storing a second copy of the state in the index.

A re-acknowledgement still scans the window once. That keeps the order exact — a state that is acknowledged again really is the most recent one — instead of approximating it with a plain arrival-order eviction, which would let a state that is current for the key fall out of the window while it is still recent.
