# Redis compatibility

`redis-gateway` serves the Redis protocol on a TCP port over a hashring-rs
cluster. A Redis client connects to it the way it would to a Redis server, so
`redis-cli`, redis-py, Jedis, Lettuce and go-redis all work against it in
single-node mode without knowing that the data lives on several nodes behind a
consistent hash ring.

This page is the honest description of what that buys and what it does not.
Read the [limits](#limits-here) before pointing anything at it that matters.

## Quick start

```sh
# With a cluster already running:
cargo run -- redis-gateway --listen 127.0.0.1:6379 \
  --coordinator http://127.0.0.1:50050

redis-cli -p 6379 set greeting hello
redis-cli -p 6379 get greeting
```

The coordinator is contacted on the **first command**, not at startup, so a
gateway may be started before, alongside, or instead of the coordinator.
Clients get `-TRYAGAIN` until the cluster answers. Several gateways may be run
at once; see [how read-modify-write stays atomic](#read-modify-write-is-a-conditional-write).

Settings are in [configuration.md](configuration.md#redis-gateway), and the
gateway itself lives in [`crates/hashring-redis`](../crates/hashring-redis).

## What it is not

It is **not Redis Cluster**. There is no slot table, no `MOVED`, no `ASK`, and
no gossip. Those exist in Redis because a client cannot compute Redis' hash
ring; this cluster already hands out its whole topology through the
coordinator, and `hashring_client` routes every key to its owner. So the gateway
adds the protocol, not the routing.

The consequence to weigh: the project deliberately chose client routing so that
normal reads and writes take no extra hop
([`docs/report/related-work.md`](report/related-work.md)). A Redis-speaking
client cannot be given the topology, so the gateway puts a proxy back in the
path. Every request becomes client → gateway → node: two hops, two
serializations of the value, and a central process that did not previously
exist. That is the price of not changing the client.

## Supported commands

`COMMAND` reports exactly this list, so a client that asks what the server
supports sees what is actually implemented.

### Connection and administration

| Command | Notes |
| --- | --- |
| `PING`, `ECHO` | |
| `HELLO` | RESP2 and RESP3. The reply is a map under RESP3 and a flat array under RESP2. `HELLO <ver> AUTH <user> <pass>` and `SETNAME` are accepted; the request is checked whole before any of it applies, so a wrong password changes neither the protocol nor the name. |
| `AUTH` | Only when `--requirepass` is set. Otherwise it answers the error Redis gives when no password is configured. |
| `SELECT` | Accepts database 0 only; one flat namespace, so any other index is refused. |
| `CLIENT` | `ID`, `SETNAME`, `GETNAME`, `SETINFO`, `INFO`, `LIST`, `NO-EVICT`, `NO-TOUCH`, `UNPAUSE`, `GETREDIR`, and `REPLY ON\|OFF\|SKIP`. As in Redis, `REPLY ON` answers `OK` and `OFF` and `SKIP` answer nothing. |
| `RESET` | Returns `+RESET` and restores RESP2, even on a RESP2 connection where Redis errors. Being liberal here is what keeps clients that send it on reconnect working. |
| `QUIT` | Answers, then closes. |
| `COMMAND` | `COMMAND`, `COMMAND COUNT`, `COMMAND INFO`, `COMMAND DOCS`. `GETKEYS` is declined. |
| `INFO` | Server, clients, memory, persistence, stats, replication, cluster and keyspace sections. |
| `CONFIG` | `GET` with globs, `SET` for the listed parameters, `RESETSTAT`, `REWRITE`. |

`INFO` and `CONFIG` are honest but minimal. `CONFIG GET` and `CONFIG SET` cover
`maxmemory`, `maxmemory-policy`, `appendonly`, `appendfsync`, `save`, `dir`,
`dbfilename`, `timeout`, `notify-keyspace-events` and `lazyfree-lazy-user-del`.
`CONFIG SET` accepts them and changes nothing, which is what Redis does for a
parameter it has no other value for; any other parameter is an error.
`notify-keyspace-events` reports empty because no events are emitted.

### Keys

| Command | Notes |
| --- | --- |
| `DEL`, `UNLINK` | Both are the same delete. `UNLINK` is not asynchronous here. |
| `EXISTS` | Counts duplicates, as Redis does. |
| `TOUCH` | |
| `TYPE` | Always `string` or `none`. |
| `RENAME`, `RENAMENX` | The value and its deadline move together. `RENAMENX` on an existing destination errors, as Redis does. |

### Keyspace

| Command | Notes |
| --- | --- |
| `KEYS` | Redis' glob, matched over bytes; only `^` negates a class, and range ends compare as signed bytes, as in Redis on x86-64. Errors rather than answer with more than 64 MiB of keys, each key counted with 64 bytes of overhead, or spend more than 100 million pattern steps matching. |
| `SCAN` | `MATCH`, `COUNT` (default 10) and `TYPE`. Returns each key that is live for the whole scan exactly once, where Redis promises at least once. A cursor may be continued through any gateway and across membership changes. `MATCH` and the reply size have `KEYS`' limits. |
| `DBSIZE` | Live keys only: each owner counts its own ranges, without its follower copies or keys past their deadline. |
| `RANDOMKEY` | The first key at or after a random point on the ring, or nil when there are none. Not uniform: a key after a wide gap is likelier, as Redis' choice is not uniform either. |

`KEYS`, `DBSIZE` and `RANDOMKEY` walk every owner, all at once, page by page;
each node pages only through the ranges it owns at the topology epoch the walk
started from. If
ranges change hands mid-walk, a node at the newer epoch refuses the page and the
walk starts over. So an answer counts every key that was live for the whole
command exactly once; a key written or deleted while it ran may or may not be
in it. Under constant topology churn a walk can run out of its deadline and
answer `TRYAGAIN`.

`SCAN` goes round the ring in hash order instead, one owner's stretch at a
time, asking whichever node owns the next stretch when it gets there. Its
cursor is the ring position to continue from, printed as the decimal integer
clients expect, so the gateway keeps no state between calls. A position means
the same thing in every topology: a range that moved between two calls is read
from its new owner, which holds all of it before it owns it, and a range the
scan already passed is not read again. That is why a key that is live
throughout comes back exactly once. As in Redis, a key written or deleted
during the scan may or may not come back, a step may return fewer keys than
`COUNT`, or none, before the scan is done, and with `TYPE` naming anything but
`string`, a type Redis knows or not, the scan is over at once, since every key
is a string.

A page of keys always ends between two ring positions, so that the cursor can
be one number. That needs a bound on how many keys one position holds: by
chance two keys share a 64-bit position almost never, but keys can be built to
collide by anyone who knows the cluster's hash seed. So a node refuses to
create a seventeenth key at one position, with an error, while writes to the
keys already there go on working. A coordinator
recreated from scratch picks a new hash seed, which moves every key and so
invalidates every cursor; nothing else does.

Unlike Redis, `DBSIZE` is not a stored counter: it costs a walk over every key,
on every node. `KEYS` costs the same plus the matching keys' bytes, buffered at
the gateway before the reply is sent, hence its limit.

### Strings

| Command | Notes |
| --- | --- |
| `GET`, `GETDEL` | |
| `GETEX` | Takes `EX`, `PX`, `EXAT`, `PXAT`, `PERSIST`. Without an option it keeps the deadline. Changing the deadline does not rewrite the value. |
| `GETRANGE` | Redis' index rules, including negative indices. |
| `SET` | `NX`, `XX`, `GET`, `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`. |
| `SETEX`, `PSETEX` | |
| `SETNX`, `GETSET` | |
| `SETRANGE` | Pads with zero bytes. An empty patch reports the length and creates nothing. |
| `APPEND`, `STRLEN` | |
| `MSET`, `MGET` | Not atomic across owners. A key named twice takes its later value. See [cross-key atomicity](#cross-key-atomicity-is-not-available-at-all). |
| `MSETNX` | One key only, where it is `SETNX`. With more than one key it is declined, since it cannot set them atomically; see [cross-key atomicity](#cross-key-atomicity-is-not-available-at-all). |
| `INCR`, `DECR`, `INCRBY`, `DECRBY` | Atomic against every writer. A value that is not an integer is an error and the value is left alone. |
| `INCRBYFLOAT` | Atomic against every writer. The result is printed in the shortest form that reads back as the same double. |

### Expiry

| Command | Notes |
| --- | --- |
| `TTL`, `PTTL` | `-1` is a key with no deadline, `-2` no key at all. `TTL` rounds up to whole seconds. |
| `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` | Accepts one of `NX`, `XX`, `GT`, `LT`. A key with no deadline counts as never expiring, so `LT` sets one and `GT` does not. A deadline that is not in the future, including a negative timeout, deletes the key. |
| `PERSIST` | Clears the deadline without rewriting the value. |

## Declined commands

These are answered with a clear error rather than a wrong answer:

```
127.0.0.1:6379> flushall
(error) ERR FLUSHALL is not supported by the hashring-rs Redis gateway; see docs/redis-compatibility.md
```

| Command | Why |
| --- | --- |
| `FLUSHALL`, `FLUSHDB` | A write to every key on every owner, which no node can apply atomically across owners: a flush that failed part-way would leave some ranges emptied and others not. |
| `MSETNX` with more than one key | Cannot set its keys atomically; see [cross-key atomicity](#cross-key-atomicity-is-not-available-at-all). |

Anything not in this document is answered with Redis' own unknown-command error.
A data structure command (`HSET`, `ZADD`, `LPUSH`, …) is unknown here, because
this gateway stores byte strings and nothing else. So are `MULTI`, `EXEC`,
`WATCH`, `EVAL`, `SCRIPT`, and the `SUBSCRIBE` family.

If a workload needs any of those, this is not a Redis substitute.

## Semantics that differ from Redis

These are the places where an application that works against Redis would notice
a difference.

### Read-modify-write is a conditional write

Redis makes `INCR` atomic because one thread owns the key. A gateway has no such
owner: the key lives on a data node. So `INCR`, `APPEND`, `SETRANGE`, `SETNX`,
`GETSET`, `GETDEL`, `GETEX`, `SET ... NX|XX|GET`, `INCRBYFLOAT`, `DEL`, `EXPIRE`
and `PERSIST` read the key, compute, and write.

The write carries a precondition: the version the read returned, or that the key
was absent. The owning node checks it under the same lock that applies the
write, and refuses the write if anything else wrote the key in between. The
gateway then reads again and recomputes. So these commands are atomic against
**every** writer: clients of the same gateway, clients of other gateways behind
the same load balancer, and programs using `hashring-client` directly.

The costs are a read before every such write, as before, and a retry when two
writers collide on one key. Within one gateway, a striped per-key lock makes its
own clients take turns, so collisions are with writers elsewhere. A command that
keeps colliding is bounded by the [command deadline](#limits-here).

A plain `SET`, `SETEX`, `PSETEX` or `MSET` has nothing to compute and is one
unconditional write. A read-modify-write that raced it simply reads again.

### Cross-key atomicity is not available at all

`MSET`, `DEL`, `EXISTS`, `MGET` and `RENAME` fan out to whichever
owners hold their keys, and each key is applied on its own. There is no
operation that spans two nodes atomically, and none can be added at this layer:
the keys are on different machines.

If any key fails, the whole command answers with an error rather than a count
or a list built from the keys that happened to answer. When a writing command
such as `DEL` or `MSET` failed on one key after writing another, that error is
`ERR` and says its writes may have been applied, never `TRYAGAIN`: sending the
command again could undo someone else's write in between.

`RENAME` makes the best of it and reports what it could not do. It writes the
destination, then deletes the source on condition that it is still the version
it copied. A source rewritten in between is left in place and the command
answers with an error, since the destination holds the earlier value.

`MSETNX` is declined when it names more than one key. Its whole purpose is to
set several keys as one, typically to claim them together, and a version that
can leave some of them set would break exactly that use: two clients racing on
overlapping keys could each set part of them and neither hold the set. With one
key it is `SETNX` and atomic.

This is the same trade Redis Cluster makes, where such commands fail with
`CROSSSLOT`. Here they succeed but are not atomic. The honest comparison for
this gateway is Redis Cluster, not Redis single-instance.

If a write must not be observed half-applied, it must be one key.

### `DEL` reads before it deletes

Redis counts the keys that existed and were removed. This cluster's delete is
idempotent and reports success whether or not the key was there, so the gateway
reads each key first and deletes it on condition that it is still the version
read, which keeps the count exact. The cost is one extra request per key, issued
concurrently. `DEL` of a large keyspace is therefore roughly twice the requests
of a Redis `DEL`.

### `EXPIRE` with a past deadline deletes the key

As in Redis, `EXPIRE key 0`, or an `EXPIREAT` in the past, removes the key
rather than setting a deadline that has already passed.

### `SET` without an expiry drops the deadline

As in Redis. `SET key value` clears any TTL the key had; `SET key value KEEPTTL`
keeps it; `SET key value EX 60` sets a new one. `NX` and `XX` may not be
combined, nor two expiry options, nor `KEEPTTL` with one.

### `INFO` reports an empty keyspace section

`# Keyspace` has no `db0:keys=` line, because counting means walking every key
on every owner, which `INFO` should not cost. `DBSIZE` does that walk. A tool
that reads key counts from `INFO` will see nothing rather than a wrong number.

## How expiry works

A deadline is a field on the record, absolute, in Unix milliseconds. It is
enforced by whichever replica is asked: a record past its deadline reads as
absent, wherever it lives. That is what makes it survive the things a
gateway-local expiry could not:

- **Failover.** A promoted replica holds the same record with the same deadline.
- **Migration.** The deadline travels with the record through the snapshot and
  the changelog, and both sides of a range include it in the verification
  digest, so a source and a destination that disagree about a deadline fail
  verification instead of handing over a key that stops being readable at two
  different times.
- **Restart.** A gateway restart, a coordinator restart, or a node restart does
  not lose it.

Records past their deadline are hidden on read and reclaimed by a sweep every
500 ms, bounded per pass so the sweep cannot stall writers. A key nobody asks
about stays resident until its next sweep. While a range is migrating off a
node, its expired records wait for the migration to end before they are
reclaimed, because both sides of the migration verify the same records.

Two consequences worth knowing:

- **Clock skew shifts deadlines.** The gateway's clock decides when a relative
  TTL such as `EX 60` becomes an absolute deadline, and the node's clock decides
  when that deadline passes. A gateway whose clock is a minute fast sets a
  deadline a minute early. Cluster nodes should be on NTP.
- **Expiry is not a deletion.** Nothing is deleted at the deadline, so an
  expired key is invisible but still occupies memory until the sweep. This is
  what avoids a delete having to travel through replication, and why `KEYS`
  and `DBSIZE` check each key's deadline rather than count what is stored.

## Errors

| Condition | Code |
| --- | --- |
| Key or value too large | `OOM` |
| Bad argument, syntax, unknown command | `ERR` |
| Retryable cluster condition: a range is moving, a follower is not ready, replication is backed up, the coordinator is unreachable, a read passed its deadline | `TRYAGAIN` |
| Command sent before `AUTH` | `NOAUTH` |
| Wrong password | `WRONGPASS` |
| A write whose outcome is unknown, including a write command that passed its deadline | `ERR`, saying the writes may or may not have been applied |
| More than `--maxclients` connections | `ERR max number of clients reached`, and the connection is closed |

`TRYAGAIN` is the code clients already back off on, so a client that honours it
behaves sensibly across a migration. A missing key is never an error: `GET`
returns nil, `EXISTS` returns 0, `TTL` returns -2.

## Client notes

- The handshake every client performs works: `HELLO`, `AUTH`, `SELECT`,
  `CLIENT SETINFO` are all answered.
- **RESP3 is supported** and negotiated through `HELLO 3`. A missing value is
  `$-1` under RESP2 and `_` under RESP3, maps are flat arrays under RESP2 and
  real maps under RESP3. go-redis defaults to RESP3 and works.
- Every value is a byte string, binary-safe, and keys are not restricted to
  UTF-8.
- `CLIENT SETINFO` is accepted and ignored.
- The connection is not closed for a bad command, a declined command, or a
  protocol error in a command; only an unparseable frame ends it, because the
  stream cannot be resynchronized.
- Before a connection authenticates it may send at most 10 arguments of at most
  16 KiB each, as in Redis, so a client without the password cannot make the
  gateway hold memory for it. Any connection may have at most 1 GiB received
  and not yet run, and a line without a newline (an inline command, or a RESP
  header) may be at most 64 KiB.

## Limits here

- One process, one extra hop. See [what it is not](#what-it-is-not).
- No atomicity across keys, ever.
- No `FLUSHALL`. `KEYS` and `DBSIZE` walk every key on every owner,
  so they cost as much as the keyspace is large.
- No data structures, transactions, scripting or Pub/Sub.
- The gateway keeps nothing, so it holds no expiry state, no keys and no
  replication state. Losing it loses no data; losing the *cluster* loses every
  value on every node, as the cluster's own guarantees describe.
- Command deadlines come from `--deadline-ms`, default 5 s, and cover the whole
  command: lock waits, every request it makes, and the retries of a conditional
  write. Redis has no such deadline; a proxy in front of a migrating cluster
  needs one. A read that passes it answers `TRYAGAIN`, so a long migration turns
  into client retries rather than silent timeouts. A write that passes it
  answers `ERR` instead, because it may already have been applied.

## Planned: parity against a real Redis

- [ ] **Differential test against `redis-server`.** `tests/redis_gateway.rs`
  drives the gateway with a real client, but every expected reply in it is
  written by hand from what Redis is believed to do, so a wrong belief passes.
  Add `tests/redis_parity.rs`, which sends the same command sequences to a real
  Redis (a local `redis-server`, else a pinned `redis:7` container; the test is
  skipped when neither is available) and to the gateway, and compares the
  replies as `redis::Value` under RESP2 and RESP3.
  - Move the `Cluster` harness out of `redis_gateway.rs` into a shared test
    module rather than copying it.
  - Isolate cases by flushing the reference and giving each case its own key
    prefix on the gateway, since `FLUSHALL` is declined there.
  - Compare error codes (`ERR`, `WRONGTYPE`, …) rather than whole messages.
  - Normalize what is not deterministic: compare `KEYS` and a full `SCAN` as
    sets, check `RANDOMKEY` only for membership, allow ±1 on `TTL` and `PTTL`,
    and skip `INFO`, `CLIENT ID`/`LIST`, the server fields of `HELLO`, and
    `COMMAND`.
  - List each difference this page documents as an expected divergence that
    links its section, so that only a difference not recorded here fails.
  - Start with a fixed script covering every supported command and its options.
    Then add a seeded random replay over a small pool of keys and values, which
    finds edge cases such as `SETRANGE` padding, `INCRBYFLOAT` formatting,
    negative offsets and `GETEX` option combinations. Print the seed on failure.
  - Out of scope: concurrency and cross-key behaviour during migration. Parity
    checks what one sequential client sees on the wire, and the existing tests
    keep covering the rest.

## Not attempted

Recorded so the gap is deliberate rather than an oversight. In rough order of
how much each would cost:

| | What it would take |
| --- | --- |
| Hashes, sets and sorted sets | Encoding each as one record under one key. A single structure would then inherit the cluster's per-key linearizability, which is the right guarantee for one. The costs are a whole-structure rewrite per mutation and the 8 MiB value limit, which bites in the tens of thousands of members. |
| Lists and streams | Not a good fit. `LPOP` and consumer groups want O(1) element access and blocking reads across the cluster; a whole-structure rewrite does not give either. |
| `MULTI`, `EXEC`, `WATCH` | Would need server-side execution, and cross-key atomicity is unavailable underneath, so `EXEC` could not honestly promise atomicity across keys. |
| Durability | The cluster is in-memory by design; the gateway adds no storage. |
