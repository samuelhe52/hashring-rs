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
at once; see [the atomicity limit](#read-modify-write-is-atomic-only-within-one-gateway).

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
| `HELLO` | RESP2 and RESP3. The reply is a map under RESP3 and a flat array under RESP2. `HELLO <ver> AUTH <user> <pass>` and `SETNAME` are accepted. |
| `AUTH` | Only when `--requirepass` is set. Otherwise it answers the error Redis gives when no password is configured. |
| `SELECT` | Accepts database 0 only; one flat namespace, so any other index is refused. |
| `CLIENT` | `ID`, `SETNAME`, `GETNAME`, `SETINFO`, `INFO`, `LIST`, `NO-EVICT`, `NO-TOUCH`, `UNPAUSE`, `GETREDIR`, and `REPLY ON\|OFF\|SKIP`. |
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

### Strings

| Command | Notes |
| --- | --- |
| `GET`, `GETDEL` | |
| `GETEX` | Takes `EX`, `PX`, `EXAT`, `PXAT`, `PERSIST`. Without an option it keeps the deadline. |
| `GETRANGE` | Redis' index rules, including negative indices. |
| `SET` | `NX`, `XX`, `GET`, `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`. |
| `SETEX`, `PSETEX` | |
| `SETNX`, `GETSET` | |
| `SETRANGE` | Pads with zero bytes. |
| `APPEND`, `STRLEN` | |
| `MSET`, `MSETNX`, `MGET` | Not atomic across owners. See [limits](#limits-here). |
| `INCR`, `DECR`, `INCRBY`, `DECRBY` | Atomic within one gateway. A value that is not an integer is an error and the value is left alone. |
| `INCRBYFLOAT` | Atomic within one gateway. The result is printed in the shortest form that reads back as the same double. |

### Expiry

| Command | Notes |
| --- | --- |
| `TTL`, `PTTL` | `-1` is a key with no deadline, `-2` no key at all. `TTL` rounds up to whole seconds. |
| `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` | Accepts `NX`, `XX`, `GT`, `LT`. |
| `PERSIST` | |

## Declined commands

These are answered with a clear error rather than a wrong answer:

```
127.0.0.1:6379> scan 0
(error) ERR SCAN is not supported by the hashring-rs Redis gateway; see docs/redis-compatibility.md
```

| Command | Why |
| --- | --- |
| `SCAN`, `KEYS` | The data node holds a token index over its keys but no RPC to enumerate a range of it. `DBSIZE` is declined for the same reason: a count would have to be summed across every owner, and two callers would never agree on the answer. |
| `FLUSHALL`, `FLUSHDB` | The same enumeration, plus the question of what flushing a sharded cache should even mean. |
| `RANDOMKEY` | Needs the keyspace. |

Anything not in this document is answered with Redis' own unknown-command error.
A data structure command (`HSET`, `ZADD`, `LPUSH`, …) is unknown here, because
this gateway stores byte strings and nothing else. So are `MULTI`, `EXEC`,
`WATCH`, `EVAL`, `SCRIPT`, and the `SUBSCRIBE` family.

If a workload needs any of those, this is not a Redis substitute.

## Semantics that differ from Redis

These are the places where an application that works against Redis would notice
a difference.

### Read-modify-write is atomic only within one gateway

Redis makes `INCR` atomic because one thread owns the key. A gateway has no such
owner: the key lives on a data node, and the node offers only get, put and
delete. So `INCR`, `APPEND`, `SETRANGE`, `SETNX`, `GETSET`, `GETDEL`, `SET ... GET`,
`INCRBYFLOAT`, `MSETNX`, `RENAME`, `EXPIRE` and `PERSIST` are read-modify-write
operations here too.

The gateway serializes them with striped per-key locks, which makes them atomic
**between clients of one gateway process**. This does **not** hold when:

- more than one gateway is running behind a load balancer;
- a client bypasses the gateway and talks to the cluster with `hashring-client`.

Run one gateway if the guarantee matters, and understand that you now have a
process that must not be restarted carelessly.

### Cross-key atomicity is not available at all

`MSET`, `MSETNX`, `DEL`, `EXISTS`, `MGET` and `RENAME` fan out to whichever
owners hold their keys, and each key is applied on its own. There is no
operation that spans two nodes atomically, and none can be added at this layer:
the keys are on different machines.

This is the same trade Redis Cluster makes, where such commands fail with
`CROSSSLOT`. Here they succeed but are not atomic. The honest comparison for
this gateway is Redis Cluster, not Redis single-instance.

If a write must not be observed half-applied, it must be one key.

### `DEL` reads before it deletes

Redis counts the keys that existed and were removed. This cluster's delete is
idempotent and reports success whether or not the key was there, so the gateway
reads each key first and counts that. The cost is one extra request per key,
issued concurrently. `DEL` of a large keyspace is therefore roughly twice the
requests of a Redis `DEL`.

### `PERSIST` rewrites the value

The cluster has no "clear this field" operation, so `PERSIST` reads the value
back and writes it without a deadline. That is a new record version and the
usual write path, where Redis just clears the TTL in place.

### `EXPIRE` with a past deadline deletes the key

As in Redis, `EXPIRE key 0`, or an `EXPIREAT` in the past, removes the key
rather than setting a deadline that has already passed.

### `SET` without an expiry drops the deadline

As in Redis. `SET key value` clears any TTL the key had; `SET key value KEEPTTL`
keeps it; `SET key value EX 60` sets a new one. `NX` and `XX` may not be
combined, nor two expiry options, nor `KEEPTTL` with one.

### `INFO` reports an empty keyspace section

`# Keyspace` has no `db0:keys=` line, because counting would mean asking every
owner. A tool that reads key counts from `INFO` will see nothing rather than a
wrong number.

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
about stays resident until its next sweep.

Two consequences worth knowing:

- **Clock skew shifts deadlines.** The gateway's clock decides when a relative
  TTL such as `EX 60` becomes an absolute deadline, and the node's clock decides
  when that deadline passes. A gateway whose clock is a minute fast sets a
  deadline a minute early. Cluster nodes should be on NTP.
- **Expiry is not a deletion.** Nothing is deleted at the deadline, so an
  expired key is invisible but still occupies memory until the sweep. This is
  what avoids a delete having to travel through replication, which in turn means
  `DBSIZE`-style counts cannot include expired keys, and is the reason the
  keyspace commands are declined rather than approximated.

## Errors

| Condition | Code |
| --- | --- |
| Key or value too large | `OOM` |
| Bad argument, syntax, unknown command | `ERR` |
| Retryable cluster condition: a range is moving, a follower is not ready, replication is backed up, the coordinator is unreachable, the command passed its deadline | `TRYAGAIN` |
| Command sent before `AUTH` | `NOAUTH` |
| Wrong password | `WRONGPASS` |
| A write whose outcome is unknown | `ERR`, because the value may well be stored |

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

## Limits here

- One process, one extra hop. See [what it is not](#what-it-is-not).
- Read-modify-write is atomic only within one gateway process.
- No atomicity across keys, ever.
- No keyspace enumeration, so no `SCAN`, `KEYS`, `DBSIZE` or `FLUSHALL`.
- No data structures, transactions, scripting or Pub/Sub.
- The gateway keeps nothing, so it holds no expiry state, no keys and no
  replication state. Losing it loses no data; losing the *cluster* loses every
  value on every node, as the cluster's own guarantees describe.
- Command deadlines come from `--deadline-ms`, default 5 s. Redis has no such
  deadline; a proxy in front of a migrating cluster needs one. A command that
  passes it answers `TRYAGAIN`, so a long migration turns into client retries
  rather than silent timeouts.

## Not attempted

Recorded so the gap is deliberate rather than an oversight. In rough order of
how much each would cost:

| | What it would take |
| --- | --- |
| Keyspace enumeration | A range-enumeration RPC on the data node over the token index it already keeps, plus client-side fan-out and a cursor. The cheapest item here. |
| Hashes, sets and sorted sets | Encoding each as one record under one key. A single structure would then inherit the cluster's per-key linearizability, which is the right guarantee for one. The costs are a whole-structure rewrite per mutation and the 8 MiB value limit, which bites in the tens of thousands of members. |
| Lists and streams | Not a good fit. `LPOP` and consumer groups want O(1) element access and blocking reads across the cluster; a whole-structure rewrite does not give either. |
| `MULTI`, `EXEC`, `WATCH` | Would need server-side execution, and cross-key atomicity is unavailable underneath, so `EXEC` could not honestly promise atomicity across keys. |
| Durability | The cluster is in-memory by design; the gateway adds no storage. |
