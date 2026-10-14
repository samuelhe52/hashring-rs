# Request id generation without a syscall per operation

Every client `get`, `put` and `delete` carries a request id that the owner uses
as the mutation's deduplication key. The client made it with
`uuid::Uuid::new_v4().to_string()`, which reads the OS random source on every
call. On macOS that is a `getentropy` syscall, and CPU profiles of the
experiment driver put it at 4.5-7% of the driver's on-CPU time
([grpc-io-bottleneck.md](grpc-io-bottleneck.md)). The driver is the bottleneck
for RF=1 writes and for all reads on one host.

## Change

`new_request_id()` in `crates/hashring-client/src/lib.rs` formats a random
`u128` from `rand`'s thread-local generator as 32 hex characters. That
generator is a ChaCha CSPRNG seeded from the OS, reseeding after every 64 KiB
of output (about 4,000 ids), so almost no call makes a syscall.

- **Uniqueness:** 128 random bits against a v4 UUID's 122.
- **Format:** nothing parses request ids as UUIDs. Nodes only check that an id
  has 1-128 bytes.
- **Size:** ids shrink from 36 to 32 bytes, which slightly reduces the
  deduplication receipt and journal bytes each mutation is charged.
- **Dependency:** the client no longer depends on `uuid`. Nodes still use it for
  process instance ids, once per process.

## Measurement

**Isolated cost** ([bench.rs](data/request-id-generation/bench.rs), single
thread, Apple M1 Pro, 2 million ids per round, three rounds): `Uuid::new_v4()`
plus `to_string()` takes 855-891 ns per id, and the `rand` hex id takes 65 ns,
13x less.

**End to end:** performance mode, 6 nodes, 300,000 keys, 128-byte values, base
commit `4ec7a0c` against this change. Five repetitions, each running the before
and after binaries back to back, on an otherwise idle host. Medians. Driver
CPU per operation is the driver's sampled user and system time during the
phase divided by its operations (`scripts/perf/run.sh`). Raw data:
[runs.tsv](data/request-id-generation/runs.tsv).

| configuration | PUT ops/s | GET ops/s | driver CPU per PUT | driver CPU per GET |
| --- | --- | --- | --- | --- |
| RF=1, concurrency 64 | 75,196 -> 76,595 (+1.9%) | 77,595 -> 80,289 (+3.5%) | 26.7 -> 26.2 µs (-2.0%) | 30.2 -> 26.9 µs (-11%) |
| RF=1, concurrency 256 | 87,651 -> 94,764 (+8.1%) | 90,625 -> 102,380 (+13%) | 22.6 -> 21.1 µs (-6.8%) | 24.5 -> 21.8 µs (-11%) |
| RF=3, concurrency 64 | 58,448 -> 59,587 (+1.9%) | 75,519 -> 77,059 (+2.0%) | 26.7 -> 26.1 µs (-2.5%) | 28.7 -> 27.0 µs (-6.1%) |

The RF=1, concurrency 64 PUT ranges do not overlap: 74,433-75,417 before
against 76,357-78,437 after. The system-time share of all PUT-phase CPU fell
by 1-3 points in every configuration, for example from 39% to 36% at RF=1,
concurrency 256.

Under load the saving exceeds the isolated cost: about 3 µs of driver CPU per
`GET` against 0.8 µs measured alone. A syscall is costlier when every core is
busy, which matches the profile's 4.5-7% share. The gain is largest where the
driver is the bottleneck (RF=1 and reads, especially at concurrency 256). At
RF=3 the write path is limited by node-side RPC latency instead, so it gains
least.

## Notes

A first attempt at this measurement ran while an Xcode build used 60-77% of a
core per process, and paired runs moved by up to 2x in either direction. It is
not used here.
