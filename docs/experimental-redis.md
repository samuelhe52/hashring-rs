# Experimental Redis Lua gateway

**Purely experimental.** `hashring-redis-lua-experimental` adds embedded Lua 5.4
scripting to `hashring-redis`. Compatibility and resource isolation are incomplete;
this is not a production-ready Lua sandbox.

## Run

From the repository root, with a cluster running as described in the
[quick start](../README.md#quick-start):

```sh
cargo run --features redis-lua-experimental -- redis-lua-experimental-gateway \
  --config examples/experimental-redis-gateway.yaml
```

This adds the `redis-lua-experimental-gateway` command to the `hashring-rs` binary.
`redis-gateway` continues to use the ordinary gateway without scripting, even when
the feature is enabled. Default builds do not enable the Lua dependency. The
experimental crate is a library, not a separate executable; it is not published.

To build and run the binary directly instead:

```sh
cargo build --release --features redis-lua-experimental
./target/release/hashring-rs redis-lua-experimental-gateway \
  --config examples/experimental-redis-gateway.yaml
```

The coordinator is contacted when a command needs cluster data. The gateway can
start before the cluster; data commands then report a connection error or deadline.

## Try Lua

Using `redis-cli` in another terminal, against the example's default port:

```sh
# No keys: returns integer 7.
redis-cli -p 6379 EVAL 'return 1 + 2 * 3' 0

# One key: increment it and give it a 60-second expiry in one script.
redis-cli -p 6379 EVAL \
  "local n = redis.call('INCR', KEYS[1]); redis.call('EXPIRE', KEYS[1], 60); return n" \
  1 demo:counter
redis-cli -p 6379 GET demo:counter
redis-cli -p 6379 TTL demo:counter
```

After the script source, `EVAL` takes the number of keys, then those keys, then
any arguments exposed as `ARGV`. Zero or one key is supported. A script may only
access the key it declared in `KEYS`.

`SCRIPT LOAD '<source>'` returns a SHA-1 hash; `EVALSHA <hash> <numkeys> [key] [args...]`
runs a cached script. `SCRIPT EXISTS <hash>...` checks the cache and `SCRIPT FLUSH`
clears it. The cache holds up to 512 scripts per gateway process and is lost on
restart; an unknown hash produces `NOSCRIPT`, so clients must resend the source.

## Settings

The experimental command uses the same [settings as `redis-gateway`](configuration.md#redis-gateway), plus
`--max-bulk-bytes` (`max_bulk_bytes` in YAML). Its default is 16 MiB and the cluster
ceiling is 128 MiB. This limits each RESP bulk string, including a script's source
or one command argument. Nodes default to an 8 MiB value admission limit; raise
their `--max-value-bytes` separately when using larger values.

Flags override YAML, which overrides defaults. Authentication uses `--requirepass`,
`requirepass` in YAML, or `HASHRING_GATEWAY_PASSWORD`, just as the base gateway does.
See the [experimental command reference](configuration.md#redis-lua-experimental-gateway)
for the added setting and `--help` for all flags:

```sh
cargo run --features redis-lua-experimental -- redis-lua-experimental-gateway --help
```

For Rust callers, add a dependency on `hashring-redis-lua-experimental` and call
its `serve(GatewayConfig, shutdown_receiver)` function. The Cargo feature above
belongs to the root CLI package; depending directly on the library does not
require that CLI feature.

## Shared implementation

The experiment depends on `hashring-redis`. It shares the RESP parser, encoder,
connection loop, authentication, cluster client and ordinary command handlers.
The base gateway's `CommandExtension` API registers additional command metadata
and handlers. `COMMAND` includes registered commands. `serve_with_extension`
installs the extension using the existing server.

The generic `Overlay` interface lets an extension run ordinary handlers against
private state through `Dispatcher::execute_with_store`. Neither base interface
mentions Lua or depends on a Lua implementation.

Only the Lua runtime, script cache, scripting policy and single-key execution state
live in the experimental crate. `EVAL`, `EVALSHA` and `SCRIPT LOAD|EXISTS|FLUSH` are
registered there. The remaining command behavior is described in
[Redis compatibility](redis-compatibility.md).

## Single-key execution

An attempt reads its one declared key into a private snapshot. Commands read and
change that snapshot, so later commands see the attempt's own writes. A completed
attempt publishes its final state with one owner-side conditional write, against
the original record version (or absence). A conflict discards the private state
and reruns the whole script, including increments. Intermediate writes never
reach other gateways or native clients.

Scripts may declare at most one key and may access only that key. Keyspace walks,
connection changes and recursive scripting are refused. These are experiment
restrictions, not Redis Cluster's key rules. An error after successful commands
still commits their staged changes, matching Redis's lack of error rollback;
a commit with an unknown outcome is reported without replaying it.

The existing instruction hook and command deadline remain. They do not constitute
complete resource isolation, especially for script-created coroutines and library
allocations. Redis Lua conversion and library compatibility also remain incomplete.

## Development scope

Review this purely experimental functionality for P0 robustness defects and
P0/P1 correctness defects. Lower-priority robustness and correctness work is
outside the current scope. This does not change the review scope of the base
Redis gateway or shared cluster services.

## Validation

```sh
cargo test -p hashring-redis
cargo test -p hashring-redis-lua-experimental
cargo test --features redis-lua-experimental --test redis_gateway --test experimental_redis_gateway
```

The existing RSSHub-style Lua cache-claim regression is preserved in
[`tests/experimental_redis_gateway.rs`](../tests/experimental_redis_gateway.rs).
It races eight callers across two gateways and requires exactly one successful
claim, and also exercises a rate-limiter script. Run it specifically with:

```sh
cargo test --features redis-lua-experimental --test experimental_redis_gateway \
  a_script_is_run_the_way_a_client_library_sends_it -- --exact
```

This test validates the Lua scripts through a real cluster and Redis client.
Full RSSHub application integration requires a separate RSSHub deployment.
