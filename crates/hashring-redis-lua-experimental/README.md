# hashring-redis-lua-experimental

Purely experimental Lua 5.4 scripting for `hashring-redis`, composed through its
Lua-agnostic extension API. The original gateway supplies the protocol, server
and ordinary command implementations; this crate does not fork them.

See [the experiment documentation](../../docs/experimental-redis.md) for usage,
single-key commit semantics, limitations and the current review scope.
