//! The commands this gateway claims, and how much of each one it implements.
//!
//! `COMMAND` answers from this table, so a client that asks what the server
//! supports sees the same list the dispatcher dispatches on. Arity and key
//! positions follow Redis, because clients use them to check a command before
//! sending it and to split a multi-key command across a cluster.

use crate::resp::Reply;

/// One command as `COMMAND` describes it.
pub struct CommandInfo {
    pub name: &'static str,
    /// Negative means "at least this many arguments", as in Redis.
    pub arity: i64,
    pub flags: &'static [&'static str],
    pub first_key: i64,
    pub last_key: i64,
    pub step: i64,
}

const READ: &[&str] = &["readonly", "fast"];
const WRITE: &[&str] = &["write", "denyoom"];
const ADMIN: &[&str] = &["admin", "no_auth", "loading", "stale"];
const LOCAL: &[&str] = &["loading", "stale", "fast"];

const fn command(name: &'static str, arity: i64, flags: &'static [&'static str]) -> CommandInfo {
    CommandInfo {
        name,
        arity,
        flags,
        first_key: 0,
        last_key: 0,
        step: 0,
    }
}

const fn keyed(
    name: &'static str,
    arity: i64,
    flags: &'static [&'static str],
    first_key: i64,
    last_key: i64,
    step: i64,
) -> CommandInfo {
    CommandInfo {
        name,
        arity,
        flags,
        first_key,
        last_key,
        step,
    }
}

/// Every command this gateway answers, and the commands it knows but declines.
pub const COMMANDS: &[CommandInfo] = &[
    // Connection and administration. These answer from the gateway itself and
    // never reach the cluster.
    command("ping", -1, &["fast"]),
    command("echo", 2, READ),
    command("hello", -1, &["no_auth"]),
    command("auth", -2, &["no_auth", "loading", "stale"]),
    command("select", 2, LOCAL),
    command("client", -2, &["no_auth"]),
    command("reset", 1, &["no_auth", "loading", "stale"]),
    command("quit", -1, &["no_auth", "loading", "stale"]),
    command("command", -1, LOCAL),
    command("info", -1, LOCAL),
    command("config", -2, ADMIN),
    // Strings. Every value in this gateway is a byte string.
    keyed("get", 2, READ, 1, 1, 1),
    keyed("getdel", 2, WRITE, 1, 1, 1),
    keyed("getex", -2, WRITE, 1, 1, 1),
    keyed("getrange", 4, READ, 1, 1, 1),
    keyed("set", -3, WRITE, 1, 1, 1),
    keyed("setex", 4, WRITE, 1, 1, 1),
    keyed("psetex", 4, WRITE, 1, 1, 1),
    keyed("setnx", 3, WRITE, 1, 1, 1),
    keyed("setrange", 4, WRITE, 1, 1, 1),
    keyed("mset", -3, WRITE, 1, -1, 2),
    keyed("msetnx", -3, WRITE, 1, -1, 2),
    keyed("mget", -2, READ, 1, -1, 1),
    keyed("getset", 3, WRITE, 1, 1, 1),
    keyed("append", 3, WRITE, 1, 1, 1),
    keyed("strlen", 2, READ, 1, 1, 1),
    keyed("incr", 2, WRITE, 1, 1, 1),
    keyed("decr", 2, WRITE, 1, 1, 1),
    keyed("incrby", 3, WRITE, 1, 1, 1),
    keyed("decrby", 3, WRITE, 1, 1, 1),
    keyed("incrbyfloat", 3, WRITE, 1, 1, 1),
    // Keys.
    keyed("del", -2, WRITE, 1, -1, 1),
    keyed("unlink", -2, &["write", "fast"], 1, -1, 1),
    keyed("exists", -2, READ, 1, -1, 1),
    keyed("touch", -2, READ, 1, -1, 1),
    keyed("type", 2, READ, 1, 1, 1),
    keyed("rename", 3, WRITE, 1, 2, 1),
    keyed("renamenx", 3, WRITE, 1, 2, 1),
    // Expiry.
    keyed("expire", -3, WRITE, 1, 1, 1),
    keyed("pexpire", -3, WRITE, 1, 1, 1),
    keyed("expireat", -3, WRITE, 1, 1, 1),
    keyed("pexpireat", -3, WRITE, 1, 1, 1),
    keyed("persist", 2, WRITE, 1, 1, 1),
    keyed("ttl", 2, READ, 1, 1, 1),
    keyed("pttl", 2, READ, 1, 1, 1),
    // Declined. Listed so that a client asking what the server knows is told
    // the command exists and was refused, rather than that the server does not
    // exist. See docs/redis-compatibility.md for why each one is absent.
    command("scan", -2, &["readonly", "random"]),
    command("keys", 2, &["readonly", "random"]),
    command("dbsize", 1, &["readonly", "fast"]),
    command("flushdb", -1, WRITE),
    command("flushall", -1, WRITE),
    command("randomkey", 1, &["readonly", "random"]),
];

pub fn lookup(name: &str) -> Option<&'static CommandInfo> {
    COMMANDS
        .iter()
        .find(|command| command.name.eq_ignore_ascii_case(name))
}

impl CommandInfo {
    pub fn to_reply(&self) -> Reply {
        Reply::Array(vec![
            Reply::bulk(self.name),
            Reply::Integer(self.arity),
            Reply::Array(self.flags.iter().map(|flag| Reply::bulk(*flag)).collect()),
            Reply::Integer(self.first_key),
            Reply::Integer(self.last_key),
            Reply::Integer(self.step),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_command_the_dispatcher_handles_is_in_the_table() {
        for name in crate::commands::HANDLED {
            assert!(
                lookup(name).is_some(),
                "{name} is dispatched but missing from the command table"
            );
        }
    }

    #[test]
    fn multi_key_commands_report_their_key_positions() {
        let mget = lookup("mget").unwrap();
        assert_eq!((mget.first_key, mget.last_key, mget.step), (1, -1, 1));
        let mset = lookup("mset").unwrap();
        assert_eq!((mset.first_key, mset.last_key, mset.step), (1, -1, 2));
        let rename = lookup("rename").unwrap();
        assert_eq!((rename.first_key, rename.last_key), (1, 2));
        assert_eq!(lookup("get").unwrap().first_key, 1);
        assert_eq!(lookup("ping").unwrap().first_key, 0);
    }

    #[test]
    fn a_command_reply_matches_the_redis_shape() {
        let Reply::Array(items) = lookup("get").unwrap().to_reply() else {
            panic!("expected an array");
        };
        assert_eq!(items.len(), 6);
        assert!(matches!(&items[0], Reply::Bulk(Some(name)) if name == b"get"));
    }
}
