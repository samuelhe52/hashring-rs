//! Scripts, as a client reaches them: `EVAL`, `EVALSHA` and `SCRIPT`.
//!
//! A script is the one thing a Redis client cannot do without. A cache client
//! claims a key with a read and a write that must not interleave, a rate
//! limiter adds one to a counter and reads its deadline, and neither client
//! library offers a way to say that in plain commands. Both send a script.
//!
//! What the gateway gives a script, and where it stops:
//!
//! - The script's commands are the gateway's own, so each is atomic in the way
//!   the gateway already makes it: a read-modify-write carries a precondition
//!   to the node that owns the key.
//! - A write the script makes is conditional on what the script read, so a
//!   script that reads its one key and then writes it cannot overwrite a write
//!   that arrived in between, whichever gateway or client made it. The script is
//!   then run again from the start, which is what a single-threaded Redis gets
//!   by never letting anyone in. A script that has already changed something a
//!   second run would change twice cannot be run again, and is failed instead:
//!   its answer would otherwise be a number no run of it produced.
//! - That guarantee covers a single key and no further, so a script declares at
//!   most one key. Two keys may sit on two owners, and a read of one cannot be
//!   made a precondition on a write to the other; a script that declares more
//!   than one is refused rather than answered with an atomicity the cluster
//!   cannot deliver. This is the same edge Redis's own cluster mode draws.
//! - A script may only touch the keys it declared in `KEYS`, checked against
//!   the command table's own key positions.
//! - One script at a time per gateway, holding its declared key for the whole
//!   run, and only the commands a script may run.
//!
//! `docs/redis-compatibility.md` describes this from the client's side.

use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
};

use hashring_core::proto::RecordVersion;
use tokio::sync::Mutex as AsyncMutex;

use super::{
    Connection, Dispatcher,
    args::{Args, keyword, parse_integer},
};
use crate::{
    error::{RedisError, RedisResult},
    resp::Reply,
    script::{self, Limits},
    table::CommandInfo,
};

/// The commands a script may not run: the ones that change the connection
/// rather than the data, and the ones that would start a script of their own
/// while this one holds the gateway.
const REFUSED: &[&str] = &[
    "auth", "hello", "reset", "select", "client", "quit", "eval", "evalsha", "script", "multi",
    "exec", "discard", "watch",
];

/// How many scripts a gateway keeps for `EVALSHA`. A client's scripts are its
/// own few texts, so the cache is small; it is bounded so that a client loading
/// scripts it never runs again cannot grow it without limit.
const CACHED_SCRIPTS: usize = 512;

/// Virtual-machine instructions a script may execute before it is stopped. A
/// cache key claim is two statements and a rate limiter's `INCR` with its
/// deadline is four more, so this is orders of magnitude above anything a
/// client library sends.
const MAX_STEPS: usize = 10_000_000;

/// The scripts this gateway has seen, and the lock that runs one at a time.
#[derive(Default)]
pub struct Scripts {
    /// One script at a time, as Redis runs them. Taken before the declared
    /// keys are locked, always in that order, so two scripts cannot deadlock.
    running: AsyncMutex<()>,
    sources: Mutex<Cached>,
    /// The Lua state every script runs in, taken out for the length of a run
    /// rather than borrowed, so holding it never waits on a lock that is held
    /// across a `.await`.
    runtime: Mutex<Option<script::Runtime>>,
}

#[derive(Default)]
pub(crate) struct Cached {
    by_hash: HashMap<[u8; 20], Arc<[u8]>>,
    /// The order scripts arrived in, so the oldest is the one to drop.
    order: VecDeque<[u8; 20]>,
}

impl Scripts {
    /// The scripts seen so far, which is what `SCRIPT EXISTS` and `SCRIPT
    /// FLUSH` read and write.
    pub(crate) fn cache(&self) -> std::sync::MutexGuard<'_, Cached> {
        self.sources.lock().expect("a script's own lock")
    }

    /// Keeps a script's source under the hash `EVALSHA` will ask for.
    pub fn remember(&self, source: &[u8]) -> Arc<[u8]> {
        self.sources
            .lock()
            .expect("a script's own lock")
            .remember(script::digest(source), source);
        Arc::from(source)
    }

    pub fn cached(&self, hash: [u8; 20]) -> Option<Arc<[u8]>> {
        self.sources
            .lock()
            .expect("a script's own lock")
            .source(hash)
    }

    /// The Lua state for the length of one run. It is taken out rather than
    /// borrowed so that no lock is held while the script runs.
    pub(crate) fn take_runtime(&self) -> script::Runtime {
        self.runtime
            .lock()
            .expect("a script's own lock")
            .take()
            .unwrap_or_default()
    }

    pub(crate) fn put_runtime(&self, runtime: script::Runtime) {
        *self.runtime.lock().expect("a script's own lock") = Some(runtime);
    }
}

impl Cached {
    /// Keeps a script's source under the hash `EVALSHA` will ask for, which is
    /// what Redis does with `EVAL` as well as with `SCRIPT LOAD`.
    fn remember(&mut self, hash: [u8; 20], source: &[u8]) {
        if self.by_hash.insert(hash, Arc::from(source)).is_none() {
            self.order.push_back(hash);
        }
        while self.order.len() > CACHED_SCRIPTS {
            if let Some(oldest) = self.order.pop_front() {
                self.by_hash.remove(&oldest);
            }
        }
    }

    fn source(&self, hash: [u8; 20]) -> Option<Arc<[u8]>> {
        self.by_hash.get(&hash).cloned()
    }

    fn knows(&self, hash: [u8; 20]) -> bool {
        self.by_hash.contains_key(&hash)
    }

    fn clear(&mut self) {
        self.by_hash.clear();
        self.order.clear();
    }
}

/// What a script read, so that the writes it makes can be conditional on it.
///
/// The lock inside is what lets this reach the [`super::store::Store`] that
/// every command handler takes by shared reference: a script's own reads and
/// writes happen on one task, and a lock that is never held across an await
/// costs nothing.
#[derive(Default)]
pub struct State {
    inner: Mutex<Read>,
}

#[derive(Default)]
struct Read {
    /// Each key's version when the script read it; `None` is a key the script
    /// read as absent.
    versions: HashMap<Vec<u8>, Option<RecordVersion>>,
    /// Whether the script has changed something a second run would change
    /// again.
    repeated: bool,
}

impl State {
    /// What the script saw of a key, or `None` when it never read it.
    pub fn version(&self, key: &[u8]) -> Option<Option<RecordVersion>> {
        self.inner
            .lock()
            .expect("a script's own lock")
            .versions
            .get(key)
            .cloned()
    }

    /// Notes what a key held, so a later write can require that it still does.
    pub fn record(&self, key: &[u8], version: Option<RecordVersion>) {
        self.inner
            .lock()
            .expect("a script's own lock")
            .versions
            .insert(key.to_vec(), version);
    }

    /// Notes a write that landed, for the write that follows it on the same key.
    pub fn written(&self, key: &[u8], version: RecordVersion) {
        self.record(key, Some(version));
    }

    /// A command whose effect a second run of this script would not repeat.
    pub fn mark_repeated(&self) {
        self.inner.lock().expect("a script's own lock").repeated = true;
    }

    pub fn repeated(&self) -> bool {
        self.inner.lock().expect("a script's own lock").repeated
    }

    /// Starts the next run of the same script, which starts from nothing.
    pub fn clear(&self) {
        let mut inner = self.inner.lock().expect("a script's own lock");
        inner.versions.clear();
        inner.repeated = false;
    }
}

/// `EVAL script numkeys key... arg...`, and `EVALSHA` with the hash in place of
/// the script.
pub async fn eval(
    dispatcher: &Arc<Dispatcher>,
    connection: &mut Connection,
    command: &CommandInfo,
    args: Args<'_>,
) -> RedisResult<Reply> {
    let rest = args.remaining();
    let Some((script, tail)) = rest.split_first() else {
        return Err(RedisError::syntax());
    };
    let source = match command.name {
        "evalsha" => {
            let source = dispatcher
                .scripts()
                .cached(digest(script)?)
                .ok_or_else(RedisError::no_such_script)?;
            dispatcher.remember(&source);
            source
        }
        _ => dispatcher.remember(script),
    };
    let (keys, argv) = keys_and_arguments(tail)?;
    run(dispatcher, connection, &source, keys, argv).await
}

/// `SCRIPT LOAD`, `SCRIPT EXISTS` and `SCRIPT FLUSH`.
pub fn script_command(dispatcher: &Dispatcher, mut args: Args<'_>) -> RedisResult<Reply> {
    let subcommand = args.next_keyword().ok_or_else(RedisError::syntax)?;
    let mut cache = dispatcher.scripts().cache();
    match subcommand.as_str() {
        "load" => {
            let source = args.next_owned()?;
            args.finish()?;
            let hash = script::hash_hex(&source);
            cache.remember(hash_of(&hash)?, &source);
            Ok(Reply::bulk(hash))
        }
        "exists" => Ok(Reply::Array(
            args.remaining()
                .iter()
                .map(|hash| match digest(hash) {
                    Ok(hash) => Reply::Integer(i64::from(cache.knows(hash))),
                    Err(error) => error.to_reply(),
                })
                .collect(),
        )),
        "flush" => {
            args.finish()?;
            cache.clear();
            Ok(Reply::status())
        }
        other => Err(RedisError::new(
            "ERR",
            format!("Unknown subcommand or wrong number of arguments for 'SCRIPT {other}'"),
        )),
    }
}

/// Runs one script, and runs it again if a write it made found the key changed
/// underneath it, which is the moment Redis's single thread would not have let
/// happen.
async fn run(
    dispatcher: &Arc<Dispatcher>,
    connection: &Connection,
    source: &[u8],
    keys: Vec<Vec<u8>>,
    argv: Vec<Vec<u8>>,
) -> RedisResult<Reply> {
    // A script is atomic here only over the one key it declared. The gateway
    // makes each of its commands atomic by carrying a precondition to the node
    // that owns the key, and a script that reads a key and then writes that
    // same key is stitched into one unit by running it again when the key
    // moved underneath it. That holds for a single key and no further: two keys
    // may sit on two owners, and a read of one cannot be made a precondition on
    // a write to the other. Rather than answer with a run whose atomicity the
    // cluster cannot deliver, a script that declares more than one key is
    // refused, which is the scope Redis's own cluster mode refuses too.
    if keys.len() > 1 {
        return Err(RedisError::new(
            "ERR",
            "a script may declare at most one key: the gateway runs a script atomically \
             over a single key, and cannot make a script that touches several keys \
             atomic when they may be owned by different nodes",
        ));
    }
    // One script at a time, then the keys it declared for as long as it runs.
    // The order is fixed so that two scripts cannot take each other's locks in
    // opposite orders.
    let _running = dispatcher.scripts().running.lock().await;
    let _held = dispatcher.lock_declared(&keys).await;
    let state = Arc::new(State::default());
    // The budget is rebuilt for each run, since a script that is run again
    // gets the whole of it, as it would have had the first time.
    let limits = || Limits {
        max_steps: MAX_STEPS,
        timeout: dispatcher.command_timeout(),
    };
    let mut runtime = dispatcher.scripts().take_runtime();
    loop {
        state.clear();
        // A script is given a connection of its own: everything it may run keeps
        // the connection as it found it, since the commands that would change
        // it are refused.
        let host = Host {
            dispatcher: dispatcher.clone(),
            connection: Connection {
                authenticated: true,
                ..connection.clone()
            },
            state: state.clone(),
        };
        let answer = runtime
            .run(host, source, keys.clone(), argv.clone(), limits())
            .await;
        dispatcher.scripts().put_runtime(runtime);
        match answer {
            Ok(reply) => return Ok(reply),
            // The script can only be run again if nothing it did would happen
            // twice. A conflict after a repeated change is reported instead,
            // because the answer the client would get is one no run produced.
            Err(error) if error.is_conflict() && !state.repeated() => {
                runtime = dispatcher.scripts().take_runtime();
                continue;
            }
            Err(error) => return Err(error),
        }
    }
}

/// The keys a script declared and the arguments it was given.
type Split = (Vec<Vec<u8>>, Vec<Vec<u8>>);

/// The keys and the arguments of `numkeys key... arg...`.
fn keys_and_arguments(tail: &[Vec<u8>]) -> RedisResult<Split> {
    let Some(count) = tail.first().and_then(|value| parse_integer(value)) else {
        return Err(RedisError::syntax());
    };
    if !(0..=(tail.len() as i64 - 1)).contains(&count) {
        return Err(RedisError::new(
            "ERR",
            "Number of keys can't be greater than number of args passed",
        ));
    }
    let count = count as usize;
    Ok((tail[1..1 + count].to_vec(), tail[1 + count..].to_vec()))
}

/// The bridge from the interpreter to the gateway: one command at a time,
/// through the same dispatcher a connection uses.
///
/// The interpreter has already checked the command against the script's `KEYS`
/// and against the command table, so what is left here is the gateway's own
/// policy: a command that would change the connection, and a command whose
/// effect a second run of this script would repeat.
struct Host {
    dispatcher: Arc<Dispatcher>,
    /// The connection a script sees, which is a copy of the client's rather
    /// than the client's itself: nothing a script may run changes a connection,
    /// so it has no business holding the real one while it runs.
    connection: Connection,
    state: Arc<State>,
}

impl script::Host for Host {
    fn call(&mut self, command: &[Vec<u8>]) -> impl Future<Output = RedisResult<Reply>> + Send {
        let dispatcher = self.dispatcher.clone();
        let state = self.state.clone();
        // The connection is copied rather than borrowed, so the future owns what
        // it needs and nothing is held across the call but the copy.
        let mut connection = self.connection.clone();
        async move {
            let name = command
                .first()
                .map(|name| keyword(name))
                .unwrap_or_default();
            if REFUSED.contains(&name.as_str()) {
                return Err(RedisError::new(
                    "ERR",
                    format!("This command is not allowed from a script: {name}"),
                ));
            }
            if !is_idempotent(&name) {
                state.mark_repeated();
            }
            dispatcher
                .dispatch(&mut connection, command, Some(&state))
                .await
                .reply
        }
    }
}

/// Commands whose effect a second run of the same script would not repeat.
/// Everything else a script may run writes what it writes again unchanged, so
/// a conflict can be answered by running the script once more.
fn is_idempotent(name: &str) -> bool {
    !matches!(
        name,
        "incr"
            | "decr"
            | "incrby"
            | "decrby"
            | "incrbyfloat"
            | "append"
            | "setrange"
            | "getset"
            | "getdel"
            | "rename"
            | "renamenx"
    )
}

/// The SHA-1 of a script as forty hex characters, read back into the twenty
/// bytes it names.
fn digest(hash: &[u8]) -> RedisResult<[u8; 20]> {
    let hash = std::str::from_utf8(hash).map_err(|_| malformed_hash())?;
    hash_of(hash)
}

fn hash_of(hash: &str) -> RedisResult<[u8; 20]> {
    let malformed = || malformed_hash();
    if hash.len() != 40 {
        return Err(malformed());
    }
    let mut digest = [0_u8; 20];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hash[index * 2..index * 2 + 2], 16).map_err(|_| malformed())?;
    }
    Ok(digest)
}

fn malformed_hash() -> RedisError {
    RedisError::new(
        "ERR",
        "SCRIPT LOAD and EVALSHA need the script's SHA-1 as forty hexadecimal characters",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_keys_come_before_the_arguments() {
        let tail: Vec<Vec<u8>> = ["2", "a", "b", "one", "two"]
            .iter()
            .map(|part| part.as_bytes().to_vec())
            .collect();
        let (keys, argv) = keys_and_arguments(&tail).unwrap();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(argv, vec![b"one".to_vec(), b"two".to_vec()]);
    }

    #[test]
    fn a_script_with_no_keys_may_carry_only_arguments() {
        let tail: Vec<Vec<u8>> = ["0", "one"].iter().map(|p| p.as_bytes().to_vec()).collect();
        let (keys, argv) = keys_and_arguments(&tail).unwrap();
        assert!(keys.is_empty());
        assert_eq!(argv, vec![b"one".to_vec()]);
    }

    #[test]
    fn a_key_count_the_arguments_do_not_cover_is_an_error() {
        for tail in [vec!["3"], vec!["-1"], vec!["x"], vec!["2", "a"]] {
            let tail: Vec<Vec<u8>> = tail.iter().map(|p| p.as_bytes().to_vec()).collect();
            assert!(keys_and_arguments(&tail).is_err(), "{tail:?}");
        }
    }

    #[test]
    fn a_hash_is_read_the_way_a_client_sends_it() {
        let source = b"return 1";
        let hex = script::hash_hex(source);
        assert_eq!(hash_of(&hex).unwrap(), script::digest(source));
        assert!(hash_of("too short").is_err());
        assert!(hash_of(&"z".repeat(40)).is_err());
        // Redis reads either case, and so does this.
        assert_eq!(
            hash_of(&hex.to_uppercase()).unwrap(),
            hash_of(&hex).unwrap()
        );
    }

    #[test]
    fn the_commands_a_second_run_would_repeat_are_named() {
        for name in ["incr", "append", "getdel", "rename"] {
            assert!(!is_idempotent(name), "{name}");
        }
        for name in ["get", "set", "setnx", "del", "expire", "ttl"] {
            assert!(is_idempotent(name), "{name}");
        }
    }
}
