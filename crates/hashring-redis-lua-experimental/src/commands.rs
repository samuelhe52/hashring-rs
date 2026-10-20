//! Experimental `EVAL`, `EVALSHA` and `SCRIPT` support.
//!
//! Each attempt runs against a private snapshot of its one declared key.
//! Commands see their own staged writes. The final state is committed once,
//! conditional on the original version; a failed condition discards the attempt
//! and reruns the entire script. No intermediate write reaches the cluster.
//! See `docs/experimental-redis.md` for the experimental scope and limitations.

use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
};

use hashring_client::{Expiry, HashringClient, Precondition};
use hashring_core::{
    limits::MAX_VALUE_BYTES,
    proto::{ErrorCode, RecordVersion},
};
use tokio::sync::Mutex as AsyncMutex;

use futures::future::BoxFuture;
use hashring_redis::commands::{
    Args, CommandExtension, Connection, Dispatcher, keyword, parse_integer,
    store::{Current, Overlay},
};

fn expiry(deadline: Option<u64>) -> Expiry {
    deadline.map_or(Expiry::Clear, Expiry::At)
}

fn no_such_script() -> RedisError {
    RedisError::new("NOSCRIPT", "No matching script. Please use EVAL.")
}
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
    "auth",
    "hello",
    "reset",
    "select",
    "client",
    "quit",
    "eval",
    "evalsha",
    "script",
    "multi",
    "exec",
    "discard",
    "watch",
    "keys",
    "scan",
    "dbsize",
    "randomkey",
];

const COMMANDS: &[CommandInfo] = &[
    CommandInfo {
        name: "eval",
        arity: -3,
        flags: &["write"],
        first_key: 0,
        last_key: 0,
        step: 0,
        declined: false,
        keyspace: false,
    },
    CommandInfo {
        name: "evalsha",
        arity: -3,
        flags: &["write"],
        first_key: 0,
        last_key: 0,
        step: 0,
        declined: false,
        keyspace: false,
    },
    CommandInfo {
        name: "script",
        arity: -2,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        step: 0,
        declined: false,
        keyspace: false,
    },
];

impl CommandExtension for Scripts {
    fn commands(&self) -> &'static [CommandInfo] {
        COMMANDS
    }

    fn execute<'a>(
        &'a self,
        dispatcher: &'a Dispatcher,
        connection: &'a mut Connection,
        command: &'a CommandInfo,
        arguments: &'a [Vec<u8>],
    ) -> BoxFuture<'a, RedisResult<Reply>> {
        Box::pin(async move {
            let args = Args::new(command.name, arguments);
            match command.name {
                "script" => script_command(self, args),
                _ => eval(self, dispatcher, connection, command, args).await,
            }
        })
    }
}

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
    /// One script at a time per extension. Cross-gateway exclusion is supplied
    /// by the final conditional commit, rather than a local key lock.
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

/// One attempt's private view of its declared key. The original version is
/// immutable; staged writes only replace `current`.
pub struct State {
    key: Option<Vec<u8>>,
    inner: Mutex<Snapshot>,
}

#[derive(Clone)]
struct Snapshot {
    expected: Option<RecordVersion>,
    current: Option<Current>,
    dirty: bool,
}

impl State {
    fn new(key: Option<Vec<u8>>, current: Option<Current>) -> Self {
        Self {
            key,
            inner: Mutex::new(Snapshot {
                expected: current.as_ref().map(|value| value.version.clone()),
                current,
                dirty: false,
            }),
        }
    }

    fn check_key(&self, key: &[u8]) -> RedisResult<()> {
        if self.key.as_deref() != Some(key) {
            return Err(RedisError::new(
                "ERR",
                "a script may only touch its declared key",
            ));
        }
        Ok(())
    }

    pub fn read(&self, key: &[u8]) -> RedisResult<Option<Current>> {
        self.check_key(key)?;
        Ok(self
            .inner
            .lock()
            .expect("a script's own lock")
            .current
            .clone())
    }

    /// Only this operation can publish a script's changes. A failed condition
    /// writes nothing, so the complete attempt can safely be repeated.
    async fn commit(&self, client: &HashringClient) -> RedisResult<bool> {
        let snapshot = self.inner.lock().expect("a script's own lock").clone();
        if !snapshot.dirty {
            return Ok(true);
        }
        let key = self.key.clone().expect("a staged write has a declared key");
        let expected = snapshot
            .expected
            .map_or(Precondition::Absent, Precondition::Version);
        Ok(match snapshot.current {
            Some(current) => client
                .put_if(key, current.value, expiry(current.deadline), expected)
                .await?
                .is_some(),
            None => client.delete_if(key, expected).await?.is_some(),
        })
    }
}

impl Overlay for State {
    fn read(&self, key: &[u8]) -> RedisResult<Option<Current>> {
        State::read(self, key)
    }

    fn write(&self, key: &[u8], value: Option<Current>) -> RedisResult<()> {
        self.check_key(key)?;
        if value
            .as_ref()
            .is_some_and(|current| current.value.len() > MAX_VALUE_BYTES)
        {
            return Err(RedisError::new("ERR", "value exceeds the cluster ceiling"));
        }
        let mut snapshot = self.inner.lock().expect("a script's own lock");
        snapshot.current = value;
        snapshot.dirty = true;
        Ok(())
    }
}

/// `EVAL script numkeys key... arg...`, and `EVALSHA` with the hash in place of
/// the script.
pub async fn eval(
    scripts: &Scripts,
    dispatcher: &Dispatcher,
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
            let source = scripts.cached(digest(script)?).ok_or_else(no_such_script)?;
            scripts.remember(&source);
            source
        }
        _ => scripts.remember(script),
    };
    let (keys, argv) = keys_and_arguments(tail)?;
    run(scripts, dispatcher, connection, &source, keys, argv).await
}

/// `SCRIPT LOAD`, `SCRIPT EXISTS` and `SCRIPT FLUSH`.
pub fn script_command(scripts: &Scripts, mut args: Args<'_>) -> RedisResult<Reply> {
    let subcommand = args.next_keyword().ok_or_else(RedisError::syntax)?;
    let mut cache = scripts.cache();
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

/// Run a private single-key attempt and publish its final state once.
async fn run(
    scripts: &Scripts,
    dispatcher: &Dispatcher,
    connection: &Connection,
    source: &[u8],
    keys: Vec<Vec<u8>>,
    argv: Vec<Vec<u8>>,
) -> RedisResult<Reply> {
    if keys.len() > 1 {
        return Err(RedisError::new(
            "ERR",
            "a script may declare at most one key",
        ));
    }
    let _running = scripts.running.lock().await;

    let deadline = tokio::time::Instant::now() + dispatcher.command_timeout();
    loop {
        let client = match keys.first() {
            Some(_) => Some(dispatcher.client.get().await?),
            None => None,
        };
        let current = match keys.first() {
            Some(key) => match client
                .as_ref()
                .expect("a declared key has a client")
                .get(key.clone())
                .await
            {
                Ok(value) => Some(Current {
                    value: value.value,
                    deadline: value.expires_at_unix_millis,
                    version: value.version,
                }),
                Err(hashring_client::ClientError::Operation(failure))
                    if failure.code == ErrorCode::NotFound =>
                {
                    None
                }
                Err(error) => return Err(error.into()),
            },
            None => None,
        };
        let state = Arc::new(State::new(keys.first().cloned(), current));
        let host = Host {
            dispatcher: dispatcher.clone(),
            connection: Connection {
                authenticated: true,
                ..connection.clone()
            },
            state: state.clone(),
        };
        let limits = Limits {
            max_steps: MAX_STEPS,
            timeout: deadline.saturating_duration_since(tokio::time::Instant::now()),
        };
        let mut runtime = scripts.take_runtime();
        let answer = runtime
            .run(host, source, keys.clone(), argv.clone(), limits)
            .await;
        scripts.put_runtime(runtime);
        // Like Redis, an error after successful commands does not roll those
        // commands back. Their staged effects still publish in one commit.
        if let Some(client) = client
            && !state.commit(&client).await?
        {
            tokio::task::yield_now().await;
            continue;
        }
        return answer;
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
/// policy: commands that change the connection or bypass the private snapshot
/// are refused.
struct Host {
    dispatcher: Dispatcher,
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
            dispatcher
                .execute_with_store(&mut connection, command, Some(state.as_ref()))
                .await
                .reply
        }
    }
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
}
