//! Command dispatch: a Redis command in, a Redis reply out.
//!
//! Every request is checked against [`crate::table`] before a handler sees it:
//! the command must exist and not be declined, its arity must match, and the
//! connection must have authenticated unless the command is part of the
//! handshake. A handler then reads its arguments through [`args::Args`].
//!
//! Commands that answer from the gateway live in [`connection`]; the ones that
//! reach the cluster live in [`strings`], [`keys`] and [`keyspace`] and go
//! through [`store::Store`], which routes each key to its owner through
//! [`hashring_client`] and makes every read-modify-write conditional on the
//! version it read. See docs/redis-compatibility.md for the semantics this
//! reproduces and the ones it deliberately does not.

mod args;
mod connection;
mod glob;
mod keys;
mod keyspace;
pub mod store;
mod strings;

use std::{sync::Arc, time::Duration};

use crate::{
    client::{KeyLocks, SharedClient},
    error::{RedisError, RedisResult},
    resp::{Limits, Protocol, Reply},
    table::{self, CommandInfo},
};

pub use args::{Args, keyword, now_millis, parse_integer};
use futures::future::BoxFuture;

/// Registers additional commands without changing the gateway's transport.
/// The dispatcher applies metadata validation, authentication and the deadline
/// before invoking an extension. Ordinary commands use the existing handlers.
pub trait CommandExtension: Send + Sync {
    fn commands(&self) -> &'static [CommandInfo];
    fn execute<'a>(
        &'a self,
        dispatcher: &'a Dispatcher,
        connection: &'a mut Connection,
        command: &'a CommandInfo,
        arguments: &'a [Vec<u8>],
    ) -> BoxFuture<'a, RedisResult<Reply>>;
}

/// The gateway, shared by every connection it serves.
#[derive(Clone)]
pub struct Dispatcher {
    pub client: Arc<SharedClient>,
    locks: Arc<KeyLocks>,
    password: Option<Arc<str>>,
    limits: Limits,
    /// How long one command that reaches the cluster may take, from its first
    /// lock wait or request to its last.
    command_timeout: Duration,
    started_at: std::time::Instant,
    extension: Option<Arc<dyn CommandExtension>>,
}

/// What one command produced: the reply, and what it does to the connection
/// once that reply is settled. An effect applies only when the command
/// succeeded, so `CLIENT REPLY BOGUS` changes nothing.
pub struct Response {
    pub reply: RedisResult<Reply>,
    pub effect: Effect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Effect {
    None,
    /// `QUIT`: answer, then close.
    Close,
    /// `CLIENT REPLY`: switch the reply mode. Redis answers `ON` with `OK` and
    /// the other two with nothing at all.
    ReplyMode(ReplyMode),
}

impl Response {
    fn reply(reply: RedisResult<Reply>) -> Self {
        Self {
            reply,
            effect: Effect::None,
        }
    }

    fn error(error: RedisError) -> Self {
        Self::reply(Err(error))
    }
}

impl Dispatcher {
    pub fn new(
        client: Arc<SharedClient>,
        password: Option<Arc<str>>,
        limits: Limits,
        command_timeout: Duration,
    ) -> Self {
        Self {
            client,
            locks: Arc::new(KeyLocks::new()),
            password,
            limits,
            command_timeout,
            started_at: std::time::Instant::now(),
            extension: None,
        }
    }

    /// Install additional command handlers before sharing the dispatcher.
    pub fn with_extension(mut self, extension: Arc<dyn CommandExtension>) -> Self {
        self.extension = Some(extension);
        self
    }

    pub fn command_timeout(&self) -> Duration {
        self.command_timeout
    }

    /// Registered commands take precedence over the built-in metadata.
    pub fn command(&self, name: &str) -> Option<&'static CommandInfo> {
        self.extension
            .as_ref()
            .and_then(|extension| {
                extension
                    .commands()
                    .iter()
                    .find(|command| command.name.eq_ignore_ascii_case(name))
            })
            .or_else(|| table::lookup(name))
    }

    pub fn commands(&self) -> impl Iterator<Item = &'static CommandInfo> + '_ {
        let extra = self
            .extension
            .as_ref()
            .map_or(&[][..], |extension| extension.commands());
        table::COMMANDS
            .iter()
            .filter(move |command| !extra.iter().any(|item| item.name == command.name))
            .chain(extra.iter())
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// Whether a connection has to authenticate before it may read or write.
    pub fn requires_auth(&self) -> bool {
        self.password.is_some()
    }

    /// Runs one command. `connection` carries everything specific to the
    /// socket: the negotiated protocol, the name the client gave itself, and
    /// whether it has authenticated.
    pub async fn execute(&self, connection: &mut Connection, args: &[Vec<u8>]) -> Response {
        self.execute_with_store(connection, args, None).await
    }

    /// Execute existing handlers against an optional private store. Extensions
    /// can compose ordinary commands without publishing intermediate changes.
    pub async fn execute_with_store(
        &self,
        connection: &mut Connection,
        args: &[Vec<u8>],
        overlay: Option<&dyn store::Overlay>,
    ) -> Response {
        let Some((name, rest)) = args.split_first() else {
            return Response::error(RedisError::new("ERR", "empty multibulk"));
        };
        let name = keyword(name);
        let Some(command) = self.command(&name) else {
            return Response::error(RedisError::new(
                "ERR",
                format!("unknown command '{name}', with args beginning with: "),
            ));
        };
        // A declined command is refused by name however it was called, so a
        // client learns it is unsupported rather than that it miscounted.
        if !command.declined && !command.accepts(args.len()) {
            return Response::error(RedisError::wrong_arity(command.name));
        }
        if self.password.is_some() && !connection.authenticated && !command.pre_auth() {
            return Response::error(RedisError::no_auth());
        }
        if command.declined {
            return Response::error(RedisError::unsupported(&command.name.to_ascii_uppercase()));
        }
        if let Some(extension) = &self.extension
            && extension
                .commands()
                .iter()
                .any(|item| item.name == command.name)
        {
            return match tokio::time::timeout(
                self.command_timeout,
                extension.execute(self, connection, command, rest),
            )
            .await
            {
                Ok(reply) => Response::reply(reply),
                Err(_) => Response::error(RedisError::deadline(command.writes())),
            };
        }
        if overlay.is_some() && command.keyspace {
            return Response::error(RedisError::new(
                "ERR",
                "keyspace commands cannot use a private store",
            ));
        }
        let args = args::Args::new(command.name, rest);
        if !command.reaches_cluster() {
            return self.local(command, connection, args);
        }
        match tokio::time::timeout(self.command_timeout, self.cluster(command, args, overlay)).await
        {
            Ok(reply) => Response::reply(reply),
            Err(_) => Response::error(RedisError::deadline(command.writes())),
        }
    }

    async fn cluster(
        &self,
        command: &CommandInfo,
        args: args::Args<'_>,
        overlay: Option<&dyn store::Overlay>,
    ) -> RedisResult<Reply> {
        let store = store::Store::new(&self.client, &self.locks).with_overlay(overlay);
        let max_value = self.limits.max_bulk_bytes;
        use args::{DeadlineMode::*, TimeUnit::*, TtlForm};
        match command.name {
            "get" => strings::get(&store, args).await,
            "getdel" => strings::getdel(&store, args).await,
            "getex" => strings::getex(&store, args).await,
            "getrange" => strings::getrange(&store, args).await,
            "set" => strings::set(&store, args).await,
            "setex" => strings::set_with_ttl(&store, args, Seconds).await,
            "psetex" => strings::set_with_ttl(&store, args, Milliseconds).await,
            "setnx" => strings::setnx(&store, args).await,
            "setrange" => strings::setrange(&store, args, max_value).await,
            "mset" => strings::mset(&store, args).await,
            "msetnx" => strings::msetnx(&store, args).await,
            "mget" => strings::mget(&store, args).await,
            "getset" => strings::getset(&store, args).await,
            "append" => strings::append(&store, args, max_value).await,
            "strlen" => strings::strlen(&store, args).await,
            "incr" => strings::incr_by(&store, args, Some(1)).await,
            "decr" => strings::incr_by(&store, args, Some(-1)).await,
            "incrby" | "decrby" => strings::incr_by(&store, args, None).await,
            "incrbyfloat" => strings::incrbyfloat(&store, args).await,
            "del" | "unlink" => keys::delete(&store, args).await,
            "exists" | "touch" => keys::exists(&store, args).await,
            "type" => keys::key_type(&store, args).await,
            "rename" => keys::rename(&store, args, false).await,
            "renamenx" => keys::rename(&store, args, true).await,
            "expire" => keys::expire(&store, args, TtlForm::new(Seconds, Relative)).await,
            "pexpire" => keys::expire(&store, args, TtlForm::new(Milliseconds, Relative)).await,
            "expireat" => keys::expire(&store, args, TtlForm::new(Seconds, Absolute)).await,
            "pexpireat" => keys::expire(&store, args, TtlForm::new(Milliseconds, Absolute)).await,
            "persist" => keys::persist(&store, args).await,
            "ttl" => keys::ttl(&store, args, Seconds).await,
            "pttl" => keys::ttl(&store, args, Milliseconds).await,
            "keys" => keyspace::keys(&store, args).await,
            "scan" => keyspace::scan(&store, args).await,
            "dbsize" => keyspace::dbsize(&store).await,
            "randomkey" => keyspace::randomkey(&store).await,
            other => Err(RedisError::unsupported(&other.to_ascii_uppercase())),
        }
    }
}

/// Everything about one client connection that survives between commands.
#[derive(Clone)]
pub struct Connection {
    pub id: u64,
    pub protocol: Protocol,
    pub authenticated: bool,
    pub name: Option<Vec<u8>>,
    pub reply_mode: ReplyMode,
}

/// Whether a command's reply is written back.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplyMode {
    On,
    /// `CLIENT REPLY OFF`: nothing is written until the mode changes back.
    Off,
    /// `CLIENT REPLY SKIP`: the next reply is dropped, then the mode is back on.
    Skip,
}

impl Connection {
    pub fn new(id: u64, authenticated: bool) -> Self {
        Self {
            id,
            protocol: Protocol::Resp2,
            authenticated,
            name: None,
            reply_mode: ReplyMode::On,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ClientConfig;

    struct Probe;

    impl CommandExtension for Probe {
        fn commands(&self) -> &'static [CommandInfo] {
            &[CommandInfo {
                name: "probe",
                arity: 2,
                flags: &[],
                first_key: 0,
                last_key: 0,
                step: 0,
                declined: false,
                keyspace: false,
            }]
        }

        fn execute<'a>(
            &'a self,
            _dispatcher: &'a Dispatcher,
            _connection: &'a mut Connection,
            _command: &'a CommandInfo,
            arguments: &'a [Vec<u8>],
        ) -> BoxFuture<'a, RedisResult<Reply>> {
            Box::pin(async move {
                if arguments[0] == b"wait" {
                    std::future::pending::<()>().await;
                }
                Ok(Reply::bulk(arguments[0].clone()))
            })
        }
    }

    #[tokio::test]
    async fn extensions_share_validation_authentication_metadata_and_deadlines() {
        let dispatcher = dispatcher(Some("secret")).with_extension(Arc::new(Probe));
        assert!(
            error(&dispatcher, &["probe"])
                .await
                .contains("wrong number")
        );
        assert!(
            error(&dispatcher, &["probe", "ok"])
                .await
                .starts_with("NOAUTH")
        );
        let mut connection = Connection::new(1, false);
        dispatcher
            .execute(&mut connection, &args(&["AUTH", "secret"]))
            .await
            .reply
            .unwrap();
        assert_eq!(
            dispatcher
                .execute(&mut connection, &args(&["probe", "ok"]))
                .await
                .reply
                .unwrap(),
            Reply::bulk("ok")
        );
        assert_eq!(
            dispatcher
                .execute(&mut connection, &args(&["PING"]))
                .await
                .reply
                .unwrap(),
            Reply::Simple("PONG".into())
        );
        let metadata = dispatcher
            .execute(&mut connection, &args(&["COMMAND", "INFO", "probe"]))
            .await
            .reply
            .unwrap();
        assert_eq!(
            metadata,
            Reply::Array(vec![dispatcher.command("probe").unwrap().to_reply()])
        );
        let error = dispatcher
            .execute(&mut connection, &args(&["probe", "wait"]))
            .await
            .reply
            .unwrap_err();
        assert_eq!(error.code, "TRYAGAIN");
    }

    #[tokio::test]
    async fn ordinary_handlers_can_run_against_a_private_store_without_cluster_access() {
        #[derive(Default)]
        struct Memory(std::sync::Mutex<std::collections::HashMap<Vec<u8>, store::Current>>);
        impl store::Overlay for Memory {
            fn read(&self, key: &[u8]) -> RedisResult<Option<store::Current>> {
                Ok(self.0.lock().unwrap().get(key).cloned())
            }
            fn write(&self, key: &[u8], value: Option<store::Current>) -> RedisResult<()> {
                let mut values = self.0.lock().unwrap();
                match value {
                    Some(value) => {
                        values.insert(key.to_vec(), value);
                    }
                    None => {
                        values.remove(key);
                    }
                }
                Ok(())
            }
        }
        let dispatcher = dispatcher(None);
        let memory = Memory::default();
        let mut connection = Connection::new(1, true);
        for (parts, expected) in [
            (vec!["SET", "key", "2"], Reply::status()),
            (vec!["INCR", "key"], Reply::Integer(3)),
            (vec!["GET", "key"], Reply::bulk("3")),
            (vec!["GETDEL", "key"], Reply::bulk("3")),
            (vec!["MSETNX", "key", "fresh"], Reply::Integer(1)),
            (vec!["MSETNX", "key", "other"], Reply::Integer(0)),
            (vec!["GET", "key"], Reply::bulk("fresh")),
        ] {
            let response = dispatcher
                .execute_with_store(&mut connection, &args(&parts), Some(&memory))
                .await;
            assert_eq!(response.reply.unwrap(), expected, "{parts:?}");
        }
        let error = dispatcher
            .execute_with_store(
                &mut connection,
                &args(&["RENAME", "key", "other"]),
                Some(&memory),
            )
            .await
            .reply
            .unwrap_err();
        assert!(error.message.contains("direct cluster access"));
    }

    fn dispatcher(password: Option<&str>) -> Dispatcher {
        Dispatcher::new(
            Arc::new(SharedClient::new(
                "http://127.0.0.1:1".into(),
                ClientConfig::new(Duration::from_millis(50)),
            )),
            password.map(Arc::from),
            Limits::default(),
            Duration::from_millis(200),
        )
    }

    fn args(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|part| part.as_bytes().to_vec()).collect()
    }

    async fn error(dispatcher: &Dispatcher, parts: &[&str]) -> String {
        let mut connection = Connection::new(1, !dispatcher.requires_auth());
        match dispatcher
            .execute(&mut connection, &args(parts))
            .await
            .reply
        {
            Ok(reply) => panic!("{parts:?} answered {reply:?}"),
            Err(error) => format!("{} {}", error.code, error.message),
        }
    }

    #[tokio::test]
    async fn scan_checks_its_arguments_before_reaching_the_cluster() {
        let dispatcher = dispatcher(None);
        for (parts, expected) in [
            (&["SCAN", "abc"][..], "ERR invalid cursor"),
            (&["SCAN", "-1"], "ERR invalid cursor"),
            (&["SCAN", "0", "COUNT", "0"], "ERR syntax error"),
            (
                &["SCAN", "0", "COUNT", "ten"],
                "ERR value is not an integer",
            ),
            (&["SCAN", "0", "MATCH"], "ERR syntax error"),
            (&["SCAN", "0", "BOGUS", "1"], "ERR syntax error"),
        ] {
            let error = error(&dispatcher, parts).await;
            assert!(error.starts_with(expected), "{parts:?} answered {error}");
        }

        // Every key is a string, so a scan for another type, known to Redis or
        // not, is done at once, without asking the cluster, which here is
        // unreachable.
        for type_name in ["HASH", "nosuchtype"] {
            let mut connection = Connection::new(1, true);
            let reply = dispatcher
                .execute(&mut connection, &args(&["SCAN", "42", "TYPE", type_name]))
                .await
                .reply
                .unwrap();
            assert_eq!(
                reply,
                Reply::Array(vec![Reply::bulk("0"), Reply::Array(Vec::new())])
            );
        }
    }

    #[tokio::test]
    async fn every_command_in_the_table_reaches_a_handler() {
        let dispatcher = dispatcher(None);
        for command in table::COMMANDS.iter().filter(|command| !command.declined) {
            let mut parts = vec![command.name];
            parts.resize(command.arity.unsigned_abs() as usize, "1");
            let mut connection = Connection::new(1, true);
            let response = dispatcher.execute(&mut connection, &args(&parts)).await;
            if let Err(error) = response.reply {
                assert!(
                    !error.message.contains("not supported")
                        && !error.message.contains("unknown command"),
                    "{} is in the table but not dispatched: {}",
                    command.name,
                    error.message
                );
            }
        }
    }

    #[tokio::test]
    async fn arity_is_checked_before_anything_else() {
        let dispatcher = dispatcher(Some("secret"));
        assert!(
            error(&dispatcher, &["get", "key", "extra"])
                .await
                .contains("wrong number of arguments for 'get'")
        );
        assert!(
            error(&dispatcher, &["get", "key"])
                .await
                .starts_with("NOAUTH")
        );
        assert!(
            error(&dispatcher, &["nonsense"])
                .await
                .contains("unknown command")
        );
    }

    #[tokio::test]
    async fn a_declined_command_is_refused_by_name() {
        let dispatcher = dispatcher(None);
        assert!(
            error(&dispatcher, &["flushall"])
                .await
                .contains("FLUSHALL is not supported")
        );
    }

    #[tokio::test]
    async fn a_command_that_cannot_finish_passes_its_deadline() {
        // No coordinator is listening, so the read never gets an answer it can
        // use; a write that runs out of time may have been applied.
        let dispatcher = dispatcher(None);
        let read = error(&dispatcher, &["get", "key"]).await;
        assert!(read.starts_with("TRYAGAIN"), "{read}");
    }
}
