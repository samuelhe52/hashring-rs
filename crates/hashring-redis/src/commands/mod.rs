//! Command dispatch: a Redis command in, a Redis reply out.
//!
//! Every request is checked against [`crate::table`] before a handler sees it:
//! the command must exist and not be declined, its arity must match, and the
//! connection must have authenticated unless the command is part of the
//! handshake. A handler then reads its arguments through [`args::Args`].
//!
//! Commands that answer from the gateway live in [`connection`]; the ones that
//! reach the cluster live in [`strings`] and [`keys`] and go through
//! [`store::Store`], which routes each key to its owner through
//! [`hashring_client`] and makes every read-modify-write conditional on the
//! version it read. See docs/redis-compatibility.md for the semantics this
//! reproduces and the ones it deliberately does not.

mod args;
mod connection;
mod keys;
mod store;
mod strings;

use std::{sync::Arc, time::Duration};

use crate::{
    client::{KeyLocks, SharedClient},
    error::{RedisError, RedisResult},
    resp::{Limits, Protocol, Reply},
    table::{self, CommandInfo},
};

pub use args::{keyword, now_millis};

/// The gateway, shared by every connection it serves.
pub struct Dispatcher {
    pub client: Arc<SharedClient>,
    locks: Arc<KeyLocks>,
    password: Option<Arc<str>>,
    limits: Limits,
    /// How long one command that reaches the cluster may take, from its first
    /// lock wait or request to its last.
    command_timeout: Duration,
    started_at: std::time::Instant,
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
        }
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
        let Some((name, rest)) = args.split_first() else {
            return Response::error(RedisError::new("ERR", "empty multibulk"));
        };
        let name = keyword(name);
        let Some(command) = table::lookup(&name) else {
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
        let args = args::Args::new(command.name, rest);
        if !command.has_keys() {
            return self.local(command, connection, args);
        }
        match tokio::time::timeout(self.command_timeout, self.keyed(command, args)).await {
            Ok(reply) => Response::reply(reply),
            Err(_) => Response::error(RedisError::deadline(command.writes())),
        }
    }

    async fn keyed(&self, command: &CommandInfo, args: args::Args<'_>) -> RedisResult<Reply> {
        let store = store::Store::new(&self.client, &self.locks);
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
            other => Err(RedisError::unsupported(&other.to_ascii_uppercase())),
        }
    }
}

/// Everything about one client connection that survives between commands.
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

    fn dispatcher(password: Option<&str>) -> Dispatcher {
        Dispatcher::new(
            Arc::new(SharedClient::new(
                "http://127.0.0.1:1".into(),
                Duration::from_millis(50),
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
            error(&dispatcher, &["scan", "0"])
                .await
                .contains("SCAN is not supported")
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
