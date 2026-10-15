//! Command dispatch: a Redis command in, a Redis reply out.
//!
//! Every command reaches the cluster through [`hashring_client`], which routes
//! each key to its owner and refreshes the topology when a node says the range
//! moved. The gateway adds no routing of its own; what it adds is the Redis
//! shape on top, and the locks and expiry handling Redis's single-threaded
//! server would otherwise provide for free. See docs/redis-compatibility.md
//! for the semantics this reproduces and the ones it deliberately does not.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use hashring_client::{ClientError, Expiry, HashringClient};
use hashring_core::proto::ErrorCode;
use tokio::task::JoinSet;

use crate::{
    client::{KeyLocks, SharedClient},
    error::{RedisError, RedisResult},
    resp::{Limits, Protocol, Reply},
    table::{self, CommandInfo},
};

/// Commands the dispatcher handles, checked against the `COMMAND` table by a
/// test so the two cannot drift apart.
pub const HANDLED: &[&str] = &[
    "ping",
    "echo",
    "hello",
    "auth",
    "select",
    "client",
    "reset",
    "quit",
    "command",
    "info",
    "config",
    "get",
    "getdel",
    "getex",
    "getrange",
    "set",
    "setex",
    "psetex",
    "setnx",
    "setrange",
    "mset",
    "msetnx",
    "mget",
    "getset",
    "append",
    "strlen",
    "incr",
    "decr",
    "incrby",
    "decrby",
    "incrbyfloat",
    "del",
    "unlink",
    "exists",
    "touch",
    "type",
    "rename",
    "renamenx",
    "expire",
    "pexpire",
    "expireat",
    "pexpireat",
    "persist",
    "ttl",
    "pttl",
];

/// How many keys one command reaches the cluster with at once. A multi-key
/// command such as `MGET` on ten thousand keys would otherwise open ten
/// thousand requests at the nodes; a batch keeps a fan-out from turning into a
/// flood, and the chunks keep the replies in argument order.
const FAN_OUT: usize = 64;

/// The gateway, shared by every connection it serves.
pub struct Dispatcher {
    pub client: Arc<SharedClient>,
    locks: Arc<KeyLocks>,
    password: Option<Arc<str>>,
    limits: Limits,
    started_at: Instant,
}

impl Dispatcher {
    pub fn new(client: Arc<SharedClient>, password: Option<Arc<str>>, limits: Limits) -> Self {
        Self {
            client,
            locks: Arc::new(KeyLocks::new()),
            password,
            limits,
            started_at: Instant::now(),
        }
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// Whether a connection has to authenticate before it may read or write.
    pub fn requires_auth(&self) -> bool {
        self.password.is_some()
    }

    pub fn started_at(&self) -> Instant {
        self.started_at
    }

    /// Runs one command. `connection` carries everything specific to the
    /// socket: the negotiated protocol, the name the client gave itself, and
    /// whether it has authenticated.
    pub async fn execute(
        &self,
        connection: &mut Connection,
        args: &[Vec<u8>],
    ) -> RedisResult<Reply> {
        let Some(name) = command_name(args) else {
            return Err(RedisError::new("ERR", "empty multibulk"));
        };
        if self.password.is_some() && !connection.authenticated && !is_pre_auth(&name) {
            return Err(RedisError::no_auth());
        }
        match name.as_str() {
            "ping" => self.ping(args),
            "echo" => self.echo(args),
            "hello" => self.hello(connection, args),
            "auth" => self.auth(connection, args),
            "select" => self.select(args),
            "client" => self.client_command(connection, args),
            "reset" => self.reset(connection),
            "quit" => Ok(Reply::status()),
            "command" => self.command(args),
            "info" => self.info(args),
            "config" => self.config(args),
            "get" => self.get(args).await,
            "getdel" => self.getdel(args).await,
            "getex" => self.getex(args).await,
            "getrange" => self.getrange(args).await,
            "set" => self.set(args).await,
            "setex" => self.set_with_ttl(args, TimeUnit::Seconds, "setex").await,
            "psetex" => {
                self.set_with_ttl(args, TimeUnit::Milliseconds, "psetex")
                    .await
            }
            "setnx" => self.setnx(args).await,
            "setrange" => self.setrange(args).await,
            "mset" => self.mset(args, false).await,
            "msetnx" => self.mset(args, true).await,
            "mget" => self.mget(args).await,
            "getset" => self.getset(args).await,
            "append" => self.append(args).await,
            "strlen" => self.strlen(args).await,
            "incr" => self.incr_by(args, 1, "incr").await,
            "decr" => self.incr_by(args, -1, "decr").await,
            "incrby" => self.incrby(args).await,
            "decrby" => self.decrby(args).await,
            "incrbyfloat" => self.incrbyfloat(args).await,
            "del" | "unlink" => self.delete(args).await,
            "exists" => self.exists(args, false).await,
            "touch" => self.exists(args, true).await,
            "type" => self.key_type(args).await,
            "rename" => self.rename(args, false).await,
            "renamenx" => self.rename(args, true).await,
            "expire" => {
                self.expire(args, TimeUnit::Seconds, DeadlineMode::Relative)
                    .await
            }
            "pexpire" => {
                self.expire(args, TimeUnit::Milliseconds, DeadlineMode::Relative)
                    .await
            }
            "expireat" => {
                self.expire(args, TimeUnit::Seconds, DeadlineMode::Absolute)
                    .await
            }
            "pexpireat" => {
                self.expire(args, TimeUnit::Milliseconds, DeadlineMode::Absolute)
                    .await
            }
            "persist" => self.persist(args).await,
            "ttl" => self.ttl(args, TimeUnit::Seconds).await,
            "pttl" => self.ttl(args, TimeUnit::Milliseconds).await,
            other => Err(unsupported(other)),
        }
    }

    // ----- Connection and administration.

    fn ping(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        match rest(args, "ping", 1, Some(2))? {
            [] => Ok(Reply::Simple("PONG".into())),
            [message] => Ok(Reply::bulk(message.clone())),
            _ => Err(RedisError::wrong_arity("ping")),
        }
    }

    fn echo(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "echo", 2, Some(2))?;
        Ok(Reply::bulk(rest[0].clone()))
    }

    fn hello(&self, connection: &mut Connection, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let mut protocol = connection.protocol;
        let mut index = 1;
        if let Some(requested) = args.get(1) {
            protocol = match parse_integer(requested) {
                Some(2) => Protocol::Resp2,
                Some(3) => Protocol::Resp3,
                Some(other) => {
                    return Err(RedisError::new(
                        "NOPROTO",
                        format!("unsupported protocol version {other}"),
                    ));
                }
                // A client that sends `HELLO AUTH ...` without a version.
                None => Protocol::Resp2,
            };
            index = 2;
        }
        while let Some(option) = args.get(index) {
            match keyword(option).as_str() {
                "auth" => {
                    let password = match (args.get(index + 1), args.get(index + 2)) {
                        (Some(user), Some(password)) if keyword(user) == "default" => password,
                        (Some(password), None) => password,
                        _ => return Err(RedisError::syntax()),
                    };
                    self.check_password(connection, password)?;
                }
                "setname" => {
                    let name = args.get(index + 1).ok_or_else(RedisError::syntax)?;
                    connection.name = Some(name.clone());
                }
                _ => return Err(RedisError::syntax()),
            }
            index += 2;
        }
        connection.protocol = protocol;
        Ok(Reply::Map(vec![
            (b"server".to_vec(), Reply::bulk("hashring-rs")),
            (b"version".to_vec(), Reply::bulk(crate::VERSION)),
            (
                b"proto".to_vec(),
                Reply::Integer(i64::from(protocol.version())),
            ),
            (b"id".to_vec(), Reply::Integer(connection.id as i64)),
            (b"mode".to_vec(), Reply::bulk("standalone")),
            (b"role".to_vec(), Reply::bulk("master")),
            (b"modules".to_vec(), Reply::Array(Vec::new())),
        ]))
    }

    fn auth(&self, connection: &mut Connection, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "auth", 2, Some(3))?;
        // `AUTH password` and `AUTH user password` are both accepted; the
        // gateway has one password and no users.
        let password = match rest {
            [password] => password,
            [user, password] if keyword(user) == "default" => password,
            _ => return Err(RedisError::wrong_arity("auth")),
        };
        self.check_password(connection, password)?;
        Ok(Reply::status())
    }

    fn check_password(&self, connection: &mut Connection, password: &[u8]) -> RedisResult<()> {
        match &self.password {
            None => Err(RedisError::new(
                "ERR",
                "Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?",
            )),
            Some(expected) if expected.as_bytes() == password => {
                connection.authenticated = true;
                Ok(())
            }
            Some(_) => Err(RedisError::wrong_pass()),
        }
    }

    fn select(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "select", 2, Some(2))?;
        // Every key lives in one flat namespace, so only database 0 exists.
        match parse_integer(&rest[0]) {
            Some(0) => Ok(Reply::status()),
            _ => Err(RedisError::new(
                "ERR",
                "DB index is out of range. This gateway serves one database, 0",
            )),
        }
    }

    fn client_command(&self, connection: &mut Connection, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "client", 2, None)?;
        let Some(subcommand) = rest.first().map(|value| keyword(value)) else {
            return Err(RedisError::wrong_arity("client"));
        };
        match subcommand.as_str() {
            "id" => Ok(Reply::Integer(connection.id as i64)),
            "setname" => {
                connection.name = Some(rest.get(1).cloned().ok_or_else(RedisError::syntax)?);
                Ok(Reply::status())
            }
            "getname" => Ok(match &connection.name {
                Some(name) => Reply::bulk(name.clone()),
                None => Reply::Nil,
            }),
            // Clients send `CLIENT SETINFO lib-name ...` on connect and expect
            // a plain OK; nothing is recorded.
            "setinfo" => {
                if rest.len() < 3 {
                    return Err(RedisError::syntax());
                }
                Ok(Reply::status())
            }
            "no-evict" | "no-touch" | "no-evict-on-expiry" | "unpause" | "tracking" => {
                Ok(Reply::status())
            }
            "reply" => {
                connection.reply_mode = match rest.get(1).map(|value| keyword(value)).as_deref() {
                    Some("on") => ReplyMode::On,
                    Some("off") => ReplyMode::Off,
                    Some("skip") => ReplyMode::Skip,
                    _ => return Err(RedisError::syntax()),
                };
                Ok(Reply::status())
            }
            "info" => Ok(Reply::bulk(client_info(connection).into_bytes())),
            "list" => Ok(Reply::bulk("")),
            "getredir" => Ok(Reply::Integer(-1)),
            other => Err(RedisError::new(
                "ERR",
                format!("Unknown subcommand or wrong number of arguments for '{other}'"),
            )),
        }
    }

    fn reset(&self, connection: &mut Connection) -> RedisResult<Reply> {
        connection.protocol = Protocol::Resp2;
        connection.name = None;
        connection.authenticated = self.password.is_none();
        connection.reply_mode = ReplyMode::On;
        Ok(Reply::Simple("RESET".into()))
    }

    fn command(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "command", 1, None)?;
        match rest.first().map(|value| keyword(value)).as_deref() {
            None => Ok(Reply::Array(
                table::COMMANDS.iter().map(CommandInfo::to_reply).collect(),
            )),
            Some("count") => Ok(Reply::Array(vec![Reply::Integer(
                table::COMMANDS.len() as i64
            )])),
            // Documentation is for commands this gateway does not implement, so
            // there is none to send.
            Some("docs") => Ok(Reply::Array(Vec::new())),
            Some("info") => Ok(Reply::Array(
                rest[1..]
                    .iter()
                    .map(|name| match table::lookup(&String::from_utf8_lossy(name)) {
                        Some(command) => command.to_reply(),
                        None => Reply::Nil,
                    })
                    .collect(),
            )),
            Some("getkeys") | Some("getkeysandflags") => {
                Err(RedisError::unsupported("COMMAND GETKEYS"))
            }
            Some(other) => Err(RedisError::new(
                "ERR",
                format!("Unknown subcommand or wrong number of arguments for '{other}'"),
            )),
        }
    }

    fn info(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "info", 1, Some(2))?;
        let section = rest
            .first()
            .map(|value| String::from_utf8_lossy(value).to_lowercase());
        let uptime = self.started_at.elapsed().as_secs();
        let text = match section.as_deref() {
            Some("server") | None => format!(
                "# Server\r\n\
                 redis_version:{version}\r\n\
                 redis_mode:standalone\r\n\
                 os:{os}\r\n\
                 arch_bits:64\r\n\
                 process_id:{pid}\r\n\
                 run_id:{run_id}\r\n\
                 tcp_port:{port}\r\n\
                 uptime_in_seconds:{uptime}\r\n\
                 uptime_in_days:{days}\r\n\
                 executable:hashring-rs redis-gateway\r\n\
                 config_file:\r\n",
                version = crate::VERSION,
                os = std::env::consts::OS,
                pid = std::process::id(),
                run_id = gateway_instance_id(),
                port = 0,
                uptime = uptime,
                days = uptime / 86_400,
            ),
            _ => String::new(),
        };
        let mut body = text;
        if matches!(section.as_deref(), None | Some("clients")) {
            body.push_str("# Clients\r\nconnected_clients:0\r\nblocked_clients:0\r\n");
        }
        if matches!(section.as_deref(), None | Some("memory")) {
            body.push_str(
                "# Memory\r\n\
                 used_memory:0\r\n\
                 used_memory_human:0B\r\n\
                 maxmemory:0\r\n\
                 maxmemory_policy:noeviction\r\n",
            );
        }
        if matches!(section.as_deref(), None | Some("persistence")) {
            body.push_str(
                "# Persistence\r\nloading:0\r\naof_enabled:0\r\nrdb_changes_since_last_save:0\r\n",
            );
        }
        if matches!(section.as_deref(), None | Some("stats")) {
            body.push_str(&format!(
                "# Stats\r\n\
                 total_connections_received:0\r\n\
                 total_commands_processed:0\r\n\
                 uptime_in_seconds:{uptime}\r\n",
                uptime = uptime,
            ));
        }
        if matches!(section.as_deref(), None | Some("replication")) {
            // There is no Redis replica to promote and no Redis master to
            // follow; the cluster has one owner per key, which `cluster`
            // describes instead.
            body.push_str("# Replication\r\nrole:master\r\nconnected_slaves:0\r\n");
        }
        if matches!(section.as_deref(), None | Some("cluster")) {
            body.push_str(
                "# Cluster\r\n\
                 cluster_enabled:0\r\n\
                 cluster_state:ok\r\n\
                 cluster_slots_assigned:0\r\n",
            );
        }
        if matches!(section.as_deref(), None | Some("keyspace")) {
            // Counting keys would mean asking every owner, and the answer would
            // be a snapshot no two callers could agree on. It is left out
            // rather than wrong.
            body.push_str("# Keyspace\r\n");
        }
        Ok(Reply::bulk(body))
    }

    fn config(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "config", 2, None)?;
        match rest.first().map(|value| keyword(value)).as_deref() {
            Some("get") => {
                let pattern = rest.get(1).ok_or_else(RedisError::syntax)?;
                let pattern = String::from_utf8_lossy(pattern).to_lowercase();
                Ok(Reply::Map(
                    config_entries()
                        .into_iter()
                        .filter(|(name, _)| matches_pattern(&pattern, name))
                        .map(|(name, value)| (name.as_bytes().to_vec(), Reply::bulk(value)))
                        .collect(),
                ))
            }
            Some("set") => {
                if rest.len() < 3 || rest.len() % 2 == 0 {
                    return Err(RedisError::syntax());
                }
                for name in rest[1..].iter().step_by(2) {
                    let name = String::from_utf8_lossy(name).to_lowercase();
                    if !config_entries().into_iter().any(|(known, _)| known == name) {
                        return Err(RedisError::new(
                            "ERR",
                            format!(
                                "Unknown option or number of arguments for CONFIG SET - '{name}'"
                            ),
                        ));
                    }
                }
                // Nothing is stored: every parameter is either already the only
                // value that can hold, or names a feature this gateway does
                // not have. Redis answers OK for a parameter it accepts.
                Ok(Reply::status())
            }
            Some("resetstat") | Some("rewrite") => Ok(Reply::status()),
            Some(other) => Err(RedisError::new(
                "ERR",
                format!("Unknown CONFIG subcommand '{other}'"),
            )),
            None => Err(RedisError::wrong_arity("config")),
        }
    }

    // ----- Single-key strings.

    async fn get(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "get", 2, Some(2))?;
        Ok(match self.read(&rest[0]).await? {
            Some(value) => Reply::bulk(value),
            None => Reply::Nil,
        })
    }

    async fn getdel(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "getdel", 2, Some(2))?;
        let _guards = self.locks.lock(&[rest[0].as_slice()]).await;
        let client = self.client.get().await?;
        match client.get(rest[0].clone()).await {
            Ok(output) => {
                client.delete(rest[0].clone()).await?;
                Ok(Reply::bulk(output.value))
            }
            Err(error) if is_not_found(&error) => Ok(Reply::Nil),
            Err(error) => Err(error.into()),
        }
    }

    async fn getex(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "getex", 2, None)?;
        let expiry = ExpiryOptions::parse(&rest[1..], "getex")?.expiry();
        let _guards = self.locks.lock(&[rest[0].as_slice()]).await;
        let client = self.client.get().await?;
        match client.get(rest[0].clone()).await {
            Ok(output) => {
                client
                    .put_with_expiry(rest[0].clone(), output.value.clone(), expiry)
                    .await?;
                Ok(Reply::bulk(output.value))
            }
            Err(error) if is_not_found(&error) => Ok(Reply::Nil),
            Err(error) => Err(error.into()),
        }
    }

    async fn getrange(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "getrange", 4, Some(4))?;
        let start = integer(&rest[1], "getrange")?;
        let end = integer(&rest[2], "getrange")?;
        let value = self.read(&rest[0]).await?.unwrap_or_default();
        Ok(Reply::bulk(slice_range(&value, start, end)))
    }

    async fn set(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "set", 3, None)?;
        let options = SetOptions::parse(&rest[2..], "set")?;
        let key = rest[0].as_slice();
        // Every conditional and every `GET` form needs the read and the write
        // to be one operation, so all of them take the key lock. A plain
        // `SET key value` does not and stays on the write path alone.
        if options.needs_read() {
            let _guards = self.locks.lock(&[key]).await;
            let client = self.client.get().await?;
            let existing = self.read_with(&client, key).await?;
            if options.conflicts_with(&existing) {
                return Ok(Reply::Nil);
            }
            self.write(client, key, rest[1].clone(), options.expiry())
                .await?;
            return Ok(match &options.get {
                true => match existing {
                    Some(value) => Reply::bulk(value),
                    None => Reply::Nil,
                },
                false => Reply::status(),
            });
        }
        let client = self.client.get().await?;
        self.write(client, key, rest[1].clone(), options.expiry())
            .await?;
        Ok(Reply::status())
    }

    async fn set_with_ttl(
        &self,
        args: &[Vec<u8>],
        unit: TimeUnit,
        name: &'static str,
    ) -> RedisResult<Reply> {
        let rest = rest(args, name, 4, Some(4))?;
        let timeout = integer(&rest[1], name)?;
        if timeout <= 0 {
            return Err(RedisError::new(
                "ERR",
                format!("invalid expire time in '{name}' command"),
            ));
        }
        let client = self.client.get().await?;
        self.write(
            client,
            rest[0].as_slice(),
            rest[2].clone(),
            Expiry::At(deadline(unit, timeout)),
        )
        .await?;
        Ok(Reply::status())
    }

    async fn setnx(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "setnx", 3, Some(3))?;
        let _guards = self.locks.lock(&[rest[0].as_slice()]).await;
        let client = self.client.get().await?;
        if self.read_with(&client, rest[0].as_slice()).await?.is_some() {
            return Ok(Reply::Integer(0));
        }
        self.write(client, rest[0].as_slice(), rest[1].clone(), Expiry::Clear)
            .await?;
        Ok(Reply::Integer(1))
    }

    async fn setrange(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "setrange", 4, Some(4))?;
        let offset = integer(&rest[1], "setrange")?;
        if offset < 0 {
            return Err(RedisError::out_of_range());
        }
        let offset = u64::try_from(offset).map_err(|_| RedisError::out_of_range())?;
        let padding = usize::try_from(offset).map_err(|_| RedisError::out_of_range())?;
        if padding + rest[2].len() > self.limits.max_bulk_bytes {
            return Err(RedisError::new(
                "ERR",
                "string exceeds maximum allowed size (proto-max-bulk-len)",
            ));
        }
        let _guards = self.locks.lock(&[rest[0].as_slice()]).await;
        let client = self.client.get().await?;
        let existing = self.read_with(&client, rest[0].as_slice()).await?;
        let mut value = existing.unwrap_or_default();
        if padding > value.len() {
            value.resize(padding, 0);
        }
        value.splice(
            padding..(padding + rest[2].len()).min(value.len()),
            rest[2].iter().copied(),
        );
        let length = value.len();
        self.write(client, rest[0].as_slice(), value, Expiry::Keep)
            .await?;
        Ok(Reply::Integer(length as i64))
    }

    async fn getset(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "getset", 3, Some(3))?;
        let _guards = self.locks.lock(&[rest[0].as_slice()]).await;
        let client = self.client.get().await?;
        let existing = self.read_with(&client, rest[0].as_slice()).await?;
        // A plain `GETSET` replaces the value, so it drops the old deadline the
        // same way a plain `SET` does.
        self.write(client, rest[0].as_slice(), rest[1].clone(), Expiry::Clear)
            .await?;
        Ok(match existing {
            Some(value) => Reply::bulk(value),
            None => Reply::Nil,
        })
    }

    async fn append(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "append", 3, Some(3))?;
        let _guards = self.locks.lock(&[rest[0].as_slice()]).await;
        let client = self.client.get().await?;
        let mut value = self
            .read_with(&client, rest[0].as_slice())
            .await?
            .unwrap_or_default();
        if value.len() + rest[1].len() > self.limits.max_bulk_bytes {
            return Err(RedisError::new(
                "ERR",
                "string exceeds maximum allowed size (proto-max-bulk-len)",
            ));
        }
        value.extend_from_slice(&rest[1]);
        let length = value.len();
        // Redis keeps the deadline an `APPEND` finds.
        self.write(client, rest[0].as_slice(), value, Expiry::Keep)
            .await?;
        Ok(Reply::Integer(length as i64))
    }

    async fn strlen(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "strlen", 2, Some(2))?;
        let value = self.read(&rest[0]).await?.unwrap_or_default();
        Ok(Reply::Integer(value.len() as i64))
    }

    async fn incr_by(
        &self,
        args: &[Vec<u8>],
        delta: i64,
        name: &'static str,
    ) -> RedisResult<Reply> {
        let rest = rest(args, name, 2, Some(2))?;
        let value = self.increment(rest[0].as_slice(), delta).await?;
        if value == i64::MIN {
            return Err(RedisError::overflow());
        }
        Ok(Reply::Integer(value))
    }

    async fn incrby(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "incrby", 3, Some(3))?;
        let delta = integer(&rest[1], "incrby")?;
        Ok(Reply::Integer(
            self.increment(rest[0].as_slice(), delta).await?,
        ))
    }

    async fn decrby(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "decrby", 3, Some(3))?;
        let delta = integer(&rest[1], "decrby")?;
        if delta == i64::MIN {
            return Err(RedisError::overflow());
        }
        Ok(Reply::Integer(
            self.increment(rest[0].as_slice(), -delta).await?,
        ))
    }

    async fn incrbyfloat(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "incrbyfloat", 3, Some(3))?;
        let delta = float(&rest[1])?;
        let _guards = self.locks.lock(&[rest[0].as_slice()]).await;
        let client = self.client.get().await?;
        let existing = self.read_with(&client, rest[0].as_slice()).await?;
        let base = match &existing {
            Some(value) if value.is_empty() => 0.0,
            Some(value) => float(value)?,
            None => 0.0,
        };
        let updated = base + delta;
        if !updated.is_finite() {
            return Err(RedisError::new(
                "ERR",
                "increment would produce NaN or Infinity",
            ));
        }
        let formatted = format_number(updated);
        // `INCRBYFLOAT` is a rewrite of the value, and Redis keeps the
        // deadline it finds.
        self.write(
            client,
            rest[0].as_slice(),
            formatted.clone().into_bytes(),
            Expiry::Keep,
        )
        .await?;
        Ok(Reply::bulk(formatted))
    }

    /// The read-modify-write behind `INCR`, `DECR`, `INCRBY` and `DECRBY`.
    async fn increment(&self, key: &[u8], delta: i64) -> RedisResult<i64> {
        let _guards = self.locks.lock(&[key]).await;
        let client = self.client.get().await?;
        let existing = self.read_with(&client, key).await?;
        // An absent key counts as zero; a key that exists but is not an integer
        // is an error rather than a reset, which is what Redis reports.
        let current = match &existing {
            Some(value) => parse_integer(value).ok_or_else(RedisError::not_an_integer)?,
            None => 0,
        };
        let updated = current
            .checked_add(delta)
            .ok_or_else(RedisError::overflow)?;
        self.write(client, key, updated.to_string().into_bytes(), Expiry::Keep)
            .await?;
        Ok(updated)
    }

    // ----- Multi-key strings.

    async fn mget(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let keys = rest(args, "mget", 2, None)?.to_vec();
        let results = self.fan_out(keys, Operation::Read).await?;
        let mut missing = Vec::new();
        for result in results {
            match result {
                Ok(Some(value)) => missing.push(Reply::bulk(value)),
                Ok(None) => missing.push(Reply::Nil),
                Err(error) => missing.push(error.to_reply()),
            }
        }
        Ok(Reply::Array(missing))
    }

    async fn mset(&self, args: &[Vec<u8>], only_if_absent: bool) -> RedisResult<Reply> {
        let name = if only_if_absent { "msetnx" } else { "mset" };
        let rest = rest(args, name, 3, None)?;
        if rest.len() % 2 != 0 {
            return Err(RedisError::wrong_length(rest.len().div_ceil(2) * 2));
        }
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = rest
            .chunks_exact(2)
            .map(|pair| (pair[0].clone(), pair[1].clone()))
            .collect();
        if only_if_absent {
            // One lock for the whole set, because the condition spans every key
            // and two `MSETNX` calls must not both see an empty keyspace.
            let keys: Vec<&[u8]> = pairs.iter().map(|(key, _)| key.as_slice()).collect();
            let _guards = self.locks.lock(&keys).await;
            let present = self
                .fan_out(
                    pairs.iter().map(|(key, _)| key.clone()).collect(),
                    Operation::Read,
                )
                .await?
                .into_iter()
                .any(|result| matches!(result, Ok(Some(_))));
            if present {
                return Ok(Reply::Integer(0));
            }
            self.write_pairs(pairs).await?;
            return Ok(Reply::Integer(1));
        }
        // Redis makes `MSET` atomic across keys. It cannot be here: the keys
        // may live on different owners, and no node offers a write that spans
        // two of them. The locks at least keep two of these commands from
        // interleaving in this gateway.
        let keys: Vec<&[u8]> = pairs.iter().map(|(key, _)| key.as_slice()).collect();
        let _guards = self.locks.lock(&keys).await;
        self.write_pairs(pairs).await?;
        Ok(Reply::status())
    }

    async fn write_pairs(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> RedisResult<()> {
        let client = self.client.get().await?;
        for (key, value) in pairs {
            // A plain `MSET` drops any deadline on the keys it writes.
            client
                .put_with_expiry(key, value, Expiry::Clear)
                .await
                .map_err(RedisError::from)?;
        }
        Ok(())
    }

    // ----- Keys.

    async fn delete(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let keys = rest(args, "del", 2, None)?.to_vec();
        let results = self.fan_out(keys, Operation::Delete).await?;
        let mut removed = 0;
        for result in results {
            if matches!(result, Ok(Some(_))) {
                removed += 1;
            }
        }
        Ok(Reply::Integer(removed))
    }

    async fn exists(&self, args: &[Vec<u8>], touch: bool) -> RedisResult<Reply> {
        let name = if touch { "touch" } else { "exists" };
        let keys = rest(args, name, 2, None)?.to_vec();
        let results = self.fan_out(keys, Operation::Read).await?;
        let mut found = 0;
        for result in results {
            if matches!(result, Ok(Some(_))) {
                found += 1;
            }
        }
        Ok(Reply::Integer(found))
    }

    async fn key_type(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "type", 2, Some(2))?;
        Ok(match self.read(&rest[0]).await? {
            // Every value this gateway stores is a byte string.
            Some(_) => Reply::Simple("string".into()),
            None => Reply::Simple("none".into()),
        })
    }

    async fn rename(&self, args: &[Vec<u8>], only_if_absent: bool) -> RedisResult<Reply> {
        let name = if only_if_absent { "renamenx" } else { "rename" };
        let rest = rest(args, name, 3, Some(3))?;
        let (from, to) = (rest[0].as_slice(), rest[1].as_slice());
        let _guards = self.locks.lock(&[from, to]).await;
        let client = self.client.get().await?;
        // The value and its deadline are read together and moved together, so a
        // key that is renamed does not silently lose the expiry it had.
        let Some((value, deadline)) = self.read_key(&client, from).await? else {
            return Err(RedisError::no_such_key());
        };
        if only_if_absent && self.read_with(&client, to).await?.is_some() {
            return Err(RedisError::new("ERR", "target key name already exists."));
        }
        client
            .put_with_expiry(
                to.to_vec(),
                value,
                deadline.map_or(Expiry::Clear, Expiry::At),
            )
            .await?;
        if from != to {
            client.delete(from.to_vec()).await?;
        }
        Ok(if only_if_absent {
            Reply::Integer(1)
        } else {
            Reply::status()
        })
    }

    // ----- Expiry.

    async fn expire(
        &self,
        args: &[Vec<u8>],
        unit: TimeUnit,
        mode: DeadlineMode,
    ) -> RedisResult<Reply> {
        let name = match (unit, mode) {
            (TimeUnit::Seconds, DeadlineMode::Relative) => "expire",
            (TimeUnit::Milliseconds, DeadlineMode::Relative) => "pexpire",
            (TimeUnit::Seconds, DeadlineMode::Absolute) => "expireat",
            (TimeUnit::Milliseconds, DeadlineMode::Absolute) => "pexpireat",
        };
        let rest = rest(args, name, 3, Some(4))?;
        let amount = integer(&rest[1], name)?;
        let guard = match rest.get(2).map(|value| keyword(value)).as_deref() {
            None => ExpiryGuard::Always,
            Some("nx") => ExpiryGuard::OnlyWithout,
            Some("xx") => ExpiryGuard::OnlyWith,
            Some("gt") => ExpiryGuard::OnlyGreater,
            Some("lt") => ExpiryGuard::OnlyLess,
            Some(_) => return Err(RedisError::syntax()),
        };
        let expires_at = match mode {
            DeadlineMode::Relative => deadline(unit, amount),
            DeadlineMode::Absolute => {
                let millis = match unit {
                    TimeUnit::Seconds => amount.saturating_mul(1000),
                    TimeUnit::Milliseconds => amount,
                };
                if millis <= 0 {
                    return Err(RedisError::out_of_range());
                }
                millis as u64
            }
        };
        let _guards = self.locks.lock(&[rest[0].as_slice()]).await;
        let client = self.client.get().await?;
        // A key that cannot be read is absent as far as this command is
        // concerned, which is the 0 Redis reports.
        let Some((_, deadline)) = self.read_key(&client, rest[0].as_slice()).await? else {
            return Ok(Reply::Integer(0));
        };
        if !guard.allows(deadline, expires_at) {
            return Ok(Reply::Integer(0));
        }
        if expires_at <= now_millis() {
            // Redis treats a deadline in the past as a delete.
            client.delete(rest[0].clone()).await?;
            return Ok(Reply::Integer(1));
        }
        let applied = client.set_expiry(rest[0].clone(), expires_at).await?;
        Ok(Reply::Integer(i64::from(!applied.key_missing)))
    }

    async fn persist(&self, args: &[Vec<u8>]) -> RedisResult<Reply> {
        let rest = rest(args, "persist", 2, Some(2))?;
        let _guards = self.locks.lock(&[rest[0].as_slice()]).await;
        let client = self.client.get().await?;
        let Some((value, deadline)) = self.read_key(&client, rest[0].as_slice()).await? else {
            return Ok(Reply::Integer(0));
        };
        if deadline.is_none() {
            return Ok(Reply::Integer(0));
        }
        // Clearing a deadline without an expiry write of its own means writing
        // the value back without one; the cluster has no "clear this field"
        // operation.
        client
            .put_with_expiry(rest[0].clone(), value, Expiry::Clear)
            .await?;
        Ok(Reply::Integer(1))
    }

    async fn ttl(&self, args: &[Vec<u8>], unit: TimeUnit) -> RedisResult<Reply> {
        let name = if matches!(unit, TimeUnit::Seconds) {
            "ttl"
        } else {
            "pttl"
        };
        let rest = rest(args, name, 2, Some(2))?;
        let found = self.read_deadline(&rest[0]).await?;
        Ok(match found {
            // -1 is a key with no deadline, -2 no key at all.
            None => Reply::Integer(-2),
            Some(None) => Reply::Integer(-1),
            Some(Some(expires_at)) => {
                let remaining = expires_at.saturating_sub(now_millis());
                let value = match unit {
                    // Redis rounds a remaining TTL up to whole seconds.
                    TimeUnit::Seconds => remaining.div_ceil(1000),
                    TimeUnit::Milliseconds => remaining,
                };
                Reply::Integer(i64::try_from(value).unwrap_or(i64::MAX))
            }
        })
    }

    // ----- Cluster access.

    /// One key, or a missing value. The two outcomes differ from a failure in
    /// every caller, so they are separated here rather than at each one.
    pub async fn read(&self, key: &[u8]) -> RedisResult<Option<Vec<u8>>> {
        let client = self.client.get().await?;
        self.read_with(&client, key).await
    }

    async fn read_with(&self, client: &HashringClient, key: &[u8]) -> RedisResult<Option<Vec<u8>>> {
        self.read_key(client, key)
            .await
            .map(|found| found.map(|(value, _)| value))
    }

    /// A key's deadline, where `None` is a key that does not exist and
    /// `Some(None)` is a key that never expires.
    async fn read_deadline(&self, key: &[u8]) -> RedisResult<Option<Option<u64>>> {
        let client = self.client.get().await?;
        self.read_key(&client, key)
            .await
            .map(|found| found.map(|(_, deadline)| deadline))
    }

    /// A key's value and deadline together, so a caller that needs both reads
    /// it once rather than twice against two versions of the key.
    async fn read_key(
        &self,
        client: &HashringClient,
        key: &[u8],
    ) -> RedisResult<Option<(Vec<u8>, Option<u64>)>> {
        match client.get(key.to_vec()).await {
            Ok(output) => Ok(Some((output.value, output.expires_at_unix_millis))),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn write(
        &self,
        client: HashringClient,
        key: &[u8],
        value: Vec<u8>,
        expiry: Expiry,
    ) -> RedisResult<()> {
        client
            .put_with_expiry(key.to_vec(), value, expiry)
            .await
            .map_err(RedisError::from)?;
        Ok(())
    }

    /// Reaches one command's keys concurrently, in batches, and returns the
    /// answers in argument order.
    async fn fan_out(
        &self,
        keys: Vec<Vec<u8>>,
        operation: Operation,
    ) -> RedisResult<Vec<RedisResult<Option<Vec<u8>>>>> {
        let client = self.client.get().await?;
        let locks = self.locks.clone();
        let mut results = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(FAN_OUT) {
            let mut tasks = JoinSet::new();
            for (offset, key) in chunk.iter().enumerate() {
                let client = client.clone();
                let locks = locks.clone();
                let key = key.clone();
                tasks.spawn(async move { (offset, apply(&client, &locks, &key, operation).await) });
            }
            let mut ordered = Vec::with_capacity(chunk.len());
            while let Some(joined) = tasks.join_next().await {
                let (offset, result) = joined.map_err(|error| {
                    RedisError::new("ERR", format!("a cluster request failed: {error}"))
                })?;
                ordered.push((offset, result));
            }
            ordered.sort_by_key(|(offset, _)| *offset);
            results.extend(ordered.into_iter().map(|(_, result)| result));
        }
        Ok(results)
    }
}

/// One key's side of a multi-key command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Read,
    Delete,
}

/// One key's side of a multi-key command, on its own connection to the cluster.
///
/// `Delete` answers whether the key held a value, because that is what `DEL`
/// counts. The cluster's delete is idempotent and reports success either way,
/// so the count comes from the read that precedes it; the two are held together
/// by the key lock so a concurrent write cannot slip between them.
async fn apply(
    client: &HashringClient,
    locks: &KeyLocks,
    key: &[u8],
    operation: Operation,
) -> RedisResult<Option<Vec<u8>>> {
    match operation {
        Operation::Read => match client.get(key.to_vec()).await {
            Ok(output) => Ok(Some(output.value)),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(error.into()),
        },
        Operation::Delete => {
            let _guard = locks.lock(&[key]).await;
            let existed = match client.get(key.to_vec()).await {
                Ok(_) => true,
                Err(error) if is_not_found(&error) => false,
                Err(error) => return Err(error.into()),
            };
            client
                .delete(key.to_vec())
                .await
                .map_err(RedisError::from)?;
            Ok(existed.then(Vec::new))
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

// ----- Arguments.

/// The arguments after the command name, with its arity checked.
///
/// `minimum` and `maximum` count the command name as well, the same way the
/// `COMMAND` table and Redis's own arity do, so the two cannot disagree.
fn rest<'a>(
    args: &'a [Vec<u8>],
    name: &str,
    minimum: usize,
    maximum: Option<usize>,
) -> RedisResult<&'a [Vec<u8>]> {
    let given = args.len();
    if given < minimum || maximum.is_some_and(|max| given > max) {
        return Err(RedisError::wrong_arity(name));
    }
    Ok(&args[1..])
}

fn command_name(args: &[Vec<u8>]) -> Option<String> {
    let name = args.first()?;
    Some(String::from_utf8_lossy(name).to_lowercase())
}

/// A command word in lower case, for comparing against `NX`, `EX` and friends.
fn keyword(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_lowercase()
}

fn is_pre_auth(name: &str) -> bool {
    matches!(name, "auth" | "hello" | "quit" | "reset")
}

fn unsupported(name: &str) -> RedisError {
    // A known name is reported as unsupported; anything else is reported as
    // unknown, which is what a client uses to decide the server is not the one
    // it expected.
    if table::lookup(name).is_some() {
        RedisError::unsupported(name.to_ascii_uppercase().as_str())
    } else {
        RedisError::new(
            "ERR",
            format!("unknown command '{name}', with args beginning with: "),
        )
    }
}

// ----- Values.

fn parse_integer(value: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(value).ok()?;
    text.trim().parse::<i64>().ok()
}

fn integer(value: &[u8], command: &str) -> RedisResult<i64> {
    parse_integer(value).ok_or_else(|| {
        RedisError::new(
            "ERR",
            format!("value is not an integer or out of range ({command})"),
        )
    })
}

fn float(value: &[u8]) -> RedisResult<f64> {
    let text = std::str::from_utf8(value).unwrap_or_default();
    match text.trim().parse::<f64>() {
        Ok(parsed) if parsed.is_finite() => Ok(parsed),
        _ => Err(RedisError::not_a_float()),
    }
}

/// Redis prints a computed float in the shortest form that reads back as the
/// same number, and always with a point or an exponent so a client parsing it
/// as a double is not left guessing.
fn format_number(value: f64) -> String {
    let text = format!("{value}");
    if text.contains(['.', 'e', 'E']) {
        text
    } else {
        format!("{text}.0")
    }
}

/// Redis's `GETRANGE` index rules: a negative index counts from the end, and
/// both ends are clamped into the value before they are compared.
fn slice_range(value: &[u8], start: i64, end: i64) -> Vec<u8> {
    let length = value.len() as i64;
    if length == 0 {
        return Vec::new();
    }
    let mut start = if start < 0 { length + start } else { start };
    let mut end = if end < 0 { length + end } else { end };
    if start < 0 {
        start = 0;
    }
    if end < 0 {
        end = 0;
    }
    if end >= length {
        end = length - 1;
    }
    if start > end || start >= length {
        return Vec::new();
    }
    value[start as usize..=end as usize].to_vec()
}

fn is_not_found(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Operation(failure) if failure.code == ErrorCode::NotFound
    )
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// An absolute deadline from a relative timeout. Saturating, because a timeout
/// large enough to overflow is a deadline no client waits for either way.
fn deadline(unit: TimeUnit, amount: i64) -> u64 {
    let millis = match unit {
        TimeUnit::Seconds => amount.saturating_mul(1000),
        TimeUnit::Milliseconds => amount,
    };
    now_millis().saturating_add(u64::try_from(millis).unwrap_or(u64::MAX))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimeUnit {
    Seconds,
    Milliseconds,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeadlineMode {
    Relative,
    Absolute,
}

/// The `NX`, `XX`, `GT` and `LT` conditions `EXPIRE` accepts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpiryGuard {
    Always,
    OnlyWithout,
    OnlyWith,
    OnlyGreater,
    OnlyLess,
}

impl ExpiryGuard {
    /// `expires_at` is the deadline the key has now, and `requested` the one
    /// being asked for, both absolute. `GT` and `LT` compare the deadlines,
    /// which for a relative command means comparing the timeouts too.
    fn allows(self, expires_at: Option<u64>, requested: u64) -> bool {
        match self {
            Self::Always => true,
            Self::OnlyWithout => expires_at.is_none(),
            Self::OnlyWith => expires_at.is_some(),
            Self::OnlyGreater => expires_at.is_some_and(|current| requested > current),
            Self::OnlyLess => expires_at.is_some_and(|current| requested < current),
        }
    }
}

/// Options shared by `SET` and `GETEX`.
#[derive(Debug, PartialEq)]
struct ExpiryOptions {
    expiry: Expiry,
    persist: bool,
}

impl Default for ExpiryOptions {
    fn default() -> Self {
        Self {
            // No option means "leave the deadline as it is", which for `GETEX`
            // is Redis's behaviour and for `SET` is corrected by `expiry()`.
            expiry: Expiry::Keep,
            persist: false,
        }
    }
}

impl ExpiryOptions {
    fn parse(args: &[Vec<u8>], command: &'static str) -> RedisResult<Self> {
        let mut options = Self::default();
        let mut seen = false;
        let mut index = 0;
        while let Some(argument) = args.get(index) {
            match keyword(argument).as_str() {
                "ex" | "px" | "exat" | "pxat" => {
                    if seen {
                        return Err(RedisError::syntax());
                    }
                    seen = true;
                    let absolute = matches!(keyword(argument).as_str(), "exat" | "pxat");
                    let unit = if keyword(argument).as_str().ends_with("x") {
                        TimeUnit::Seconds
                    } else {
                        TimeUnit::Milliseconds
                    };
                    let amount =
                        integer(args.get(index + 1).ok_or_else(RedisError::syntax)?, command)?;
                    if amount <= 0 {
                        return Err(RedisError::new(
                            "ERR",
                            format!("invalid expire time in '{command}' command"),
                        ));
                    }
                    options.expiry = if absolute {
                        let millis = match unit {
                            TimeUnit::Seconds => amount.saturating_mul(1000),
                            TimeUnit::Milliseconds => amount,
                        };
                        Expiry::At(u64::try_from(millis).map_err(|_| RedisError::out_of_range())?)
                    } else {
                        Expiry::At(deadline(unit, amount))
                    };
                    index += 2;
                    continue;
                }
                "persist" => {
                    if seen {
                        return Err(RedisError::syntax());
                    }
                    seen = true;
                    options.persist = true;
                }
                _ => return Err(RedisError::syntax()),
            }
            index += 1;
        }
        Ok(options)
    }

    /// `GETEX` without an option keeps the deadline it finds.
    fn expiry(self) -> Expiry {
        if self.persist {
            Expiry::Clear
        } else {
            self.expiry
        }
    }
}

/// The options `SET` accepts.
#[derive(Debug, PartialEq)]
struct SetOptions {
    nx: bool,
    xx: bool,
    get: bool,
    keepttl: bool,
    expiry: Expiry,
}

impl Default for SetOptions {
    fn default() -> Self {
        Self {
            nx: false,
            xx: false,
            get: false,
            keepttl: false,
            // A plain `SET` drops the deadline the key already has, which is
            // Redis's behaviour. `KEEPTTL` is what asks for it to stay.
            expiry: Expiry::Clear,
        }
    }
}

impl SetOptions {
    fn parse(args: &[Vec<u8>], command: &'static str) -> RedisResult<Self> {
        let mut options = Self::default();
        let mut condition = false;
        let mut seen_expiry = false;
        let mut index = 0;
        while let Some(argument) = args.get(index) {
            match keyword(argument).as_str() {
                "nx" | "xx" => {
                    if condition {
                        return Err(RedisError::syntax());
                    }
                    condition = true;
                    match keyword(argument).as_str() {
                        "nx" => options.nx = true,
                        _ => options.xx = true,
                    }
                }
                "get" => options.get = true,
                "keepttl" => options.keepttl = true,
                "ex" | "px" | "exat" | "pxat" => {
                    if seen_expiry {
                        return Err(RedisError::syntax());
                    }
                    seen_expiry = true;
                    let expiry = ExpiryOptions::parse(&args[index..index + 2], command)?;
                    options.expiry = expiry.expiry;
                    index += 1;
                }
                // `SET` has no `PERSIST`, and neither does anything else it takes.
                _ => return Err(RedisError::syntax()),
            }
            index += 1;
        }
        if options.keepttl && seen_expiry {
            return Err(RedisError::syntax());
        }
        Ok(options)
    }

    /// `SET` without an expiry option drops the deadline the key already has,
    /// exactly as Redis does; `KEEPTTL` is what asks for it to stay.
    fn expiry(&self) -> Expiry {
        if self.keepttl {
            Expiry::Keep
        } else {
            self.expiry
        }
    }

    fn needs_read(&self) -> bool {
        self.nx || self.xx || self.get
    }

    fn conflicts_with(&self, existing: &Option<Vec<u8>>) -> bool {
        if self.nx {
            existing.is_some()
        } else {
            self.xx && existing.is_none()
        }
    }
}

// ----- Static answers.

/// The `CONFIG GET` parameters this gateway reports, and accepts unchanged.
fn config_entries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("maxmemory", "0"),
        ("maxmemory-policy", "noeviction"),
        ("appendonly", "no"),
        ("appendfsync", "everysec"),
        ("save", ""),
        ("dir", ""),
        ("dbfilename", ""),
        ("timeout", "0"),
        ("notify-keyspace-events", ""),
        ("lazyfree-lazy-user-del", "no"),
    ]
}

/// `CONFIG GET` takes a glob, and only the characters Redis supports matter
/// here: `*`, `?`, character classes and a backslash escape.
fn matches_pattern(pattern: &str, name: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let name: Vec<char> = name.chars().collect();
    glob(&pattern, &name)
}

fn glob(pattern: &[char], name: &[char]) -> bool {
    if pattern.is_empty() {
        return name.is_empty();
    }
    match pattern[0] {
        '*' => (0..=name.len()).any(|skip| glob(&pattern[1..], &name[skip..])),
        '?' => !name.is_empty() && glob(&pattern[1..], &name[1..]),
        '[' => {
            let Some(close) = pattern.iter().position(|c| *c == ']') else {
                return false;
            };
            if name.is_empty() {
                return false;
            }
            let mut set = &pattern[1..close];
            // Redis treats `!` and `^` alike at the front of a class.
            let negated = matches!(set.first(), Some('!' | '^'));
            if negated {
                set = &set[1..];
            }
            let mut matched = false;
            let mut index = 0;
            while index < set.len() {
                if index + 2 < set.len() && set[index + 1] == '-' {
                    if name[0] >= set[index] && name[0] <= set[index + 2] {
                        matched = true;
                    }
                    index += 3;
                } else {
                    if set[index] == name[0] {
                        matched = true;
                    }
                    index += 1;
                }
            }
            matched != negated && glob(&pattern[close + 1..], &name[1..])
        }
        '\\' if pattern.len() > 1 => {
            !name.is_empty() && pattern[1] == name[0] && glob(&pattern[2..], &name[1..])
        }
        literal => !name.is_empty() && literal == name[0] && glob(&pattern[1..], &name[1..]),
    }
}

fn client_info(connection: &Connection) -> String {
    let name = match &connection.name {
        Some(name) => String::from_utf8_lossy(name).into_owned(),
        None => String::new(),
    };
    format!(
        "id={id} addr=127.0.0.1:0 laddr=127.0.0.1:0 fd=8 name={name} age=0 idle=0 flags=N db=0 \
         sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=26 qbuf-free=20448 argv-mem=10 multi-mem=0 \
         rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-net-in=0 tot-net-out=0 resp={resp}",
        id = connection.id,
        resp = connection.protocol.version(),
    )
}

/// A per-process identifier for `INFO`, so two gateways are distinguishable in
/// a log without a client caring.
fn gateway_instance_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arity_is_checked_before_anything_else() {
        let args = vec![b"get".to_vec(), b"key".to_vec(), b"extra".to_vec()];
        let error = rest(&args, "get", 2, Some(2)).unwrap_err();
        assert!(
            error
                .message
                .contains("wrong number of arguments for 'get'")
        );
    }

    #[test]
    fn getrange_follows_the_redis_index_rules() {
        assert_eq!(slice_range(b"Hello", 0, 4), b"Hello");
        assert_eq!(slice_range(b"Hello", 1, 3), b"ell");
        assert_eq!(slice_range(b"Hello", -3, -1), b"llo");
        assert_eq!(slice_range(b"Hello", 0, 100), b"Hello");
        // A start far below the beginning clamps to 0 and leaves the end alone.
        assert_eq!(slice_range(b"Hello", -100, 2), b"Hel");
        assert_eq!(slice_range(b"Hello", 3, 1), b"");
        assert_eq!(slice_range(b"Hello", 5, 6), b"");
        assert_eq!(slice_range(b"", 0, -1), b"");
    }

    #[test]
    fn set_options_are_parsed_the_way_redis_documents_them() {
        let parse = |args: &[&[u8]]| {
            SetOptions::parse(
                &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
                "set",
            )
        };
        assert!(parse(&[b"NX"]).unwrap().nx);
        assert_eq!(
            parse(&[b"EX", b"10"]).unwrap().expiry(),
            Expiry::At(now_millis() + 10_000)
        );
        // NX and XX together, and two expiry options, are both syntax errors.
        assert!(parse(&[b"NX", b"XX"]).is_err());
        assert!(parse(&[b"EX", b"10", b"PX", b"10"]).is_err());
        assert!(parse(&[b"KEEPTTL", b"EX", b"10"]).is_err());
        assert!(parse(&[b"EX", b"0"]).is_err());
    }

    #[test]
    fn a_plain_set_drops_the_deadline_and_keepttl_keeps_it() {
        let parse = |args: &[&[u8]]| {
            SetOptions::parse(
                &args.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>(),
                "set",
            )
            .unwrap()
        };
        assert_eq!(parse(&[]).expiry(), Expiry::Clear);
        assert_eq!(parse(&[b"KEEPTTL"]).expiry(), Expiry::Keep);
        assert_eq!(
            parse(&[b"EX", b"10"]).expiry(),
            Expiry::At(now_millis() + 10_000)
        );
    }

    #[test]
    fn the_condition_forms_agree_with_the_existing_value() {
        let nx = SetOptions {
            nx: true,
            ..Default::default()
        };
        assert!(nx.conflicts_with(&Some(Vec::new())));
        assert!(!nx.conflicts_with(&None));
        let xx = SetOptions {
            xx: true,
            ..Default::default()
        };
        assert!(xx.conflicts_with(&None));
        assert!(!xx.conflicts_with(&Some(Vec::new())));
    }

    #[test]
    fn the_expiry_conditions_compare_the_deadline() {
        let now = now_millis();
        assert!(ExpiryGuard::Always.allows(None, now));
        assert!(ExpiryGuard::OnlyWithout.allows(None, now));
        assert!(!ExpiryGuard::OnlyWithout.allows(Some(now + 1), now));
        assert!(ExpiryGuard::OnlyWith.allows(Some(now + 1), now));
        assert!(ExpiryGuard::OnlyGreater.allows(Some(now), now + 1));
        assert!(!ExpiryGuard::OnlyGreater.allows(Some(now + 1), now));
        assert!(ExpiryGuard::OnlyLess.allows(Some(now + 1), now));
    }

    #[test]
    fn config_patterns_match_the_way_redis_documents_them() {
        assert!(matches_pattern("*", "maxmemory"));
        assert!(matches_pattern("max*", "maxmemory"));
        assert!(matches_pattern("max?emory", "maxmemory"));
        assert!(!matches_pattern("max?emory", "maxmemoryy"));
        assert!(matches_pattern("save", "save"));
        assert!(!matches_pattern("sav", "save"));
        assert!(matches_pattern("[ms]*", "save"));
        assert!(!matches_pattern("[!s]*", "save"));
    }

    #[test]
    fn a_computed_float_always_reads_back_as_a_double() {
        for value in [1.0_f64, 1.5, -0.25, 1e17, 3.0e-7] {
            let text = format_number(value);
            assert!(
                text.contains('.') || text.contains('e'),
                "{text} would be read back as an integer"
            );
            assert_eq!(text.parse::<f64>().unwrap(), value);
        }
    }
}
