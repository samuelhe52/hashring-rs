//! Commands the gateway answers itself: the handshake, connection settings and
//! the introspection clients run on connect. None of them reaches the cluster.

use std::sync::atomic::{AtomicU64, Ordering};

use super::{
    Connection, Dispatcher, Effect, ReplyMode, Response,
    args::{Args, keyword, parse_integer},
};
use crate::{
    error::{RedisError, RedisResult},
    resp::{Protocol, Reply},
    table::CommandInfo,
};

impl Dispatcher {
    pub(super) fn local(
        &self,
        command: &CommandInfo,
        connection: &mut Connection,
        args: Args<'_>,
    ) -> Response {
        let reply = match command.name {
            // Redis ignores anything after `QUIT`.
            "quit" => {
                return Response {
                    reply: Ok(Reply::status()),
                    effect: Effect::Close,
                };
            }
            "client" => return self.client_command(connection, args),
            "ping" => ping(args),
            "echo" => echo(args),
            "hello" => self.hello(connection, args),
            "auth" => self.auth(connection, args),
            "select" => select(args),
            "reset" => self.reset(connection, args),
            "command" => command_command(self, args),
            "info" => self.info(args),
            "config" => config(args),
            other => Err(RedisError::unsupported(&other.to_ascii_uppercase())),
        };
        Response::reply(reply)
    }

    /// `HELLO [protover [AUTH username password] [SETNAME name]]`.
    ///
    /// Everything is parsed and checked before anything is applied, so a
    /// request that fails part-way, on a bad option or a wrong password,
    /// leaves the connection exactly as it was.
    fn hello(&self, connection: &mut Connection, mut args: Args<'_>) -> RedisResult<Reply> {
        let mut protocol = connection.protocol;
        if let Some(version) = args
            .remaining()
            .first()
            .and_then(|value| parse_integer(value))
        {
            args.next()?;
            protocol = match version {
                2 => Protocol::Resp2,
                3 => Protocol::Resp3,
                other => {
                    return Err(RedisError::new(
                        "NOPROTO",
                        format!("unsupported protocol version {other}"),
                    ));
                }
            };
        }
        let mut credentials = None;
        let mut name = None;
        while let Some(option) = args.next_keyword() {
            match option.as_str() {
                "auth" => credentials = Some((args.next()?, args.next()?)),
                "setname" => name = Some(args.next_owned()?),
                _ => return Err(RedisError::syntax()),
            }
        }
        if let Some((user, password)) = credentials {
            self.check_credentials(Some(user), password)?;
            connection.authenticated = true;
        }
        if name.is_some() {
            connection.name = name;
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

    /// `AUTH password` and `AUTH user password` are both accepted; the gateway
    /// has one password and one user, `default`.
    fn auth(&self, connection: &mut Connection, mut args: Args<'_>) -> RedisResult<Reply> {
        let first = args.next()?;
        let credentials = match args.next_optional() {
            Some(password) => (Some(first), password),
            None => (None, first),
        };
        args.finish()?;
        self.check_credentials(credentials.0, credentials.1)?;
        connection.authenticated = true;
        Ok(Reply::status())
    }

    fn check_credentials(&self, user: Option<&[u8]>, password: &[u8]) -> RedisResult<()> {
        let Some(expected) = &self.password else {
            return Err(RedisError::new(
                "ERR",
                "Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?",
            ));
        };
        let user_ok = user.is_none_or(|user| keyword(user) == "default");
        if user_ok && expected.as_bytes() == password {
            Ok(())
        } else {
            Err(RedisError::wrong_pass())
        }
    }

    fn client_command(&self, connection: &mut Connection, mut args: Args<'_>) -> Response {
        let Some(subcommand) = args.next_keyword() else {
            return Response::error(RedisError::wrong_arity("client"));
        };
        if subcommand == "reply" {
            let mode = match args.next_keyword().as_deref() {
                Some("on") => ReplyMode::On,
                Some("off") => ReplyMode::Off,
                Some("skip") => ReplyMode::Skip,
                _ => return Response::error(RedisError::syntax()),
            };
            return match args.finish() {
                Ok(()) => Response {
                    reply: Ok(Reply::status()),
                    effect: Effect::ReplyMode(mode),
                },
                Err(error) => Response::error(error),
            };
        }
        Response::reply(client_subcommand(connection, &subcommand, args))
    }

    fn reset(&self, connection: &mut Connection, args: Args<'_>) -> RedisResult<Reply> {
        args.finish()?;
        connection.protocol = Protocol::Resp2;
        connection.name = None;
        connection.authenticated = self.password.is_none();
        connection.reply_mode = ReplyMode::On;
        Ok(Reply::Simple("RESET".into()))
    }

    fn info(&self, mut args: Args<'_>) -> RedisResult<Reply> {
        let section = args.next_keyword();
        args.finish().map_err(|_| RedisError::wrong_arity("info"))?;
        let uptime = self.started_at.elapsed().as_secs();
        let mut body = match section.as_deref() {
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
}

fn ping(mut args: Args<'_>) -> RedisResult<Reply> {
    let message = args.next_optional();
    args.finish().map_err(|_| RedisError::wrong_arity("ping"))?;
    Ok(match message {
        None => Reply::Simple("PONG".into()),
        Some(message) => Reply::bulk(message.to_vec()),
    })
}

fn echo(mut args: Args<'_>) -> RedisResult<Reply> {
    Ok(Reply::bulk(args.next_owned()?))
}

fn select(mut args: Args<'_>) -> RedisResult<Reply> {
    // Every key lives in one flat namespace, so only database 0 exists.
    match parse_integer(args.next()?) {
        Some(0) => Ok(Reply::status()),
        _ => Err(RedisError::new(
            "ERR",
            "DB index is out of range. This gateway serves one database, 0",
        )),
    }
}

fn client_subcommand(
    connection: &mut Connection,
    subcommand: &str,
    mut args: Args<'_>,
) -> RedisResult<Reply> {
    match subcommand {
        "id" => Ok(Reply::Integer(connection.id as i64)),
        "setname" => {
            let name = args.next_owned()?;
            args.finish()?;
            connection.name = Some(name);
            Ok(Reply::status())
        }
        "getname" => Ok(match &connection.name {
            Some(name) => Reply::bulk(name.clone()),
            None => Reply::Nil,
        }),
        // Clients send `CLIENT SETINFO lib-name ...` on connect and expect a
        // plain OK; nothing is recorded.
        "setinfo" => {
            args.next()?;
            args.next()?;
            args.finish()?;
            Ok(Reply::status())
        }
        "no-evict" | "no-touch" | "no-evict-on-expiry" | "unpause" | "tracking" => {
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

fn command_command(dispatcher: &Dispatcher, mut args: Args<'_>) -> RedisResult<Reply> {
    match args.next_keyword().as_deref() {
        None => Ok(Reply::Array(
            dispatcher.commands().map(CommandInfo::to_reply).collect(),
        )),
        Some("count") => Ok(Reply::Array(vec![Reply::Integer(
            dispatcher.commands().count() as i64,
        )])),
        // Documentation is for commands this gateway does not implement, so
        // there is none to send.
        Some("docs") => Ok(Reply::Array(Vec::new())),
        Some("info") => Ok(Reply::Array(
            args.remaining()
                .iter()
                .map(
                    |name| match dispatcher.command(&String::from_utf8_lossy(name)) {
                        Some(command) => command.to_reply(),
                        None => Reply::Nil,
                    },
                )
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

fn config(mut args: Args<'_>) -> RedisResult<Reply> {
    match args.next_keyword().as_deref() {
        Some("get") => {
            let pattern = super::glob::Pattern::new(keyword(args.next()?).as_bytes());
            Ok(Reply::Map(
                config_entries()
                    .into_iter()
                    .filter(|(name, _)| pattern.matches(name.as_bytes()))
                    .map(|(name, value)| (name.as_bytes().to_vec(), Reply::bulk(value)))
                    .collect(),
            ))
        }
        Some("set") => {
            let pairs = args.remaining();
            if pairs.is_empty() || !pairs.len().is_multiple_of(2) {
                return Err(RedisError::syntax());
            }
            for name in pairs.iter().step_by(2) {
                let name = keyword(name);
                if !config_entries().into_iter().any(|(known, _)| known == name) {
                    return Err(RedisError::new(
                        "ERR",
                        format!("Unknown option or number of arguments for CONFIG SET - '{name}'"),
                    ));
                }
            }
            // Nothing is stored: every parameter is either already the only
            // value that can hold, or names a feature this gateway does not
            // have. Redis answers OK for a parameter it accepts.
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

/// `CONFIG GET` takes the same glob as `KEYS`.
#[cfg(test)]
fn matches_pattern(pattern: &str, name: &str) -> bool {
    super::glob::Pattern::new(pattern.as_bytes()).matches(name.as_bytes())
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
    use std::{sync::Arc, time::Duration};

    use super::*;
    use crate::{client::SharedClient, resp::Limits};

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

    async fn run(dispatcher: &Dispatcher, connection: &mut Connection, parts: &[&str]) -> Response {
        let args: Vec<Vec<u8>> = parts.iter().map(|part| part.as_bytes().to_vec()).collect();
        dispatcher.execute(connection, &args).await
    }

    #[tokio::test]
    async fn hello_authenticates_and_switches_protocol_in_one_request() {
        let dispatcher = dispatcher(Some("secret"));
        let mut connection = Connection::new(1, false);
        let response = run(
            &dispatcher,
            &mut connection,
            &["HELLO", "3", "AUTH", "default", "secret", "SETNAME", "app"],
        )
        .await;
        assert!(response.reply.is_ok(), "{:?}", response.reply);
        assert!(connection.authenticated);
        assert_eq!(connection.protocol, Protocol::Resp3);
        assert_eq!(connection.name.as_deref(), Some(b"app".as_slice()));
    }

    #[tokio::test]
    async fn a_failed_hello_changes_nothing() {
        let dispatcher = dispatcher(Some("secret"));
        let mut connection = Connection::new(1, false);
        for parts in [
            &["HELLO", "3", "AUTH", "default", "wrong"][..],
            &["HELLO", "3", "AUTH", "default"][..],
            &["HELLO", "3", "SETNAME", "app", "BOGUS"][..],
        ] {
            assert!(
                run(&dispatcher, &mut connection, parts)
                    .await
                    .reply
                    .is_err()
            );
            assert!(!connection.authenticated);
            assert_eq!(connection.protocol, Protocol::Resp2);
            assert_eq!(connection.name, None);
        }
    }

    #[tokio::test]
    async fn hello_without_a_version_still_reads_its_options() {
        let dispatcher = dispatcher(Some("secret"));
        let mut connection = Connection::new(1, false);
        let response = run(
            &dispatcher,
            &mut connection,
            &["HELLO", "AUTH", "default", "secret"],
        )
        .await;
        assert!(response.reply.is_ok(), "{:?}", response.reply);
        assert!(connection.authenticated);
    }

    #[tokio::test]
    async fn client_reply_reports_its_switch_only_when_it_parses() {
        let dispatcher = dispatcher(None);
        let mut connection = Connection::new(1, true);
        let off = run(&dispatcher, &mut connection, &["CLIENT", "REPLY", "OFF"]).await;
        assert_eq!(off.effect, Effect::ReplyMode(ReplyMode::Off));
        let bogus = run(&dispatcher, &mut connection, &["CLIENT", "REPLY", "BOGUS"]).await;
        assert!(bogus.reply.is_err());
        assert_eq!(bogus.effect, Effect::None);
    }

    #[tokio::test]
    async fn quit_answers_and_closes() {
        let dispatcher = dispatcher(None);
        let mut connection = Connection::new(1, true);
        let quit = run(&dispatcher, &mut connection, &["QUIT"]).await;
        assert_eq!(quit.reply.unwrap(), Reply::status());
        assert_eq!(quit.effect, Effect::Close);
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
        assert!(!matches_pattern("[^s]*", "save"));
    }
}
