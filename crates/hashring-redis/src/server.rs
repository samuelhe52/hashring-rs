//! Accepting connections and running each one's commands in order.
//!
//! One connection is one task. Commands on a connection are answered in the
//! order they arrive, which is what a Redis client expects and what lets a
//! pipelined burst be written back as one reply stream.
//!
//! This module owns what a command does to its connection: a command returns
//! its reply and an [`Effect`], and only here are replies suppressed, the
//! reply mode switched, or the connection closed. It also owns the bounds on a
//! connection: how many may be open, how much a client may have sent and not
//! yet had run, and how much reply output is held before it is written.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};

use crate::{
    commands::{Connection, Dispatcher, Effect, ReplyMode, Response},
    error::RedisError,
    resp::{self, Frame, Reply},
};

/// How much is read from a socket at a time. Large enough that a pipelined
/// burst usually arrives whole, small enough not to hold a large read buffer
/// per connection.
const READ_CHUNK: usize = 64 * 1024;

/// Replies held for one pipelined burst before they are written out, so a
/// burst of large `GET`s does not hold every value in memory at once.
const REPLY_FLUSH_BYTES: usize = 1024 * 1024;

/// How long the listener waits after an `accept` failure that means the
/// process is out of file descriptors, so it does not spin on the same error.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

pub struct Gateway {
    dispatcher: Arc<Dispatcher>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    max_connections: usize,
}

impl Gateway {
    pub fn new(
        dispatcher: Arc<Dispatcher>,
        shutdown: tokio::sync::watch::Receiver<bool>,
        max_connections: usize,
    ) -> Self {
        Self {
            dispatcher,
            shutdown,
            max_connections,
        }
    }

    /// Serves until the process is asked to stop. A failed `accept` loses that
    /// one connection, never the listener.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        let mut shutdown = self.shutdown;
        let permits = Arc::new(Semaphore::new(self.max_connections));
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return Ok(());
                    }
                }
                accepted = listener.accept() => {
                    let (socket, address) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            tracing::warn!(%error, "redis gateway could not accept a connection");
                            if is_resource_exhaustion(&error) {
                                tokio::time::sleep(ACCEPT_BACKOFF).await;
                            }
                            continue;
                        }
                    };
                    let dispatcher = self.dispatcher.clone();
                    let permit = permits.clone().try_acquire_owned();
                    tokio::spawn(async move {
                        let result = match permit {
                            Ok(_permit) => serve_connection(dispatcher, socket).await,
                            Err(_) => refuse(socket).await,
                        };
                        if let Err(error) = result {
                            tracing::debug!(%address, %error, "redis gateway connection ended");
                        }
                    });
                }
            }
        }
    }
}

fn is_resource_exhaustion(error: &std::io::Error) -> bool {
    // EMFILE and ENFILE: the process or the system is out of descriptors.
    matches!(error.raw_os_error(), Some(23 | 24))
}

/// Redis's answer once `maxclients` connections are open.
async fn refuse(mut socket: TcpStream) -> std::io::Result<()> {
    let mut out = Vec::new();
    let error = RedisError::new("ERR", "max number of clients reached");
    resp::encode(&error.to_reply(), resp::Protocol::Resp2, &mut out);
    socket.write_all(&out).await
}

async fn serve_connection(dispatcher: Arc<Dispatcher>, socket: TcpStream) -> std::io::Result<()> {
    // A reply that waits on another request to be written is a reply that
    // waits on the cluster, so Nagle would add latency to every command that is
    // not part of a pipeline.
    let _ = socket.set_nodelay(true);
    let (mut reader, mut writer) = socket.into_split();
    let mut connection = Connection::new(next_connection_id(), !dispatcher.requires_auth());
    let mut pending: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let mut chunk = vec![0_u8; READ_CHUNK];
    let mut replies = Vec::new();
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            // The client closed between commands, which is not an error.
            return Ok(());
        }
        pending.extend_from_slice(&chunk[..read]);
        // Checked on what has arrived, before anything in it runs, so the
        // bound does not depend on how the socket happened to split the bytes.
        let buffer_limit = match connection.authenticated {
            true => dispatcher.limits(),
            false => dispatcher.limits().unauthenticated(),
        }
        .max_query_buffer_bytes;
        if pending.len() > buffer_limit {
            return close_with(
                &mut writer,
                &mut replies,
                &connection,
                "query buffer limit reached",
            )
            .await;
        }
        let mut consumed = 0;
        loop {
            // A connection is held to the handshake's bounds until it has
            // authenticated, which can change between two pipelined requests.
            let limits = match connection.authenticated {
                true => dispatcher.limits(),
                false => dispatcher.limits().unauthenticated(),
            };
            match resp::parse(&pending[consumed..], &limits) {
                Frame::Command { args, length } => {
                    consumed += length;
                    if args.is_empty() {
                        continue;
                    }
                    let response = dispatcher.execute(&mut connection, &args).await;
                    let (reply, close) = settle(&mut connection, response);
                    if let Some(reply) = reply {
                        resp::encode(&reply, connection.protocol, &mut replies);
                    }
                    if close {
                        writer.write_all(&replies).await?;
                        return Ok(());
                    }
                    if replies.len() >= REPLY_FLUSH_BYTES {
                        writer.write_all(&replies).await?;
                        replies.clear();
                    }
                }
                Frame::Incomplete => break,
                Frame::Invalid(message) => {
                    // The stream cannot be resynchronized after a bad frame,
                    // so the error goes out and the connection ends.
                    return close_with(&mut writer, &mut replies, &connection, &message).await;
                }
            }
        }
        pending.drain(..consumed);
        if !replies.is_empty() {
            writer.write_all(&replies).await?;
            replies.clear();
        }
    }
}

async fn close_with(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    replies: &mut Vec<u8>,
    connection: &Connection,
    message: &str,
) -> std::io::Result<()> {
    let error = RedisError::protocol(format!("Protocol error: {message}"));
    resp::encode(&error.to_reply(), connection.protocol, replies);
    writer.write_all(replies).await
}

/// Applies a command's effect to its connection and decides whether its reply
/// is written: the reply to send, if any, and whether to close afterwards.
fn settle(connection: &mut Connection, response: Response) -> (Option<Reply>, bool) {
    let reply = response.reply.unwrap_or_else(|error| error.to_reply());
    match response.effect {
        // `ON` is answered; `OFF` and `SKIP` are not, which is how Redis tells
        // a client the switch took effect. `SKIP` while replies are off
        // changes nothing, as in Redis.
        Effect::ReplyMode(mode) => {
            if !(mode == ReplyMode::Skip && connection.reply_mode == ReplyMode::Off) {
                connection.reply_mode = mode;
            }
            ((mode == ReplyMode::On).then_some(reply), false)
        }
        Effect::Close => (visible(connection, reply), true),
        Effect::None => (visible(connection, reply), false),
    }
}

/// The reply as the connection's reply mode lets it through, consuming a
/// pending `SKIP`.
fn visible(connection: &mut Connection, reply: Reply) -> Option<Reply> {
    match connection.reply_mode {
        ReplyMode::On => Some(reply),
        ReplyMode::Skip => {
            connection.reply_mode = ReplyMode::On;
            None
        }
        ReplyMode::Off => None,
    }
}

fn next_connection_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connection() -> Connection {
        Connection::new(1, true)
    }

    fn args(parts: &[&[u8]]) -> Vec<Vec<u8>> {
        parts.iter().map(|part| part.to_vec()).collect()
    }

    /// A dispatcher is only needed for the handshake, which these do not reach.
    fn dispatcher() -> Arc<Dispatcher> {
        Arc::new(Dispatcher::new(
            Arc::new(crate::client::SharedClient::new(
                "http://127.0.0.1:1".into(),
                Duration::from_millis(50),
            )),
            None,
            crate::resp::Limits::default(),
            Duration::from_millis(200),
        ))
    }

    async fn run(
        dispatcher: &Dispatcher,
        connection: &mut Connection,
        parts: &[&[u8]],
    ) -> (Option<Reply>, bool) {
        let response = dispatcher.execute(connection, &args(parts)).await;
        settle(connection, response)
    }

    #[tokio::test]
    async fn a_command_is_answered_while_replies_are_on() {
        let mut connection = connection();
        let (reply, _) = run(&dispatcher(), &mut connection, &[b"PING"]).await;
        assert!(matches!(reply, Some(Reply::Simple(text)) if text == "PONG"));
    }

    #[tokio::test]
    async fn reply_off_is_silent_until_reply_on_answers() {
        let mut connection = connection();
        let dispatcher = dispatcher();
        // Redis answers neither `OFF` nor anything after it.
        let switching = run(&dispatcher, &mut connection, &[b"CLIENT", b"REPLY", b"OFF"]).await;
        assert_eq!(switching, (None, false));
        assert_eq!(
            run(&dispatcher, &mut connection, &[b"PING"]).await,
            (None, false)
        );
        let restoring = run(&dispatcher, &mut connection, &[b"CLIENT", b"REPLY", b"ON"]).await;
        assert_eq!(restoring, (Some(Reply::status()), false));
        assert!(
            run(&dispatcher, &mut connection, &[b"PING"])
                .await
                .0
                .is_some()
        );
    }

    #[tokio::test]
    async fn reply_skip_silences_itself_and_exactly_one_command() {
        let mut connection = connection();
        let dispatcher = dispatcher();
        let skip = run(
            &dispatcher,
            &mut connection,
            &[b"CLIENT", b"REPLY", b"SKIP"],
        )
        .await;
        assert_eq!(skip, (None, false));
        assert_eq!(
            run(&dispatcher, &mut connection, &[b"PING"]).await,
            (None, false)
        );
        assert!(
            run(&dispatcher, &mut connection, &[b"PING"])
                .await
                .0
                .is_some()
        );
    }

    #[tokio::test]
    async fn a_malformed_reply_switch_is_answered_and_changes_nothing() {
        let mut connection = connection();
        let (reply, _) = run(
            &dispatcher(),
            &mut connection,
            &[b"CLIENT", b"REPLY", b"MAYBE"],
        )
        .await;
        assert!(matches!(reply, Some(Reply::Error(_))));
        assert_eq!(connection.reply_mode, ReplyMode::On);
    }

    #[tokio::test]
    async fn quit_is_answered_before_the_connection_closes() {
        let mut connection = connection();
        let quit = run(&dispatcher(), &mut connection, &[b"QUIT"]).await;
        assert_eq!(quit, (Some(Reply::status()), true));
    }

    #[tokio::test]
    async fn a_failing_command_is_answered_rather_than_dropped() {
        let mut connection = connection();
        // No coordinator is listening, so this reaches the cluster and fails.
        let (reply, close) = run(&dispatcher(), &mut connection, &[b"GET", b"key"]).await;
        assert!(matches!(reply, Some(Reply::Error(_))));
        assert!(!close);
    }

    #[tokio::test]
    async fn connections_past_the_limit_are_refused_with_the_redis_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_stop, shutdown) = tokio::sync::watch::channel(false);
        tokio::spawn(Gateway::new(dispatcher(), shutdown, 1).serve(listener));
        let mut first = TcpStream::connect(address).await.unwrap();
        first.write_all(b"PING\r\n").await.unwrap();
        let mut answer = [0_u8; 7];
        first.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"+PONG\r\n");

        let mut second = TcpStream::connect(address).await.unwrap();
        let mut refused = Vec::new();
        second.read_to_end(&mut refused).await.unwrap();
        assert_eq!(refused, b"-ERR max number of clients reached\r\n");
    }

    #[tokio::test]
    async fn an_unauthenticated_client_cannot_make_the_gateway_buffer_a_large_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_stop, shutdown) = tokio::sync::watch::channel(false);
        let dispatcher = Arc::new(Dispatcher::new(
            Arc::new(crate::client::SharedClient::new(
                "http://127.0.0.1:1".into(),
                Duration::from_millis(50),
            )),
            Some(Arc::from("secret")),
            crate::resp::Limits::default(),
            Duration::from_millis(200),
        ));
        tokio::spawn(Gateway::new(dispatcher, shutdown, 8).serve(listener));
        let mut client = TcpStream::connect(address).await.unwrap();
        // A single argument announced at a megabyte is refused on its header.
        client.write_all(b"*1\r\n$1048576\r\n").await.unwrap();
        let mut refused = Vec::new();
        client.read_to_end(&mut refused).await.unwrap();
        assert!(
            refused.starts_with(b"-ERR Protocol error"),
            "{:?}",
            String::from_utf8_lossy(&refused)
        );
    }

    #[tokio::test]
    async fn the_query_buffer_limit_holds_for_a_request_that_arrives_whole() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_stop, shutdown) = tokio::sync::watch::channel(false);
        let dispatcher = Arc::new(Dispatcher::new(
            Arc::new(crate::client::SharedClient::new(
                "http://127.0.0.1:1".into(),
                Duration::from_millis(50),
            )),
            None,
            crate::resp::Limits {
                max_query_buffer_bytes: 32,
                ..Default::default()
            },
            Duration::from_millis(200),
        ));
        tokio::spawn(Gateway::new(dispatcher, shutdown, 8).serve(listener));
        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"*2\r\n$4\r\nECHO\r\n$40\r\n0123456789012345678901234567890123456789\r\n")
            .await
            .unwrap();
        let mut refused = Vec::new();
        client.read_to_end(&mut refused).await.unwrap();
        assert!(
            refused.starts_with(b"-ERR Protocol error: query buffer limit reached"),
            "{:?}",
            String::from_utf8_lossy(&refused)
        );
    }
}
