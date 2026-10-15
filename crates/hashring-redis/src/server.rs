//! Accepting connections and running each one's commands in order.
//!
//! One connection is one task. Commands on a connection are answered in the
//! order they arrive, which is what a Redis client expects and what lets a
//! pipelined burst be written back as one reply stream.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    commands::{Connection, Dispatcher, ReplyMode},
    error::RedisError,
    resp::{self, Frame, Reply},
};

/// How much is read from a socket at a time. Large enough that a pipelined
/// burst usually arrives whole, small enough not to hold a large read buffer
/// per connection.
const READ_CHUNK: usize = 64 * 1024;

pub struct Gateway {
    dispatcher: Arc<Dispatcher>,
    shutdown: tokio::sync::watch::Receiver<bool>,
}

impl Gateway {
    pub fn new(dispatcher: Arc<Dispatcher>, shutdown: tokio::sync::watch::Receiver<bool>) -> Self {
        Self {
            dispatcher,
            shutdown,
        }
    }

    /// Serves until the process is asked to stop.
    pub async fn serve(self, listener: TcpListener) -> std::io::Result<()> {
        let mut shutdown = self.shutdown;
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return Ok(());
                    }
                }
                accepted = listener.accept() => {
                    let (socket, address) = accepted?;
                    let dispatcher = self.dispatcher.clone();
                    tokio::spawn(async move {
                        if let Err(error) = serve_connection(dispatcher, socket).await {
                            tracing::debug!(%address, %error, "redis gateway connection ended");
                        }
                    });
                }
            }
        }
    }
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
        let mut consumed = 0;
        loop {
            match resp::parse(&pending[consumed..], &dispatcher.limits()) {
                Frame::Command { args, length } => {
                    consumed += length;
                    if args.is_empty() {
                        continue;
                    }
                    match run(&dispatcher, &mut connection, &args).await {
                        Outcome::Reply(reply) => {
                            resp::encode(&reply, connection.protocol, &mut replies);
                        }
                        Outcome::Silent => {}
                        Outcome::Close => {
                            writer.write_all(&replies).await?;
                            return Ok(());
                        }
                    }
                }
                Frame::Incomplete => break,
                Frame::Invalid(message) => {
                    // The stream cannot be resynchronized after a bad frame,
                    // so the error goes out and the connection ends.
                    let error = RedisError::protocol(message);
                    resp::encode(&error.to_reply(), connection.protocol, &mut replies);
                    writer.write_all(&replies).await?;
                    return Ok(());
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

/// What running one command did to the connection.
enum Outcome {
    /// Write this reply.
    Reply(Reply),
    /// `CLIENT REPLY OFF` or `SKIP` was in effect.
    Silent,
    /// `QUIT`: write whatever is pending and close.
    Close,
}

async fn run(dispatcher: &Dispatcher, connection: &mut Connection, args: &[Vec<u8>]) -> Outcome {
    let name = args
        .first()
        .map(|name| String::from_utf8_lossy(name).to_lowercase())
        .unwrap_or_default();
    // `CLIENT REPLY` answers for itself even when it turns replies off: the
    // client is asking to be told that the setting changed.
    let is_reply_mode = name == "client"
        && args
            .get(1)
            .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case(b"reply"));
    let reply = dispatcher.execute(connection, args).await;
    if name == "quit" {
        return Outcome::Close;
    }
    if is_reply_mode {
        return Outcome::Reply(Reply::status());
    }
    match connection.reply_mode {
        ReplyMode::On => Outcome::Reply(reply.unwrap_or_else(|error| error.to_reply())),
        ReplyMode::Skip => {
            connection.reply_mode = ReplyMode::On;
            Outcome::Silent
        }
        ReplyMode::Off => Outcome::Silent,
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
    async fn dispatcher() -> Arc<Dispatcher> {
        Arc::new(Dispatcher::new(
            Arc::new(crate::client::SharedClient::new(
                "http://127.0.0.1:1".into(),
                std::time::Duration::from_millis(50),
            )),
            None,
            crate::resp::Limits::default(),
        ))
    }

    #[tokio::test]
    async fn a_command_is_answered_while_replies_are_on() {
        let mut connection = connection();
        let reply = run(
            dispatcher().await.as_ref(),
            &mut connection,
            &args(&[b"PING"]),
        )
        .await;
        assert!(matches!(reply, Outcome::Reply(Reply::Simple(text)) if text == "PONG"));
    }

    #[tokio::test]
    async fn reply_off_silences_the_next_command_and_answers_the_switch() {
        let mut connection = connection();
        let dispatcher = dispatcher().await;
        let switching = run(
            &dispatcher,
            &mut connection,
            &args(&[b"CLIENT", b"REPLY", b"OFF"]),
        )
        .await;
        // The switch itself is answered, or a client could never tell whether
        // it took effect.
        assert!(matches!(switching, Outcome::Reply(_)));
        assert!(matches!(
            run(&dispatcher, &mut connection, &args(&[b"PING"])).await,
            Outcome::Silent
        ));
        let restoring = run(
            &dispatcher,
            &mut connection,
            &args(&[b"CLIENT", b"REPLY", b"ON"]),
        )
        .await;
        assert!(matches!(restoring, Outcome::Reply(_)));
        assert!(matches!(
            run(&dispatcher, &mut connection, &args(&[b"PING"])).await,
            Outcome::Reply(_)
        ));
    }

    #[tokio::test]
    async fn reply_skip_silences_exactly_one_command() {
        let mut connection = connection();
        let dispatcher = dispatcher().await;
        assert!(matches!(
            run(
                &dispatcher,
                &mut connection,
                &args(&[b"CLIENT", b"REPLY", b"SKIP"])
            )
            .await,
            Outcome::Reply(_)
        ));
        assert!(matches!(
            run(&dispatcher, &mut connection, &args(&[b"PING"])).await,
            Outcome::Silent
        ));
        assert!(matches!(
            run(&dispatcher, &mut connection, &args(&[b"PING"])).await,
            Outcome::Reply(_)
        ));
    }

    #[tokio::test]
    async fn quit_closes_the_connection() {
        let mut connection = connection();
        assert!(matches!(
            run(
                dispatcher().await.as_ref(),
                &mut connection,
                &args(&[b"QUIT"])
            )
            .await,
            Outcome::Close
        ));
    }

    #[tokio::test]
    async fn a_failing_command_is_answered_rather_than_dropped() {
        let mut connection = connection();
        // No coordinator is listening, so this reaches the cluster and fails.
        let outcome = run(
            dispatcher().await.as_ref(),
            &mut connection,
            &args(&[b"GET", b"key"]),
        )
        .await;
        assert!(matches!(outcome, Outcome::Reply(Reply::Error(_))));
    }
}
