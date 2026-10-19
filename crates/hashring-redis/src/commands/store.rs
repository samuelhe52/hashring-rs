//! How a command reaches the cluster.
//!
//! A read returns the key's value, deadline and version together. A command
//! that changes a key based on what it read goes through [`Store::update`],
//! which writes with a precondition on that version: the owning node applies
//! the write only if nothing else wrote the key in between, under the same
//! lock that applies it, and otherwise the command reads again and recomputes.
//! That is what makes `INCR` atomic against every other writer, whichever
//! gateway or client it came through. The gateway's own key locks only keep
//! its clients from colliding with each other, which saves retries; they no
//! longer carry correctness.

use std::future::Future;

use futures::{StreamExt, stream};
use hashring_client::{ClientError, Expiry, HashringClient, Precondition};
use hashring_core::proto::{ErrorCode, RecordVersion};

use crate::{
    client::{KeyLocks, SharedClient},
    error::{RedisError, RedisResult},
};

/// How many keys one command reaches the cluster with at once. A multi-key
/// command such as `MGET` on ten thousand keys would otherwise open ten
/// thousand requests at the nodes.
const FAN_OUT: usize = 64;

/// A key as one read saw it.
#[derive(Clone, Debug)]
pub struct Current {
    pub value: Vec<u8>,
    /// Absolute Unix milliseconds, or `None` for a key that never expires.
    pub deadline: Option<u64>,
    pub version: RecordVersion,
}

/// What a command decided to do with a key, given what it read, and what it
/// answers once that is done.
pub enum Change<T> {
    /// Store this value with this deadline (`None` for no deadline).
    Put {
        value: Vec<u8>,
        deadline: Option<u64>,
        reply: T,
    },
    /// Keep the value, store this deadline. Only for a key that exists.
    Expire { deadline: Option<u64>, reply: T },
    /// Delete the key. Only for a key that exists.
    Delete { reply: T },
    /// Write nothing.
    Nothing(T),
}

pub struct Store<'a> {
    client: &'a SharedClient,
    locks: &'a KeyLocks,
    /// The script this command runs inside, when it runs inside one. A plain
    /// write becomes conditional on what that script read, which is what makes
    /// a script that reads a key and then writes it atomic against every other
    /// writer rather than against one gateway's clients only.
    script: Option<&'a super::script::State>,
}

impl<'a> Store<'a> {
    pub fn new(
        client: &'a SharedClient,
        locks: &'a KeyLocks,
        script: Option<&'a super::script::State>,
    ) -> Self {
        Self {
            client,
            locks,
            script,
        }
    }

    pub async fn client(&self) -> RedisResult<HashringClient> {
        self.client.get().await
    }

    pub async fn read(&self, key: &[u8]) -> RedisResult<Option<Current>> {
        let current = read_with(&self.client().await?, key).await?;
        if let Some(script) = self.script {
            script.record(key, current.as_ref().map(|current| current.version.clone()));
        }
        Ok(current)
    }

    /// An unconditional write, for commands whose result does not depend on
    /// what the key held: `SET`, `SETEX`, `MSET`.
    ///
    /// Inside a script it is conditional on what the script read, and a key
    /// that changed underneath the script makes the script fail rather than
    /// overwrite the change.
    pub async fn put(&self, key: &[u8], value: Vec<u8>, expiry: Expiry) -> RedisResult<()> {
        let expected = match self.script {
            Some(script) => script.version(key),
            None => None,
        };
        let Some(expected) = expected else {
            self.client()
                .await?
                .put_with_expiry(key.to_vec(), value, expiry)
                .await?;
            return Ok(());
        };
        let applied = self
            .client()
            .await?
            .put_if(key.to_vec(), value, expiry, precondition(expected))
            .await?;
        match applied {
            Some(output) => {
                if let Some(script) = self.script {
                    script.written(key, output.version);
                }
                Ok(())
            }
            None => Err(RedisError::conflict(key)),
        }
    }

    /// Reads `key`, asks `decide` what to do, and applies that only if the key
    /// is still as read; otherwise reads again. `decide` may run more than
    /// once, so it computes from its argument and nothing else.
    ///
    /// The loop ends when a write lands or `decide` writes nothing. Contention
    /// keeps it going, which the command deadline bounds.
    pub async fn update<T>(
        &self,
        key: &[u8],
        mut decide: impl FnMut(Option<&Current>) -> RedisResult<Change<T>>,
    ) -> RedisResult<T> {
        // A script holds its declared keys for its whole run, so a command
        // inside it must not wait for a lock the script itself took. The
        // conditional write below is what keeps this correct, not the lock.
        let held = self.script.is_some_and(|_| true);
        let _guard = if held {
            None
        } else {
            Some(self.locks.lock(&[key]).await)
        };
        let client = self.client().await?;
        loop {
            let current = read_with(&client, key).await?;
            if let Some(script) = self.script {
                script.record(key, current.as_ref().map(|current| current.version.clone()));
            }
            let expected = match &current {
                Some(current) => Precondition::Version(current.version.clone()),
                None => Precondition::Absent,
            };
            let key = key.to_vec();
            // The version the key carries after the write, if one landed.
            let mut written: Option<Option<RecordVersion>> = None;
            let reply = match decide(current.as_ref())? {
                Change::Nothing(reply) => return Ok(reply),
                Change::Put {
                    value,
                    deadline,
                    reply,
                } => {
                    written = client
                        .put_if(key.clone(), value, expiry(deadline), expected)
                        .await?
                        .as_ref()
                        .map(|output| Some(output.version.clone()));
                    reply
                }
                Change::Expire { deadline, reply } => {
                    debug_assert!(current.is_some(), "an expiry change needs a key");
                    written = client
                        .set_expiry_if(key.clone(), deadline, expected)
                        .await?
                        .as_ref()
                        .map(|output| Some(output.version.clone()));
                    reply
                }
                Change::Delete { reply } => {
                    debug_assert!(current.is_some(), "a delete change needs a key");
                    if client.delete_if(key.clone(), expected).await?.is_some() {
                        // A delete leaves no record behind, so what a script
                        // saw afterwards is that the key is not there.
                        written = Some(None);
                    }
                    reply
                }
            };
            if let Some(version) = written {
                if let Some(script) = self.script {
                    script.record(&key, version);
                }
                return Ok(reply);
            }
        }
    }

    /// Runs `operation` on every key, at most [`FAN_OUT`] at a time, and
    /// returns the answers in argument order. Any key's failure fails the
    /// command, reported for the first failing key in argument order: a count
    /// or a value list built from the keys that happened to answer would be a
    /// wrong answer that looks like a right one.
    pub async fn each<'k, T, F, Fut>(
        &self,
        keys: &'k [Vec<u8>],
        operation: F,
    ) -> RedisResult<Vec<T>>
    where
        F: Fn(&'k [u8]) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        // The futures are built before the stream runs them, which keeps the
        // closure out of the state held across `.await`; a future built inside
        // the stream does not prove `Send` for every key lifetime.
        let operations: Vec<Fut> = keys.iter().map(|key| operation(key)).collect();
        let results: Vec<RedisResult<T>> =
            stream::iter(operations).buffered(FAN_OUT).collect().await;
        results.into_iter().collect()
    }

    /// [`Store::each`] for an operation that writes. A key that failed after
    /// another key's write landed leaves the command partly applied, which a
    /// retryable error would hide: a client that sent it again could, say,
    /// delete a key someone recreated in between. So the command fails with an
    /// error that says its writes may have been applied whenever any key
    /// succeeded or any failure may itself have applied, and with the first
    /// key's own error only when nothing was written.
    pub async fn each_write<'k, T, F, Fut>(
        &self,
        keys: &'k [Vec<u8>],
        operation: F,
    ) -> RedisResult<Vec<T>>
    where
        F: Fn(&'k [u8]) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        let operations: Vec<Fut> = keys.iter().map(|key| operation(key)).collect();
        let results: Vec<RedisResult<T>> =
            stream::iter(operations).buffered(FAN_OUT).collect().await;
        partial_write_outcome(results)
    }
}

fn partial_write_outcome<T>(results: Vec<RedisResult<T>>) -> RedisResult<Vec<T>> {
    let wrote = results.iter().any(Result::is_ok);
    let mut first = None;
    let mut ambiguous = false;
    for error in results.iter().filter_map(|result| result.as_ref().err()) {
        ambiguous |= error.may_have_applied;
        first.get_or_insert_with(|| error.clone());
    }
    match first {
        None => Ok(results
            .into_iter()
            .map(|result| result.ok().unwrap())
            .collect()),
        Some(error) if !wrote && !ambiguous => Err(error),
        Some(error) => Err(RedisError::outcome_unknown(&format!(
            "the command failed on some of its keys after others were written ({} {})",
            error.code, error.message
        ))),
    }
}

async fn read_with(client: &HashringClient, key: &[u8]) -> RedisResult<Option<Current>> {
    match client.get(key.to_vec()).await {
        Ok(output) => Ok(Some(Current {
            value: output.value,
            deadline: output.expires_at_unix_millis,
            version: output.version,
        })),
        Err(ClientError::Operation(failure)) if failure.code == ErrorCode::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// A conditional write states the deadline it computed from what it read, so
/// the node never resolves "keep" against a record other than that one.
pub fn expiry(deadline: Option<u64>) -> Expiry {
    deadline.map_or(Expiry::Clear, Expiry::At)
}

/// What a conditional write expects, given what the reader saw.
fn precondition(expected: Option<RecordVersion>) -> Precondition {
    match expected {
        Some(version) => Precondition::Version(version),
        None => Precondition::Absent,
    }
}

/// The deadline a write that keeps the key's deadline stores.
pub fn kept(current: Option<&Current>) -> Option<u64> {
    current.and_then(|current| current.deadline)
}

pub fn value_of(current: Option<&Current>) -> Option<Vec<u8>> {
    current.map(|current| current.value.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_partly_applied_write_is_never_a_plain_retry() {
        let busy = || RedisError::new("TRYAGAIN", "range is moving");
        let none_written: Vec<RedisResult<()>> = vec![Err(busy()), Err(busy())];
        assert_eq!(
            partial_write_outcome(none_written).unwrap_err().code,
            "TRYAGAIN"
        );

        let one_written: Vec<RedisResult<()>> = vec![Ok(()), Err(busy())];
        let error = partial_write_outcome(one_written).unwrap_err();
        assert!(error.may_have_applied && error.code == "ERR", "{error}");

        let unknown: Vec<RedisResult<()>> =
            vec![Err(busy()), Err(RedisError::outcome_unknown("timed out"))];
        assert!(partial_write_outcome(unknown).unwrap_err().may_have_applied);

        let all: Vec<RedisResult<u8>> = vec![Ok(1), Ok(2)];
        assert_eq!(partial_write_outcome(all).unwrap(), vec![1, 2]);
    }
}
