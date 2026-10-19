//! Commands over the whole keyspace rather than named keys: `KEYS`, `SCAN`,
//! `DBSIZE` and `RANDOMKEY`.
//!
//! `KEYS` and `DBSIZE` walk every owner through the client (see
//! `hashring_client::HashringClient::keys`), which starts over if the topology
//! changes underneath it. So an answer covers every key that was live for the
//! whole command; a key written or deleted while it ran may or may not count.
//!
//! `SCAN` goes round the ring in token order instead, a stretch at a time (see
//! `hashring_client::HashringClient::scan`), and its cursor is the token to
//! continue from. A token means the same thing in every epoch and at every
//! gateway, so a scan survives membership changes and may be continued
//! through any gateway, and returns a key that was live throughout exactly
//! once, where Redis promises only at least once.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use hashring_client::ClientError;
use hashring_core::proto::ErrorCode;

use super::{
    args::{Args, keyword, parse_integer},
    glob,
    store::Store,
};
use crate::{
    error::{RedisError, RedisResult},
    resp::Reply,
};

/// The most `KEYS` or one `SCAN` gathers before giving up, counting each key's
/// bytes plus the client's per-key overhead. Redis has no such limit, but it
/// answers from memory it already holds; a gateway would have to buffer every
/// node's keys, so `KEYS *` on a large cluster could exhaust it for every
/// other client.
pub const MAX_KEYS_REPLY_BYTES: usize = 64 * 1024 * 1024;

/// Pattern steps one `KEYS` or `SCAN` may spend matching, across every key it
/// sees. Matching runs synchronously as pages arrive, where the command
/// deadline cannot interrupt it, so a pattern built to be slow has to be cut
/// off here. At a few nanoseconds a step this is well under a second.
pub const MAX_KEYS_MATCH_STEPS: u64 = 100_000_000;

/// `SCAN`'s `COUNT` when none is given, as in Redis.
const DEFAULT_SCAN_COUNT: usize = 10;

/// A `MATCH` pattern and the matching budget of the command that applies it.
struct Matcher {
    pattern: Option<glob::Pattern>,
    remaining: AtomicU64,
    exhausted: AtomicBool,
}

impl Matcher {
    fn new(pattern: Option<&[u8]>) -> Self {
        Self {
            pattern: pattern.map(glob::Pattern::new),
            remaining: AtomicU64::new(MAX_KEYS_MATCH_STEPS),
            exhausted: AtomicBool::new(false),
        }
    }

    fn keep(&self, key: &[u8]) -> bool {
        let Some(pattern) = &self.pattern else {
            return true;
        };
        let before = self.remaining.load(Ordering::Relaxed);
        let mut budget = before;
        let matched = pattern.matches_within(key, &mut budget);
        // Owners' pages are matched concurrently, so another may have spent
        // from the same budget since it was read.
        let spent = before - budget;
        let _ = self
            .remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                Some(left.saturating_sub(spent))
            });
        matched.unwrap_or_else(|| {
            self.exhausted.store(true, Ordering::Relaxed);
            false
        })
    }

    /// An error if the budget ran out, so some keys went unmatched.
    fn finish(&self, command: &str) -> RedisResult<()> {
        if self.exhausted.load(Ordering::Relaxed) {
            return Err(RedisError::new(
                "ERR",
                format!("the {command} pattern takes too much work to match; simplify it"),
            ));
        }
        Ok(())
    }
}

/// The client's `TooLarge` for a gathering that passed its byte limit, as an
/// error that says which command and what to do about it.
fn too_many_keys(
    command: &'static str,
    advice: &'static str,
) -> impl Fn(ClientError) -> RedisError {
    move |error| match error {
        ClientError::Operation(failure) if failure.code == ErrorCode::TooLarge => RedisError::new(
            "ERR",
            format!("{command} gathered more than {MAX_KEYS_REPLY_BYTES} bytes of keys; {advice}"),
        ),
        other => other.into(),
    }
}

pub async fn keys(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let matcher = Matcher::new(Some(args.next()?));
    let keys = store
        .client()
        .await?
        .keys(|key| matcher.keep(key), MAX_KEYS_REPLY_BYTES)
        .await
        .map_err(too_many_keys("KEYS", "narrow the pattern"))?;
    matcher.finish("KEYS")?;
    Ok(Reply::Array(keys.into_iter().map(Reply::bulk).collect()))
}

/// `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`.
pub async fn scan(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let cursor = std::str::from_utf8(args.next()?)
        .ok()
        .and_then(|cursor| cursor.parse::<u64>().ok())
        .ok_or_else(|| RedisError::new("ERR", "invalid cursor"))?;
    let mut pattern = None;
    let mut count = DEFAULT_SCAN_COUNT;
    let mut strings_only = true;
    while let Some(option) = args.next_keyword() {
        match option.as_str() {
            "match" => pattern = Some(args.next()?),
            "count" => {
                count = parse_integer(args.next()?)
                    .ok_or_else(RedisError::not_an_integer)?
                    .try_into()
                    .ok()
                    .filter(|count| *count >= 1)
                    .ok_or_else(RedisError::syntax)?;
            }
            // Every key here is a string, so naming any other type, one Redis
            // knows or not, selects nothing. Redis too accepts any name.
            "type" => strings_only = keyword(args.next()?) == "string",
            _ => return Err(RedisError::syntax()),
        }
    }
    // No key is of another type, so the scan is complete before it starts.
    if !strings_only {
        return Ok(scan_reply(0, Vec::new()));
    }
    let matcher = Matcher::new(pattern);
    let (keys, next) = store
        .client()
        .await?
        .scan(cursor, count, |key| matcher.keep(key), MAX_KEYS_REPLY_BYTES)
        .await
        .map_err(too_many_keys("SCAN", "use a smaller COUNT"))?;
    matcher.finish("SCAN")?;
    Ok(scan_reply(next.unwrap_or(0), keys))
}

/// A cursor is the decimal integer clients expect, `0` once the scan is done.
fn scan_reply(cursor: u64, keys: Vec<Vec<u8>>) -> Reply {
    Reply::Array(vec![
        Reply::bulk(cursor.to_string()),
        Reply::Array(keys.into_iter().map(Reply::bulk).collect()),
    ])
}

pub async fn dbsize(store: &Store<'_>) -> RedisResult<Reply> {
    let count = store.client().await?.key_count().await?;
    Ok(Reply::Integer(i64::try_from(count).unwrap_or(i64::MAX)))
}

pub async fn randomkey(store: &Store<'_>) -> RedisResult<Reply> {
    let key = store.client().await?.random_key().await?;
    Ok(Reply::Bulk(key))
}
