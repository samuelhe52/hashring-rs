//! Commands over the whole keyspace rather than named keys: `KEYS`, `DBSIZE`
//! and `RANDOMKEY`.
//!
//! Each walks every owner through the client (see
//! `hashring_client::HashringClient::keys`), which starts over if the topology
//! changes underneath it. So an answer covers every key that was live for the
//! whole command; a key written or deleted while it ran may or may not count,
//! as with Redis' own `SCAN`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use hashring_client::ClientError;
use hashring_core::proto::ErrorCode;

use super::{args::Args, glob, store::Store};
use crate::{
    error::{RedisError, RedisResult},
    resp::Reply,
};

/// The most `KEYS` gathers before giving up, counting each key's bytes plus
/// the client's per-key overhead. Redis has no such limit, but it answers
/// from memory it already holds; a gateway would have to buffer every node's
/// keys, so `KEYS *` on a large cluster could exhaust it for every other
/// client.
pub const MAX_KEYS_REPLY_BYTES: usize = 64 * 1024 * 1024;

/// Pattern steps one `KEYS` may spend matching, across every key it sees.
/// Matching runs synchronously as pages arrive, where the command deadline
/// cannot interrupt it, so a pattern built to be slow has to be cut off here.
/// At a few nanoseconds a step this is well under a second.
pub const MAX_KEYS_MATCH_STEPS: u64 = 100_000_000;

pub async fn keys(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let pattern = glob::Pattern::new(args.next()?);
    let remaining = AtomicU64::new(MAX_KEYS_MATCH_STEPS);
    let exhausted = AtomicBool::new(false);
    let keep = |key: &[u8]| {
        let before = remaining.load(Ordering::Relaxed);
        let mut budget = before;
        let matched = pattern.matches_within(key, &mut budget);
        // Owners' pages are matched concurrently, so another may have spent
        // from the same budget since it was read.
        let spent = before - budget;
        let _ = remaining.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
            Some(left.saturating_sub(spent))
        });
        matched.unwrap_or_else(|| {
            exhausted.store(true, Ordering::Relaxed);
            false
        })
    };
    let keys = store
        .client()
        .await?
        .keys(keep, MAX_KEYS_REPLY_BYTES)
        .await
        .map_err(|error| match error {
            ClientError::Operation(failure) if failure.code == ErrorCode::TooLarge => {
                RedisError::new(
                    "ERR",
                    format!(
                        "KEYS matched more than {MAX_KEYS_REPLY_BYTES} bytes of keys; \
                         narrow the pattern"
                    ),
                )
            }
            other => other.into(),
        })?;
    if exhausted.load(Ordering::Relaxed) {
        return Err(RedisError::new(
            "ERR",
            "the KEYS pattern takes too much work to match; simplify it",
        ));
    }
    Ok(Reply::Array(keys.into_iter().map(Reply::bulk).collect()))
}

pub async fn dbsize(store: &Store<'_>) -> RedisResult<Reply> {
    let count = store.client().await?.key_count().await?;
    Ok(Reply::Integer(i64::try_from(count).unwrap_or(i64::MAX)))
}

pub async fn randomkey(store: &Store<'_>) -> RedisResult<Reply> {
    let key = store.client().await?.random_key().await?;
    Ok(Reply::Bulk(key))
}
