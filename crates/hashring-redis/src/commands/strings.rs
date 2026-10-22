//! String commands. Every value this gateway stores is a byte string.

use std::collections::HashMap;

use hashring_client::Expiry;

use super::{
    args::{Args, DeadlineMode, TimeUnit, TtlForm, float, parse_integer, positive_deadline},
    store::{Change, Current, Store, kept, value_of},
};
use crate::{
    error::{RedisError, RedisResult},
    resp::Reply,
};

fn bulk_or_nil(value: Option<Vec<u8>>) -> Reply {
    match value {
        Some(value) => Reply::bulk(value),
        None => Reply::Nil,
    }
}

fn too_large() -> RedisError {
    RedisError::new(
        "ERR",
        "string exceeds maximum allowed size (proto-max-bulk-len)",
    )
}

pub async fn get(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let key = args.next_required()?;
    Ok(bulk_or_nil(
        store.read(key).await?.map(|current| current.value),
    ))
}

pub async fn strlen(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let key = args.next_required()?;
    let length = store
        .read(key)
        .await?
        .map_or(0, |current| current.value.len());
    Ok(Reply::Integer(length as i64))
}

pub async fn getrange(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let key = args.next_required()?;
    let start = args.next_integer()?;
    let end = args.next_integer()?;
    let value = store
        .read(key)
        .await?
        .map(|current| current.value)
        .unwrap_or_default();
    Ok(Reply::bulk(slice_range(&value, start, end)))
}

pub async fn getdel(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let key = args.next_required()?;
    store
        .update(key, |current| {
            Ok(match current {
                None => Change::Nothing(Reply::Nil),
                Some(current) => Change::Delete {
                    reply: Reply::bulk(current.value.clone()),
                },
            })
        })
        .await
}

/// `GETEX key [EX s | PX ms | EXAT ts | PXAT ts | PERSIST]`. Without an option
/// it is a `GET`; with one it changes only the deadline.
pub async fn getex(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let key = args.next_required()?;
    let mut change: Option<Option<u64>> = None;
    while let Some(option) = args.next_keyword() {
        if change.is_some() {
            return Err(RedisError::syntax());
        }
        change = Some(match TtlForm::from_option(&option) {
            Some(form) => Some(positive_deadline(form, args.next_integer()?, "getex")?),
            None if option == "persist" => None,
            None => return Err(RedisError::syntax()),
        });
    }
    store
        .update(key, |current| {
            let Some(current) = current else {
                return Ok(Change::Nothing(Reply::Nil));
            };
            let reply = Reply::bulk(current.value.clone());
            Ok(match change {
                Some(deadline) if deadline != current.deadline => {
                    Change::Expire { deadline, reply }
                }
                _ => Change::Nothing(reply),
            })
        })
        .await
}

/// The options `SET` accepts.
#[derive(Debug, PartialEq)]
struct SetOptions {
    condition: Option<SetCondition>,
    get: bool,
    ttl: SetTtl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SetCondition {
    /// `NX`: only if the key does not exist.
    Absent,
    /// `XX`: only if it does.
    Present,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SetTtl {
    /// A plain `SET` drops the deadline the key had, as in Redis.
    Clear,
    /// `KEEPTTL`.
    Keep,
    At(u64),
}

impl SetOptions {
    fn parse(args: &mut Args<'_>) -> RedisResult<Self> {
        let mut options = Self {
            condition: None,
            get: false,
            ttl: SetTtl::Clear,
        };
        let mut ttl_given = false;
        while let Some(option) = args.next_keyword() {
            let mut set_ttl = |ttl| {
                if std::mem::replace(&mut ttl_given, true) {
                    return Err(RedisError::syntax());
                }
                options.ttl = ttl;
                Ok(())
            };
            match option.as_str() {
                "nx" | "xx" => {
                    if options.condition.is_some() {
                        return Err(RedisError::syntax());
                    }
                    options.condition = Some(match option.as_str() {
                        "nx" => SetCondition::Absent,
                        _ => SetCondition::Present,
                    });
                }
                "get" => options.get = true,
                "keepttl" => set_ttl(SetTtl::Keep)?,
                other => match TtlForm::from_option(other) {
                    Some(form) => {
                        let deadline = positive_deadline(form, args.next_integer()?, "set")?;
                        set_ttl(SetTtl::At(deadline))?;
                    }
                    None => return Err(RedisError::syntax()),
                },
            }
        }
        Ok(options)
    }

    fn allows(&self, current: Option<&Current>) -> bool {
        match self.condition {
            None => true,
            Some(SetCondition::Absent) => current.is_none(),
            Some(SetCondition::Present) => current.is_some(),
        }
    }

    fn deadline(&self, current: Option<&Current>) -> Option<u64> {
        match self.ttl {
            SetTtl::Clear => None,
            SetTtl::Keep => kept(current),
            SetTtl::At(deadline) => Some(deadline),
        }
    }
}

pub async fn set(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let key = args.next_required()?;
    let value = args.next_owned()?;
    let options = SetOptions::parse(&mut args)?;
    if options.condition.is_none() && !options.get {
        // Nothing the reply or the write depends on is read, so the owner
        // resolves even `KEEPTTL` itself, under the lock that applies it.
        let expiry = match options.ttl {
            SetTtl::Clear => Expiry::Clear,
            SetTtl::Keep => Expiry::Keep,
            SetTtl::At(deadline) => Expiry::At(deadline),
        };
        store.put(key, value, expiry).await?;
        return Ok(Reply::status());
    }
    store
        .update(key, |current| {
            // `GET` answers with what the key held whether or not the
            // condition let the write through.
            let reply = match options.get {
                true => bulk_or_nil(value_of(current)),
                false => Reply::status(),
            };
            if !options.allows(current) {
                return Ok(Change::Nothing(match options.get {
                    true => reply,
                    false => Reply::Nil,
                }));
            }
            Ok(Change::Put {
                value: value.clone(),
                deadline: options.deadline(current),
                reply,
            })
        })
        .await
}

pub async fn set_with_ttl(
    store: &Store<'_>,
    mut args: Args<'_>,
    unit: TimeUnit,
) -> RedisResult<Reply> {
    let key = args.next_required()?;
    let amount = args.next_integer()?;
    let value = args.next_owned()?;
    let deadline = positive_deadline(
        TtlForm::new(unit, DeadlineMode::Relative),
        amount,
        args.name(),
    )?;
    store.put(key, value, Expiry::At(deadline)).await?;
    Ok(Reply::status())
}

pub async fn setnx(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let key = args.next_required()?;
    let value = args.next_owned()?;
    store
        .update(key, |current| {
            Ok(match current {
                Some(_) => Change::Nothing(Reply::Integer(0)),
                None => Change::Put {
                    value: value.clone(),
                    deadline: None,
                    reply: Reply::Integer(1),
                },
            })
        })
        .await
}

pub async fn getset(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let key = args.next_required()?;
    let value = args.next_owned()?;
    store
        .update(key, |current| {
            // A plain `GETSET` replaces the value, so it drops the old deadline
            // the same way a plain `SET` does.
            Ok(Change::Put {
                value: value.clone(),
                deadline: None,
                reply: bulk_or_nil(value_of(current)),
            })
        })
        .await
}

pub async fn setrange(
    store: &Store<'_>,
    mut args: Args<'_>,
    max_value: usize,
) -> RedisResult<Reply> {
    let key = args.next_required()?;
    let offset = args.next_integer()?;
    let patch = args.next_required()?;
    let offset = usize::try_from(offset).map_err(|_| RedisError::out_of_range())?;
    if patch.is_empty() {
        // Redis neither creates nor pads a key for an empty patch; it only
        // reports the length.
        let length = store
            .read(key)
            .await?
            .map_or(0, |current| current.value.len());
        return Ok(Reply::Integer(length as i64));
    }
    if offset.saturating_add(patch.len()) > max_value {
        return Err(too_large());
    }
    store
        .update(key, |current| {
            let mut value = value_of(current).unwrap_or_default();
            if offset > value.len() {
                value.resize(offset, 0);
            }
            let end = (offset + patch.len()).min(value.len());
            value.splice(offset..end, patch.iter().copied());
            let length = value.len() as i64;
            Ok(Change::Put {
                value,
                deadline: kept(current),
                reply: Reply::Integer(length),
            })
        })
        .await
}

pub async fn append(store: &Store<'_>, mut args: Args<'_>, max_value: usize) -> RedisResult<Reply> {
    let key = args.next_required()?;
    let suffix = args.next_required()?;
    store
        .update(key, |current| {
            let mut value = value_of(current).unwrap_or_default();
            if value.len() + suffix.len() > max_value {
                return Err(too_large());
            }
            value.extend_from_slice(suffix);
            let length = value.len() as i64;
            // Redis keeps the deadline an `APPEND` finds.
            Ok(Change::Put {
                value,
                deadline: kept(current),
                reply: Reply::Integer(length),
            })
        })
        .await
}

/// `INCR` and `DECR` pass their fixed step; `INCRBY` and `DECRBY` read theirs.
pub async fn incr_by(
    store: &Store<'_>,
    mut args: Args<'_>,
    step: Option<i64>,
) -> RedisResult<Reply> {
    let key = args.next_required()?;
    let delta = match step {
        Some(step) => step,
        None => {
            let amount = args.next_integer()?;
            match args.name() {
                "decrby" => amount
                    .checked_neg()
                    .ok_or_else(|| RedisError::new("ERR", "decrement would overflow"))?,
                _ => amount,
            }
        }
    };
    store
        .update(key, |current| {
            // An absent key counts as zero; a key that exists but is not an
            // integer is an error rather than a reset, which is what Redis
            // reports.
            let base = match current {
                Some(current) => {
                    parse_integer(&current.value).ok_or_else(RedisError::not_an_integer)?
                }
                None => 0,
            };
            let updated = base.checked_add(delta).ok_or_else(RedisError::overflow)?;
            Ok(Change::Put {
                value: updated.to_string().into_bytes(),
                deadline: kept(current),
                reply: Reply::Integer(updated),
            })
        })
        .await
}

pub async fn incrbyfloat(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let key = args.next_required()?;
    let delta = args.next_float()?;
    store
        .update(key, |current| {
            let base = match current {
                Some(current) if current.value.is_empty() => 0.0,
                Some(current) => float(&current.value)?,
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
            Ok(Change::Put {
                value: formatted.clone().into_bytes(),
                deadline: kept(current),
                reply: Reply::bulk(formatted),
            })
        })
        .await
}

pub async fn mget(store: &Store<'_>, args: Args<'_>) -> RedisResult<Reply> {
    let values = store
        .each(args.remaining(), |key| async move {
            Ok(bulk_or_nil(
                store.read(key).await?.map(|current| current.value),
            ))
        })
        .await?;
    Ok(Reply::Array(values))
}

/// The pairs of `MSET` or `MSETNX`, one per key. A key named twice takes the
/// later value, as it would when Redis applies the pairs in order; the writes
/// here run concurrently, so that order has to be settled before they start.
fn pairs<'a>(args: &Args<'a>) -> RedisResult<Vec<(&'a [u8], &'a [u8])>> {
    let rest = args.remaining();
    if !rest.len().is_multiple_of(2) {
        return Err(RedisError::wrong_arity(args.name()));
    }
    let mut latest: HashMap<&[u8], usize> = HashMap::new();
    let pairs: Vec<_> = rest
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| (pair[0].as_slice(), pair[1].as_slice()))
        .collect();
    for (index, (key, _)) in pairs.iter().enumerate() {
        latest.insert(key, index);
    }
    Ok(pairs
        .iter()
        .enumerate()
        .filter(|(index, (key, _))| latest[key] == *index)
        .map(|(_, pair)| *pair)
        .collect())
}

/// Redis makes `MSET` atomic across keys. It cannot be here: the keys may live
/// on different owners, and no node offers a write that spans two of them.
pub async fn mset(store: &Store<'_>, args: Args<'_>) -> RedisResult<Reply> {
    let pairs = pairs(&args)?;
    let keys: Vec<Vec<u8>> = pairs.iter().map(|(key, _)| key.to_vec()).collect();
    let values: HashMap<&[u8], &[u8]> = pairs.into_iter().collect();
    store
        .each_write(&keys, |key| {
            let value = values[key].to_vec();
            async move { store.put(key, value, Expiry::Clear).await }
        })
        .await?;
    Ok(Reply::status())
}

/// `MSETNX` exists to set several keys as one: all of them or none. The keys may
/// live on different owners and no node offers a write that spans two, so
/// that promise cannot be kept, and a version that sometimes leaves part of
/// the keys set would break the callers that use it to claim keys together.
/// It is declined unless it names one key, where it is `SETNX`.
pub async fn msetnx(store: &Store<'_>, args: Args<'_>) -> RedisResult<Reply> {
    let pairs = pairs(&args)?;
    let [(key, value)] = pairs[..] else {
        return Err(RedisError::new(
            "ERR",
            "MSETNX with more than one key is not supported by the hashring-rs \
             Redis gateway: the keys cannot be set atomically; see \
             docs/redis-compatibility.md",
        ));
    };
    Ok(Reply::Integer(i64::from(
        store.put_if_absent(key, value).await?,
    )))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::args::now_millis;

    fn parse(parts: &[&[u8]]) -> RedisResult<SetOptions> {
        let rest: Vec<Vec<u8>> = parts.iter().map(|part| part.to_vec()).collect();
        SetOptions::parse(&mut Args::new("set", &rest))
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
        assert_eq!(
            parse(&[b"NX"]).unwrap().condition,
            Some(SetCondition::Absent)
        );
        let SetTtl::At(deadline) = parse(&[b"EX", b"10"]).unwrap().ttl else {
            panic!("EX did not set a deadline");
        };
        assert!(deadline.abs_diff(now_millis() + 10_000) < 1_000);
        let SetTtl::At(deadline) = parse(&[b"PX", b"500"]).unwrap().ttl else {
            panic!("PX did not set a deadline");
        };
        assert!(
            deadline.abs_diff(now_millis() + 500) < 100,
            "PX is milliseconds"
        );
        assert_eq!(
            parse(&[b"EXAT", b"1800000000"]).unwrap().ttl,
            SetTtl::At(1_800_000_000_000),
            "EXAT is seconds"
        );
        assert_eq!(
            parse(&[b"PXAT", b"1800000000000"]).unwrap().ttl,
            SetTtl::At(1_800_000_000_000)
        );
        // NX and XX together, and two expiry options, are both syntax errors.
        assert!(parse(&[b"NX", b"XX"]).is_err());
        assert!(parse(&[b"EX", b"10", b"PX", b"10"]).is_err());
        assert!(parse(&[b"KEEPTTL", b"EX", b"10"]).is_err());
        assert!(parse(&[b"EX", b"0"]).is_err());
        // An option with its operand missing is an error, not a panic.
        assert!(parse(&[b"EX"]).is_err());
        assert!(parse(&[b"NX", b"PXAT"]).is_err());
    }

    #[test]
    fn a_plain_set_drops_the_deadline_and_keepttl_keeps_it() {
        assert_eq!(parse(&[]).unwrap().ttl, SetTtl::Clear);
        assert_eq!(parse(&[b"KEEPTTL"]).unwrap().ttl, SetTtl::Keep);
    }

    #[test]
    fn a_key_named_twice_takes_its_later_value() {
        let rest: Vec<Vec<u8>> = [b"a", b"1", b"b", b"2", b"a", b"3"]
            .iter()
            .map(|part| part.to_vec())
            .collect();
        let args = Args::new("mset", &rest);
        let pairs = pairs(&args).unwrap();
        assert_eq!(
            pairs,
            vec![(b"b".as_slice(), b"2".as_slice()), (b"a", b"3")]
        );
        let odd: Vec<Vec<u8>> = vec![b"a".to_vec()];
        assert!(pairs_of(&odd).is_err());
    }

    fn pairs_of(rest: &[Vec<u8>]) -> RedisResult<Vec<(&[u8], &[u8])>> {
        pairs(&Args::new("mset", rest))
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
