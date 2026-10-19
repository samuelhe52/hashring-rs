//! Commands about keys rather than values: existence, deletion, renaming and
//! deadlines.

use hashring_client::Precondition;

use super::{
    args::{Args, TimeUnit, TtlForm, invalid_expire_time, now_millis},
    store::{Change, Store, expiry},
};
use crate::{
    error::{RedisError, RedisResult},
    resp::Reply,
};

/// `DEL` counts the keys that existed and were removed. The cluster's delete
/// is idempotent and reports success either way, so each key is read and then
/// deleted on condition that it is still the version read; a key that changed
/// in between is read again.
pub async fn delete(store: &Store<'_>, args: Args<'_>) -> RedisResult<Reply> {
    let removed = store
        .each_write(args.remaining(), |key| async move {
            store
                .update(key, |current| {
                    Ok(match current {
                        Some(_) => Change::Delete { reply: 1 },
                        None => Change::Nothing(0),
                    })
                })
                .await
        })
        .await?;
    Ok(Reply::Integer(removed.into_iter().sum()))
}

/// `EXISTS` and `TOUCH`. A key named twice counts twice, as in Redis.
pub async fn exists(store: &Store<'_>, args: Args<'_>) -> RedisResult<Reply> {
    let found = store
        .each(args.remaining(), |key| async move {
            Ok(i64::from(store.read(key).await?.is_some()))
        })
        .await?;
    Ok(Reply::Integer(found.into_iter().sum()))
}

pub async fn key_type(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let key = args.next()?;
    Ok(match store.read(key).await? {
        // Every value this gateway stores is a byte string.
        Some(_) => Reply::Simple("string".into()),
        None => Reply::Simple("none".into()),
    })
}

/// The value and its deadline move together. The two keys may live on
/// different owners, so the copy and the delete are two writes: the
/// destination is written first, and the source is then deleted only if it is
/// still the version that was copied. A source rewritten in between is left
/// in place and reported, since the destination now holds its earlier value.
pub async fn rename(
    store: &Store<'_>,
    mut args: Args<'_>,
    only_if_absent: bool,
) -> RedisResult<Reply> {
    let from = args.next()?;
    let to = args.next()?;
    let Some(source) = store.read(from).await? else {
        return Err(RedisError::no_such_key());
    };
    let done = || match only_if_absent {
        true => Reply::Integer(1),
        false => Reply::status(),
    };
    if from == to {
        return Ok(match only_if_absent {
            true => Reply::Integer(0),
            false => done(),
        });
    }
    let client = store.client().await?;
    let value = source.value.clone();
    if only_if_absent {
        let written = client
            .put_if(
                to.to_vec(),
                value,
                expiry(source.deadline),
                Precondition::Absent,
            )
            .await?;
        if written.is_none() {
            return Ok(Reply::Integer(0));
        }
    } else {
        store.put(to, value, expiry(source.deadline)).await?;
    }
    let deleted = client
        .delete_if(from.to_vec(), Precondition::Version(source.version))
        .await?;
    match deleted {
        Some(_) => Ok(done()),
        None => Err(RedisError::new(
            "ERR",
            "the source key changed during RENAME; the destination holds its earlier value",
        )),
    }
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
    fn parse(args: &mut Args<'_>) -> RedisResult<Self> {
        let guard = match args.next_keyword().as_deref() {
            None => Self::Always,
            Some("nx") => Self::OnlyWithout,
            Some("xx") => Self::OnlyWith,
            Some("gt") => Self::OnlyGreater,
            Some("lt") => Self::OnlyLess,
            Some(_) => return Err(RedisError::syntax()),
        };
        args.next_optional()
            .map_or(Ok(guard), |_| Err(RedisError::syntax()))
    }

    /// `current` is the deadline the key has now, and `requested` the one
    /// being asked for, both absolute. A key without a deadline never expires,
    /// so for `GT` and `LT` it compares as infinitely far away: no deadline is
    /// greater, and every deadline is less.
    fn allows(self, current: Option<u64>, requested: i128) -> bool {
        match self {
            Self::Always => true,
            Self::OnlyWithout => current.is_none(),
            Self::OnlyWith => current.is_some(),
            Self::OnlyGreater => current.is_some_and(|current| requested > i128::from(current)),
            Self::OnlyLess => current.is_none_or(|current| requested < i128::from(current)),
        }
    }
}

/// `EXPIRE`, `PEXPIRE`, `EXPIREAT` and `PEXPIREAT`. A deadline that is not in
/// the future deletes the key, as Redis does, whichever form named it.
pub async fn expire(store: &Store<'_>, mut args: Args<'_>, form: TtlForm) -> RedisResult<Reply> {
    let key = args.next()?;
    let amount = args.next_integer()?;
    let guard = ExpiryGuard::parse(&mut args)?;
    let requested = form
        .deadline(amount, now_millis())
        .ok_or_else(|| invalid_expire_time(args.name()))?;
    store
        .update(key, |current| {
            let Some(current) = current else {
                return Ok(Change::Nothing(Reply::Integer(0)));
            };
            if !guard.allows(current.deadline, requested) {
                return Ok(Change::Nothing(Reply::Integer(0)));
            }
            let reply = Reply::Integer(1);
            Ok(match u64::try_from(requested) {
                Ok(deadline) if deadline > now_millis() => Change::Expire {
                    deadline: Some(deadline),
                    reply,
                },
                _ => Change::Delete { reply },
            })
        })
        .await
}

pub async fn persist(store: &Store<'_>, mut args: Args<'_>) -> RedisResult<Reply> {
    let key = args.next()?;
    store
        .update(key, |current| {
            Ok(match current {
                Some(current) if current.deadline.is_some() => Change::Expire {
                    deadline: None,
                    reply: Reply::Integer(1),
                },
                _ => Change::Nothing(Reply::Integer(0)),
            })
        })
        .await
}

pub async fn ttl(store: &Store<'_>, mut args: Args<'_>, unit: TimeUnit) -> RedisResult<Reply> {
    let key = args.next()?;
    Ok(match store.read(key).await? {
        // -2 is no key at all, -1 a key with no deadline.
        None => Reply::Integer(-2),
        Some(current) => match current.deadline {
            None => Reply::Integer(-1),
            Some(deadline) => {
                let remaining = deadline.saturating_sub(now_millis());
                let value = match unit {
                    // Redis rounds a remaining TTL up to whole seconds.
                    TimeUnit::Seconds => remaining.div_ceil(1000),
                    TimeUnit::Milliseconds => remaining,
                };
                Reply::Integer(i64::try_from(value).unwrap_or(i64::MAX))
            }
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_expiry_conditions_compare_the_deadline() {
        let now = i128::from(now_millis());
        let at = |offset: i128| u64::try_from(now + offset).unwrap();
        assert!(ExpiryGuard::Always.allows(None, now));
        assert!(ExpiryGuard::OnlyWithout.allows(None, now));
        assert!(!ExpiryGuard::OnlyWithout.allows(Some(at(1)), now));
        assert!(ExpiryGuard::OnlyWith.allows(Some(at(1)), now));
        assert!(ExpiryGuard::OnlyGreater.allows(Some(at(0)), now + 1));
        assert!(!ExpiryGuard::OnlyGreater.allows(Some(at(1)), now));
        assert!(ExpiryGuard::OnlyLess.allows(Some(at(1)), now));
    }

    #[test]
    fn a_key_without_a_deadline_compares_as_never_expiring() {
        let now = i128::from(now_millis());
        assert!(ExpiryGuard::OnlyLess.allows(None, now + 10_000));
        assert!(!ExpiryGuard::OnlyGreater.allows(None, now + 10_000));
    }

    #[test]
    fn expire_takes_at_most_one_condition() {
        let rest: Vec<Vec<u8>> = vec![b"NX".to_vec(), b"GT".to_vec()];
        assert!(ExpiryGuard::parse(&mut Args::new("expire", &rest)).is_err());
    }
}
