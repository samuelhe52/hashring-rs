//! Paging through the keys a node owns, for callers that need the keyspace
//! rather than one key: `KEYS`, `DBSIZE` and `RANDOMKEY` at the Redis gateway.
//!
//! A node also holds follower copies of other owners' ranges, so a page walks
//! only the token segments this node owns in its current topology and jumps
//! over the rest. A segment is `(previous assignment, assignment]`, plus the
//! wrap-around tail past the last assignment, which belongs to the first.

use super::*;

/// Entries and segments one page may examine, live or not. This bounds how
/// long a page holds the state read lock, whatever the caller asked for.
pub(super) const MAX_EXAMINED_PER_PAGE: usize = 10_000;
pub(super) const MAX_KEYS_PER_PAGE: usize = 10_000;
pub(super) const MAX_KEY_BYTES_PER_PAGE: usize = 4 * 1024 * 1024;

/// Where a page starts: keys at or after `token`, and when `after_key` is set,
/// only the keys at `token` that sort after it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct KeyCursor {
    pub token: u64,
    pub after_key: Option<Vec<u8>>,
}

pub(super) struct PageLimits {
    pub max_keys: usize,
    pub max_bytes: usize,
    pub max_examined: usize,
    pub count_only: bool,
}

#[derive(Debug, Default)]
pub(super) struct KeyPage {
    pub keys: Vec<Vec<u8>>,
    pub count: u64,
    /// `None` when no owned key remains.
    pub next: Option<KeyCursor>,
}

impl NodeState {
    /// One page of the live keys `node_id` owns in this state's topology, from
    /// `cursor` on, in token order. Deleted records and records past their
    /// deadline are not keys.
    pub(super) fn owned_keys_page(
        &self,
        node_id: &str,
        cursor: KeyCursor,
        now_unix_millis: u64,
        limits: &PageLimits,
    ) -> KeyPage {
        let assignments = &self.topology.assignments;
        let mut page = KeyPage::default();
        if assignments.is_empty() {
            return page;
        }
        let mut examined = 0;
        let mut bytes = 0;
        let mut position = cursor.token;
        let mut after_key = cursor.after_key;
        loop {
            let index = assignments.partition_point(|assignment| assignment.token < position);
            let (segment_end, owner) = match assignments.get(index) {
                Some(assignment) => (assignment.token, &assignment.node_id),
                None => (u64::MAX, &assignments[0].node_id),
            };
            examined += 1;
            if owner == node_id {
                for (&token, bucket) in self.records_by_token.by_token.range(position..=segment_end)
                {
                    // Only the bucket the cursor points into is resumed part
                    // way, and it is entered just past the cursor's key rather
                    // than walked from its start.
                    let resume = match token == position {
                        true => after_key.clone(),
                        false => None,
                    };
                    for key in bucket.keys_after(resume.as_deref()) {
                        if examined >= limits.max_examined
                            || page.count as usize >= limits.max_keys
                            || bytes >= limits.max_bytes
                        {
                            page.next = Some(KeyCursor {
                                token: position,
                                after_key,
                            });
                            return page;
                        }
                        examined += 1;
                        position = token;
                        after_key = Some(key.to_vec());
                        let live = self.records.get(key).is_some_and(|record| {
                            !record.deleted && !record.is_expired(now_unix_millis)
                        });
                        if !live {
                            continue;
                        }
                        page.count += 1;
                        if !limits.count_only {
                            bytes += key.len();
                            page.keys.push(key.to_vec());
                        }
                    }
                }
            }
            if segment_end == u64::MAX {
                return page;
            }
            position = segment_end + 1;
            after_key = None;
            if examined >= limits.max_examined {
                page.next = Some(KeyCursor {
                    token: position,
                    after_key: None,
                });
                return page;
            }
        }
    }
}

impl TokenBucket<Arc<[u8]>> {
    /// The bucket's keys in order, starting just after `after` when given.
    fn keys_after<'a>(&'a self, after: Option<&'a [u8]>) -> impl Iterator<Item = &'a Arc<[u8]>> {
        use std::ops::Bound;
        let (one, many) = match self {
            TokenBucket::One(key) => (
                after
                    .is_none_or(|after| key.as_ref() > after)
                    .then_some(key),
                None,
            ),
            TokenBucket::Many(keys) => {
                let start = after.map_or(Bound::Unbounded, Bound::Excluded);
                (None, Some(keys.range::<[u8], _>((start, Bound::Unbounded))))
            }
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
}
