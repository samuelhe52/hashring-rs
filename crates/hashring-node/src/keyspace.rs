//! Paging through the keys a node owns, for callers that need the keyspace
//! rather than one key: `KEYS`, `SCAN`, `DBSIZE` and `RANDOMKEY` at the Redis
//! gateway.
//!
//! A node also holds follower copies of other owners' ranges, so a page walks
//! only the token segments this node owns in its current topology and jumps
//! over the rest. A segment is `(previous assignment, assignment]`, plus the
//! wrap-around tail past the last assignment, which belongs to the first.
//!
//! A page ends only between tokens, never inside one, so where the next page
//! starts is a token alone. That is what lets `SCAN` hand its cursor to a
//! Redis client as a plain integer. A token holds more than one key only when
//! keys' 64-bit hashes collide, which by chance is all but unheard of, but
//! keys can be built to collide by anyone who knows the hash seed. So an owner
//! refuses to put more than [`MAX_KEYS_PER_TOKEN`] keys on one token, which
//! bounds what finishing a token can add to a page.

use super::*;

/// Entries and segments one page may examine, live or not. This bounds how
/// long a page holds the state read lock, whatever the caller asked for.
pub(super) const MAX_EXAMINED_PER_PAGE: usize = 10_000;
pub(super) const MAX_KEYS_PER_PAGE: usize = 10_000;
pub(super) const MAX_KEY_BYTES_PER_PAGE: usize = 4 * 1024 * 1024;
/// The most keys one token may hold, deleted or not. Even at the largest key
/// size, a full token adds a megabyte to a page, which keeps every page well
/// inside one data message.
pub(super) const MAX_KEYS_PER_TOKEN: usize = 16;

pub(super) struct PageLimits {
    pub max_keys: usize,
    pub max_bytes: usize,
    pub max_examined: usize,
    pub count_only: bool,
    /// Stop at the first segment another node owns, rather than jump over it:
    /// the page then covers one contiguous stretch of the ring, which is what
    /// a `SCAN` cursor walks.
    pub one_stretch: bool,
}

#[derive(Debug, Default)]
pub(super) struct KeyPage {
    pub keys: Vec<Vec<u8>>,
    pub count: u64,
    /// The token the next page starts at, or `None` at the end of the ring.
    pub next: Option<u64>,
}

impl NodeState {
    /// One page of the live keys `node_id` owns in this state's topology, from
    /// token `start` on, in token order. Deleted records and records past
    /// their deadline are not keys.
    pub(super) fn owned_keys_page(
        &self,
        node_id: &str,
        start: u64,
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
        let mut position = start;
        loop {
            let index = assignments.partition_point(|assignment| assignment.token < position);
            let (segment_end, owner) = match assignments.get(index) {
                Some(assignment) => (assignment.token, &assignment.node_id),
                None => (u64::MAX, &assignments[0].node_id),
            };
            examined += 1;
            if owner != node_id && limits.one_stretch {
                page.next = Some(position);
                return page;
            }
            if owner == node_id {
                for (&token, bucket) in self.records_by_token.by_token.range(position..=segment_end)
                {
                    // Checked before a token, never inside one, so a page
                    // always ends on a token boundary. The first token is
                    // always taken, which is what keeps every page moving.
                    if examined >= limits.max_examined
                        || page.count as usize >= limits.max_keys
                        || bytes >= limits.max_bytes
                    {
                        page.next = Some(token);
                        return page;
                    }
                    for key in bucket.values() {
                        examined += 1;
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
            if examined >= limits.max_examined {
                page.next = Some(position);
                return page;
            }
        }
    }
}

impl<T> TokenBucket<T> {
    fn values(&self) -> impl Iterator<Item = &T> {
        let (one, many) = match self {
            TokenBucket::One(value) => (Some(value), None),
            TokenBucket::Many(values) => (None, Some(values)),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
}
