//! Walking the keyspace: every live key, from every owner.
//!
//! A whole walk (`keys`, `key_count`) pages through every owner at once.
//! Every page names the epoch the walk started from, and a node at a newer
//! epoch refuses it: a range that changed hands mid-walk may hold keys the
//! walk already passed over at its old owner. The walk then catches up with
//! the topology and starts over, within the operation deadline, so a finished
//! walk saw every key that was live throughout it exactly once.
//!
//! A scan (`scan`) instead goes round the ring in token order, a stretch at a
//! time, each stretch from whichever node owns it when asked. Its cursor is a
//! token, which means the same thing in every epoch, so a scan can span
//! topology changes, and calls on different clients, without starting over.

use futures::future::try_join_all;
use hashring_core::proto::{ListKeysRequest, ListKeysResponse};
use tokio::sync::Semaphore;

use super::*;

const KEYS_PER_PAGE: u32 = 1_000;
const COUNT_PER_PAGE: u32 = 10_000;
const KEY_BYTES_PER_PAGE: u64 = 1 << 20;
/// Pages one walk holds at once, from request to the end of filtering. Every
/// owner is walked concurrently, and a page is buffered whole whether or not
/// any of its keys are kept, so without this a walk over a thousand owners
/// could hold a gigabyte of pages that its result never needed.
const PAGES_IN_FLIGHT: usize = 16;
/// Pages one `scan` call reads, across however many owners' stretches it
/// crosses. A call ends sooner once it has seen its `count` of keys; this
/// bounds a call over a sparse stretch of the ring, which may see none.
const SCAN_PAGES_PER_CALL: usize = 64;
/// What holding one key costs beyond its bytes: its allocation, and the
/// framing a reply wraps it in. Charged against `keys`' budget so that many
/// tiny keys cannot pass a budget meant to bound memory.
pub const PER_KEY_OVERHEAD_BYTES: usize = 64;

/// Why one owner's part of a walk stopped short.
enum Interrupted {
    /// The owner could not answer for this epoch, or not at all: `None` for a
    /// transport failure. The walk catches up and starts over.
    Retry(Option<OperationError>),
    Failed(ClientError),
}

impl From<ClientError> for Interrupted {
    fn from(error: ClientError) -> Self {
        Self::Failed(error)
    }
}

impl HashringClient {
    /// Every live key for which `keep` holds, in no particular order. Fails
    /// with `TooLarge` once the kept keys, each charged its length plus
    /// [`PER_KEY_OVERHEAD_BYTES`], add up to more than `max_bytes`, rather than
    /// holding a reply that size.
    pub async fn keys(
        &self,
        keep: impl Fn(&[u8]) -> bool + Sync,
        max_bytes: usize,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        Ok(self.walk(false, &keep, max_bytes).await?.0)
    }

    /// How many live keys there are. Each owner counts its own, so nothing but
    /// the counts crosses the network.
    pub async fn key_count(&self) -> Result<u64, ClientError> {
        Ok(self.walk(true, &|_| true, 0).await?.1)
    }

    /// A live key, or `None` when there is none. The key is the first one at
    /// or after a random token, so a key that follows a wide gap in the ring
    /// is likelier than one in a crowded stretch; Redis' choice is not
    /// uniform either.
    pub async fn random_key(&self) -> Result<Option<Vec<u8>>, ClientError> {
        let deadline = Instant::now() + self.inner.operation_timeout;
        let mut attempt = 0;
        loop {
            let start = rand::random::<u64>();
            let (epoch, owners) = {
                let topology = self.inner.topology.read().await;
                let first = topology
                    .owner_for_token(start)
                    .map_err(anyhow::Error::from)?
                    .endpoint
                    .clone();
                let mut owners = owner_endpoints(&topology);
                owners.retain(|endpoint| *endpoint != first);
                owners.insert(0, first);
                (topology.epoch, owners)
            };
            match self.first_key(&owners, epoch, start, deadline).await {
                Ok(key) => return Ok(key),
                Err(Interrupted::Failed(error)) => return Err(error),
                Err(Interrupted::Retry(error)) => {
                    self.recover(error, epoch, deadline, &mut attempt).await?
                }
            }
        }
    }

    /// One step of a scan round the ring: the live keys at or after token
    /// `cursor` for which `keep` holds, in token order, and the token to
    /// continue from, or `None` once the scan has reached the end of the ring.
    /// Start a scan at token 0.
    ///
    /// A step stops once it has seen about `count` live keys, kept or not, so
    /// a selective `keep` can return fewer, or none, before the scan is done.
    /// A key that was live for the whole scan is returned exactly once,
    /// however the topology changed between steps: every key sits at one
    /// token, and the scan passes each token once, asking whichever node owns
    /// it at the time. A key written or deleted during the scan may or may not
    /// be returned. Fails with `TooLarge` once the kept keys, each charged its
    /// length plus [`PER_KEY_OVERHEAD_BYTES`], add up to more than
    /// `max_bytes`.
    pub async fn scan(
        &self,
        cursor: u64,
        count: usize,
        keep: impl Fn(&[u8]) -> bool,
        max_bytes: usize,
    ) -> Result<(Vec<Vec<u8>>, Option<u64>), ClientError> {
        let count = count.max(1);
        let deadline = Instant::now() + self.inner.operation_timeout;
        let mut attempt = 0;
        let mut position = cursor;
        let mut keys = Vec::new();
        let (mut pages, mut seen, mut kept_bytes) = (0, 0, 0);
        while pages < SCAN_PAGES_PER_CALL && seen < count {
            let (epoch, endpoint) = {
                let topology = self.inner.topology.read().await;
                let owner = topology
                    .owner_for_token(position)
                    .map_err(anyhow::Error::from)?;
                (topology.epoch, owner.endpoint.clone())
            };
            let request = ListKeysRequest {
                topology_epoch: epoch,
                start_token: position,
                max_keys: u32::try_from(count - seen).unwrap_or(u32::MAX),
                max_bytes: KEY_BYTES_PER_PAGE,
                one_stretch: true,
                ..Default::default()
            };
            // A retry asks the same token again, of its owner as the client
            // now knows it, so nothing is skipped or seen twice.
            let page = match self.list_page(&endpoint, request, deadline).await {
                Ok(page) => page,
                Err(Interrupted::Failed(error)) => return Err(error),
                Err(Interrupted::Retry(error)) => {
                    self.recover(error, epoch, deadline, &mut attempt).await?;
                    continue;
                }
            };
            pages += 1;
            seen += usize::try_from(page.count).unwrap_or(usize::MAX);
            for key in page.keys.into_iter().filter(|key| keep(key)) {
                kept_bytes += key.len() + PER_KEY_OVERHEAD_BYTES;
                if kept_bytes > max_bytes {
                    return Err(size_failure(format!(
                        "the matching keys exceed {max_bytes} bytes"
                    )));
                }
                keys.push(key);
            }
            if page.done {
                return Ok((keys, None));
            }
            position = page.next_token;
        }
        // Every page ends past the token it started at, so a scan that has
        // read one is past token 0, which a caller may use to mean "done".
        debug_assert!(position > cursor || pages == 0);
        Ok((keys, Some(position)))
    }

    async fn walk(
        &self,
        count_only: bool,
        keep: &(dyn Fn(&[u8]) -> bool + Sync),
        max_bytes: usize,
    ) -> Result<(Vec<Vec<u8>>, u64), ClientError> {
        let deadline = Instant::now() + self.inner.operation_timeout;
        let mut attempt = 0;
        loop {
            let (epoch, owners) = {
                let topology = self.inner.topology.read().await;
                (topology.epoch, owner_endpoints(&topology))
            };
            let budget = WalkBudget {
                kept_bytes: AtomicUsize::new(0),
                max_bytes,
                pages: Semaphore::new(PAGES_IN_FLIGHT),
            };
            let walks = owners.iter().map(|endpoint| {
                self.walk_owner(endpoint, epoch, count_only, keep, &budget, deadline)
            });
            match try_join_all(walks).await {
                Ok(parts) => {
                    let mut keys = Vec::new();
                    let mut count = 0;
                    for (part, part_count) in parts {
                        keys.extend(part);
                        count += part_count;
                    }
                    return Ok((keys, count));
                }
                Err(Interrupted::Failed(error)) => return Err(error),
                Err(Interrupted::Retry(error)) => {
                    self.recover(error, epoch, deadline, &mut attempt).await?
                }
            }
        }
    }

    async fn walk_owner(
        &self,
        endpoint: &str,
        epoch: u64,
        count_only: bool,
        keep: &(dyn Fn(&[u8]) -> bool + Sync),
        budget: &WalkBudget,
        deadline: Instant,
    ) -> Result<(Vec<Vec<u8>>, u64), Interrupted> {
        let mut request = ListKeysRequest {
            topology_epoch: epoch,
            max_keys: if count_only {
                COUNT_PER_PAGE
            } else {
                KEYS_PER_PAGE
            },
            max_bytes: KEY_BYTES_PER_PAGE,
            count_only,
            ..Default::default()
        };
        let mut keys = Vec::new();
        let mut count = 0;
        loop {
            let _page_slot = budget
                .pages
                .acquire()
                .await
                .expect("the walk's page semaphore is never closed");
            let page = self.list_page(endpoint, request, deadline).await?;
            count += page.count;
            for key in page.keys.into_iter().filter(|key| keep(key)) {
                let cost = key.len() + PER_KEY_OVERHEAD_BYTES;
                let total = budget.kept_bytes.fetch_add(cost, Ordering::Relaxed) + cost;
                if total > budget.max_bytes {
                    return Err(Interrupted::Failed(size_failure(format!(
                        "the matching keys exceed {} bytes",
                        budget.max_bytes
                    ))));
                }
                keys.push(key);
            }
            if page.done {
                return Ok((keys, count));
            }
            request.start_token = page.next_token;
        }
    }

    /// The first live key the owners hold at or after `start`, trying each
    /// owner from `start` to the end of the ring and then from its beginning.
    async fn first_key(
        &self,
        owners: &[String],
        epoch: u64,
        start: u64,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>, Interrupted> {
        for endpoint in owners {
            for from in [start, 0] {
                let mut request = ListKeysRequest {
                    topology_epoch: epoch,
                    start_token: from,
                    max_keys: 1,
                    max_bytes: KEY_BYTES_PER_PAGE,
                    ..Default::default()
                };
                loop {
                    let page = self.list_page(endpoint, request, deadline).await?;
                    if let Some(key) = page.keys.into_iter().next() {
                        return Ok(Some(key));
                    }
                    if page.done {
                        break;
                    }
                    request.start_token = page.next_token;
                }
            }
        }
        Ok(None)
    }

    async fn list_page(
        &self,
        endpoint: &str,
        request: ListKeysRequest,
        deadline: Instant,
    ) -> Result<ListKeysResponse, Interrupted> {
        let mut client = match self.data_node_client(endpoint, deadline).await {
            Ok(client) => client,
            Err(ClientError::DeadlineExceeded { .. }) => {
                return Err(ClientError::deadline(false).into());
            }
            Err(_) => return Err(Interrupted::Retry(None)),
        };
        let response =
            tokio::time::timeout(remaining(deadline, false)?, client.list_keys(request)).await;
        match response {
            Err(_) => Err(ClientError::deadline(false).into()),
            Ok(Err(status)) if retryable_status(&status) => Err(Interrupted::Retry(None)),
            Ok(Err(status)) => Err(rpc_error(status, false).into()),
            Ok(Ok(response)) => {
                let response = response.into_inner();
                match response.error {
                    Some(error) => Err(Interrupted::Retry(Some(error))),
                    None => Ok(response),
                }
            }
        }
    }

    /// Catches up after an interrupted walk, or gives up on an error that
    /// waiting cannot fix.
    async fn recover(
        &self,
        error: Option<OperationError>,
        epoch: u64,
        deadline: Instant,
        attempt: &mut u32,
    ) -> Result<(), ClientError> {
        let Some(error) = error else {
            self.refresh_after_unavailable(epoch, deadline, false)
                .await?;
            return self
                .retry_delay(deadline, attempt, false, RetryReason::Rpc)
                .await;
        };
        if self
            .handle_retryable(error.clone(), epoch, deadline, attempt, false)
            .await?
        {
            return Ok(());
        }
        Err(OperationFailure::from(error).into())
    }
}

/// What one walk may hold, shared by its owners' walks.
struct WalkBudget {
    kept_bytes: AtomicUsize,
    max_bytes: usize,
    pages: Semaphore,
}

/// The endpoint of every member that owns part of the ring.
fn owner_endpoints(topology: &TopologySnapshot) -> Vec<String> {
    topology
        .members
        .iter()
        .filter(|member| {
            topology
                .assignments
                .iter()
                .any(|assignment| assignment.node_id == member.node_id)
        })
        .map(|member| member.endpoint.clone())
        .collect()
}
