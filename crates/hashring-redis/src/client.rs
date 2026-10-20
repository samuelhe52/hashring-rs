//! The cluster connection a connection's commands share, and the locks that
//! keep read-modify-write commands within one gateway from colliding.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

pub use hashring_client::ClientConfig;
use hashring_client::{ClientError, HashringClient};
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

use crate::error::{RedisError, RedisResult};

/// One cluster client per gateway rather than per connection.
///
/// `HashringClient` caches a topology and a channel per data node, both of
/// which are worth sharing: every connection to the same gateway would
/// otherwise hold its own copy and poll the coordinator separately. The client
/// is built on first use and dropped when it stops working, so a gateway that
/// starts before its coordinator still answers clients once the coordinator is
/// up.
pub struct SharedClient {
    endpoint: String,
    config: ClientConfig,
    client: RwLock<Option<HashringClient>>,
    connecting: Mutex<()>,
    failures: AtomicU64,
}

impl SharedClient {
    pub fn new(endpoint: String, config: ClientConfig) -> Self {
        Self {
            endpoint,
            config,
            client: RwLock::new(None),
            connecting: Mutex::new(()),
            failures: AtomicU64::new(0),
        }
    }

    pub async fn get(&self) -> RedisResult<HashringClient> {
        if let Some(client) = self.client.read().await.clone() {
            return Ok(client);
        }
        // One connection attempt at a time, so a burst of commands after a
        // gateway restart does not open a burst of coordinator connections.
        let _guard = self.connecting.lock().await;
        if let Some(client) = self.client.read().await.clone() {
            return Ok(client);
        }
        let client = self.connect().await?;
        *self.client.write().await = Some(client.clone());
        self.failures.store(0, Ordering::Relaxed);
        Ok(client)
    }

    async fn connect(&self) -> RedisResult<HashringClient> {
        let attempt = self.failures.fetch_add(1, Ordering::Relaxed) + 1;
        match HashringClient::connect_with_config(self.endpoint.clone(), self.config.clone()).await
        {
            Ok(client) => {
                tracing::info!(endpoint = %self.endpoint, "connected to the cluster coordinator");
                Ok(client)
            }
            Err(error) => {
                tracing::debug!(endpoint = %self.endpoint, attempt, %error, "coordinator connection failed");
                Err(connect_error(error))
            }
        }
    }

    /// Drops the cached client so the next command reconnects. Called when a
    /// command fails in a way that means the cached topology or channels are no
    /// longer usable; a later command rebuilds them.
    pub async fn invalidate(&self) {
        self.client.write().await.take();
    }
}

/// A gateway that cannot reach the coordinator is not down: the cluster may be
/// starting, or migrating. `TRYAGAIN` is the code clients already back off on.
fn connect_error(error: ClientError) -> RedisError {
    let message = error.to_string();
    tracing::debug!(%error, "gateway could not reach the coordinator");
    RedisError::new("TRYAGAIN", format!("cluster is unavailable: {message}"))
}

/// Keeps two read-modify-write commands on one key from the same gateway from
/// running at once.
///
/// Correctness does not depend on it: every such command writes on condition
/// that the key is still the version it read, and the owner enforces that
/// against every writer. What the lock saves is the retry. Without it, eight
/// clients incrementing one hot key would each read the same version and all
/// but one would have to read again; with it they take turns within this
/// gateway and only collide with writers elsewhere.
pub struct KeyLocks {
    stripes: Vec<Arc<Mutex<()>>>,
}

/// Stripe count. Two keys landing on one stripe wait for each other, so this
/// trades a little concurrency for a fixed-size table; 4096 stripes make a
/// collision between two hot keys unlikely while costing 4096 tiny allocations.
const STRIPES: usize = 4096;

impl KeyLocks {
    pub fn new() -> Self {
        Self {
            stripes: (0..STRIPES).map(|_| Arc::new(Mutex::new(()))).collect(),
        }
    }

    fn stripe(&self, key: &[u8]) -> usize {
        // FNV-1a: cheap, and the same for a key every time.
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in key {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        (hash % self.stripes.len() as u64) as usize
    }

    /// Locks every key's stripe, in ascending stripe order so two commands that
    /// name the same keys in opposite orders cannot deadlock. Repeated keys
    /// lock once.
    pub async fn lock(&self, keys: &[&[u8]]) -> Vec<OwnedMutexGuard<()>> {
        let mut stripes: Vec<usize> = keys.iter().map(|key| self.stripe(key)).collect();
        stripes.sort_unstable();
        stripes.dedup();
        let mut guards = Vec::with_capacity(stripes.len());
        for stripe in stripes {
            guards.push(self.stripes[stripe].clone().lock_owned().await);
        }
        guards
    }
}

impl Default for KeyLocks {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_always_lands_on_the_same_stripe() {
        let locks = KeyLocks::new();
        assert_eq!(locks.stripe(b"key"), locks.stripe(b"key"));
    }

    #[tokio::test]
    async fn two_holders_of_one_key_wait_for_each_other() {
        let locks = Arc::new(KeyLocks::new());
        let held = locks.lock(&[b"key"]).await;
        let other = {
            let locks = locks.clone();
            tokio::spawn(async move {
                let _guard = locks.lock(&[b"key"]).await;
                true
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!other.is_finished(), "the second holder did not wait");
        drop(held);
        assert!(other.await.unwrap());
    }

    #[tokio::test]
    async fn one_key_in_a_set_locks_its_stripe_once() {
        let locks = KeyLocks::new();
        let guards = locks.lock(&[b"key", b"key"]).await;
        assert_eq!(guards.len(), 1);
    }
}
