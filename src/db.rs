use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

/// Server state shared across all connections.
#[derive(Debug, Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// Shared state guarded by a mutex.
    state: Mutex<State>,
    /// Notifies the background task handling entry expiration.
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// The key-value store.
    entries: HashMap<String, Entry>,
    /// The pub-sub key space.
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,
    /// Tracks key TTLs.
    expirations: BTreeMap<(Instant, u64), String>,
    /// Identifier to use for the next expiration.
    next_id: u64,
    /// True when the Db instance is shutting down.
    shutdown: bool,
}

#[derive(Debug)]
struct Entry {
    /// Unique identifier for this entry.
    id: u64,
    /// Stored data.
    data: Bytes,
    /// Time to expire.
    expires_at: Option<Instant>,
}

impl Db {
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeMap::new(),
                next_id: 0,
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // start the background task.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        let id = state.next_id;
        state.next_id += 1;

        let mut notify = false;

        let expires_at = expire.map(|duration| {
            let when = Instant::now() + duration;

            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            // track the expiration.
            state.expirations.insert((when, id), key.clone());
            when
        });

        // insert the entry into the hashmap.
        let prev = state.entries.insert(
            key,
            Entry {
                id,
                data: value,
                expires_at,
            },
        );

        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // clear expiration
                state.expirations.remove(&(when, prev.id));
            }
        }

        drop(state);

        if notify {
            self.shared.background_task.notify_one();
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // If this is the last active `Db` instance, the background task must be
        // notified to shut down.
        if Arc::strong_count(&self.shared) == 2 {
            let mut state = self.shared.state.lock().unwrap();
            state.shutdown = true;

            drop(state);
            self.shared.background_task.notify_one();
        }
    }
}

impl Shared {
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            return None;
        }

        let mut state = &mut *state;

        // find all key scheduled to expire before now.
        let now = Instant::now();

        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            if when > now {
                // done purging.
                return Some(when);
            }

            // the key expired, remove it.
            state.entries.remove(key);
            state.expirations.remove(&(when, id));
        }

        None
    }

    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .keys()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// Routine executed by the background task.
async fn purge_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        if let Some(when) = shared.purge_expired_keys() {
           tokio::select! {
               _ = time::sleep_until(when) => {}
               _ = shared.background_task.notified() => {}
           }
        } else {
            // there are no keys expiring in the future. Wait until the tasks is notified.  
            shared.background_task.notified().await;
        }
    }
}
