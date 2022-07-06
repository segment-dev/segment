use anyhow::{anyhow, Result};
use bytes::Bytes;
use dashmap::DashMap;
use libproc::libproc::pid_rusage;
use parking_lot::Mutex;
use rand::Rng;
use std::collections::HashMap;
use std::process;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::time::sleep;

const MAX_MEMORY_EVICTOR_SAMPLE_SIZE: usize = 3;

#[derive(Debug)]
pub struct KeyspaceManager {
    server_max_memory: u64,
    keyspaces: DashMap<String, Keyspace>,
}

#[derive(Debug)]
pub struct Keyspace {
    db: Arc<Db>,
}

#[derive(Debug)]
pub struct Db {
    store: Mutex<HashMap<String, Value>>,
    shutdown: Mutex<bool>,
    notifier: Notify,
    evictor: Evictor,
    server_max_memory: u64,
}

#[derive(Debug)]
pub struct Value {
    data: Bytes,
    last_accessed: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Evictor {
    Random,
    Noop,
    Lru,
}

impl KeyspaceManager {
    pub fn new(server_max_memory: u64) -> Self {
        KeyspaceManager {
            server_max_memory,
            keyspaces: DashMap::new(),
        }
    }

    pub fn with_keyspace<T>(
        &self,
        keyspace: &str,
        f: impl FnOnce(&mut Keyspace) -> Result<T>,
    ) -> Result<T> {
        if let Some(mut keyspace) = self.keyspaces.get_mut(keyspace) {
            return f(&mut keyspace);
        }

        Err(anyhow!("ERR keyspace '{}' does not exist", keyspace))
    }

    pub fn create(&self, name: String, evictor: Evictor) -> u8 {
        if self.keyspaces.contains_key(&name) {
            return 0;
        }
        let keyspace = Keyspace::new(evictor, self.server_max_memory);
        keyspace.start_evictor();
        self.keyspaces.insert(name, keyspace);
        1
    }
}

impl Keyspace {
    pub fn new(evictor: Evictor, server_max_memory: u64) -> Self {
        Keyspace {
            db: Arc::new(Db::new(evictor, server_max_memory)),
        }
    }

    pub fn set(&self, key: String, value: Bytes) -> u8 {
        self.db.store.lock().insert(key, Value::new(value));
        1
    }
    pub fn get(&self, key: &str) -> Option<Bytes> {
        if let Some(mut value) = self.db.store.lock().get_mut(key) {
            value.last_accessed = Instant::now();
            return Some(value.data.clone());
        }
        None
    }

    pub fn del(&self, key: &str) -> u8 {
        let value = self.db.store.lock().remove(key);

        if value.is_some() {
            return 1;
        }

        0
    }

    pub fn start_evictor(&self) {
        if self.db.evictor == Evictor::Noop {
            return;
        }
        let db = self.db.clone();
        tokio::spawn(async move {
            start_background_max_memory_evictor(db).await;
        });
    }
}

impl Value {
    pub fn new(data: Bytes) -> Self {
        Value {
            data,
            last_accessed: Instant::now(),
        }
    }
}

impl Db {
    pub fn new(evictor: Evictor, server_max_memory: u64) -> Self {
        Db {
            store: Mutex::new(HashMap::new()),
            shutdown: Mutex::new(false),
            notifier: Notify::new(),
            evictor,
            server_max_memory,
        }
    }

    pub fn shutdown(&self) {
        let mut handle = self.shutdown.lock();
        *handle = true;
        drop(handle);
        self.notifier.notify_one();
    }

    pub fn is_shutdown(&self) -> bool {
        *self.shutdown.lock()
    }
}

impl Drop for Keyspace {
    fn drop(&mut self) {
        self.db.shutdown()
    }
}

async fn start_background_max_memory_evictor(db: Arc<Db>) {
    while !db.is_shutdown() {
        tokio::select! {
            _ = sleep(Duration::from_millis(100)) => {
                sample_and_evict(db.clone());
            }
            _ = db.notifier.notified() => {}
        }
    }
}

fn sample_and_evict(db: Arc<Db>) {
    if db.is_shutdown() {
        return;
    }
    let rss = get_rss();
    if rss < db.server_max_memory {
        return;
    }
    let (mut key_to_delete, mut access_time): (Option<String>, Instant) = (None, Instant::now());
    let mut handle = db.store.lock();
    // We run the loop until we have enough samples (defined by MAX_MEMORY_EVICTOR_SAMPLE_SIZE)
    // to evict, for random evictor we play a game of odds, we generate a random number
    // and if the number is less than < 0.5 the key is selected for eviction.
    // A scenario can occur where for all the samples none of the random numbers were < 0.5
    // in that case we do nothing, this scenario should only occur for random evictor.
    // For LRU evictor we choose the oldest key out of the sample and delete it.
    for (samples, entry) in handle.iter().enumerate() {
        if db.evictor == Evictor::Random {
            if rand::thread_rng().gen::<f32>() < 0.5 {
                key_to_delete = Some(entry.0.clone())
            }
        } else if entry.1.last_accessed <= access_time {
            access_time = entry.1.last_accessed;
            key_to_delete = Some(entry.0.clone());
        }
        if (samples + 1) == std::cmp::min(MAX_MEMORY_EVICTOR_SAMPLE_SIZE, handle.len()) {
            if let Some(key) = key_to_delete {
                handle.remove(&key);
            }
            break;
        }
    }
}

fn get_rss() -> u64 {
    let mut rss = pid_rusage::pidrusage::<pid_rusage::RUsageInfoV2>(process::id() as i32)
        .map_or(0, |r| r.ri_resident_size);
    if rss > 0 && cfg!(target_os = "linux") {
        rss *= 1024
    }
    rss
}
