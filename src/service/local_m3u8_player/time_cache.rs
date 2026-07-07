use std::{collections::BTreeMap, sync::Arc};

use foyer::HybridCache;

use crate::service::local_m3u8_player::{M3U8CacheKey, SegmentId};

struct NoCopy;

pub struct TimeCache {
    segments_time_cache2: HybridCache<M3U8CacheKey, BTreeMap<usize, f32>>,
    multiple_mutex: MultipleValueMutex,
}

impl TimeCache {
    pub async fn add_segment(&mut self, segment_id: &SegmentId, time: f32) {
        let guard = self.multiple_mutex.lock(segment_id.m3u8.clone()).await;
        let _guard = guard.lock().await;
        let r = self
            .segments_time_cache2
            .get(&segment_id.m3u8)
            .await
            .unwrap();
        match r {}
    }
}

struct RemoveOnDrop {
    mutex: tokio::sync::Mutex<()>,
    active_mutexes: Arc<dashmap::DashMap<M3U8CacheKey, std::sync::Weak<RemoveOnDrop>>>,
    key: M3U8CacheKey,
}

impl RemoveOnDrop {
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.mutex.lock().await
    }
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        self.active_mutexes
            .remove_if(&self.key, |_, v| v.upgrade().is_none());
    }
}

#[derive(Default)]
struct MultipleValueMutex {
    active_mutexes: Arc<dashmap::DashMap<M3U8CacheKey, std::sync::Weak<RemoveOnDrop>>>,
}

impl MultipleValueMutex {
    pub fn new() -> Self {
        Self::default()
    }

    async fn lock(&self, key: M3U8CacheKey) -> Arc<RemoveOnDrop> {
        match self.active_mutexes.entry(key.clone()) {
            dashmap::Entry::Occupied(mut entry) => match entry.get().upgrade() {
                Some(v) => v,
                None => {
                    let value = RemoveOnDrop {
                        active_mutexes: self.active_mutexes.clone(),
                        key,
                        mutex: tokio::sync::Mutex::new(()),
                    };
                    let value = Arc::new(value);
                    entry.insert(Arc::downgrade(&value));
                    value
                }
            },
            dashmap::Entry::Vacant(entry) => {
                let value = RemoveOnDrop {
                    active_mutexes: self.active_mutexes.clone(),
                    key,
                    mutex: tokio::sync::Mutex::new(()),
                };
                let value = Arc::new(value);
                entry.insert(Arc::downgrade(&value));
                value
            }
        }
    }
}
