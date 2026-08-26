use std::{hash::Hash, sync::Arc};

pub(crate) struct RemoveOnDrop<K: Eq + Hash> {
    mutex: Arc<tokio::sync::Mutex<()>>,
    active_mutexes: Arc<dashmap::DashMap<K, std::sync::Weak<RemoveOnDrop<K>>>>,
    key: K,
}

impl<K: Eq + Hash> RemoveOnDrop<K> {
    async fn lock_owned(self: Arc<Self>) -> RemoveOnDropGuardOwned<K> {
        let guard = self.mutex.clone().lock_owned().await;
        RemoveOnDropGuardOwned {
            _guard: guard,
            _remove_on_drop: self,
        }
    }
}

impl<K: Eq + Hash> Drop for RemoveOnDrop<K> {
    fn drop(&mut self) {
        self.active_mutexes
            .remove_if(&self.key, |_, v| v.upgrade().is_none());
    }
}

pub(crate) struct RemoveOnDropGuardOwned<K: Eq + Hash> {
    _guard: tokio::sync::OwnedMutexGuard<()>,
    _remove_on_drop: Arc<RemoveOnDrop<K>>,
}

#[derive(Clone)]
pub(crate) struct MultipleValueMutex<K: Eq + Hash> {
    active_mutexes: Arc<dashmap::DashMap<K, std::sync::Weak<RemoveOnDrop<K>>>>,
}

impl<K: Clone + Eq + Hash> MultipleValueMutex<K> {
    pub(crate) fn new() -> Self {
        Self {
            active_mutexes: Arc::new(dashmap::DashMap::new()),
        }
    }

    pub(crate) async fn lock_mutex(&self, key: K) -> RemoveOnDropGuardOwned<K> {
        let value = match self.active_mutexes.entry(key) {
            dashmap::Entry::Occupied(mut entry) => match entry.get().upgrade() {
                Some(value) => value,
                None => {
                    let value = Arc::new(RemoveOnDrop {
                        active_mutexes: self.active_mutexes.clone(),
                        key: entry.key().clone(),
                        mutex: Arc::new(tokio::sync::Mutex::new(())),
                    });
                    entry.insert(Arc::downgrade(&value));
                    value
                }
            },
            dashmap::Entry::Vacant(entry) => {
                let value = Arc::new(RemoveOnDrop {
                    active_mutexes: self.active_mutexes.clone(),
                    key: entry.key().clone(),
                    mutex: Arc::new(tokio::sync::Mutex::new(())),
                });
                entry.insert(Arc::downgrade(&value));
                value
            }
        };

        value.lock_owned().await
    }
}
