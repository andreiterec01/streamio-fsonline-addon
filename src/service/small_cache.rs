use std::{collections::HashMap, hash::Hash, time::Duration};

use mut_binary_heap::MinComparator;

pub struct SmallCache<K, V> {
    cache: HashMap<K, V>,
    expiration: mut_binary_heap::BinaryHeap<K, std::time::Instant, MinComparator>,
    time_to_idle: Duration,
}

impl<K: Eq + Hash + Clone, V> SmallCache<K, V> {
    pub(crate) fn new(time_to_idle: Duration) -> Self {
        Self {
            cache: HashMap::new(),
            expiration: mut_binary_heap::BinaryHeap::new(),
            time_to_idle,
        }
    }

    pub(crate) fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        if let Some(mut v) = self.expiration.get_mut(k) {
            let expire_at = std::time::Instant::now() + self.time_to_idle;
            *v = expire_at;
        }
        self.cleanup();
        self.cache.get_mut(k)
    }

    pub(crate) fn get_or_insert_mut(&mut self, k: K, value: impl FnOnce() -> V) -> &mut V {
        self.expiration
            .push(k.clone(), std::time::Instant::now() + self.time_to_idle);
        self.cleanup();
        self.cache.entry(k).or_insert_with(value)
    }

    fn cleanup(&mut self) {
        let now = std::time::Instant::now();
        while let Some(instant) = self.expiration.peek()
            && *instant < now
        {
            let (k, _) = self.expiration.pop_with_key().unwrap();
            self.cache.remove(&k).expect("The value should be there");
        }
    }
}
