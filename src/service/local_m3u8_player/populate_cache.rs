use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::TryStreamExt;
use mut_binary_heap::{BinaryHeap, MinComparator};

use crate::{
    service::{
        local_m3u8_player::{M3U8CacheKey, SegmentId, segments_database::LocalPlayerInner},
        small_cache,
    },
    ts_parser,
};

#[derive(Clone)]
struct NextCompute {
    not_computed_index: Option<usize>,
    duration_from_start_to_not_computed_index: Duration,
}

struct NextResult {
    next_index: usize,
    time_taken: Duration,
}

struct MovieData {
    next_compute: HashMap<usize, NextCompute>,
    segments_count: usize,
}

impl MovieData {
    fn get_next_compute_inner(&mut self, index: usize) -> Option<NextCompute> {
        let mut old_compute = self.next_compute.get(&index)?.clone();
        let Some(not_computed_index) = old_compute.not_computed_index else {
            return Some(old_compute);
        };
        let new_compute = self.get_next_compute_inner(not_computed_index);
        if let Some(new_compute) = new_compute {
            old_compute.not_computed_index = new_compute.not_computed_index;
            old_compute.duration_from_start_to_not_computed_index +=
                new_compute.duration_from_start_to_not_computed_index;
            let r = self.next_compute.insert(index, old_compute.clone());
            debug_assert!(r.is_some(), "The value should be there");
        }
        Some(old_compute)
    }

    fn get_next_to_compute(&mut self, index: usize) -> Option<NextResult> {
        let Some(newest) = self.get_next_compute_inner(index) else {
            return Some(NextResult {
                next_index: index,
                time_taken: Duration::ZERO,
            });
        };

        Some(NextResult {
            next_index: newest.not_computed_index?,
            time_taken: newest.duration_from_start_to_not_computed_index,
        })
    }

    fn insert(&mut self, index: usize, duration: Duration) {
        self.next_compute.insert(
            index,
            NextCompute {
                not_computed_index: (index + 1 < self.segments_count).then_some(index + 1),
                duration_from_start_to_not_computed_index: duration,
            },
        );
    }
}

pub struct LoadCacheRequest {
    pub(crate) segment_id: SegmentId,
    pub(crate) cache_in_the_future: Duration,
}

#[derive(Clone)]
pub struct HeapEntry {
    duration_in_future: Duration,
    total_duration: Duration,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.duration_in_future == other.duration_in_future
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.duration_in_future.cmp(&other.duration_in_future)
    }
}

fn add_to_cache(
    heap: &mut BinaryHeap<SegmentId, HeapEntry, MinComparator>,
    segment_id: SegmentId,
    entry: HeapEntry,
) {
    if let Some(mut old_entry) = heap.get_mut(&segment_id) {
        let (smaller_entry, mut bigger_entry) = if entry < *old_entry {
            (entry, old_entry.clone())
        } else {
            (old_entry.clone(), entry)
        };

        let in_future_diff = bigger_entry.duration_in_future - smaller_entry.duration_in_future;
        bigger_entry.duration_in_future = smaller_entry.duration_in_future;
        bigger_entry.total_duration -= in_future_diff;
        let entry = if smaller_entry.total_duration > bigger_entry.total_duration {
            smaller_entry
        } else {
            bigger_entry
        };
        *old_entry = entry;
        return;
    }
    heap.push(segment_id, entry);
}

impl LocalPlayerInner {
    pub(crate) async fn load_cache(
        self: Arc<Self>,
        mut requests: tokio::sync::mpsc::UnboundedReceiver<LoadCacheRequest>,
    ) {
        let mut heap = mut_binary_heap::BinaryHeap::<SegmentId, HeapEntry, MinComparator>::new();

        let mut mini =
            small_cache::SmallCache::<M3U8CacheKey, MovieData>::new(Duration::from_mins(15));
        'a: loop {
            while let Ok(LoadCacheRequest {
                segment_id,
                cache_in_the_future,
            }) = requests.try_recv()
            {
                add_to_cache(
                    &mut heap,
                    segment_id,
                    HeapEntry {
                        duration_in_future: Duration::ZERO,
                        total_duration: cache_in_the_future,
                    },
                );
            }
            let (
                mut segment_id,
                HeapEntry {
                    mut duration_in_future,
                    total_duration,
                },
            ) = if let Some(entry) = heap.pop_with_key() {
                entry
            } else {
                let Some(LoadCacheRequest {
                    segment_id,
                    cache_in_the_future,
                }) = requests.recv().await
                else {
                    return;
                };

                (
                    segment_id,
                    HeapEntry {
                        duration_in_future: Duration::ZERO,
                        total_duration: cache_in_the_future,
                    },
                )
            };

            let Some(segments_count) = self
                .get_m3u8(&segment_id.m3u8)
                .await
                .inspect_err(|e| {
                    tracing::error!(
                        "Failed to compute the segment count for {:?}: {e:?}",
                        segment_id.m3u8
                    );
                })
                .ok()
                .map(|v| v.segments.len())
            else {
                continue;
            };
            if segment_id.segment_index >= segments_count {
                continue;
            }
            let times = mini.get_or_insert_mut(segment_id.m3u8.clone(), || MovieData {
                next_compute: HashMap::new(),
                segments_count,
            });

            let Some(compute_next) = times.get_next_to_compute(segment_id.segment_index) else {
                tracing::debug!("Nothing more to compute for segment: {:?}", segment_id);
                continue;
            };
            tracing::debug!(
                "Next compute for segment: {:?}, next_index: {}, time_taken: {:.2}, already_computed_in_future: {:.2}",
                segment_id,
                compute_next.next_index,
                compute_next.time_taken.as_secs_f64(),
                duration_in_future.as_secs_f64()
            );
            duration_in_future += compute_next.time_taken;
            if duration_in_future >= total_duration {
                continue;
            }
            if !compute_next.time_taken.is_zero() {
                segment_id.segment_index = compute_next.next_index;

                add_to_cache(
                    &mut heap,
                    segment_id,
                    HeapEntry {
                        duration_in_future,
                        total_duration,
                    },
                );

                continue;
            }
            let mut stream = match self
                .get_segments(
                    segment_id.m3u8.imdb,
                    segment_id.m3u8.server_name.clone(),
                    compute_next.next_index..compute_next.next_index + 1,
                )
                .await
            {
                Ok(stream) => stream.into_stream(),
                Err(e) => {
                    tracing::error!("Error getting segments: {:?}", e);
                    continue;
                }
            };
            let mut ts = ts_parser::TsTimeParser::new(false);
            loop {
                let bytes = match stream.try_next().await {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => break,
                    Err(e) => {
                        tracing::error!("Failed to read the stream bytes: {e:?}");
                        continue 'a;
                    }
                };
                ts.parse_packets(bytes);
            }

            let duration =
                if let (Some(start_time), Some(end_time)) = (ts.start_time(), ts.end_time()) {
                    end_time - start_time
                } else {
                    // default of 10 seconds for each segment
                    10.
                } as f64;
            let duration = Duration::from_secs_f64(duration);
            times.insert(compute_next.next_index, duration);
            duration_in_future += duration;
            if duration_in_future >= total_duration {
                continue;
            }
            segment_id.segment_index = compute_next.next_index + 1;
            if segment_id.segment_index >= segments_count {
                continue;
            }

            add_to_cache(
                &mut heap,
                segment_id,
                HeapEntry {
                    duration_in_future,
                    total_duration,
                },
            );
        }
    }
}
