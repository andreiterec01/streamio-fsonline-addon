use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::TryStreamExt;
use mut_binary_heap::MaxComparator;

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
                time_taken: Duration::from_secs(0),
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
}

impl LocalPlayerInner {
    pub(crate) async fn load_cache(
        self: Arc<Self>,
        mut requests: tokio::sync::mpsc::UnboundedReceiver<LoadCacheRequest>,
    ) {
        let mut heap = mut_binary_heap::BinaryHeap::<SegmentId, Duration, MaxComparator>::new();

        let mut mini =
            small_cache::SmallCache::<M3U8CacheKey, MovieData>::new(Duration::from_secs(15 * 60));
        'a: loop {
            while let Ok(LoadCacheRequest { segment_id }) = requests.try_recv() {
                heap.push(segment_id, self.cache_in_the_future);
            }
            let (mut segment_id, mut time_remaining) =
                if let Some((segment_id, time_remaining)) = heap.pop_with_key() {
                    (segment_id, time_remaining)
                } else {
                    let Some(LoadCacheRequest { segment_id }) = requests.recv().await else {
                        return;
                    };
                    (segment_id, self.cache_in_the_future)
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
                "Next compute for segment: {:?}, next_index: {}, time_taken: {:.2}",
                segment_id,
                compute_next.next_index,
                compute_next.time_taken.as_secs_f64()
            );
            time_remaining = time_remaining.saturating_sub(compute_next.time_taken);
            if time_remaining.is_zero() {
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
            time_remaining = time_remaining.saturating_sub(duration);
            if time_remaining.is_zero() {
                continue;
            }
            segment_id.segment_index = compute_next.next_index;

            heap.push(segment_id, time_remaining);
        }
    }
}
