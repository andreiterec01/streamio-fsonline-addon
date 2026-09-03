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
    duration_from_start_to_not_computed_index: f64,
}

struct NextResult {
    next_index: usize,
    time_taken: f64,
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
                time_taken: 0.,
            });
        };

        Some(NextResult {
            next_index: newest.not_computed_index?,
            time_taken: newest.duration_from_start_to_not_computed_index,
        })
    }

    fn insert(&mut self, index: usize, duration: f64) {
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
    pub(crate) time_remaining: f64,
}

impl LocalPlayerInner {
    pub(crate) async fn load_cache(
        self: Arc<Self>,
        mut requests: tokio::sync::mpsc::UnboundedReceiver<LoadCacheRequest>,
    ) {
        #[derive(PartialEq, PartialOrd)]
        struct F64Total(f64);
        impl Eq for F64Total {}
        impl Ord for F64Total {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.total_cmp(&other.0)
            }
        }

        let mut heap = mut_binary_heap::BinaryHeap::<SegmentId, F64Total, MaxComparator>::new();

        let mut mini =
            small_cache::SmallCache::<M3U8CacheKey, MovieData>::new(Duration::from_secs(15 * 60));
        loop {
            while let Some(LoadCacheRequest {
                segment_id,
                time_remaining,
            }) = requests.try_recv().ok()
            {
                heap.push(segment_id, F64Total(time_remaining));
            }
            let item = if let Some((segment_id, F64Total(time_remaining))) = heap.pop_with_key() {
                LoadCacheRequest {
                    segment_id,
                    time_remaining,
                }
            } else {
                let Some(value) = requests.recv().await else {
                    return;
                };
                value
            };
            let LoadCacheRequest {
                mut segment_id,
                mut time_remaining,
            } = item;
            // TODO: remove this log
            tracing::info!(
                "Loading cache for segment: {:?}, time_remaining: {}",
                segment_id,
                time_remaining
            );
            // TODO: remove this unwrap
            let segments_count = self
                .get_m3u8(&segment_id.m3u8)
                .await
                .unwrap()
                .segments
                .len();
            let times = mini.get_or_insert_mut(segment_id.m3u8.clone(), || MovieData {
                next_compute: HashMap::new(),
                segments_count,
            });

            let Some(compute_next) = times.get_next_to_compute(segment_id.segment_index) else {
                tracing::info!("Nothing more to compute for segment: {:?}", segment_id);
                continue;
            };
            tracing::info!(
                "Next compute for segment: {:?}, next_index: {}, time_taken: {}",
                segment_id,
                compute_next.next_index,
                compute_next.time_taken
            );
            time_remaining -= compute_next.time_taken;
            if time_remaining <= 0. {
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
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!("Error getting segments: {:?}", e);
                    continue;
                }
            };
            let mut ts = ts_parser::TsTimeParser::new(false);
            //TODO: remove the unwrap
            while let Some(bytes) = stream.stream.try_next().await.unwrap() {
                ts.parse_packets(bytes);
            }
            let duration =
                if let (Some(start_time), Some(end_time)) = (ts.start_time(), ts.end_time()) {
                    end_time - start_time
                } else {
                    // default of 10 seconds for each segment
                    10.
                } as f64;
            times.insert(compute_next.next_index, duration);
            time_remaining -= duration;
            if time_remaining <= 0. {
                continue;
            }
            segment_id.segment_index = compute_next.next_index;

            heap.push(segment_id, F64Total(time_remaining));
        }
    }
}
