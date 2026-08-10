use std::{collections::BTreeMap, convert::Infallible, path::Path, sync::Arc, time::Duration};

use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, RecoverMode,
};
use futures::{TryStreamExt, future::join_all};
use m3u8_rs::MediaPlaylist;
use mpeg2ts_reader::packet::Packet;
use serde::{Deserialize, Serialize};

use crate::{
    service::local_m3u8_player::{M3U8CacheKey, OneSegmentTime, SegmentId, intervals::Interval},
    ts_parser,
};

#[derive(thiserror::Error, Debug)]
enum GetSegmentTimeError {
    #[error("Timeout waiting to get the segment time")]
    Timeout,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub struct TimeCacheOptions<'a> {
    pub(crate) smaller_time_between_segments: f32,
    pub(crate) bigger_time_between_segments: f32,
    pub(crate) cache_size_file_mb: usize,
    pub(crate) cache_size_memory_mb: usize,
    pub(crate) cache_path: &'a Path,
    pub(crate) timeout_fast_time: Duration,
    pub(crate) client: reqwest::Client,
}

#[derive(Serialize, Deserialize, Clone)]
struct SegmentTimeCacheValue {
    segments_time: BTreeMap<usize, f32>,
    movie_duration: f32,
    total_segments_count: usize,
}

#[derive(Clone)]
pub struct TimeCache {
    segments_time_cache: HybridCache<M3U8CacheKey, SegmentTimeCacheValue>,
    mutexes: MultipleValueMutex,
    smaller_time_between_segments: f32,
    bigger_time_between_segments: f32,
    timeout_fast_time: Duration,
    client: reqwest::Client,
}

impl TimeCache {
    pub async fn new(
        TimeCacheOptions {
            bigger_time_between_segments,
            smaller_time_between_segments,
            timeout_fast_time,
            cache_size_file_mb,
            cache_size_memory_mb,
            cache_path,
            client,
        }: TimeCacheOptions<'_>,
    ) -> anyhow::Result<Self> {
        assert!(
            smaller_time_between_segments <= bigger_time_between_segments,
            "Invalid arguments"
        );

        let device = FsDeviceBuilder::new(cache_path)
            .with_capacity(1024 * 1024 * cache_size_file_mb)
            .build()?;

        let segments_time_cache: HybridCache<M3U8CacheKey, SegmentTimeCacheValue> =
            HybridCacheBuilder::new()
                .with_policy(HybridCachePolicy::WriteOnEviction)
                // .memory(memory_segments_cache_size)
                .memory(1024 * 1024 * cache_size_memory_mb)
                .with_filter(|_: &M3U8CacheKey, v: &SegmentTimeCacheValue| {
                    !v.segments_time.is_empty()
                })
                .with_weighter(|key: &M3U8CacheKey, v: &SegmentTimeCacheValue| {
                    key.size()
                        + size_of_val(v)
                        + v.segments_time.len() * (size_of::<usize>() + size_of::<f32>())
                })
                .storage()
                .with_recover_mode(RecoverMode::Quiet)
                .with_compression(foyer::Compression::Lz4)
                // use block-based disk cache engine with default configuration
                .with_engine_config(BlockEngineConfig::new(device))
                .build()
                .await?;
        Ok(Self {
            segments_time_cache,
            mutexes: MultipleValueMutex::new(),
            smaller_time_between_segments,
            bigger_time_between_segments,
            timeout_fast_time,
            client,
        })
    }

    async fn get_segment_time(
        &self,
        url: &str,
        deadline_on: Option<tokio::time::Instant>,
    ) -> Result<f32, GetSegmentTimeError> {
        let client = &self.client;
        let segment_uri = url;

        #[derive(thiserror::Error, Debug)]
        enum RetryOrStop {
            #[error(transparent)]
            Retry(#[from] anyhow::Error),
            #[error("The function should be stopped")]
            Stop,
        }

        let mut content_length = None;
        for _ in 0..10 {
            let mut f = async |range: std::ops::Range<usize>| {
                let mut parser = ts_parser::TsStartTimeParser::new();
                let start = range.start * Packet::SIZE;
                let mut end = Some(range.end * Packet::SIZE);
                if let Some(content_length) = content_length {
                    if start >= content_length {
                        return Err(RetryOrStop::Stop);
                    }
                    if end.unwrap() >= content_length {
                        end = None
                    }
                }
                let range = match end {
                    Some(end) => {
                        format!("bytes={}-{}", start, end)
                    }
                    None => {
                        format!("bytes={}-", start)
                    }
                };
                let response = client
                    .get(segment_uri)
                    .header(reqwest::header::RANGE, range)
                    .send()
                    .await
                    .map_err(anyhow::Error::from)?
                    .error_for_status()
                    .map_err(anyhow::Error::from)?;
                if content_length.is_none() {
                    let value = response
                        .headers()
                        .get(reqwest::header::CONTENT_RANGE)
                        .and_then(|v| {
                            let value = v.to_str().ok()?;
                            let (_, length) = value.split_once("/")?;

                            length.parse::<usize>().ok()
                        });
                    content_length = value;
                }
                let mut bytes_stream = response.bytes_stream();
                let mut duration = None;
                while let Some(bytes) =
                    bytes_stream.try_next().await.map_err(anyhow::Error::from)?
                {
                    if let Some(seconds) = parser.parse_packets(bytes) {
                        duration = Some(seconds);
                        break;
                    }
                }
                Ok(duration)
            };
            match f(0..3).await {
                Ok(Some(duration)) => {
                    return Ok(duration);
                }
                Ok(None) => {
                    let duration = f(3..20).await.map_err(anyhow::Error::from)?;
                    if let Some(time) = duration {
                        tracing::error!("Got it in the 3..10 segments!!!");
                        return Ok(time);
                    } else {
                        return Err(anyhow::anyhow!(
                            "Can't find the time packet in segments 3..15"
                        )
                        .into());
                    }
                }

                Err(RetryOrStop::Retry(e)) => {
                    tracing::error!("Error received: {e:?}");
                    if let Some(deadline) = deadline_on {
                        if tokio::time::Instant::now() + Duration::from_secs(2) < deadline {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        } else {
                            return Err(GetSegmentTimeError::Timeout);
                        }
                    } else {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
                Err(RetryOrStop::Stop) => {
                    return Err(anyhow::anyhow!("Nothing found").into());
                }
            }
        }
        Err(anyhow::anyhow!("Too many retries").into())
    }

    pub(super) async fn insert(&self, id: &SegmentId, content: &bytes::Bytes) {
        let mutexes = self.mutexes.get_mutexes(id.m3u8.clone());
        let _insert = mutexes.mutex_insert.lock().await;
        let mut old = {
            let old = self.segments_time_cache.get(&id.m3u8).await;
            let old = match old {
                Ok(Some(old)) => old,
                Ok(None) => {
                    // No need to populate this cache entry now. We can do this only when is needed
                    return;
                }
                Err(e) => {
                    tracing::error!("Failed to load the cache: {e:?}");
                    return;
                }
            };

            if old.segments_time.contains_key(&id.segment_index) {
                return;
            }
            old.value().clone()
        };
        let mut parser = ts_parser::TsStartTimeParser::new();
        let Some(time) = parser.parse_packets(content.clone()) else {
            tracing::warn!("Received packet for {id:?}, but we failed to parse it");
            return;
        };
        let r = old.segments_time.insert(id.segment_index, time).is_none();
        debug_assert!(
            r,
            "The value should not be already there because we checked"
        );
        self.segments_time_cache.insert(id.m3u8.clone(), old);
    }

    async fn get_inner(
        &self,
        segments_data: &HybridCache<SegmentId, bytes::Bytes>,
        m3u8: &M3U8CacheKey,
        original_media_playlist: &MediaPlaylist,
        deadline_on: Option<tokio::time::Instant>,
        max_interval_between_segments: f32,
    ) -> anyhow::Result<Vec<OneSegmentTime>> {
        let mut one_segment_times = Vec::new();
        let movie_duration: f32 = original_media_playlist
            .segments
            .iter()
            .map(|s| s.duration)
            .sum();
        let segments_len = original_media_playlist.segments.len();

        let mut intervals = {
            let segments = self
                .segments_time_cache
                .get_or_fetch(m3u8, || {
                    let segment_data = segments_data.clone();
                    let m3u8_key = m3u8.clone();
                    async move {
                        Result::<_, Infallible>::Ok(
                            get_all_times(&segment_data, &m3u8_key, segments_len, movie_duration)
                                .await,
                        )
                    }
                })
                .await?;

            // TODO: the movie returned changed. We need to delete the old cache
            if (segments.movie_duration - movie_duration).abs() > 0.1
                || segments.total_segments_count != segments_len
            {
                tracing::error!(
                    "The movie {m3u8:?} changed. We will delete all the old cache values"
                );
                for segment_index in 0..segments.total_segments_count {
                    segments_data.remove(&SegmentId {
                        m3u8: m3u8.clone(),
                        segment_index,
                    });
                }
                self.segments_time_cache.remove(m3u8);
                Interval::new(segments_len, movie_duration, [])
            } else {
                let initial_segments =
                    segments
                        .segments_time
                        .iter()
                        .map(|(segment_index, start_time)| OneSegmentTime {
                            segment_index: *segment_index,
                            start_time: *start_time,
                        });
                one_segment_times.extend(initial_segments);
                Interval::new(
                    segments_len,
                    movie_duration,
                    one_segment_times.iter().cloned(),
                )
            }
        };
        let mut something_changed = false;
        while let Some(next_interval) = intervals.next_best_to_split() {
            let duration = next_interval.item().duration();
            if duration < max_interval_between_segments {
                break;
            }

            let index = next_interval.index();

            let r = self
                .get_segment_time(&original_media_playlist.segments[index].uri, deadline_on)
                .await;

            match r {
                Ok(start_time) => {
                    let segment_time = OneSegmentTime {
                        segment_index: index,
                        start_time,
                    };
                    one_segment_times.push(segment_time);
                    next_interval.split(segment_time.start_time);
                    something_changed = true;
                }
                Err(e) => {
                    tracing::error!("Failed to get segment timestamp for index {index}: {e:?}");
                    next_interval.remove();
                }
            }
        }
        if something_changed {
            let mut value = BTreeMap::new();
            for OneSegmentTime {
                segment_index,
                start_time,
            } in one_segment_times.iter()
            {
                value.insert(*segment_index, *start_time);
            }
            let mutexes = self.mutexes.get_mutexes(m3u8.clone());
            let _insert_mutex = mutexes.mutex_insert.lock().await;
            match self.segments_time_cache.get(m3u8).await? {
                None => {}
                Some(old) => {
                    for (segment_index, time) in old.segments_time.iter() {
                        let segment_index = *segment_index;
                        let start_time = *time;
                        if value.insert(segment_index, start_time).is_none() {
                            one_segment_times.push(OneSegmentTime {
                                segment_index,
                                start_time,
                            });
                        }
                    }
                }
            };
            self.segments_time_cache.insert(
                m3u8.clone(),
                SegmentTimeCacheValue {
                    segments_time: value,
                    movie_duration,
                    total_segments_count: segments_len,
                },
            );
        }
        Ok(one_segment_times)
    }

    pub(super) async fn get_or_fetch(
        &self,
        segments_data: &HybridCache<SegmentId, bytes::Bytes>,
        m3u8: &M3U8CacheKey,
        original_media_playlist: &MediaPlaylist,
        fast_response: bool,
    ) -> anyhow::Result<Vec<OneSegmentTime>> {
        let now = tokio::time::Instant::now();
        let mutexes = self.mutexes.get_mutexes(m3u8.clone());
        let _slow_mutex;
        if !fast_response {
            _slow_mutex = mutexes.mutex_slow.lock().await;
        }
        let fast_mutex = mutexes.mutex_fast.lock().await;
        let segments = self
            .get_inner(
                segments_data,
                m3u8,
                original_media_playlist,
                None,
                self.bigger_time_between_segments,
            )
            .await?;
        if fast_response {
            return Ok(segments);
        }
        drop(fast_mutex);
        let deadline_on = fast_response
            .then_some(Some(now + self.timeout_fast_time))
            .flatten();
        self.get_inner(
            segments_data,
            m3u8,
            original_media_playlist,
            deadline_on,
            self.smaller_time_between_segments,
        )
        .await
    }

    pub(super) async fn close(&self) -> foyer::Result<()> {
        self.segments_time_cache.close().await
    }
}

async fn get_time(
    segment_data: &HybridCache<SegmentId, bytes::Bytes>,
    id: &SegmentId,
) -> Option<f32> {
    let r = segment_data.get(id).await.ok()??;
    let time = ts_parser::TsStartTimeParser::new().parse_packets(r.value().clone())?;
    Some(time)
}

async fn get_all_times(
    segment_data: &HybridCache<SegmentId, bytes::Bytes>,
    m3u8_key: &M3U8CacheKey,
    segments: usize,
    movie_duration: f32,
) -> SegmentTimeCacheValue {
    let values = (0..segments).map(async |i| {
        get_time(
            segment_data,
            &SegmentId {
                m3u8: m3u8_key.clone(),
                segment_index: i,
            },
        )
        .await
        .and_then(|time| Some((i, time)))
    });
    let segments_time = join_all(values).await.into_iter().flatten().collect();

    SegmentTimeCacheValue {
        segments_time,
        movie_duration,
        total_segments_count: segments,
    }
}

struct RemoveOnDrop {
    mutex_slow: tokio::sync::Mutex<()>,
    mutex_fast: tokio::sync::Mutex<()>,
    // TODO: this maybe we should remove
    mutex_insert: tokio::sync::Mutex<()>,
    active_mutexes: Arc<dashmap::DashMap<M3U8CacheKey, std::sync::Weak<RemoveOnDrop>>>,
    key: M3U8CacheKey,
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        self.active_mutexes
            .remove_if(&self.key, |_, v| v.upgrade().is_none());
    }
}

#[derive(Default, Clone)]
struct MultipleValueMutex {
    active_mutexes: Arc<dashmap::DashMap<M3U8CacheKey, std::sync::Weak<RemoveOnDrop>>>,
}

impl MultipleValueMutex {
    pub fn new() -> Self {
        Self::default()
    }

    fn get_mutexes(&self, key: M3U8CacheKey) -> Arc<RemoveOnDrop> {
        match self.active_mutexes.entry(key.clone()) {
            dashmap::Entry::Occupied(mut entry) => match entry.get().upgrade() {
                Some(v) => v,
                None => {
                    let value = RemoveOnDrop {
                        active_mutexes: self.active_mutexes.clone(),
                        key,
                        mutex_fast: tokio::sync::Mutex::new(()),
                        mutex_slow: tokio::sync::Mutex::new(()),
                        mutex_insert: tokio::sync::Mutex::new(()),
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
                    mutex_fast: tokio::sync::Mutex::new(()),
                    mutex_slow: tokio::sync::Mutex::new(()),
                    mutex_insert: tokio::sync::Mutex::new(()),
                };
                let value = Arc::new(value);
                entry.insert(Arc::downgrade(&value));
                value
            }
        }
    }
}
