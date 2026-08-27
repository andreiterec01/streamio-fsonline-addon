use std::{collections::BTreeMap, convert::Infallible, path::Path, sync::Arc, time::Duration};

use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, RecoverMode,
};
use futures::{StreamExt, TryStreamExt, future::join_all};
use m3u8_rs::MediaPlaylist;
use moka::ops::compute::{CompResult, Op};
use mpeg2ts_reader::packet::Packet;

use crate::{
    service::{
        SegmentInfo,
        local_m3u8_player::{
            M3U8CacheKey, OneSegmentTime, SegmentId, intervals::Interval,
            segments_database::Database,
        },
    },
    ts_parser,
    utils::MultipleValueMutex,
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

#[derive(Clone)]
pub struct TimeCache {
    // TODO: we should also save the movie duration and segments count. In case the response changes
    // TODO: change Vec to Arc<[]>
    segments_time_cache_moka: moka::future::Cache<M3U8CacheKey, Vec<OneSegmentTime>>,
    mutexes: MultipleValueMutex<M3U8CacheKey>,
    smaller_time_between_segments: f32,
    bigger_time_between_segments: f32,
    timeout_fast_time: Duration,
    client: reqwest::Client,
    db: Database,
}

impl TimeCache {
    pub async fn new(
        db: Database,
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

        let segments_time_cache_moka = moka::future::CacheBuilder::<
            M3U8CacheKey,
            Vec<OneSegmentTime>,
            _,
        >::new(1024 * 1024 * cache_size_memory_mb as u64)
        .weigher(|key, v| {
            (key.size() + size_of_val(v) + v.len() * (size_of::<usize>() + size_of::<f32>())) as u32
        })
        .build();

        Ok(Self {
            mutexes: MultipleValueMutex::new(),
            smaller_time_between_segments,
            bigger_time_between_segments,
            timeout_fast_time,
            client,
            segments_time_cache_moka,
            db,
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
        let _guard = self.mutexes.lock_mutex(id.m3u8.clone()).await;
        self.segments_time_cache_moka
            .entry_by_ref(&id.m3u8)
            .and_compute_with(async |entry| {
                tracing::info!("Entered compute_with");
                let mut old = match entry {
                    None => {
                        return Op::Nop;
                    }
                    Some(entry) => entry.into_value(),
                };

                let index = match old.binary_search_by_key(&id.segment_index, |x| x.segment_index) {
                    Ok(_) => {
                        return Op::Nop;
                    }
                    Err(index) => index,
                };

                let mut parser = ts_parser::TsStartTimeParser::new();
                let Some(time) = parser.parse_packets(content.clone()) else {
                    tracing::warn!("Received packet for {id:?}, but we failed to parse it");
                    return Op::Nop;
                };

                old.insert(
                    index,
                    OneSegmentTime {
                        segment_index: index,
                        start_time: time,
                    },
                );
                Op::Put(old)
            })
            .await;
        tracing::info!("Finished with compute_with");
    }

    async fn get_inner(
        &self,
        m3u8: &M3U8CacheKey,
        original_media_playlist: &MediaPlaylist,
        deadline_on: Option<tokio::time::Instant>,
    ) -> anyhow::Result<Vec<OneSegmentTime>> {
        let movie_duration: f32 = original_media_playlist
            .segments
            .iter()
            .map(|s| s.duration)
            .sum();
        let segments_len = original_media_playlist.segments.len();

        let guard = self.mutexes.lock_mutex(m3u8.clone()).await;
        let response = self
            .segments_time_cache_moka
            .try_get_with_by_ref(m3u8, async {
                tracing::info!("Computed all segment times");
                // TODO: extract into a function this inner methods
                // TODO: we should also get the movie segments count and duration. To check them against the original media playlist
                let mut times = get_all_times_new(&self.db, &m3u8).await?;
                tracing::info!("Done computing all segment times");

                let mut intervals =
                    Interval::new(segments_len, movie_duration, times.iter().cloned());

                let mut something_changed = false;
                while let Some(next_interval) = intervals.next_best_to_split() {
                    let duration = next_interval.item().duration();
                    if duration < self.bigger_time_between_segments {
                        tracing::info!("Duration is {}. Stopping", duration);
                        break;
                    }

                    let index = next_interval.index();
                    // TODO: remove this log
                    tracing::info!("Received new index {index}");

                    let r = self
                        .get_segment_time(&original_media_playlist.segments[index].uri, None)
                        .await;

                    match r {
                        Ok(start_time) => {
                            let segment_time = OneSegmentTime {
                                segment_index: index,
                                start_time,
                            };
                            times.push(segment_time);
                            next_interval.split(segment_time.start_time);
                            something_changed = true;
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to get segment timestamp for index {index}: {e:?}"
                            );
                            next_interval.remove();
                        }
                    }
                }

                if something_changed {
                    times.sort_by_key(|t| t.segment_index);
                }
                anyhow::Ok(times)
            })
            .await
            .map_err(|e| match Arc::try_unwrap(e) {
                Ok(e) => e,
                Err(e) => anyhow::anyhow!("{e:?}"),
            })?;
        drop(guard);
        if deadline_on.is_some_and(|d| d < tokio::time::Instant::now()) {
            return Ok(response);
        }

        let r = self
            .segments_time_cache_moka
            .entry_by_ref(m3u8)
            .and_compute_with(async |entry| {
                let mut something_changed = entry.is_none();
                let mut times = entry.map_or_else(|| response, |entry| entry.into_value());

                let mut intervals =
                    Interval::new(segments_len, movie_duration, times.iter().cloned());

                while let Some(next_interval) = intervals.next_best_to_split()
                    && deadline_on.is_some_and(|d| d < tokio::time::Instant::now())
                {
                    let duration = next_interval.item().duration();
                    if duration < self.bigger_time_between_segments {
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
                            times.push(segment_time);
                            next_interval.split(segment_time.start_time);
                            something_changed = true;
                            if let Err(e) = self
                                .db
                                .set_segment_info(
                                    m3u8.imdb,
                                    &m3u8.server_name,
                                    &SegmentInfo {
                                        segment_index: index,
                                        start_time: Some(start_time as f64),
                                        size: 0,
                                    },
                                    false,
                                )
                                .await
                            {
                                tracing::error!(
                                    "Failed to save in the database the segment metadata: {e:?}"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to get segment timestamp for index {index}: {e:?}"
                            );
                            next_interval.remove();
                        }
                    }
                }

                if something_changed {
                    times.sort_by_key(|t| t.segment_index);
                    Op::Put(times)
                } else {
                    Op::Nop
                }
            })
            .await;
        Ok(r.unwrap().into_value())
    }

    pub(super) async fn get_or_fetch(
        &self,
        m3u8: &M3U8CacheKey,
        original_media_playlist: &MediaPlaylist,
        fast_response: bool,
    ) -> anyhow::Result<Vec<OneSegmentTime>> {
        let deadline_on = fast_response
            .then_some(Some(tokio::time::Instant::now() + self.timeout_fast_time))
            .flatten();
        let segments = self
            .get_inner(m3u8, original_media_playlist, deadline_on)
            .await?;
        Ok(segments)
    }

    // TODO: this is not needed. The last acces time should not be updated from here
    // pub(super) async fn close(&self) -> anyhow::Result<()> {
    // self.segments_time_cache_moka.invalidate_all();
    // self.segments_time_cache_moka.run_pending_tasks().await;
    // }
}

async fn get_time(
    segment_data: &HybridCache<SegmentId, bytes::Bytes>,
    id: &SegmentId,
) -> Option<f32> {
    let r = segment_data.get(id).await.ok()??;
    let time = ts_parser::TsStartTimeParser::new().parse_packets(r.value().clone())?;
    Some(time)
}

async fn get_all_times_new(
    db: &Database,
    m3u8_key: &M3U8CacheKey,
) -> anyhow::Result<Vec<OneSegmentTime>> {
    let r = db
        .get_segments_info(m3u8_key.imdb, &m3u8_key.server_name)
        .try_filter_map(|row| {
            std::future::ready(Ok(row.start_time.map(|start_time| OneSegmentTime {
                start_time: start_time as f32,
                segment_index: row.segment_index,
            })))
        })
        .try_collect()
        .await?;
    Ok(r)
}
