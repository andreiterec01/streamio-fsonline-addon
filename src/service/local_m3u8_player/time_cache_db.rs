use std::{ops::Deref, path::PathBuf, sync::Arc, time::Duration};

use futures::TryStreamExt;
use m3u8_rs::MediaPlaylist;
use moka::ops::compute::Op;
use mpeg2ts_reader::packet::Packet;
use tokio::task::JoinSet;

use crate::{
    service::{
        PlaylistInfoMetadata, SegmentInfo,
        local_m3u8_player::{
            M3U8CacheKey, OneSegmentTime, SegmentId,
            intervals::Interval,
            segments_database::{Database, LocalPlayerInner},
        },
    },
    ts_parser,
};

#[derive(thiserror::Error, Debug)]
enum GetSegmentTimeError {
    #[error("Timeout waiting to get the segment time")]
    Timeout,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub struct TimeCacheOptions {
    pub(crate) smaller_time_between_segments: f32,
    pub(crate) bigger_time_between_segments: f32,
    pub(crate) cache_size_memory_mb: usize,
    pub(crate) timeout_fast_time: Duration,
    pub(crate) client: reqwest::Client,
    pub(crate) cache_directory: PathBuf,
}

#[derive(Clone)]
struct SegmentTimeCacheValue {
    segments_time: Arc<[OneSegmentTime]>,
    dont_compute: Arc<[usize]>,
    movie_duration: f32,
    total_segments_count: usize,
}

#[derive(Clone)]
pub struct TimeCache {
    segments_time_cache_moka: moka::future::Cache<M3U8CacheKey, SegmentTimeCacheValue>,
    target_time_between_segments: f32,
    max_time_between_segments: f32,
    timeout_fast_time: Duration,
    client: reqwest::Client,
    cache_directory: PathBuf,
    db: Database,
}

impl TimeCache {
    pub async fn new(
        db: Database,
        TimeCacheOptions {
            bigger_time_between_segments,
            smaller_time_between_segments,
            timeout_fast_time,
            cache_size_memory_mb,
            client,
            cache_directory,
        }: TimeCacheOptions,
    ) -> anyhow::Result<Self> {
        assert!(
            smaller_time_between_segments <= bigger_time_between_segments,
            "Invalid arguments"
        );

        let segments_time_cache_moka =
            moka::future::CacheBuilder::<M3U8CacheKey, SegmentTimeCacheValue, _>::new(
                1024 * 1024 * cache_size_memory_mb as u64,
            )
            .weigher(|key, v| {
                (key.size() + size_of_val(v) + v.segments_time.len() * size_of::<OneSegmentTime>())
                    as u32
            })
            .build();

        Ok(Self {
            cache_directory,
            target_time_between_segments: smaller_time_between_segments,
            max_time_between_segments: bigger_time_between_segments,
            timeout_fast_time,
            client,
            segments_time_cache_moka,
            db,
        })
    }

    async fn get_segment_time(
        &self,
        url: &str,
        deadline_on: tokio::time::Instant,
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
                let mut parser = ts_parser::TsTimeParser::new(true);
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
                    if let Some(seconds) = parser.parse_and_return_start_time(bytes) {
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
                    if tokio::time::Instant::now() + Duration::from_secs(2) < deadline_on {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    } else {
                        return Err(GetSegmentTimeError::Timeout);
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
        self.segments_time_cache_moka
            .entry_by_ref(&id.m3u8)
            .and_compute_with(async |entry| {
                let old = match entry {
                    None => {
                        return Op::Nop;
                    }
                    Some(entry) => entry.into_value(),
                };

                let index = match old
                    .segments_time
                    .binary_search_by_key(&id.segment_index, |x| x.segment_index)
                {
                    Ok(_) => {
                        return Op::Nop;
                    }
                    Err(index) => index,
                };

                let mut parser = ts_parser::TsTimeParser::new(true);
                let Some(time) = parser.parse_and_return_start_time(content.clone()) else {
                    tracing::warn!("Received packet for {id:?}, but we failed to parse it");
                    return Op::Nop;
                };
                let dont_compute = if old.dont_compute.contains(&id.segment_index) {
                    old.dont_compute
                        .iter()
                        .filter(|x| **x != id.segment_index)
                        .copied()
                        .collect()
                } else {
                    old.dont_compute
                };
                let new_segments = old.segments_time[..index]
                    .iter()
                    .cloned()
                    .chain(std::iter::once(OneSegmentTime {
                        segment_index: id.segment_index,
                        start_time: time,
                    }))
                    .chain(old.segments_time[index..].iter().cloned())
                    .collect::<Vec<_>>();
                let new_value = SegmentTimeCacheValue {
                    segments_time: Arc::from(new_segments),
                    movie_duration: old.movie_duration,
                    total_segments_count: old.total_segments_count,
                    dont_compute,
                };

                Op::Put(new_value)
            })
            .await;
    }

    async fn compute_times_without_cache(
        &self,
        m3u8: &M3U8CacheKey,
        original_media_playlist: &MediaPlaylist,
        deadline_on: tokio::time::Instant,
        entry: Option<SegmentTimeCacheValue>,
    ) -> anyhow::Result<(SegmentTimeCacheValue, bool)> {
        let movie_duration: f32 = original_media_playlist
            .segments
            .iter()
            .map(|s| s.duration)
            .sum();
        let segments_len = original_media_playlist.segments.len();
        let mut finished_computing;
        let result = match entry {
            None => {
                let times = get_all_times_new(&self.db, m3u8).await?;
                let metadata = self
                            .db
                            .get_playlist_metadata(m3u8.imdb, &m3u8.server_name)
                            .await?
                            .unwrap_or_else(|| {
                                tracing::warn!("No metadata found for {m3u8:?}. We will asume the one from the original media playlist");
                                PlaylistInfoMetadata {
                                movie_duration: movie_duration as f64,
                                total_segments: segments_len,
                            }});

                // TODO: maybe we want to also save in the database so we don't compute this times
                SegmentTimeCacheValue {
                    segments_time: times.into(),
                    movie_duration: metadata.movie_duration as f32,
                    total_segments_count: metadata.total_segments,
                    dont_compute: Arc::default(),
                }
            }
            Some(entry) => entry,
        };
        let mut times = result.segments_time.deref().to_owned();
        if (result.movie_duration - movie_duration).abs() > 0.1
            || result.total_segments_count != segments_len
        {
            tracing::warn!(
                "The movie duration or total segments count changed for {m3u8:?}. We should recompute all segment times"
            );
            times = Vec::new();
            self.delete_full_movie(
                m3u8,
                PlaylistInfoMetadata {
                    movie_duration: movie_duration as f64,
                    total_segments: segments_len,
                },
            )
            .await?;
        }

        let mut intervals = Interval::new(
            segments_len,
            movie_duration,
            times.iter().cloned(),
            result.dont_compute.iter().copied(),
        );
        finished_computing = intervals.next_best_to_split().is_none();
        while let Some(next_interval) = intervals.next_best_to_split() {
            let duration = next_interval.item().duration();
            if duration < self.target_time_between_segments {
                finished_computing = true;
                break;
            }
            if duration < self.max_time_between_segments
                && tokio::time::Instant::now() > deadline_on
            {
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
                Err(GetSegmentTimeError::Timeout) => {
                    tracing::info!("Timeout getting segment time for index {index}");
                    break;
                }
                Err(GetSegmentTimeError::Other(e)) => {
                    tracing::error!("Failed to get segment timestamp for index {index}: {e:?}");
                    next_interval.remove();
                }
            }
        }

        // TODO: Implement a merge sort. Between the previous times and the new times added
        times.sort_by_key(|t| t.segment_index);
        anyhow::Ok((
            SegmentTimeCacheValue {
                segments_time: Arc::from(times),
                movie_duration,
                total_segments_count: segments_len,
                dont_compute: intervals.forbidden_indexes().collect(),
            },
            finished_computing,
        ))
    }

    async fn fast_get_times(
        &self,
        m3u8: &M3U8CacheKey,
        original_media_playlist: &MediaPlaylist,
    ) -> anyhow::Result<SegmentTimeCacheValue> {
        let deadline_on = tokio::time::Instant::now() + self.timeout_fast_time;
        let r = self
            .segments_time_cache_moka
            .entry_by_ref(m3u8)
            .or_try_insert_with(async {
                let (r, _) = self
                    .compute_times_without_cache(m3u8, original_media_playlist, deadline_on, None)
                    .await?;
                anyhow::Ok(r)
            })
            .await
            .map_err(|e| match Arc::try_unwrap(e) {
                Ok(e) => e,
                Err(e) => anyhow::anyhow!("{e:?}"),
            })?
            .into_value();

        Ok(r)
    }

    pub async fn slow_fetch_times(
        &self,
        m3u8: &M3U8CacheKey,
        original_media_playlist: &MediaPlaylist,
    ) -> anyhow::Result<()> {
        // TODO: make this configurable
        let stop_retry_at = std::time::Instant::now() + Duration::from_mins(20);
        loop {
            let mut finished = false;
            let mut computed = false;
            self.segments_time_cache_moka
                .entry_by_ref(m3u8)
                .and_try_compute_if_nobody_else(|entry| async {
                    computed = true;
                    let entry = match entry {
                        Some(entry) => entry.into_value(),
                        None => self.fast_get_times(m3u8, original_media_playlist).await?,
                    };

                    // TODO: make the 10s configurable
                    let (r, f) = self
                        .compute_times_without_cache(
                            m3u8,
                            original_media_playlist,
                            tokio::time::Instant::now() + Duration::from_secs(10),
                            Some(entry),
                        )
                        .await?;
                    finished = f;
                    anyhow::Ok(Op::Put(r))
                })
                .await?;
            if computed {
                if finished {
                    tracing::info!("Finished computing segment times for {m3u8}");
                } else {
                    tracing::info!("Incremental computed segment times for {m3u8}");
                }
            }
            if !computed || finished || stop_retry_at < std::time::Instant::now() {
                break;
            }
        }

        Ok(())
    }

    pub(super) async fn get_or_fetch(
        &self,
        m3u8: M3U8CacheKey,
        original_media_playlist: Arc<MediaPlaylist>,
    ) -> anyhow::Result<Arc<[OneSegmentTime]>> {
        let r = self
            .fast_get_times(&m3u8, &original_media_playlist)
            .await?
            .segments_time;
        let this = self.clone();

        tokio::spawn(async move {
            if let Err(e) = this.slow_fetch_times(&m3u8, &original_media_playlist).await {
                tracing::error!("Failed to fetch times: {e:?}");
            }
        });
        Ok(r)
    }

    pub(crate) async fn delete_full_movie(
        &self,
        m3u8_key: &M3U8CacheKey,
        new_metadata: PlaylistInfoMetadata,
    ) -> anyhow::Result<()> {
        let directory = LocalPlayerInner::directory_movie_file_path(
            &self.cache_directory,
            m3u8_key.imdb,
            &m3u8_key.server_name,
        );
        let mut directory_content = tokio::fs::read_dir(&directory).await?;
        let mut s = JoinSet::new();
        while let Some(file) = directory_content.next_entry().await? {
            let path = file.path();
            s.spawn(LocalPlayerInner::delete_file(path));
        }
        while let Some(res) = s.join_next().await {
            res??;
        }
        drop(s);
        self.db
            .delete_movie_segment_data(m3u8_key.imdb, &m3u8_key.server_name)
            .await?;
        self.db
            .set_playlist_metadata(m3u8_key.imdb, &m3u8_key.server_name, &new_metadata)
            .await?;
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use moka::ops::compute::Op;

    #[tokio::test]
    async fn test_combine_get_with_compute() {
        let cache = moka::future::Cache::<i32, i32>::new(100);

        let r = cache
            .entry(10)
            .and_compute_with(|_| async {
                let r = cache.get_with(10, async { 20 }).await;
                Op::Put(r)
            })
            .await
            .unwrap()
            .into_value();

        assert_eq!(r, 20);
    }
}
