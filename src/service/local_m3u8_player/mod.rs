use std::{collections::BinaryHeap, ops::Deref, path::Path, sync::Arc, time::Duration};

use anyhow::Context;
use axum::body::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, HybridCacheProperties, Location, RecoverMode,
};
use futures::TryStreamExt;
use mpeg2ts_reader::packet::Packet;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
mod intervals;
mod time_cache;
use crate::{
    contracts::Imdb,
    service::{ImdbToVideoServer, local_m3u8_player::intervals::Interval},
    ts_parser,
};

#[derive(Clone)]
pub struct M3U8Data {
    pub master: m3u8_rs::MasterPlaylist,
    pub playlist: m3u8_rs::MediaPlaylist,
}

struct CounterWritter {
    size: usize,
}

impl CounterWritter {
    fn new() -> Self {
        Self { size: 0 }
    }
}

impl std::io::Write for CounterWritter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.size += buf.len();
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl M3U8Data {
    fn size(&self) -> usize {
        let mut writer = CounterWritter::new();
        self.master.write_to(&mut writer).unwrap();
        self.playlist.write_to(&mut writer).unwrap();
        writer.size
    }
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Debug, Clone)]
pub struct SegmentId {
    pub m3u8: M3U8CacheKey,
    pub segment_index: usize,
}

impl SegmentId {
    fn size(&self) -> usize {
        self.m3u8.size() + size_of_val(&self.segment_index)
    }
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Debug, Clone)]
pub struct M3U8CacheKey {
    pub imdb: Imdb,
    pub server_name: Arc<str>,
}

impl M3U8CacheKey {
    fn size(&self) -> usize {
        self.server_name.len() + size_of_val(&self.server_name) + size_of_val(&self.imdb)
    }
}

pub struct LocalPlayer {
    client: reqwest::Client,
    m3u8_master_files: moka::future::Cache<M3U8CacheKey, Arc<M3U8Data>>,
    segments_data: HybridCache<SegmentId, hyper::body::Bytes>,
    segments_time_cache: HybridCache<SegmentId, f32>,
    parallelism_count: usize,
    send_to_cache: tokio::sync::mpsc::UnboundedSender<LoadCacheRequest>,
    imdb_to_video_service: ImdbToVideoServer,

    max_segment_duration: f32,
    timeout_waiting_for_playlist: Duration,
    max_segment_duration_after_timeout: f32,
}

impl Clone for LocalPlayer {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            m3u8_master_files: self.m3u8_master_files.clone(),
            segments_data: self.segments_data.clone(),
            segments_time_cache: self.segments_time_cache.clone(),
            parallelism_count: self.parallelism_count,
            send_to_cache: self.send_to_cache.clone(),
            imdb_to_video_service: self.imdb_to_video_service.clone(),
            max_segment_duration: self.max_segment_duration,
            max_segment_duration_after_timeout: self.max_segment_duration_after_timeout,
            timeout_waiting_for_playlist: self.timeout_waiting_for_playlist,
        }
    }
}

fn weigher(key: &M3U8CacheKey, value: &Arc<M3U8Data>) -> u32 {
    (key.size() + value.size()) as u32
}

#[derive(Clone)]
struct SegmentHeapEntry {
    priority: usize,
    range: std::ops::Range<usize>,
    metadata: Arc<M3U8Data>,
    m3u8: M3U8CacheKey,
}

impl PartialEq for SegmentHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl Eq for SegmentHeapEntry {}

impl PartialOrd for SegmentHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SegmentHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority).reverse()
    }
}

struct LoadCacheRequest {
    segment_id: SegmentId,
}

async fn load_cache(
    segments_cache: HybridCache<SegmentId, hyper::body::Bytes>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<LoadCacheRequest>,
    segments_count: usize,
    parallelism: usize,
    m3u8_master_files: moka::future::Cache<M3U8CacheKey, Arc<M3U8Data>>,
    imdb_to_video_service: ImdbToVideoServer,
    client: reqwest::Client,
) {
    let mut segments = BinaryHeap::<SegmentHeapEntry>::new();
    let mut set = JoinSet::new();
    loop {
        while set.len() > parallelism {
            if let Err(e) = set.join_next().await.unwrap() {
                tracing::error!("Error joining cache task: {e}");
            }
        }
        // TODO: fix the range to take into account the segment duration
        while let Ok(LoadCacheRequest { segment_id }) = rx.try_recv() {
            match LocalPlayer::get_m3u8_inner(
                &m3u8_master_files,
                &imdb_to_video_service,
                &client,
                &segment_id.m3u8,
            )
            .await
            {
                Ok(metadata) => {
                    segments.push(SegmentHeapEntry {
                        priority: 0,
                        range: segment_id.segment_index + 1
                            ..segment_id.segment_index + 1 + segments_count,
                        metadata,
                        m3u8: segment_id.m3u8,
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to load the metadata for {:?}: {e}", segment_id.m3u8);
                }
            }
        }
        if segments.is_empty() {
            tracing::info!("Done processing the cached entries");
            let Some(LoadCacheRequest { segment_id }) = rx.recv().await else {
                return;
            };
            match LocalPlayer::get_m3u8_inner(
                &m3u8_master_files,
                &imdb_to_video_service,
                &client,
                &segment_id.m3u8,
            )
            .await
            {
                Ok(metadata) => {
                    segments.push(SegmentHeapEntry {
                        priority: 0,
                        range: segment_id.segment_index + 1
                            ..segment_id.segment_index + 1 + segments_count,
                        metadata,
                        m3u8: segment_id.m3u8,
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to load the metadata for {:?}: {e}", segment_id.m3u8);
                }
            }
        }
        let segment = segments.pop().unwrap();

        let Some(segment_data) = segment.metadata.playlist.segments.get(segment.range.start) else {
            continue;
        };

        {
            let mut new_segment = segment.clone();
            new_segment.range.start += 1;
            new_segment.priority += 1;
            if !new_segment.range.is_empty() {
                segments.push(new_segment);
            }
        }
        let id = SegmentId {
            m3u8: segment.m3u8,
            segment_index: segment.range.start,
        };
        if segments_cache.contains(&id) {
            continue;
        }
        let cache_location = if segment.priority > 15 {
            Location::OnDisk
        } else {
            Location::Default
        };
        let client = client.clone();
        let uri = segment_data.uri.clone();
        let segments_cache = segments_cache.clone();
        set.spawn(async move {
            let r = segments_cache
                .get_or_fetch(&id, || async move {
                    let segment_data = client
                        .get(uri)
                        .send()
                        .await?
                        .error_for_status()?
                        .bytes()
                        .await?;
                    let properties = HybridCacheProperties::default().with_location(cache_location);
                    anyhow::Ok((segment_data, properties))
                })
                .await;
            match r {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("Failed to compute cache: {e}");
                }
            }
        });
    }
}

pub struct LocalPlayerConfig<'a> {
    pub client: reqwest::Client,
    pub cache_ttl: Duration,
    pub master_cache_size_bytes: u64,
    pub directory_cache: &'a Path,
    pub file_segments_cache_size: usize,
    pub memory_segments_cache_size: usize,
    pub parallelism_count: usize,
    pub cache_next_segments: usize,
    pub imdb_to_video_service: ImdbToVideoServer,

    pub max_segment_duration: f32,
    pub timeout_waiting_for_playlist: Duration,
    pub max_segment_duration_after_timeout: f32,
    pub block_size_segments_mb: usize,
}

struct MyEventListener {
    segments_time_cache: HybridCache<SegmentId, f32>,
}

impl foyer::EventListener for MyEventListener {
    type Key = SegmentId;
    type Value = hyper::body::Bytes;
    fn on_leave(&self, reason: foyer::Event, key: &Self::Key, value: &Self::Value)
    where
        Self::Key: foyer::Key,
        Self::Value: foyer::Value,
    {
        match reason {
            foyer::Event::Clear | foyer::Event::Evict => {}
            foyer::Event::Remove | foyer::Event::Replace => {
                return;
            }
        }
        let mut parser = ts_parser::TsStartTimeParser::new();
        let Some(time) = parser.parse_packets(value.clone()) else {
            return;
        };
        self.segments_time_cache.insert(key.clone(), time);
    }
}

impl LocalPlayer {
    pub async fn new(
        LocalPlayerConfig {
            client,
            cache_ttl,
            master_cache_size_bytes,
            directory_cache,
            file_segments_cache_size,
            memory_segments_cache_size,
            parallelism_count,
            imdb_to_video_service,
            cache_next_segments,
            max_segment_duration,
            max_segment_duration_after_timeout,
            timeout_waiting_for_playlist,
            block_size_segments_mb,
        }: LocalPlayerConfig<'_>,
    ) -> anyhow::Result<Self> {
        let device = FsDeviceBuilder::new(directory_cache)
            .with_capacity(file_segments_cache_size)
            .build()?;

        let segments_data: HybridCache<SegmentId, hyper::body::Bytes> = HybridCacheBuilder::new()
            .with_policy(HybridCachePolicy::WriteOnEviction)
            .memory(memory_segments_cache_size)
            .with_weighter(|key: &SegmentId, value: &Bytes| key.size() + value.len())
            .storage()
            .with_recover_mode(RecoverMode::Quiet)
            .with_compression(foyer::Compression::Lz4)
            // use block-based disk cache engine with default configuration
            .with_engine_config(
                BlockEngineConfig::new(device.clone())
                    .with_block_size(block_size_segments_mb * 1024 * 1024),
            )
            .build()
            .await?;

        let device2 = FsDeviceBuilder::new("./cache-timestamps")
            .with_capacity(1024 * 1024 * 512)
            .build()?;

        let segments_time_cache: HybridCache<SegmentId, f32> = HybridCacheBuilder::new()
            .with_policy(HybridCachePolicy::WriteOnEviction)
            .memory(memory_segments_cache_size)
            .with_weighter(|key: &SegmentId, value: &f32| key.size() + size_of_val(value))
            .storage()
            .with_recover_mode(RecoverMode::Quiet)
            .with_compression(foyer::Compression::Lz4)
            // use block-based disk cache engine with default configuration
            .with_engine_config(BlockEngineConfig::new(device2))
            .build()
            .await?;

        let m3u8_master_files = moka::future::CacheBuilder::new(master_cache_size_bytes)
            .weigher(weigher)
            .time_to_live(cache_ttl)
            .build();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(load_cache(
            segments_data.clone(),
            rx,
            cache_next_segments,
            parallelism_count,
            m3u8_master_files.clone(),
            imdb_to_video_service.clone(),
            client.clone(),
        ));

        Ok(Self {
            client,
            m3u8_master_files: moka::future::CacheBuilder::new(master_cache_size_bytes)
                .weigher(weigher)
                .time_to_live(cache_ttl)
                .build(),
            segments_time_cache,
            segments_data,
            parallelism_count,
            send_to_cache: tx,
            imdb_to_video_service,
            max_segment_duration,
            max_segment_duration_after_timeout,
            timeout_waiting_for_playlist,
        })
    }

    async fn get_m3u8_inner(
        m3u8_master_files: &moka::future::Cache<M3U8CacheKey, Arc<M3U8Data>>,
        imdb_to_video_service: &ImdbToVideoServer,
        client: &reqwest::Client,
        m3u8_key: &M3U8CacheKey,
    ) -> anyhow::Result<Arc<M3U8Data>> {
        let r = m3u8_master_files
            .try_get_with_by_ref(m3u8_key, async {
                let m3u8_url = imdb_to_video_service
                    .get_from_server(m3u8_key.imdb, &m3u8_key.server_name)
                    .await?
                    .context("Player not found")?
                    .data
                    .video
                    .context("Video url not scrapped")?;

                let master_bytes = client
                    .get(m3u8_url.deref())
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes()
                    .await?;

                let mut master = m3u8_rs::parse_master_playlist_res(&master_bytes)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                drop(master_bytes);

                let mut first_stream = true;
                master.variants.retain(|v| {
                    if v.is_i_frame {
                        return true;
                    }
                    if first_stream {
                        first_stream = false;
                        return true;
                    }
                    tracing::warn!("Multiple streams available for {m3u8_url}");
                    false
                });
                let stream = master
                    .variants
                    .iter()
                    .find(|v| !v.is_i_frame)
                    .context("No data stream")?;

                let playlist_data = client
                    .get(&stream.uri)
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes()
                    .await?;

                let playlist = m3u8_rs::parse_media_playlist_res(&playlist_data)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                drop(playlist_data);
                anyhow::Ok(Arc::new(M3U8Data { master, playlist }))
            })
            .await;

        match r {
            Ok(r) => Ok(r),
            Err(e) => match Arc::try_unwrap(e) {
                Ok(e) => Err(e),
                Err(e) => Err(anyhow::anyhow!("{e}")),
            },
        }
    }

    pub async fn compute_m3u8_real_segments_duration(
        &self,
        m3u8_key: &M3U8CacheKey,
        with_timeout: bool,
    ) -> anyhow::Result<Vec<SegmentsTime>> {
        let m3u8 = self.get_m3u8(m3u8_key).await?;

        let playlist = &m3u8.playlist;
        let movie_duration: f32 = m3u8.playlist.segments.iter().map(|s| s.duration).sum();
        let segments_len = m3u8.playlist.segments.len();
        let mut one_segment_times = Vec::new();
        _ = self
            .get_segment(SegmentId {
                m3u8: m3u8_key.clone(),
                segment_index: 0,
            })
            .await?;

        for index in 0..playlist.segments.len() {
            let id = SegmentId {
                m3u8: m3u8_key.clone(),
                segment_index: index,
            };
            #[derive(thiserror::Error, Debug)]
            #[error("The cache value was not present")]
            struct Empty;

            let time = self
                .segments_time_cache
                .get_or_fetch(&id.clone(), move || {
                    let segments_data = self.segments_data.clone();
                    async move {
                        let Some(r) = segments_data.get(&id).await.ok().flatten() else {
                            return Err(Empty);
                        };

                        let mut parser = ts_parser::TsStartTimeParser::new();
                        match parser.parse_packets(r.value().clone()) {
                            Some(timestamp) => Ok(timestamp),
                            None => {
                                tracing::error!("The timestamp was not found in the full segment");
                                return Err(Empty);
                            }
                        }
                    }
                })
                .await
                .map(|v| *v)
                .ok();
            if let Some(time) = time {
                one_segment_times.push(OneSegmentTime {
                    start_time: time,
                    segment_index: index,
                });
            }
        }

        let mut intervals = Interval::new(
            playlist.segments.len(),
            movie_duration,
            one_segment_times.iter().cloned(),
        );

        let mut max_segment_duration = self.max_segment_duration;
        let deadline_on = if with_timeout {
            Some(std::time::Instant::now() + self.timeout_waiting_for_playlist)
        } else {
            None
        };
        let mut interval_changed = false;
        while let Some(next_interval) = intervals.next_best_to_split() {
            if !interval_changed && let Some(deadline) = deadline_on {
                if deadline < std::time::Instant::now() {
                    interval_changed = true;
                    max_segment_duration = self.max_segment_duration_after_timeout;
                }
            }
            if next_interval.item().duration() < max_segment_duration {
                break;
            }

            let index = next_interval.index();
            let segment = &playlist.segments[index];
            let id = SegmentId {
                m3u8: m3u8_key.clone(),
                segment_index: index,
            };

            // TODO: this is huge. Separate it
            let r = self
                .segments_time_cache
                .get_or_fetch(&id.clone(), move || {
                    let segments_data = self.segments_data.clone();
                    let client = self.client.clone();
                    let segment_uri = segment.uri.clone();
                    async move {
                        let r = segments_data.get(&id).await.map_err(anyhow::Error::from)?;
                        match r {
                            Some(v) => {
                                let mut parser = ts_parser::TsStartTimeParser::new();
                                match parser.parse_packets(v.value().clone()) {
                                    Some(timestamp) => Ok(timestamp),
                                    None => {
                                        anyhow::bail!(
                                            "The timestamp was not found"
                                        );
                                    }
                                }
                            }
                            None => {
                                let segment_seconds = async {
                                    #[derive(thiserror::Error, Debug)]
                                    enum RetryOrStop {
                                        #[error(transparent)]
                                        Retry(#[from] anyhow::Error),
                                        #[error("The function should be stopped")]
                                        Stop,
                                    }

                                    let mut content_length = None;
                                    for _ in 0..5 {
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
                                                .get(&segment_uri)
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
                                            while let Some(bytes) = bytes_stream
                                                .try_next()
                                                .await
                                                .map_err(anyhow::Error::from)?
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
                                                let duration = f(3..20).await?;
                                                if let Some(time) = duration {
                                                    tracing::error!(
                                                        "Got it in the 3..10 segments!!!"
                                                    );
                                                    return Ok(time);
                                                } else {
                                                    anyhow::bail!(
                                                        "Can't find the time packet in segments 3..10"
                                                    );
                                                }
                                            }

                                            Err(RetryOrStop::Retry(e)) => {
                                                tracing::error!("Error received: {e:?}");
                                                if let Some(deadline) = deadline_on && !interval_changed {
                                                    if std::time::Instant::now()+Duration::from_secs(2) < deadline {
                                                        tokio::time::sleep(Duration::from_secs(2)).await;
                                                    } else {
                                                        anyhow::bail!("Deadline elapsed. We are no longer sleeping");
                                                    }
                                                } else {
                                                    tokio::time::sleep(Duration::from_secs(2)).await;
                                                }
                                            }
                                            Err(RetryOrStop::Stop) => {
                                                anyhow::bail!("Nothing found");
                                            }
                                        }
                                    }
                                    anyhow::bail!("Too many retries");
                                };
                                let r = segment_seconds.await?;
                                Ok(r)
                            }
                        }
                    }
                })
                .await;
            match r {
                Ok(timestamp) => {
                    let segment_time = OneSegmentTime {
                        segment_index: index,
                        start_time: *timestamp.value(),
                    };
                    one_segment_times.push(segment_time);
                    next_interval.split(segment_time.start_time);
                }
                Err(e) => {
                    if index == 0 {
                        anyhow::bail!("Failed to get the timestamp for the first segment: {e:?}");
                    }
                    tracing::error!("Failed to get segment timestamp for index {index}: {e:?}");
                    next_interval.remove();
                }
            }
        }

        let mut segments = Vec::new();

        one_segment_times.sort_by_key(|v| v.segment_index);

        for i in 0..one_segment_times.len() - 1 {
            let segment = SegmentsTime {
                duration: one_segment_times[i + 1].start_time - one_segment_times[i].start_time,
                segments_range: one_segment_times[i].segment_index
                    ..one_segment_times[i + 1].segment_index,
            };
            segments.push(segment);
        }
        let last_segment = one_segment_times.last().unwrap();
        segments.push(SegmentsTime {
            segments_range: last_segment.segment_index..segments_len,
            duration: movie_duration - last_segment.start_time,
        });
        Ok(segments)
    }

    // TODO: find a way to fix the cache duration
    pub async fn get_m3u8(&self, m3u8_key: &M3U8CacheKey) -> anyhow::Result<Arc<M3U8Data>> {
        Self::get_m3u8_inner(
            &self.m3u8_master_files,
            &self.imdb_to_video_service,
            &self.client,
            m3u8_key,
        )
        .await
    }

    pub async fn get_segment(&self, segment_id: SegmentId) -> anyhow::Result<Bytes> {
        self.send_to_cache
            .send(LoadCacheRequest {
                segment_id: segment_id.clone(),
            })
            .ok();
        let r = self
            .segments_data
            .get_or_fetch(&segment_id, || {
                let this = self.clone();
                let m3u8_key = segment_id.m3u8.clone();
                async move {
                    let metadata = this.get_m3u8(&m3u8_key).await?;

                    let segment = metadata
                        .playlist
                        .segments
                        .get(segment_id.segment_index)
                        .context("Invalid segment index")?;

                    let segment_data = this
                        .client
                        .get(&segment.uri)
                        .send()
                        .await?
                        .error_for_status()?
                        .bytes()
                        .await?;
                    anyhow::Ok(segment_data)
                }
            })
            .await?
            .deref()
            .clone();
        Ok(r)
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        let (r1, r2) = tokio::join!(self.segments_time_cache.close(), self.segments_data.close());
        r1?;
        r2?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct SegmentsTime {
    pub segments_range: std::ops::Range<usize>,
    pub duration: f32,
}

#[derive(Clone, Copy, Debug)]
pub(self) struct OneSegmentTime {
    pub(self) segment_index: usize,
    pub(self) start_time: f32,
}

#[cfg(test)]
mod tests {
    #[test]
    fn master_playlist_parser() {
        let input = include_str!("../../../test_files/m3u8_master_file.txt");
        let r = m3u8_rs::parse_master_playlist_res(input.as_bytes()).unwrap();

        let variant = r.variants.into_iter().find(|v| !v.is_i_frame).unwrap();

        dbg!(variant);
        let playlist = include_str!("../../../test_files/m3u8_playlist.txt");
        let _playlist = m3u8_rs::parse_media_playlist_res(playlist.as_bytes()).unwrap();
    }
}
