use std::{collections::BinaryHeap, ops::Deref, path::Path, sync::Arc, time::Duration};

use anyhow::Context;
use axum::body::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, HybridCacheProperties, Location, RecoverMode,
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::{contracts::Imdb, service::ImdbToVideoServer};

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
    parallelism_count: usize,
    send_to_cache: tokio::sync::mpsc::UnboundedSender<LoadCacheRequest>,
    imdb_to_video_service: ImdbToVideoServer,
}

impl Clone for LocalPlayer {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            m3u8_master_files: self.m3u8_master_files.clone(),
            segments_data: self.segments_data.clone(),
            parallelism_count: self.parallelism_count,
            send_to_cache: self.send_to_cache.clone(),
            imdb_to_video_service: self.imdb_to_video_service.clone(),
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
        let cache_location = if segment.priority == 0 {
            Location::Default
        } else {
            Location::OnDisk
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
    pub cache_next_seconds_on_disk: usize,
    pub imdb_to_video_service: ImdbToVideoServer,
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
            cache_next_seconds_on_disk,
            imdb_to_video_service,
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
            .with_engine_config(BlockEngineConfig::new(device))
            .build()
            .await?;

        let m3u8_master_files = moka::future::CacheBuilder::new(master_cache_size_bytes)
            .weigher(weigher)
            .time_to_live(cache_ttl)
            .build();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // TODO: make this configurable
        tokio::spawn(load_cache(
            segments_data.clone(),
            rx,
            30,
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
            segments_data,
            parallelism_count,
            send_to_cache: tx,
            imdb_to_video_service,
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
        self.segments_data.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn master_playlist_parser() {
        let input = include_str!("../../test_files/m3u8_master_file.txt");
        let r = m3u8_rs::parse_master_playlist_res(input.as_bytes()).unwrap();

        let variant = r.variants.into_iter().find(|v| !v.is_i_frame).unwrap();

        dbg!(variant);
        let playlist = include_str!("../../test_files/m3u8_playlist.txt");
        let _playlist = m3u8_rs::parse_media_playlist_res(playlist.as_bytes()).unwrap();
    }
}
