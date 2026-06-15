use std::{collections::BinaryHeap, ops::Deref, path::Path, sync::Arc, time::Duration};

use anyhow::Context;
use axum::body::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, HybridCacheProperties, Location, RecoverMode,
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::contracts::ImdbId;

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
    pub server_name: Arc<str>,
    pub imdb: ImdbId,
    pub segment_index: usize,
}

impl SegmentId {
    fn size(&self) -> usize {
        self.server_name.len()
            + size_of_val(&self.server_name)
            + size_of_val(&self.imdb)
            + size_of_val(&self.segment_index)
    }
}

pub struct LocalPlayer {
    client: reqwest::Client,
    m3u8_master_files: moka::future::Cache<String, Arc<M3U8Data>>,
    segments_data: HybridCache<SegmentId, hyper::body::Bytes>,
    parallelism_count: usize,
    send_to_cache: tokio::sync::mpsc::UnboundedSender<LoadCacheRequest>,
}

impl Clone for LocalPlayer {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            m3u8_master_files: self.m3u8_master_files.clone(),
            segments_data: self.segments_data.clone(),
            parallelism_count: self.parallelism_count,
            send_to_cache: self.send_to_cache.clone(),
        }
    }
}

fn weigher(key: &String, value: &Arc<M3U8Data>) -> u32 {
    (key.capacity() + size_of::<String>() + value.size()) as u32
}

#[derive(Clone)]
struct SegmentHeapEntry {
    priority: usize,
    range: std::ops::Range<usize>,
    metadata: Arc<M3U8Data>,
    server_name: Arc<str>,
    imdb: ImdbId,
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
    metadata: Arc<M3U8Data>,
    start_index: usize,
    server_name: Arc<str>,
    imdb: ImdbId,
}

async fn load_cache(
    segments_cache: HybridCache<SegmentId, hyper::body::Bytes>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<LoadCacheRequest>,
    client: reqwest::Client,
    segments_count: usize,
    parallelism: usize,
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
        while let Ok(LoadCacheRequest {
            metadata,
            start_index,
            server_name,
            imdb,
        }) = rx.try_recv()
        {
            segments.push(SegmentHeapEntry {
                priority: 0,
                range: start_index + 1..start_index + 1 + segments_count,
                metadata,
                server_name,
                imdb,
            });
        }
        if segments.is_empty() {
            tracing::info!("Done processing the cached entries");
            let Some(LoadCacheRequest {
                metadata,
                start_index,
                server_name,
                imdb,
            }) = rx.recv().await
            else {
                return;
            };
            segments.push(SegmentHeapEntry {
                priority: 0,
                range: start_index + 1..start_index + 1 + segments_count,
                metadata,
                server_name,
                imdb,
            });
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
            imdb: segment.imdb,
            segment_index: segment.range.start,
            server_name: segment.server_name.clone(),
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

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // TODO: make this configurable
        tokio::spawn(load_cache(
            segments_data.clone(),
            rx,
            client.clone(),
            30,
            parallelism_count,
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
        })
    }
    // TODO: find a way to fix the cache duration
    pub async fn get_m3u8(&self, m3u8_url: &str) -> anyhow::Result<Arc<M3U8Data>> {
        let r = self
            .m3u8_master_files
            .try_get_with_by_ref(m3u8_url, async {
                let master_bytes = self
                    .client
                    .get(m3u8_url)
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

                let playlist_data = self
                    .client
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

    pub async fn get_segment(
        &self,
        segment_id: SegmentId,
        m3u8_url: &str,
    ) -> anyhow::Result<Bytes> {
        let metadata = self.get_m3u8(&m3u8_url).await?;
        self.send_to_cache
            .send(LoadCacheRequest {
                metadata: metadata.clone(),
                start_index: segment_id.segment_index + 1,
                server_name: segment_id.server_name.clone(),
                imdb: segment_id.imdb,
            })
            .ok();
        let r = self
            .segments_data
            .get_or_fetch(&segment_id, || {
                let this = self.clone();
                async move {
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
