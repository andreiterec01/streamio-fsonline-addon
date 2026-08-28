use std::{collections::BinaryHeap, ops::Deref, path::Path, sync::Arc, time::Duration};

use anyhow::Context;
use axum::body::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, HybridCacheProperties, Location, RecoverMode,
};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
mod intervals;
pub mod segments_database;
pub(crate) mod time_cache_db;
use crate::{
    contracts::Imdb,
    service::{ImdbToVideoServer, local_m3u8_player::time_cache_db::TimeCache},
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

#[derive(Debug)]
pub struct SegmentsTime {
    pub segments_range: std::ops::Range<usize>,
    pub duration: f32,
}

#[derive(Clone, Copy, Debug)]
struct OneSegmentTime {
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
