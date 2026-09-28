use std::sync::Arc;

use serde::{Deserialize, Serialize};
mod intervals;
mod populate_cache;
pub mod segments_database;
pub(crate) mod time_cache_db;
use crate::contracts::Imdb;

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub struct SegmentId {
    pub m3u8: M3U8CacheKey,
    pub segment_index: usize,
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Debug, Clone)]
pub struct M3U8CacheKey {
    pub imdb: Imdb,
    pub server_name: Arc<str>,
}

impl M3U8CacheKey {
    pub(crate) fn size(&self) -> usize {
        self.server_name.len() + size_of::<Self>()
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
