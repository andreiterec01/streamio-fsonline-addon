use std::{sync::Arc, time::Duration};

use anyhow::Context;
use axum::body::Bytes;

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

pub struct LocalPlayer {
    client: reqwest::Client,
    m3u8_master_files: moka::future::Cache<String, Arc<M3U8Data>>,
}

impl Clone for LocalPlayer {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            m3u8_master_files: self.m3u8_master_files.clone(),
        }
    }
}

fn weigher(key: &String, value: &Arc<M3U8Data>) -> u32 {
    (key.capacity() + size_of::<String>() + value.size()) as u32
}

impl LocalPlayer {
    pub fn new(client: reqwest::Client, cache_ttl: Duration, master_cache_size_bytes: u64) -> Self {
        Self {
            client,
            m3u8_master_files: moka::future::CacheBuilder::new(master_cache_size_bytes)
                .weigher(weigher)
                .time_to_live(cache_ttl)
                .build(),
        }
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

    pub async fn get_segment(&self, m3u8_url: &str, segment_index: usize) -> anyhow::Result<Bytes> {
        let metadata = self.get_m3u8(m3u8_url).await?;

        let segment = metadata
            .playlist
            .segments
            .get(segment_index)
            .context("Invalid segment index")?;

        let segment_data = self
            .client
            .get(&segment.uri)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        Ok(segment_data)
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
