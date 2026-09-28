use std::{collections::HashMap, ops::Deref, sync::Arc};

use anyhow::Context;
use m3u8_rs::MediaPlaylist;
use reqwest::IntoUrl;

use crate::{
    contracts::PlayerOption,
    service::fsonline_service::{VideoAndSubtitles, VideoAndSubtitlesScrapper},
};
pub mod browser_discovery_scrapper;
pub mod file_sun;
pub mod vidmoly;
pub struct PlayerScrappers {
    default_scrapper: PlayerScrapperBox,
    specific_scrapper: HashMap<&'static str, PlayerScrapperBox>,
    client: reqwest::Client,
}

impl PlayerScrappers {
    pub fn new(client: reqwest::Client, default_scrapper: impl PlayerScrapper + 'static) -> Self {
        Self {
            default_scrapper: Box::new(default_scrapper),
            specific_scrapper: HashMap::new(),
            client,
        }
    }

    pub fn add_scrapper(&mut self, scrapper: impl SpecificScrapper + 'static) {
        let server_name = scrapper.server_name();
        if self
            .specific_scrapper
            .insert(server_name, Box::new(scrapper))
            .is_some()
        {
            panic!("A scrapper for the server {server_name} has added twice");
        }
    }

    pub async fn get_video(&self, player: &PlayerOption) -> anyhow::Result<VideoAndSubtitles> {
        let scrapper = self
            .specific_scrapper
            .get(player.server_name.deref())
            .unwrap_or(&self.default_scrapper);
        let VideoAndSubtitlesScrapper {
            m3u8_url,
            subtitles,
        } = scrapper.get_video(&player.iframe_player).await?;

        let video = match m3u8_url {
            Some(url) => get_media_playlist(&self.client, url)
                .await
                .inspect_err(|e| tracing::warn!("Failed to download the media playlist: {e}"))
                .ok(),
            None => None,
        };
        Ok(VideoAndSubtitles {
            subtitles,
            video: video.map(Arc::new),
        })
    }
}

pub type PlayerScrapperBox = Box<dyn PlayerScrapper>;

#[async_trait::async_trait]
pub trait PlayerScrapper: Send + Sync {
    async fn get_video(&self, url: &str) -> anyhow::Result<VideoAndSubtitlesScrapper>;
}

pub trait SpecificScrapper: PlayerScrapper {
    fn server_name(&self) -> &'static str;
}

pub(super) async fn get_media_playlist(
    client: &reqwest::Client,
    m3u8_url: impl IntoUrl,
) -> anyhow::Result<MediaPlaylist> {
    let m3u8_url = m3u8_url.into_url()?;
    let master_bytes = client
        .get(m3u8_url.clone())
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    let playlist =
        m3u8_rs::parse_playlist_res(&master_bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
    drop(master_bytes);

    let master = match playlist {
        m3u8_rs::Playlist::MediaPlaylist(media_playlist) => {
            return Ok(media_playlist);
        }
        m3u8_rs::Playlist::MasterPlaylist(master_playlist) => master_playlist,
    };
    let mut first_stream = true;
    master.variants.iter().for_each(|v| {
        if v.is_i_frame {
            return;
        }
        if first_stream {
            first_stream = false;
            return;
        }
        tracing::warn!("Multiple streams available for {m3u8_url}");
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

    let playlist =
        m3u8_rs::parse_media_playlist_res(&playlist_data).map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(playlist)
}
