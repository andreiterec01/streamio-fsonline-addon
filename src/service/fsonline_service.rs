use std::{collections::BTreeSet, ops::Deref, sync::Arc, time::Duration};

use futures::future::join_all;
use itertools::Itertools;
use m3u8_rs::MediaPlaylist;
use scraper::{Element, Html, Selector};
use serde::Serialize;

use crate::{
    contracts::{Imdb, Language, MovieKey, PlayerData, PlayerOption, SeriesData},
    service::{MediaPlaylistQueue, local_m3u8_player::M3U8CacheKey, scrappers},
};

const INVALID_BROWSER_SERVERS: &[&str] = &["Doodstream"];
const INVALID_SCRAPPING_SERVERS: &[&str] = &["Vidsrc", "VOE"];

#[derive(Clone)]
pub struct MovieData {
    pub movie_name: Arc<str>,
    pub release_year: u16,
}

fn normalize_movie_name(movie: &str) -> String {
    // TODO: double space should be only one dash
    movie.trim().to_lowercase().replace(" ", "-")
}

pub struct VideoServerResponse {
    pub players: Arc<[PlayerData]>,
    pub fsonline_url: String,
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

fn weigher(key: &Imdb, value: &Arc<[PlayerData]>) -> u32 {
    let value_size = size_of_val(value) + value.iter().map(|v| v.size()).sum::<usize>();
    (size_of_val(key) + value_size) as u32
}

pub struct VideoServerOptions {
    pub metadata_cache_size: u64,
    pub metadata_cache_time_to_live: Duration,
    pub metadata_cache_time_to_idle: Duration,
}

#[derive(Clone)]
pub struct VideoServer {
    cache: moka::future::Cache<Imdb, Arc<[PlayerData]>>,
    player_scrapper: Arc<scrappers::PlayerScrappers>,
    client: reqwest::Client,
    new_metadata: MediaPlaylistQueue,
}

impl VideoServer {
    pub async fn new(
        client: reqwest::Client,
        player_scrapper: scrappers::PlayerScrappers,
        new_metadata: MediaPlaylistQueue,
        VideoServerOptions {
            metadata_cache_size,
            metadata_cache_time_to_live,
            metadata_cache_time_to_idle,
        }: VideoServerOptions,
    ) -> anyhow::Result<Self> {
        let m3u8_master_files = moka::future::CacheBuilder::new(metadata_cache_size)
            .weigher(weigher)
            .time_to_live(metadata_cache_time_to_live)
            .time_to_idle(metadata_cache_time_to_idle)
            .build();
        Ok(Self {
            client,
            player_scrapper: Arc::new(player_scrapper),
            cache: m3u8_master_files,
            new_metadata,
        })
    }

    pub async fn delete(&self, imdb: &Imdb) {
        self.cache.invalidate(imdb).await;
    }

    pub async fn get(&self, imdb: Imdb, key: &MovieKey) -> anyhow::Result<VideoServerResponse> {
        let MovieKey { movie, data } = key;
        let movie = normalize_movie_name(movie);

        let initial_url = match data {
            crate::contracts::MovieOrSeriesDataKey::Movie { release_year } => {
                format!("https://www3.fsonline.app/film/{movie}-{release_year}/")
            }
            crate::contracts::MovieOrSeriesDataKey::Series(SeriesData { season, episode }) => {
                format!(
                    "https://www3.fsonline.app/episoade/{movie}-sezonul-{season}-episodul-{episode}/"
                )
            }
        };
        let players = self
            .cache
            .try_get_with(imdb, async {
                let response = self
                    .client
                    .get(&initial_url)
                    .send()
                    .await?
                    .error_for_status()?;
                let body = response.text().await?;

                let movie_id = get_movie_id(body)?;

                let response = self
                    .client
                    .post("https://www3.fsonline.app/wp-admin/admin-ajax.php")
                    .form(&[("action", "lazy_player"), ("movieID", &movie_id)])
                    .send()
                    .await?
                    .error_for_status()?
                    .text()
                    .await?;
                let mut players = get_player_options(response);
                players.retain(|player| {
                    !INVALID_BROWSER_SERVERS.contains(&player.server_name.deref())
                });
                let players_string = players
                    .iter()
                    .map(|p| format!("{}: {}", p.server_name, p.iframe_player))
                    .join("\n");
                tracing::info!("For {} got the players:\n{}", movie, players_string);
                let players = players.into_iter().map(async |p| {
                    if INVALID_SCRAPPING_SERVERS.contains(&p.server_name.deref()) {
                        return PlayerData {
                            data: VideoAndSubtitles::default(),
                            iframe_player: p.iframe_player.into(),
                            server_name: p.server_name.into(),
                        };
                    }
                    let data = self
                        .player_scrapper
                        .get_video(&p)
                        .await
                        .inspect_err(|e| {
                            tracing::warn!(
                                "Failed to get the video from server {} for {}: {}",
                                p.server_name,
                                p.iframe_player,
                                e
                            );
                        })
                        .ok()
                        .unwrap_or_default();
                    PlayerData {
                        data,
                        iframe_player: p.iframe_player.into(),
                        server_name: p.server_name.into(),
                    }
                });
                let players: Arc<[PlayerData]> = join_all(players).await.into_iter().collect();
                for player in players.iter() {
                    if let Some(playlist) = &player.data.video {
                        let m3u8_key = M3U8CacheKey {
                            imdb,
                            server_name: player.server_name.clone(),
                        };
                        self.new_metadata.push((m3u8_key, playlist.clone()));
                    }
                }
                anyhow::Ok(players)
            })
            .await
            .map_err(|e| match Arc::try_unwrap(e) {
                Ok(e) => e,
                Err(e) => {
                    anyhow::anyhow!("{e}")
                }
            })?;

        Ok(VideoServerResponse {
            players,
            fsonline_url: initial_url,
        })
    }
}

fn get_movie_id(body: String) -> anyhow::Result<String> {
    let document = scraper::Html::parse_document(&body);

    let selector = Selector::parse("[movie-id]").unwrap();

    let movie_id = document
        .select(&selector)
        .flat_map(|s| s.attr("movie-id"))
        .collect::<BTreeSet<_>>();
    if movie_id.len() != 1 {
        anyhow::bail!(
            "Didn't return one movie id. Returned {} movies",
            movie_id.len()
        );
    }
    let movie_id = movie_id.into_iter().next().unwrap().to_owned();
    Ok(movie_id)
}

fn get_player_options(body: String) -> Vec<PlayerOption> {
    let html = Html::parse_document(&body);

    let selector = Selector::parse("[data-vs]").unwrap();

    html.select(&selector)
        .filter_map(|s| {
            let text = s
                .first_element_child()?
                .first_element_child()?
                .text()
                .next()?;
            Some(PlayerOption {
                iframe_player: s.attr("data-vs")?.to_owned(),
                server_name: text.to_owned(),
            })
        })
        .collect::<Vec<_>>()
}

#[derive(Debug, Default)]
pub struct VideoAndSubtitlesScrapper {
    pub m3u8_url: Option<String>,
    pub subtitles: Arc<[SubtitleFsonline]>,
}

#[derive(Debug, Clone, Default)]
pub struct VideoAndSubtitles {
    pub video: Option<Arc<MediaPlaylist>>,
    pub subtitles: Arc<[SubtitleFsonline]>,
}

impl VideoAndSubtitles {
    pub(crate) fn size(&self) -> usize {
        let video_size = self
            .video
            .as_ref()
            .map(|value| {
                let mut writer = CounterWritter::new();
                value.write_to(&mut writer).unwrap();
                writer.size
            })
            .unwrap_or_default();
        let subtitles_size = self.subtitles.iter().map(|s| s.size()).sum::<usize>();
        size_of_val(self) + video_size + subtitles_size
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct SubtitleFsonline {
    pub url: Arc<str>,
    pub lang: Language,
}

impl SubtitleFsonline {
    fn size(&self) -> usize {
        size_of_val(self) + self.url.len()
    }
}

impl SubtitleFsonline {
    pub fn md5(&self) -> uuid::Uuid {
        let r = md5::compute(self.url.as_bytes());
        uuid::Uuid::from_bytes(*r)
    }
}

impl SubtitleFsonline {
    pub fn new(url: Arc<str>) -> Option<Self> {
        static CORELATIONS: &[(&str, &str, Option<Language>)] = &[
            ("romanian.vtt", "ron", Some(Language::Romania)),
            ("english.vtt", "eng", Some(Language::English)),
            ("russian.vtt", "rus", None),
            ("bulgarian.vtt", "bul", None),
            ("finnish.vtt", "fin", None),
            ("swedish.vtt", "swe", None),
            ("norwegian.vtt", "nno", None),
            ("french.vtt", "fra", None),
            ("indonesian.vtt", "ind", None),
            ("hungarian.vtt", "hun", None),
            ("portuguese.vtt", "por", None),
            ("czech.vtt", "ces", None),
            ("german.vtt", "deu", None),
            ("polish.vtt", "pol", None),
            ("greek.vtt", "ell", None),
            ("italian.vtt", "ita", None),
            ("danish.vtt", "dan", None),
            ("turkish.vtt", "tur", None),
            ("spanish.vtt", "spa", None),
            ("arabic.vtt", "ara", None),
            ("serbian.vtt", "srp", None),
            ("croatian.vtt", "hrv", None),
            ("icelandic.vtt", "isl", None),
        ];
        let Some(last) = url.split('/').next_back() else {
            return Some(Self {
                url,
                lang: Language::Unrecognized,
            });
        };
        let name = last.to_lowercase();
        for (corelation, _, lang) in CORELATIONS {
            if name.ends_with(corelation) {
                return lang.map(|lang| Self { url, lang });
            }
        }

        tracing::warn!("Failed to categorize subtitle {}", url);
        Some(Self {
            url,
            lang: Language::Unrecognized,
        })
    }
}
