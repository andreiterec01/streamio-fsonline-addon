use std::{collections::BTreeSet, ops::Deref, sync::Arc, time::Duration};

use futures::future::join_all;
use itertools::Itertools;
use scraper::{Element, Html, Selector};
use serde::Serialize;

use crate::{
    contracts::{Language, MovieKey, PlayerData, PlayerOption, SeriesData},
    service::scrappers,
};

const INVALID_BROWSER_SERVERS: &[&str] = &["Doodstream"];
const INVALID_SCRAPPING_SERVERS: &[&str] = &["Vidsrc", "VOE"];

#[derive(Clone)]
pub struct MovieData {
    pub movie_name: Arc<str>,
    pub release_year: u16,
}

#[derive(Clone)]
pub struct VideoServer {
    cache: moka::future::Cache<MovieKey, Arc<[PlayerData]>>,
    player_scrapper: Arc<scrappers::PlayerScrappers>,
    client: reqwest::Client,
}

fn normalize_movie_name(movie: &str) -> String {
    // TODO: double space should be only one dash
    movie.trim().to_lowercase().replace(" ", "-")
}

impl VideoServer {
    pub async fn new(
        client: reqwest::Client,
        player_scrapper: scrappers::PlayerScrappers,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            player_scrapper: Arc::new(player_scrapper),
            cache: moka::future::CacheBuilder::new(100_000)
                .time_to_live(Duration::from_secs(3600 * 4))
                .build(),
        })
    }

    pub async fn get(&self, movie: &MovieKey) -> anyhow::Result<Arc<[PlayerData]>> {
        let players = self.cache.try_get_with_by_ref(movie, async {
            let MovieKey { movie, data } = movie;
            let movie = normalize_movie_name(movie);

            let initial_url = match data {
                crate::contracts::MovieOrSeriesDataKey::Movie { release_year } => {
                    format!(
                        "https://www3.fsonline.app/film/{movie}-{release_year}/"
                    )
                },
                crate::contracts::MovieOrSeriesDataKey::Series(SeriesData { season, episode }) => {
                    format!(
                        "https://www3.fsonline.app/episoade/{movie}-sezonul-{season}-episodul-{episode}/"
                    )
                }
            };


            let response = self.client.get(initial_url).send().await?.error_for_status()?;
            let body = response.text().await?;

            let movie_id = get_movie_id(body)?;

            let response = self.client
                .post("https://www3.fsonline.app/wp-admin/admin-ajax.php")
                .form(&[("action", "lazy_player"), ("movieID", &movie_id)])
                .send()
                .await?
                .error_for_status()?
                .text()
                .await?;
            let mut players = get_player_options(response);
            players.retain(|player| !INVALID_BROWSER_SERVERS.contains(&player.server_name.deref()));
            let players_string = players.iter().map(|p| format!("{}: {}", p.server_name,p.iframe_player)).join("\n");
            tracing::info!("For {} got the players:\n{}", movie, players_string);
            let players = players.into_iter().map(async |p|  {
                if INVALID_SCRAPPING_SERVERS.contains(&p.server_name.deref()) {
                    return PlayerData {
                        data: VideoAndSubtitles::default(),
                        iframe_player: p.iframe_player.into(),
                        server_name: p.server_name.into()
                    };
                }
                let data = self.player_scrapper.get_video(&p).await.inspect_err(|e| {
                    tracing::warn!("Failed to get the video from server {} for {}: {}", p.server_name,p.iframe_player, e);
                }).ok().unwrap_or_default();
                PlayerData {
                    data,
                    iframe_player: p.iframe_player.into(),
                    server_name: p.server_name.into()
                }
            });
            let players = join_all(players).await.into_iter().collect();

            anyhow::Ok(players)
        }).await.map_err(|e| {
            match Arc::try_unwrap(e) {
                Ok(e) => e,
                Err(e) => {
                    anyhow::anyhow!("{e}")
                }
            }
        })?;

        Ok(players)
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

#[derive(Debug, Serialize, Clone, Default)]
pub struct VideoAndSubtitles {
    pub video: Option<Arc<str>>,
    pub subtitles: Arc<[SubtitleFsonline]>,
}

#[derive(Debug, Serialize, Clone)]
pub struct SubtitleFsonline {
    pub url: Arc<str>,
    pub lang: Language,
}

impl SubtitleFsonline {
    pub fn md5(&self) -> String {
        format!("{:x}", md5::compute(self.url.as_bytes()))
    }
}

impl SubtitleFsonline {
    pub fn new(url: Arc<str>) -> Option<Self> {
        static CORELATIONS: &[(&'static str, &'static str, Option<Language>)] = &[
            ("romanian.vtt", "ron", Some(Language::Romania)),
            ("english.vtt", "eng", Some(Language::English)),
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
                return lang.clone().map(|lang| Self { url, lang });
            }
        }

        tracing::warn!("Failed to categorize subtitle {}", url);
        Some(Self {
            url,
            lang: Language::Unrecognized,
        })
    }
}
