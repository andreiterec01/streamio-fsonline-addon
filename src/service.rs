use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::Context;
use chromiumoxide::{Browser, cdp::browser_protocol::network::EventRequestWillBeSent};
use chrono::{DateTime, Datelike, Utc};
use futures::future::join_all;
use itertools::Itertools;
use scraper::{Element, Html, Selector};
use serde::Serialize;

use crate::contracts::{MovieKey, PlayerData, PlayerOption, SeriesData};

#[derive(Clone)]
pub struct MovieData {
    pub movie_name: Arc<str>,
    pub release_year: u16,
}

#[derive(Clone)]
pub struct ImdbService {
    client: reqwest::Client,
    imbp_to_movie_name_cache: moka::future::Cache<u64, MovieData>,
}

impl ImdbService {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            imbp_to_movie_name_cache: moka::future::CacheBuilder::new(100_000).build(),
        }
    }

    pub async fn get(&self, imdb_id: u64, is_series: bool) -> anyhow::Result<MovieData> {
        #[derive(serde::Deserialize)]
        struct Root {
            meta: MetaResponse,
        }

        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub enum ReleasedValue {
            Released(DateTime<Utc>),
            ReleaseInfo(String),
        }
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct MetaResponse {
            name: String,
            #[serde(flatten)]
            released: ReleasedValue,
        }

        let r = self
            .imbp_to_movie_name_cache
            .try_get_with(imdb_id, async {
                let path = if is_series { "series" } else { "movie" };
                let response: Root = self
                    .client
                    .get(dbg!(format!(
                        "https://v3-cinemeta.strem.io/meta/{path}/tt{imdb_id}.json"
                    )))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                let release_year: u16 = match response.meta.released {
                    ReleasedValue::ReleaseInfo(year) => {
                        year.parse().context("Invalid release year")?
                    }
                    ReleasedValue::Released(released) => {
                        released.year().try_into().context("Invalid release year")?
                    }
                };
                let data = MovieData {
                    movie_name: response.meta.name.into(),
                    release_year,
                };
                Ok(data)
            })
            .await
            .map_err(|e| match Arc::try_unwrap(e) {
                Ok(e) => e,
                Err(e) => {
                    anyhow::anyhow!("{e}")
                }
            })?;

        Ok(r)
    }
}

#[derive(Clone)]
pub struct VideoServer {
    cache: moka::future::Cache<MovieKey, Arc<[PlayerData]>>,
    browser: Arc<BrowserDiscovery>,
    client: reqwest::Client,
}

fn normalize_movie_name(movie: &str) -> String {
    // TODO: double space should be only one dash
    movie.trim().to_lowercase().replace(" ", "-")
}

impl VideoServer {
    pub async fn new(client: reqwest::Client, headless_browser: bool) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            browser: Arc::new(BrowserDiscovery::new(headless_browser).await?),
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
            let players = get_player_options(response);
            let players_string = players.iter().map(|p| format!("{}: {}", p.server_name,p.iframe_player)).join("\n");
            tracing::info!("For {} got the players:\n{}", movie, players_string);


            let players = players.into_iter().map(async |p|  {
                let data = self.browser.get_video(&p.iframe_player).await.inspect_err(|e| {
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

#[derive(Debug, Default, Serialize, Clone)]
pub struct VideoAndSubtitles {
    pub video: Option<Arc<str>>,
    pub subtitles: Arc<[Arc<str>]>,
}

pub struct BrowserDiscovery {
    browser: Browser,
}

impl BrowserDiscovery {
    pub async fn new(headless_browser: bool) -> anyhow::Result<Self> {
        use chromiumoxide::{Browser, BrowserConfig};
        use futures::StreamExt;
        let mut config = BrowserConfig::builder();

        if !headless_browser {
            config = config.with_head();
        } else {
            config = config.new_headless_mode();
        }
        let (browser, mut handler) =
            Browser::launch(config.build().map_err(|e| anyhow::anyhow!("{e}"))?).await?;

        tokio::spawn(async move {
            while let Some(_event) = handler.next().await {
                // TODO: add error handling and reopen the browser connection when this happens
            }
        });
        Ok(Self { browser })
    }
    pub async fn get_video(&self, url: &str) -> anyhow::Result<VideoAndSubtitles> {
        use futures::StreamExt;

        let page = self.browser.new_page(url).await?;

        let r = async {
            let mut requests = page.event_listener::<EventRequestWillBeSent>().await?;

            tokio::time::timeout(Duration::from_secs(10), async {
                page.wait_for_navigation().await?;
                page.reload().await?;
                page.wait_for_navigation().await
            })
            .await
            .context("Timeout while waiting for the page to reload")??;

            let mut elapsed_at = tokio::time::Instant::now() + Duration::from_secs(3);
            let player_future = async {
                let mut subtitles = Vec::new();
                let mut video = None;
                while let Some(event) = tokio::time::timeout_at(elapsed_at, requests.next())
                    .await
                    .ok()
                    .and_then(|x| x)
                {
                    let url = match event.request.url.parse::<reqwest::Url>() {
                        Ok(url) => url,
                        Err(_) => {
                            continue;
                        }
                    };
                    let Some(mut path_segments) = url.path_segments() else {
                        continue;
                    };
                    let Some(last_part) = path_segments.next_back() else {
                        continue;
                    };
                    if last_part == "master.m3u8" {
                        let was_empty = video.is_none();
                        video = Some(url.to_string().into());
                        if was_empty && !subtitles.is_empty() {
                            // if we have everything, wait only another 0.2 seconds to make sure we get all the subtitles
                            elapsed_at = tokio::time::Instant::now() + Duration::from_secs_f32(0.2);
                        }
                    } else if last_part.split('.').next_back() == Some("vtt") {
                        let was_empty = subtitles.is_empty();
                        subtitles.push(url.to_string().into());
                        if was_empty && video.is_some() {
                            // if we have everything, wait only another 0.2 seconds to make sure we get all the subtitles
                            elapsed_at = tokio::time::Instant::now() + Duration::from_secs_f32(0.2);
                        }
                    }
                }

                if subtitles.is_empty() && video.is_none() {
                    anyhow::bail!("Didn't find the video or the subtitles");
                }
                Ok(VideoAndSubtitles {
                    video,
                    subtitles: subtitles.into(),
                })
            };

            let test_error = async {
                let page_html = page.content().await?.to_lowercase();
                if page_html.contains("security verification") {
                    anyhow::bail!("Page contains security validation");
                }
                if page_html.contains("not found") {
                    anyhow::bail!("The url was not found")
                }
                anyhow::Ok(())
            };

            tokio::select! {
                biased;
                r = player_future => {
                    r
                }
                Err(e) = test_error => {
                    Err(e)
                }
            }
        };
        let r = r.await;
        page.close().await?;
        r
    }
}
