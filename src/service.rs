use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::Context;
use chromiumoxide::{
    Browser, cdp::browser_protocol::network::EventRequestWillBeSent, error::CdpError,
};
use scraper::{Element, Html, Selector};

use crate::contracts::{PlayerOption, SeriesKey};

#[derive(Clone)]
pub struct ImdbService {
    client: reqwest::Client,
    imbp_to_movie_name_cache: moka::future::Cache<u64, Arc<str>>,
}

impl ImdbService {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            imbp_to_movie_name_cache: moka::future::CacheBuilder::new(100_000).build(),
        }
    }

    pub async fn get(&self, series_id: u64) -> anyhow::Result<Arc<str>> {
        #[derive(serde::Deserialize)]
        struct Root {
            meta: MetaResponse,
        }
        #[derive(serde::Deserialize)]
        struct MetaResponse {
            name: String,
        }

        let r = self
            .imbp_to_movie_name_cache
            .try_get_with(series_id, async {
                let response: Root = self
                    .client
                    .get(format!(
                        "https://v3-cinemeta.strem.io/meta/series/tt{series_id}.json"
                    ))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                Ok(response.meta.name.into())
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
    cache: moka::future::Cache<SeriesKey, Arc<[PlayerOption]>>,
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
                .time_to_live(Duration::from_secs(3600))
                .build(),
        })
    }

    pub async fn get(&self, series: &SeriesKey) -> anyhow::Result<Arc<[PlayerOption]>> {
        let players = self.cache.try_get_with_by_ref(series, async {
            let SeriesKey { movie, season, episode } = series;
            let movie = normalize_movie_name(movie);
            let initial_url = format!(
                "https://www3.fsonline.app/episoade/{movie}-sezonul-{season}-episodul-{episode}/"
            );

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
            players.retain(|v| v.server_name=="Filemoon");
            let Some(mut player) = players.into_iter().next() else {
                return anyhow::Ok(Vec::new().into())
            };
            player.data_vs = self.browser.get_video(&player.data_vs).await?.to_string();
            anyhow::Ok(vec![player].into())
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
                data_vs: s.attr("data-vs")?.to_owned(),
                server_name: text.to_owned(),
            })
        })
        .collect::<Vec<_>>()
}

pub struct BrowserDiscovery {
    browser: tokio::sync::Mutex<Browser>,
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
        Ok(Self {
            browser: tokio::sync::Mutex::new(browser),
        })
    }
    pub async fn get_video(&self, url: &str) -> anyhow::Result<reqwest::Url> {
        use futures::StreamExt;
        let browser = self.browser.lock().await;
        let page = browser.new_page(url).await?;

        let future = async {
            let mut requests = page.event_listener::<EventRequestWillBeSent>().await?;

            while let Some(event) = requests.next().await {
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
                    return Ok(url);
                }
            }
            anyhow::bail!("Failed to find the page");
        };
        let r = tokio::time::timeout(Duration::from_secs(30), future)
            .await
            .context("Timeout waiting for master.m3u8");
        page.close().await?;

        r?
    }
}
