use std::time::Duration;

use anyhow::Context;
use chromiumoxide::{Browser, cdp::browser_protocol::network::EventRequestWillBeSent};

use crate::service::{
    fsonline_service::{SubtitleFsonline, VideoAndSubtitles},
    scrappers,
};

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
}

#[async_trait::async_trait]
impl scrappers::PlayerScrapper for BrowserDiscovery {
    async fn get_video(&self, url: &str) -> anyhow::Result<VideoAndSubtitles> {
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
                        if let Some(subtitle) = SubtitleFsonline::new(url.to_string().into()) {
                            subtitles.push(subtitle);
                        }
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
