use std::sync::Arc;

use anyhow::Context;
use chrono::{DateTime, Datelike, Utc};

use crate::service::fsonline_service::MovieData;

#[derive(Clone)]
pub struct ImdbService {
    client: reqwest::Client,
    imbp_to_movie_name_cache: moka::future::Cache<u32, MovieData>,
}

impl ImdbService {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            imbp_to_movie_name_cache: moka::future::CacheBuilder::new(100_000).build(),
        }
    }

    pub async fn get(&self, imdb_id: u32, is_series: bool) -> anyhow::Result<MovieData> {
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
                    .get(format!(
                        "https://v3-cinemeta.strem.io/meta/{path}/tt{imdb_id:07}.json"
                    ))
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
