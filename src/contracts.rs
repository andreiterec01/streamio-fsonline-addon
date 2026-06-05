use std::{fmt::Display, str::FromStr, sync::Arc};

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlayerOption {
    pub server_name: String,
    pub data_vs: String,
    pub url: Option<String>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
pub struct SeriesKey {
    pub movie: Arc<str>,
    pub season: u32,
    pub episode: u32,
}

pub struct ImdbSeries {
    pub series_id: u64,
    pub season: u32,
    pub episode: u32,
}

impl Display for ImdbSeries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tt{}:{}:{}", self.series_id, self.season, self.episode)
    }
}

impl FromStr for ImdbSeries {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid_imdb_name = || format!("Invalid imdb name received: {s}");
        let name = s.strip_prefix("tt").with_context(invalid_imdb_name)?;
        let name = name.strip_suffix(".json").unwrap_or(name);

        let mut items = name.split(':');
        let series_id = items
            .next()
            .with_context(invalid_imdb_name)?
            .parse()
            .with_context(invalid_imdb_name)?;
        let season = items
            .next()
            .with_context(invalid_imdb_name)?
            .parse()
            .with_context(invalid_imdb_name)?;
        let episode = items
            .next()
            .with_context(invalid_imdb_name)?
            .parse()
            .with_context(invalid_imdb_name)?;
        if items.next().is_some() {
            return Err(anyhow::anyhow!("{}", invalid_imdb_name()));
        }
        Ok(Self {
            series_id,
            season,
            episode,
        })
    }
}

impl Serialize for ImdbSeries {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ImdbSeries {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let r = <&str as Deserialize<'de>>::deserialize(deserializer)?;
        r.parse::<ImdbSeries>().map_err(D::Error::custom)
    }
}
