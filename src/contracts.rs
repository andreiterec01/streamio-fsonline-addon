use std::{borrow::Cow, fmt::Display, str::FromStr, sync::Arc};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::service::fsonline_service::{SubtitleFsonline, VideoAndSubtitles};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    #[serde(rename = "ron")]
    Romania,
    #[serde(rename = "eng")]
    English,
    #[serde(rename = "_unrecognized")]
    Unrecognized,
}

impl Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Romania => "ron",
            Self::English => "eng",
            Self::Unrecognized => "_unrecognized",
        };
        value.fmt(f)
    }
}

#[derive(Serialize)]
pub struct Subtitle {
    /**
     * Unique identifier for each subtitle, if you have more than one subtitle with the same language, the id will differentiate them.
     */
    // id: uuid::Uuid,
    /**
     * Url to the subtitle file.
     */
    url: String,
    /**
     * Language code for the subtitle, if a valid ISO 639-2 code is not sent, the text of this value will be used instead.
     */
    lang: Language,
}

impl Subtitle {
    pub fn subtitle(
        uses_https: bool,
        host: &str,
        fsonline_subtitle: &SubtitleFsonline,
        imdb: Imdb,
    ) -> Self {
        let protocol = if uses_https { "https" } else { "http" };
        Self {
            // id: uuid::Uuid::new_v4(),
            url: format!(
                "{protocol}://{host}/v1/api/subtitles/{imdb}/{}.vtt",
                fsonline_subtitle.md5()
            ),
            lang: fsonline_subtitle.lang,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlayerOption {
    pub server_name: String,
    pub iframe_player: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlayerData {
    pub server_name: Arc<str>,
    pub iframe_player: Arc<str>,
    pub data: VideoAndSubtitles,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Deserialize)]
pub struct SeriesData {
    pub season: u32,
    pub episode: u32,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
#[serde(tag = "type")]
pub enum MovieOrSeriesDataKey {
    Movie { release_year: u16 },
    Series(SeriesData),
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize)]
pub struct MovieKey {
    pub movie: Arc<str>,
    pub data: MovieOrSeriesDataKey,
}

impl Display for MovieKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.data {
            MovieOrSeriesDataKey::Movie { release_year } => {
                write!(f, "movie {}:{}", self.movie, release_year)
            }
            MovieOrSeriesDataKey::Series(SeriesData { season, episode }) => {
                write!(f, "series {}:{}:{}", self.movie, season, episode)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct Imdb {
    pub imdb_id: u64,
    pub series_data: Option<SeriesData>,
}

impl Imdb {
    pub fn is_series(&self) -> bool {
        self.series_data.is_some()
    }
}

impl Display for Imdb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tt{:07}", self.imdb_id)?;
        if let Some(data) = &self.series_data {
            write!(f, ":{}:{}", data.season, data.episode)?;
        }
        Ok(())
    }
}

impl FromStr for Imdb {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid_imdb_name = || format!("Invalid imdb name received: {s}");
        let name = s.strip_prefix("tt").with_context(invalid_imdb_name)?;
        let name = name.strip_suffix(".json").unwrap_or(name);

        let mut items = name.split(':');
        let imdb_id = items
            .next()
            .with_context(invalid_imdb_name)?
            .parse()
            .with_context(invalid_imdb_name)?;
        let series_data = match items.next() {
            Some(season) => {
                let season = season.parse().with_context(invalid_imdb_name)?;
                let episode = items
                    .next()
                    .with_context(invalid_imdb_name)?
                    .parse()
                    .with_context(invalid_imdb_name)?;
                if items.next().is_some() {
                    return Err(anyhow::anyhow!("{}", invalid_imdb_name()));
                }
                Some(SeriesData { season, episode })
            }
            None => None,
        };

        Ok(Self {
            imdb_id,
            series_data,
        })
    }
}

impl Serialize for Imdb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Imdb {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let r: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
        r.parse::<Imdb>().map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use crate::contracts::Imdb;

    #[test]
    fn test_imdb() {
        let input = "tt1234567";
        let imdb: Imdb = input.parse().unwrap();
        assert_eq!(imdb.to_string(), "tt1234567");

        let input2 = "tt0234567";
        let imdb: Imdb = input2.parse().unwrap();
        assert_eq!(imdb.to_string(), "tt0234567");
    }
}
