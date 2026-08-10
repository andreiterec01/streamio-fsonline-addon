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
    id: String,
    /**
     * Url to the subtitle file.
     */
    url: String,
    /**
     * Language code for the subtitle, if a valid ISO 639-2 code is not sent, the text of this value will be used instead.
     */
    lang: Language,
    // label: String,
}

impl Subtitle {
    pub fn new(
        uses_https: bool,
        host: &str,
        fsonline_subtitle: &SubtitleFsonline,
        imdb: Imdb,
        server_name: &str,
    ) -> Self {
        let protocol = if uses_https { "https" } else { "http" };
        let id = fsonline_subtitle.md5();
        Self {
            id: format!("FSonline {server_name}"),
            url: format!(
                "{protocol}://{host}/v1/api/subtitles/{imdb}/{}/subtitle.vtt",
                id
            ),
            lang: fsonline_subtitle.lang,
            // label: format!("FSonline {server_name}"),
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
    pub season: u16,
    pub episode: u16,
}

impl SeriesData {
    pub fn as_u32(&self) -> u32 {
        ((self.season as u32) << 16) | self.episode as u32
    }

    pub fn from_u32(value: u32) -> Option<Self> {
        if value == 0 {
            return None;
        }
        Some(Self {
            episode: value as u16,
            season: (value >> 16) as u16,
        })
    }
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
    pub imdb_id: u32,
    pub series_data: Option<SeriesData>,
}

impl Imdb {
    pub fn is_series(&self) -> bool {
        self.series_data.is_some()
    }

    pub fn to_u64(&self) -> u64 {
        (self.imdb_id as u64) << 32
            | self
                .series_data
                .map(|v| v.as_u32() as u64)
                .unwrap_or_default()
    }

    pub fn from_u64(imdb_encoded: u64) -> Self {
        let series_data = SeriesData::from_u32(imdb_encoded as u32);
        Self {
            imdb_id: (imdb_encoded >> 32) as u32,
            series_data,
        }
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

bitflags::bitflags! {
    pub struct OptionsBytes: u8 {
        const LOCAL_PLAYER = 1;
        const SHOW_ORIGINAL_PLAYER = 2;
        const BROWSER_PLAYERS = 4;
        const FSONLINE_LINK = 8;
    }
}

impl Default for OptionsBytes {
    fn default() -> Self {
        Self::LOCAL_PLAYER
    }
}

impl<'de> Deserialize<'de> for OptionsBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let Some(flags) = Option::<u8>::deserialize(deserializer)? else {
            return Ok(Self::default());
        };

        let options = OptionsBytes::from_bits(flags)
            .ok_or_else(|| D::Error::custom("Invalid flags were set"))?;

        if options.is_empty() {
            return Err(D::Error::custom("The options were 0"));
        }

        Ok(options)
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
