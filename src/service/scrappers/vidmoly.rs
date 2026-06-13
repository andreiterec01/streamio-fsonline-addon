use scraper::Selector;

use crate::service::{
    fsonline_service::{SubtitleFsonline, VideoAndSubtitles},
    scrappers::{PlayerScrapper, SpecificScrapper},
};

pub struct VidmolyScrapper {
    client: reqwest::Client,
    parser: VidmolyParser,
}

impl VidmolyScrapper {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            parser: VidmolyParser::new(),
        }
    }
}

impl SpecificScrapper for VidmolyScrapper {
    fn server_name(&self) -> &'static str {
        "Vidmoly"
    }
}

#[async_trait::async_trait]
impl PlayerScrapper for VidmolyScrapper {
    async fn get_video(&self, url: &str) -> anyhow::Result<VideoAndSubtitles> {
        let html_string = self
            .client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        Ok(self.parser.parse_vidmoly_html(html_string))
    }
}

fn regex_m3u8() -> regex::Regex {
    regex::Regex::new(r#"['"](https://.*master\.m3u8[^'"]*)['"]"#).unwrap()
}

fn regex_vtt() -> regex::Regex {
    regex::Regex::new(r#"['"](https://.*\.vtt)['"]"#).unwrap()
}

struct VidmolyParser {
    m3u8_regex: regex::Regex,
    vtt_regex: regex::Regex,
}

impl VidmolyParser {
    fn new() -> Self {
        Self {
            m3u8_regex: regex_m3u8(),
            vtt_regex: regex_vtt(),
        }
    }

    fn parse_vidmoly_html(&self, html_string: String) -> VideoAndSubtitles {
        let html = { scraper::html::Html::parse_document(&html_string) };
        drop(html_string);
        let selector = Selector::parse("script").unwrap();

        let mut m3u8_url = None;
        let mut subtitles_result = Vec::new();
        for script in html.select(&selector) {
            let Some(js_code) = script.text().next() else {
                continue;
            };

            if m3u8_url.is_none()
                && let Some(m3u8) = self.m3u8_regex.captures(js_code)
            {
                let url = m3u8.get(1).unwrap();
                m3u8_url = Some(url.as_str().to_owned());
            }
            let subtitles = self
                .vtt_regex
                .captures_iter(js_code)
                .map(|subtitle| subtitle.get(1).unwrap().as_str().to_owned().into())
                .flat_map(SubtitleFsonline::new);
            subtitles_result.extend(subtitles);
        }
        VideoAndSubtitles {
            video: m3u8_url.map(|url| url.into()),
            subtitles: subtitles_result.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, ops::Deref};

    use super::*;
    #[test]
    fn test_parse_vidmoly_html() {
        let vidmoly = include_str!("../../../test_files/vidmoly_html.html").to_owned();
        let r = VidmolyParser::new().parse_vidmoly_html(vidmoly);
        assert_eq!(
            r.video.as_deref(),
            Some(
                "https://prx-1546-ant.vmwesa.online/hls2/01/02469/u1lemqhj7hqi_n/master.m3u8?t=rQp1o1rXAXxH0Hosd-PtcoR0JzjCcqTK5KNz2YX6T3c=&s=1781334063&e=43200&v=&srv=transit-1478-v1&i=0.4&sp=0&asn=8708"
            )
        );
        let subtitles = r
            .subtitles
            .iter()
            .map(|s| s.url.deref())
            .collect::<BTreeSet<_>>();

        let real_subtitles = [
            "https://srt.vidmoly.me/srt/02469/u1lemqhj7hqi_English.vtt",
            "https://srt.vidmoly.me/srt/02469/u1lemqhj7hqi_Romanian.vtt",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        assert_eq!(subtitles, real_subtitles);
    }
}
