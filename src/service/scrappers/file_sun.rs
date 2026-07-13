use scraper::Selector;

use crate::service::{
    fsonline_service::{SubtitleFsonline, VideoAndSubtitles},
    scrappers::{PlayerScrapper, SpecificScrapper},
};

pub struct FileSuN {
    client: reqwest::Client,
    parser: FileSuNParser,
}

impl FileSuN {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            parser: FileSuNParser::new(),
        }
    }
}

impl SpecificScrapper for FileSuN {
    fn server_name(&self) -> &'static str {
        "FileSuN"
    }
}

#[async_trait::async_trait]
impl PlayerScrapper for FileSuN {
    async fn get_video(&self, url: &str) -> anyhow::Result<VideoAndSubtitles> {
        let html_string = self
            .client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        Ok(self.parser.parse_file_sun_html(html_string))
    }
}

fn regex_m3u8() -> regex::Regex {
    regex::Regex::new(r#"['"](https://.*master\.m3u8[^'"]*)['"]"#).unwrap()
}

fn regex_vtt() -> regex::Regex {
    regex::Regex::new(r#"['"](https://.*\.vtt)['"]"#).unwrap()
}

struct FileSuNParser {
    m3u8_regex: regex::Regex,
    vtt_regex: regex::Regex,
}

impl FileSuNParser {
    fn new() -> Self {
        Self {
            m3u8_regex: regex_m3u8(),
            vtt_regex: regex_vtt(),
        }
    }

    fn parse_file_sun_html(&self, html_string: String) -> VideoAndSubtitles {
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
        let file_sun = include_str!("../../../test_files/filesun_html.html").to_owned();
        let r = FileSuNParser::new().parse_file_sun_html(file_sun);
        assert_eq!(
            r.video.as_deref(),
            Some(
                "https://prx-1559-ant.vmwesa.online/hls2/02/02589/k0kfg2ai8kny_,n,l,.urlset/master.m3u8?t=gcCCgLPFEQ2_tXrBUryjbVcJTvZw9tPUytg3SkYsZc4=&s=1783968858&e=43200&v=&srv=bck-1564-ant-p&i=0.4&sp=0&asn=8708"
            )
        );
        let subtitles = r
            .subtitles
            .iter()
            .map(|s| s.url.deref())
            .collect::<BTreeSet<_>>();

        let real_subtitles = [
            "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_English.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Turkish.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Spanish.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Russian.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Arabic.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_French.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_German.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Polish.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Greek.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Italian.vtt",
            "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Romanian.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Danish.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Finnish.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Swedish.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Norwegian.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_French.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Indonesian.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Hungarian.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Portuguese.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Bulgarian.vtt",
            // "https://srt.vidmoly.me/srt/02589/hvpihozhm2ny_Czech.vtt",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        assert_eq!(subtitles, real_subtitles);
    }
}
