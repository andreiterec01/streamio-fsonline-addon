use std::{sync::Arc, time::Duration};

use anyhow::Context;
use base64::Engine;
use regex::Regex;
use scraper::Selector;

use crate::service::{
    fsonline_service::VideoAndSubtitlesScrapper,
    scrappers::{PlayerScrapper, SpecificScrapper},
};

pub struct Voe {
    client: reqwest::Client,
    parser: VOEParser,
}

impl Voe {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            parser: VOEParser::new(),
        }
    }
}

impl SpecificScrapper for Voe {
    fn server_name(&self) -> &'static str {
        "VOE"
    }
}

#[async_trait::async_trait]
impl PlayerScrapper for Voe {
    async fn get_video(&self, url: &str) -> anyhow::Result<VideoAndSubtitlesScrapper> {
        let redirect_html_page = self
            .client
            .get(url)
            .timeout(Duration::from_secs(3))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let redirect_url = self
            .parser
            .parse_redirect_page(redirect_html_page)
            .context("No redirect url found")?;
        let html_content = self
            .client
            .get(redirect_url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        Ok(self.parser.parse_voe_html(html_content))
    }
}

fn redirect_regex() -> Regex {
    Regex::new(r#"window\s*\.\s*location\s*\.\s*href\s*=\s*(?:'([^']*)'|"([^"]*)")"#).unwrap()
}

fn script_selector() -> Selector {
    Selector::parse("script").unwrap()
}

fn script_application_json_selector() -> Selector {
    Selector::parse(r#"script[type="application/json""#).unwrap()
}

struct VOEParser {
    redirect_regex: Regex,
    script_selector: Selector,
    script_application_json_selector: Selector,
}

impl VOEParser {
    fn new() -> Self {
        Self {
            redirect_regex: redirect_regex(),
            script_selector: script_selector(),
            script_application_json_selector: script_application_json_selector(),
        }
    }

    fn parse_redirect_page(&self, html: String) -> Option<reqwest::Url> {
        let document = scraper::html::Html::parse_document(&html);
        drop(html);
        for script in document.select(&self.script_selector) {
            let script = script.text().collect::<String>();

            if let Some(captures) = self.redirect_regex.captures(&script) {
                return captures
                    .get(1)
                    .or_else(|| captures.get(2))
                    .and_then(|m| m.as_str().parse().ok());
            }
        }

        None
    }

    fn parse_voe_html(&self, html_content: String) -> VideoAndSubtitlesScrapper {
        let html = scraper::html::Html::parse_document(&html_content);
        drop(html_content);
        let mut m3u8_url = None;
        for script in html.select(&self.script_application_json_selector) {
            let Some(js_code) = script.text().next() else {
                continue;
            };
            match decode_scrypt(js_code) {
                Ok(m3u8) => {
                    m3u8_url = Some(m3u8);
                    break;
                }
                Err(e) => {
                    tracing::warn!("Failed to decode scrypt content: {e}");
                }
            }
        }
        VideoAndSubtitlesScrapper {
            m3u8_url,
            // TODO: when I find a VOE player with subtitles, I should scrape them too
            subtitles: Arc::default(),
        }
    }
}

fn decode_scrypt(scrypt_content: &str) -> anyhow::Result<String> {
    let response: Vec<String> = serde_json::from_str(scrypt_content)?;

    let encoded = response
        .first()
        .context("Embedded payload array is empty")?;

    let config = decode_player_config(encoded)?;

    let m3u8 = config
        .get("source")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .context("Decoded configuration has no source URL")?;
    Ok(m3u8)
}

/// Equivalent to the JavaScript ROT13 function.
fn rot13(input: &str) -> String {
    input
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' => (b - b'A' + 13) % 26 + b'A',
            b'a'..=b'z' => (b - b'a' + 13) % 26 + b'a',
            _ => b,
        })
        .map(char::from)
        .collect()
}

/// Decodes the obfuscated string into the player configuration JSON.
///
/// Transformation sequence:
/// 1. ROT13
/// 2. Replace obfuscation markers with underscores
/// 3. Remove all underscores
/// 4. Base64 decode
/// 5. Subtract 3 from every decoded byte
/// 6. Reverse the bytes
/// 7. Base64 decode again
/// 8. Parse JSON
fn decode_player_config(encoded: &str) -> anyhow::Result<serde_json::Value> {
    let mut transformed = rot13(encoded);

    for marker in ["@$", "^^", "~@", "%?", "*~", "!!", "#&"] {
        transformed = transformed.replace(marker, "_");
    }

    transformed.retain(|c| c != '_');

    // First Base64 decoding.
    let decoded = base64::prelude::BASE64_STANDARD
        .decode(transformed.as_bytes())
        .context("First Base64 decoding failed")?;

    // Equivalent to:
    // String.fromCharCode(charCodeAt(i) - 3)
    // followed by split("").reverse().join("").
    //
    // The payload bytes in the supplied HAR are all >= 3.
    let mut shifted: Vec<u8> = decoded
        .into_iter()
        .map(|b| {
            b.checked_sub(3)
                .ok_or_else(|| anyhow::anyhow!("Invalid byte during subtraction"))
        })
        .collect::<anyhow::Result<_>>()?;

    shifted.reverse();

    // Second Base64 decoding.
    let json_bytes = base64::prelude::BASE64_STANDARD
        .decode(&shifted)
        .context("Second Base64 decoding failed")?;

    serde_json::from_slice(&json_bytes).context("Decoded payload is not valid JSON")
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_voe_redirect_url() {
        let redirect_html = include_str!("../../../test_files/voe_redirect.html").to_owned();
        let voe_parser = VOEParser::new();

        let result = voe_parser.parse_redirect_page(redirect_html).unwrap();
        assert_eq!(
            result.as_str(),
            "https://jamespatterncamera.com/e/j5gam7dwcqcq"
        );
    }

    #[test]
    fn test_parse_voe_html() {
        let voe_html = include_str!("../../../test_files/voe_html.html").to_owned();
        let voe_parser = VOEParser::new();

        let VideoAndSubtitlesScrapper {
            m3u8_url,
            subtitles,
        } = voe_parser.parse_voe_html(voe_html);

        assert_eq!(
            m3u8_url.as_deref(),
            Some(
                "https://ugc-cdn-caching-n3mqgdbmysvmw0rswd.cloudwindow-route.com/engine/hls2-c/01/17599/j5gam7dwcqcq_,n,.urlset/master.m3u8?t=5-60NQf7XstHojXxSV-MS-TSygFyDBRH_EXUU4T_GwQ&s=1791982222&e=14400&f=87998559&node=DJsMI4tk52m11WEDwotzcYxtCucLGBlIC+QYYC7aSG8=&i=188.24&sp=2500&asn=8708&q=n&rq=Esk3mxBwA4J7eHZbNdqsW4HK8YyOZgNUzz5xbMj6"
            )
        );
        assert!(subtitles.is_empty());
    }
}
