use anyhow::Context;
use scraper::{ElementRef, Html, Selector};

use crate::contracts::{MovieOrSeriesDataKey, PlayerOption};

fn selector(s: &str) -> Selector {
    Selector::parse(s).expect("valid CSS selector")
}

fn element_text(element: ElementRef<'_>) -> String {
    element.text().collect::<String>().trim().to_owned()
}

fn server_name(container: ElementRef<'_>, is_series: bool) -> anyhow::Result<String> {
    if is_series {
        // Series: "SERVER 1 - Filesun"
        let label = container
            .select(&selector("label.lables"))
            .next()
            .map(element_text)
            .context("Server container has no label")?;

        let (_, name) = label
            .split_once(" - ")
            .with_context(|| format!("Unexpected server label: {label:?}"))?;

        Ok(name.trim().to_owned())
    } else {
        // Movies: the name is inside the option's .servers element.
        let option = container
            .select(&selector("li.dooplay_player_option"))
            .next()
            .context("Movie server container has no player option")?;

        let name = option
            .select(&selector(".servers"))
            .next()
            .map(element_text)
            .context("Movie player option has no server name")?;

        Ok(name)
    }
}

pub fn get_urls(
    html: String,
    series_data: &MovieOrSeriesDataKey,
) -> anyhow::Result<Vec<PlayerOption>> {
    let document = Html::parse_document(&html);
    drop(html);
    let containers = selector(".servers-container .allservers");
    let options = selector("li.dooplay_player_option[data-vs]");
    let url_attr = "data-vs";

    let mut players = Vec::new();

    for container in document.select(&containers) {
        let name = match server_name(
            container,
            matches!(&series_data, MovieOrSeriesDataKey::Series(_)),
        ) {
            Ok(name) => name,
            // Skip containers that don't describe an actual server.
            Err(_) => continue,
        };

        // Movies can include a trailer in the same list of containers.
        if name.eq_ignore_ascii_case("trailer") {
            continue;
        }

        let option = if let MovieOrSeriesDataKey::Series(series) = series_data {
            // Each series server has options numbered by episode.
            container.select(&options).find(|option| {
                option
                    .select(&selector(".servers"))
                    .next()
                    .is_some_and(|number| {
                        element_text(number).parse::<u16>().ok() == Some(series.episode)
                    })
            })
        } else {
            // A movie normally has one option per server.
            container.select(&options).next()
        };

        let Some(option) = option else {
            continue;
        };

        let Some(url) = option.value().attr(url_attr) else {
            continue;
        };

        if url.trim().is_empty() {
            continue;
        }

        players.push(PlayerOption {
            server_name: name,
            iframe_player: url.to_owned(),
        });
    }

    Ok(players)
}

#[cfg(test)]
mod tests {
    use crate::contracts::{MovieOrSeriesDataKey, SeriesData};

    use super::get_urls;

    #[test]
    fn gets_movie_player_urls_in_page_order() {
        let html = include_str!("../../test_files/movie-filmehd.html").to_owned();
        let urls = get_urls(html, &MovieOrSeriesDataKey::Movie { release_year: 2024 }).unwrap();

        let actual = urls
            .iter()
            .map(|player| (player.server_name.as_str(), player.iframe_player.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            [
                (
                    "Filesun",
                    "https://cdn.filmehd.to/movies/iframe/OGJSK0tXYjlIMVJSNDRveit2NjMwWU9yYzVNNG92ZDhpSzc1aU1Kelk4a1ZpNklIUHBXT1B5TTI="
                ),
                (
                    "VOE",
                    "https://cdn.filmehd.to/movies/iframe/OGJSK0tXYjlIMVJSNDRveit2NjMwWU9yYzVNNG92ZDhpSzc1aU1Kelk4a1ZpNlFHY1ljPQ=="
                ),
                (
                    "Doodstream",
                    "https://cdn.filmehd.to/movies/iframe/OGJSK0tXYjlIMVJSNDRveit2NjMwWU9yYzVNNG92ZDhpSzc1aU1Kelk4a1ZtYVFNTjRtV05HUXE1MHI1"
                ),
            ]
        );
    }

    #[test]
    fn gets_series_player_urls_for_each_episode() {
        const FILESUN_PREFIX: &str = "https://cdn.filmehd.to/iframe/OGJSK0tXYjlIMVJSNDRwd3Z2aXcwSk8xWk5sdDhxMDdpZW14M1BGb0paNVlrYkpCZjlpTU5GNDQ3eExyN25sRVFza1gveGpDZnRxZnJud25tc0lDK3";
        const VOE_PREFIX: &str = "https://cdn.filmehd.to/iframe/OGJSK0tXYjlIMVJSNDRwd3Z2aXcwSk8xWk5sdDhxMDdpZW14M05GdUpORWIzNlVSREltSFBHNGxxRkttc25sU1FwVkhqRi9jWmZhVnVq";
        const DOODSTREAM_PREFIX: &str = "https://cdn.filmehd.to/iframe/OGJSK0tXYjlIMVJSNDRwd3Z2aXcwSk8xWk5sdDhxMDdpZW14M01OdUxwZEVpYmtHTXBmQWFpTWwrRGYzNVNFUkR0a1A4UWlPSUtlVXJFb3hoYzlUcm1OVW";
        let episode_url_suffixes = [
            ("pZTA==", "ZHV4TnM9", "VHQlA="),
            ("pVTA==", "ZHV4OXM9", "VHTlA="),
            ("pRTA==", "ZHV4dHM9", "VHSlA="),
            ("pNTA==", "ZHV3ZHM9", "VHVlA="),
            ("pJTA==", "ZHV3TnM9", "VHUlA="),
            ("pFTA==", "ZHV3OXM9", "VHZFA="),
            ("pBTA==", "ZHV3dHM9", "VHWlA="),
            ("o4TA==", "ZHV6ZHM9", "VHbFA="),
        ];
        let html = include_str!("../../test_files/series-filmehd.html").to_owned();

        for (episode, (filesun_suffix, voe_suffix, doodstream_suffix)) in
            (1..=8).zip(episode_url_suffixes)
        {
            let urls = get_urls(
                html.clone(),
                &MovieOrSeriesDataKey::Series(SeriesData { season: 2, episode }),
            )
            .unwrap();
            let actual = urls
                .iter()
                .map(|player| (player.server_name.clone(), player.iframe_player.clone()))
                .collect::<Vec<_>>();

            assert_eq!(
                actual,
                [
                    (
                        "Filesun".to_owned(),
                        format!("{FILESUN_PREFIX}{filesun_suffix}")
                    ),
                    ("VOE".to_owned(), format!("{VOE_PREFIX}{voe_suffix}")),
                    (
                        "Doodstream".to_owned(),
                        format!("{DOODSTREAM_PREFIX}{doodstream_suffix}")
                    ),
                ],
                "unexpected player URLs for episode {episode}"
            );
        }
    }
}
