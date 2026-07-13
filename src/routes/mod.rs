use anyhow::Context;
use axum::{
    Form, Json, Router,
    extract::{Path, State},
};
use axum_extra::TypedHeader;
use serde::{Deserialize, Serialize};
use std::{ops::Deref, sync::Arc};
use tower_http::services::ServeFile;

use crate::{
    AppState, UsesHttps,
    contracts::{Imdb, MovieKey, OptionsBytes, PlayerData, Subtitle},
    custom_extractor::axum_range::Ranged,
    error::WebResult,
    service::{
        ImdbToVideoServer,
        fsonline_service::{SubtitleFsonline, VideoServer},
        local_m3u8_player::{LocalPlayer, M3U8CacheKey},
    },
};
pub mod m3u8_routes;

pub fn routes() -> Router<AppState> {
    use axum::routing::*;
    Router::new()
        .route_service("/{options}/manifest.json", ServeFile::new(r"manifest.json"))
        .route("/v1/api/season", get(get_movie_url))
        .route("/{options}/stream/series/{imdb_id}", get(series))
        .route("/{options}/stream/movie/{imdb_id}", get(series))
        .route(
            "/{options}/subtitles/series/{imdb_id}/{filename}",
            get(subtitles),
        )
        .route(
            "/{options}/subtitles/movie/{imdb_id}/{filename}",
            get(subtitles),
        )
        .route(
            "/v1/api/subtitles/{imdb}/{md5}/subtitle.vtt",
            get(redirect_subtitles),
        )
        .merge(m3u8_routes::routes())
}

pub async fn install_ui() -> axum::response::Html<&'static str> {
    let html_code = include_str!("../../index.html");
    axum::response::Html(html_code)
}

#[axum::debug_handler]
async fn get_movie_url(
    State(movie): State<VideoServer>,
    Form(series): Form<MovieKey>,
) -> WebResult<Json<Arc<[PlayerData]>>> {
    let r = movie.get(&series).await?.players;
    Ok(Json(r))
}

#[derive(Serialize)]
pub struct SeriesResponse {
    streams: Vec<Stream>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BehaviourHints {
    not_web_ready: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Stream {
    name: &'static str,
    title: Arc<str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<Arc<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    behavior_hints: Option<BehaviourHints>,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_url: Option<Arc<str>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    subtitles: Vec<Subtitle>,
}

impl Stream {
    fn url<'a>(
        url: Arc<str>,
        server_name: Arc<str>,
        subtitles: impl IntoIterator<Item = &'a SubtitleFsonline>,
        uses_https: bool,
        host: &str,
        imdb: Imdb,
    ) -> Stream {
        Stream {
            name: "FSonline",
            title: server_name,
            url: Some(url),
            behavior_hints: Some(BehaviourHints {
                not_web_ready: true,
            }),
            external_url: None,
            subtitles: subtitles
                .into_iter()
                .map(|s| Subtitle::new(uses_https, host, s, imdb))
                .collect(),
        }
    }

    fn local_player_url<'a>(
        server_name: Arc<str>,
        subtitles: impl IntoIterator<Item = &'a SubtitleFsonline>,
        uses_https: bool,
        host: &str,
        imdb: Imdb,
    ) -> Stream {
        let protocol = if uses_https { "https" } else { "http" };
        Stream {
            name: "FSonline local player",
            url: Some(
                format!("{protocol}://{host}/v1/api/{server_name}/{imdb}/playlist.m3u8").into(),
            ),
            title: server_name,
            behavior_hints: Some(BehaviourHints {
                not_web_ready: true,
            }),
            external_url: None,
            subtitles: subtitles
                .into_iter()
                .map(|s| Subtitle::new(uses_https, host, s, imdb))
                .collect(),
        }
    }

    fn external_url(external_url: Arc<str>, server_name: &str) -> Stream {
        Stream {
            name: "FSonline browser player",
            title: format!("{} Server\nThis will play in the browser", server_name).into(),
            url: None,
            behavior_hints: None,
            external_url: Some(external_url),
            subtitles: Vec::new(),
        }
    }

    fn fsonline_url(external_url: Arc<str>) -> Stream {
        Stream {
            name: "FSonline browser",
            title: "The fsonline webpage".into(),
            url: None,
            behavior_hints: None,
            external_url: Some(external_url),
            subtitles: Vec::new(),
        }
    }
}

async fn series_function(
    movie: ImdbToVideoServer,
    uses_https: bool,
    host: Arc<str>,
    local_player_service: LocalPlayer,
    options: OptionsBytes,
    imdb_id: Imdb,
) -> WebResult<Json<SeriesResponse>> {
    let r = movie.get(imdb_id).await?;
    let original_players = if options.contains(OptionsBytes::SHOW_ORIGINAL_PLAYER) {
        Some(r.players.iter().flat_map(|r| {
            Some(Stream::url(
                r.data.video.clone()?,
                r.server_name.clone(),
                r.data.subtitles.iter(),
                uses_https,
                &host,
                imdb_id,
            ))
        }))
    } else {
        None
    }
    .into_iter()
    .flatten();

    let browsers = if options.contains(OptionsBytes::BROWSER_PLAYERS) {
        Some(
            r.players
                .iter()
                .map(|r| Stream::external_url(r.iframe_player.clone(), &r.server_name)),
        )
    } else {
        None
    }
    .into_iter()
    .flatten();

    let local_players = if options.contains(OptionsBytes::LOCAL_PLAYER) {
        Some(r.players.iter().flat_map(|r| {
            r.data.video.as_ref()?;
            Some(Stream::local_player_url(
                r.server_name.clone(),
                r.data.subtitles.iter(),
                uses_https,
                &host,
                imdb_id,
            ))
        }))
    } else {
        None
    }
    .into_iter()
    .flatten();
    let server_names = r.players.iter().flat_map(|player| {
        player
            .data
            .video
            .is_some()
            .then(|| player.server_name.clone())
    });
    for server_name in server_names {
        let local_player_service = local_player_service.clone();

        tokio::spawn(async move {
            // TODO: make the 60 seconds configurable
            match local_player_service
                .compute_m3u8_real_segments_duration(
                    &M3U8CacheKey {
                        imdb: imdb_id,
                        server_name: server_name.clone(),
                    },
                    false,
                )
                .await
            {
                Ok(_) => {
                    tracing::info!(
                        "Finished computing the real segments duration in cache for {}:{}",
                        imdb_id,
                        server_name
                    )
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to compute the real segments durations in cache: {e:?}"
                    );
                }
            }
        });
    }

    let original_url = if options.contains(OptionsBytes::FSONLINE_LINK) {
        Some(Stream::fsonline_url(r.fsonline_url.into()))
    } else {
        None
    };

    let streams = local_players
        .chain(original_url)
        .chain(original_players)
        .chain(browsers)
        .collect();
    Ok(Json(SeriesResponse { streams }))
}

async fn series(
    State(movie): State<ImdbToVideoServer>,
    State(UsesHttps(uses_https)): State<UsesHttps>,
    State(crate::Host(host)): State<crate::Host>,
    State(local_player_service): State<LocalPlayer>,
    Path((options, imdb_id)): Path<(OptionsBytes, Imdb)>,
) -> WebResult<Json<SeriesResponse>> {
    series_function(
        movie,
        uses_https,
        host,
        local_player_service,
        options,
        imdb_id,
    )
    .await
}

#[derive(Serialize)]
struct SubtitlesList {
    pub subtitles: Vec<Subtitle>,
}

async fn subtitles(
    State(movie): State<ImdbToVideoServer>,
    State(UsesHttps(uses_https)): State<UsesHttps>,
    State(crate::Host(host)): State<crate::Host>,
    Path((_, imdb_id, _)): Path<(OptionsBytes, Imdb, String)>,
) -> WebResult<Json<SubtitlesList>> {
    let r = movie.get(imdb_id).await?;

    let subtitles = r
        .players
        .iter()
        .flat_map(|r| {
            r.data
                .subtitles
                .iter()
                .map(|s| Subtitle::new(uses_https, &host, s, imdb_id))
        })
        .collect();
    Ok(Json(SubtitlesList { subtitles }))
}

#[derive(Deserialize)]
struct SubtitleQuery {
    imdb: Imdb,
    md5: uuid::Uuid,
}

async fn redirect_subtitles(
    State(movie): State<ImdbToVideoServer>,
    State(client): State<reqwest::Client>,
    Path(SubtitleQuery { imdb, md5 }): Path<SubtitleQuery>,
    range: Option<TypedHeader<axum_extra::headers::Range>>,
) -> WebResult<(
    hyper::HeaderMap,
    // StreamMapper<impl futures::stream::Stream<Item = std::io::Result<axum::body::Bytes>>>,
    Ranged,
)> {
    let r = movie.get(imdb).await?;

    let subtitle = r
        .players
        .iter()
        .flat_map(|player| player.data.subtitles.iter())
        .find(|s| s.md5() == md5)
        .context("Failed to find the language requested")?;

    // TODO: add to cache
    let response = client
        .get(subtitle.url.deref())
        .send()
        .await
        .map_err(anyhow::Error::from)?
        .error_for_status()
        .map_err(anyhow::Error::from)?
        .text()
        .await
        .map_err(anyhow::Error::from)?;

    let response = axum::body::Bytes::from(response);
    let response = vec![response];
    let range = range.map(|TypedHeader(range)| range);

    let headers: hyper::HeaderMap = [(
        hyper::header::CONTENT_TYPE,
        "text/vtt; charset=utf-8".parse().unwrap(),
    )]
    .into_iter()
    .collect();
    Ok((headers, Ranged::new(range, response)))
}
