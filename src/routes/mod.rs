use anyhow::Context;
use axum::{
    Form, Json, Router,
    extract::{Path, State},
};
use axum_extra::TypedHeader;
use serde::{Deserialize, Serialize};
use std::{ops::Deref, sync::Arc};

use crate::{
    AppState, UsesHttps,
    contracts::{Imdb, MovieKey, PlayerData, Subtitle},
    custom_extractor::axum_range::Ranged,
    error::WebResult,
    service::{
        ImdbToVideoServer,
        fsonline_service::{SubtitleFsonline, VideoServer},
        local_m3u8_player::{LocalPlayer, M3U8CacheKey},
    },
};
mod m3u8_routes;

pub fn routes() -> Router<AppState> {
    use axum::routing::*;
    Router::new()
        .route("/v1/api/season", get(get_movie_url))
        .route("/stream/series/{imdb_id}", get(series))
        .route("/stream/movie/{imdb_id}", get(series))
        .route("/subtitles/series/{imdb_id}/{filename}", get(subtitles))
        .route("/subtitles/movie/{imdb_id}/{filename}", get(subtitles))
        .route(
            "/v1/api/subtitles/{imdb}/{md5}/subtitle.vtt",
            get(redirect_subtitles),
        )
        .merge(m3u8_routes::routes())
}

#[axum::debug_handler]
async fn get_movie_url(
    State(movie): State<VideoServer>,
    Form(series): Form<MovieKey>,
) -> WebResult<Json<Arc<[PlayerData]>>> {
    let r = movie.get(&series).await?;
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
                .map(|s| Subtitle::subtitle(uses_https, host, s, imdb))
                .into_iter()
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
                .map(|s| Subtitle::subtitle(uses_https, host, s, imdb))
                .into_iter()
                .collect(),
        }
    }

    fn external_url(external_url: Arc<str>, server_name: &str) -> Stream {
        Stream {
            name: "FSonline browser",
            title: format!(
                "{} Server\nThis will play in the browser\n{}",
                server_name, external_url
            )
            .into(),
            url: None,
            behavior_hints: None,
            external_url: Some(external_url),
            subtitles: Vec::new(),
        }
    }
}

async fn series(
    State(movie): State<ImdbToVideoServer>,
    State(UsesHttps(uses_https)): State<UsesHttps>,
    State(crate::Host(host)): State<crate::Host>,
    State(local_player_service): State<LocalPlayer>,
    Path(imdb_id): Path<Imdb>,
) -> WebResult<Json<SeriesResponse>> {
    let r = movie.get(imdb_id).await?;

    let original_urls = r.iter().flat_map(|r| {
        Some(Stream::url(
            r.data.video.clone()?,
            r.server_name.clone(),
            r.data.subtitles.iter(),
            uses_https,
            &host,
            imdb_id,
        ))
    });

    let browsers = r
        .iter()
        .map(|r| Stream::external_url(r.iframe_player.clone(), &r.server_name));

    let local_players = r.iter().flat_map(|r| {
        if r.data.video.is_none() {
            return None;
        }
        Some(Stream::local_player_url(
            r.server_name.clone(),
            r.data.subtitles.iter(),
            uses_https,
            &host,
            imdb_id,
        ))
    });
    let server_names = r.iter().flat_map(|player| {
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
    let streams = local_players.chain(original_urls).chain(browsers).collect();
    Ok(Json(SeriesResponse { streams }))
}

#[derive(Serialize)]
struct SubtitlesList {
    pub subtitles: Vec<Subtitle>,
}

async fn subtitles(
    State(movie): State<ImdbToVideoServer>,
    State(UsesHttps(uses_https)): State<UsesHttps>,
    State(crate::Host(host)): State<crate::Host>,
    Path((imdb_id, _)): Path<(Imdb, String)>,
) -> WebResult<Json<SubtitlesList>> {
    let r = movie.get(imdb_id).await?;

    let subtitles = r
        .iter()
        .flat_map(|r| {
            r.data
                .subtitles
                .iter()
                .map(|s| Subtitle::subtitle(uses_https, &host, s, imdb_id))
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
