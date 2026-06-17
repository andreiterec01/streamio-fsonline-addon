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
    contracts::{ImdbId, MovieKey, PlayerData, Subtitle},
    custom_extractor::axum_range::Ranged,
    error::WebResult,
    service::{
        ImdbToVideoServer,
        fsonline_service::{SubtitleFsonline, VideoServer},
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
        .route("/v1/api/subtitles/{imdb}/{md5}", get(redirect_subtitles))
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
        imdb: ImdbId,
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
        imdb: ImdbId,
    ) -> Stream {
        let protocol = if uses_https { "https" } else { "http" };
        Stream {
            name: "FSonline local player",
            url: Some(
                format!("{protocol}://{host}/v1/api/{server_name}/{imdb}/master.m3u8").into(),
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
    Path(imdb_id): Path<ImdbId>,
) -> WebResult<Json<SeriesResponse>> {
    let r = movie.get(imdb_id).await?;

    let streams = r
        .iter()
        .flat_map(|r| {
            Some(Stream::url(
                r.data.video.clone()?,
                r.server_name.clone(),
                r.data.subtitles.iter(),
                uses_https,
                &host,
                imdb_id,
            ))
        })
        .chain(
            r.iter()
                .map(|r| Stream::external_url(r.iframe_player.clone(), &r.server_name)),
        )
        .chain(r.iter().flat_map(|r| {
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
        }))
        .collect();
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
    Path((imdb_id, _)): Path<(ImdbId, String)>,
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
    imdb: ImdbId,
    md5: String,
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
    let md5 = md5
        .strip_suffix(".vtt")
        .context("Suffix .vtt was missing")?;

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
    let range = range.map(|TypedHeader(range)| range);

    let headers: hyper::HeaderMap = [(
        hyper::header::CONTENT_TYPE,
        "text/vtt; charset=utf-8".parse().unwrap(),
    )]
    .into_iter()
    .collect();
    Ok((headers, Ranged::new(range, response)))
}
