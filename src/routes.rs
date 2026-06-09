use axum::{
    Form, Json, Router,
    extract::{Path, Query, State},
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::{ops::Deref, sync::Arc};

use crate::{
    AppState, UsesHttps,
    contracts::{ImdbSeries, MovieKey, MovieOrSeriesDataKey, PlayerData, Subtitle},
    error::WebResult,
    service::{ImdbService, MovieData, VideoServer},
};

pub fn routes() -> Router<AppState> {
    use axum::routing::*;
    Router::new()
        .route("/v1/api/season", get(get_movie_url))
        .route("/stream/series/{imdb_id}", get(series))
        .route("/stream/movie/{imdb_id}", get(series))
        .route("/subtitles/series/{imdb_id}/{filename}", get(subtitles))
        .route("/subtitles/movie/{imdb_id}/{filename}", get(subtitles))
        .route("/v1/api/subtitles/redirect", get(redirect_subtitles))
}

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
        subtitles: impl IntoIterator<Item = &'a str>,
        uses_https: bool,
        host: &str,
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
                .map(|s| Subtitle::detect_from_url(uses_https, host, s))
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
    State(movie): State<VideoServer>,
    State(imdb_service): State<ImdbService>,
    State(UsesHttps(uses_https)): State<UsesHttps>,
    State(crate::Host(host)): State<crate::Host>,
    imdb_id: Path<ImdbSeries>,
) -> WebResult<Json<SeriesResponse>> {
    let MovieData {
        movie_name,
        release_year,
    } = imdb_service
        .get(imdb_id.imdb_id, imdb_id.is_series())
        .await?;
    let data = match imdb_id.series_data {
        Some(data) => MovieOrSeriesDataKey::Series(data),
        None => MovieOrSeriesDataKey::Movie { release_year },
    };
    let r = movie
        .get(&MovieKey {
            movie: movie_name,
            data,
        })
        .await?;

    let streams = r
        .iter()
        .flat_map(|r| {
            Some(Stream::url(
                r.data.video.clone()?,
                r.server_name.clone(),
                r.data.subtitles.iter().map(|v| v.deref()),
                uses_https,
                &host,
            ))
        })
        .chain(
            r.iter()
                .map(|r| Stream::external_url(r.iframe_player.clone(), &r.server_name)),
        )
        .collect();
    Ok(Json(SeriesResponse { streams }))
}

#[derive(Serialize)]
struct SubtitlesList {
    pub subtitles: Vec<Subtitle>,
}

async fn subtitles(
    State(movie): State<VideoServer>,
    State(imdb_service): State<ImdbService>,
    State(UsesHttps(uses_https)): State<UsesHttps>,
    State(crate::Host(host)): State<crate::Host>,
    Path((imdb_id, _)): Path<(ImdbSeries, String)>,
) -> WebResult<Json<SubtitlesList>> {
    let MovieData {
        movie_name,
        release_year,
    } = imdb_service
        .get(imdb_id.imdb_id, imdb_id.is_series())
        .await?;
    let data = match imdb_id.series_data {
        Some(data) => MovieOrSeriesDataKey::Series(data),
        None => MovieOrSeriesDataKey::Movie { release_year },
    };
    let r = movie
        .get(&MovieKey {
            movie: movie_name,
            data,
        })
        .await?;

    let subtitles = r
        .iter()
        .flat_map(|r| {
            r.data
                .subtitles
                .iter()
                .flat_map(|s| Some(Subtitle::detect_from_url(uses_https, &host, &s)))
        })
        .collect();
    Ok(Json(SubtitlesList { subtitles }))
}

#[derive(Deserialize)]
struct SubtitleQuery {
    url: String,
}

// TODO: is dangerous to make a request where the user wants. I should use the id of the movie and use the cache to get the subtitle.
async fn redirect_subtitles(
    State(client): State<reqwest::Client>,
    Query(SubtitleQuery { url }): Query<SubtitleQuery>,
) -> WebResult<(
    hyper::HeaderMap,
    StreamMapper<impl futures::stream::Stream<Item = std::io::Result<axum::body::Bytes>>>,
)> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(anyhow::Error::from)?
        .error_for_status()
        .map_err(anyhow::Error::from)?
        .bytes_stream()
        .map_err(std::io::Error::other);
    let headers: hyper::HeaderMap = [(
        hyper::header::CONTENT_TYPE,
        "text/vtt; charset=utf-8".parse().unwrap(),
    )]
    .into_iter()
    .collect();
    Ok((headers, StreamMapper(response)))
}

struct StreamMapper<
    S: futures::stream::Stream<Item = std::io::Result<axum::body::Bytes>> + Send + 'static,
>(S);
impl<S> axum::response::IntoResponse for StreamMapper<S>
where
    S: futures::stream::Stream<Item = std::io::Result<axum::body::Bytes>> + Send + 'static,
{
    fn into_response(self) -> axum::response::Response {
        axum::response::Response::new(axum::body::Body::from_stream(self.0))
    }
}
