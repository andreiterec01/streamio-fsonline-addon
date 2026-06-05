use axum::{
    Form, Json, Router,
    extract::{Path, State},
};
use serde::Serialize;
use std::sync::Arc;

use crate::{
    AppState,
    contracts::{ImdbSeries, PlayerOption, SeriesKey},
    error::WebResult,
    service::{ImdbService, VideoServer},
};

pub fn routes() -> Router<AppState> {
    use axum::routing::*;
    Router::new()
        .route("/v1/api/season", get(get_movie_url))
        .route("/stream/series/{imbp_id}", get(series))
}

async fn get_movie_url(
    State(movie): State<VideoServer>,
    Form(series): Form<SeriesKey>,
) -> WebResult<Json<Arc<[PlayerOption]>>> {
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
    title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    behavior_hints: Option<BehaviourHints>,
    #[serde(skip_serializing_if = "Option::is_none")]
    external_url: Option<String>,
}

impl Stream {
    fn url(url: String, server_name: String) -> Stream {
        Stream {
            name: "FSonline",
            title: server_name,
            url: Some(url),
            behavior_hints: Some(BehaviourHints {
                not_web_ready: true,
            }),
            external_url: None,
        }
    }

    fn external_url(external_url: String, server_name: &str) -> Stream {
        Stream {
            name: "FSonline browser",
            title: format!(
                "{} Server\nThis will play in the browser\n{}",
                server_name, external_url
            ),
            url: None,
            behavior_hints: None,
            external_url: Some(external_url),
        }
    }
}

async fn series(
    State(movie): State<VideoServer>,
    State(imdb_service): State<ImdbService>,
    imdb_id: Path<ImdbSeries>,
) -> WebResult<Json<SeriesResponse>> {
    let movie_name = imdb_service.get(imdb_id.series_id).await?;
    let r = movie
        .get(&SeriesKey {
            movie: movie_name,
            season: imdb_id.season,
            episode: imdb_id.episode,
        })
        .await?;

    let streams = r
        .iter()
        .flat_map(|r| Some(Stream::url(r.url.clone()?, r.server_name.clone())))
        .chain(
            r.iter()
                .map(|r| Stream::external_url(r.data_vs.clone(), &r.server_name)),
        )
        .collect();
    Ok(Json(SeriesResponse { streams }))
}
