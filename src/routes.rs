use std::sync::Arc;

use axum::{
    Form, Json, Router,
    extract::{Path, Query, State},
};

use crate::{
    AppState,
    contracts::{ImdbSeries, PlayerOption, SeriesKey},
    error::{WebResult},
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

#[derive(serde::Serialize)]
pub struct SeriesResponse {
    streams: Vec<Stream>,
}

#[derive(serde::Serialize)]
pub struct Stream {
    title: String,
    url: String,
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
        .map(|r| Stream {
            title: r.server_name.to_owned(),
            url: r.data_vs.clone(),
        })
        .collect();
    Ok(Json(SeriesResponse { streams }))
}
