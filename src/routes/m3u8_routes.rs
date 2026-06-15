use std::ops::Deref;

use anyhow::Context;
use axum::{
    Router,
    extract::{Path, State},
};
use hyper::HeaderMap;
use serde::Deserialize;

use crate::{
    AppState, UsesHttps,
    contracts::{ImdbId, MovieKey, MovieOrSeriesDataKey},
    error::WebResult,
    service::{
        fsonline_service::{MovieData, VideoServer},
        imdb_service::ImdbService,
        local_m3u8_player::{LocalPlayer, SegmentId},
    },
};

pub(super) fn routes() -> Router<AppState> {
    use axum::routing::*;
    Router::new()
        .route("/v1/api/{server_name}/{imdb}/master.m3u8", get(m3u8_master))
        .route(
            "/v1/api/{server_name}/{imdb}/m3u8/playlist.m3u8",
            get(m3u8_playlist),
        )
        .route(
            "/v1/api/{server_name}/{imdb}/m3u8/segments/{segment_number}",
            get(m3u8_segment),
        )
}

#[derive(Deserialize)]
struct ServerNameAndImdb {
    server_name: String,
    imdb: ImdbId,
}

#[derive(Deserialize)]
struct SegmentRequest {
    server_name: String,
    imdb: ImdbId,
    segment_number: usize,
}

// TODO: extract all the duplicated code
async fn m3u8_master(
    imdb_service: State<ImdbService>,
    video_server: State<VideoServer>,
    local_player: State<LocalPlayer>,
    path: Path<ServerNameAndImdb>,
    State(UsesHttps(uses_https)): State<UsesHttps>,
    State(crate::Host(host)): State<crate::Host>,
) -> WebResult<(HeaderMap, Vec<u8>)> {
    let MovieData {
        movie_name,
        release_year,
    } = imdb_service
        .get(path.imdb.imdb_id, path.imdb.is_series())
        .await?;
    let data = match path.imdb.series_data {
        Some(data) => MovieOrSeriesDataKey::Series(data),
        None => MovieOrSeriesDataKey::Movie { release_year },
    };
    let r = video_server
        .get(&MovieKey {
            movie: movie_name,
            data,
        })
        .await?;
    let player = r
        .iter()
        .find(|p| p.server_name.deref() == path.server_name)
        .context("Didn't find the server name")?;
    let m3u8_url = player
        .data
        .video
        .as_deref()
        .context("The video server was not extracted")?;
    let metadata = local_player.get_m3u8(m3u8_url).await?;

    let mut master = metadata.master.clone();
    let protocol = if uses_https { "https" } else { "http" };
    for v in master.variants.iter_mut() {
        if v.is_i_frame {
            continue;
        }
        v.uri = format!(
            "{protocol}://{host}/v1/api/{server_name}/{imdb}/m3u8/playlist.m3u8",
            server_name = path.server_name,
            imdb = path.imdb
        );
    }
    let mut result = std::io::Cursor::new(Vec::new());
    master.write_to(&mut result).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        hyper::header::CONTENT_TYPE,
        "application/vnd.apple.mpegurl".parse().unwrap(),
    );
    Ok((headers, result.into_inner()))
}

async fn m3u8_playlist(
    imdb_service: State<ImdbService>,
    video_server: State<VideoServer>,
    local_player: State<LocalPlayer>,
    path: Path<ServerNameAndImdb>,
    State(UsesHttps(uses_https)): State<UsesHttps>,
    State(crate::Host(host)): State<crate::Host>,
) -> WebResult<(HeaderMap, Vec<u8>)> {
    let MovieData {
        movie_name,
        release_year,
    } = imdb_service
        .get(path.imdb.imdb_id, path.imdb.is_series())
        .await?;
    let data = match path.imdb.series_data {
        Some(data) => MovieOrSeriesDataKey::Series(data),
        None => MovieOrSeriesDataKey::Movie { release_year },
    };
    let r = video_server
        .get(&MovieKey {
            movie: movie_name,
            data,
        })
        .await?;
    let player = r
        .iter()
        .find(|p| p.server_name.deref() == path.server_name)
        .context("Didn't find the server name")?;
    let m3u8_url = player
        .data
        .video
        .as_deref()
        .context("The video server was not extracted")?;
    let metadata = local_player.get_m3u8(m3u8_url).await?;

    let mut playlist = metadata.playlist.clone();
    let protocol = if uses_https { "https" } else { "http" };
    for (segment_number, v) in playlist.segments.iter_mut().enumerate() {
        v.uri = format!(
            "{protocol}://{host}/v1/api/{server_name}/{imdb}/m3u8/segments/{segment_number}",
            server_name = path.server_name,
            imdb = path.imdb
        );
    }
    let mut result = std::io::Cursor::new(Vec::new());
    playlist.write_to(&mut result).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        hyper::header::CONTENT_TYPE,
        "application/vnd.apple.mpegurl".parse().unwrap(),
    );
    Ok((headers, result.into_inner()))
}

async fn m3u8_segment(
    imdb_service: State<ImdbService>,
    video_server: State<VideoServer>,
    local_player: State<LocalPlayer>,
    Path(path): Path<SegmentRequest>,
) -> WebResult<(HeaderMap, axum::body::Bytes)> {
    let MovieData {
        movie_name,
        release_year,
    } = imdb_service
        .get(path.imdb.imdb_id, path.imdb.is_series())
        .await?;
    let data = match path.imdb.series_data {
        Some(data) => MovieOrSeriesDataKey::Series(data),
        None => MovieOrSeriesDataKey::Movie { release_year },
    };
    let r = video_server
        .get(&MovieKey {
            movie: movie_name,
            data,
        })
        .await?;
    let player = r
        .iter()
        .find(|p| p.server_name.deref() == path.server_name)
        .context("Didn't find the server name")?;
    let m3u8_url = player
        .data
        .video
        .as_deref()
        .context("The video server was not extracted")?;

    let id = SegmentId {
        imdb: path.imdb,
        server_name: path.server_name.into(),
        segment_index: path.segment_number,
    };
    let content = local_player.get_segment(id, m3u8_url).await?;
    let mut headers = HeaderMap::new();
    headers.insert(hyper::header::CONTENT_TYPE, "video/MP2T".parse().unwrap());
    // TODO: add support for this
    // headers.insert(hyper::header::ACCEPT_RANGES, "bytes".parse().unwrap());
    Ok((headers, content))
}
