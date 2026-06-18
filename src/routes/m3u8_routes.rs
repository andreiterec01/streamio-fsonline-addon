use axum::{
    Router,
    extract::{Path, State},
};
use axum_extra::TypedHeader;
use hyper::HeaderMap;
use serde::Deserialize;

use crate::{
    AppState, UsesHttps,
    contracts::Imdb,
    custom_extractor::{DontLogResponse, axum_range::Ranged},
    error::WebResult,
    service::local_m3u8_player::{LocalPlayer, M3U8CacheKey, SegmentId},
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
    imdb: Imdb,
}

#[derive(Deserialize)]
struct SegmentRequest {
    server_name: String,
    imdb: Imdb,
    segment_number: usize,
}

// TODO: extract all the duplicated code
async fn m3u8_master(
    local_player: State<LocalPlayer>,
    Path(path): Path<ServerNameAndImdb>,
    State(UsesHttps(uses_https)): State<UsesHttps>,
    State(crate::Host(host)): State<crate::Host>,
) -> WebResult<(HeaderMap, Vec<u8>)> {
    let key = M3U8CacheKey {
        imdb: path.imdb,
        server_name: path.server_name.into(),
    };
    let metadata = local_player.get_m3u8(&key).await?;

    let mut master = metadata.master.clone();
    let protocol = if uses_https { "https" } else { "http" };
    for v in master.variants.iter_mut() {
        if v.is_i_frame {
            continue;
        }
        v.uri = format!(
            "{protocol}://{host}/v1/api/{server_name}/{imdb}/m3u8/playlist.m3u8",
            server_name = key.server_name,
            imdb = key.imdb
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
    local_player: State<LocalPlayer>,
    Path(path): Path<ServerNameAndImdb>,
    State(UsesHttps(uses_https)): State<UsesHttps>,
    State(crate::Host(host)): State<crate::Host>,
) -> WebResult<(HeaderMap, Vec<u8>)> {
    let key = M3U8CacheKey {
        imdb: path.imdb,
        server_name: path.server_name.into(),
    };
    let metadata = local_player.get_m3u8(&key).await?;

    let mut playlist = metadata.playlist.clone();
    let protocol = if uses_https { "https" } else { "http" };
    for (segment_number, v) in playlist.segments.iter_mut().enumerate() {
        v.uri = format!(
            "{protocol}://{host}/v1/api/{server_name}/{imdb}/m3u8/segments/{segment_number}",
            server_name = key.server_name,
            imdb = key.imdb
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
    local_player: State<LocalPlayer>,
    Path(path): Path<SegmentRequest>,
    range: Option<TypedHeader<axum_extra::headers::Range>>,
) -> WebResult<DontLogResponse<(HeaderMap, Ranged)>> {
    let id = SegmentId {
        m3u8: M3U8CacheKey {
            imdb: path.imdb,
            server_name: path.server_name.into(),
        },
        segment_index: path.segment_number,
    };
    let content = local_player.get_segment(id).await?;
    let mut headers = HeaderMap::new();
    headers.insert(hyper::header::CONTENT_TYPE, "video/MP2T".parse().unwrap());
    // TODO: add support for this
    // headers.insert(hyper::header::ACCEPT_RANGES, "bytes".parse().unwrap());
    let range = range.map(|TypedHeader(range)| range);

    let content = Ranged::new(range, content);
    Ok(DontLogResponse((headers, content)))
}
