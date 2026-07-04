use axum::{
    Router,
    extract::{Path, State},
    response::IntoResponse,
};
use axum_extra::TypedHeader;
use chrono::NaiveTime;
use futures::StreamExt;
use hyper::HeaderMap;
use itertools::Itertools;
use m3u8_rs::{MediaPlaylist, MediaSegment};
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
            "/v1/api/{server_name}/{imdb}/m3u8/segments/{segment_start}/{segment_end}",
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
    segment_start: usize,
    segment_end: usize,
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

async fn create_new_playlist(
    player: &LocalPlayer,
    protocol: &str,
    host: &str,
    key: &M3U8CacheKey,
    original_playlist: &MediaPlaylist,
) -> anyhow::Result<MediaPlaylist> {
    let segments_data = player.compute_m3u8_real_segments_duration(key).await?;
    let mut playlist = original_playlist.clone();

    playlist.segments.clear();
    for segments in segments_data {
        let uri = format!(
            "{protocol}://{host}/v1/api/{server_name}/{imdb}/m3u8/segments/{segment_start}/{segment_end}",
            server_name = key.server_name,
            imdb = key.imdb,
            segment_start = segments.segments_range.start,
            segment_end = segments.segments_range.end
        );
        let created_segment = MediaSegment {
            duration: segments.duration,
            uri,
            ..Default::default()
        };

        playlist.segments.push(created_segment);
    }
    let max_duration = playlist
        .segments
        .iter()
        .map(|s| s.duration)
        .max_by(|a, b| a.total_cmp(b))
        .unwrap_or(10.);
    playlist.target_duration = max_duration.ceil() as u64;
    Ok(playlist)
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

    // let mut playlist = metadata.playlist.clone();
    // let mut playlist = local_player.compute_m3u8_real_segments(&key).await?;
    let protocol = if uses_https { "https" } else { "http" };

    let playlist =
        create_new_playlist(&local_player, protocol, &host, &key, &metadata.playlist).await?;

    // for (segment_number, v) in playlist.segments.iter_mut().enumerate() {
    //     v.uri = format!(
    //         "{protocol}://{host}/v1/api/{server_name}/{imdb}/m3u8/segments/{segment_start}/{segment_end}",
    //         server_name = key.server_name,
    //         imdb = key.imdb,
    //         segment_start = segment_number,
    //         segment_end = segment_number + 1
    //     );
    // }
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
    // range: Option<TypedHeader<axum_extra::headers::Range>>,
) -> WebResult<DontLogResponse<(HeaderMap, RespondeMultipleBytes)>> {
    let mut segments = Vec::new();
    let m3u8 = M3U8CacheKey {
        imdb: path.imdb,
        server_name: path.server_name.into(),
    };

    for index in path.segment_start..path.segment_end {
        let id = SegmentId {
            m3u8: m3u8.clone(),
            segment_index: index,
        };
        let content = local_player.get_segment(id).await?;
        segments.push(content);
    }
    let mut headers = HeaderMap::new();
    headers.insert(hyper::header::CONTENT_TYPE, "video/MP2T".parse().unwrap());

    // let range = range.map(|TypedHeader(range)| range);

    // let content = Ranged::new(range, content);
    Ok(DontLogResponse((headers, RespondeMultipleBytes(segments))))
}

struct RespondeMultipleBytes(Vec<bytes::Bytes>);

impl IntoResponse for RespondeMultipleBytes {
    fn into_response(self) -> axum::response::Response {
        let stream = futures::stream::iter(self.0).map(std::io::Result::Ok);
        axum::response::Response::new(axum::body::Body::from_stream(stream))
    }
}
