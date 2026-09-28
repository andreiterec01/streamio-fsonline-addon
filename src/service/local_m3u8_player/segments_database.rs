use std::{
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    },
};

use anyhow::Context;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt, future::try_join_all};
use itertools::Itertools;
use m3u8_rs::MediaPlaylist;
use sqlx::sqlite::SqliteConnectOptions;

use crate::{
    contracts::{Imdb, Language},
    service::{
        ImdbToVideoServer, MediaPlaylistQueue, PlaylistInfoMetadata, SegmentInfo,
        local_m3u8_player::{
            M3U8CacheKey, OneSegmentTime, SegmentId, SegmentsTime,
            populate_cache::LoadCacheRequest,
            time_cache_db::{self, TimeCache, TimeCacheOptions},
        },
    },
    ts_parser::TsTimeParser,
    utils::MultipleValueMutex,
};

#[derive(Clone)]
pub struct Database {
    pool: sqlx::SqlitePool,
}

impl Database {
    pub async fn new(file: &Path) -> anyhow::Result<Self> {
        let new_file_created = !file.is_file();
        let connect_options = SqliteConnectOptions::new()
            .create_if_missing(new_file_created)
            .filename(file)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Delete)
            .optimize_on_close(true, None);

        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_with(connect_options)
            .await?;

        if new_file_created {
            sqlx::query_file!("./database/create_tables.sql")
                .execute(&pool)
                .await?;
        }
        Ok(Self { pool })
    }

    // TODO: download the subtitles and store them
    #[allow(dead_code)]
    pub async fn get_subtitle_content(
        &self,
        imdb: Imdb,
        server: &str,
        lang: Language,
    ) -> anyhow::Result<Option<String>> {
        let Some(record) = sqlx::query!(
            "SELECT text FROM subtitles WHERE imdb=? AND server=? AND language=?",
            imdb.to_u64() as i64,
            server,
            lang.to_string()
        )
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };

        Ok(Some(record.text))
    }

    // TODO: download the subtitles and store them
    #[allow(dead_code)]
    pub async fn set_subtitle_content(
        &self,
        imdb: Imdb,
        server: &str,
        lang: Language,
        content: &str,
    ) -> anyhow::Result<()> {
        sqlx::query!(
            "INSERT OR REPLACE INTO subtitles (imdb, server, language, text) VALUES (?, ?, ?, ?)",
            imdb.to_u64() as i64,
            server,
            lang.to_string(),
            content
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_playlist_metadata(
        &self,
        imdb: Imdb,
        server: &str,
    ) -> anyhow::Result<Option<PlaylistInfoMetadata>> {
        let Some(record_movie) = sqlx::query!(
            "SELECT segments_count, duration FROM movies WHERE imdb=? AND server=?",
            imdb.to_u64() as i64,
            server
        )
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };

        Ok(Some(PlaylistInfoMetadata {
            movie_duration: record_movie.duration,
            total_segments: record_movie.segments_count as usize,
        }))
    }

    pub async fn set_playlist_metadata(
        &self,
        imdb: Imdb,
        server: &str,
        playlist_info: &PlaylistInfoMetadata,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        sqlx::query!(
            "INSERT OR REPLACE INTO movies (imdb, server, segments_count, duration, last_acces) VALUES (?, ?, ?, ?, ?)",
            imdb.to_u64() as i64,
            server,
            playlist_info.total_segments as i32,
            playlist_info.movie_duration,
            now
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub fn get_segments_info(
        &self,
        imdb: Imdb,
        server: &str,
    ) -> impl Stream<Item = sqlx::Result<SegmentInfo>> {
        sqlx::query!(
            "SELECT segment, size, start_time FROM segments WHERE imdb=? AND server=? ORDER BY segment",
            imdb.to_u64() as i64,
            server
        )
        .fetch(&self.pool)
        .map_ok(|v| SegmentInfo {
            segment_index: v.segment as usize,
            size: v.size as u64,
            start_time: v.start_time,
        })
    }

    pub async fn set_segment_info(
        &self,
        imdb: Imdb,
        server: &str,
        segment_info: &SegmentInfo,
        was_accesed: bool,
    ) -> anyhow::Result<()> {
        let now = was_accesed.then(|| chrono::Utc::now().timestamp());

        sqlx::query!(
            "INSERT INTO segments (imdb, server, segment, size, start_time, last_acces) VALUES (?, ?, ?, ?, ?, ?) \
            ON CONFLICT(imdb, server, segment) DO UPDATE SET \
            size=MAX(EXCLUDED.size, size), \
            start_time=COALESCE(EXCLUDED.start_time, start_time), \
            last_acces=COALESCE(EXCLUDED.last_acces, last_acces)",
            imdb.to_u64() as i64,
            server,
            segment_info.segment_index as i32,
            segment_info.size as i64,
            segment_info.start_time,
            now
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn set_last_acces_time(
        &self,
        imdb: Imdb,
        server: &str,
        index: usize,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        sqlx::query!(
            "UPDATE segments SET last_acces=? WHERE imdb=? AND server=? AND segment=?",
            now,
            imdb.to_u64() as i64,
            server,
            index as i64
        )
        .execute(&self.pool)
        .await?;

        sqlx::query!(
            "UPDATE movies SET last_acces=? WHERE imdb=? AND server=?",
            now,
            imdb.to_u64() as i64,
            server
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_segment_local(
        &self,
        imdb: Imdb,
        server: &str,
        segment_index: usize,
    ) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE segments SET last_acces = NULL WHERE imdb=? AND server=? AND segment=?",
            imdb.to_u64() as i64,
            server,
            segment_index as i32
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_total_size(&self) -> anyhow::Result<u64> {
        let record =
            sqlx::query_scalar!("SELECT SUM(size) FROM segments WHERE last_acces IS NOT NULL;")
                .fetch_one(&self.pool)
                .await?
                .unwrap_or_default();

        Ok(record as u64)
    }

    fn get_least_accessed(
        &self,
        limit: u32,
    ) -> impl Stream<Item = anyhow::Result<LeastAccessedSegment>> {
        sqlx::query!(
            "SELECT s.imdb, s.server, s.size, s.segment, s.start_time
FROM segments s
INNER JOIN movies m ON s.imdb = m.imdb AND s.server = m.server
WHERE s.last_acces IS NOT NULL
ORDER BY
    m.last_acces ASC,
    s.last_acces ASC
LIMIT ?;",
            limit as i64
        )
        .fetch(&self.pool)
        .map_ok(|row| LeastAccessedSegment {
            imdb: Imdb::from_u64(row.imdb as u64),
            server: row.server.into(),
            segment: SegmentInfo {
                segment_index: row.segment as usize,
                size: row.size as u64,
                start_time: row.start_time,
            },
        })
        .map_err(anyhow::Error::from)
    }

    pub(crate) async fn delete_movie_segment_data(
        &self,
        imdb: Imdb,
        server: &str,
    ) -> anyhow::Result<()> {
        sqlx::query!(
            "DELETE FROM segments WHERE imdb=? AND server=?",
            imdb.to_u64() as i64,
            server
        )
        .execute(&self.pool)
        .await?;

        sqlx::query!(
            "DELETE FROM subtitles WHERE imdb=? AND server=?",
            imdb.to_u64() as i64,
            server
        )
        .execute(&self.pool)
        .await?;

        sqlx::query!(
            "DELETE FROM movies WHERE imdb=? AND server=?",
            imdb.to_u64() as i64,
            server
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

struct LeastAccessedSegment {
    imdb: Imdb,
    server: Arc<str>,
    segment: SegmentInfo,
}

struct DeleteFileOnDrop {
    path: std::path::PathBuf,
    delete_on_drop: bool,
}

impl DeleteFileOnDrop {
    async fn move_to(mut self, new_path: impl AsRef<Path>) -> std::io::Result<()> {
        let new_path = new_path.as_ref();
        let mut r = tokio::fs::rename(&self.path, new_path).await;
        if let Err(e) = &r
            && e.kind() == std::io::ErrorKind::NotFound
            && let Some(parent) = new_path.parent()
        {
            tokio::fs::create_dir_all(parent).await?;
            r = tokio::fs::rename(&self.path, new_path).await;
        }
        r?;
        self.delete_on_drop = false;
        Ok(())
    }
}

impl Drop for DeleteFileOnDrop {
    fn drop(&mut self) {
        if self.delete_on_drop {
            LocalPlayerInner::delete_file_sync(&self.path).ok();
        }
    }
}

pub struct SegmentsContent<S> {
    pub stream: S,
    pub len: u64,
}

pub struct NewLocalPlayerOptions {
    pub cache_directory: PathBuf,
    pub max_total_file_size: u64,

    pub new_media_playlist: MediaPlaylistQueue,
    pub time_cache_options: TimeCacheOptions,
}

#[derive(Clone)]
pub struct LocalPlayer {
    // TODO: make the inner field private again
    pub inner: Arc<LocalPlayerInner>,
    load_cache_sender: tokio::sync::mpsc::UnboundedSender<LoadCacheRequest>,
}

impl LocalPlayer {
    pub async fn get_segments(
        &self,
        imdb: Imdb,
        server: Arc<str>,
        segment_range: std::ops::Range<usize>,
    ) -> anyhow::Result<
        SegmentsContent<
            impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + 'static,
        >,
    > {
        let end = segment_range.end;
        self.load_cache_sender
            .send(LoadCacheRequest {
                segment_id: SegmentId {
                    m3u8: M3U8CacheKey {
                        imdb,
                        server_name: server.clone(),
                    },
                    segment_index: end,
                },
            })
            .ok();
        self.inner.get_segments(imdb, server, segment_range).await
    }

    pub async fn compute_m3u8_real_segments_duration(
        &self,
        m3u8_key: &M3U8CacheKey,
        with_timeout: bool,
    ) -> anyhow::Result<Vec<SegmentsTime>> {
        self.inner
            .compute_m3u8_real_segments_duration(m3u8_key, with_timeout)
            .await
    }
}

pub struct LocalPlayerInner {
    client: reqwest::Client,
    db: Database,
    imdb_to_video_service: ImdbToVideoServer,
    pub(super) time_cache: time_cache_db::TimeCache,

    cache_directory: Arc<std::path::Path>,

    file_path_mutexes: crate::utils::MultipleValueMutex<(M3U8CacheKey, usize)>,

    total_file_size: Arc<std::sync::atomic::AtomicU64>,
    cleanup_in_progress: Arc<std::sync::atomic::AtomicBool>,
    max_total_file_size: u64,

    pub(super) cache_in_the_future: std::time::Duration,

    new_media_playlist: MediaPlaylistQueue,
}

impl LocalPlayer {
    pub async fn new(
        imdb_to_video_service: ImdbToVideoServer,
        client: reqwest::Client,
        NewLocalPlayerOptions {
            cache_directory,
            max_total_file_size,
            time_cache_options,
            new_media_playlist,
        }: NewLocalPlayerOptions,
    ) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&cache_directory).await?;
        tokio::fs::create_dir_all(&cache_directory.join("tmp")).await?;
        let db = Database::new(&cache_directory.join("metadata.sql")).await?;
        cleanup_cache_folder(&cache_directory).await?;
        let total_file_size = dbg!(db.get_total_size().await?);

        let time_cache = TimeCache::new(db.clone(), time_cache_options).await?;

        let inner = LocalPlayerInner {
            client,
            time_cache,
            db,
            cleanup_in_progress: Arc::new(AtomicBool::new(false)),
            file_path_mutexes: MultipleValueMutex::new(),
            imdb_to_video_service,
            max_total_file_size,
            total_file_size: Arc::new(AtomicU64::new(total_file_size)),
            cache_directory: cache_directory.into(),
            cache_in_the_future: std::time::Duration::from_secs(15 * 60),
            new_media_playlist,
        };
        inner.check_and_start_cleanup_if_needed(0);
        let inner = Arc::new(inner);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // TODO: use a cancellation token to stop the task
        tokio::spawn(inner.clone().load_cache(rx));
        let this = Self {
            inner,
            load_cache_sender: tx,
        };

        Ok(this)
    }
}

impl LocalPlayerInner {
    pub(crate) fn directory_movie_file_path(
        segments_directory: &std::path::Path,
        imdb: Imdb,
        server: &str,
    ) -> std::path::PathBuf {
        let mut path = segments_directory
            .join("movies")
            .join(format!("tt{:07}", imdb.imdb_id));
        if let Some(series_data) = &imdb.series_data {
            path = path
                .join(series_data.season.to_string())
                .join(series_data.episode.to_string());
        }
        path.join(server)
    }

    fn movie_file_path(
        segments_directory: &std::path::Path,
        imdb: Imdb,
        server: &str,
        index: usize,
    ) -> std::path::PathBuf {
        Self::directory_movie_file_path(segments_directory, imdb, server).join(index.to_string())
    }

    fn tmp_file_name(&self) -> DeleteFileOnDrop {
        let uuid = uuid::Uuid::new_v4().to_string();
        DeleteFileOnDrop {
            path: self.cache_directory.join("tmp").join(uuid),
            delete_on_drop: true,
        }
    }

    pub async fn get_segments(
        &self,
        imdb: Imdb,
        server: Arc<str>,
        segment_range: std::ops::Range<usize>,
        // TODO: implement content_range
        // content_range: Option<std::ops::Range<u64>>,
    ) -> anyhow::Result<
        SegmentsContent<
            impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + 'static,
        >,
    > {
        let segment_files = segment_range.map(async |index| {
            let path = Self::movie_file_path(&self.cache_directory, imdb, &server, index);

            let file_mutex_guard = self
                .file_path_mutexes
                .lock_mutex((
                    M3U8CacheKey {
                        imdb,
                        server_name: server.clone(),
                    },
                    index,
                ))
                .await;
            // TODO: before opening the file, we should check a memory cache if it exists. And when we are done reading the file, we should add the entry in the cache
            match tokio::fs::File::open(&path).await {
                Ok(file) => {
                    // TODO: a query to the database should be faster to compute the len
                    let len = file.metadata().await?.len();
                    let stream = tokio_util::codec::FramedRead::new(
                        file,
                        tokio_util::codec::BytesCodec::new(),
                    )
                    .map_ok(bytes::BytesMut::freeze);
                    let stream = Box::pin(stream)
                        as Pin<
                            Box<
                                dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>>
                                    + Send,
                            >,
                        >;
                    let db = self.db.clone();
                    let server = server.clone();
                    // TODO: when we add a cache for the segments, we should also move this to the eviction listener, keeping the tokio::spawn
                    tokio::spawn(async move {
                        if let Err(e) = db.set_last_acces_time(imdb, &server, index).await {
                            tracing::error!("Failed to set the last acces time: {e:?}");
                        }
                    });

                    Ok((stream, len))
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    let segment_id = SegmentId {
                        m3u8: M3U8CacheKey {
                            imdb,
                            server_name: server.clone(),
                        },
                        segment_index: index,
                    };
                    let result = self.download_segment(segment_id).await?;
                    let result_clone = result.clone();
                    let tmp_file = self.tmp_file_name();
                    let db = self.db.clone();
                    let server = server.clone();

                    self.check_and_start_cleanup_if_needed(result.len() as u64);

                    let total_file_size = self.total_file_size.clone();
                    let time_cache = self.time_cache.clone();
                    let id = SegmentId {
                        m3u8: M3U8CacheKey {
                            imdb,
                            server_name: server.clone(),
                        },
                        segment_index: index,
                    };

                    tokio::spawn(async move {
                        if let Err(e) = tokio::fs::write(&tmp_file.path, &result).await {
                            tracing::error!("Failed to write temporary segment file: {e:?}");
                            return;
                        }

                        // TODO: this should be inside the insert method on time_cache.insert
                        let start_time = TsTimeParser::new(true)
                            .parse_and_return_start_time(result.clone())
                            .map(|v| v as f64);

                        if let Err(e) = db
                            .set_segment_info(
                                imdb,
                                &server,
                                &SegmentInfo {
                                    segment_index: index,
                                    size: result.len() as u64,
                                    start_time,
                                },
                                true,
                            )
                            .await
                        {
                            tracing::error!("Failed to save segment info to database: {e:?}");
                            return;
                        }

                        time_cache.insert(&id, &result).await;

                        if let Err(e) = tmp_file.move_to(path).await {
                            tracing::error!("Failed to move temporary segment file: {e:?}");
                            db.delete_segment_local(imdb, &server, index)
                                .await
                                .inspect_err(|e| {
                                    tracing::error!(
                                        "Failed to delete metadata from the cache: {e:?}"
                                    );
                                })
                                .ok();
                            return;
                        }
                        drop(file_mutex_guard);
                        total_file_size
                            .fetch_add(result.len() as u64, std::sync::atomic::Ordering::SeqCst);
                        //TODO: This sets for a second time the last acces time for the segment. We should only do this for the movie
                        if let Err(e) = db.set_last_acces_time(imdb, &server, index).await {
                            tracing::error!("Failed to set last acces time: {e:?}");
                        }
                    });
                    let len = result_clone.len() as u64;
                    let stream =
                        Box::pin(futures::stream::iter([std::io::Result::Ok(result_clone)]))
                            as Pin<
                                Box<
                                    dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>>
                                        + Send,
                                >,
                            >;
                    Ok((stream, len))
                }
                Err(e) => Err(e).context("Failed to open segment file"),
            }
        });
        let segment_files = try_join_all(segment_files).await?;
        let len = segment_files.iter().map(|(_, len)| len).sum();
        let stream =
            futures::stream::iter(segment_files.into_iter().map(|(stream, _)| stream)).flatten();
        Ok(SegmentsContent { stream, len })
    }

    // TODO: finish this function and call it in get_segments
    fn check_and_start_cleanup_if_needed(&self, new_segment_size: u64) {
        if self
            .total_file_size
            .load(std::sync::atomic::Ordering::SeqCst)
            + new_segment_size
            > self.max_total_file_size
        {
            let was_cleanup_in_progress = self
                .cleanup_in_progress
                .fetch_or(true, std::sync::atomic::Ordering::SeqCst);
            if was_cleanup_in_progress {
                return;
            }
            tracing::warn!("Starting cleanup");
            let db = self.db.clone();
            let segments_directory = self.cache_directory.clone();
            let total_file_size = self.total_file_size.clone();
            let mut least_accesed_count = 5;
            let max_total_file_size = self.max_total_file_size;
            let cleanup_future = async move {
                'enough_memory: loop {
                    let stream = db
                        .get_least_accessed(least_accesed_count)
                        .try_collect::<Vec<_>>()
                        .await?;
                    for LeastAccessedSegment {
                        imdb,
                        server,
                        segment,
                    } in stream
                    {
                        tracing::info!("Received value {imdb} {}", segment.segment_index);
                        let path = Self::movie_file_path(
                            &segments_directory,
                            imdb,
                            &server,
                            segment.segment_index,
                        );
                        let new_size = match tokio::fs::remove_file(&path).await {
                            Ok(_) => {
                                tracing::info!("removed {}", path.display());
                                let new_size = total_file_size
                                    .fetch_sub(segment.size, std::sync::atomic::Ordering::SeqCst)
                                    - segment.size;
                                db.delete_segment_local(imdb, &server, segment.segment_index)
                                    .await
                                    .inspect_err(|e| {
                                        tracing::error!(
                                            "Failed to delete the local segment: {e:?}"
                                        );
                                        least_accesed_count += 1;
                                    })
                                    .ok();
                                new_size
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                let new_size = total_file_size
                                    .fetch_sub(segment.size, std::sync::atomic::Ordering::SeqCst)
                                    - segment.size;
                                db.delete_segment_local(imdb, &server, segment.segment_index)
                                    .await
                                    .inspect_err(|e| {
                                        tracing::error!(
                                            "Failed to delete the local segment: {e:?}"
                                        );
                                        least_accesed_count += 1;
                                    })
                                    .ok();
                                new_size
                            }
                            Err(e) => {
                                tracing::error!("Failed to delete segment file {path:?}: {e:?}");
                                // Returning with one more to make sure we don't get stuck redeliting the same ones that we can't
                                least_accesed_count += 1;
                                continue;
                            }
                        };
                        if new_size <= max_total_file_size {
                            break 'enough_memory;
                        }
                    }

                    tracing::info!("Loop ended, but we didn't cleanup enough");
                }

                anyhow::Ok(())
            };
            let cleanup_in_progress = self.cleanup_in_progress.clone();

            tokio::spawn(async move {
                match cleanup_future.await {
                    Ok(()) => {
                        tracing::info!("Cleanup finished");
                    }
                    Err(e) => {
                        tracing::error!("Cleanup finished with error: {e:?}")
                    }
                }
                cleanup_in_progress.store(false, std::sync::atomic::Ordering::SeqCst);
            });
        }
    }

    async fn new_metadata_received(
        new_media_playlist: &MediaPlaylistQueue,
        db: &Database,
        time_cache: &TimeCache,
    ) -> anyhow::Result<()> {
        let mut futures = Vec::new();
        while let Some((m3u8_key, playlist)) = new_media_playlist.pop() {
            futures.push(async move {
                let guard = scopeguard::guard((), |()| {
                    new_media_playlist.push((m3u8_key.clone(), playlist.clone()));
                });
                let old_metadata = db
                    .get_playlist_metadata(m3u8_key.imdb, &m3u8_key.server_name)
                    .await?;
                let new_metadata = PlaylistInfoMetadata::from_playlist(&playlist);
                if let Some(old_metadata) = old_metadata
                    && (old_metadata.total_segments != new_metadata.total_segments
                        || (old_metadata.movie_duration - new_metadata.movie_duration).abs() > 0.1)
                {
                    tracing::warn!("Deleting the full movie for {m3u8_key:?}. Metadata mismatch");
                    time_cache
                        .delete_full_movie(&m3u8_key, new_metadata)
                        .await?;
                } else {
                    // TODO: we can also check at the beginning if we have all the segments. If we do, we can skip the request to fsonline
                    db.set_playlist_metadata(m3u8_key.imdb, &m3u8_key.server_name, &new_metadata)
                        .await?;
                }
                scopeguard::ScopeGuard::into_inner(guard);
                anyhow::Ok(())
            });
        }
        try_join_all(futures).await?;
        Ok(())
    }

    async fn get_m3u8_inner(
        imdb_to_video_service: &ImdbToVideoServer,
        m3u8_key: &M3U8CacheKey,
        db: &Database,
        time_cache: &TimeCache,
        new_media_playlist: &MediaPlaylistQueue,
    ) -> anyhow::Result<Arc<MediaPlaylist>> {
        let playlist = imdb_to_video_service
            .get_from_server(m3u8_key.imdb, &m3u8_key.server_name)
            .await?
            .context("Player not found")?
            .data
            .video
            .context("Video url not scrapped")?;
        Self::new_metadata_received(new_media_playlist, db, time_cache).await?;
        Ok(playlist)
    }

    pub async fn compute_m3u8_real_segments_duration(
        &self,
        m3u8_key: &M3U8CacheKey,
        with_timeout: bool,
    ) -> anyhow::Result<Vec<SegmentsTime>> {
        let playlist = self.get_m3u8(m3u8_key).await?;

        let movie_duration: f32 = playlist.segments.iter().map(|s| s.duration).sum();
        let segments_len = playlist.segments.len();
        // TODO: add the cache back
        _ = self
            .get_segments(m3u8_key.imdb, m3u8_key.server_name.clone(), 0..1)
            .await?;

        // waiting to be added the 0 element to the cache
        self.file_path_mutexes
            .lock_mutex((m3u8_key.clone(), 0))
            .await;

        let one_segment_times = self
            .time_cache
            .get_or_fetch(m3u8_key, &playlist, with_timeout)
            .await?;

        let mut segments = Vec::new();
        let one_segment_times_first_element = if !one_segment_times
            .first()
            .is_some_and(|s| s.segment_index == 0)
        {
            tracing::error!(
                "There was an error and we didn't received the first segment time. Assuming to be 0"
            );
            Some(OneSegmentTime {
                segment_index: 0,
                start_time: 0.,
            })
        } else {
            None
        };

        let one_segment_times_iter = one_segment_times_first_element
            .into_iter()
            .chain(one_segment_times.iter().copied());
        for (prev, current) in one_segment_times_iter.tuple_windows() {
            let segment = SegmentsTime {
                duration: current.start_time - prev.start_time,
                segments_range: prev.segment_index..current.segment_index,
            };
            segments.push(segment);
        }
        let last_segment = one_segment_times.last().unwrap_or(&OneSegmentTime {
            segment_index: 0,
            start_time: 0.,
        });
        segments.push(SegmentsTime {
            segments_range: last_segment.segment_index..segments_len,
            duration: movie_duration - last_segment.start_time,
        });
        Ok(segments)
    }

    // TODO: find a way to fix the cache duration
    pub async fn get_m3u8(&self, m3u8_key: &M3U8CacheKey) -> anyhow::Result<Arc<MediaPlaylist>> {
        Self::get_m3u8_inner(
            &self.imdb_to_video_service,
            m3u8_key,
            &self.db,
            &self.time_cache,
            &self.new_media_playlist,
        )
        .await
    }

    async fn download_segment(&self, segment_id: SegmentId) -> anyhow::Result<Bytes> {
        let metadata = self.get_m3u8(&segment_id.m3u8).await?;

        let segment = metadata
            .segments
            .get(segment_id.segment_index)
            .context("Invalid segment index")?;

        let segment_data = self
            .client
            .get(&segment.uri)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;

        Ok(segment_data)
    }

    #[cfg(windows)]
    pub(crate) async fn delete_file(path: impl AsRef<Path>) -> anyhow::Result<()> {
        const FILE_FLAG_DELETE_ON_CLOSE: u32 = 0x04000000;
        tokio::fs::File::options()
            .read(true)
            .custom_flags(FILE_FLAG_DELETE_ON_CLOSE)
            .open(path)
            .await?;
        Ok(())
    }

    #[cfg(unix)]
    pub(crate) async fn delete_file(path: impl AsRef<Path>) -> anyhow::Result<()> {
        tokio::fs::remove_file(path).await?;
        Ok(())
    }

    #[cfg(windows)]
    fn delete_file_sync(path: impl AsRef<Path>) -> anyhow::Result<()> {
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_FLAG_DELETE_ON_CLOSE: u32 = 0x04000000;
        std::fs::File::options()
            .custom_flags(FILE_FLAG_DELETE_ON_CLOSE)
            .open(path)?;
        Ok(())
    }

    #[cfg(unix)]
    async fn delete_file_sync(path: impl AsRef<Path>) -> anyhow::Result<()> {
        std::fs::remove_file(path)?;
        Ok(())
    }
}

async fn cleanup_cache_folder(cache_directory: &Path) -> anyhow::Result<()> {
    let mut tmp_files = tokio::fs::read_dir(cache_directory.join("tmp")).await?;
    while let Some(file) = tmp_files.next_entry().await? {
        let file_type = file.file_type().await?;
        if file_type.is_file() {
            let path = file.path();
            tokio::spawn(async move {
                tokio::fs::remove_file(path).await.ok();
            });
        } else {
            tracing::error!(
                "got a non file in the tmp folder: {}",
                file.path().display()
            );
        }
    }
    Ok(())
}
