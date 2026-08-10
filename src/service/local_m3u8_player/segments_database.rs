use std::{
    ops::Deref,
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
use m3u8_rs::MediaPlaylist;
use sqlx::{
    migrate::{Migrate, MigrateDatabase},
    sqlite::SqliteConnectOptions,
};

use crate::{
    contracts::{Imdb, Language},
    service::{
        ImdbToVideoServer, PlaylistInfo, PlaylistInfoMetadata, SegmentInfo,
        local_m3u8_player::{M3U8CacheKey, M3U8Data, SegmentId, SegmentsTime},
    },
    ts_parser::TsStartTimeParser,
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
        sqlx::query!(
            "INSERT OR REPLACE INTO movies (imdb, server, segments_count, duration) VALUES (?, ?, ?, ?)",
            imdb.to_u64() as i64,
            server,
            playlist_info.total_segments as i32,
            playlist_info.movie_duration
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
        let segments = sqlx::query!(
            "SELECT segment, size, start_time FROM segments WHERE imdb=? AND server=?",
            imdb.to_u64() as i64,
            server
        )
        .fetch(&self.pool)
        .map_ok(|v| SegmentInfo {
            segment_index: v.segment as usize,
            size: v.size as u64,
            start_time: v.start_time,
        });
        segments
    }

    pub async fn set_segment_info(
        &self,
        imdb: Imdb,
        server: &str,
        segment_info: &SegmentInfo,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().timestamp();
        sqlx::query!(
            "INSERT OR REPLACE INTO segments (imdb, server, segment, size, start_time, last_acces) VALUES (?, ?, ?, ?, ?, ?)",
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
        let records = sqlx::query!(
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
                segment_index: 0, // Placeholder, adjust as needed
                size: row.size as u64,
                start_time: None, // Placeholder, adjust as needed
            },
        })
        .map_err(anyhow::Error::from);

        records
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
        tokio::fs::rename(&self.path, new_path).await?;
        self.delete_on_drop = false;
        Ok(())
    }
}

impl Drop for DeleteFileOnDrop {
    fn drop(&mut self) {
        if self.delete_on_drop {
            std::fs::remove_file(&self.path).ok();
        }
    }
}

pub struct SegmentsContent<S> {
    pub stream: S,
}

pub struct NewLocalPlayerOptions {
    cache_directory: PathBuf,
    max_total_file_size: u64,
    metadata_memory_cache_size: u64,
}

struct CounterWritter {
    size: usize,
}

impl CounterWritter {
    fn new() -> Self {
        Self { size: 0 }
    }
}

impl std::io::Write for CounterWritter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.size += buf.len();
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn weigher(key: &M3U8CacheKey, value: &Arc<MediaPlaylist>) -> u32 {
    let mut writer = CounterWritter::new();
    value.write_to(&mut writer).unwrap();
    (key.size() + writer.size) as u32
}

pub struct NewLocalPlayer {
    client: reqwest::Client,
    db: Database,
    cache_directory: Arc<std::path::Path>,
    m3u8_master_files: moka::future::Cache<M3U8CacheKey, Arc<MediaPlaylist>>,

    imdb_to_video_service: ImdbToVideoServer,
    file_path_mutexes: crate::utils::MultipleValueMutex<M3U8CacheKey>,

    total_file_size: Arc<std::sync::atomic::AtomicU64>,
    cleanup_in_progress: Arc<std::sync::atomic::AtomicBool>,
    max_total_file_size: u64,
}

impl NewLocalPlayer {
    pub async fn new(
        imdb_to_video_service: ImdbToVideoServer,
        client: reqwest::Client,
        NewLocalPlayerOptions {
            cache_directory,
            max_total_file_size,
            metadata_memory_cache_size,
        }: NewLocalPlayerOptions,
    ) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&cache_directory).await?;
        let db = Database::new(&cache_directory.join("metadata.sql")).await?;
        let total_file_size = db.get_total_size().await?;

        let m3u8_master_files = moka::future::CacheBuilder::new(metadata_memory_cache_size)
            .weigher(weigher)
            .build();
        Ok(Self {
            client,
            db,
            cleanup_in_progress: Arc::new(AtomicBool::new(false)),
            file_path_mutexes: MultipleValueMutex::new(),
            imdb_to_video_service,
            max_total_file_size,
            total_file_size: Arc::new(AtomicU64::new(total_file_size)),
            cache_directory: cache_directory.into(),
            m3u8_master_files,
        })
    }

    fn movie_file_path(
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
        SegmentsContent<impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>>>,
    > {
        let segment_files = segment_range.map(async |index| {
            let path = Self::movie_file_path(&self.cache_directory, imdb, &server);

            let file_mutex_guard = self
                .file_path_mutexes
                .lock_mutex(M3U8CacheKey {
                    imdb,
                    server_name: server.clone(),
                })
                .await;
            // TODO: before opening the file, we should check a memory cache if it exists. And when we are done reading the file, we should add the entry in the cache
            match tokio::fs::File::open(&path).await {
                Ok(file) => {
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
                    Ok(stream)
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
                    tokio::spawn(async move {
                        if let Err(e) = tokio::fs::write(&tmp_file.path, &result).await {
                            tracing::error!("Failed to write temporary segment file: {e:?}");
                            return;
                        }

                        let start_time = TsStartTimeParser::new()
                            .parse_packets(result.clone())
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
                            )
                            .await
                        {
                            tracing::error!("Failed to save segment info to database: {e:?}");
                            return;
                        }

                        // TODO: save segment info to database before moving the file
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
                    });
                    let stream =
                        Box::pin(futures::stream::iter([std::io::Result::Ok(result_clone)]))
                            as Pin<
                                Box<
                                    dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>>
                                        + Send,
                                >,
                            >;
                    Ok(stream)
                }
                Err(e) => Err(e).context("Failed to open segment file"),
            }
        });
        let segment_files = try_join_all(segment_files).await?;
        let stream = futures::stream::iter(segment_files).flatten();
        Ok(SegmentsContent { stream })
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
            let db = self.db.clone();
            let segments_directory = self.cache_directory.clone();
            let total_file_size = self.total_file_size.clone();
            let mut least_accesed_count = 5;
            let max_total_file_size = self.max_total_file_size;
            let cleanup_future = async move {
                'enough_memory: loop {
                    let mut stream = db.get_least_accessed(least_accesed_count);
                    while let Some(LeastAccessedSegment {
                        imdb,
                        server,
                        segment,
                    }) = stream.try_next().await?
                    {
                        let path = Self::movie_file_path(&segments_directory, imdb, &server);
                        let new_size = match tokio::fs::remove_file(&path).await {
                            Ok(_) => {
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

    async fn get_m3u8_inner(
        m3u8_master_files: &moka::future::Cache<M3U8CacheKey, Arc<MediaPlaylist>>,
        imdb_to_video_service: &ImdbToVideoServer,
        client: &reqwest::Client,
        m3u8_key: &M3U8CacheKey,
    ) -> anyhow::Result<Arc<MediaPlaylist>> {
        let r = m3u8_master_files
            .try_get_with_by_ref(m3u8_key, async {
                let m3u8_url = imdb_to_video_service
                    .get_from_server(m3u8_key.imdb, &m3u8_key.server_name)
                    .await?
                    .context("Player not found")?
                    .data
                    .video
                    .context("Video url not scrapped")?;

                let master_bytes = client
                    .get(m3u8_url.deref())
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes()
                    .await?;

                let mut master = m3u8_rs::parse_master_playlist_res(&master_bytes)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                drop(master_bytes);

                let mut first_stream = true;
                master.variants.retain(|v| {
                    if v.is_i_frame {
                        return true;
                    }
                    if first_stream {
                        first_stream = false;
                        return true;
                    }
                    tracing::warn!("Multiple streams available for {m3u8_url}");
                    false
                });
                let stream = master
                    .variants
                    .iter()
                    .find(|v| !v.is_i_frame)
                    .context("No data stream")?;

                let playlist_data = client
                    .get(&stream.uri)
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes()
                    .await?;

                let playlist = m3u8_rs::parse_media_playlist_res(&playlist_data)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                anyhow::Ok(Arc::new(playlist))
            })
            .await;

        match r {
            Ok(r) => Ok(r),
            Err(e) => match Arc::try_unwrap(e) {
                Ok(e) => Err(e),
                Err(e) => Err(anyhow::anyhow!("{e}")),
            },
        }
    }

    // pub async fn compute_m3u8_real_segments_duration(
    //     &self,
    //     m3u8_key: &M3U8CacheKey,
    //     with_timeout: bool,
    // ) -> anyhow::Result<Vec<SegmentsTime>> {
    //     let playlist = self.get_m3u8(m3u8_key).await?;

    //     let movie_duration: f32 = playlist.segments.iter().map(|s| s.duration).sum();
    //     let segments_len = playlist.segments.len();
    //     // TODO: add the cache back
    //     _ = self
    //         .get_segments(m3u8_key.imdb, m3u8_key.server_name.clone(), 0..1)
    //         .await?;

    //     let mut one_segment_times = self
    //         .time_cache
    //         .get_or_fetch(&self.segments_data, m3u8_key, &m3u8.playlist, with_timeout)
    //         .await?;

    //     let mut segments = Vec::new();

    //     if !one_segment_times.iter().any(|s| s.segment_index == 0) {
    //         tracing::error!(
    //             "There was an error and we didn't received the first segment time. Assuming to be 0"
    //         );
    //         one_segment_times.push(OneSegmentTime {
    //             segment_index: 0,
    //             start_time: 0.,
    //         });
    //     }
    //     one_segment_times.sort_by_key(|v| v.segment_index);

    //     for i in 0..one_segment_times.len() - 1 {
    //         let segment = SegmentsTime {
    //             duration: one_segment_times[i + 1].start_time - one_segment_times[i].start_time,
    //             segments_range: one_segment_times[i].segment_index
    //                 ..one_segment_times[i + 1].segment_index,
    //         };
    //         segments.push(segment);
    //     }
    //     let last_segment = one_segment_times.last().unwrap();
    //     segments.push(SegmentsTime {
    //         segments_range: last_segment.segment_index..segments_len,
    //         duration: movie_duration - last_segment.start_time,
    //     });
    //     Ok(segments)
    // }

    // TODO: find a way to fix the cache duration
    pub async fn get_m3u8(&self, m3u8_key: &M3U8CacheKey) -> anyhow::Result<Arc<MediaPlaylist>> {
        Self::get_m3u8_inner(
            &self.m3u8_master_files,
            &self.imdb_to_video_service,
            &self.client,
            m3u8_key,
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
}
