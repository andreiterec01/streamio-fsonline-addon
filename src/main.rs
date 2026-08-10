use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};

use axum::extract::FromRef;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use clap::Parser;
use time::macros::format_description;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{EnvFilter, fmt::time::LocalTime};

use crate::{
    routes::install_ui,
    service::{
        ImdbToVideoServer,
        fsonline_service::VideoServer,
        imdb_service::ImdbService,
        local_m3u8_player::{self, LocalPlayer, LocalPlayerConfig},
        scrappers::{PlayerScrappers, file_sun::FileSuN, vidmoly::VidmolyScrapper},
    },
};

mod args;
mod contracts;
mod custom_extractor;
mod error;
mod mw;
mod routes;
mod service;
mod ts_parser;
#[derive(Clone)]
pub struct UsesHttps(pub bool);

#[derive(Clone)]
pub struct Host(pub Arc<str>);

#[derive(FromRef, Clone)]
pub struct AppState {
    video_service: VideoServer,
    imdb_to_video_server: ImdbToVideoServer,
    client: reqwest::Client,
    uses_https: UsesHttps,
    host: Host,
    local_player: LocalPlayer,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // all imdb keys at: https://datasets.imdbws.com/title.basics.tsv.gz
    dotenvy::dotenv().ok();
    let args = args::Args::parse();
    let filter = EnvFilter::from_default_env();
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stderr());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(non_blocking)
        .with_timer(LocalTime::new(format_description!(
            "[day]/[month] [hour]:[minute]:[second].[subsecond digits:3]"
        )))
        .init();
    tracing::info!("Running with args {args:?}");

    let config = match (args.ssl_cert_path, args.ssl_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let config = RustlsConfig::from_pem_file(cert_path, key_path).await?;
            Some(config)
        }
        (_, _) => None,
    };
    let client = reqwest::Client::new();

    let browser = crate::service::scrappers::browser_discovery_scrapper::BrowserDiscovery::new(
        args.headless_browser,
    )
    .await?;

    let mut scrappers = PlayerScrappers::new(browser);
    scrappers.add_scrapper(VidmolyScrapper::new(client.clone()));
    scrappers.add_scrapper(FileSuN::new(client.clone()));
    let video_service = VideoServer::new(client.clone(), scrappers).await?;
    let imdb_server = ImdbService::new(client.clone());
    let imdb_to_video_server = ImdbToVideoServer::new(video_service.clone(), imdb_server);
    let local_player_config = LocalPlayerConfig {
        // TODO: make this configurable
        cache_ttl: Duration::from_secs(3600 * 2),
        block_size_segments_mb: args.cache_block_size_mb,
        client: client.clone(),
        directory_cache: &args.cache_path,
        master_cache_size_bytes: args.master_cache_size,
        memory_segments_cache_size: args.memory_segments_cache_size,
        file_segments_cache_size: args.file_segments_cache_size_mb * 1024 * 1024,
        cache_next_segments: args.cache_next_segments,
        parallelism_count: 4,
        imdb_to_video_service: imdb_to_video_server.clone(),
    };

    let time_cache_options = local_m3u8_player::time_cache::TimeCacheOptions {
        client: client.clone(),
        // TODO: make this configurable
        cache_path: std::path::Path::new("./cache-new-timestamp"),
        cache_size_file_mb: 1024,
        cache_size_memory_mb: 200,
        bigger_time_between_segments: args.max_segment_duration,
        smaller_time_between_segments: args.target_segment_duration,
        timeout_fast_time: Duration::from_secs(args.timeout_waiting_for_playlist_sec),
    };

    let time_cache = local_m3u8_player::time_cache::TimeCache::new(time_cache_options).await?;

    let local_player = LocalPlayer::new(time_cache, local_player_config).await?;
    let state = AppState {
        video_service,
        imdb_to_video_server,
        uses_https: UsesHttps(config.is_some()),
        local_player: local_player.clone(),
        client,
        host: Host(args.host.into()),
    };

    let cors_layer = CorsLayer::new()
        .allow_origin(tower_http::cors::Any) // Open access to selected route
        .allow_methods(tower_http::cors::Any);
    let router = routes::routes()
        .route("/install", axum::routing::get(install_ui))
        // .nest_service(
        //     "/frontend",
        //     ServeDir::new(r"../client/build")
        //         .fallback(ServeFile::new(r"../client/build/index.html")),
        // )
        .layer(
            ServiceBuilder::new()
                .layer(axum::middleware::from_fn(mw::log_request_response))
                .layer(cors_layer),
        )
        .with_state(state);

    let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, args.port);

    tracing::info!("Starting the server on port {}", args.port);
    let handle = axum_server::Handle::new();
    let server = axum_server::bind(SocketAddr::V4(addr)).handle(handle.clone());

    let server = tokio::spawn(async move {
        match config {
            Some(config) => {
                server
                    .acceptor(RustlsAcceptor::new(config))
                    .serve(router.into_make_service())
                    .await
            }
            None => server.serve(router.into_make_service()).await,
        }
    });
    tokio::pin!(server);
    tokio::select! {
        r = &mut server => {
            anyhow::bail!("The server ended early with code: {r:?}");
        }
        () = wait_for_signal() => {
            handle.graceful_shutdown(Some(Duration::from_secs(10)))
        }
    };

    tracing::warn!("Received signal. Waiting for graceful shutdown");
    let r = server.await;
    local_player.close().await?;
    r??;
    Ok(())
}

/// Waits for a signal that requests a graceful shutdown, like SIGTERM or SIGINT.
#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    // Infos here:
    // https://www.gnu.org/software/libc/manual/html_node/Termination-Signals.html
    let mut signal_terminate = signal(SignalKind::terminate()).unwrap();
    let mut signal_interrupt = signal(SignalKind::interrupt()).unwrap();

    tokio::select! {
        _ = signal_terminate.recv() => tracing::debug!("Received SIGTERM."),
        _ = signal_interrupt.recv() => tracing::debug!("Received SIGINT."),
    };
}

/// Waits for a signal that requests a graceful shutdown, Ctrl-C (SIGINT).
#[cfg(windows)]
async fn wait_for_signal() {
    use tokio::signal::windows;

    // Infos here:
    // https://learn.microsoft.com/en-us/windows/console/handlerroutine
    let mut signal_c = windows::ctrl_c().unwrap();
    let mut signal_break = windows::ctrl_break().unwrap();
    let mut signal_close = windows::ctrl_close().unwrap();
    let mut signal_shutdown = windows::ctrl_shutdown().unwrap();

    tokio::select! {
        _ = signal_c.recv() => tracing::debug!("Received CTRL_C."),
        _ = signal_break.recv() => tracing::debug!("Received CTRL_BREAK."),
        _ = signal_close.recv() => tracing::debug!("Received CTRL_CLOSE."),
        _ = signal_shutdown.recv() => tracing::debug!("Received CTRL_SHUTDOWN."),
    };
}
