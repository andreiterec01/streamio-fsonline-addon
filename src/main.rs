use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};

use axum::extract::FromRef;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use clap::Parser;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, services::ServeFile};

use crate::service::{
    fsonline_service::VideoServer,
    imdb_service::ImdbService,
    local_m3u8_player::LocalPlayer,
    scrappers::{PlayerScrappers, vidmoly::VidmolyScrapper},
};

mod args;
mod contracts;
mod error;
mod mw;
mod routes;
mod service;
#[derive(Clone)]
pub struct UsesHttps(pub bool);

#[derive(Clone)]
pub struct Host(pub Arc<str>);

#[derive(FromRef, Clone)]
pub struct AppState {
    server: VideoServer,
    imdb_server: ImdbService,
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
    tracing_subscriber::fmt().init();
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
    let state = AppState {
        server: VideoServer::new(client.clone(), scrappers).await?,
        imdb_server: ImdbService::new(client.clone()),
        uses_https: UsesHttps(config.is_some()),
        // TODO: make the 3600*4 configurable
        local_player: LocalPlayer::new(
            client.clone(),
            Duration::from_secs(3600 * 4),
            10 * 1024 * 1024,
        ),
        client,
        host: Host(args.host.into()),
    };

    let cors_layer = CorsLayer::new()
        .allow_origin(tower_http::cors::Any) // Open access to selected route
        .allow_methods(tower_http::cors::Any);

    let router = routes::routes()
        // .nest_service(
        //     "/frontend",
        //     ServeDir::new(r"../client/build")
        //         .fallback(ServeFile::new(r"../client/build/index.html")),
        // )
        .route_service("/manifest.json", ServeFile::new(r"manifest.json"))
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
    server.await??;

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
