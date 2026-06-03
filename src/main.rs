use std::{
    collections::BTreeSet,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use axum::{
    Router,
    extract::{FromRef, State},
    response::IntoResponse,
};
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use chromiumoxide::{Browser, cdp::browser_protocol::network::EventRequestWillBeSent};
use clap::Parser;
use html5ever::{interface::TreeSink, parse_document, tendril::TendrilSink};
use itertools::Itertools;
use scraper::{Element, Html, Selector};
use tower::ServiceBuilder;
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
};

use crate::service::{ImdbService, VideoServer};

mod args;
mod contracts;
mod error;
mod mw;
mod routes;
mod service;

#[derive(Clone)]
pub struct IndexHtml(pub Arc<str>);

#[derive(FromRef, Clone)]
pub struct AppState {
    server: VideoServer,
    imdb_server: ImdbService,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // all imdb keys at: https://datasets.imdbws.com/title.basics.tsv.gz
    dotenvy::dotenv().ok();
    let args = args::Args::parse();
    tracing_subscriber::fmt().init();
    let config = RustlsConfig::from_pem_file(args.ssl_cert_path, args.ssl_key_path).await?;
    let client = reqwest::Client::new();
    let state = AppState {
        server: VideoServer::new(client.clone(), args.headless_browser).await?,
        imdb_server: ImdbService::new(client),
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
    let server = tokio::spawn(
        axum_server::bind(SocketAddr::V4(addr))
            .acceptor(RustlsAcceptor::new(config))
            .handle(handle.clone())
            .serve(router.into_make_service()),
    );
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
