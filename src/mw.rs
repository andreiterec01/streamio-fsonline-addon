use std::sync::Arc;

use axum::{extract::Request, middleware::Next, response::Response};

use crate::error::WebError;

pub async fn log_request_response(request: Request, next: Next) -> Response {
    let now = std::time::Instant::now();
    let uri = request.uri().clone();
    let response = next.run(request).await;
    tracing::info!(
        "Received request {}. Responded in {:.2}s with {}",
        uri,
        now.elapsed().as_secs_f32(),
        response.status()
    );
    let error = response.extensions().get::<Arc<WebError>>();
    if let Some(e) = error {
        tracing::error!("Responded with error: {e:?}");
    }
    response
}
