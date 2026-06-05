use axum::{extract::Request, middleware::Next, response::Response};

pub async fn log_request_response(request: Request, next: Next) -> Response {
    let now = std::time::Instant::now();
    let uri = request.uri().clone();
    let response = next.run(request).await;
    tracing::info!(
        "Received request {}. Responded in {:.2}s",
        uri,
        now.elapsed().as_secs_f32()
    );

    response
}
