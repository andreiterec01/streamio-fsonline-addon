use axum::{extract::Request, middleware::Next, response::Response};

pub async fn log_request_response(request: Request, next: Next) -> Response {
    tracing::info!("Received request {}", request.uri());
    let response = next.run(request).await;

    response
}
