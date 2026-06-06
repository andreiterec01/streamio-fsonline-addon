use std::sync::Arc;

use axum::{Json, response::IntoResponse};
use serde::Serialize;

#[derive(thiserror::Error, Debug)]
pub enum WebError {
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type WebResult<T> = Result<T, WebError>;

#[derive(Serialize)]
struct ErrorResponse {
    reason: &'static str,
    message: String,
}

impl IntoResponse for WebError {
    fn into_response(self) -> axum::response::Response {
        let (status, error) = match &self {
            Self::Other(error) => (
                hyper::StatusCode::INTERNAL_SERVER_ERROR,
                ErrorResponse {
                    message: error.to_string(),
                    reason: "other",
                },
            ),
        };

        let mut response = (status, Json(error)).into_response();
        response.extensions_mut().insert(Arc::new(self));
        response
    }
}
