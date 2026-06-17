use axum::response::IntoResponse;
pub mod axum_range;
#[derive(Clone, Copy)]
pub struct DontLog;
pub struct DontLogResponse<T>(pub T);

impl<T> IntoResponse for DontLogResponse<T>
where
    T: IntoResponse,
{
    fn into_response(self) -> axum::response::Response {
        let mut r = self.0.into_response();
        r.extensions_mut().insert(DontLog);
        r
    }
}
