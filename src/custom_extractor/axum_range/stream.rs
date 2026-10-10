use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::Stream;
use http_body::{Body, Frame, SizeHint};
use pin_project::pin_project;

use super::RangeBody;

/// Response body stream. Implements [`Stream`], [`Body`], and [`IntoResponse`].
#[pin_project]
pub struct RangedStream<B> {
    state: StreamState,
    length: u64,
    #[pin]
    body: B,
}

impl<B: RangeBody + Send + 'static> RangedStream<B> {
    pub(crate) fn new(body: B, start: u64, length: u64) -> Self {
        RangedStream {
            state: StreamState::Seek { start },
            length,
            body,
        }
    }
}

#[derive(Debug)]
enum StreamState {
    Seek { start: u64 },
    Seeking { remaining: u64 },
    Reading { remaining: u64 },
}

impl<B: RangeBody + Send + 'static> IntoResponse for RangedStream<B> {
    fn into_response(self) -> Response {
        Response::new(axum::body::Body::new(self))
    }
}

impl<B: RangeBody> Body for RangedStream<B> {
    type Data = Bytes;
    type Error = io::Error;

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.length)
    }

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Frame<Bytes>>>> {
        self.poll_next(cx)
            .map(|item| item.map(|result| result.map(Frame::data)))
    }
}

impl<B: RangeBody> Stream for RangedStream<B> {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<io::Result<Bytes>>> {
        let mut this = self.project();

        loop {
            match this.state {
                StreamState::Seek { start } => match this.body.as_mut().start_seek(*start) {
                    Err(e) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                    Ok(()) => {
                        let remaining = *this.length;
                        *this.state = StreamState::Seeking { remaining };
                    }
                },
                StreamState::Seeking { remaining } => {
                    match ready!(this.body.as_mut().poll_complete(cx)) {
                        Err(e) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Ok(()) => {
                            *this.state = StreamState::Reading {
                                remaining: *remaining,
                            };
                        }
                    }
                }

                StreamState::Reading { remaining } => {
                    if *remaining == 0 {
                        return Poll::Ready(None);
                    }
                    let result = ready!(this.body.as_mut().poll_next(cx));
                    match result {
                        None => {
                            return Poll::Ready(None);
                        }
                        Some(Err(e)) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Some(Ok(bytes)) => {
                            if *remaining < bytes.len() as u64 {
                                let result = bytes.slice(..*remaining as usize);
                                *remaining = 0;
                                return Poll::Ready(Some(Ok(result)));
                            } else {
                                *remaining -= bytes.len() as u64;
                                return Poll::Ready(Some(Ok(bytes)));
                            }
                        }
                    }
                }
            }
        }
    }
}
