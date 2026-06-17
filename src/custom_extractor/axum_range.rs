//! # axum-range
//!
//! HTTP range responses for [`axum`][1].
//!
//! Fully generic, supports any body implementing the [`RangeBody`] trait.
//!
//! Any type implementing both [`AsyncRead`] and [`AsyncSeekStart`] can be
//! used the [`KnownSize`] adapter struct. There is also special cased support
//! for [`tokio::fs::File`], see the [`KnownSize::file`] method.
//!
//! [`AsyncSeekStart`] is a trait defined by this crate which only allows
//! seeking from the start of a file. It is automatically implemented for any
//! type implementing [`AsyncSeek`].
//!
//! ```
//! use axum::Router;
//! use axum::routing::get;
//! use axum_extra::TypedHeader;
//! use axum_extra::headers::Range;
//!
//! use tokio::fs::File;
//!
//! use axum_range::Ranged;
//! use axum_range::KnownSize;
//!
//! async fn file(range: Option<TypedHeader<Range>>) -> Ranged<KnownSize<File>> {
//!     let file = File::open("The Sims 1 - The Complete Collection.rar").await.unwrap();
//!     let body = KnownSize::file(file).await.unwrap();
//!     let range = range.map(|TypedHeader(range)| range);
//!     Ranged::new(range, body)
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     // build our application with a single route
//!     let _app = Router::<()>::new().route("/", get(file));
//!
//!     // run it with hyper on localhost:3000
//!     #[cfg(feature = "run_server_in_example")]
//!     axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
//!        .serve(_app.into_make_service())
//!        .await
//!        .unwrap();
//! }
//! ```
//!
//! [1]: https://docs.rs/axum

use std::ops::Bound;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum_extra::TypedHeader;
use axum_extra::headers::{AcceptRanges, ContentLength, ContentRange, Range};

/// The main responder type. Implements [`IntoResponse`].
pub struct Ranged {
    range: Option<Range>,
    body: axum::body::Bytes,
}

impl Ranged {
    /// Construct a ranged response over any type implementing [`RangeBody`]
    /// and an optional [`Range`] header.
    pub fn new(range: Option<Range>, body: axum::body::Bytes) -> Self {
        Ranged { range, body }
    }

    /// Responds to the request, returning headers and body as
    /// [`RangedResponse`]. Returns [`RangeNotSatisfiable`] error if requested
    /// range in header was not satisfiable.
    pub fn try_respond(self) -> Result<RangedResponse, RangeNotSatisfiable> {
        let total_bytes = self.body.len();

        // we don't support multiple byte ranges, only none or one
        // fortunately, only responding with one of the requested ranges and
        // no more seems to be compliant with the HTTP spec.
        let range = self
            .range
            .and_then(|range| range.satisfiable_ranges(total_bytes as u64).nth(0));

        // pull seek positions out of range header
        let seek_start = match range {
            Some((Bound::Included(seek_start), _)) => seek_start as usize,
            _ => 0,
        };

        let seek_end_excl = match range {
            // HTTP byte ranges are inclusive, so we translate to exclusive by adding 1:
            Some((_, Bound::Included(end))) => {
                if end >= total_bytes as u64 {
                    total_bytes
                } else {
                    end as usize + 1
                }
            }
            _ => total_bytes,
        };

        // check seek positions and return with 416 Range Not Satisfiable if invalid
        let seek_start_beyond_seek_end = seek_start > seek_end_excl;
        // we could use >= above but I think this reads more clearly:
        let zero_length_range = seek_start == seek_end_excl;

        if seek_start_beyond_seek_end || zero_length_range {
            let content_range = ContentRange::unsatisfied_bytes(total_bytes as u64);
            return Err(RangeNotSatisfiable(content_range));
        }

        // if we're good, build the response
        let content_range = range.map(|_| {
            ContentRange::bytes(seek_start as u64..seek_end_excl as u64, total_bytes as u64)
                .expect("ContentRange::bytes cannot panic in this usage")
        });

        let content_length = ContentLength((seek_end_excl - seek_start) as u64);

        let stream = self.body.slice(seek_start..seek_end_excl);

        Ok(RangedResponse {
            content_range,
            content_length,
            stream,
        })
    }
}

impl IntoResponse for Ranged {
    fn into_response(self) -> Response {
        self.try_respond().into_response()
    }
}

/// Error type indicating that the requested range was not satisfiable. Implements [`IntoResponse`].
#[derive(Debug, Clone)]
pub struct RangeNotSatisfiable(pub ContentRange);

impl IntoResponse for RangeNotSatisfiable {
    fn into_response(self) -> Response {
        let status = StatusCode::RANGE_NOT_SATISFIABLE;
        let header = TypedHeader(self.0);
        (status, header, ()).into_response()
    }
}

/// Data type containing computed headers and body for a range response. Implements [`IntoResponse`].
pub struct RangedResponse {
    pub content_range: Option<ContentRange>,
    pub content_length: ContentLength,
    pub stream: axum::body::Bytes,
}

impl IntoResponse for RangedResponse {
    fn into_response(self) -> Response {
        let content_range = self.content_range.map(TypedHeader);
        let content_length = TypedHeader(self.content_length);
        let accept_ranges = TypedHeader(AcceptRanges::bytes());
        let stream = self.stream;

        let status = match content_range {
            Some(_) => StatusCode::PARTIAL_CONTENT,
            None => StatusCode::OK,
        };

        (status, content_range, content_length, accept_ranges, stream).into_response()
    }
}

// #[cfg(test)]
// mod tests {
//     use std::io;

//     use axum::body::Bytes;
//     use axum::http::HeaderValue;
//     use axum_extra::headers::{ContentRange, Header, Range};
//     use futures::{Stream, StreamExt, pin_mut};
//     use tokio::fs::File;

//     use crate::KnownSize;
//     use crate::Ranged;

//     async fn collect_stream(stream: impl Stream<Item = io::Result<Bytes>>) -> String {
//         let mut string = String::new();
//         pin_mut!(stream);
//         while let Some(chunk) = stream.next().await.transpose().unwrap() {
//             string += std::str::from_utf8(&chunk).unwrap();
//         }
//         string
//     }

//     fn range(header: &str) -> Option<Range> {
//         let val = HeaderValue::from_str(header).unwrap();
//         Some(Range::decode(&mut [val].iter()).unwrap())
//     }

//     async fn body() -> KnownSize<File> {
//         let file = File::open("test/fixture.txt").await.unwrap();
//         KnownSize::file(file).await.unwrap()
//     }

//     #[tokio::test]
//     async fn test_full_response() {
//         let ranged = Ranged::new(None, body().await);

//         let response = ranged.try_respond().expect("try_respond should return Ok");

//         assert_eq!(54, response.content_length.0);
//         assert!(response.content_range.is_none());
//         assert_eq!(
//             "Hello world this is a file to test range requests on!\n",
//             &collect_stream(response.stream).await
//         );
//     }

//     #[tokio::test]
//     async fn test_partial_response_1() {
//         let ranged = Ranged::new(range("bytes=0-29"), body().await);

//         let response = ranged.try_respond().expect("try_respond should return Ok");

//         assert_eq!(30, response.content_length.0);

//         let expected_content_range = ContentRange::bytes(0..30, 54).unwrap();
//         assert_eq!(Some(expected_content_range), response.content_range);

//         assert_eq!(
//             "Hello world this is a file to ",
//             &collect_stream(response.stream).await
//         );
//     }

//     #[tokio::test]
//     async fn test_partial_response_2() {
//         let ranged = Ranged::new(range("bytes=30-53"), body().await);

//         let response = ranged.try_respond().expect("try_respond should return Ok");

//         assert_eq!(24, response.content_length.0);

//         let expected_content_range = ContentRange::bytes(30..54, 54).unwrap();
//         assert_eq!(Some(expected_content_range), response.content_range);

//         assert_eq!(
//             "test range requests on!\n",
//             &collect_stream(response.stream).await
//         );
//     }

//     #[tokio::test]
//     async fn test_unbounded_start_response() {
//         // unbounded ranges in HTTP are actually a suffix

//         let ranged = Ranged::new(range("bytes=-20"), body().await);

//         let response = ranged.try_respond().expect("try_respond should return Ok");

//         assert_eq!(20, response.content_length.0);

//         let expected_content_range = ContentRange::bytes(34..54, 54).unwrap();
//         assert_eq!(Some(expected_content_range), response.content_range);

//         assert_eq!(
//             " range requests on!\n",
//             &collect_stream(response.stream).await
//         );
//     }

//     #[tokio::test]
//     async fn test_unbounded_end_response() {
//         let ranged = Ranged::new(range("bytes=40-"), body().await);

//         let response = ranged.try_respond().expect("try_respond should return Ok");

//         assert_eq!(14, response.content_length.0);

//         let expected_content_range = ContentRange::bytes(40..54, 54).unwrap();
//         assert_eq!(Some(expected_content_range), response.content_range);

//         assert_eq!(" requests on!\n", &collect_stream(response.stream).await);
//     }

//     #[tokio::test]
//     async fn test_one_byte_response() {
//         let ranged = Ranged::new(range("bytes=30-30"), body().await);

//         let response = ranged.try_respond().expect("try_respond should return Ok");

//         assert_eq!(1, response.content_length.0);

//         let expected_content_range = ContentRange::bytes(30..31, 54).unwrap();
//         assert_eq!(Some(expected_content_range), response.content_range);

//         assert_eq!("t", &collect_stream(response.stream).await);
//     }

//     #[tokio::test]
//     async fn test_invalid_range() {
//         let ranged = Ranged::new(range("bytes=30-29"), body().await);

//         let err = ranged
//             .try_respond()
//             .err()
//             .expect("try_respond should return Err");

//         let expected_content_range = ContentRange::unsatisfied_bytes(54);
//         assert_eq!(expected_content_range, err.0)
//     }

//     #[tokio::test]
//     async fn test_range_end_exceed_length() {
//         let ranged = Ranged::new(range("bytes=30-99"), body().await);

//         let response = ranged.try_respond().expect("try_respond should return Ok");

//         let expected_content_range = ContentRange::bytes(30..54, 54).unwrap();
//         assert_eq!(Some(expected_content_range), response.content_range);

//         assert_eq!(
//             "test range requests on!\n",
//             &collect_stream(response.stream).await
//         );
//     }

//     #[tokio::test]
//     async fn test_range_start_exceed_length() {
//         let ranged = Ranged::new(range("bytes=99-"), body().await);

//         let err = ranged
//             .try_respond()
//             .err()
//             .expect("try_respond should return Err");

//         let expected_content_range = ContentRange::unsatisfied_bytes(54);
//         assert_eq!(expected_content_range, err.0)
//     }
// }
