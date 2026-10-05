use std::pin::Pin;

use bytes::Buf;
use futures::TryStreamExt;
use tokio::io::AsyncSeekExt;

pub(super) enum FileOrContent {
    File(tokio::fs::File),
    Content(bytes::Bytes),
}

impl FileOrContent {
    pub(super) fn into_stream(
        self,
    ) -> Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>> {
        match self {
            Self::File(file) => {
                let stream =
                    tokio_util::codec::FramedRead::new(file, tokio_util::codec::BytesCodec::new())
                        .map_ok(bytes::BytesMut::freeze);
                Box::pin(stream)
            }
            Self::Content(content) => {
                Box::pin(futures::stream::iter([std::io::Result::Ok(content)]))
            }
        }
    }

    pub(super) async fn seek(&mut self, start_index: u64) -> anyhow::Result<()> {
        match self {
            Self::File(file) => {
                file.seek(std::io::SeekFrom::Start(start_index)).await?;
            }
            Self::Content(content) => {
                content.advance(start_index as usize);
            }
        }

        Ok(())
    }

    pub(super) async fn len(&self) -> anyhow::Result<u64> {
        match self {
            Self::File(file) => Ok(file.metadata().await?.len()),
            Self::Content(bytes) => Ok(bytes.len() as u64),
        }
    }
}
