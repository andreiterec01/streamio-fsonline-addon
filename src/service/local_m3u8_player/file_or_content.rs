use std::{collections::VecDeque, task::ready};

use futures::StreamExt;
use tokio::io::AsyncRead;

use crate::custom_extractor::axum_range::{AsyncSeekStart, RangeBody};

pub enum FileOrBytes {
    File { file: tokio::fs::File, len: u64 },
    Bytes(bytes::Bytes),
}

impl FileOrBytes {
    pub(crate) fn len(&self) -> u64 {
        match self {
            FileOrBytes::File { len, .. } => *len,
            FileOrBytes::Bytes(bytes) => bytes.len() as u64,
        }
    }
}

pub struct StreamFileOrBytes {
    content: VecDeque<FileOrBytes>,
    current_stream: Option<tokio_util::io::ReaderStream<AsyncReadMultipleFiles>>,
    len: u64,
}

impl StreamFileOrBytes {
    pub(crate) fn new(content: VecDeque<FileOrBytes>, len: u64) -> Self {
        StreamFileOrBytes {
            content,
            current_stream: None,
            len,
        }
    }
}

impl RangeBody for StreamFileOrBytes {
    fn byte_size(&self) -> u64 {
        self.len
    }
}

impl AsyncSeekStart for StreamFileOrBytes {
    fn start_seek(mut self: std::pin::Pin<&mut Self>, position: u64) -> std::io::Result<()> {
        let mut remaining = position;
        while remaining > 0 {
            match self.content.front_mut() {
                Some(FileOrBytes::File { file, len }) => {
                    if *len <= remaining {
                        remaining -= *len;
                        self.content.pop_front();
                    } else {
                        std::pin::Pin::new(file).start_seek(position)?;
                        *len -= remaining;
                        remaining = 0;
                    }
                }
                Some(FileOrBytes::Bytes(bytes)) => {
                    if bytes.len() as u64 <= remaining {
                        remaining -= bytes.len() as u64;
                        self.content.pop_front();
                    } else {
                        *bytes = bytes.slice(remaining as usize..);
                        remaining = 0;
                    }
                }
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Not enough data to seek",
                    ));
                }
            }
        }
        Ok(())
    }

    fn poll_complete(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let Some(FileOrBytes::File { file, len: _ }) = self.content.front_mut() else {
            return std::task::Poll::Ready(Ok(()));
        };

        std::pin::Pin::new(file).poll_complete(cx)
    }
}

impl futures::Stream for StreamFileOrBytes {
    type Item = std::io::Result<bytes::Bytes>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if let Some(stream) = &mut self.current_stream {
                let r = ready!(stream.poll_next_unpin(cx));
                match r {
                    Some(bytes) => {
                        return std::task::Poll::Ready(Some(bytes));
                    }
                    None => {
                        self.current_stream = None;
                    }
                }
            }
            let mut multiple_files = AsyncReadMultipleFiles {
                files: VecDeque::new(),
            };
            while let Some(next) = self.content.pop_front() {
                match next {
                    FileOrBytes::Bytes(bytes) => {
                        if multiple_files.files.is_empty() {
                            return std::task::Poll::Ready(Some(Ok(bytes)));
                        } else {
                            self.content.push_front(FileOrBytes::Bytes(bytes));
                            break;
                        }
                    }
                    FileOrBytes::File { file, len: _ } => {
                        multiple_files.files.push_back(file);
                    }
                }
            }
            if multiple_files.files.is_empty() {
                return std::task::Poll::Ready(None);
            }
            self.current_stream = Some(tokio_util::io::ReaderStream::new(multiple_files));
        }
    }
}

struct AsyncReadMultipleFiles {
    files: VecDeque<tokio::fs::File>,
}

impl AsyncRead for AsyncReadMultipleFiles {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let initial_filled = buf.filled().len();
        loop {
            let Some(file) = self.files.front_mut() else {
                return std::task::Poll::Ready(Ok(()));
            };
            let old_filled = buf.filled().len();
            match std::pin::Pin::new(file).poll_read(cx, buf) {
                std::task::Poll::Pending => {
                    return if buf.filled().len() > initial_filled {
                        std::task::Poll::Ready(Ok(()))
                    } else {
                        std::task::Poll::Pending
                    };
                }
                std::task::Poll::Ready(result) => match result {
                    Ok(()) => {
                        if buf.remaining() == 0 {
                            return std::task::Poll::Ready(Ok(()));
                        }
                        if buf.filled().len() == old_filled {
                            self.files.pop_front();
                        }
                        continue;
                    }
                    Err(e) => {
                        return std::task::Poll::Ready(Err(e));
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, pin::Pin};

    use tokio::io::AsyncReadExt;

    use super::AsyncReadMultipleFiles;

    #[tokio::test]
    async fn reads_across_multiple_files_without_losing_buffered_data() {
        let first = tokio::fs::File::open("src/custom_extractor/axum_range/test/fixture.txt")
            .await
            .unwrap();
        let second = tokio::fs::File::open("src/custom_extractor/axum_range/test/fixture.txt")
            .await
            .unwrap();
        let fixture = include_bytes!("../../custom_extractor/axum_range/test/fixture.txt");
        let expected = [fixture.as_slice(), fixture.as_slice()].concat();
        let mut reader = AsyncReadMultipleFiles {
            files: VecDeque::from([first, second]),
        };
        let mut actual = Vec::new();
        Pin::new(&mut reader)
            .read_to_end(&mut actual)
            .await
            .unwrap();
        assert_eq!(expected, actual);
    }
}
