use std::path::Path;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::io::{ReadBuf, SeekFrom};
use tokio::prelude::AsyncRead;

use crate::checksum::calculate;
use crate::LogPosition;

#[derive(Debug)]
pub enum Error {
    Io(Box<dyn std::error::Error + Sync + Send>),
    /// Indicates that a read position was not aligned to an item, or the log is corrupt.
    InvalidItemChecksum,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => e.fmt(f),
            Error::InvalidItemChecksum => write!(f, "Item checksum failed"),
        }
    }
}

impl std::error::Error for Error {}

pub struct ReaderFactory {
    pub(crate) path: Box<Path>,
    pub(crate) tail_recv: tokio::sync::watch::Receiver<LogPosition>,
}

struct ReaderState {
    tail_recv: tokio::sync::watch::Receiver<LogPosition>,
    pos: LogPosition,
}

pub struct Reader {
    file: File,
    state: ReaderState,
}

impl Reader {
    pub fn position(&self) -> LogPosition {
        self.state.pos
    }
}

pub struct LogItem {
    start_pos: LogPosition,
    len: u32,
    read: usize,
    file: File,

    reader_state: ReaderState,
}

impl LogItem {
    pub fn position(&self) -> LogPosition {
        self.start_pos
    }

    /// Returns the length of the contents in bytes.
    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn left_to_read(&self) -> usize {
        self.len as usize - self.read
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Finish with the log item so we can recover the reader to fetch the next.
    pub fn finish(self) -> Reader {
        Reader {
            file: self.file,
            state: self.reader_state,
        }
    }
}

impl<'a> AsyncRead for LogItem {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // There must be a better way to do this?
        let length_before = buf.filled().len();
        let left_to_read = self.left_to_read();
        let mut limited_file = (&mut self.file).take(left_to_read as u64);
        let pinned_file = Pin::new(&mut limited_file);
        let res = pinned_file.poll_read(cx, buf);
        let length_after = buf.filled().len();
        self.read += length_after - length_before;
        res
    }
}

impl ReaderFactory {
    pub async fn read_from(&self, position: LogPosition) -> Result<Reader, Error> {
        let path = self.path.join("log");
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .await
            .map_err(|e| Error::Io(Box::new(e)))?;
        let tail_recv = self.tail_recv.clone();

        Ok(Reader {
            file,
            state: ReaderState {
                tail_recv,
                pos: position,
            },
        })
    }
}

async fn read_log_item_size(file: &mut File, position: LogPosition) -> Result<u32, Error> {
    let checksum = file.read_u16().await.map_err(|e| Error::Io(Box::new(e)))?;
    let len = file.read_u32().await.map_err(|e| Error::Io(Box::new(e)))?;

    if calculate(position, len) == checksum {
        Ok(len)
    } else {
        Err(Error::InvalidItemChecksum)
    }
}

pub enum NextItem {
    Item(LogItem),
    End(Reader),
}

impl NextItem {
    pub fn unwrap(self) -> LogItem {
        match self {
            NextItem::Item(item) => item,
            NextItem::End(_) => panic!("Unwrapped an End"),
        }
    }

    pub fn is_end(&self) -> bool {
        match self {
            NextItem::Item(_) => false,
            NextItem::End(_) => true,
        }
    }
}

impl Reader {
    async fn read_item(self) -> Result<LogItem, Error> {
        let Reader { mut file, state } = self;

        // Might not have read entire item last time
        file.seek(SeekFrom::Start(state.pos))
            .await
            .map_err(|e| Error::Io(Box::new(e)))?;
        let len = read_log_item_size(&mut file, state.pos).await?;

        let next_pos = state.pos + 2 + 4 + (len as u64);

        Ok(LogItem {
            start_pos: state.pos,
            file,
            read: 0,
            len,
            reader_state: ReaderState {
                pos: next_pos,
                tail_recv: state.tail_recv,
            },
        })
    }

    pub async fn next<'a>(mut self, wait_for_more: bool) -> Result<NextItem, Error> {
        let mut log_tail: LogPosition = *self.state.tail_recv.borrow();

        while log_tail <= self.state.pos && wait_for_more {
            match self.state.tail_recv.changed().await {
                Err(_) => return Ok(NextItem::End(self)),
                Ok(()) => {
                    log_tail = *self.state.tail_recv.borrow();
                }
            }
        }

        if self.state.pos < log_tail {
            self.read_item().await.map(NextItem::Item)
        } else {
            Ok(NextItem::End(self))
        }
    }
}
