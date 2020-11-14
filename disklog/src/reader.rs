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
    Io(Box<dyn std::error::Error>),
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
    pub(crate) tail_pos_recv: tokio::sync::watch::Receiver<LogPosition>,
}

pub struct Reader {
    file: File,
    tail_pos_recv: tokio::sync::watch::Receiver<LogPosition>,
    pos: LogPosition,
}

pub struct LogItem<'a> {
    pos: LogPosition,
    len: u32,
    read: usize,
    file: &'a mut File,
}

impl LogItem<'_> {
    pub fn pos(&self) -> LogPosition {
        self.pos
    }
}

impl<'a> AsyncRead for LogItem<'a> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // There must be a better way to do this?
        let length_before = buf.filled().len();
        let left_to_read: usize = (self.len as usize) - self.read;
        let mut limited_file = self.file.take(left_to_read as u64);
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

        Ok(Reader {
            file,
            tail_pos_recv: self.tail_pos_recv.clone(),
            pos: position,
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

impl Reader {
    async fn read_item<'a>(&'a mut self) -> Result<LogItem<'a>, Error> {
        // Might not have read entire item last time
        self.file
            .seek(SeekFrom::Start(self.pos))
            .await
            .map_err(|e| Error::Io(Box::new(e)))?;
        let len = read_log_item_size(&mut self.file, self.pos).await?;
        let last_pos = self.pos;

        self.pos = self.pos + 2 + 4 + (len as u64);

        Ok(LogItem {
            pos: last_pos,
            file: &mut self.file,
            read: 0,
            len,
        })
    }

    pub async fn next<'a>(&'a mut self, wait_for_more: bool) -> Result<Option<LogItem<'a>>, Error> {
        let log_tail_pos: LogPosition = { *self.tail_pos_recv.borrow() };
        if log_tail_pos <= self.pos {
            if wait_for_more {
                self.tail_pos_recv
                    .changed()
                    .await
                    .map_err(|e| Error::Io(Box::new(e)))?;

                let changed_tail_pos: LogPosition = { *self.tail_pos_recv.borrow() };
                if log_tail_pos == changed_tail_pos {
                    // The writer has closed
                    Ok(None)
                } else {
                    self.read_item().await.map(|li| Some(li))
                }
            } else {
                Ok(None)
            }
        } else {
            self.read_item().await.map(|li| Some(li))
        }
    }
}
