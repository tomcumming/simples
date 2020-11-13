mod checksum;
mod open;

pub mod writer;

use std::path::{Path};

use writer::Writer;
use open::{open_log_file, open_tail_file};

pub(crate) const U64SIZE: usize = std::mem::size_of::<u64>();

pub type LogPosition = u64;

pub struct ReaderFactory {
    path: Box<Path>,
    tail_pos_recv: tokio::sync::watch::Receiver<LogPosition>,
}

#[derive(Debug)]
pub enum OpenError {
    Io(Box<dyn std::error::Error>),
    CorruptTailPosition,
    LogTooSmall,
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Io(e) => e.fmt(f),
            OpenError::CorruptTailPosition => write!(f, "Corrupt tail position"),
            OpenError::LogTooSmall => write!(f, "Log file was smalled than expected"),
        }
    }
}

impl std::error::Error for OpenError {}

pub struct OpenedLog {
    pub writer: Writer,
    pub reader_factory: ReaderFactory,
    pub recovered: bool,
}

pub async fn open_log(path: impl AsRef<Path>) -> Result<OpenedLog, OpenError> {
    let path: Box<Path> = path.as_ref().into();
    let (tail_file, tail_pos) = open_tail_file(&path).await?;
    let (log_file, recovered) = open_log_file(&path, tail_pos).await?;

    let (tail_pos_sender, tail_pos_recv) = tokio::sync::watch::channel(tail_pos);

    Ok(OpenedLog {
        writer: Writer {
            log_file,
            tail_file,
            tail_pos_sender,
            tail_pos,
        },
        reader_factory: ReaderFactory {
            path,
            tail_pos_recv,
        },
        recovered,
    })
}
