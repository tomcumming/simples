mod checksum;
mod open;

pub mod reader;
pub mod writer;

use std::path::Path;

use open::{open_log_file, open_tail_file};
use reader::ReaderFactory;
use writer::Writer;

pub(crate) const U64SIZE: usize = std::mem::size_of::<u64>();

pub type LogPosition = u64;

#[derive(Debug)]
pub enum OpenError {
    Io(Box<dyn std::error::Error + Send + Sync>),
    CorruptTailPosition,
    /// Indicates a corrupt or mismatched logfile relative to the tail position file.
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
    /// Indicates that there was a partial write before the database crashed or was terminated
    /// without the chance to shutdown. This partial item will be discarded.
    pub recovered: bool,
}

pub async fn open_log(path: impl AsRef<Path>) -> Result<OpenedLog, OpenError> {
    let path: Box<Path> = path.as_ref().into();
    let (tail_file, tail_pos) = open_tail_file(&path).await?;
    let (log_file, recovered) = open_log_file(&path, tail_pos).await?;

    let (tail_sender, tail_recv) = tokio::sync::watch::channel(tail_pos);

    Ok(OpenedLog {
        writer: Writer {
            log_file,
            tail_file,
            tail_sender,
            tail_pos,
        },
        reader_factory: ReaderFactory { path, tail_recv },
        recovered,
    })
}
