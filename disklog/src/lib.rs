mod tail;

use std::convert::TryInto;
use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use tail::{parse_tail_position, U64SIZE};

pub struct Writer {
    log_file: tokio::fs::File,
    tail_file: tokio::fs::File,
    tail_pos_sender: tokio::sync::watch::Sender<u64>,
    tail_pos: u64,
}

pub struct ReaderFactory {
    path: Box<Path>,
    tail_pos_recv: tokio::sync::watch::Receiver<u64>,
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

async fn open_tail_file(path: &Path) -> Result<(tokio::fs::File, u64), OpenError> {
    let tail_file_path = PathBuf::from(path).join("tail");
    tokio::fs::create_dir_all(
        tail_file_path
            .parent()
            .expect("Could not find parent directory"),
    )
    .await
    .map_err(|e| OpenError::Io(Box::new(e)))?;
    let mut tail_file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(tail_file_path)
        .await
        .map_err(|e| OpenError::Io(Box::new(e)))?;

    let mut contents = Vec::new();
    tail_file
        .read_to_end(&mut contents)
        .await
        .map_err(|e| OpenError::Io(Box::new(e)))?;

    let position: u64 = if contents.len() == 0 {
        let zero = 0u64;

        for _ in 0usize..3 {
            tail_file
                .write(&zero.to_le_bytes())
                .await
                .map_err(|e| OpenError::Io(Box::new(e)))?;
        }

        zero
    } else if contents.len() == U64SIZE * 3 {
        let bytes: [u8; U64SIZE * 3] = contents.as_slice().try_into().expect("Size was checked");
        parse_tail_position(bytes).ok_or(OpenError::CorruptTailPosition)?
    } else {
        Err(OpenError::CorruptTailPosition)?
    };
    Ok((tail_file, position))
}

async fn open_log_file(
    path: &Path,
    expected_tail_pos: u64,
) -> Result<(tokio::fs::File, bool), OpenError> {
    let log_file_path = PathBuf::from(path).join("log");
    tokio::fs::create_dir_all(
        log_file_path
            .parent()
            .expect("Could not find parent directory"),
    )
    .await
    .map_err(|e| OpenError::Io(Box::new(e)))?;
    let mut log_file = tokio::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(log_file_path)
        .await
        .map_err(|e| OpenError::Io(Box::new(e)))?;

    let actual_tail_pos = log_file
        .seek(tokio::io::SeekFrom::Current(0))
        .await
        .map_err(|e| OpenError::Io(Box::new(e)))?;

    if actual_tail_pos < expected_tail_pos {
        Err(OpenError::LogTooSmall)?;
    }

    Ok((log_file, actual_tail_pos > expected_tail_pos))
}

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
