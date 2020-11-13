mod checksum;
pub mod writer;

use std::convert::TryInto;
use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use writer::Writer;

pub const U64SIZE: usize = std::mem::size_of::<u64>();

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

async fn open_tail_file(path: &Path) -> Result<(tokio::fs::File, LogPosition), OpenError> {
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
        .read(true)
        .write(true)
        .open(tail_file_path)
        .await
        .map_err(|e| OpenError::Io(Box::new(e)))?;

    let mut contents = Vec::new();
    tail_file
        .read_to_end(&mut contents)
        .await
        .map_err(|e| OpenError::Io(Box::new(e)))?;

    let position: LogPosition = if contents.len() == 0 {
        let zero = 0u64;

        for _ in 0usize..3 {
            tail_file
                .write_u64(zero)
                .await
                .map_err(|e| OpenError::Io(Box::new(e)))?;
        }

        zero
    } else if contents.len() == U64SIZE * 3 {
        tail_file
            .seek(tokio::io::SeekFrom::Start(0))
            .await
            .map_err(|e| OpenError::Io(Box::new(e)))?;
        let first_pos = tail_file
            .read_u64()
            .await
            .map_err(|e| OpenError::Io(Box::new(e)))?;
        let second_pos = tail_file
            .read_u64()
            .await
            .map_err(|e| OpenError::Io(Box::new(e)))?;
        let third_pos = tail_file
            .read_u64()
            .await
            .map_err(|e| OpenError::Io(Box::new(e)))?;
        if first_pos == second_pos {
            first_pos
        } else if second_pos == third_pos {
            second_pos
        } else {
            Err(OpenError::CorruptTailPosition)?
        }
    } else {
        Err(OpenError::CorruptTailPosition)?
    };
    println!("position {} {:?}", position, contents);
    Ok((tail_file, position))
}

async fn open_log_file(
    path: &Path,
    expected_tail_pos: LogPosition,
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
        .write(true)
        .create(true)
        .open(log_file_path)
        .await
        .map_err(|e| OpenError::Io(Box::new(e)))?;

    let actual_tail_pos = log_file
        .seek(tokio::io::SeekFrom::End(0))
        .await
        .map_err(|e| OpenError::Io(Box::new(e)))?;

    println!("actual pos {}", actual_tail_pos);

    if actual_tail_pos < expected_tail_pos {
        println!("{} {}", actual_tail_pos, expected_tail_pos);
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
