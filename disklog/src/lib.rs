mod tail;

use std::path::{Path, PathBuf};
use std::convert::TryInto;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use tail::{parse_tail_position, U64SIZE};

pub struct Disklog {
    log_file: tokio::fs::File,
    tail_file: tokio::fs::File,
}

#[derive(Debug)]
pub enum OpenError {
    Io(Box<dyn std::error::Error>),
    CorruptTailPosition,
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Io(e) => e.fmt(f),
            OpenError::CorruptTailPosition => write!(f, "Corrupt tail position"),
        }
    }
}

impl std::error::Error for OpenError {}

async fn open_tail_file(path: impl AsRef<Path>) -> Result<(tokio::fs::File, u64), OpenError> {
    let tail_file_path = PathBuf::from(path.as_ref()).join("tail");
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
        let bytes: [u8; U64SIZE * 3] = contents
            .as_slice()
            .try_into()
            .expect("Size was checked");
        parse_tail_position(bytes).ok_or(OpenError::CorruptTailPosition)?
    } else {
        Err(OpenError::CorruptTailPosition)?
    };
    Ok((tail_file, position))
}

async fn open_log_file(path: impl AsRef<Path>) -> Result<tokio::fs::File, OpenError> {
    let log_file_path = PathBuf::from(path.as_ref()).join("log");
    tokio::fs::create_dir_all(
        log_file_path
            .parent()
            .expect("Could not find parent directory"),
    )
    .await
    .map_err(|e| OpenError::Io(Box::new(e)))?;
    let log_file = tokio::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(log_file_path)
        .await
        .map_err(|e| OpenError::Io(Box::new(e)))?;
    Ok(log_file)
}

impl Disklog {
    pub async fn open(path: impl AsRef<Path>) -> Result<Disklog, OpenError> {
        let (tail_file, tail_pos) = open_tail_file(path.as_ref()).await?;
        let log_file = open_log_file(path).await?;

        Ok(Disklog {
            log_file,
            tail_file,
        })
    }
}
