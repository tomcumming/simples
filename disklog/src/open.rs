use std::path::{Path, PathBuf};

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{LogPosition, OpenError, U64SIZE};

async fn read_log_position(tail_file: &mut File) -> Result<LogPosition, OpenError> {
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
        Ok(first_pos)
    } else if second_pos == third_pos {
        Ok(second_pos)
    } else {
        Err(OpenError::CorruptTailPosition)
    }
}

pub async fn open_tail_file(path: &Path) -> Result<(tokio::fs::File, LogPosition), OpenError> {
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
        read_log_position(&mut tail_file).await?
    } else {
        Err(OpenError::CorruptTailPosition)?
    };
    Ok((tail_file, position))
}

pub async fn open_log_file(
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

    if actual_tail_pos < expected_tail_pos {
        Err(OpenError::LogTooSmall)?;
    }

    Ok((log_file, actual_tail_pos > expected_tail_pos))
}
