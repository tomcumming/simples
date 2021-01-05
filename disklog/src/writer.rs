use std::convert::TryInto;

use tokio::io::{AsyncRead, AsyncSeekExt, AsyncWriteExt};

use crate::checksum;
use crate::LogPosition;

pub struct Writer {
    pub(crate) log_file: tokio::fs::File,
    pub(crate) tail_file: tokio::fs::File,
    pub(crate) tail_sender: tokio::sync::watch::Sender<LogPosition>,
    pub(crate) tail_pos: u64,
}

#[derive(Debug)]
pub enum Error {
    Io(Box<dyn std::error::Error + Send + Sync>),
    /// Log items are limited to 2^32 bytes.
    ItemTooLarge,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => e.fmt(f),
            Error::ItemTooLarge => write!(f, "Item too large"),
        }
    }
}

impl std::error::Error for Error {}

impl Writer {
    async fn append_item<Contents: AsyncRead + Unpin>(
        &mut self,
        contents: &mut Contents,
    ) -> Result<LogPosition, Error> {
        self.log_file
            .seek(tokio::io::SeekFrom::Start(self.tail_pos))
            .await
            .map_err(|e| Error::Io(Box::new(e)))?;

        // Placeholders
        self.log_file
            .write_u16(0u16)
            .await
            .map_err(|e| Error::Io(Box::new(e)))?;
        self.log_file
            .write_u32(0u32)
            .await
            .map_err(|e| Error::Io(Box::new(e)))?;

        let bytes_written: u32 = tokio::io::copy(contents, &mut self.log_file)
            .await
            .map_err(|e| Error::Io(Box::new(e)))?
            .try_into()
            .map_err(|_| Error::ItemTooLarge)?;

        self.log_file
            .seek(tokio::io::SeekFrom::Start(self.tail_pos))
            .await
            .map_err(|e| Error::Io(Box::new(e)))?;

        let checksum = checksum::calculate(self.tail_pos, bytes_written);

        self.log_file
            .write_u16(checksum)
            .await
            .map_err(|e| Error::Io(Box::new(e)))?;
        self.log_file
            .write_u32(bytes_written)
            .await
            .map_err(|e| Error::Io(Box::new(e)))?;

        self.log_file
            .flush()
            .await
            .map_err(|e| Error::Io(Box::new(e)))?;

        Ok(self.tail_pos + 2 + 4 + (bytes_written as u64))
    }

    async fn write_tail_file(&mut self, new_tail_pos: LogPosition) -> Result<(), Error> {
        self.tail_file
            .seek(tokio::io::SeekFrom::Start(0))
            .await
            .map_err(|e| Error::Io(Box::new(e)))?;
        for _ in 0..3 {
            self.tail_file
                .write_u64(new_tail_pos)
                .await
                .map_err(|e| Error::Io(Box::new(e)))?;
        }
        self.tail_file
            .flush()
            .await
            .map_err(|e| Error::Io(Box::new(e)))?;
        Ok(())
    }

    pub async fn append<Contents: AsyncRead + Unpin>(
        &mut self,
        contents: &mut Contents,
    ) -> Result<LogPosition, Error> {
        let old_tail_pos = self.tail_pos;

        let new_tail_pos = self.append_item(contents).await?;
        self.write_tail_file(new_tail_pos).await?;

        self.tail_pos = new_tail_pos;

        self.tail_sender
            .send(new_tail_pos)
            .map_err(|e| Error::Io(Box::new(e)))?;

        Ok(old_tail_pos)
    }
}
