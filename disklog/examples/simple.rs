use tokio::fs::OpenOptions;
use tokio::io::{self, AsyncWriteExt};
use tokio::prelude::*;

#[tokio::main]
async fn main() {
    let disklog::OpenedLog {
        mut writer,
        reader_factory,
        recovered,
    } = disklog::open_log("testdb").await.unwrap();

    let mut reader: &[u8] = b"Hello World";

    writer.append(&mut reader).await;
}
