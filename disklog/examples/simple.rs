use tokio::fs::OpenOptions;
use tokio::io::{self, AsyncWriteExt};
use tokio::prelude::*;

#[tokio::main]
async fn main() {
    let disklog::OpenedLog {
        writer,
        reader_factory,
        recovered,
    } = disklog::open_log("testdb").await.unwrap();
}
