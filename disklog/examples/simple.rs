use tokio::fs::OpenOptions;
use tokio::io::{self, AsyncWriteExt};
use tokio::prelude::*;

#[tokio::main]
async fn main() {
    disklog::Disklog::open("testdb").await.unwrap();
}
