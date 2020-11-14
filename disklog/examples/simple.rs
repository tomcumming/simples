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

    let mut reader = reader_factory.read_from(0).await.unwrap();

    {
        let mut log_item = reader.next(false).await.unwrap().unwrap();
        println!("Reading item at pos {}", log_item.pos());
        let mut msg = String::new();
        log_item.read_to_string(&mut msg).await.unwrap();
        println!("Read '{:?}'", msg);
    }
    {
        let mut log_item = reader.next(false).await.unwrap().unwrap();
        println!("Reading item at pos {}", log_item.pos());
        let mut msg = String::new();
        log_item.read_to_string(&mut msg).await.unwrap();
        println!("Read '{:?}'", msg);
    }
}
