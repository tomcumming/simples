use std::time::Duration;

use tempdir::TempDir;
use tokio::fs::OpenOptions;
use tokio::io::{self, AsyncWriteExt, ReadBuf};
use tokio::prelude::*;
use tokio::time;

async fn writer_task(mut writer: disklog::writer::Writer) {
    let first_msgs = ["Hello World", "Another Message"];

    for msg in first_msgs.iter() {
        let pos = writer.append(&mut msg.as_bytes()).await.unwrap();
        println!("Wrote message at {}: '{}'", pos, msg);
    }

    time::sleep(Duration::from_secs(2)).await;

    {
        let msg = "Last Message!";
        println!("Writing final message: '{}'", msg);
        let pos = writer.append(&mut msg.as_bytes()).await.unwrap();
    }
}

async fn reader_task(name: &'static str, mut reader: disklog::reader::Reader, wait_for_more: bool) {
    // Wait for writer to write first few messages...
    time::sleep(Duration::from_secs(1)).await;

    loop {
        match reader.next(wait_for_more).await.unwrap() {
            None => {
                break;
            }
            Some(mut item) => {
                let mut contents = String::new();
                item.read_to_string(&mut contents).await.unwrap();
                println!("Reader {} read {}: '{}'", name, item.position(), contents);
            }
        }
    }
    println!("Reader {} reached the end", name);
}

#[tokio::main]
async fn main() {
    let temp_dir = TempDir::new("test-db").unwrap();

    let disklog::OpenedLog {
        writer,
        reader_factory,
        recovered,
    } = disklog::open_log(&temp_dir).await.unwrap();

    assert_eq!(recovered, false);

    let ((), (), ()) = tokio::join!(
        writer_task(writer),
        reader_task("1", reader_factory.read_from(0).await.unwrap(), false),
        reader_task("2", reader_factory.read_from(0).await.unwrap(), true),
    );

    println!("Finished with {:?}", temp_dir);
}
