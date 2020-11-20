use futures::future;
use tempdir::TempDir;
use tokio::prelude::*;
use tokio::sync::Barrier;

const FIRST_MSGS: [&str; 2] = ["Hello World", "Another Message"];
const LAST_MSG: &str = "Last Message!";

async fn writer_task(
    mut writer: disklog::writer::Writer,
    first_barrier: &Barrier,
    last_barrier: &Barrier,
) {
    for msg in FIRST_MSGS.iter() {
        writer.append(&mut msg.as_bytes()).await.unwrap();
    }

    first_barrier.wait().await;
    last_barrier.wait().await;

    writer.append(&mut LAST_MSG.as_bytes()).await.unwrap();
}

async fn reader_dont_wait(
    mut reader: disklog::reader::Reader,
    first_barrier: &Barrier,
    last_barrier: &Barrier,
) {
    first_barrier.wait().await;

    for msg in FIRST_MSGS.iter() {
        let mut item = reader.next(false).await.unwrap().unwrap();
        let mut contents = String::new();
        item.read_to_string(&mut contents).await.unwrap();
        assert_eq!(*msg, contents);
        reader = item.finish();
    }

    assert!(
        reader.next(false).await.unwrap().is_end(),
        "Should have reached end of reader"
    );

    last_barrier.wait().await;
}

async fn reader_wait(
    mut reader: disklog::reader::Reader,
    first_barrier: &Barrier,
    last_barrier: &Barrier,
) {
    first_barrier.wait().await;

    for msg in FIRST_MSGS.iter() {
        let mut item = reader.next(true).await.unwrap().unwrap();
        let mut contents = String::new();
        item.read_to_string(&mut contents).await.unwrap();
        assert_eq!(*msg, contents);
        reader = item.finish();
    }

    let task = future::maybe_done(reader.next(true));
    tokio::pin!(task);
    assert!(
        task.as_mut().take_output().is_none(),
        "Should be waiting for next item"
    );

    last_barrier.wait().await;

    {
        task.as_mut().await;
        let mut item = task.take_output().unwrap().unwrap().unwrap();
        let mut contents = String::new();
        item.read_to_string(&mut contents).await.unwrap();
        assert_eq!(LAST_MSG, contents);
        reader = item.finish();
    }

    {
        let task = future::maybe_done(reader.next(true));
        tokio::pin!(task);
        assert!(
            task.take_output().is_none(),
            "Should be waiting for next item"
        );
    }
}

#[tokio::test]
async fn read_and_write() {
    let temp_dir = TempDir::new("test-db").unwrap();

    let disklog::OpenedLog {
        writer,
        reader_factory,
        recovered,
    } = disklog::open_log(&temp_dir).await.unwrap();

    assert_eq!(recovered, false);

    let first_barrier = Barrier::new(3);
    let last_barrier = Barrier::new(3);

    let ((), (), ()) = tokio::join!(
        writer_task(writer, &first_barrier, &last_barrier),
        reader_dont_wait(
            reader_factory.read_from(0).await.unwrap(),
            &first_barrier,
            &last_barrier
        ),
        reader_wait(
            reader_factory.read_from(0).await.unwrap(),
            &first_barrier,
            &last_barrier
        ),
    );
}
