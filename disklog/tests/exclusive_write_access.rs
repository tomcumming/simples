use tempdir::TempDir;

#[tokio::test]
async fn exclusive_write_access() {
    let temp_dir = TempDir::new("test-db").unwrap();

    let disklog::OpenedLog {
        writer,
        reader_factory,
        recovered,
    } = disklog::open_log(&temp_dir).await.unwrap();

    if let Ok(_) = disklog::open_log(&temp_dir).await {
        panic!("Opened log for writing twice!");
    }

    // Ensure first writer is still alive
    std::mem::drop(writer);
}