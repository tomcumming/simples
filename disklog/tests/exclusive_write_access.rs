use tempdir::TempDir;

#[tokio::test]
async fn exclusive_write_access() {
    let temp_dir = TempDir::new("test-db").unwrap();

    let opened = disklog::open_log(&temp_dir).await.unwrap();

    match disklog::open_log(&temp_dir).await {
        Ok(_) => panic!("Opened log for writing twice!"),
        Err(disklog::OpenError::AlreadyOpen) => {}
        Err(_) => panic!("Second open failed for wrong reason"),
    };

    std::mem::drop(opened);

    // second open should succeed
    disklog::open_log(&temp_dir).await.unwrap();
}
