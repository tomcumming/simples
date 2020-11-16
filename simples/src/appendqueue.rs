use std::collections::HashMap;
use std::path::{Path, PathBuf};

use hyper::Body;
use tokio::sync::mpsc;

use disklog::LogPosition;

use crate::error::BoxedError;
use crate::topicname::TopicName;

const QUEUE_SIZE: usize = 64; // TODO

#[derive(Debug)]
pub enum Error {
    Io(BoxedError),
    TopicDoesNotExist,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => e.fmt(f),
            Error::TopicDoesNotExist => write!(f, "Topic does not exist"),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug)]
pub struct AppendRequest {
    pub topic_name: TopicName,
    pub body: Body,
    pub result_sender: tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
}

#[derive(Debug)]
struct TopicAppendRequest {
    pub body: Body,
    pub result_sender: tokio::sync::oneshot::Sender<Result<LogPosition, Error>>,
}

async fn handle_queue(
    path: PathBuf,
    mut recv: mpsc::Receiver<TopicAppendRequest>,
) -> Result<(), BoxedError> {
    while let Some(req) = recv.recv().await {
        todo!("Handle queue for '{:?}'", path);
    }

    Ok(())
}

pub async fn handle_appends(mut queue: mpsc::Receiver<AppendRequest>) -> Result<(), BoxedError> {
    let mut tasks: Vec<tokio::task::JoinHandle<Result<(), BoxedError>>> = Vec::new();
    let mut queues = HashMap::<TopicName, mpsc::Sender<TopicAppendRequest>>::new();

    while let Some(req) = queue.recv().await {
        match queues.get(&req.topic_name) {
            Some(sender) => {
                let req = TopicAppendRequest {
                    body: req.body,
                    result_sender: req.result_sender,
                };
                sender.send(req).await?;
            }
            None => {
                let topic_path = Path::new("topics").join(req.topic_name.to_str());
                let metadata = tokio::fs::metadata(&topic_path).await;
                if metadata.is_ok() {
                    let (sender, recv) = mpsc::channel(QUEUE_SIZE);
                    let task = tokio::task::spawn(handle_queue(topic_path, recv));
                    tasks.push(task);
                    let topic_req = TopicAppendRequest {
                        body: req.body,
                        result_sender: req.result_sender,
                    };
                    sender.send(topic_req).await?;
                    queues.insert(req.topic_name, sender);
                } else {
                    if let Err(_e) = req.result_sender.send(Err(Error::TopicDoesNotExist)) {
                        println!("Client didnt wait for answer")
                    }
                }
            }
        }
    }

    for result in futures::future::join_all(tasks).await {
        result??;
    }

    Ok(())
}
