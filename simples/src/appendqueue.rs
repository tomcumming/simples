use hyper::Body;

use disklog::LogPosition;

use crate::error::BoxedError;
use crate::topicname::TopicName;

#[derive(Debug)]
pub struct AppendRequest {
    pub topic_name: TopicName,
    pub body: Body,
    pub result_sender: tokio::sync::oneshot::Sender<Result<LogPosition, BoxedError>>,
}

pub async fn handle_appends(mut queue: tokio::sync::mpsc::Receiver<AppendRequest>) {
    while let Some(req) = queue.recv().await {
        todo!("Handle incoming write")
    }
}
