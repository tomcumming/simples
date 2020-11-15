use hyper::Body;

use disklog::LogPosition;

use crate::error::BoxedError;
use crate::topicname::TopicName;

pub struct WriteRequest {
    pub topic_name: TopicName,
    pub body: Body,
    pub result_sender: tokio::sync::oneshot::Sender<Result<LogPosition, BoxedError>>,
}

pub async fn handle_writes(mut queue: tokio::sync::mpsc::Receiver<WriteRequest>) {
    while let Some(req) = queue.recv().await {
        todo!("Handle incoming write")
    }
}
