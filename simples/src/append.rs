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
