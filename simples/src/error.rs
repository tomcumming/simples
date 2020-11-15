pub type BoxedError = Box<dyn std::error::Error + Sync + Send>;

#[derive(Debug)]
pub enum Error {
    InvalidTopicName,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidTopicName => write!(f, "Invalid topic name"),
        }
    }
}

impl std::error::Error for Error {}
