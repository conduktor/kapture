use thiserror::Error;

use crate::filter::FilterError;

#[derive(Debug, Error)]
pub enum KaptureError {
    #[error("kafka client error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("invalid configuration: {0}")]
    Config(String),

    #[error("already capturing")]
    AlreadyCapturing,

    #[error("not capturing")]
    NotCapturing,

    #[error("filter: {0}")]
    Filter(#[from] FilterError),
}

pub type Result<T> = std::result::Result<T, KaptureError>;

impl serde::Serialize for KaptureError {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
