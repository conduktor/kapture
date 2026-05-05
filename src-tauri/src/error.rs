use thiserror::Error;

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
