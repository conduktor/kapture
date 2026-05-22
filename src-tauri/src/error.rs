use thiserror::Error;

use crate::filter::FilterError;
use crate::profiles::ProfileError;

#[derive(Debug, Error)]
pub enum KaptureError {
    #[error("invalid configuration: {0}")]
    Config(String),

    #[error("a proxy listener is already running")]
    AlreadyProxying,

    #[error("no proxy listener is running")]
    NotProxying,

    #[error("proxy: {0}")]
    Proxy(String),

    #[error("a JVM tap session is already running")]
    AlreadyJvmTapping,

    #[error("no JVM tap session is running")]
    NotJvmTapping,

    #[error("jvm-tap: {0}")]
    JvmTap(String),

    #[error("filter: {0}")]
    Filter(#[from] FilterError),

    #[error("profile: {0}")]
    Profile(#[from] ProfileError),
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
