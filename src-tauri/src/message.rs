use serde::Serialize;

use crate::decode::DecodedValue;

#[derive(Debug, Serialize, Clone)]
pub struct KafkaHeader {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CapturedMessage {
    pub id: String,
    /// ISO-8601 timestamp.
    pub timestamp: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub schema_name: Option<String>,
    pub schema_id: Option<u32>,
    pub size_bytes: usize,
    pub headers: Vec<KafkaHeader>,
    pub payload: DecodedValue,
    /// Raw bytes rendered as space-separated hex.
    pub raw_hex: String,
}
