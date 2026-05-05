use std::collections::VecDeque;

use parking_lot::RwLock;
use serde::Serialize;

use crate::message::CapturedMessage;

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CaptureStats {
    pub total_received: u64,
    pub in_buffer: usize,
    pub buffer_capacity: usize,
    pub drops: u64,
    pub throughput_per_sec: f64,
}

#[derive(Debug)]
pub struct RingBuffer {
    inner: RwLock<RingState>,
}

#[derive(Debug)]
struct RingState {
    items: VecDeque<CapturedMessage>,
    capacity: usize,
    drops: u64,
    total_received: u64,
}

impl RingBuffer {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: RwLock::new(RingState {
                items: VecDeque::with_capacity(capacity.min(1024)),
                capacity,
                drops: 0,
                total_received: 0,
            }),
        }
    }

    pub fn push(&self, message: CapturedMessage) {
        let mut state = self.inner.write();
        state.total_received = state.total_received.saturating_add(1);
        if state.items.len() >= state.capacity {
            state.items.pop_front();
            state.drops = state.drops.saturating_add(1);
        }
        state.items.push_back(message);
    }

    pub fn snapshot(&self) -> Vec<CapturedMessage> {
        let state = self.inner.read();
        state.items.iter().cloned().collect()
    }

    pub fn clear(&self) {
        let mut state = self.inner.write();
        state.items.clear();
    }

    pub fn stats(&self, throughput_per_sec: f64) -> CaptureStats {
        let state = self.inner.read();
        CaptureStats {
            total_received: state.total_received,
            in_buffer: state.items.len(),
            buffer_capacity: state.capacity,
            drops: state.drops,
            throughput_per_sec,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::DecodedValue;

    fn msg(id: &str) -> CapturedMessage {
        CapturedMessage {
            id: id.to_owned(),
            timestamp: "2026-05-05T12:00:00Z".to_owned(),
            topic: "t".to_owned(),
            partition: 0,
            offset: 0,
            key: None,
            schema_name: None,
            schema_id: None,
            size_bytes: 0,
            headers: vec![],
            payload: DecodedValue::Bytes {
                hex: String::new(),
                length: 0,
            },
            raw_hex: String::new(),
        }
    }

    #[test]
    fn drops_when_full() {
        let buf = RingBuffer::new(2);
        buf.push(msg("a"));
        buf.push(msg("b"));
        buf.push(msg("c"));
        let snap = buf.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].id, "b");
        let stats = buf.stats(0.0);
        assert_eq!(stats.drops, 1);
        assert_eq!(stats.total_received, 3);
    }

    #[test]
    fn clears() {
        let buf = RingBuffer::new(4);
        buf.push(msg("a"));
        buf.clear();
        assert_eq!(buf.snapshot().len(), 0);
    }
}
