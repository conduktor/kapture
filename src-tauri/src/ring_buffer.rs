use std::collections::VecDeque;

use parking_lot::RwLock;
use schemars::JsonSchema;
use serde::Serialize;

use crate::message::CapturedMessage;

/// Default per-buffer byte budget. Independent from the count cap —
/// whichever fills first triggers oldest-drop. Sized so a single
/// 1 MB-message stream cannot blow the process memory while still
/// holding ~256 messages of 1 MB or ~256 K of 1 KB.
pub const DEFAULT_BYTE_CAPACITY: usize = 256 * 1024 * 1024;

#[derive(Debug, Serialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CaptureStats {
    pub total_received: u64,
    pub in_buffer: usize,
    pub buffer_capacity: usize,
    pub buffer_bytes: usize,
    pub buffer_byte_capacity: usize,
    pub drops: u64,
    pub throughput_per_sec: f64,
    /// Drops/sec over the same rolling window as `throughput_per_sec`.
    /// Lets the UI tell hemorrhage (>0 sustained) from a one-shot byte
    /// cap eviction (spikes then 0). Computed by the stats emitter,
    /// which holds the prior `drops` baseline.
    pub drops_per_sec: f64,
}

#[derive(Debug)]
pub struct RingBuffer {
    inner: RwLock<RingState>,
}

#[derive(Debug)]
struct RingState {
    items: VecDeque<CapturedMessage>,
    capacity: usize,
    byte_capacity: usize,
    bytes: usize,
    drops: u64,
    total_received: u64,
}

impl RingBuffer {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self::with_byte_capacity(capacity, DEFAULT_BYTE_CAPACITY)
    }

    #[must_use]
    pub fn with_byte_capacity(capacity: usize, byte_capacity: usize) -> Self {
        Self {
            inner: RwLock::new(RingState {
                items: VecDeque::with_capacity(capacity.min(1024)),
                capacity,
                byte_capacity,
                bytes: 0,
                drops: 0,
                total_received: 0,
            }),
        }
    }

    pub fn push(&self, message: CapturedMessage) {
        let mut state = self.inner.write();
        state.total_received = state.total_received.saturating_add(1);
        let incoming = message.size_bytes;
        // Drop oldest until the new message fits both the count and
        // the byte budget. Both caps are enforced so a flood of small
        // messages cannot exceed the count cap, and a flood of large
        // messages cannot exceed the byte cap regardless of count.
        while state.items.len() >= state.capacity
            || (incoming <= state.byte_capacity
                && state.bytes.saturating_add(incoming) > state.byte_capacity)
        {
            if let Some(victim) = state.items.pop_front() {
                state.bytes = state.bytes.saturating_sub(victim.size_bytes);
                state.drops = state.drops.saturating_add(1);
            } else {
                break;
            }
        }
        // Pathological case: a single message larger than the byte
        // cap. Drop it on the floor — buffering it would evict
        // every other message.
        if incoming > state.byte_capacity {
            state.drops = state.drops.saturating_add(1);
            return;
        }
        state.bytes = state.bytes.saturating_add(incoming);
        state.items.push_back(message);
    }

    pub fn snapshot(&self) -> Vec<CapturedMessage> {
        let state = self.inner.read();
        state.items.iter().cloned().collect()
    }

    /// Look up a single message by id. `None` if it has aged out of
    /// the ring buffer. Cheap O(n) scan — n ≤ 100k and the buffer
    /// is in-memory; we don't bother with an id index.
    pub fn find_by_id(&self, id: &str) -> Option<CapturedMessage> {
        let state = self.inner.read();
        state.items.iter().find(|m| m.id == id).cloned()
    }

    /// Iterate the most recent matching messages without cloning the
    /// whole buffer. `keep` is called per message in newest-first
    /// order; iteration stops once `limit` messages have matched.
    /// Returns the matches in oldest-first (chronological) order.
    ///
    /// The read lock is held for the entire scan. This is a deliberate
    /// trade-off: holding the lock blocks pushes, but we visit at most
    /// `min(limit, ring_size)` messages and the per-message work is
    /// cheap (filter eval + clone). Pushes recover within microseconds
    /// even at full snapshot size.
    #[allow(clippy::significant_drop_tightening)]
    pub fn recent_filtered<F>(&self, limit: usize, mut keep: F) -> Vec<CapturedMessage>
    where
        F: FnMut(&CapturedMessage) -> bool,
    {
        let state = self.inner.read();
        let mut out: Vec<CapturedMessage> = Vec::with_capacity(limit.min(state.items.len()));
        for msg in state.items.iter().rev() {
            if out.len() >= limit {
                break;
            }
            if keep(msg) {
                out.push(msg.clone());
            }
        }
        out.reverse();
        out
    }

    pub fn clear(&self) {
        let mut state = self.inner.write();
        state.items.clear();
        state.bytes = 0;
    }

    pub fn stats(&self, throughput_per_sec: f64) -> CaptureStats {
        self.stats_with_drops_rate(throughput_per_sec, 0.0)
    }

    pub fn stats_with_drops_rate(
        &self,
        throughput_per_sec: f64,
        drops_per_sec: f64,
    ) -> CaptureStats {
        let state = self.inner.read();
        CaptureStats {
            total_received: state.total_received,
            in_buffer: state.items.len(),
            buffer_capacity: state.capacity,
            buffer_bytes: state.bytes,
            buffer_byte_capacity: state.byte_capacity,
            drops: state.drops,
            throughput_per_sec,
            drops_per_sec,
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
            topic_id: None,
            partition: 0,
            offset: 0,
            key: None,
            schema_name: None,
            schema_id: None,
            size_bytes: 0,
            key_size: 0,
            value_size: 0,
            headers: vec![],
            payload: DecodedValue::Bytes {
                hex: String::new(),
                length: 0,
            },
            raw_hex: String::new(),
            fetch: None,
            connection_id: None,
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
    fn drops_on_byte_cap() {
        // Each `msg()` has size_bytes = 0, so use a sized variant.
        fn sized(id: &str, bytes: usize) -> CapturedMessage {
            let mut m = msg(id);
            m.size_bytes = bytes;
            m
        }
        let buf = RingBuffer::with_byte_capacity(100, 1024);
        buf.push(sized("a", 600));
        buf.push(sized("b", 600));
        // 600 + 600 > 1024 → "a" evicted before "b" is admitted.
        let snap = buf.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].id, "b");
        let stats = buf.stats(0.0);
        assert_eq!(stats.drops, 1);
        assert_eq!(stats.buffer_bytes, 600);
    }

    #[test]
    fn drops_oversized_message() {
        let buf = RingBuffer::with_byte_capacity(100, 256);
        buf.push({
            let mut m = msg("huge");
            m.size_bytes = 1024;
            m
        });
        let snap = buf.snapshot();
        assert_eq!(snap.len(), 0);
        assert_eq!(buf.stats(0.0).drops, 1);
    }

    #[test]
    fn recent_filtered_iterates_lazily() {
        let buf = RingBuffer::new(10);
        for c in 'a'..='e' {
            buf.push(msg(&c.to_string()));
        }
        let last_two = buf.recent_filtered(2, |_| true);
        assert_eq!(
            last_two.iter().map(|m| m.id.clone()).collect::<Vec<_>>(),
            vec!["d", "e"]
        );
        let none_match = buf.recent_filtered(10, |_| false);
        assert!(none_match.is_empty());
    }

    #[test]
    fn clears() {
        let buf = RingBuffer::new(4);
        buf.push(msg("a"));
        buf.clear();
        assert_eq!(buf.snapshot().len(), 0);
    }
}
