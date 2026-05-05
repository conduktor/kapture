//! Correlate raw `ProtoEvent`s emitted by the patched librdkafka with
//! the application-level messages they bring.
//!
//! The current implementation keeps the latest `Fetch` response seen on
//! each broker and a single "global latest" pointer. When a
//! `CapturedMessage` is built, we look up the global latest as an
//! approximation — sufficient for single-broker dev clusters and a
//! useful first pass for multi-broker production. Sharper
//! partition→broker mapping (via `Metadata` responses or rdkafka's
//! metadata API) is future work.
//!
//! Thread model: the trampoline in `proto_hook` runs on the librdkafka
//! broker thread and calls [`ProtoCorrelator::record_event`]
//! **synchronously** before returning. Because the same broker thread
//! then continues parsing the Fetch response and dispatching messages
//! to the consumer queue, by the time `consumer.recv()` returns a
//! message the correlator has already absorbed the corresponding RECV
//! event — there is no async race.

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::Serialize;

use crate::proto_hook::{ProtoDirection, ProtoEvent};

const FETCH_API_KEY: i32 = 1;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchMetadata {
    /// API key — always `1` (Fetch) today, kept for forward-compat.
    pub api_key: i32,
    pub api_name: &'static str,
    pub api_version: i32,
    pub broker_id: i32,
    pub corr_id: i32,
    pub response_size: usize,
    pub rtt_ms: f64,
}

#[derive(Debug, Default)]
pub struct ProtoCorrelator {
    state: RwLock<CorrelatorState>,
}

#[derive(Debug, Default)]
struct CorrelatorState {
    by_broker: HashMap<i32, FetchMetadata>,
    latest: Option<FetchMetadata>,
}

impl ProtoCorrelator {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a protocol event. Called synchronously from the
    /// librdkafka broker thread (via the proto-hook trampoline).
    pub fn record_event(&self, event: &ProtoEvent) {
        if event.api_key != FETCH_API_KEY {
            return;
        }
        if !matches!(event.direction, ProtoDirection::Recv) {
            return;
        }
        let meta = FetchMetadata {
            api_key: event.api_key,
            api_name: ProtoEvent::api_name(event.api_key),
            api_version: event.api_version,
            broker_id: event.broker_id,
            corr_id: event.corr_id,
            response_size: event.payload_size,
            rtt_ms: event.rtt_ms,
        };
        let mut state = self.state.write();
        state.by_broker.insert(event.broker_id, meta.clone());
        state.latest = Some(meta);
    }

    /// Approximation: returns the most-recent `Fetch` response across
    /// all brokers. The (topic, partition) inputs are accepted for
    /// future per-leader correlation but are unused today.
    #[must_use]
    pub fn lookup(&self, _topic: &str, _partition: i32) -> Option<FetchMetadata> {
        self.state.read().latest.clone()
    }

    /// Snapshot of the per-broker map. Useful for diagnostics; not yet
    /// surfaced to the UI.
    #[must_use]
    #[allow(dead_code)]
    pub fn per_broker(&self) -> HashMap<i32, FetchMetadata> {
        self.state.read().by_broker.clone()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::proto_hook::ProtoDirection;

    fn ev(direction: ProtoDirection, api_key: i32, broker_id: i32, rtt_ms: f64) -> ProtoEvent {
        ProtoEvent {
            direction,
            api_key,
            api_version: 11,
            corr_id: 42,
            broker_id,
            payload_size: 1024,
            rtt_ms,
        }
    }

    #[test]
    fn records_fetch_recv_only() {
        let c = ProtoCorrelator::new();
        c.record_event(&ev(ProtoDirection::Send, 1, 0, 0.0));
        assert!(c.lookup("orders.raw", 0).is_none());

        c.record_event(&ev(ProtoDirection::Recv, 3, 0, 1.0)); // Metadata, not Fetch
        assert!(c.lookup("orders.raw", 0).is_none());

        c.record_event(&ev(ProtoDirection::Recv, 1, 0, 2.5));
        let meta = c.lookup("orders.raw", 0).unwrap();
        assert_eq!(meta.broker_id, 0);
        assert!((meta.rtt_ms - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn updates_per_broker_and_latest() {
        let c = ProtoCorrelator::new();
        c.record_event(&ev(ProtoDirection::Recv, 1, 0, 1.0));
        c.record_event(&ev(ProtoDirection::Recv, 1, 1, 2.0));
        let map = c.per_broker();
        assert_eq!(map.len(), 2);
        let latest = c.lookup("any", 0).unwrap();
        assert_eq!(latest.broker_id, 1);
    }
}
