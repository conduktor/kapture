//! Cluster-wide map `topic_id (UUID) → topic_name` populated by
//! observing `MetadataResponse` traffic and consulted by the records
//! extractor when a `FetchResponse` v13+ omits the topic name on the
//! wire.
//!
//! Lives at the proxy level — topic IDs are cluster-scoped, not
//! connection-scoped — and is shared across every per-connection pump
//! task via `Arc<TopicIdMap>`.
//!
//! Lookups are best-effort: if a Fetch carries a `topic_id` we never
//! observed in a Metadata response (for example because the client
//! cached metadata before the proxy started), the records extractor
//! falls back to a `[topic-id <uuid>]` placeholder. Never panics.

use std::collections::HashMap;

use parking_lot::RwLock;
use tracing::warn;
use uuid::Uuid;

/// Cap on resolved topic-id entries. A malicious upstream could
/// advertise an unbounded `MetadataResponse`; cap defensively. 10k is
/// well above any realistic cluster — Kafka best-practice tops out at
/// ~4k topics per cluster — so this cap is effectively unreachable
/// for honest workloads while protecting against memory exhaustion.
pub const MAX_TOPIC_ID_ENTRIES: usize = 10_000;

/// Shared topic-id resolver.
///
/// `record` is called from the response rewriter (write-side, rare);
/// `lookup` is called from the records extractor (read-side, hot path
/// on Fetch). `RwLock` lets parallel pump tasks resolve in parallel.
pub struct TopicIdMap {
    inner: RwLock<HashMap<Uuid, String>>,
}

impl TopicIdMap {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Insert / update one mapping. Skips the nil UUID (used by
    /// kafka-protocol as the "absent `topic_id`" sentinel for
    /// pre-v10 Metadata / pre-v13 Fetch).
    ///
    /// Capped at `MAX_TOPIC_ID_ENTRIES`. New IDs past the cap are
    /// dropped with a warn-log; updates to existing IDs are always
    /// allowed (a topic getting deleted+recreated reuses the slot).
    pub fn record(&self, topic_id: Uuid, name: String) {
        if topic_id.is_nil() || name.is_empty() {
            return;
        }
        let mut guard = self.inner.write();
        // Update existing entry without growing the map (always allowed,
        // even past the cap — handles topic delete+recreate cleanly).
        if let Some(slot) = guard.get_mut(&topic_id) {
            *slot = name;
            return;
        }
        if guard.len() >= MAX_TOPIC_ID_ENTRIES {
            warn!(
                cap = MAX_TOPIC_ID_ENTRIES,
                topic_id = %topic_id,
                "TopicIdMap full; dropping new entry"
            );
            return;
        }
        guard.insert(topic_id, name);
    }

    /// Resolve `topic_id` to a topic name, if observed.
    #[must_use]
    pub fn lookup(&self, topic_id: Uuid) -> Option<String> {
        if topic_id.is_nil() {
            return None;
        }
        self.inner.read().get(&topic_id).cloned()
    }

    /// Snapshot for diagnostics (`proxy_smoke` prints the size; tests
    /// assert round-trip).
    #[must_use]
    pub fn snapshot(&self) -> Vec<(Uuid, String)> {
        self.inner
            .read()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Number of resolved entries. Cheap read-lock.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

impl Default for TopicIdMap {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for TopicIdMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicIdMap")
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn topic_id_map_record_then_lookup() {
        let map = TopicIdMap::new();
        let id = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
        map.record(id, "records-test".to_owned());
        assert_eq!(map.lookup(id).as_deref(), Some("records-test"));
    }

    #[test]
    fn topic_id_map_lookup_returns_none_for_unknown() {
        let map = TopicIdMap::new();
        let known = Uuid::from_u128(1);
        let unknown = Uuid::from_u128(2);
        map.record(known, "k".to_owned());
        assert!(map.lookup(unknown).is_none());
        // Nil UUID is rejected on lookup as well — never resolves.
        assert!(map.lookup(Uuid::nil()).is_none());
    }

    #[test]
    fn topic_id_map_caps_at_max_entries() {
        let map = TopicIdMap::new();
        // Fill to the cap with distinct UUIDs.
        for i in 0..MAX_TOPIC_ID_ENTRIES {
            map.record(Uuid::from_u128(i as u128 + 1), format!("t{i}"));
        }
        assert_eq!(map.len(), MAX_TOPIC_ID_ENTRIES);
        // One more new entry is dropped.
        let overflow = Uuid::from_u128((MAX_TOPIC_ID_ENTRIES + 100) as u128);
        map.record(overflow, "overflow".to_owned());
        assert_eq!(map.len(), MAX_TOPIC_ID_ENTRIES);
        assert!(map.lookup(overflow).is_none());
        // Updates to existing IDs are still allowed past the cap.
        let existing = Uuid::from_u128(1);
        map.record(existing, "renamed".to_owned());
        assert_eq!(map.lookup(existing).as_deref(), Some("renamed"));
        assert_eq!(map.len(), MAX_TOPIC_ID_ENTRIES);
    }

    #[test]
    fn topic_id_map_snapshot_round_trips() {
        let map = TopicIdMap::new();
        let a = Uuid::from_u128(0xAAAA);
        let b = Uuid::from_u128(0xBBBB);
        map.record(a, "alpha".to_owned());
        map.record(b, "beta".to_owned());
        // Nil UUID and empty name are silently dropped — never recorded.
        map.record(Uuid::nil(), "should-be-ignored".to_owned());
        map.record(Uuid::from_u128(0xCCCC), String::new());

        let mut snap = map.snapshot();
        snap.sort_by(|x, y| x.1.cmp(&y.1));
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0], (a, "alpha".to_owned()));
        assert_eq!(snap[1], (b, "beta".to_owned()));
        assert_eq!(map.len(), 2);
        assert!(!map.is_empty());
    }
}
