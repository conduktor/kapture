//! Session-level aggregate maintained incrementally as protocol
//! frames flow through the correlator.
//!
//! The earlier design re-derived the aggregate from the proto frame
//! ring on every poll. That broke as soon as a frame aged out: a
//! single `ApiVersionsRequest` lives at most one connection's
//! worth of traffic into the ring, so the "Client: librdkafka 2.x"
//! hint reverted to "unknown" the moment the user generated enough
//! traffic to evict it. Same for topics and groups whose
//! advertising frame had scrolled out.
//!
//! Folding happens once per event from the typed
//! [`crate::proto_summary::FrameSummary`] projection. The state
//! persists for the lifetime of the proxy session — cleared on
//! `clear_capture` / `stop_proxy` (via `ProtoCorrelator::clear`).

use std::collections::{HashMap, HashSet, VecDeque};

use schemars::JsonSchema;
use serde::Serialize;

use crate::correlator::ProtoFrame;
use crate::proto_summary::FrameSummary;

/// Cap on the chronological errors deque. The full per-frame trail
/// stays in the proto ring; this is the at-a-glance summary in the
/// Session Activity tab.
const ERRORS_CAP: usize = 200;
const LATENCY_HISTOGRAM_BUCKETS: usize = 256;
const LATENCY_BUCKETS_PER_OCTAVE: f64 = 8.0;

#[derive(Debug, Default, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClientInfo {
    pub software: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInfo {
    pub local_port: u16,
    pub frame_count: u32,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicStats {
    pub name: String,
    /// Seen in at least one `MetadataResponse` topic list.
    pub metadata: bool,
    /// Seen as the target of at least one `ProduceRequest`.
    pub produced: bool,
    /// Seen as the target of at least one `FetchRequest`.
    pub consumed: bool,
    pub error_count: u32,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GroupStats {
    pub group_id: String,
    /// Member ids observed across `JoinGroupResponse` /
    /// `SyncGroupRequest` / `Heartbeat` / `OffsetCommit`.
    pub members: Vec<String>,
    /// Latest generation observed; `None` until the first
    /// (Sync|Heartbeat)Request lands.
    pub generation: Option<i32>,
    pub join_count: u32,
    pub heartbeat_count: u32,
    pub commit_count: u32,
    pub error_count: u32,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ErrorEvent {
    /// RFC 3339 timestamp lifted from the source frame.
    pub ts: String,
    pub frame_id: String,
    /// Full request/response form (`ProduceResponse`,
    /// `JoinGroupResponse`, ...).
    pub api_name: String,
    pub error_code: i16,
    /// Optional context — populated when the projection includes a
    /// `group_id` or topic. Today only `group_id` is wired.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LatencyStats {
    pub local_port: u16,
    pub api_key: i32,
    pub api_name: String,
    pub count: u64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
}

#[derive(Debug, Default, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionStats {
    pub client: Option<ClientInfo>,
    pub connections: Vec<ConnectionInfo>,
    pub topics: Vec<TopicStats>,
    pub groups: Vec<GroupStats>,
    pub errors: Vec<ErrorEvent>,
    pub latencies: Vec<LatencyStats>,
}

#[derive(Debug, Default)]
pub struct SessionFold {
    client: Option<ClientInfo>,
    /// `local_port → frame_count`.
    connections: HashMap<u16, u32>,
    topics: HashMap<String, TopicAcc>,
    groups: HashMap<String, GroupAcc>,
    errors: VecDeque<ErrorEvent>,
    /// Mergeable fixed-memory log histograms keyed by broker listener
    /// and API. Eight buckets per power-of-two octave (~9% steps).
    latencies: HashMap<(u16, i32), FixedLatencyHistogram>,
}

#[derive(Debug)]
struct FixedLatencyHistogram {
    buckets: Vec<u64>,
    count: u64,
    min_ms: f64,
    max_ms: f64,
    api_name: String,
}

impl FixedLatencyHistogram {
    fn new(api_name: String) -> Self {
        Self {
            buckets: vec![0; LATENCY_HISTOGRAM_BUCKETS],
            count: 0,
            min_ms: f64::INFINITY,
            max_ms: 0.0,
            api_name,
        }
    }

    fn record(&mut self, milliseconds: f64) {
        let micros = (milliseconds * 1_000.0).max(1.0);
        let raw = (micros.log2() * LATENCY_BUCKETS_PER_OCTAVE).floor();
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let index = (raw.max(0.0) as usize).min(LATENCY_HISTOGRAM_BUCKETS - 1);
        self.buckets[index] = self.buckets[index].saturating_add(1);
        self.count = self.count.saturating_add(1);
        self.min_ms = self.min_ms.min(milliseconds);
        self.max_ms = self.max_ms.max(milliseconds);
    }

    fn quantile(&self, quantile: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let target = ((self.count as f64 * quantile).ceil() as u64).max(1);
        let mut seen = 0u64;
        for (index, count) in self.buckets.iter().enumerate() {
            seen = seen.saturating_add(*count);
            if seen >= target {
                #[allow(clippy::cast_precision_loss)]
                let exponent = (index + 1) as f64 / LATENCY_BUCKETS_PER_OCTAVE;
                return exponent.exp2() / 1_000.0;
            }
        }
        self.max_ms
    }
}

impl Default for FixedLatencyHistogram {
    fn default() -> Self {
        Self::new(String::new())
    }
}

#[derive(Debug, Default)]
struct TopicAcc {
    metadata: bool,
    produced: bool,
    consumed: bool,
    error_count: u32,
}

#[derive(Debug, Default)]
struct GroupAcc {
    members: HashSet<String>,
    generation: Option<i32>,
    join_count: u32,
    heartbeat_count: u32,
    commit_count: u32,
    error_count: u32,
}

impl SessionFold {
    /// Fold one frame into the session aggregate. Called from
    /// `ProtoCorrelator::record_event` for every captured event,
    /// summary-or-not. Branches on the typed `FrameSummary` to update
    /// the right sub-aggregate; pure connection counters update
    /// regardless.
    #[allow(clippy::match_same_arms)]
    // The "*Response { error_code } => push_error_if_nonzero(...)"
    // arms are intentionally listed per-variant: they're interleaved
    // with the matching Request arms (which all do something
    // different), and the `OffsetCommitResponse` variant binds
    // `max_error_code` not `error_code`. Collapsing via OR-pattern
    // would either reorder the match or special-case OffsetCommit —
    // both worse than keeping the table flat.
    #[allow(clippy::too_many_lines)]
    pub fn absorb(&mut self, frame: &ProtoFrame, summary: Option<&FrameSummary>) {
        *self.connections.entry(frame.local_port).or_insert(0) += 1;
        if matches!(frame.direction, crate::proto_event::ProtoDirection::Recv) && frame.rtt_ms > 0.0
        {
            self.latencies
                .entry((frame.local_port, frame.api_key))
                .or_insert_with(|| FixedLatencyHistogram::new(frame.api_name.clone()))
                .record(frame.rtt_ms);
        }
        let Some(s) = summary else { return };
        match s {
            FrameSummary::ApiVersionsRequest {
                client_software_name,
                client_software_version,
            } => {
                if !client_software_name.is_empty() {
                    self.client = Some(ClientInfo {
                        software: client_software_name.clone(),
                        version: client_software_version.clone(),
                    });
                }
            }
            FrameSummary::MetadataResponse { topics, .. } => {
                for name in topics {
                    self.topics.entry(name.clone()).or_default().metadata = true;
                }
            }
            FrameSummary::ProduceRequest { topics, .. } => {
                for name in topics {
                    self.topics.entry(name.clone()).or_default().produced = true;
                }
            }
            // New summaries surfaced only for the anti-pattern detector.
            // SessionFold doesn't aggregate these — they intentionally
            // fall through with no work.
            FrameSummary::ApiVersionsResponse { .. }
            | FrameSummary::ProduceResponse { .. }
            | FrameSummary::InitProducerIdRequest { .. }
            | FrameSummary::AddPartitionsToTxnRequest { .. }
            | FrameSummary::EndTxnRequest { .. }
            | FrameSummary::FetchResponse { .. }
            | FrameSummary::MetadataRequest { .. }
            | FrameSummary::SaslAuthenticateResponse { .. } => {}
            FrameSummary::FetchRequest { topics, .. } => {
                for name in topics {
                    self.topics.entry(name.clone()).or_default().consumed = true;
                }
            }
            FrameSummary::FindCoordinatorRequest { keys } => {
                for k in keys {
                    self.groups.entry(k.clone()).or_default();
                }
            }
            FrameSummary::FindCoordinatorResponse { error_code, .. } => {
                self.push_error_if_nonzero(*error_code, frame, None);
            }
            FrameSummary::JoinGroupRequest { group_id, .. } => {
                self.groups.entry(group_id.clone()).or_default().join_count += 1;
            }
            FrameSummary::JoinGroupResponse { error_code, .. } => {
                self.push_error_if_nonzero(*error_code, frame, None);
            }
            FrameSummary::SyncGroupRequest {
                group_id,
                member_id,
                generation_id,
            } => {
                let g = self.groups.entry(group_id.clone()).or_default();
                if !member_id.is_empty() {
                    g.members.insert(member_id.clone());
                }
                g.generation = Some(*generation_id);
            }
            FrameSummary::SyncGroupResponse { error_code, .. } => {
                self.push_error_if_nonzero(*error_code, frame, None);
            }
            FrameSummary::HeartbeatRequest {
                group_id,
                member_id,
                generation_id,
            } => {
                let g = self.groups.entry(group_id.clone()).or_default();
                g.heartbeat_count += 1;
                if !member_id.is_empty() {
                    g.members.insert(member_id.clone());
                }
                g.generation = Some(*generation_id);
            }
            FrameSummary::HeartbeatResponse { error_code, .. } => {
                self.push_error_if_nonzero(*error_code, frame, None);
            }
            FrameSummary::LeaveGroupRequest { group_id } => {
                self.groups.entry(group_id.clone()).or_default();
            }
            FrameSummary::LeaveGroupResponse { error_code, .. } => {
                self.push_error_if_nonzero(*error_code, frame, None);
            }
            FrameSummary::OffsetCommitRequest {
                group_id,
                member_id,
                topics,
            } => {
                let g = self.groups.entry(group_id.clone()).or_default();
                g.commit_count += 1;
                if !member_id.is_empty() {
                    g.members.insert(member_id.clone());
                }
                for t in topics {
                    self.topics.entry(t.clone()).or_default();
                }
            }
            FrameSummary::OffsetCommitResponse { max_error_code, .. } => {
                self.push_error_if_nonzero(*max_error_code, frame, None);
            }
        }
    }

    fn push_error_if_nonzero(&mut self, code: i16, frame: &ProtoFrame, group_id: Option<String>) {
        if code == 0 {
            return;
        }
        if let Some(ref g) = group_id {
            if let Some(acc) = self.groups.get_mut(g) {
                acc.error_count += 1;
            }
        }
        self.errors.push_back(ErrorEvent {
            ts: frame.timestamp.clone(),
            frame_id: frame.id.clone(),
            api_name: frame.api_name.clone(),
            error_code: code,
            group_id,
        });
        while self.errors.len() > ERRORS_CAP {
            self.errors.pop_front();
        }
    }

    /// Cheap snapshot for the Tauri command. Maps → sorted Vecs so
    /// the frontend gets a deterministic order without re-sorting on
    /// every render.
    #[must_use]
    pub fn snapshot(&self) -> SessionStats {
        let mut connections: Vec<ConnectionInfo> = self
            .connections
            .iter()
            .map(|(local_port, frame_count)| ConnectionInfo {
                local_port: *local_port,
                frame_count: *frame_count,
            })
            .collect();
        connections.sort_by_key(|c| c.local_port);

        let mut topics: Vec<TopicStats> = self
            .topics
            .iter()
            .map(|(name, t)| TopicStats {
                name: name.clone(),
                metadata: t.metadata,
                produced: t.produced,
                consumed: t.consumed,
                error_count: t.error_count,
            })
            .collect();
        topics.sort_by(|a, b| a.name.cmp(&b.name));

        let mut groups: Vec<GroupStats> = self
            .groups
            .iter()
            .map(|(group_id, g)| {
                let mut members: Vec<String> = g.members.iter().cloned().collect();
                members.sort();
                GroupStats {
                    group_id: group_id.clone(),
                    members,
                    generation: g.generation,
                    join_count: g.join_count,
                    heartbeat_count: g.heartbeat_count,
                    commit_count: g.commit_count,
                    error_count: g.error_count,
                }
            })
            .collect();
        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));

        let mut latencies: Vec<LatencyStats> = self
            .latencies
            .iter()
            .map(|((local_port, api_key), histogram)| LatencyStats {
                local_port: *local_port,
                api_key: *api_key,
                api_name: histogram.api_name.clone(),
                count: histogram.count,
                min_ms: histogram.min_ms,
                max_ms: histogram.max_ms,
                p50_ms: histogram.quantile(0.50),
                p95_ms: histogram.quantile(0.95),
                p99_ms: histogram.quantile(0.99),
                p999_ms: histogram.quantile(0.999),
            })
            .collect();
        latencies.sort_by_key(|latency| (latency.local_port, latency.api_key));

        SessionStats {
            client: self.client.clone(),
            connections,
            topics,
            groups,
            errors: self.errors.iter().cloned().collect(),
            latencies,
        }
    }

    pub fn clear(&mut self) {
        self.client = None;
        self.connections.clear();
        self.topics.clear();
        self.groups.clear();
        self.errors.clear();
        self.latencies.clear();
    }
}
