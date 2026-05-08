import { useMemo, type JSX } from "react";

import { aggregateSession, type SessionState } from "../lib/sessionStats";
import type { DecodedFieldPair } from "../lib/protoFilter";
import type { ProtoFrame } from "../types";

interface Props {
  /**
   * Same `proto_frames` snapshot the Protocol + Brokers tabs consume.
   * Re-aggregated on every poll via `useMemo` — pure function, no
   * incrementalisation needed at the ring sizes we care about.
   */
  protoFrames: ProtoFrame[];
  /**
   * Switch to the Protocol tab and apply path-aware predicates that
   * scope it to the clicked entity. The caller (App.tsx) appends each
   * pair as a `decodedField` clause; multiple pairs in the same call
   * land in the same slot and OR within the kind, so a topic click
   * (which surfaces under `topics.name` / `topic_data.name` /
   * `topics.topic` depending on the RPC) matches any of those paths.
   * `frameId` pre-selects a specific row — used by the Errors list.
   */
  onJumpToProtocol: (predicates: DecodedFieldPair[], frameId?: string) => void;
}

/** Paths under which a topic name surfaces across the supported
 *  RPCs. ORed when the user clicks a topic in the table. */
const TOPIC_PATHS: readonly string[] = [
  "topics.name", // MetadataResponse, OffsetCommitRequest, FetchRequest v0..=12 (`name`)
  "topics.topic", // FetchRequest v0..=12 (`topic` field on FetchTopic)
  "topic_data.name", // ProduceRequest
  "responses.name", // ProduceResponse, FetchResponse
];

export function SessionActivityTab({ protoFrames, onJumpToProtocol }: Props): JSX.Element {
  const session: SessionState = useMemo(() => aggregateSession(protoFrames), [protoFrames]);

  if (protoFrames.length === 0) {
    return (
      <div className="session session--empty">
        <div className="session__empty">
          Waiting for protocol frames — connect a client to the proxy and watch this tab populate as
          it talks to Kafka.
        </div>
      </div>
    );
  }

  const topicNames = [...session.topics.keys()].sort();
  const groupIds = [...session.groups.keys()].sort();
  const errorCount = session.errors.length;

  return (
    <div className="session">
      <section className="session__header" aria-label="Session summary">
        <SummaryTile
          label="Client"
          value={
            session.client !== null
              ? `${session.client.software} ${session.client.version}`
              : "unknown"
          }
          hint={session.client === null ? "no ApiVersionsRequest v3+ seen yet" : undefined}
        />
        <SummaryTile
          label="Connections"
          value={String(session.connections.length)}
          hint={`${String(protoFrames.length)} frames total`}
        />
        <SummaryTile
          label="Topics"
          value={String(topicNames.length)}
          hint={topicNames.length > 0 ? topicNames.slice(0, 4).join(", ") : "—"}
        />
        <SummaryTile
          label="Groups"
          value={String(groupIds.length)}
          hint={groupIds.length > 0 ? groupIds.slice(0, 3).join(", ") : "—"}
        />
        <SummaryTile
          label="Errors"
          value={String(errorCount)}
          tone={errorCount > 0 ? "warn" : "ok"}
        />
      </section>

      <div className="session__columns">
        <section className="session__panel" aria-label="Topics">
          <h3 className="session__title">Topics</h3>
          {topicNames.length === 0 ? (
            <div className="session__placeholder">
              No topic names extracted yet — comes from MetadataResponse, ProduceRequest,
              FetchRequest.
            </div>
          ) : (
            <div
              className="session__table session__table--topics"
              role="table"
              aria-label="Topics seen this session"
            >
              <div className="session__row session__row--head" role="row">
                <div className="session__cell" role="columnheader">
                  Topic
                </div>
                <div
                  className="session__cell session__cell--center"
                  role="columnheader"
                  title="Seen in MetadataResponse"
                >
                  Meta
                </div>
                <div
                  className="session__cell session__cell--center"
                  role="columnheader"
                  title="Seen in ProduceRequest"
                >
                  Prod
                </div>
                <div
                  className="session__cell session__cell--center"
                  role="columnheader"
                  title="Seen in FetchRequest"
                >
                  Cons
                </div>
                <div
                  className="session__cell session__cell--center"
                  role="columnheader"
                  title="Errors mentioning this topic"
                >
                  Err
                </div>
              </div>
              {topicNames.map((name) => {
                const t = session.topics.get(name);
                if (t === undefined) {
                  return null;
                }
                return (
                  <button
                    key={name}
                    type="button"
                    className="session__row session__row--clickable"
                    role="row"
                    title={`Filter Protocol tab on "${name}"`}
                    onClick={() => {
                      onJumpToProtocol(TOPIC_PATHS.map((path) => ({ path, value: name })));
                    }}
                  >
                    <div className="session__cell session__cell--name" role="cell">
                      {name}
                    </div>
                    <Tick on={t.metadata} />
                    <Tick on={t.produced} />
                    <Tick on={t.consumed} />
                    <div
                      className={`session__cell session__cell--center${t.errorCount > 0 ? " session__cell--error" : ""}`}
                      role="cell"
                    >
                      {t.errorCount === 0 ? "—" : t.errorCount}
                    </div>
                  </button>
                );
              })}
            </div>
          )}

          {groupIds.length > 0 ? (
            <>
              <h3 className="session__title session__title--secondary">Consumer groups</h3>
              <div
                className="session__table session__table--groups"
                role="table"
                aria-label="Consumer groups seen this session"
              >
                <div className="session__row session__row--head" role="row">
                  <div className="session__cell" role="columnheader">
                    Group
                  </div>
                  <div
                    className="session__cell session__cell--center"
                    role="columnheader"
                    title="Latest generation seen"
                  >
                    Gen
                  </div>
                  <div
                    className="session__cell session__cell--center"
                    role="columnheader"
                    title="Members observed"
                  >
                    Mbrs
                  </div>
                  <div
                    className="session__cell session__cell--center"
                    role="columnheader"
                    title="JoinGroup requests (rebalance signal)"
                  >
                    Join
                  </div>
                  <div className="session__cell session__cell--center" role="columnheader">
                    HB
                  </div>
                  <div className="session__cell session__cell--center" role="columnheader">
                    Commits
                  </div>
                </div>
                {groupIds.map((id) => {
                  const g = session.groups.get(id);
                  if (g === undefined) {
                    return null;
                  }
                  return (
                    <button
                      key={id}
                      type="button"
                      className="session__row session__row--clickable"
                      role="row"
                      title={`Filter Protocol tab on "${id}"`}
                      onClick={() => {
                        onJumpToProtocol([{ path: "group_id", value: id }]);
                      }}
                    >
                      <div className="session__cell session__cell--name" role="cell">
                        {id}
                      </div>
                      <div className="session__cell session__cell--center" role="cell">
                        {g.generation ?? "—"}
                      </div>
                      <div className="session__cell session__cell--center" role="cell">
                        {g.members.size === 0 ? "—" : g.members.size}
                      </div>
                      <div className="session__cell session__cell--center" role="cell">
                        {g.joinCount}
                      </div>
                      <div className="session__cell session__cell--center" role="cell">
                        {g.heartbeatCount}
                      </div>
                      <div className="session__cell session__cell--center" role="cell">
                        {g.commitCount}
                      </div>
                    </button>
                  );
                })}
              </div>
            </>
          ) : null}
        </section>

        <section className="session__panel" aria-label="Errors">
          <h3 className="session__title">
            Errors{errorCount > 0 ? ` (${String(errorCount)})` : ""}
          </h3>
          {errorCount === 0 ? (
            <div className="session__placeholder">
              No protocol-level errors yet — top-level error codes on group RPC responses + commit
              responses are tracked here.
            </div>
          ) : (
            <ul className="session__errors">
              {[...session.errors].reverse().map((e) => (
                <li key={e.frameId} className="session__error">
                  <button
                    type="button"
                    className="session__error-row"
                    onClick={() => {
                      // Errors get no scoping filter — the Protocol
                      // list opens unfiltered with the offending frame
                      // pre-selected, so the user can read its full
                      // body without first guessing which path the
                      // error code lives at.
                      onJumpToProtocol([], e.frameId);
                    }}
                    title="Open this frame in the Protocol tab"
                  >
                    <span className="session__error-ts">{formatTime(e.ts)}</span>
                    <span className="session__error-api">{e.apiName}</span>
                    <span className="session__error-name">{e.errorName}</span>
                    <span className="session__error-code">({e.errorCode})</span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>
    </div>
  );
}

interface SummaryTileProps {
  label: string;
  value: string;
  hint?: string | undefined;
  tone?: "ok" | "warn" | undefined;
}

function SummaryTile({ label, value, hint, tone }: SummaryTileProps): JSX.Element {
  return (
    <div className={`session__tile${tone !== undefined ? ` session__tile--${tone}` : ""}`}>
      <div className="session__tile-label">{label}</div>
      <div className="session__tile-value">{value}</div>
      {hint !== undefined ? (
        <div className="session__tile-hint" title={hint}>
          {hint}
        </div>
      ) : null}
    </div>
  );
}

function Tick({ on }: { on: boolean }): JSX.Element {
  return (
    <div
      className={`session__cell session__cell--center session__cell--tick${on ? " is-on" : ""}`}
      role="cell"
      aria-label={on ? "yes" : "no"}
    >
      {on ? "●" : "·"}
    </div>
  );
}

/** Show the wall-clock time portion of an RFC3339 timestamp. The
 *  date is implicit (this session). Strips microseconds. */
function formatTime(ts: string): string {
  // Expected format: 2026-05-07T10:23:45.123456Z
  const t = ts.indexOf("T");
  if (t < 0) {
    return ts;
  }
  const dot = ts.indexOf(".", t);
  const end = dot < 0 ? ts.length : dot;
  return ts.slice(t + 1, end);
}
