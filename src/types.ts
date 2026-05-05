export type DecodedValue =
  | { kind: "primitive"; type: "string" | "number" | "boolean" | "null"; value: string }
  | { kind: "bytes"; hex: string; length: number }
  | { kind: "object"; fields: { name: string; value: DecodedValue }[] }
  | { kind: "array"; items: DecodedValue[] };

export interface KafkaHeader {
  key: string;
  value: string;
}

export interface KafkaMessage {
  id: string;
  timestamp: string;
  topic: string;
  partition: number;
  offset: number;
  key: string | null;
  schemaName: string | null;
  schemaId: number | null;
  sizeBytes: number;
  headers: KafkaHeader[];
  payload: DecodedValue;
  rawHex: string;
}

export interface AppInfo {
  name: string;
  version: string;
  status: string;
}

export type ConnectionStatus = "disconnected" | "connecting" | "connected" | "error";

export interface ConnectionState {
  status: ConnectionStatus;
  cluster: string | null;
  topics: string[];
  error: string | null;
}

export interface CaptureStats {
  totalReceived: number;
  inBuffer: number;
  bufferCapacity: number;
  drops: number;
  throughputPerSec: number;
}

export interface ConnectArgs {
  bootstrapServers: string;
  topics: string[];
  fromBeginning: boolean;
}
