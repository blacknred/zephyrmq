import crypto from "crypto";

export interface ITopicConfig {
  schema?: string; // registered schema` name
  persistThresholdMs?: number; // flush delay, 1000 default, if need ephemeral set Infinity
  retentionMs?: number; // 86_400_000 1 day
  maxSizeBytes?: number;
  maxDeliveryAttempts?: number;
  maxMessageSize?: number;
  maxMessageTTLMs?: number;
  ackTimeoutMs?: number; // e.g., 30_000
  consumerInactivityThresholdMs?: number; // 600_000;
  consumerProcessingTimeThresholdMs?: number;
  consumerPendingThresholdMs?: number;
  initialBackoffMs?: number; // e.g., 1000 ms
  maxBackoffMs?: number; // e.g., 30_000 ms
  deduplicationWindowMs?: number;
  encryptionkey?: crypto.CipherKey;
  // partitions?: number;
  // archivalThresholdMs?: number; // 100_000
}
