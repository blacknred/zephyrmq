![TypeScript](https://shields.io/badge/TypeScript-3178C6?logo=TypeScript&logoColor=FFF&style=flat-square)
![Node.js](https://shields.io/badge/Node.js-417e38?logo=nodedotjs&logoColor=FFF&style=flat-square)
[![Build Status](https://github.com/blcknrd/mbroker/workflows/Code%20quality%20checks/badge.svg)](https://github.com/blcknrd/mbroker/actions)

# Zodiak ⚡️⚡️⚡️

LIGHWEIGHT HIGH-PERFOMANCE EMBEDDED MESSAGE BROKER FRAMEWORK

Designed for high scalability and reliability within a Nodejs monolith/msa.

## Overview

### 🧠 High-Level

- `Publishing`: Validated messages are published to topics. Uses JSON schema precompilation validation with AJV
- `Consumption`: Consumers pull or subscribe (push) to messages from topics.
- `Routing`: Based on consumer groups, correlation IDs, and routing keys (semantic routing).
- `Queue fanout`: uses queue per consumer for message distribution. In contrary with virtual offsets it allows message priority, delay and other features.
- `Delivery guaranties`: Exactly-Once(deduplication, idempotent processing) while most message brokers are At-Least-Once(manual ack) by default.
- `Acknowledgment`(ACK/NACK): Ensures reliable message processing in noAck=false mode.
- `Dead Letter Queue` (DLQ): For failed or expired messages, support reading and replay
- `Delayed Message`(Time To Delay) Delivery: Messages can be scheduled to become available after a delay.
- `Message retention`(Time To Live): Expired messages go to DLQ
- `Consistent hashing`: Hash ring is used to distribute messages among consumers.
- `BinaryCodec`: Custom binary packing codec based on Buffer, fixed structure(metadata), bitflags for optional fields (no redundant data), precomputed Offsets. Codec provides max speed, min size/resource_usage within nodejs specific impl.
- `Persistence`: uses LevelDB (native cpp addon) for states and message metadata, and WAL+SegmentedLog for messages. Retention policies based on time.
- `Backpressure`: Inactive consumers will not be fanned out. In Consumer groups the idle one is preffered.
- `Metrics`: collects topic metrics

### 🔁 Data Flow Summary

1. Producer publishes a message to a topic.
2. The message is validated, encoded, and stored.
3. Publishing Service routes the message to consumers using:
   - Routing keys: 1m => x∈n consumers
     - consumers with routingKey get only messages with the same routingKey
     - consumers without routingKey get all messages
   - Consumer group membership: 1m => 1c per group
     - load is scalled horizontally and balanced among group members by delivering to the only one consumer
     - failover - if one fails, another takes over
     - use cases: Worker pools, job queues, ordered streams, command handlers
   - Correlation IDs: consistent message (sticky) processing (no race condition): 1m => 1c correlated
4. Consumers either:
   - Pull from queue or
   - Receive via push subscription
5. After processing:
   - Message must be acknowledged (ACK) in no AutoACK
   - If not ACK'd within timeout → NACK’d and optionally requeued
6. Failed messages go to DLQ .
7. Delayed messages are queued until their delay expires.

### ✅ Use Cases

- Event sourcing
- Task scheduling
- Microservices communication
- Real-time streaming
- Retryable job processing
- Delayed notifications

### 🛡️ Reliability Features

- Message persistence
- Backpressure handling
- Retries and DLQ
- Acknowledgment tracking
- Delayed delivery
- Client health monitoring

### 🧪 Scalability

- Consumer groups enable horizontal scaling.
- Hash ring ensures even distribution.
- Priority queues allow weighted message ordering.
- Threaded binary codec allows parallel encoding/decoding.

### 📊 Monitoring

- Per-topic and global metrics.
- Logs for all operations.
- Operability checks for clients.
- DLQ size and replay capability.

## 📌 Getting started

### ⚙️ Topic conf

- `schema`: Schema name for validation
- `persistThresholdMs`: Timeout for flush, use Infinity to skip persistence
- `retentionMs`: How long messages are retained
- `maxDeliveryAttempts`: Max retries before DLQ
- `maxMessageSize`: Max size of a single message
- `maxSizeBytes`: Max total size of topic
- `ackTimeoutMs`: Time before unacked messages are retried
- `consumerInactivityThresholdMs`: Timeout for inactive consumers
- `consumerProcessingTimeThresholdMs`: Max processing time for inactive consumers
- `consumerPendingThresholdMs`: Max pending acks for inactive consumers

## 🔁 Changelog

[CHANGELOG](CHANGELOG.md)

## ⚖️ License

Apache License 2.0 [License](LICENSE). Copyright blcknrd.
