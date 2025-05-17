import level, { LevelDB } from "level";
import crypto from "node:crypto";
import { EventEmitter } from "node:events";
import pino from "pino";
import { Counter, Gauge } from "prom-client";
import { wait, uniqueIntGenerator } from "./utils";
import fs from "fs/promises";
import path from "path";
import Buffer from "node:buffer";
import Ajv, { JSONSchemaType } from "ajv";

// logs & metrics
const bus = new EventEmitter();
const logger = pino({
  level: process.env.LOG_LEVEL || "info",
  transport:
    process.env.NODE_ENV === "development"
      ? { target: "pino-pretty" }
      : undefined,
  base: { service: "message-broker" },
});
bus.on("routed", (event: Metadata) =>
  logger.info(event, `Message routed to ${event.topic}.`)
);
bus.on("consumed", (event: Metadata[]) =>
  logger.info(event, `Message consumed from ${event[0]?.topic}.`)
);
bus.on("delayed", (event: Metadata) => logger.info(event, "Message delayed"));
bus.on("dlq", (event: Metadata, reason: string) =>
  logger.warn(event, `Routed ro DLQ. Reason: ${reason}.`)
);
//
// const topicDepth = new Counter({
//   name: "queue_depth",
//   help: "Current messages in queue",
//   labelNames: ["topic"],
// });
// const producerMessageCount = new Counter({
//   name: "producer_messages_total",
//   help: "Total messages sent by producer",
//   labelNames: ["topic", "producer_id"],
// });
// const consumerLag = new Gauge({
//   name: "consumer_pending_messages",
//   help: "Pending messages per consumer",
//   labelNames: ["topic", "consumer_id"],
// });
// bus.on("producerMessageCount", ({ topic, producerId: producer_id }) => {
//   producerMessageCount.inc({ topic, producer_id });
//   topicDepth.inc({ topic });
// });
// bus.on("consumerLag", ({ topic, onsumerId: consumer_id, lag }) => {
//   consumerLag.set({ topic, consumer_id }, lag);
// });
// class MetricsExporter {
//   private metricsProviders = new Map<string, () => Record<string, number>>();

//   register(topicName: string, getMetrics: () => Record<string, number>) {
//     this.metricsProviders.set(topicName, getMetrics);
//   }

//   start() {
//     setInterval(() => {
//       this.metricsProviders.forEach((getMetrics, topicName) => {
//         const lags = getMetrics(); // E.g., { "consumer-1": 5, "consumer-2": 10 }
//         Object.entries(lags).forEach(([consumerId, lag]) => {
//           consumerLagGauge.set({ topic: topicName, consumerId }, lag);
//         });
//       });
//     }, 15_000);
//   }
// }

// type QueueMetrics = {
//   depth: number;
//   enqueueRate: number; // Messages/sec
//   dequeueRate: number;
//   avgLatencyMs: number; // Time in queue
// };

// class BrokerMetrics {
//   private metrics: Record<string, QueueMetrics> = {};

//   update(topic: string, operation: 'enqueue' | 'dequeue', latencyMs: number) {
//     if (!this.metrics[topic]) {
//       this.metrics[topic] = { depth: 0, enqueueRate: 0, dequeueRate: 0, avgLatencyMs: 0 };
//     }
//     const m = this.metrics[topic];
//     m.depth += operation === 'enqueue' ? 1 : -1;
//     m[`${operation}Rate`] += 1;
//     m.avgLatencyMs = (m.avgLatencyMs * 0.9) + (latencyMs * 0.1); // Exponential moving average
//   }

//   getMetrics(): Readonly<Record<string, QueueMetrics>> {
//     return this.metrics;
//   }
// }
//   enqueue(topic: string, message: Message) {
//     const start = Date.now();
//     this.queues[topic].enqueue(Message.encode(message));
//     this.metrics.update(topic, 'enqueue', Date.now() - start);
//   }

//   dequeue(topic: string): Message | undefined {
//     const start = Date.now();
//     const data = this.queues[topic].dequeue();
//     if (data) {
//       this.metrics.update(topic, 'dequeue', Date.now() - start);
//       return Message.decode(data);
//     }
//   }
// {
//   "orders": {
//     "depth": 42,
//     "enqueueRate": 10,
//     "dequeueRate": 8,
//     "avgLatencyMs": 50
//   }
// }
///
//
//
//
//
//
// SRC/HASHER/
interface IHashService {
  hash(key: string): number;
}
class SHA256HashService implements IHashService {
  hash(key: string): number {
    const hex = crypto.createHash("sha256").update(key).digest("hex");
    return parseInt(hex.slice(0, 8), 16);
  }
}
interface IConsistencyHasher {
  addNode(id: number): void;
  removeNode(id: number): void;
  getNode(key: string): number | undefined;
}
/** Consistent hashing class.
 * The system works regardless of how different the key hashes are because the lookup is always
 * relative to the fixed node positions on the ring. Sorted nodes in a ring:
 * [**100(A)**, _180(user-123 key hash always belong to the B)_, **200(B)**, **300(A)**, **400(B)**, **500(A)**, **600(B)**]
 */
class InMemoryConsistencyHasher implements IConsistencyHasher {
  private ring = new Map<number, number>();

  /**
   * Create a new instance of ConsistencyHasher with the given number of virtual nodes.
   * @param {number} [replicas=3] The number of virtual nodes to create for each node.
   * The more virtual nodes gives you fewer hotspots, more balanced traffic. However, setting
   * this number too high can lead to a large memory footprint and slower lookups.
   */
  constructor(private hashService: IHashService, private replicas = 3) {}

  addNode(id: number) {
    for (let i = 0; i < this.replicas; i++) {
      const hash = this.hashService.hash(`${id}-${i}`);
      this.ring.set(hash, id);
    }
  }

  removeNode(id: number) {
    if (![...this.ring.values()].includes(id)) return;
    for (let i = 0; i < this.replicas; i++) {
      const hash = this.hashService.hash(`${id}-${i}`);
      this.ring.delete(hash);
    }
  }

  getNode(key: string): number | undefined {
    const keyHash = this.hashService.hash(key);
    const sortedHashes = Array.from(this.ring.keys()).sort((a, b) => a - b);
    const node = sortedHashes.find((h) => h >= keyHash) || sortedHashes[0];
    return this.ring.get(node);
  }
}
interface IRoutingStrategy {
  getConsumerId(key: string): number;
  addConsumer(id: number): void;
  removeConsumer(id: number): void;
}
class ConsistentHashingStrategy implements IRoutingStrategy {
  constructor(private hasher: IConsistencyHasher) {}

  getConsumerId(key: string) {
    return this.hasher.getNode(key)!;
  }
  addConsumer(id: number) {
    return this.hasher.addNode(id);
  }
  removeConsumer(id: number) {
    return this.hasher.removeNode(id);
  }
}
//
//
//
//
// SRC/STORAGE/TYPES.TS
// interface IStorage {
//   put(topic: string, id: string, record: Buffer): Promise<void>;
//   get(topic: string, id: string): Promise<Buffer | undefined>;
//   delete(topic: string, id: string): Promise<void>;
//   stream(topic: string): AsyncIterable<{ id: string; record: Buffer }>;
// }
// interface IStorageConfig {
//   path: string;
//   compression?: boolean;
//   compressionThreasholdBytes?: number;
//   wal?: boolean;
// }
// SRC/STORAGE/LEVELDB.STORAGE.TS
// class LevelDBStorage implements IStorage {
//   private db: LevelDB;

//   constructor(private config: IStorageConfig) {
//     this.db = level(this.config.path, { valueEncoding: "binary" });
//   }

//   private shouldCompress(record: Buffer) {
//     if (!this.config.compression) return false;
//     if (!this.config.compressionThreasholdBytes) return true;
//     return this.config.compressionThreasholdBytes <= record.byteLength;
//   }

//   async put(topic: string, id: string, record: Buffer): Promise<void> {
//     const data = this.shouldCompress(record)
//       ? await snappy.compress(record)
//       : record;
//     await this.db.put(`${topic}!${id}`, data);
//   }

//   async get(topic: string, id: string): Promise<Buffer | undefined> {
//     try {
//       const record = await this.db.get(`${topic}!${id}`);
//       return record ? snappy.uncompress(record) : undefined;
//     } catch (err) {
//       if (err.notFound) return undefined;
//       throw err;
//     }
//   }

//   async delete(topic: string, id: string): Promise<void> {
//     await this.db.del(`${topic}!${id}`);
//   }

//   async *stream(topic: string): AsyncIterable<{ id: string; record: Buffer }> {
//     const stream = this.db.createReadStream({
//       gt: `${topic}!`,
//       lt: `${topic}!\xff`,
//     });

//     for await (const { key, value } of stream) {
//       const id = key.slice(topic.length + 1);
//       yield { id, record: value };
//     }
//   }
// }
// SRC/STORAGE/WAL_MANAGER.TS
// class WALManager {
//   private walFile: string;
//   private writeQueue: Promise<void> = Promise.resolve();

//   constructor(storagePath: string) {
//     this.walFile = path.join(storagePath, "write-ahead.log");
//     this.recover();
//   }

//   private async recover() {
//     try {
//       const log = await fs.readFile(this.walFile, "utf8");
//       const pendingOps = log.split("\n").filter(Boolean);
//       await Promise.all(pendingOps.map((op) => this.replayOperation(op)));
//       await fs.truncate(this.walFile);
//     } catch (err) {
//       if (err.code !== "ENOENT") throw err;
//     }
//   }

//   private async replayOperation(op: string) {
//     const [action, key, value] = op.split("|");
//     // Implement replay logic for your storage
//   }

//   logOperation(op: string): Promise<void> {
//     this.writeQueue = this.writeQueue
//       .then(() => fs.appendFile(this.walFile, `${op}\n`))
//       .catch(() => {});
//     return this.writeQueue;
//   }
// }
// // SRC/STORAGE/SAFE_LEVELDB.STORAGE.TS
// class SafeLevelDBStorage extends LevelDBStorage {
//   private wal: WALManager;

//   constructor(path: string) {
//     super(path);
//     this.wal = new WALManager(path);
//   }

//   async put(topic: string, id: string, record: Buffer): Promise<void> {
//     await this.wal.logOperation(`PUT|${topic}!${id}|${record.toString("hex")}`);
//     await super.put(topic, id, record);
//     await this.wal.logOperation(`COMMIT|${topic}!${id}`);
//   }
// }
//
//
//
//
// SRC/METADATA.TS
class Metadata {
  // Fixed-width fields
  id: number; // 4 bytes
  ts: number; // 8 bytes (double)
  producerId: number; // 4 bytes
  priority?: number; // 1 byte (0-255)
  ttl?: number; // 4 bytes
  ttd?: number; // 4 bytes
  batchId?: number; // 4 bytes
  batchIdx?: number; // 2 bytes
  batchSize?: number; // 2 bytes
  attempts: number; // 1 byte
  consumedAt?: number; // 8 bytes

  // Variable-width fields
  topic: string;
  correlationId?: string;

  // Bit flags for optional fields (1 byte)
  get flags(): number {
    return (
      (this.priority !== undefined ? 0x01 : 0) |
      (this.ttl !== undefined ? 0x02 : 0) |
      (this.ttd !== undefined ? 0x04 : 0) |
      (this.batchId !== undefined ? 0x08 : 0) |
      (this.correlationId !== undefined ? 0x10 : 0)
    );
  }

  get keys(): Exclude<keyof Metadata, "flags" | "keys">[] {
    return [
      "id",
      "ts",
      "topic",
      "producerId",
      "correlationId",
      "priority",
      "ttl",
      "ttd",
      "batchId",
      "batchIdx",
      "batchSize",
      "attempts",
      "consumedAt",
    ];
  }
}
//
//
//
// SRC/CODECS/
interface ICodec {
  encode<T>(data: T, meta: Metadata): Buffer;
  decode<T>(buffer: Buffer): [T, Metadata];
}
class JSONCodec implements ICodec {
  encode<T>(data: T, meta: Metadata) {
    try {
      // 1. Build the complete object
      const message = { "0": data };
      meta.keys.forEach((k, i) => {
        if (meta[k] !== undefined) message[i + 1] = meta[k];
      });

      // 2. Pre-allocation reduces GC pressure
      const jsonString = JSON.stringify(message);
      const buffer = Buffer.allocUnsafe(Buffer.byteLength(jsonString));

      // 3. Single write operation
      buffer.write(jsonString, 0, "utf8");
      return buffer;
    } catch (e) {
      throw new Error("Failed to encode message");
    }
  }

  decode<T>(buffer: Buffer): [T, Metadata] {
    try {
      // 1. Fast path for empty/small buffers
      if (buffer.length < 2) throw new Error("Invalid message");

      // 2. Single string conversion
      const str =
        buffer.length < 4096
          ? buffer.toString("utf8") // Small buffers
          : Buffer.prototype.toString.call(buffer, "utf8"); // Large buffers avoids prototype lookup

      // 3. Parse with reviver for direct metadata mapping
      const meta = new Metadata();
      const data = JSON.parse(str, (k, v: number | string) => {
        if (k === "0") return v; // Return payload as-is
        const metaIndex = parseInt(k, 10) - 1;
        if (!isNaN(metaIndex)) {
          const metaKey = meta.keys[metaIndex];
          if (metaKey) meta[metaKey] = v;
        }
        return;
      }) as T;

      return [data, meta];
    } catch (e) {
      throw new Error("Failed to decode message");
    }
  }
}
// Efficient codec.
// this code vs json: - 40-60% ram, ~3x faster encode & ~5x faster decode than JSON
// this codec(+ajv precompiled validation which is 2x faster) vs protobuf(validation+encoding): 2x faster, 10% less size, 40% less ram; but no cross-lang, no schema-evolution, no faster for messages with mostly optional keys
class BinaryCodec implements ICodec {
  encode<T>(data: T, meta: Metadata): Buffer {
    // Serialize payload first to determine size
    const payloadJson = JSON.stringify(data);
    const payloadBuf = Buffer.from(payloadJson);

    // Calculate string sizes
    const topicBuf = Buffer.from(meta.topic, "utf8");
    const corrIdBuf = meta.correlationId
      ? Buffer.from(meta.correlationId, "utf8")
      : Buffer.alloc(0);

    // Allocate buffer (46B fixed + vars + payload)
    const buffer = Buffer.allocUnsafe(
      46 + topicBuf.length + corrIdBuf.length + payloadBuf.length
    );

    let offset = 0;

    // --- Metadata Header (46 bytes fixed) ---
    buffer.writeUInt32BE(meta.id, offset);
    offset += 4; // 4B
    buffer.writeDoubleBE(meta.ts, offset);
    offset += 8; // 8B
    buffer.writeUInt32BE(meta.producerId, offset);
    offset += 4; // 4B
    buffer.writeUInt8(meta.flags, offset);
    offset += 1; // 1B

    // Optional fixed fields
    if (meta.priority !== undefined) {
      buffer.writeUInt8(meta.priority, offset);
      offset += 1; // 1B
    }
    if (meta.ttl !== undefined) {
      buffer.writeUInt32BE(meta.ttl, offset);
      offset += 4; // 4B
    }
    if (meta.ttd !== undefined) {
      buffer.writeUInt32BE(meta.ttd, offset);
      offset += 4; // 4B
    }
    if (meta.batchId !== undefined) {
      buffer.writeUInt32BE(meta.batchId, offset);
      offset += 4; // 4B
      buffer.writeUInt16BE(meta.batchIdx!, offset);
      offset += 2; // 2B
      buffer.writeUInt16BE(meta.batchSize!, offset);
      offset += 2; // 2B
    }

    buffer.writeUInt8(meta.attempts, offset);
    offset += 1; // 1B

    if (meta.consumedAt !== undefined) {
      buffer.writeDoubleBE(meta.consumedAt, offset);
      offset += 8; // 8B
    }

    // --- Variable-Length Fields ---
    // Topic (length-prefixed)
    buffer.writeUInt8(topicBuf.length, offset);
    offset += 1;
    topicBuf.copy(buffer, offset);
    offset += topicBuf.length;

    // Correlation ID (if exists)
    if (meta.correlationId) {
      buffer.writeUInt8(corrIdBuf.length, offset);
      offset += 1;
      corrIdBuf.copy(buffer, offset);
      offset += corrIdBuf.length;
    }

    // --- Payload ---
    buffer.writeUInt32BE(payloadBuf.length, offset);
    offset += 4;
    payloadBuf.copy(buffer, offset);

    return buffer;
  }

  decode<T>(buffer: Buffer): [T, Metadata] {
    const meta = new Metadata();
    let offset = 0;

    // --- Metadata Header ---
    meta.id = buffer.readUInt32BE(offset);
    offset += 4;
    meta.ts = buffer.readDoubleBE(offset);
    offset += 8;
    meta.producerId = buffer.readUInt32BE(offset);
    offset += 4;
    const flags = buffer.readUInt8(offset);
    offset += 1;

    // Optional fixed fields
    if (flags & 0x01) meta.priority = buffer.readUInt8(offset++);
    if (flags & 0x02) meta.ttl = buffer.readUInt32BE(offset);
    offset += 4;
    if (flags & 0x04) meta.ttd = buffer.readUInt32BE(offset);
    offset += 4;
    if (flags & 0x08) {
      meta.batchId = buffer.readUInt32BE(offset);
      offset += 4;
      meta.batchIdx = buffer.readUInt16BE(offset);
      offset += 2;
      meta.batchSize = buffer.readUInt16BE(offset);
      offset += 2;
    }

    meta.attempts = buffer.readUInt8(offset++);

    if (flags & 0x10) {
      meta.consumedAt = buffer.readDoubleBE(offset);
      offset += 8;
    }

    // --- Variable-Length Fields ---
    // Topic
    const topicLen = buffer.readUInt8(offset++);
    meta.topic = buffer.toString("utf8", offset, offset + topicLen);
    offset += topicLen;

    // Correlation ID
    if (flags & 0x20) {
      const corrIdLen = buffer.readUInt8(offset++);
      meta.correlationId = buffer.toString("utf8", offset, offset + corrIdLen);
      offset += corrIdLen;
    }

    // --- Payload ---
    const payloadLen = buffer.readUInt32BE(offset);
    offset += 4;
    const payload = buffer.toString("utf8", offset, offset + payloadLen);
    const data = JSON.parse(payload) as T;

    return [data, meta];
  }
}
//
//
//
//
// SRC/CONTEXT.TS
interface IPriorityQueue<Data = any> {
  enqueue(data: Data, priority?: number): void;
  dequeue(): Data | undefined;
  peek(): Data | undefined;
  isEmpty(): boolean;
  size(): number;
}
interface IContext {
  codec: ICodec;
  eventBus: EventEmitter;
  queueFactory: () => IPriorityQueue<Buffer>;
  // validator?: IValidator<any>;
}
class Context implements IContext {
  constructor(
    public codec: ICodec,
    public eventBus: EventEmitter,
    public queueFactory: () => IPriorityQueue<Buffer>
  ) {}
}
//
//
//
//
// SRC/PIPELINE/PROCESSOR.TS
interface IMessageProcessor {
  process(record: Buffer, meta: Metadata): boolean;
}
// SRC/PIPELINE/PROCESSORS/
class ExpirationProcessor implements IMessageProcessor {
  constructor(private dlq: TopicDLQManager) {}
  process(record: Buffer, meta: Metadata): boolean {
    if (!meta.ttl) return false;
    const isExpired = meta.ts + meta.ttl <= Date.now();
    if (isExpired) this.dlq.send(record, meta, "expired");
    return isExpired;
  }
}
class AttemptsProcessor implements IMessageProcessor {
  constructor(private dlq: TopicDLQManager, private maxAttempts: number) {}
  process(record: Buffer, meta: Metadata): boolean {
    const shouldDeadLetter = meta.attempts > this.maxAttempts;
    if (shouldDeadLetter) {
      this.dlq.send(record, meta, "max_attempts");
    }
    return shouldDeadLetter;
  }
}
class DelayProcessor implements IMessageProcessor {
  constructor(private delayedQueue: TopicDelayedQueueManager) {}
  process(record: Buffer, meta: Metadata): boolean {
    const shouldDelay = !!meta.ttd && meta.ttd > Date.now();
    if (shouldDelay) this.delayedQueue.send(record, meta);
    return shouldDelay;
  }
}
// SRC/PIPELINE/MESSAGE_PIPELINE.TS
class MessagePipeline {
  private processors: IMessageProcessor[] = [];
  addProcessor(processor: IMessageProcessor): void {
    this.processors.push(processor);
  }
  async process(record: Buffer, meta: Metadata): Promise<boolean> {
    for (const processor of this.processors) {
      if (processor.process(record, meta)) return true;
    }
    return false;
  }
}
// SRC/PIPELINE/PIPELINE_FACTORY.TS
class PipelineFactory {
  create(
    dlqManager: TopicDLQManager,
    delayedQueue: TopicDelayedQueueManager,
    maxAttempts?: number
  ): MessagePipeline {
    const pipeline = new MessagePipeline();
    pipeline.addProcessor(new ExpirationProcessor(dlqManager));
    pipeline.addProcessor(new DelayProcessor(delayedQueue));
    if (maxAttempts !== undefined) {
      pipeline.addProcessor(new AttemptsProcessor(dlqManager, maxAttempts));
    }

    return pipeline;
  }
}
//
//
//
//
// SRC/VALIDATION/VALIDATOR_REGISTRY.TS
class ValidatorRegistry {
  private validators = new Map<string, (data: any) => boolean>();
  private ajv: Ajv;

  constructor(options?: Ajv.Options) {
    this.ajv = new Ajv({
      allErrors: true,
      coerceTypes: false,
      useDefaults: true,
      code: { optimize: true, esm: true },
      ...options,
    });
  }

  register<Data>(topic: string, schema: JSONSchemaType<Data>): void {
    if (this.validators.has(topic)) {
      throw new Error(`Schema already exists for topic ${topic}`);
    }

    const validate = this.ajv.compile(schema);
    this.validators.set(topic, (data) => {
      const valid = validate(data);
      if (!valid) {
        throw new Error(
          `Validation failed: ${this.ajv.errorsText(validate.errors)}`
        );
      }
      return true;
    });
  }

  get(topic: string): ((data: any) => boolean) | undefined {
    return this.validators.get(topic);
  }

  remove(topic: string): void {
    this.validators.delete(topic);
  }
}
//
//
//
//
// SRC/TOPICS/METRICS.TS
interface ITopicMetadata {
  name: string;
  createdAt: string;
  pendingMessages: number;
  dlqSize: number;
  delayedQueueSize: number;
  producersCount: number;
  producersIds: number[];
  totalMessagesPublished: number;
  consumerCount: number;
  consumerIds: number[];
}
class TopicMetricsCollector {
  private totalMessagesPublished = 0;
  private ts = Date.now();
  constructor(private eventBus: EventEmitter) {}

  recordMessagePublished(meta: Metadata): void {
    this.totalMessagesPublished++;
    this.eventBus.emit("routed", { meta });
  }

  getMetrics(): Partial<ITopicMetadata> {
    return {
      createdAt: new Date(this.ts).toISOString(),
      totalMessagesPublished: this.totalMessagesPublished,
    };
  }
}
// SRC/TOPICS/MEMBERSHIP_MANAGER.TS
interface IClientMetadata {
  lastActiveAt: number;
  messageCount?: number;
  pendingMessages?: number;
}
class TopicMembershipManager {
  private producers = new Map<number, IClientMetadata>();
  private consumers = new Map<number, IClientMetadata>();
  constructor(private eventBus: EventEmitter) {}

  addProducer(id: number): void {
    this.producers.set(id, { lastActiveAt: Date.now() });
    this.eventBus.emit("producerAdded", { producerId: id });
  }

  removeProducer(id: number): void {
    this.producers.delete(id);
    this.eventBus.emit("producerRemoved", { producerId: id });
  }

  addConsumer(id: number): void {
    this.consumers.set(id, { lastActiveAt: Date.now() });
    this.eventBus.emit("consumerAdded", { consumerId: id });
  }

  removeConsumer(id: number): void {
    this.consumers.delete(id);
    this.eventBus.emit("consumerRemoved", { consumerId: id });
  }

  getMembership(): { producers: number[]; consumers: number[] } {
    return {
      producers: Array.from(this.producers.keys()),
      consumers: Array.from(this.consumers.keys()),
    };
  }
}
// SRC/TOPICS/QUEUE_MANAGER.TS
class TopicQueueManager {
  private sharedQueue: IPriorityQueue;
  private unicastQueues = new Map<number, IPriorityQueue>();
  constructor(
    private routingStrategy: IRoutingStrategy,
    private queueFactory: () => IPriorityQueue<Buffer>
  ) {
    this.sharedQueue = this.queueFactory();
  }

  getQueue(key?: string): IPriorityQueue {
    if (key) {
      const targetConsumer = this.routingStrategy.getConsumerId(key);
      return this.unicastQueues.get(targetConsumer) || this.sharedQueue;
    }
    return this.sharedQueue;
  }

  addConsumerQueue(consumerId: number): void {
    this.unicastQueues.set(consumerId, this.queueFactory());
  }

  removeConsumerQueue(consumerId: number): void {
    this.unicastQueues.delete(consumerId);
  }

  enqueue(record: Buffer, meta: Metadata): void {
    const queue = this.getQueue(meta.correlationId);
    queue.enqueue(record, meta.priority);
  }

  dequeue(consumerId: number): Buffer | undefined {
    const queue = this.unicastQueues.get(consumerId);
    if (queue?.size()) return queue.dequeue();
    return this.sharedQueue.dequeue();
  }
}
// SRC/TOPICS/DELAYED_QUEUE_MANAGER.TS
class TopicDelayedQueueManager {
  private queue: IPriorityQueue<Buffer>;
  private nextTimeout?: number;
  private isProcessing = false;

  constructor(
    private context: IContext,
    private onReady: (record: Buffer, meta: Metadata) => Promise<void>
  ) {
    this.queue = this.context.queueFactory();
  }

  send(record: Buffer, meta: Metadata): void {
    this.queue.enqueue(record, meta.ttd);
    this.context.eventBus.emit("delayed", meta);
    this.scheduleProcessing();
  }

  size(): number {
    return this.queue.size();
  }

  private scheduleProcessing(): void {
    if (this.isProcessing || this.queue.isEmpty()) return;

    // TODO: implement peakPriority to drop decoding
    const record = this.queue.peek()!;
    const [, meta] = this.context.codec.decode(record);
    const delay = Math.max(0, meta.ts + meta.ttd! - Date.now());

    if (this.nextTimeout) clearTimeout(this.nextTimeout);
    this.nextTimeout = setTimeout(() => this.processQueue(), delay);
  }

  private async processQueue(): Promise<void> {
    if (this.isProcessing) return;
    this.isProcessing = true;

    try {
      while (!this.queue.isEmpty()) {
        const record = this.queue.peek()!;
        const [, meta] = this.context.codec.decode(record);
        if (meta.ts + meta.ttd! > Date.now()) break; // Not ready yet

        this.queue.dequeue();
        await this.onReady(record, meta);
      }
    } finally {
      this.isProcessing = false;
      this.scheduleProcessing();
    }
  }
}
// SRC/TOPICS/DLQ_MANAGER.TS
class TopicDLQManager {
  private queue: IPriorityQueue;

  constructor(private context: IContext) {
    this.queue = this.context.queueFactory();
  }

  send(
    record: Buffer,
    meta: Metadata,
    reason: "expired" | "max_attempts" | "validation" | "processing_error"
  ): void {
    this.queue.enqueue(record, meta.ts);
    this.context.eventBus.emit("dlq", meta, reason);
  }

  size(): number {
    return this.queue.size();
  }

  async consume(): Promise<[any, Metadata] | undefined> {
    const record = this.queue.dequeue();
    if (!record) return;
    return this.context.codec.decode(record);
  }

  async replayMessages(
    handler: (record: Buffer) => Promise<void>,
    filter?: (meta: Metadata) => boolean
  ): Promise<number> {
    const tempQueue = this.context.queueFactory();
    let count = 0;

    // Move all messages to temp queue
    while (this.queue.size() > 0) {
      const record = this.queue.dequeue()!;
      tempQueue.enqueue(record);
    }

    // Process messages
    while (tempQueue.size() > 0) {
      const record = tempQueue.dequeue()!;
      const [, meta] = this.context.codec.decode(record);

      if (!filter || filter(meta)) {
        await handler(record);
        count++;
      } else {
        // Return filtered-out messages to DLQ
        this.queue.enqueue(record);
      }
    }

    return count;
  }
}
// SRC/TOPICS/TOPIC.TS
interface ITopicConfig<Data> {
  schema?: JSONSchemaType<Data>;
  persist?: boolean;
  retentionMs?: number; // 86_400_000 1 day
  archivalThreshold?: number; //100_000
  maxSizeBytes?: number;
  maxDeliveryAttempts?: number;
  maxMessageSize?: number;
  //   partitions?: number;
}
class Topic<Data> {
  private readonly pipeline: MessagePipeline;
  public readonly metrics: TopicMetricsCollector;
  public readonly membership: TopicMembershipManager;
  public readonly queues: TopicQueueManager;
  public readonly dlq: TopicDLQManager;
  public readonly delayedQueue: TopicDelayedQueueManager;
  // private storage: IStorage | null;

  constructor(
    public name: string,
    private context: IContext,
    routingStrategy: IRoutingStrategy,
    private config?: ITopicConfig<Data>
  ) {
    this.metrics = new TopicMetricsCollector(context.eventBus);
    this.membership = new TopicMembershipManager(context.eventBus);
    this.queues = new TopicQueueManager(routingStrategy, context.queueFactory);
    this.delayedQueue = new TopicDelayedQueueManager(context, this.send);
    this.dlq = new TopicDLQManager(context);

    this.pipeline = new PipelineFactory().create(
      this.dlq,
      this.delayedQueue,
      config?.maxDeliveryAttempts
    );
  }

  async send(record: Buffer, meta: Metadata): Promise<void> {
    if (await this.pipeline.process(record, meta)) return;
    this.queues.enqueue(record, meta);
    this.metrics.recordMessagePublished(meta);
  }

  async consume(consumerId: number): Promise<[Data, Metadata] | undefined> {
    const record = this.queues.dequeue(consumerId);
    if (!record) return;
    const message = this.context.codec.decode<Data>(record);
    if (await this.pipeline.process(record, message[1])) return;
    message[1].consumedAt = Date.now();
    return message;
  }

  getMetadata(): ITopicMetadata {
    const base = this.metrics.getMetrics();
    const { producers, consumers } = this.membership.getMembership();

    return {
      ...base,
      name: this.name,
      producersCount: producers.length,
      producersIds: producers,
      consumerCount: consumers.length,
      consumerIds: consumers,
      pendingMessages: this.queues.getQueue().size(),
      dlqSize: this.dlq.size(),
      delayedQueueSize: this.delayedQueue.size(),
    } as ITopicMetadata;
  }
}
// SRC/TOPICS/TOPICREGISTRY.TS
class TopicRegistry {
  private topics = new Map<string, Topic<any>>();

  constructor(
    private routingStrategy: () => IRoutingStrategy,
    private context: IContext
  ) {}

  create<Data>(name: string, config: ITopicConfig<Data>): Topic<Data> {
    if (this.topics.has(name)) {
      throw new Error("Topic already exists");
    }

    if (!name || !/^[a-zA-Z0-9_-]+$/.test(name)) {
      throw new Error("Invalid topic name");
    }

    this.topics.set(
      name,
      new Topic<Data>(name, this.context, this.routingStrategy(), config)
    );

    return this.topics.get(name)!;
  }

  list() {
    return this.topics.keys();
  }

  get(name: string): Topic<any> | undefined {
    return this.topics.get(name);
  }

  getOrThrow(name: string) {
    if (!this.topics.has(name)) throw new Error("Topic not found");
    return this.topics.get(name);
  }

  delete(name: string): void {
    this.getOrThrow(name);
    this.topics.delete(name);
  }
}
// SRC/TOPICS/TOPICBROKER.TS (Facade)
class TopicBroker {
  private readonly topicRegistry: TopicRegistry;
  public readonly validatorRegistry: ValidatorRegistry;

  constructor(
    routingStrategy: () => IRoutingStrategy,
    private readonly context: IContext,
    public readonly config: {
      maxDeliveryAttempts?: number;
      maxMessageSize?: number;
      schemaRegistryOptions?: Ajv.Options;
    } = {}
  ) {
    this.topicRegistry = new TopicRegistry(routingStrategy, context);
    this.validatorRegistry = new ValidatorRegistry(
      config.schemaRegistryOptions
    );
  }

  // Client Registration

  registerProducer(topicName: string, producerId: number): void {
    this.topicRegistry
      .getOrThrow(topicName)!
      .membership.addProducer(producerId);
  }

  unregisterProducer(topicName: string, producerId: number): void {
    this.topicRegistry
      .getOrThrow(topicName)!
      .membership.removeProducer(producerId);
  }

  registerConsumer(topicName: string, consumerId: number): void {
    const topic = this.topicRegistry.getOrThrow(topicName);
    topic!.membership.addConsumer(consumerId);
    topic!.queues.addConsumerQueue(consumerId);
  }

  unregisterConsumer(topicName: string, consumerId: number): void {
    const topic = this.topicRegistry.getOrThrow(topicName);
    topic!.membership.removeConsumer(consumerId);
    topic!.queues.removeConsumerQueue(consumerId);
  }

  // Topic Management

  createTopic<Data>(name: string, config: ITopicConfig<Data>): Topic<Data> {
    if (config?.schema) {
      this.validatorRegistry.register(name, config.schema);
    }
    if (!config.maxDeliveryAttempts) {
      config.maxDeliveryAttempts = this.config.maxDeliveryAttempts;
    }
    return this.topicRegistry.create(name, config);
  }

  getTopic(name: string): Topic<any> | undefined {
    return this.topicRegistry.get(name);
  }

  deleteTopic(name: string): void {
    this.topicRegistry.delete(name);
  }

  // Factories
  createProducer<Data>(topicName: string): Producer<Data> {
    return new ProducerFactory(this, this.context).create<Data>(topicName);
  }

  createConsumer<Data>(topicName: string): Consumer<Data> {
    return new ConsumerFactory(this, this.context).create<Data>(topicName);
  }
}
//
//
//
//
// SRC/PRODUCERS/VALIDATORS/
interface IValidator<Data> {
  validate(data: { data: Data; record: Buffer<Data> }): void;
}
class SchemaValidator<Data> implements IValidator<Data> {
  constructor(
    private readonly validatorRegistry: ValidatorRegistry,
    private topic: string
  ) {}

  validate({ data }): void {
    this.validatorRegistry.get(this.topic)?.(data);
  }
}
class SizeValidator implements IValidator<Buffer> {
  constructor(private maxSize: number) {}

  validate({ record }: { record: Buffer }) {
    if (record.length > this.maxSize) throw new Error("Message too large");
  }
}
// SRC/PRODUCERS/MESSAGE_FACTORY.ts
type MetadataInput = Pick<
  Metadata,
  "priority" | "correlationId" | "ttd" | "ttl"
>;
class MessageFactory<Data> {
  constructor(private codec: ICodec, private validators: IValidator<Data>[]) {}

  create(
    batch: Data[],
    metadataInput: MetadataInput & { topic: string; producerId: number }
  ): Array<{ record: Buffer; meta: Metadata }> {
    const batchId = Date.now();
    return batch.map((data, index) => {
      const meta = new Metadata();
      Object.assign(meta, metadataInput);
      meta.id = uniqueIntGenerator();
      meta.ts = Date.now();
      meta.attempts = 1;

      if (batch.length > 1) {
        meta.batchId = batchId;
        meta.batchIdx = index;
        meta.batchSize = batch.length;
      }

      const record = this.codec.encode(data, meta);
      this.validators.forEach((v) => v.validate({ record, data }));
      return { record, meta };
    });
  }
}
// SRC/PRODUCERS/METRICS.TS
class ProducerMetrics {
  private messagesSent = 0;
  private lastMessages = 0;
  constructor(private producerId: number, private eventBus: EventEmitter) {}

  recordMessage(count: number = 1): void {
    this.messagesSent += count;
    this.lastMessages = Date.now();
    this.eventBus.emit("producerActivity", {
      producerId: this.producerId,
      messagesSent: this.messagesSent,
      ts: this.lastMessages,
    });
  }

  getMetrics() {
    return {
      producerId: this.producerId,
      messagesSent: this.messagesSent,
      lastActivity: this.lastMessages,
    };
  }
}
// SRC/PRODUCERS/PRODUCER.TS
interface MessageResult {
  id: number;
  status: "success" | "error";
  ts: number;
  error?: string;
}
class Producer<Data> {
  private metrics: ProducerMetrics;
  private messageFactory: MessageFactory<Data>;
  constructor(
    private readonly topic: Topic<Data>,
    private readonly validatorRegistry: ValidatorRegistry,
    private readonly context: IContext,
    private readonly options: {
      id: number;
      topic: string;
      maxMessageSize?: number;
    }
  ) {
    this.metrics = new ProducerMetrics(options.id, context.eventBus);
    const validators: IValidator<Data>[] = [
      new SchemaValidator(validatorRegistry, this.options.topic),
    ];
    if (options.maxMessageSize) {
      validators.push(new SizeValidator(options.maxMessageSize));
    }
    this.messageFactory = new MessageFactory<Data>(context.codec, validators);
  }

  async send(
    batch: Data[],
    metadata: MetadataInput = {}
  ): Promise<{
    producerId: number;
    topic: string;
    sentAt: number;
    results: MessageResult[];
  }> {
    const results: MessageResult[] = [];
    const messages = this.messageFactory.create(batch, {
      ...metadata,
      ...this.options,
      producerId: this.options.id,
    });

    for (const { record, meta } of messages) {
      try {
        await this.topic.send(record, meta);
        results.push({
          id: meta.id,
          status: "success",
          ts: meta.ts,
        });
      } catch (error) {
        results.push({
          id: meta.id,
          status: "error",
          error: error instanceof Error ? error.message : "Unknown error",
          ts: Date.now(),
        });
      }
    }

    this.metrics.recordMessage(messages.length);
    return {
      producerId: this.options.id,
      topic: this.options.topic,
      sentAt: Date.now(),
      results,
    };
  }

  getMetrics() {
    return this.metrics.getMetrics();
  }
}
// SRC/PRODUCERS/FACTORY.TS
class ProducerFactory {
  constructor(
    private readonly broker: TopicBroker,
    private readonly context: IContext
  ) {}

  create<Data>(topicName: string) {
    const id = uniqueIntGenerator();
    this.broker.registerProducer(topicName, id);
    const topic = this.broker.getTopic(topicName)!;
    return new Producer<Data>(
      topic,
      this.broker.validatorRegistry,
      this.context,
      {
        maxMessageSize: this.broker.config.maxMessageSize,
        topic: topicName,
        id,
      }
    );
  }
}
//
//
//
//
// SRC/CONSUMERS/METRICS.TS
class ConsumerMetrics {
  private messagesProcessed = 0;
  private messagesFailed = 0;
  private lastActivity = 0;
  private processingTime = 0;
  private pendingMessages = 0;

  constructor(private consumerId: number, private eventBus: EventEmitter) {}

  recordMessageProcessed(processingTimeMs: number): void {
    this.messagesProcessed++;
    this.lastActivity = Date.now();
    this.processingTime += processingTimeMs;
    this.pendingMessages = Math.max(0, this.pendingMessages - 1);

    this.eventBus.emit("consumerActivity", {
      consumerId: this.consumerId,
      type: "processed",
      processingTimeMs,
    });
  }

  recordMessageFailed(): void {
    this.messagesFailed++;
    this.lastActivity = Date.now();
    this.eventBus.emit("consumerActivity", {
      consumerId: this.consumerId,
      type: "failed",
    });
  }

  recordMessageReceived(): void {
    this.pendingMessages++;
    this.lastActivity = Date.now();
  }

  getMetrics() {
    return {
      consumerId: this.consumerId,
      messagesProcessed: this.messagesProcessed,
      messagesFailed: this.messagesFailed,
      pendingMessages: this.pendingMessages,
      lastActivity: this.lastActivity,
      avgProcessingTime:
        this.messagesProcessed > 0
          ? this.processingTime / this.messagesProcessed
          : 0,
    };
  }
}
// SRC/CONSUMERS/ACK_MANAGER.ts
class AckManager<Data> {
  private pending = new Map<number, [Data, Metadata]>();

  addToPending(message: [Data, Metadata]): void {
    this.pending.set(message[1].id, message);
  }

  getMessages(messageId?: number): [Data, Metadata][] {
    return messageId
      ? [this.pending.get(messageId)!]
      : Array.from(this.pending.values());
  }

  ack(messageId?: number): number {
    if (messageId) {
      this.pending.delete(messageId);
      return 1;
    }
    const size = this.pending.size;
    this.pending.clear();
    return size;
  }
}
// SRC/CONSUMERS/SUBSCRIPTION_MANAGER.ts
class SubscriptionManager<Data> {
  private isActive = false;
  private abortController = new AbortController();

  constructor(
    private fetcher: () => Promise<Data[]>,
    private onError?: (e?: Error) => void,
    private delayInterval = 1000
  ) {}

  async subscribe(handler: (messages: Data[]) => Promise<void>): Promise<void> {
    this.isActive = true;

    while (this.isActive) {
      try {
        const messages = await this.fetcher();
        if (messages.length > 0) {
          await handler(messages);
        } else {
          await wait(this.delayInterval, this.abortController.signal);
        }
      } catch (err) {
        this.onError?.(err);
        if (err.name !== "AbortError") throw err;
      }
    }
  }

  unsubscribe(): void {
    this.isActive = false;
    this.abortController.abort();
  }
}
// SRC/CONSUMERS/CONSUMER.ts (Facade)
class Consumer<Data> {
  private metrics: ConsumerMetrics;
  private ackManager: AckManager<Data>;
  private subscriptionManager: SubscriptionManager<Data>;
  constructor(
    private topic: Topic<Data>,
    private context: IContext,
    private options: {
      id: number;
      topic: string;
      limit?: number;
      autoAck?: boolean;
      pollingInterval?: number;
    }
  ) {
    this.metrics = new ConsumerMetrics(options.id, context.eventBus);
    this.ackManager = new AckManager<Data>();
    this.subscriptionManager = new SubscriptionManager<Data>(
      this.consume,
      () => this.nack(),
      this.options.pollingInterval
    );
  }

  async consume() {
    const { id, limit = 1 } = this.options;
    const messages: Data[] = [];
    const metas: Metadata[] = [];

    for (let i = 0; i < limit; i++) {
      const message = await this.topic.consume(id);
      if (!message) continue;

      this.metrics.recordMessageReceived();

      if (this.options.autoAck) {
        this.metrics.recordMessageProcessed(0);
      } else {
        this.ackManager.addToPending(message);
      }

      messages.push(message[0]);
      metas.push(message[1]);
    }

    this.context.eventBus.emit("consumed", metas);
    return messages;
  }

  async consumeDlq() {
    const { id, limit = 1 } = this.options;
    const messages: Data[] = [];

    for (let i = 0; i < limit; i++) {
      const message = await this.topic.consume(id);
      if (message) messages.push(message[0]);
    }

    return messages;
  }

  // async replayDlq(filter?: (meta: Metadata) => boolean): Promise<number> {
  //   return this.topic.dlq.replayMessages(
  //     async (record) => {
  //       const [, meta] = this.context.codec.decode(record);
  //       await this.topic.send(record, meta);
  //     },
  //     filter
  //   );
  // }

  subscribe(handler: (messages: Data[]) => Promise<void>): void {
    this.subscriptionManager.subscribe(handler);
  }

  unsubscribe(): void {
    this.subscriptionManager.unsubscribe();
  }

  ack(messageId?: number): void {
    const messages = this.ackManager.getMessages(messageId);
    const now = Date.now();

    messages.forEach((message) => {
      this.ackManager.ack(message[1].id);
      this.metrics.recordMessageProcessed(now - (message[1].consumedAt || now));
    });
  }

  nack(messageId?: number, requeue = true): void {
    const messages = this.ackManager.getMessages(messageId);

    messages.forEach(([data, meta]) => {
      this.metrics.recordMessageFailed();
      meta.attempts = requeue ? meta.attempts + 1 : Infinity;
      const record = this.context.codec.encode(data, meta);
      this.topic.send(record, meta);
      this.ackManager.ack(meta.id);
    });
  }
}
// SRC/CONSUMERS/FACTORY.TS
class ConsumerFactory {
  constructor(
    private readonly broker: TopicBroker,
    private readonly context: IContext,
    private readonly defaultOptions: {
      limit?: number;
      autoAck?: boolean;
      pollingInterval?: number;
    } = {}
  ) {}

  create<Data>(
    topicName: string,
    options?: typeof this.defaultOptions
  ): Consumer<Data> {
    const id = uniqueIntGenerator();
    this.broker.registerConsumer(topicName, id);
    const topic = this.broker.getTopic(topicName)!;
    const { limit, autoAck, pollingInterval } = this.defaultOptions;

    return new Consumer<Data>(topic, this.context, {
      id,
      topic: topicName,
      limit: limit ?? options?.limit,
      autoAck: autoAck ?? options?.autoAck,
      pollingInterval: pollingInterval ?? options?.pollingInterval,
    });
  }
}
