import level, { LevelDB } from "level";
import crypto from "node:crypto";
import { EventEmitter } from "node:events";
import pino from "pino";
import { Counter, Gauge } from "prom-client";
import Zod, { z } from "zod";
import { ProtoMessage } from "../generated/message";
import { wait } from "./utils";
import fs from "fs/promises";
import path from "path";
import snappy from "snappy";
import Buffer from "node:buffer";

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
  logger.info(event, `Message consumed from ${event[0].topic}.`)
);
bus.on("topicNotFound", (event: Metadata) =>
  logger.info(event, "Message Topic not found. Routed ro DLQ.")
);
bus.on("expired", (event: Metadata) =>
  logger.info(event, "Message expired. Routed ro DLQ.")
);
bus.on("maxAttempts", (event: Metadata) =>
  logger.info(event, "Message exceeded delivery maxAttempts. Routed ro DLQ.")
);
bus.on("delayed", (event: Metadata) => logger.info(event, "Message delayed"));
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
  addNode(id: string): void;
  removeNode(id: string): void;
  getNode(key: string): string | undefined;
}
/** Consistent hashing class.
 * The system works regardless of how different the key hashes are because the lookup is always
 * relative to the fixed node positions on the ring. Sorted nodes in a ring:
 * [**100(A)**, _180(user-123 key hash always belong to the B)_, **200(B)**, **300(A)**, **400(B)**, **500(A)**, **600(B)**]
 */
class InMemoryConsistencyHasher implements IConsistencyHasher {
  private ring = new Map<number, string>();

  /**
   * Create a new instance of ConsistencyHasher with the given number of virtual nodes.
   * @param {number} [replicas=3] The number of virtual nodes to create for each node.
   * The more virtual nodes gives you fewer hotspots, more balanced traffic. However, setting
   * this number too high can lead to a large memory footprint and slower lookups.
   */
  constructor(private hashService: IHashService, private replicas = 3) {}

  addNode(id: string) {
    for (let i = 0; i < this.replicas; i++) {
      const hash = this.hashService.hash(`${id}-${i}`);
      this.ring.set(hash, id);
    }
  }

  removeNode(id: string) {
    if (![...this.ring.values()].includes(id)) return;
    for (let i = 0; i < this.replicas; i++) {
      const hash = this.hashService.hash(`${id}-${i}`);
      this.ring.delete(hash);
    }
  }

  getNode(key: string): string | undefined {
    const keyHash = this.hashService.hash(key);
    const sortedHashes = Array.from(this.ring.keys()).sort((a, b) => a - b);
    const node = sortedHashes.find((h) => h >= keyHash) || sortedHashes[0];
    return this.ring.get(node);
  }
}
interface IRoutingStrategy {
  getConsumerId(key: string): string;
  addConsumer(id: string): void;
  removeConsumer(id: string): void;
}
class ConsistentHashingStrategy implements IRoutingStrategy {
  constructor(private hasher: IConsistencyHasher) {}

  getConsumerId(key: string) {
    return this.hasher.getNode(key)!;
  }
  addConsumer(id: string) {
    return this.hasher.addNode(id);
  }
  removeConsumer(id: string) {
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
  id: string;
  ts: number;
  topic: string;
  producerId: string;
  correlationId?: string;
  priority?: number;
  ttl?: number;
  ttd?: number;
  batchId?: string;
  batchIdx?: number;
  batchSize?: number;
  attempts: number;
  consumedAt?: number;
}
//
//
//
// SRC/CODECS/
interface ICodec {
  encode<T>(data: T, meta: Metadata): Buffer;
  decode<T>(bytes: Buffer): [T, Metadata];
}
class JSONCodec implements ICodec {
  constructor(
    private metaProperties = [
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
    ]
  ) {}

  encode<Data>(data: Data, meta: Metadata) {
    try {
      const reducedMeta: Record<number, unknown> = {};
      this.metaProperties.forEach((k, i) => {
        reducedMeta[i + 1] = meta[k];
      });
      return Buffer.from(JSON.stringify({ "0": data, ...reducedMeta }));
    } catch (e) {
      throw new Error("Failed to encode message");
    }
  }

  decode<Data>(bytes: Buffer) {
    try {
      const raw = JSON.parse(Buffer.toString(bytes));
      // const message = messageSchema.parse(raw);
      const meta = new Metadata();
      this.metaProperties.forEach((k, i) => {
        meta[k] = raw[i + 1];
      });
      const result: [Data, Metadata] = [raw[0] as Data, meta];
      return result;
    } catch (e) {
      throw new Error("Failed to decode message");
    }
  }
}
// class ProtobufCodec implements ICodec {
//   encode<Data>(message: Message<Data>): Buffer {
//     const protoMsg = ProtoMessage.create({
//       data: this.serializeData(message.data),
//       // Convert all numbers to bigint
//       ts: BigInt(message.ts),
//       attempts: BigInt(message.attempts),
//       priority: message.priority ? BigInt(message.priority) : message.priority,
//       ttl: message.ttl ? BigInt(message.ttl) : message.ttl,
//       ttd: message.ttd ? BigInt(message.ttd) : message.ttd,
//       batchIdx: message.batchIdx ? BigInt(message.batchIdx) : message.batchIdx,
//       batchSize: message.batchSize
//         ? BigInt(message.batchSize)
//         : message.batchSize,
//     });
//     return ProtoMessage.encode(protoMsg).finish();
//   }

//   decode<Data>(bytes: Buffer): Message<Data> {
//     const protoMsg = ProtoMessage.decode(bytes);
//     return Object.assign(new Message(), {
//       data: this.deserializeData<Data>(protoMsg.data),
//       // Convert back to JS numbers
//       ts: Number(protoMsg.ts),
//       attempts: Number(protoMsg.attempts),
//       priority: protoMsg.priority
//         ? Number(protoMsg.priority)
//         : protoMsg.priority,
//       ttl: protoMsg.ttl ? Number(protoMsg.ttl) : protoMsg.ttl,
//       ttd: protoMsg.ttd ? Number(protoMsg.ttd) : protoMsg.ttd,
//       batchIdx: protoMsg.batchIdx
//         ? Number(protoMsg.batchIdx)
//         : protoMsg.batchIdx,
//       batchSize: protoMsg.batchSize
//         ? Number(protoMsg.batchSize)
//         : protoMsg.batchSize,
//     });
//   }

//   private serializeData<T>(data: T): Buffer {
//     // no data schema fallback - just binary
//     return Buffer.from(JSON.stringify(data));
//   }

//   private deserializeData<T>(bytes: Buffer): T {
//     // no data schema fallback - just binary
//     return JSON.parse(Buffer.toString(bytes));
//   }
// }
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
  validator?: IValidator<any>;
}
class Context implements IContext {
  constructor(
    public codec: ICodec,
    public eventBus: EventEmitter,
    public queueFactory: () => IPriorityQueue<Buffer>,
    public validator?: IValidator<any>
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
abstract class BaseProcessor implements IMessageProcessor {
  constructor(protected context: IContext) {}
  abstract process(record: Buffer, meta: Metadata): boolean;
  // protected decode<Data>(record: Buffer): Message<Data> {
  //   return this.context.codec.decode(record);
  // }
}
// SRC/PIPELINE/PROCESSORS/
class ExpirationProcessor extends BaseProcessor {
  process(record: Buffer, meta: Metadata): boolean {
    if (!meta.ttl) return false;
    const isExpired = meta.ts + meta.ttl <= Date.now();
    if (isExpired) {
      this.context.eventBus.emit("deadLetter", record);
      this.context.eventBus.emit("expired", meta);
    }
    return isExpired;
  }
}
class AttemptsProcessor extends BaseProcessor {
  constructor(context: IContext, private maxAttempts: number) {
    super(context);
  }
  process(record: Buffer, meta: Metadata): boolean {
    const shouldDeadLetter = meta.attempts > this.maxAttempts;
    if (shouldDeadLetter) {
      this.context.eventBus.emit("deadLetter", record);
      this.context.eventBus.emit("maxAttempts", meta);
    }
    return shouldDeadLetter;
  }
}
class DelayProcessor extends BaseProcessor {
  constructor(context: IContext, private delayedQueue: IQueueStrategy) {
    super(context);
  }
  process(record: Buffer, meta: Metadata): boolean {
    const shouldDelay = !!meta.ttd && meta.ttd > Date.now();
    if (shouldDelay) {
      this.delayedQueue.enqueue(record, meta);
      this.context.eventBus.emit("delayed", meta);
    }
    return shouldDelay;
  }
}
class NoTopicProcessor extends BaseProcessor {
  constructor(context: IContext, private topicRegistry: TopicRegistry) {
    super(context);
  }
  process(record: Buffer, meta: Metadata): boolean {
    const topicExists = this.topicRegistry.get(meta.topic);

    if (!topicExists) {
      this.context.eventBus.emit("deadLetter", record);
      this.context.eventBus.emit("topicNotFound", meta);
      return true;
    }

    return false;
  }
}
// SRC/PIPELINE/MESSAGE_PIPELINE.TS
class MessagePipeline {
  private processors: IMessageProcessor[] = [];
  constructor(private context: IContext) {}
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
// SRC/PIPELINE/FACTORY
class PipelineFactory {
  constructor(
    private context: IContext,
    private topicRegistry: TopicRegistry,
    private maxAttempts: number = 5
  ) {}

  create(): MessagePipeline {
    const pipeline = new MessagePipeline(this.context);
    const delayedQueue = new DelayedQueueStrategy(this.context);

    pipeline.addProcessor(
      new NoTopicProcessor(this.context, this.topicRegistry)
    );
    pipeline.addProcessor(new ExpirationProcessor(this.context));
    pipeline.addProcessor(
      new AttemptsProcessor(this.context, this.maxAttempts)
    );
    pipeline.addProcessor(new DelayProcessor(this.context, delayedQueue));

    return pipeline;
  }
}
//
//
//
//
// SRC/QUEUES/STRATEGIES.TS
interface IQueueStrategy {
  enqueue(record: Buffer, meta: Metadata): void;
  dequeue(): Buffer | undefined;
  size(): number;
}
class PriorityQueueStrategy implements IQueueStrategy {
  constructor(private queue: IPriorityQueue<Buffer>) {}
  enqueue(record: Buffer, meta: Metadata): void {
    this.queue.enqueue(record, meta.priority);
  }
  dequeue(): Buffer | undefined {
    return this.queue.dequeue();
  }
  size(): number {
    return this.queue.size();
  }
}
class DelayedQueueStrategy implements IQueueStrategy {
  private nextTimeout?: number;
  constructor(
    private context: IContext,
    private queue = this.context.queueFactory()
  ) {}

  private scheduleDelivery(): void {
    if (this.nextTimeout) clearTimeout(this.nextTimeout);
    if (this.queue.isEmpty()) return;

    const record = this.queue.peek()!;
    const [, meta] = this.context.codec.decode(record);
    const delay = Math.max(0, meta.ts + meta.ttd! - Date.now());

    this.nextTimeout = setTimeout(this.processQueue.bind(this), delay);
  }

  private processQueue(): void {
    // while (!this.queue.isEmpty()) {
    const record = this.queue.dequeue()!;
    const [, meta] = this.context.codec.decode(record);
    this.context.eventBus.emit("delayedMessageReady", { record, meta });
    // }
    this.scheduleDelivery();
  }

  enqueue(record: Buffer, meta: Metadata): void {
    if (!meta.ttd) throw new Error("TTD required for delayed messages");
    this.queue.enqueue(record, meta.ttd);
    this.scheduleDelivery();
  }

  dequeue(): Buffer | undefined {
    return undefined; // Delayed queue doesn't support direct dequeue
  }

  size(): number {
    return this.queue.size();
  }
}
//
//
//
//
// MESSAGE_ROUTER.TS
class DLQManager {
  constructor(
    private queue: IPriorityQueue<Buffer>,
    private eventBus: EventEmitter
  ) {
    eventBus.on("deadLetter", (record) => this.addMessage(record));
  }
  addMessage(record: Buffer) {
    this.queue.enqueue(record);
  }
}
class MessageRouter {
  private pipeline: MessagePipeline;
  constructor(
    private topicRegistry: TopicRegistry,
    private dlqManager: DLQManager,
    private context: IContext,
    maxAttempts: number
  ) {
    this.pipeline = new PipelineFactory(
      context,
      this.topicRegistry,
      maxAttempts
    ).create();

    context.eventBus.on("delayedMessageReady", ({ record, meta }) => {
      this.route(record, meta);
    });
  }

  async route(record: Buffer, meta: Metadata): Promise<void> {
    if (await this.pipeline.process(record, meta)) return;

    const topic = this.topicRegistry.get(meta.topic)!;
    topic.queues.enqueue(record, meta);
    this.context.eventBus.emit("routed", { meta });
  }
}
//
//
//
//
// SRC/TOPICS/MEMBERSHIP.TS
class TopicMembershipManager {
  private producers = new Map<string, IClientMetadata>();
  private consumers = new Map<string, IClientMetadata>();
  constructor(private eventBus: EventEmitter) {}

  addProducer(id: string): void {
    this.producers.set(id, { lastActiveAt: Date.now() });
    this.eventBus.emit("producerAdded", { producerId: id });
  }

  removeProducer(id: string): void {
    this.producers.delete(id);
    this.eventBus.emit("producerRemoved", { producerId: id });
  }

  addConsumer(id: string): void {
    this.consumers.set(id, { lastActiveAt: Date.now() });
    this.eventBus.emit("consumerAdded", { consumerId: id });
  }

  removeConsumer(id: string): void {
    this.consumers.delete(id);
    this.eventBus.emit("consumerRemoved", { consumerId: id });
  }

  getMembership(): { producers: string[]; consumers: string[] } {
    return {
      producers: Array.from(this.producers.keys()),
      consumers: Array.from(this.consumers.keys()),
    };
  }
}
// SRC/TOPICS/QUEUE_MANAGER.TS
class TopicQueueManager {
  private unicastQueues = new Map<string, IQueueStrategy>();
  constructor(
    private sharedQueue: IQueueStrategy,
    private routingStrategy: IRoutingStrategy,
    private context: IContext
  ) {}

  getQueue(key?: string): IQueueStrategy {
    if (key) {
      const targetConsumer = this.routingStrategy.getConsumerId(key);
      return this.unicastQueues.get(targetConsumer) || this.sharedQueue;
    }
    return this.sharedQueue;
  }

  addConsumerQueue(consumerId: string): void {
    this.unicastQueues.set(
      consumerId,
      new PriorityQueueStrategy(this.context.queueFactory())
    );
  }

  removeConsumerQueue(consumerId: string): void {
    this.unicastQueues.delete(consumerId);
  }

  enqueue(record: Buffer, meta: Metadata): void {
    const queue = this.getQueue(meta.correlationId);
    queue.enqueue(record, meta);
  }

  dequeue(consumerId: string): Buffer | undefined {
    const queue = this.unicastQueues.get(consumerId);
    if (queue?.size()) return queue.dequeue();
    return this.sharedQueue.dequeue();
  }
}
// SRC/TOPICS/METRICS.TS
class TopicMetricsCollector {
  private totalMessagesPublished = 0;
  private ts = Date.now();

  constructor(private eventBus: EventEmitter) {}

  recordMessagePublished(producerId?: string): void {
    this.totalMessagesPublished++;
    if (producerId) {
      this.eventBus.emit("messagePublished", { producerId });
    }
  }

  getMetrics(): ITopicMetadata {
    return {
      name: "",
      createdAt: new Date(this.ts).toISOString(),
      pendingMessages: 0,
      producersCount: 0,
      producersIds: [],
      totalMessagesPublished: this.totalMessagesPublished,
      consumerCount: 0,
      consumerIds: [],
    };
  }
}
// SRC/TOPICS/TOPIC.TS
interface ITopicMetadata {
  name: string;
  createdAt: string;
  pendingMessages: number;
  producersCount: number;
  producersIds: string[];
  totalMessagesPublished: number;
  consumerCount: number;
  consumerIds: string[];
}
interface IClientMetadata {
  lastActiveAt: number;
  messageCount?: number;
  pendingMessages?: number;
}
class Topic<Data> {
  public readonly metrics: TopicMetricsCollector;
  public readonly membership: TopicMembershipManager;
  public readonly queues: TopicQueueManager;
  // private storage: IStorage | null;
  // config: {
  //   retentionMs?: number;           // How long messages are retained
  //   maxSizeBytes?: number;          // Max topic size
  //   partitions?: number;            // Number of partitions (if partitioned)
  // };
  constructor(
    public name: string,
    private router: MessageRouter,
    private routingStrategy: IRoutingStrategy,
    private context: IContext
  ) {
    this.metrics = new TopicMetricsCollector(context.eventBus);
    this.membership = new TopicMembershipManager(context.eventBus);
    this.queues = new TopicQueueManager(
      new PriorityQueueStrategy(context.queueFactory()),
      routingStrategy,
      context
    );
  }

  send(record: Buffer, meta: Metadata): void {
    this.router.route(record, meta);
    this.metrics.recordMessagePublished(meta.producerId);
  }

  consume(consumerId: string): [Data, Metadata] | undefined {
    const record = this.queues.dequeue(consumerId);
    if (!record) return;
    const message = this.context.codec.decode<Data>(record);

    // TODO: refactor
    if (message[1].ttl && message[1].ts + message[1].ttl <= Date.now()) {
      this.router.route(record, message[1]);
      return;
    }

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
    };
  }
}
// SRC/TOPICS/TOPICREGISTRY.TS
class TopicRegistry {
  private topics = new Map<string, Topic<any>>();

  constructor(
    private router: MessageRouter,
    private routingStrategy: () => IRoutingStrategy,
    private context: IContext
  ) {}

  create<Data>(name: string): Topic<Data> {
    if (this.topics.has(name)) {
      throw new Error("Topic already exists");
    }

    if (!name || !/^[a-zA-Z0-9_-]+$/.test(name)) {
      throw new Error("Invalid topic name");
    }

    this.topics.set(
      name,
      new Topic<Data>(name, this.router, this.routingStrategy(), this.context)
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
  private dlqManager: DLQManager;
  private router: MessageRouter;

  constructor(
    private readonly topicRegistry: TopicRegistry,
    private context: IContext,
    private config: {
      maxDeliveryAttempts?: number;
      delayedQueuePolling?: number;
    } = {}
  ) {
    this.dlqManager = new DLQManager(
      this.context.queueFactory(),
      this.context.eventBus
    );
    this.router = new MessageRouter(
      this.topicRegistry,
      this.dlqManager,
      this.context,
      this.config.maxDeliveryAttempts ?? 5
    );
  }

  // Client Registration

  registerProducer(topicName: string, producerId: string): void {
    this.topicRegistry
      .getOrThrow(topicName)!
      .membership.addProducer(producerId);
  }

  unregisterProducer(topicName: string, producerId: string): void {
    this.topicRegistry
      .getOrThrow(topicName)!
      .membership.removeProducer(producerId);
  }

  registerConsumer(topicName: string, consumerId: string): void {
    const topic = this.topicRegistry.getOrThrow(topicName);
    topic!.membership.addConsumer(consumerId);
    topic!.queues.addConsumerQueue(consumerId);
  }

  unregisterConsumer(topicName: string, consumerId: string): void {
    const topic = this.topicRegistry.getOrThrow(topicName);
    topic!.membership.removeConsumer(consumerId);
    topic!.queues.removeConsumerQueue(consumerId);
  }

  // Topic Management

  createTopic(name: string): Topic<any> {
    return this.topicRegistry.create(name);
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
class SizeValidator implements IValidator<Buffer> {
  constructor(private maxSize: number) {}

  validate({ record }: { record: Buffer }) {
    if (record.length > this.maxSize) throw new Error("Message too large");
  }
}
class MessageValidator<Data> implements IValidator<Data> {
  constructor(private schema: Zod.Schema<Data>) {}

  validate({ data }: { data: Data }) {
    this.schema?.parse(data);
  }
}
// SRC/PRODUCERS/MESSAGE_FACTORY.ts
type MetadataInput = Pick<
  Metadata,
  "priority" | "correlationId" | "ttd" | "ttl"
>;
class MessageFactory<Data> {
  constructor(private validators: IValidator<Data>[], private codec: ICodec) {}

  create(
    batch: Data[],
    metadataInput: MetadataInput & { topic: string; producerId: string }
  ): Array<{ record: Buffer; meta: Metadata }> {
    return batch.map((data, index) => {
      const meta = new Metadata();
      Object.assign(meta, metadataInput);
      meta.id = crypto.randomUUID();
      meta.ts = Date.now();
      meta.attempts = 1;

      if (batch.length > 1) {
        meta.batchId = `batch-${meta.id}`;
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

  constructor(private producerId: string, private eventBus: EventEmitter) {}

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
  id: string;
  status: "success" | "error";
  ts: number;
  error?: string;
}
class Producer<Data> {
  private metrics: ProducerMetrics;
  private messageFactory: MessageFactory<Data>;
  constructor(
    private readonly topic: Topic<Data>,
    private readonly context: IContext,
    private readonly options: { producerId: string; topic: string }
  ) {
    this.metrics = new ProducerMetrics(options.producerId, context.eventBus);
    this.messageFactory = new MessageFactory<Data>([], context.codec);
  }

  async send(
    batch: Data[],
    metadata: MetadataInput = {}
  ): Promise<{
    producerId: string;
    topic: string;
    sentAt: number;
    results: MessageResult[];
  }> {
    const results: MessageResult[] = [];
    const messages = this.messageFactory.create(batch, {
      ...metadata,
      ...this.options,
    });

    for (const { record, meta } of messages) {
      try {
        this.topic.send(record, meta);
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
      producerId: this.options.producerId,
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
  private static iterator = 1;
  constructor(
    private readonly broker: TopicBroker,
    private readonly context: IContext
  ) {}

  create<Data>(topicName: string) {
    const producerId = `producer-${ProducerFactory.iterator++}`;
    this.broker.registerProducer(topicName, producerId);
    const topic = this.broker.getTopic(topicName)!;
    return new Producer<Data>(topic, this.context, {
      topic: topicName,
      producerId,
    });
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

  constructor(private consumerId: string, private eventBus: EventEmitter) {}

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
  private pending = new Map<string, [Data, Metadata]>();

  addToPending(message: [Data, Metadata]): void {
    this.pending.set(message[1].id, message);
  }

  getMessages(messageId?: string): [Data, Metadata][] {
    return messageId
      ? [this.pending.get(messageId)!]
      : Array.from(this.pending.values());
  }

  ack(messageId?: string): number {
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
    private fetcher: () => Data[],
    private onError?: (e?: Error) => void,
    private delayInterval = 1000
  ) {}

  async subscribe(handler: (messages: Data[]) => Promise<void>): Promise<void> {
    this.isActive = true;

    while (this.isActive) {
      try {
        const messages = this.fetcher();
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
      consumerId: string;
      topic: string;
      limit?: number;
      autoAck?: boolean;
      pollingInterval?: number;
    }
  ) {
    this.metrics = new ConsumerMetrics(options.consumerId, context.eventBus);
    this.ackManager = new AckManager<Data>();
    this.subscriptionManager = new SubscriptionManager<Data>(
      this.consume,
      () => this.nack(),
      this.options.pollingInterval
    );
  }

  consume() {
    const { consumerId, limit = 1 } = this.options;
    const messages: Data[] = [];
    const metas: Metadata[] = [];
    const now = Date.now();

    for (let i = 0; i < limit; i++) {
      const message = this.topic.consume(consumerId);
      if (!message) continue;

      message[1].consumedAt = now;
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

  subscribe(handler: (messages: Data[]) => Promise<void>): void {
    this.subscriptionManager.subscribe(handler);
  }

  unsubscribe(): void {
    this.subscriptionManager.unsubscribe();
  }

  ack(messageId?: string): void {
    const messages = this.ackManager.getMessages(messageId);
    const now = Date.now();

    messages.forEach((message) => {
      this.ackManager.ack(message[1].id);
      this.metrics.recordMessageProcessed(now - (message[1].consumedAt || now));
    });
  }

  nack(messageId?: string, requeue = true): void {
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
  private static iterator = 1;

  constructor(
    private readonly broker: TopicBroker,
    private readonly context: IContext,
    private readonly defaultOptions?: {
      limit?: number;
      autoAck?: boolean;
      pollingInterval?: number;
    }
  ) {}

  create<Data>(
    topicName: string,
    options?: typeof this.defaultOptions
  ): Consumer<Data> {
    const consumerId = `consumer-${ConsumerFactory.iterator++}`;
    this.broker.registerConsumer(topicName, consumerId);
    const topic = this.broker.getTopic(topicName)!;

    return new Consumer<Data>(topic, this.context, {
      consumerId,
      topic: topicName,
      limit: this.defaultOptions?.limit || options?.limit,
      autoAck: this.defaultOptions?.autoAck || options?.autoAck,
      pollingInterval:
        this.defaultOptions?.pollingInterval || options?.pollingInterval,
    });
  }
}
