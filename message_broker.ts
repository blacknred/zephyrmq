import crypto from "node:crypto";
import { EventEmitter } from "node:events";
import pino from "pino";
import Zod, { z } from "zod";
import {
  LinkedListPriorityQueue,
  HighCapacityBinaryHeapPriorityQueue,
} from "../queues";
import { wait } from "./utils";
import { ProtoMessage } from "../generated/message";
import { Gauge, Counter } from "prom-client";

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
const topicDepth = new Counter({
  name: "queue_depth",
  help: "Current messages in queue",
  labelNames: ["topic"],
});
const producerMessageCount = new Counter({
  name: "producer_messages_total",
  help: "Total messages sent by producer",
  labelNames: ["topic", "producer_id"],
});
const consumerLag = new Gauge({
  name: "consumer_pending_messages",
  help: "Pending messages per consumer",
  labelNames: ["topic", "consumer_id"],
});
bus.on("messageRouted", (event) => {
  logger.info(event, "Message routed");
});
bus.on("messagesConsumed", (event) => {
  logger.info(event, "Messages consumed");
});
bus.on("producerMessageCount", ({ topic, producerId: producer_id }) => {
  producerMessageCount.inc({ topic, producer_id });
  topicDepth.inc({ topic });
});
bus.on("consumerLag", ({ topic, onsumerId: consumer_id, lag }) => {
  consumerLag.set({ topic, consumer_id }, lag);
});
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

// SRC/MESSAGE.TS
interface IMessageMetadata {
  id: string;
  topic: string;
  attempts: number;
  timestamp: number;
  correlationId?: string;
  priority?: number;
  ttl?: number;
  ttd?: number;
  batchId?: string;
  batchIndex?: number;
  batchSize?: number;
  consumedAt?: number;
}
class Message<Data = any> {
  public metadata: IMessageMetadata;
  constructor(
    public data: Data,
    metadata: Omit<IMessageMetadata, "timestamp" | "attempts">
  ) {
    this.metadata = {
      ...metadata,
      timestamp: Date.now(),
      attempts: 1,
    };
  }
}
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
// SRC/CODECS/
const messageSchema = z.object({
  data: z.unknown(),
  metadata: z.object({
    id: z.string(),
    topic: z.string(),
    attempts: z.number().int().min(1),
    timestamp: z.number(),
  }),
});
class JSONCodec implements ICodec {
  encode<Data>(message: Message<Data>) {
    try {
      return new TextEncoder().encode(JSON.stringify(message));
    } catch (e) {
      throw new Error("Failed to encode message");
    }
  }
  decode<Data>(bytes: Uint8Array) {
    try {
      const raw = JSON.parse(new TextDecoder().decode(bytes));
      const { data, metadata } = messageSchema.parse(raw);
      return new Message<Data>(data, metadata);
    } catch (e) {
      throw new Error("Failed to decode message");
    }
  }
}
class ProtobufCodec implements ICodec {
  encode<Data>({ data, metadata }: Message<Data>): Uint8Array {
    const { timestamp, attempts, priority, ttd, ttl, batchIndex, batchSize } =
      metadata;
    const protoMsg = ProtoMessage.create({
      data: this.serializeData(data),
      metadata: {
        ...metadata,
        // Convert all numbers to bigint
        timestamp: BigInt(timestamp),
        attempts: BigInt(attempts),
        priority: priority ? BigInt(priority) : priority,
        ttl: ttl ? BigInt(ttl) : ttl,
        ttd: ttd ? BigInt(ttd) : ttd,
        batchIndex: batchIndex ? BigInt(batchIndex) : batchIndex,
        batchSize: batchSize ? BigInt(batchSize) : batchSize,
      },
    });
    return ProtoMessage.encode(protoMsg).finish();
  }

  decode<Data>(bytes: Uint8Array): Message<Data> {
    const protoMsg = ProtoMessage.decode(bytes);
    const { timestamp, attempts, priority, ttd, ttl, batchIndex, batchSize } =
      protoMsg.metadata;
    return new Message(this.deserializeData<Data>(protoMsg.data), {
      ...protoMsg.metadata,
      // Convert back to JS numbers
      timestamp: Number(timestamp),
      attempts: Number(attempts),
      priority: priority ? Number(priority) : priority,
      ttl: ttl ? Number(ttl) : ttl,
      ttd: ttd ? Number(ttd) : ttd,
      batchIndex: batchIndex ? Number(batchIndex) : batchIndex,
      batchSize: batchSize ? Number(batchSize) : batchSize,
    });
  }

  private serializeData<T>(data: T): Uint8Array {
    // no data schema fallback - just binary
    return new TextEncoder().encode(JSON.stringify(data));
  }

  private deserializeData<T>(bytes: Uint8Array): T {
    // no data schema fallback - just binary
    return JSON.parse(new TextDecoder().decode(bytes));
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
interface ICodec {
  encode<T>(message: Message<T>): Uint8Array;
  decode<T>(bytes: Uint8Array): Message<T>;
}
interface IValidator<Data> {
  validate(data: { message: Message<Data>; record: Uint8Array }): void;
}
interface IContext {
  codec: ICodec;
  eventBus: EventEmitter;
  queueFactory: () => IPriorityQueue<Uint8Array>;
  validator?: IValidator<any>;
}
class Context implements IContext {
  constructor(
    public codec: ICodec,
    public eventBus: EventEmitter,
    public queueFactory: () => IPriorityQueue<Uint8Array>,
    public validator?: IValidator<any>
  ) {}
}
//
//
//
//
// SRC/PIPELINE/PROCESSOR.TS
interface IMessageProcessor {
  process(record: Uint8Array, metadata: IMessageMetadata): boolean;
}
abstract class BaseProcessor implements IMessageProcessor {
  constructor(protected context: IContext) {}
  abstract process(record: Uint8Array, metadata: IMessageMetadata): boolean;
  protected decode<Data>(record: Uint8Array): Message<Data> {
    return this.context.codec.decode(record);
  }
}
// SRC/PIPELINE/PROCESSORS/
class ExpirationProcessor extends BaseProcessor {
  process(record: Uint8Array, metadata: IMessageMetadata): boolean {
    if (!metadata.ttl) return false;
    const isExpired = metadata.timestamp + metadata.ttl <= Date.now();
    if (isExpired) {
      this.context.eventBus.emit("expired", {
        record,
        metadata: {
          ...metadata,
          attempts: Infinity,
        },
      });
    }
    return isExpired;
  }
}
class AttemptsProcessor extends BaseProcessor {
  constructor(context: IContext, private maxAttempts: number) {
    super(context);
  }
  process(record: Uint8Array, metadata: IMessageMetadata): boolean {
    const shouldDeadLetter = metadata.attempts > this.maxAttempts;
    if (shouldDeadLetter) {
      this.context.eventBus.emit("maxAttempts", { record, metadata });
    }
    return shouldDeadLetter;
  }
}
class DelayProcessor extends BaseProcessor {
  constructor(context: IContext, private delayedQueue: IQueueStrategy) {
    super(context);
  }
  process(record: Uint8Array, metadata: IMessageMetadata): boolean {
    const shouldDelay = !!metadata.ttd && metadata.ttd > Date.now();
    if (shouldDelay) {
      this.delayedQueue.enqueue(record, metadata);
      this.context.eventBus.emit("delayed", { record, metadata });
    }
    return shouldDelay;
  }
}
class NoTopicProcessor extends BaseProcessor {
  constructor(context: IContext, private topicRegistry: TopicRegistry) {
    super(context);
  }
  process(record: Uint8Array, metadata: IMessageMetadata): boolean {
    const topicExists = this.topicRegistry.get(metadata.topic);

    if (!topicExists) {
      this.context.eventBus.emit("deadLetter", { record });
      this.context.eventBus.emit("topicNotFound", { record, metadata });
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
  async process(
    record: Uint8Array,
    metadata: IMessageMetadata
  ): Promise<boolean> {
    for (const processor of this.processors) {
      if (processor.process(record, metadata)) return true;
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
  enqueue(record: Uint8Array, metadata: IMessageMetadata): void;
  dequeue(): Uint8Array | undefined;
  size(): number;
}
class PriorityQueueStrategy implements IQueueStrategy {
  constructor(private queue: IPriorityQueue<Uint8Array>) {}
  enqueue(record: Uint8Array, metadata: IMessageMetadata): void {
    this.queue.enqueue(record, metadata.priority);
  }
  dequeue(): Uint8Array | undefined {
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
  enqueue(record: Uint8Array, metadata: IMessageMetadata): void {
    if (!metadata.ttd) throw new Error("TTD required for delayed messages");
    this.queue.enqueue(record, metadata.ttd);
    this.scheduleDelivery();
  }
  private scheduleDelivery(): void {
    if (this.nextTimeout) clearTimeout(this.nextTimeout);
    if (this.queue.isEmpty()) return;

    const record = this.queue.peek()!;
    const message = this.context.codec.decode(record);
    const { ttd, timestamp } = message.metadata;
    const delay = Math.max(0, timestamp + ttd! - Date.now());

    this.nextTimeout = setTimeout(() => {
      this.processQueue();
    }, delay);
  }

  private processQueue(): void {
    // while (!this.queue.isEmpty()) {
    const record = this.queue.dequeue()!;
    const { metadata } = this.context.codec.decode(record);
    this.context.eventBus.on("delayedMessageReady", { record, metadata });
    // }
    this.scheduleDelivery();
  }

  dequeue(): Uint8Array | undefined {
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
    private queue: IPriorityQueue<Uint8Array>,
    private eventBus: EventEmitter
  ) {
    eventBus.on("deadLetter", ({ record }) => this.addMessage(record));
  }
  addMessage(record: Uint8Array) {
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

    context.eventBus.on("delayedMessageReady", ({ record, metadata }) => {
      this.route(record, metadata);
    });
  }

  async route(record: Uint8Array, metadata: IMessageMetadata): Promise<void> {
    if (await this.pipeline.process(record, metadata)) return;

    const topic = this.topicRegistry.get(metadata.topic)!;
    topic.queues.enqueue(record, metadata);
    this.context.eventBus.emit("routed", { record, metadata });
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

  enqueue(record: Uint8Array, metadata: IMessageMetadata): void {
    const queue = this.getQueue(metadata.correlationId);
    queue.enqueue(record, metadata);
  }

  dequeue(consumerId: string): Uint8Array | undefined {
    const queue = this.unicastQueues.get(consumerId);
    if (queue?.size()) return queue.dequeue();
    return this.sharedQueue.dequeue();
  }
}
// SRC/TOPICS/METRICS.TS
class TopicMetricsCollector {
  private totalMessagesPublished = 0;
  private timestamp = Date.now();

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
      createdAt: new Date(this.timestamp).toISOString(),
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
  // config: {
  //   retentionMs?: number;           // How long messages are retained
  //   maxSizeBytes?: number;          // Max topic size
  //   partitions?: number;            // Number of partitions (if partitioned)
  // };
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

  send(
    producerId: string,
    record: Uint8Array,
    metadata: IMessageMetadata
  ): void {
    this.router.route(record, metadata);
    this.metrics.recordMessagePublished(producerId);
  }

  consume(consumerId: string): Message | undefined {
    const record = this.queues.dequeue(consumerId);
    if (!record) return;
    const message = this.context.codec.decode<Data>(record);
    const { timestamp, ttl } = message.metadata;

    if (ttl && timestamp + ttl <= Date.now()) {
      this.router.route(record, {
        ...message.metadata,
        attempts: Infinity,
      });
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

  // Message Routing

  route(record: Uint8Array, metadata: IMessageMetadata): void {
    // TODO: pre-routing logic (e.g., rate limiting)
    this.router.route(record, metadata);
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
class SizeValidator implements IValidator<Uint8Array> {
  constructor(private maxSize: number) {}

  validate({ record }: { record: Uint8Array }) {
    if (record.length > this.maxSize) throw new Error("Message too large");
  }
}
class MessageValidator<Data> implements IValidator<Data> {
  constructor(private schema: Zod.Schema<Data>) {}

  validate({ message }: { message: Message<Data> }) {
    this.schema?.parse(message);
  }
}
// SRC/PRODUCERS/MESSAGE_FACTORY.ts
class MessageFactory<Data> {
  constructor(private validators: IValidator<Data>[], private codec: ICodec) {}

  create(
    data: Data[],
    metadata: Partial<IMessageMetadata> & { topic: string }
  ): Array<{ message: Message<Data>; record: Uint8Array }> {
    return data.map((item, index) => {
      const message = new Message(item, {
        ...metadata,
        id: crypto.randomUUID(),
        ...(data.length > 1 && {
          batchId: `batch-${Date.now()}`,
          batchIndex: index,
          batchSize: data.length,
        }),
      });
      const record = this.codec.encode(message);
      this.validators.forEach((v) => v.validate({ message, record }));
      return { message, record };
    });
  }
}
// SRC/PRODUCERS/METRICS.TS
class ProducerMetrics {
  private messagesSent = 0;
  private lastMessageTimestamp = 0;

  constructor(private producerId: string, private eventBus: EventEmitter) {}

  recordMessage(count: number = 1): void {
    this.messagesSent += count;
    this.lastMessageTimestamp = Date.now();
    this.eventBus.emit("producerActivity", {
      producerId: this.producerId,
      messagesSent: this.messagesSent,
      timestamp: this.lastMessageTimestamp,
    });
  }

  getMetrics() {
    return {
      producerId: this.producerId,
      messagesSent: this.messagesSent,
      lastActivity: this.lastMessageTimestamp,
    };
  }
}
// SRC/PRODUCERS/PRODUCER.TS
interface MessageResult {
  messageId: string;
  status: "success" | "error";
  timestamp: number;
  error?: string;
}
interface SendResult {
  producerId: string;
  topic: string;
  sentAt: number;
  results: MessageResult[];
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
    data: Data[],
    metadata: Pick<
      IMessageMetadata,
      "priority" | "correlationId" | "ttd" | "ttl"
    >
  ): Promise<SendResult> {
    const { topic, producerId } = this.options;
    const results: MessageResult[] = [];
    const messages = this.messageFactory.create(data, {
      ...metadata,
      topic,
    });

    for (const { message, record } of messages) {
      const { id: messageId, timestamp } = message.metadata;
      try {
        this.topic.send(producerId, record, message.metadata);
        results.push({
          messageId,
          status: "success",
          timestamp,
        });
      } catch (error) {
        results.push({
          messageId,
          status: "error",
          error: error instanceof Error ? error.message : "Unknown error",
          timestamp: Date.now(),
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
  private pending = new Map<string, Message<Data>>();

  addToPending(msg: Message<Data>): void {
    this.pending.set(msg.metadata.id, msg);
  }

  getMessages(messageId?: string): Message<Data>[] {
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
    private fetcher: () => Message<Data>[],
    private onError?: (e?: Error) => void,
    private delayInterval = 1000
  ) {}

  async subscribe(
    handler: (messages: Message<Data>[]) => Promise<void>
  ): Promise<void> {
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
    const messages: Message<Data>[] = [];
    const now = Date.now();

    for (let i = 0; i < limit; i++) {
      const record = this.topic.consume(consumerId);
      if (!record) continue;

      const message = this.context.codec.decode<Data>(record);
      this.context.eventBus.emit("messageConsumed", { consumerId, message });
      message.metadata.consumedAt = now;
      this.metrics.recordMessageReceived();

      if (this.options.autoAck) {
        this.metrics.recordMessageProcessed(0);
      } else {
        this.ackManager.addToPending(message);
      }

      messages.push(message);
    }

    return messages;
  }

  subscribe(handler: (messages: Message<Data>[]) => Promise<void>): void {
    this.subscriptionManager.subscribe(handler);
  }

  unsubscribe(): void {
    this.subscriptionManager.unsubscribe();
  }

  ack(messageId?: string): void {
    const messages = this.ackManager.getMessages(messageId);
    const now = Date.now();

    messages.forEach(({ metadata }) => {
      this.ackManager.ack(metadata.id);
      this.metrics.recordMessageProcessed(now - (metadata.consumedAt || now));
    });
  }

  nack(messageId?: string, requeue = true): void {
    const messages = this.ackManager.getMessages(messageId);

    messages.forEach((message) => {
      this.metrics.recordMessageFailed();
      const { id, attempts } = message.metadata;
      message.metadata.attempts = requeue ? attempts + 1 : Infinity;
      const record = this.context.codec.encode(message);
      this.topic.send("", record, message.metadata);
      this.ackManager.ack(id);
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
    const limit = this.defaultOptions?.limit || options?.limit;
    const autoAck = this.defaultOptions?.autoAck || options?.autoAck;
    const pollingInterval =
      this.defaultOptions?.pollingInterval || options?.pollingInterval;

    return new Consumer<Data>(topic, this.context, {
      consumerId,
      topic: topicName,
      limit,
      autoAck,
      pollingInterval,
    });
  }
}
