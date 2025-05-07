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

//
//
//
// interfaces.ts
interface IPriorityQueue<Data = any> {
  enqueue(data: Data, priority?: number): void;
  dequeue(): Data | undefined;
  peek(): Data | undefined;
  isEmpty(): boolean;
  size(): number;
}
//
//
//
// ConsistencyHasher.ts
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
//
//
//
// src/codecs/JSONCodec.ts
interface ICodec {
  encode<T>(message: Message<T>): Uint8Array;
  decode<T>(bytes: Uint8Array): Message<T>;
}
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
      data: this.serializeData(data), // 👈 Custom data serializer
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
    // Use JSON as default, but can be overridden
    return new TextEncoder().encode(JSON.stringify(data));
  }

  private deserializeData<T>(bytes: Uint8Array): T {
    return JSON.parse(new TextDecoder().decode(bytes));
  }
}
//
//
//
// Message.ts
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
// src/validators.ts
interface IValidator<Data> {
  validate(data: { message: Message<Data>; record: Uint8Array }): void;
}
class SizeValidator implements IValidator<Uint8Array> {
  constructor(private maxSize: number) {}

  validate({ record }: { record: Uint8Array }) {
    if (record.length > this.maxSize) throw new Error("Message too large");
  }
}
class MessageValidator<Data> implements IValidator<Data> {
  constructor(private schema: Zod.Schema<Data>) {}

  validate({ message }: { message: Message<Data> }) {
    this.schema.parse(message);
  }
}
// src/factories/MessageFactory.ts
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
      const record = {
        message,
        record: this.codec.encode(message),
      };

      this.validators.forEach((v) => v.validate(record));
      return record;
    });
  }
}
//
//
//
// routing-engine.ts
interface IRoutingStrategy {
  getConsumerId(topic: string, key: string): string;
  addConsumer(topic: string, id: string): void;
  removeConsumer(topic: string, id: string): void;
}
class ConsistentHashingStrategy implements IRoutingStrategy {
  constructor(private hasher: IConsistencyHasher) {}

  getConsumerId(_topic: string, key: string) {
    return this.hasher.getNode(key)!;
  }
  addConsumer(_topic: string, id: string) {
    return this.hasher.addNode(id);
  }
  removeConsumer(_topic: string, id: string) {
    return this.hasher.removeNode(id);
  }
}
// Topic.ts
interface ITopicMetadata {
  name: string;
  createdAt: string;
  pendingMessages: number;
  producersCount: number;
  producersIds: string[];
  totalMessagesPublished: number;
  consumerCount: number;
  consumerIds: string[];
  lagPerConsumer?: Record<
    // Message lag per consumer (queue depth)
    string, // Consumer ID
    number // Pending messages
  >;
  // config: {
  //   retentionMs?: number;           // How long messages are retained
  //   maxSizeBytes?: number;          // Max topic size
  //   partitions?: number;            // Number of partitions (if partitioned)
  // };
  // subscriptionTypes: ("unicast" | "broadcast")[];
  // broadcastConsumerCount: number;    // Consumers listening to broadcasts
  // unicastConsumerCount: number;     // Consumers with dedicated queues
}
interface IClientMetadata {
  lastActiveAt: number;
  messageCount?: number;
  pendingMessages?: number;
}
class Topic<Data> {
  timestamp = Date.now();
  private totalMessagesPublished: number = 0;
  private producers = new Map<string, IClientMetadata>();
  private consumers = new Map<string, IClientMetadata>();
  unicastQueues: Map<string, IPriorityQueue<Uint8Array>> = new Map();

  constructor(
    public name: string,
    private router: MessageRouter,
    private routingStrategy: IRoutingStrategy,
    private sharedUnicastQueue: IPriorityQueue<Uint8Array>,
    private codec: ICodec
  ) {}

  getConsumerId(key: string) {
    return this.routingStrategy.getConsumerId(this.name, key);
  }
  addConsumer(id: string) {
    this.routingStrategy.addConsumer(this.name, id);
    this.consumers.set(id, { lastActiveAt: Date.now() });
  }
  removeConsumer(id: string) {
    this.unicastQueues.delete(id);
    this.routingStrategy.removeConsumer(this.name, id);
    this.consumers.delete(id);
  }
  addProducer(id: string) {
    this.producers.set(id, { lastActiveAt: Date.now() });
  }
  removeProducer(id: string) {
    this.producers.delete(id);
  }

  // message handling
  send(producerId: string, record: Uint8Array, metadata: IMessageMetadata) {
    this.router.route(record, metadata);
    if (producerId) this._recordProducerActivity(producerId);
  }
  consume(consumerId: string, limit = 1, signal?: AbortSignal) {
    if (signal?.aborted) return [];

    const messages: Message<Data>[] = [];
    const queues = [
      this.unicastQueues.get(consumerId),
      this.sharedUnicastQueue,
    ];

    for (const queue of queues) {
      while (queue && !queue.isEmpty() && messages.length < limit) {
        const record = queue.dequeue()!;
        const message = this.codec.decode<Data>(record);
        const { timestamp, ttl } = message.metadata;

        if (ttl && timestamp + ttl <= Date.now()) {
          this.router.route(record, {
            ...message.metadata,
            attempts: Infinity,
          });

          continue;
        }

        messages.push(message);
      }
    }

    this.recordConsumerLag(consumerId, messages.length);
    return messages;
  }

  // misc
  private _recordProducerActivity(producerId: string) {
    const messageCount = this.producers.get(producerId)?.messageCount || 0;
    this.totalMessagesPublished++;
    this.producers.set(producerId, {
      lastActiveAt: Date.now(),
      messageCount: messageCount + 1,
    });
  }
  recordConsumerLag(consumerId: string, count: number) {
    const pending = this.consumers.get(consumerId)?.pendingMessages || 0;
    this.consumers.set(consumerId, {
      lastActiveAt: Date.now(),
      pendingMessages: Math.max(0, pending - count),
    });
  }
  getMetadata(): ITopicMetadata {
    return {
      name: this.name,
      createdAt: new Date(this.timestamp).toISOString(),
      pendingMessages: this.sharedUnicastQueue.size(),
      producersCount: this.producers.size,
      producersIds: [...this.producers.keys()],
      totalMessagesPublished: this.totalMessagesPublished,
      consumerCount: this.consumers.size,
      consumerIds: [...this.consumers.keys()],
    };
  }
}
// TopicRegistry.ts (need sharding)
class TopicRegistry {
  private topics = new Map<string, Topic<any>>();

  constructor(
    private queueFactory: () => IPriorityQueue<Uint8Array>,
    private routingStrategy: () => IRoutingStrategy,
    private router: MessageRouter,
    private codec: ICodec
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
      new Topic<Data>(
        name,
        this.router,
        this.routingStrategy(),
        this.queueFactory(),
        this.codec
      )
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
// ClientManager.ts
class ClientManager {
  constructor(
    private topicRegistry: TopicRegistry,
    private queueFactory: () => IPriorityQueue<Uint8Array>
  ) {}

  registerConsumer(topicName: string, consumerId: string) {
    const topic = this.topicRegistry.getOrThrow(topicName);
    topic?.unicastQueues.set(consumerId, this.queueFactory());
    topic?.addConsumer(consumerId);
    // TODO: logs
  }

  unregisterConsumer(topicName: string, consumerId: string): void {
    const topic = this.topicRegistry.getOrThrow(topicName);
    topic?.unicastQueues.delete(consumerId);
    topic?.removeConsumer(consumerId);
    // TODO: logs
  }

  registerProducer(topicName: string, producerId: string) {
    const topic = this.topicRegistry.getOrThrow(topicName)!;
    topic?.addProducer(producerId);
    // TODO: logs
  }

  unregisterProducer(topicName: string, producerId: string): void {
    const topic = this.topicRegistry.getOrThrow(topicName)!;
    topic?.removeProducer(producerId);
    // TODO: logs and
  }
}
// DelayedQueueManager.ts
class DelayedQueueManager {
  private nextTimeout?: number;

  constructor(
    private queue: IPriorityQueue<Uint8Array>,
    private codec: ICodec,
    private onRoute: (record: Uint8Array, metadata: IMessageMetadata) => void
  ) {}

  addMessage(record: Uint8Array, ttd: number): void {
    this.queue.enqueue(record, ttd);
    this.scheduleDelivery();
  }

  private scheduleDelivery(): void {
    if (this.nextTimeout) clearTimeout(this.nextTimeout);
    if (this.queue.isEmpty()) return;

    const record = this.queue.peek()!;
    const message = this.codec.decode(record);
    const { ttd, timestamp } = message.metadata;
    const delay = Math.max(0, timestamp + ttd! - Date.now());

    this.nextTimeout = setTimeout(() => {
      this.processQueue();
    }, delay);
  }

  private processQueue(): void {
    while (!this.queue.isEmpty()) {
      const record = this.queue.dequeue()!;
      const message = this.codec.decode(record);
      this.onRoute(record, message.metadata);
    }

    this.scheduleDelivery();
  }
}
class DLQManager {
  constructor(private queue: IPriorityQueue<Uint8Array>) {}

  addMessage(record: Uint8Array) {
    this.queue.enqueue(record);
  }
}
// MessageRouter.ts
class MessageRouter {
  constructor(
    private topicRegistry: TopicRegistry,
    private delayedQueueManager: DelayedQueueManager,
    private dlqManager: DLQManager,
    private maxDeliveryAttempts: number,
    private eventBus?: EventEmitter
  ) {}

  route(record: Uint8Array, metadata: IMessageMetadata): void {
    if (this._isExpired(metadata)) {
      this.dlqManager.addMessage(record);
      this._log(metadata, {
        ttl: metadata.ttl,
        target: "DLQ",
        reason: "TTL_EXPIRED",
      });
      return;
    }

    if (this._isOutOfAttempts(metadata)) {
      this.dlqManager.addMessage(record);
      this._log(metadata, {
        attempts: metadata.attempts,
        target: "DLQ",
        reason: "MAX_ATTEMPTS",
      });
      return;
    }

    if (this._isDelayed(metadata)) {
      this.delayedQueueManager.addMessage(record, metadata.ttd!);
      this._log(metadata, {
        ttd: metadata.ttd,
        target: "DELAYED",
        reason: "TTD",
      });
      return;
    }

    this._routeToTopic(record, metadata);
  }

  private _isExpired({ ttl, timestamp }: IMessageMetadata): boolean {
    return !!ttl && timestamp + ttl <= Date.now();
  }

  private _isOutOfAttempts({ attempts }: IMessageMetadata): boolean {
    return attempts > this.maxDeliveryAttempts;
  }

  private _isDelayed({ ttd }: IMessageMetadata): boolean {
    return !!ttd && ttd > Date.now();
  }

  private _routeToTopic(record: Uint8Array, metadata: IMessageMetadata) {
    const { topic: topicName, correlationId, priority } = metadata;
    const topic = this.topicRegistry.get(topicName);
    if (!topic) {
      this.dlqManager.addMessage(record);
      this._log(metadata, {
        target: "DLQ",
        reason: "TOPIC_NOT_EXIST",
      });
      return;
    }

    if (correlationId) {
      const consumerId = topic.getConsumerId(correlationId)!;
      topic.unicastQueues.get(consumerId)?.enqueue(record, priority);
    } else {
      topic.sharedUnicastQueue.enqueue(record, priority);
    }

    this._log(metadata, { target: topic?.name });
  }

  private _log(
    metadata: IMessageMetadata,
    extraFields: Record<string, unknown>
  ) {
    this.eventBus?.emit("routed", {
      messageId: metadata.id,
      topic: metadata.topic,
      ...extraFields,
    });
  }
}
// TopicBroker.ts (Facade)
class TopicBroker {
  constructor(
    private readonly topicRegistry: TopicRegistry,
    private readonly clientManager: ClientManager,
    private readonly dlqManager: DLQManager,
    private readonly router: MessageRouter,
    private readonly eventBus: EventEmitter = new EventEmitter(),
    private readonly codec: ICodec = new JSONCodec()
  ) {}

  getEventBus() {
    return this.eventBus;
  }

  getCodec() {
    return this.codec;
  }

  route(record: Uint8Array, metadata: IMessageMetadata) {
    // TODO: pre-routing logic (e.g., rate limiting)
    this.router.route(record, metadata);
  }

  createTopic(name: string): void {
    this.topicRegistry.create(name);
  }

  getTopic(name: string) {
    return this.topicRegistry.get(name);
  }

  getTopicMetadata(name: string): ITopicMetadata | undefined {
    return this.topicRegistry.getOrThrow(name)?.getMetadata();
  }

  deleteTopic(name: string) {
    this.topicRegistry.delete(name);
  }

  registerConsumer(topicName: string, consumerId: string) {
    this.clientManager.registerConsumer(topicName, consumerId);
  }

  unregisterConsumer(topicName: string, consumerId: string) {
    this.clientManager.unregisterConsumer(topicName, consumerId);
  }

  registerProducer(topicName: string, producerId: string) {
    this.clientManager.registerProducer(topicName, producerId);
  }

  unregisterProducer(topicName: string, producerId: string) {
    this.clientManager.unregisterProducer(topicName, producerId);
  }
}
//
//
//
// Producer.ts (Facade)
class Producer<Data> {
  constructor(
    private readonly topic: Topic<Data>,
    private readonly messageFactory: MessageFactory<Data>,
    private readonly options: { producerId: string; topic: string }
  ) {}

  send(
    data: Data[],
    metadata: Pick<
      IMessageMetadata,
      "priority" | "correlationId" | "ttd" | "ttl"
    >
  ): number {
    const { topic, producerId } = this.options;
    const messages = this.messageFactory.create(data, {
      ...metadata,
      topic,
    });

    messages.forEach(({ message: { metadata }, record }) => {
      this.topic.send(producerId, record, metadata);
    });

    return messages.length;
  }
}
class ProducerFactory {
  private static iterator = 1;
  constructor(
    private readonly broker: TopicBroker,
    private readonly messageFactory: MessageFactory<any>
  ) {}

  create<Data>(topicName: string) {
    const producerId = `producer-${ProducerFactory.iterator++}`;
    this.broker.registerProducer(topicName, producerId);
    const topic = this.broker.getTopic(topicName)!;
    return new Producer<Data>(topic, this.messageFactory, {
      topic: topicName,
      producerId,
    });
  }
}
//
//
//
// AckManager.ts
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
// SubscriptionManager.ts
class SubscriptionManager<Data> {
  private isActive = false;
  private abortController = new AbortController();

  constructor(private interval = 1000) {}

  async subscribe(
    fetcher: () => Message<Data>[],
    handler: (messages: Message<Data>[]) => Promise<void>
  ): Promise<void> {
    this.isActive = true;

    while (this.isActive) {
      try {
        const messages = fetcher();
        if (messages.length > 0) await handler(messages);
        await wait(this.interval, this.abortController.signal);
      } catch (err) {
        if (err.name !== "AbortError") throw err;
      }
    }
  }

  unsubscribe(): void {
    this.isActive = false;
    this.abortController.abort();
  }
}
// src/consumers/Consumer.ts (Facade)
class Consumer<Data> {
  constructor(
    private topic: Topic<Data>,
    private ackManager: AckManager<Data>,
    private subscriptionManager: SubscriptionManager<Data>,
    private codec: ICodec,
    private eventBus: EventEmitter,
    private readonly options: {
      consumerId: string;
      topic: string;
      limit?: number;
      autoAck?: boolean;
    }
  ) {}

  consume(): Message<Data>[] {
    const { consumerId, limit } = this.options;
    const messages = this.topic.consume(consumerId, limit);
    messages.forEach(this.ackManager.addToPending);

    this.eventBus.emit("messagesConsumed", {
      messageCount: messages.length,
      ...this.options,
    });

    return messages;
  }

  subscribe(handler: (messages: Message<Data>[]) => Promise<void>): void {
    this.subscriptionManager.subscribe(this.consume, handler);
  }

  unsubscribe(): void {
    this.subscriptionManager.unsubscribe();
  }

  ack(messageId?: string): void {
    const ackedCount = this.ackManager.ack(messageId);
    this.topic.recordConsumerLag(this.options.consumerId, ackedCount);
  }

  nack(messageId?: string, requeue = true): void {
    const messages = this.ackManager.getMessages(messageId);

    messages.forEach((message) => {
      const record = this.codec.encode(message);
      this.topic.send("", record, {
        ...message.metadata,
        attempts: requeue ? message.metadata.attempts + 1 : Infinity,
      });
    });

    this.ackManager.ack(messageId);
  }
}
// src/factories/ConsumerFactory.ts
class ConsumerFactory {
  private static consumerIterator = 1;

  constructor(
    private readonly broker: TopicBroker,
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
    const consumerId = `consumer-${ConsumerFactory.consumerIterator++}`;
    this.broker.registerConsumer(topicName, consumerId);
    const topic = this.broker.getTopic(topicName)!;
    const limit = this.defaultOptions?.limit || options?.limit;
    const autoAck = this.defaultOptions?.autoAck || options?.autoAck;
    const pollingInterval =
      this.defaultOptions?.pollingInterval || options?.pollingInterval;

    return new Consumer<Data>(
      topic,
      new AckManager<Data>(),
      new SubscriptionManager<Data>(pollingInterval),
      this.broker.getCodec(),
      this.broker.getEventBus(),
      {
        consumerId,
        topic: topicName,
        limit,
        autoAck,
      }
    );
  }
}
