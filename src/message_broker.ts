import level, { LevelDB } from "level";
import crypto from "node:crypto";
import { setImmediate, clearImmediate } from "node:timers";
import { wait, uniqueIntGenerator } from "./utils";
import fs from "fs/promises";
import path from "path";
import Buffer from "node:buffer";
import Ajv, { JSONSchemaType } from "ajv";
import { HighCapacityBinaryHeapPriorityQueue } from "../queues";
import { ICodec } from "./codec/binary_codec";
import { ThreadedBinaryCodec } from "./codec/binary_codec.threaded";
//
//
//
// HASH_RING *******************************************************
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
// SRC/HASH_RING
interface IHashRing {
  addNode(id: number): void;
  removeNode(id: number): void;
  getNode(key: string): Generator<number, void, unknown>;
}
/** Hash ring.
 * The system works regardless of how different the key hashes are because the lookup is always relative to the fixed node positions on the ring.
 * Sorted nodes in a ring: [**100(A)**, _180(user-123 key hash always belong to the B)_, **200(B)**, **300(A)**, **400(B)**, **500(A)**, **600(B)**]
 */
class InMemoryHashRing implements IHashRing {
  private sortedHashes: number[] = [];
  private hashToNodeMap = new Map<number, number>();

  /**
   * Create a new instance of HashRing with the given number of virtual nodes.
   * @param {number} [replicas=3] The number of virtual nodes to create for each node.
   * The more virtual nodes gives you fewer hotspots, more balanced traffic. However, setting
   * this number too high can lead to a large memory footprint and slower lookups.
   */
  constructor(private hashService: IHashService, private replicas = 3) {}

  addNode(id: number): void {
    for (let i = 0; i < this.replicas; i++) {
      const hash = this.hashService.hash(`${id}-${i}`);
      this.hashToNodeMap.set(hash, id);
      this.insertHash(hash);
    }
  }

  removeNode(id: number): void {
    // Find and remove all virtual nodes for the given ID
    const hashesToRemove = Array.from(this.hashToNodeMap.entries())
      .filter(([_, nodeId]) => nodeId === id)
      .map(([hash]) => hash);

    for (const hash of hashesToRemove) {
      this.hashToNodeMap.delete(hash);
      this.removeHash(hash);
    }
  }

  *getNode(key: string): Generator<number, void, unknown> {
    if (this.sortedHashes.length === 0) {
      throw new Error("No nodes available in the hash ring");
    }

    let nodeCount = this.sortedHashes.length;
    const keyHash = this.hashService.hash(key);
    let currentIndex = this.findNodeIndex(keyHash);

    while (--nodeCount > 0) {
      yield this.hashToNodeMap.get(this.sortedHashes[currentIndex])!;
      currentIndex = (currentIndex + 1) % this.sortedHashes.length;
    }
  }

  // --- Inlined search helpers ---
  private findNodeIndex(keyHash: number): number {
    let low = 0;
    let high = this.sortedHashes.length - 1;

    while (low <= high) {
      const mid = (low + high) >>> 1; // Bitwise floor division
      if (this.sortedHashes[mid] < keyHash) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return low % this.sortedHashes.length; // Wrap around
  }

  private insertHash(hash: number): void {
    const index = this.findInsertIndex(hash);
    this.sortedHashes.splice(index, 0, hash);
  }

  private removeHash(hash: number): void {
    const index = this.sortedHashes.indexOf(hash);
    if (index !== -1) {
      this.sortedHashes.splice(index, 1);
    }
  }

  private findInsertIndex(hash: number): number {
    let low = 0;
    let high = this.sortedHashes.length;

    while (low < high) {
      const mid = (low + high) >>> 1;
      if (this.sortedHashes[mid] < hash) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low;
  }
}
//
//
//
//
// MESSAGE *********************************************************
// SRC/MESSAGE/METADATA.TS
export class MessageMetadata {
  // Fixed-width fields
  id: number = 0; // 4 bytes
  ts: number = Date.now(); // 8 bytes (double)
  producerId: number = 0; // 4 bytes
  priority?: number; // 1 byte (0-255)
  ttl?: number; // 4 bytes
  ttd?: number; // 4 bytes
  batchId?: number; // 4 bytes
  batchIdx?: number; // 2 bytes
  batchSize?: number; // 2 bytes
  attempts: number = 1; // 1 byte
  consumedAt?: number; // 8 bytes
  size: number = 0;
  needAcks: number = 0;

  // Variable-width fields
  topic: string = "";
  correlationId?: string;
  routingKey?: string;

  // Bit flags for optional fields (1 byte)
  get flags(): number {
    return (
      (this.priority !== undefined ? 0x01 : 0) |
      (this.ttl !== undefined ? 0x02 : 0) |
      (this.ttd !== undefined ? 0x04 : 0) |
      (this.batchId !== undefined ? 0x08 : 0) |
      (this.correlationId !== undefined ? 0x10 : 0) |
      (this.routingKey !== undefined ? 0x20 : 0)
    );
  }
}
// SRC/MESSAGE/VALIDATORS/
interface IMessageValidator<Data> {
  validate(data: { data: Data; meta: MessageMetadata }): void;
}
class SchemaValidator<Data> implements IMessageValidator<Data> {
  constructor(private schemaValidator: (data: any) => boolean) {}

  validate({ data }): void {
    if (!this.schemaValidator(data)) {
      // @ts-ignore
      throw new Error(this.schemaValidator.errors);
    }
  }
}
class SizeValidator implements IMessageValidator<any> {
  constructor(private maxSize: number) {}

  validate({ meta }) {
    if (meta.size > this.maxSize) throw new Error("Message too large");
  }
}
class CapacityValidator implements IMessageValidator<any> {
  constructor(
    private topicMaxCapacity: number,
    private getTopicCapacity: () => number
  ) {}
  validate({ meta }) {
    if (this.getTopicCapacity() + meta.size > this.topicMaxCapacity) {
      throw new Error(`Exceeds topic max size ${this.topicMaxCapacity}`);
    }
  }
}
// SRC/MESSAGE/MESSAGE_FACTORY.ts
type MetadataInput = Pick<
  MessageMetadata,
  "priority" | "correlationId" | "ttd" | "ttl"
>;
class MessageFactory<Data> {
  constructor(
    private codec: ICodec,
    private validators: IMessageValidator<Data>[]
  ) {}

  async create(
    batch: Data[],
    metadataInput: MetadataInput & { topic: string; producerId: number }
  ) {
    const batchId = Date.now();
    return Promise.all(
      batch.map(async (data, index) => {
        const meta = new MessageMetadata();
        Object.assign(meta, metadataInput);
        meta.id = uniqueIntGenerator();
        meta.ts = Date.now();
        meta.attempts = 1;
        meta.needAcks = 0;

        if (batch.length > 1) {
          meta.batchId = batchId;
          meta.batchIdx = index;
          meta.batchSize = batch.length;
        }

        try {
          const message = await this.codec.encode(data);
          meta.size = message.byteLength;
          this.validators.forEach((v) => v.validate({ data, meta }));
          return { meta, message, error: null };
        } catch (error) {
          return { meta, error, message: null };
        }
      })
    );
  }
}
//
//
//
//
// PRODUCER ********************************************************
interface ICanPublish {
  name: string;
  publish: (
    producerId: number,
    message: Buffer,
    meta: MessageMetadata
  ) => Promise<void>;
  recordClientActivity(
    clientId: number,
    activityRecord: Partial<ITopicClientState>
  ): void;
}
// SRC/PRODUCERS/PRODUCER.TS (Facade)
class Producer<Data> {
  constructor(
    private readonly topic: ICanPublish,
    private readonly messageFactory: MessageFactory<Data>,
    private readonly id: number
  ) {}

  async publish(batch: Data[], metadata: MetadataInput = {}) {
    const start = Date.now();
    let messagesSent = 0;
    const results: {
      id: number;
      status: "success" | "error";
      ts: number;
      error?: string;
    }[] = [];

    this.topic.recordClientActivity(this.id, {
      status: "active",
      pendingMessages: batch.length,
    });

    const messages = await this.messageFactory.create(batch, {
      ...metadata,
      topic: this.topic.name,
      producerId: this.id,
    });

    for (const { message, meta, error } of messages) {
      const { id, ts } = meta;

      if (error) {
        const err = error instanceof Error ? error.message : "Unknown error";
        results.push({ id, ts, error: err, status: "error" });
        continue;
      }

      try {
        await this.topic.publish(this.id, message, meta);
        results.push({ id, ts, status: "success" });
        messagesSent++;
      } catch (err) {
        const error = err instanceof Error ? err.message : "Unknown error";
        results.push({ id, ts, error, status: "error" });
      }
    }

    this.topic.recordClientActivity(this.id, {
      messageCount: messagesSent,
      pendingMessages: -batch.length,
      processingTime: Date.now() - start,
      status: "idle",
    });

    return results;
  }
}
// SRC/PRODUCERS/PRODUCER_FACTORY
class ProducerFactory<Data> {
  private messageFactory: MessageFactory<Data>;
  constructor(
    codec: ICodec,
    getTopicCapacity: () => number,
    schemaValidator?: (data: any) => boolean,
    maxMessageSize?: number,
    maxSizeBytes?: number
  ) {
    const validators: IMessageValidator<Data>[] = [];
    if (schemaValidator) validators.push(new SchemaValidator(schemaValidator));
    if (maxMessageSize) validators.push(new SizeValidator(maxMessageSize));
    if (maxSizeBytes) {
      validators.push(new CapacityValidator(maxSizeBytes, getTopicCapacity));
    }

    this.messageFactory = new MessageFactory<Data>(codec, validators);
  }

  create(publisher: ICanPublish, id = uniqueIntGenerator()) {
    return new Producer(publisher, this.messageFactory, id);
  }
}
//
//
//
//
// CONSUMER ********************************************************
// SRC/CONSUMERS/SUBSCRIPTION_MANAGER.TS
class SubscriptionManager<Data> {
  private isActive = false;
  private abortController = new AbortController();

  constructor(private delayInterval = 1000) {}

  async subscribe(
    handler: (messages: Data[]) => Promise<void>,
    fetcher: () => Promise<Data[]>,
    onError?: (e?: Error) => void
  ): Promise<void> {
    this.isActive = true;

    while (this.isActive) {
      try {
        const messages = await fetcher();
        if (messages.length > 0) {
          await handler(messages);
        } else {
          await wait(this.delayInterval, this.abortController.signal);
        }
      } catch (err) {
        onError?.(err);
        if (err.name !== "AbortError") throw err;
      }
    }
  }

  unsubscribe(): void {
    this.isActive = false;
    this.abortController.abort();
  }
}
// SRC/CONSUMERS/CONSUMER.TS (Facade)
interface ICanConsume<Data> {
  consume(consumerId: number, autoAck?: boolean): Promise<Data | undefined>;
  ack(consumerId: number, messageId?: number): Promise<number[]>;
  nack(
    consumerId: number,
    messageId?: number,
    requeue?: boolean
  ): Promise<number[]>;
  recordClientActivity(
    clientId: number,
    activityRecord: Partial<ITopicClientState>
  ): void;
}
class Consumer<Data> {
  private readonly limit: number;
  private lastConsumptionTs: number;
  constructor(
    private readonly topic: ICanConsume<Data>,
    private readonly subscriptionManager: SubscriptionManager<Data>,
    private readonly id: number,
    private readonly autoAck?: boolean,
    limit?: number
  ) {
    this.limit = Math.max(1, limit!);
  }

  async consume() {
    const messages: Data[] = [];

    for (let i = 0; i < this.limit; i++) {
      const message = await this.topic.consume(this.id, this.autoAck);
      if (!message) break;
      messages.push(message);
    }

    this.lastConsumptionTs = Date.now();

    if (this.autoAck) {
      this.topic.recordClientActivity(this.id, {
        messageCount: messages.length,
        pendingMessages: 0,
        processingTime: 0,
        status: "idle",
      });
    } else {
      this.topic.recordClientActivity(this.id, {
        pendingMessages: messages.length,
        status: "active",
      });
    }

    return messages;
  }

  async ack(messageId?: number) {
    const now = Date.now();
    const ackedMessages = await this.topic.ack(this.id, messageId);

    this.topic.recordClientActivity(this.id, {
      pendingMessages: -ackedMessages.length,
      messageCount: ackedMessages.length,
      processingTime: now - (this.lastConsumptionTs || now),
      status: "idle",
    });
  }

  async nack(messageId?: number, requeue = true): Promise<void> {
    const nackedMessages = await this.topic.nack(this.id, messageId, requeue);

    const now = Date.now();
    this.topic.recordClientActivity(this.id, {
      pendingMessages: -nackedMessages.length,
      processingTime: now - (this.lastConsumptionTs || now),
      status: "idle",
    });
  }

  subscribe(handler: (messages: Data[]) => Promise<void>): void {
    this.subscriptionManager.subscribe(handler, this.consume);
  }

  unsubscribe(): void {
    this.subscriptionManager.unsubscribe();
  }
}
// SRC/CONSUMERS/CONSUMER_FACTORY
class ConsumerFactory<Data> {
  create(
    consumer: ICanConsume<Data>,
    id = uniqueIntGenerator(),
    options?: {
      limit?: number;
      autoAck?: boolean;
      pollingInterval?: number;
    }
  ) {
    const { limit, autoAck, pollingInterval } = options || {};
    const subscriptionManager = new SubscriptionManager<Data>(pollingInterval);
    return new Consumer(consumer, subscriptionManager, id, autoAck, limit);
  }
}
// SRC/CONSUMERS/DLQ_CONSUMER.TS (Facade)
interface ICanConsumeDLQ<Data> {
  createDlqReader(
    consumerId: number
  ): AsyncGenerator<DLQEntry<Data>, void, unknown>;
  replayDlq(
    consumerId: number,
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ): Promise<number>;
  recordClientActivity(
    clientId: number,
    activityRecord: Partial<ITopicClientState>
  ): void;
}
class DLQConsumer<Data> {
  private readonly limit: number;
  private reader: AsyncGenerator<DLQEntry<Data>, void, unknown>;
  constructor(
    private readonly topic: ICanConsumeDLQ<Data>,
    private readonly subscriptionManager: SubscriptionManager<DLQEntry<Data>>,
    private readonly id: number,
    limit?: number
  ) {
    this.limit = Math.max(1, limit!);
    // singleton reader allows you to read only once, waiting for the newest messages to arrive
    this.reader = this.topic.createDlqReader(this.id);
  }

  async consume() {
    const messages: DLQEntry<Data>[] = [];

    for await (const message of this.reader) {
      if (!message) break;
      messages.push(message);
      if (messages.length == this.limit) break;
    }

    return messages;
  }

  async replayDlq(
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ) {
    const start = Date.now();
    this.topic.recordClientActivity(this.id, {
      status: "active",
    });

    const replayedCount = await this.topic.replayDlq(this.id, handler, filter);

    this.topic.recordClientActivity(this.id, {
      messageCount: replayedCount,
      processingTime: Date.now() - start,
      status: "idle",
    });
    return replayedCount;
  }

  subscribe(handler: (messages: DLQEntry<Data>[]) => Promise<void>): void {
    this.subscriptionManager.subscribe(handler, this.consume);
  }

  unsubscribe(): void {
    this.subscriptionManager.unsubscribe();
  }
}
// SRC/CONSUMERS/DLQ_CONSUMER_FACTORY
class DLQConsumerFactory<Data> {
  create(
    consumer: ICanConsumeDLQ<Data>,
    id = uniqueIntGenerator(),
    options?: {
      limit?: number;
      pollingInterval?: number;
    }
  ) {
    const { limit, pollingInterval } = options || {};
    const subscriptionManager = new SubscriptionManager<DLQEntry<Data>>(
      pollingInterval
    );
    return new DLQConsumer(consumer, subscriptionManager, id, limit);
  }
}
//
//
//
//
// MESSAGE_STORAGE *************************************************
interface IMessageStorage<Data> {
  writeAll(message: Buffer, meta: MessageMetadata): Promise<number>;
  readAll(
    id: number
  ): Promise<
    [
      Awaited<Data> | undefined,
      Pick<MessageMetadata, keyof MessageMetadata> | undefined
    ]
  >;
  readMessage(id: number): Promise<Data | undefined>;
  readMetadata<K extends keyof MessageMetadata>(
    id: number,
    keys?: K[]
  ): Promise<Pick<MessageMetadata, K> | undefined>;
  updateMetadata(id: number, meta: Partial<MessageMetadata>): Promise<void>;
  flush(): Promise<void>;
}
// SRC/STORAGE/MESSAGE_LOG_MANAGER.TS
class LevelDBMessageStorage<Data> implements IMessageStorage<Data> {
  // separates metadata/messages buffers since we: 1. need read only data/metadata, 2. need update only metadata, 3. need often read metadata (retention timer)
  private messages = new Map<number, Buffer<Data>>();
  private metadatas = new Map<number, Buffer<MessageMetadata>>();
  private flushId?: number;

  constructor(
    private topic: string,
    private codec: ICodec,
    private retentionMs = 86_400_000, // 1day
    private isPersist = true,
    private persistThresholdMs = 100,
    private chunkSize = 50
  ) {
    // TODO: timeout retention check meta:
    // meta.consumedAt ====> delete
    // meta.ts + meta.ttl < now ====> DLQ
    // (non-expired & non-consumed) && now - meta.ts >= retentionMs ====> DLQ
  }

  async writeAll(message: Buffer, meta: MessageMetadata): Promise<number> {
    const encodedMeta = await this.codec.encodeMetadata(meta);
    this.messages.set(meta.id, message);
    this.metadatas.set(meta.id, encodedMeta);

    this.scheduleFlush();
    return this.messages.size;
  }

  async readAll(id: number) {
    return Promise.all([this.readMessage(id), this.readMetadata(id)]);
  }

  async readMessage(id: number) {
    const buffer = this.messages.get(id);
    if (!buffer) return;
    return this.codec.decode<Data>(buffer);
  }

  async readMetadata<K extends keyof MessageMetadata>(id: number, keys?: K[]) {
    const buffer = this.metadatas.get(id);
    if (!buffer) return;
    return this.codec.decodeMetadata(buffer, keys);
  }

  async updateMetadata(id: number, meta: Partial<MessageMetadata>) {
    const buffer = this.metadatas.get(id);
    if (!buffer) return;
    const newBuffer = this.codec.updateMetadata(buffer, meta);
    this.metadatas.set(id, newBuffer);
  }

  private scheduleFlush() {
    if (!this.isPersist) return;
    this.flushId ??= setImmediate(this.flush, this.persistThresholdMs);
  }

  flush = async () => {
    this.flushId = undefined;
    let count = 0;
    const idIterator = this.metadatas.keys();

    for (let id of idIterator) {
      if (this.chunkSize && count >= this.chunkSize) break;
      // leveldb put
      this.messages.delete(id);
      this.metadatas.delete(id);
      count++;
    }

    if (this.metadatas.size > 0) {
      this.scheduleFlush();
    }
  };
}
//
//
//
//
// PIPELINE ********************************************************
// SRC/PIPELINE/PROCESSOR.TS
interface IMessageProcessor {
  process(meta: MessageMetadata): boolean;
}
// SRC/PIPELINE/PROCESSORS/
class ExpirationProcessor<Data> implements IMessageProcessor {
  constructor(private dlq: TopicDLQManager<Data>) {}
  process(meta: MessageMetadata): boolean {
    if (!meta.ttl) return false;
    const isExpired =
      meta.ts + meta.ttl <= Date.now() || !!(meta.ttd && meta.ttd >= meta.ttl);
    if (isExpired) this.dlq.publish(meta, "expired");
    return isExpired;
  }
}
class AttemptsProcessor<Data> implements IMessageProcessor {
  constructor(
    private dlq: TopicDLQManager<Data>,
    private maxAttempts: number
  ) {}
  process(meta: MessageMetadata): boolean {
    const shouldDeadLetter = meta.attempts > this.maxAttempts;
    if (shouldDeadLetter) {
      this.dlq.publish(meta, "max_attempts");
    }
    return shouldDeadLetter;
  }
}
class DelayProcessor<Data> implements IMessageProcessor {
  constructor(private delayedQueue: TopicDelayedQueueManager<Data>) {}
  process(meta: MessageMetadata): boolean {
    if (!meta.ttd) return false;
    const shouldDelay = meta.ts + meta.ttd > Date.now();
    if (shouldDelay) this.delayedQueue.publish(meta);
    return shouldDelay;
  }
}
// SRC/PIPELINE/MESSAGE_PIPELINE.TS
class MessagePipeline {
  private processors: IMessageProcessor[] = [];
  addProcessor(processor: IMessageProcessor): void {
    this.processors.push(processor);
  }
  process(meta: MessageMetadata): boolean {
    for (const processor of this.processors) {
      if (processor.process(meta)) return true;
    }
    return false;
  }
}
// SRC/PIPELINE/PIPELINE_FACTORY.TS
class PipelineFactory<Data> {
  create(
    dlqManager: TopicDLQManager<Data>,
    delayedQueue: TopicDelayedQueueManager<Data>,
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
// TOPIC **********************************************************
interface IPriorityQueue<Data = any> {
  enqueue(data: Data, priority?: number): void;
  dequeue(): Data | undefined;
  peek(): Data | undefined;
  isEmpty(): boolean;
  size(): number;
}
// SRC/TOPIC/METRICS.TS
class TopicMetricsCollector {
  private totalMessagesPublished = 0;
  private totalBytes = 0;
  private ts = Date.now();
  private depth = 0;
  private enqueueRate = 0;
  private dequeueRate = 0;
  private avgLatencyMs = 0; // Time in queue

  recordEnqueue(byteSize: number, latencyMs: number): void {
    this.totalMessagesPublished++;
    this.totalBytes += byteSize;
    this.depth += 1;
    this.enqueueRate += 1;
    this.updateAvgLatency(latencyMs);
  }

  recordDequeue(latencyMs: number): void {
    this.depth -= 1;
    this.dequeueRate += 1;
    this.updateAvgLatency(latencyMs);
  }

  private updateAvgLatency(latencyMs: number) {
    this.avgLatencyMs = this.avgLatencyMs * 0.9 + latencyMs * 0.1; // Exponential moving average
  }

  getMetrics() {
    return {
      ts: this.ts,
      totalMessagesPublished: this.totalMessagesPublished,
      totalBytes: this.totalBytes,
      depth: this.depth,
      enqueueRate: this.enqueueRate,
      dequeueRate: this.dequeueRate,
      avgLatencyMs: this.avgLatencyMs,
    };
  }
}
// SRC/TOPIC/CLIENT_REGISTRY.TS
interface ITopicClientState {
  id: number;
  clientType: "producer" | "consumer" | "dlq_consumer";
  registeredAt: number;
  lastActiveAt: number;
  // Metrics
  status: "active" | "idle" | "lagging";
  messageCount: number;
  processingTime: number;
  avgProcessingTime: number;
  pendingMessages: number;
  groupId?: string;
}
class TopicClientManager {
  // TODO: persist
  private clients = new Map<number, ITopicClientState>();
  private consumerGroups = new Map<string | undefined, Set<number>>();

  constructor(
    private inactivityThresholdMs = 300_000,
    private processingTimeThresholdMs = 50_000,
    private pendingThresholdMs = 100
  ) {}

  addClient(
    type: ITopicClientState["clientType"],
    id = uniqueIntGenerator(),
    groupId?: string
  ) {
    const now = Date.now();
    this.clients.set(id, {
      registeredAt: now,
      lastActiveAt: now,
      clientType: type,
      messageCount: 0,
      pendingMessages: 0,
      processingTime: 0,
      avgProcessingTime: 0,
      status: "active",
      groupId,
      id,
    });

    if (type === "consumer") {
      if (!this.consumerGroups.has(groupId)) {
        this.consumerGroups.set(groupId, new Set());
      }
      this.consumerGroups.get(groupId)?.add(id);
    }

    this[`total${type[0].toUpperCase() + type.slice(1)}s`]++;
    return this.clients.get(id);
  }

  removeClient(id: number) {
    const client = this.clients.get(id);
    if (!client) return;
    const type = client.clientType;

    if (type === "consumer" && client.groupId) {
      this.consumerGroups.get(client.groupId)?.delete(id);
      if (!this.consumerGroups.get(client.groupId)?.size) {
        this.consumerGroups.delete(client.groupId);
      }
    }

    this[`total${type[0].toUpperCase() + type.slice(1)}s`]--;
    this.clients.delete(id);
    return this.clients.size;
  }

  getClients(filter?: (client: ITopicClientState) => boolean) {
    const states = this.clients.values();
    if (!filter) states;
    const results = new Set<ITopicClientState>();
    for (const state of states) {
      if (filter?.(state)) results.add(state);
    }
    return results;
  }

  getClient(clientId: number) {
    return this.clients.get(clientId);
  }

  getConsumerGroups() {
    return this.consumerGroups;
  }

  validateClient(id: number, expectedType?: ITopicClientState["clientType"]) {
    const metadata = this.clients.get(id);
    if (!metadata) {
      throw new Error(`Client with ID ${id} not found`);
    }
    if (expectedType && metadata.clientType !== expectedType) {
      throw new Error(`Client with ID ${id} is not a ${expectedType}`);
    }
  }

  isOperable(id: number, now: number) {
    const client = this.getClient(id);
    if (!client) return false;
    if (client.status == "lagging") return false;
    if (client.avgProcessingTime > this.processingTimeThresholdMs) return false;
    if (client.pendingMessages > this.pendingThresholdMs) return false;
    return now - client.lastActiveAt < this.inactivityThresholdMs;
  }

  isIdle(id: number) {
    const client = this.getClient(id);
    if (!client) return false;
    return client.status === "idle";
  }

  recordActivity(clientId: number, activityRecord: Partial<ITopicClientState>) {
    if (!this.clients.has(clientId)) return;
    const client = this.clients.get(clientId)!;
    client.lastActiveAt = Date.now();

    for (let key in activityRecord) {
      if (typeof client[key] != "number") {
        client[key] = activityRecord[key];
      } else {
        client[key] = client[key] + activityRecord[key];
      }
    }

    if (client.messageCount > 0) {
      client.avgProcessingTime = client.processingTime / client.messageCount;
    }

    this.clients.set(clientId, client);
  }

  getMetadata() {
    return {
      totalClientsSize: this.clients.size,
    };
  }
}
class TopicAckManager<Data> {
  // TODO: persist
  private pendingMessages = new Map<number, Map<number, number>>();
  private awaitedAcksCount = new Map<number, number>();
  private timer?: number;

  constructor(
    private pipeline: MessagePipeline,
    private messageStorage: IMessageStorage<Data>,
    private queueManager: TopicQueueManager,
    private metrics: TopicMetricsCollector,
    private logger?: LogCollector,
    private ackTimeoutMs: number = 30_000
  ) {
    // Check more frequently than ackTimeoutMs
    this.timer = setInterval(
      this.nackTimedOutPendings,
      Math.max(1000, this.ackTimeoutMs / 2)
    );
  }

  private nackTimedOutPendings = async () => {
    const now = Date.now();
    for (const [consumerId, messages] of this.pendingMessages.entries()) {
      for (const [messageId, consumedAt] of messages.entries()) {
        if (now - consumedAt > this.ackTimeoutMs) {
          await this.nack(consumerId, messageId, true);
        }
      }
    }
  };

  async decrementAwaitedAcks(messageId: number) {
    let neededAcks = this.awaitedAcksCount.get(messageId);
    if (!neededAcks) return;
    this.awaitedAcksCount.set(messageId, --neededAcks);
    if (neededAcks > 0) return;

    const meta = await this.messageStorage.readMetadata(messageId, ["ts"]);
    if (!meta) return;
    const consumedAt = Date.now();
    await this.messageStorage.updateMetadata(messageId, { consumedAt });
    this.metrics.recordDequeue(consumedAt - meta.ts);
  }

  setAwaitedAcksCount(messageid: number, awaitedAcks: number) {
    this.awaitedAcksCount.set(messageid, awaitedAcks);
  }

  async ack(consumerId: number, messageId?: number) {
    const pendingMessages: number[] = [];

    if (messageId) {
      pendingMessages.push(messageId);
      this.removePending(consumerId, messageId);
    } else {
      const pendingMap = this.pendingMessages.get(consumerId)!;
      pendingMessages.push(...pendingMap.keys());
      this.removePending(consumerId);
    }

    for (const messageId of pendingMessages) {
      await this.decrementAwaitedAcks(messageId);
    }

    return pendingMessages;
  }

  async nack(consumerId: number, messageId?: number, requeue = true) {
    const messages = await this.ack(consumerId, messageId);

    for (const messageId of messages) {
      const meta = await this.messageStorage.readMetadata(messageId);
      if (!meta) continue;
      await this.messageStorage.updateMetadata(messageId, {
        attempts: requeue ? meta.attempts + 1 : Infinity,
        consumedAt: undefined,
      });

      if (this.pipeline.process(meta)) continue;

      this.queueManager.enqueue(consumerId, meta);
      this.logger?.log(
        `Message is nacked to ${requeue ? "queue" : "DLQ"}.`,
        meta
      );
    }

    return messages;
  }

  addPending(consumerId: number, messageId: number): void {
    if (!this.pendingMessages.has(consumerId)) {
      this.pendingMessages.set(consumerId, new Map());
    }
    this.pendingMessages.get(consumerId)?.set(messageId, Date.now());
  }

  removePending(consumerId: number, messageId?: number): void {
    if (messageId) {
      this.pendingMessages.get(consumerId)?.delete(messageId);
    } else {
      this.pendingMessages.get(consumerId)?.clear();
    }

    if (this.pendingMessages.get(consumerId)?.size == 0) {
      this.pendingMessages.delete(consumerId);
    }
  }

  getMetadata() {
    return {};
  }
}
class TopicSubscriptionManager<
  Data,
  Listener = (message: Data) => Promise<void>
> {
  private subscriptions = new Map<number, Listener>();

  constructor(
    private clientManager: TopicClientManager,
    private queueManager: TopicQueueManager
  ) {}

  addListener(consumerId: number, listener: Listener) {
    this.subscriptions.set(consumerId, listener);
    // drain queue
  }

  removeListener(consumerId: number) {
    this.subscriptions.delete(consumerId);
  }

  getListener(consumerId: number) {
    return this.subscriptions.get(consumerId);
  }
}
// SRC/TOPIC/ROUTER.ts
class TopicMessageRouter<Data> {
  private groupHashRingRegistry = new Map<string | undefined, IHashRing>();
  private keySubscriptions = new Map<number, Set<string>>();

  constructor(
    private clientManager: TopicClientManager,
    private queueManager: TopicQueueManager,
    private dlqManager: TopicDLQManager<Data>
  ) {}

  addRoutingEntry(
    consumerId: number,
    groupId?: string,
    routingKeys?: string[]
  ) {
    if (routingKeys?.length) {
      this.keySubscriptions.set(consumerId, new Set(routingKeys));
    }

    if (!groupId) return;
    if (!this.groupHashRingRegistry.has(groupId)) {
      this.groupHashRingRegistry.set(
        groupId,
        new InMemoryHashRing(new SHA256HashService())
      );
    }
    this.groupHashRingRegistry.get(groupId)?.addNode(consumerId);
  }

  removeRoutingEntry(consumerId: number) {
    this.keySubscriptions.delete(consumerId);

    const client = this.clientManager.getClient(consumerId);
    if (!client?.groupId) return;
    this.groupHashRingRegistry.get(client.groupId)?.removeNode(consumerId);
    // TODO: if there is no node left remove InMemoryHashRing for group
  }

  route(meta: MessageMetadata) {
    const consumerGroups = this.clientManager.getConsumerGroups();
    const { routingKey, correlationId, id } = meta;
    const now = Date.now();
    let deliveredCount = 0;

    // fanout O(n2)
    for (const [groupId, members] of consumerGroups.entries()) {
      const isSingleConsumer = groupId || correlationId;
      let candidates: Iterable<number> = members;
      let fallbackCandidate: number | null = null;

      if (isSingleConsumer) {
        // we use traversal operable node lookup (O(n)) instead of constantly removing/adding non-operable ones (O(n2))
        candidates = this.groupHashRingRegistry
          .get(groupId)!
          .getNode(correlationId || id.toString());
      }

      for (const candidateId of candidates) {
        if (!this.clientManager.isOperable(candidateId, now)) continue;
        const routingKeys = this.keySubscriptions.get(candidateId);
        // filtered-out
        if (routingKeys && !routingKeys.has(routingKey!)) continue;
        // prefer idle consumer for single consumer mode
        if (isSingleConsumer && !this.clientManager.isIdle(candidateId)) {
          fallbackCandidate ??= candidateId;
          continue;
        }

        this.queueManager.enqueue(candidateId, meta);
        deliveredCount++;
        fallbackCandidate = null;
        if (isSingleConsumer) break;
      }

      // falback for single consumer mode
      if (fallbackCandidate) {
        this.queueManager.enqueue(fallbackCandidate, meta);
        deliveredCount++;
      }
    }

    // all consumers are nonOperable or binded to other routingKeys
    if (!deliveredCount) {
      this.dlqManager.publish(meta, "no_consumers");
    }

    return deliveredCount;
  }
}
// SRC/TOPIC/QUEUE_MANAGER.TS
class TopicQueueManager {
  // TODO: persist
  private queues = new Map<number, IPriorityQueue<number>>();
  private totalQueuedMessages: 0;

  addConsumerQueue(consumerId: number) {
    this.queues.set(consumerId, new HighCapacityBinaryHeapPriorityQueue());
  }

  removeConsumerQueue(consumerId: number) {
    this.queues.delete(consumerId);
  }

  enqueue(consumerId: number, meta: MessageMetadata) {
    this.queues.get(consumerId)?.enqueue(meta.id, meta.priority);
    this.totalQueuedMessages++;
  }

  dequeue(consumerId: number) {
    const messageId = this.queues.get(consumerId)?.dequeue();
    if (messageId) this.totalQueuedMessages--;
    return messageId;
  }

  getMetadata() {
    return {
      size: this.totalQueuedMessages,
    };
  }
}
// SRC/TOPIC/DELAYED_QUEUE_MANAGER.TS
class TopicDelayedQueueManager<Data> {
  // TODO: persist
  private queue = new HighCapacityBinaryHeapPriorityQueue<[number, number]>();
  private nextTimeout?: number;
  private isProcessing = false;

  constructor(
    private messageStorage: IMessageStorage<Data>,
    private messageRouter: TopicMessageRouter<Data>,
    private logger?: LogCollector
  ) {}

  publish(meta: MessageMetadata) {
    const readyTs = meta.ts + meta.ttd!;
    this.queue.enqueue([meta.id, readyTs], readyTs);
    this.logger?.log("Message delayed", meta);
    this.scheduleProcessing();
  }

  private scheduleProcessing() {
    if (this.isProcessing || this.queue.isEmpty()) return;

    const record = this.queue.peek();
    if (!record) return;

    const delay = Math.max(0, record[1] - Date.now());
    if (this.nextTimeout) clearTimeout(this.nextTimeout);
    this.nextTimeout = setTimeout(this.processQueue, delay);
  }

  private processQueue = async () => {
    if (this.isProcessing) return;
    this.isProcessing = true;

    try {
      while (!this.queue.isEmpty()) {
        const [messageId, readyTs] = this.queue.peek()!;
        if (readyTs > Date.now()) break; // second peak is not ready yet

        this.queue.dequeue();
        const meta = await this.messageStorage.readMetadata(messageId);
        if (!meta) return;

        this.messageRouter.route(meta);
        this.logger?.log(`Delayed message is routed to ${meta.topic}.`, meta);
      }
    } finally {
      this.isProcessing = false;
      this.scheduleProcessing();
    }
  };

  getMetadata() {
    return {
      size: this.queue.size(),
    };
  }
}
// SRC/TOPIC/DLQ_MANAGER.TS
interface DLQEntry<Data> {
  reason:
    | "no_consumers"
    | "expired"
    | "max_attempts"
    | "validation"
    | "processing_error";
  message: Data;
  meta: MessageMetadata;
}
class TopicDLQManager<Data> {
  // TODO: persist
  private messages = new Map<number, DLQEntry<Data>["reason"]>();
  public totalMessagesProcessed = 0;

  constructor(
    private topic: string,
    private messageStorage: IMessageStorage<any>,
    private logger?: LogCollector
  ) {}

  size() {
    return this.messages.size;
  }

  publish(meta: MessageMetadata, reason: DLQEntry<Data>["reason"]): void {
    this.messages.set(meta.id, reason);
    this.totalMessagesProcessed++;
    this.logger?.log(`Routed to DLQ. Reason: ${reason}.`, meta, "warn");
  }

  async *createReader(): AsyncGenerator<DLQEntry<Data>, void, unknown> {
    for (const [messageId, reason] of this.messages.entries()) {
      const [message, meta] = await this.messageStorage.readAll(messageId);
      if (!meta || !message) continue;
      yield { message, meta, reason };
    }
  }

  async replayMessages(
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ) {
    let count = 0;
    const records = this.createReader();

    for await (const { message, meta } of records) {
      if (!filter || filter(meta)) {
        try {
          await handler(message, meta);
          this.messages.delete(meta.id);
          count++;
        } catch (e) {}
      }
    }

    this.logger?.log(
      `Replayed DLQ messages.`,
      { count, topic: this.topic },
      "warn"
    );

    return count;
  }

  getMetadata() {
    return {
      totalProcessed: this.totalMessagesProcessed,
      size: this.messages.size,
    };
  }
}
// SRC/TOPIC/TOPIC.TS
interface ITopicConfig {
  schema?: string; // registered schema` name
  persist?: boolean; // true by def
  persistThresholdMs?: number; // persist flush delay, // 100
  retentionMs?: number; // 86_400_000 1 day
  archivalThresholdMs?: number; // 100_000
  maxSizeBytes?: number;
  maxDeliveryAttempts?: number;
  maxMessageSize?: number;
  ackTimeoutMs?: number; // e.g., 30_000
  consumerInactivityThresholdMs?: 600_000;
  consumerProcessingTimeThresholdMs?: number;
  consumerPendingThresholdMs?: number;
  //   partitions?: number;
}
class Topic<Data>
  implements ICanPublish, ICanConsume<Data>, ICanConsumeDLQ<Data>
{
  constructor(
    public readonly name: string,
    private readonly pipeline: MessagePipeline,
    private readonly messageStorage: IMessageStorage<Data>,
    private readonly messageRouter: TopicMessageRouter<Data>,
    private readonly queueManager: TopicQueueManager,
    private readonly ackManager: TopicAckManager<Data>,
    private readonly subscriptionManager: TopicSubscriptionManager<Data>,
    private readonly dlqManager: TopicDLQManager<Data>,
    private readonly delayedQueueManager: TopicDelayedQueueManager<Data>,
    private readonly clientManager: TopicClientManager,
    private readonly producerFactory: ProducerFactory<Data>,
    private readonly consumerFactory: ConsumerFactory<Data>,
    private readonly dlqConsumerFactory: DLQConsumerFactory<Data>,
    private readonly metrics: TopicMetricsCollector,
    private readonly logger?: LogCollector
  ) {}

  // core

  async publish(
    producerId: number,
    message: Buffer,
    meta: MessageMetadata
  ): Promise<void> {
    this.clientManager.validateClient(producerId, "producer");
    await this.messageStorage.writeAll(message, meta);
    this.metrics.recordEnqueue(meta.size, Date.now() - meta.ts);

    if (this.pipeline.process(meta)) return;

    const awaitedAcks = this.messageRouter.route(meta);
    this.ackManager.setAwaitedAcksCount(meta.id, awaitedAcks);
    this.logger?.log(`Message is routed to ${this.name}.`, meta);
  }

  async consume(consumerId: number, autoAck = false) {
    this.clientManager.validateClient(consumerId, "consumer");
    const messageId = this.queueManager.dequeue(consumerId);
    if (!messageId) return;

    const [message, meta] = await this.messageStorage.readAll(messageId);
    if (!meta || !message) return;

    if (autoAck) {
      await this.ackManager.decrementAwaitedAcks(messageId);
    } else {
      this.ackManager.addPending(consumerId, messageId);
    }

    this.logger?.log(`Message is consumed from ${this.name}.`, meta);
    return message;
  }

  async ack(consumerId: number, messageId?: number) {
    this.clientManager.validateClient(consumerId, "consumer");
    return this.ackManager.ack(consumerId, messageId);
  }

  async nack(consumerId: number, messageId?: number, requeue = true) {
    this.clientManager.validateClient(consumerId, "consumer");
    return this.ackManager.nack(consumerId, messageId, requeue);
  }

  createDlqReader(consumerId: number) {
    this.clientManager.validateClient(consumerId, "dlq_consumer");
    return this.dlqManager.createReader();
  }

  async replayDlq(
    consumerId: number,
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ) {
    this.clientManager.validateClient(consumerId, "dlq_consumer");
    return this.dlqManager.replayMessages(handler, filter);
  }

  // clients

  createProducer() {
    const id = uniqueIntGenerator();
    this.clientManager.addClient("producer", id);
    this.logger?.log(`producer_created`, {
      topic: this.name,
      clientId: id,
    });

    return this.producerFactory.create(this, id);
  }

  createConsumer(
    options: {
      routingKeys?: string[];
      groupId?: string;
      limit?: number;
      autoAck?: boolean;
      pollingInterval?: number;
    } = {}
  ) {
    const { groupId, routingKeys, ...restOptions } = options;
    const id = uniqueIntGenerator();
    this.clientManager.addClient("consumer", id, groupId);
    this.messageRouter.addRoutingEntry(id, groupId, routingKeys);
    this.logger?.log(`consumer_created`, {
      topic: this.name,
      clientId: id,
    });

    return this.consumerFactory.create(this, id, restOptions);
  }

  createDLQConsumer(options?: { limit?: number; pollingInterval?: number }) {
    const id = uniqueIntGenerator();
    this.clientManager.addClient("dlq_consumer", id);
    this.logger?.log(`dlq_consumer_created`, {
      topic: this.name,
      clientId: id,
    });

    return this.dlqConsumerFactory.create(this, id, options);
  }

  deleteClient(id: number) {
    this.messageRouter.removeRoutingEntry(id);
    this.clientManager.removeClient(id);
    this.logger?.log("client_deleted", {
      topic: this.name,
      clientId: id,
    });
    // TODO: reballance etc
  }

  recordClientActivity(
    ...args: Parameters<typeof this.clientManager.recordActivity>
  ) {
    this.clientManager.recordActivity(...args);
  }

  // metadata

  getMetadata() {
    return {
      ...this.metrics.getMetrics(),
      name: this.name,
      clients: this.clientManager.getMetadata(),
      pendingMessages: this.queueManager.getMetadata(),
      delayedQueue: this.delayedQueueManager.getMetadata(),
      dlq: this.dlqManager.getMetadata(),
    };
  }
}
// SRC/TOPICS/TOPIC.TS
class TopicFactory {
  private codec: ICodec;
  constructor(
    private schemaRegistry: SchemaRegistry,
    private logService?: LogService,
    codec?: ICodec,
    private defaultConfig: ITopicConfig = {
      retentionMs: 86_400_000, // 1 day
      maxDeliveryAttempts: 5,
      persist: true,
    }
  ) {
    this.codec = codec ?? new ThreadedBinaryCodec();
  }

  create<Data>(name: string, config?: Partial<ITopicConfig>): Topic<Data> {
    const mergedConfig = { ...this.defaultConfig, ...config };

    this.validateTopicName(name);
    const logger = this.logService?.forTopic(name);
    const validator = this.getSchemaValidator(mergedConfig);

    // Build dependencies
    const metrics = new TopicMetricsCollector();
    const clientManager = new TopicClientManager(
      mergedConfig.consumerInactivityThresholdMs,
      mergedConfig.consumerProcessingTimeThresholdMs,
      mergedConfig.consumerPendingThresholdMs
    );

    const queueManager = new TopicQueueManager();
    const messageStorage = new LevelDBMessageStorage<Data>(
      name,
      this.codec,
      mergedConfig.retentionMs,
      mergedConfig.persist,
      mergedConfig.persistThresholdMs
    );
    const dlqManager = new TopicDLQManager<Data>(name, messageStorage, logger);
    const messageRouter = new TopicMessageRouter<Data>(
      clientManager,
      queueManager,
      dlqManager
    );
    const delayedQueueManager = new TopicDelayedQueueManager(
      messageStorage,
      messageRouter,
      logger
    );
    const pipeline = new PipelineFactory().create(
      dlqManager,
      delayedQueueManager,
      mergedConfig?.maxDeliveryAttempts
    );
    const ackManager = new TopicAckManager(
      pipeline,
      messageStorage,
      queueManager,
      metrics,
      logger,
      mergedConfig?.ackTimeoutMs
    );
    const subscriptionManager = new TopicSubscriptionManager<Data>(
      clientManager,
      queueManager
    );

    return new Topic<Data>(
      name,
      pipeline,
      messageStorage,
      messageRouter,
      queueManager,
      ackManager,
      subscriptionManager,
      dlqManager,
      delayedQueueManager,
      clientManager,
      new ProducerFactory(
        this.codec,
        () => metrics.getMetrics().totalBytes,
        validator,
        mergedConfig.maxMessageSize
      ),
      new ConsumerFactory(),
      new DLQConsumerFactory(),
      metrics,
      logger
    );
  }

  private validateTopicName(name: string): void {
    if (!name || !/^[a-zA-Z0-9_-]+$/.test(name)) {
      throw new Error(
        "Invalid topic name. Use alphanumeric, underscore and hyphen characters."
      );
    }
  }

  private getSchemaValidator(
    config: ITopicConfig
  ): ((data: any) => boolean) | undefined {
    return config.schema
      ? this.schemaRegistry.getValidator(config.schema)
      : undefined;
  }
}
//
//
//
//
// ROOT ************************************************************
// SRC/LOGGER/
interface ILogger {
  info(msg: string, extra?: unknown): void;
  warn(msg: string, extra?: unknown): void;
  error(msg: string, extra?: unknown): void;
  debug?(msg: string, extra?: unknown): void;
}
class LogCollector {
  private flushId?: number;
  private buffer = new Set<[string, object, keyof ILogger]>();

  constructor(
    private logger: ILogger,
    private chunkSize = 50,
    private topic?: string
  ) {}

  log(msg: string, extra?: object, level: keyof ILogger = "info") {
    this.buffer.add([msg, extra ?? {}, level]);
    this.scheduleFlush();
  }

  private scheduleFlush() {
    this.flushId ??= setImmediate(this.flush);
  }

  flush = () => {
    this.flushId = undefined;
    let count = 0;
    const ts = Date.now();

    for (const entry of this.buffer) {
      if (count++ >= this.chunkSize) break;
      const [message, extra, level] = entry;
      this.logger[level]?.(message, { ...extra, topic: this.topic, ts });
      this.buffer.delete(entry);
    }

    if (this.buffer.size > 0) {
      this.scheduleFlush();
    }
  };

  destroy() {
    clearImmediate(this.flushId);
    this.flushId = undefined;
    this.buffer = new Set();
  }
}
class LogService {
  private topicCollectors = new Map<string, LogCollector>();
  public globalCollector: LogCollector;

  constructor(
    private logger: ILogger,
    private config: { bufferSize?: number } = {}
  ) {
    this.globalCollector = new LogCollector(logger, config.bufferSize);
  }

  forTopic(name: string): LogCollector {
    if (!this.topicCollectors.has(name)) {
      this.topicCollectors.set(
        name,
        new LogCollector(this.logger, this.config.bufferSize, name)
      );
    }
    return this.topicCollectors.get(name)!;
  }

  flushAll(): void {
    this.globalCollector.flush();
    this.topicCollectors.forEach((collector) => collector.flush());
  }
}
// SRC/VALIDATION/SCHEMA_REGISTRY.TS
class SchemaRegistry {
  // TODO: persist
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

  register<Data>(name: string, schema: JSONSchemaType<Data>): void {
    this.validators.set(name, this.ajv.compile(schema));
  }

  getValidator(schema: string): ((data: any) => boolean) | undefined {
    return this.validators.get(schema);
  }

  remove(schema: string): void {
    this.validators.delete(schema);
  }
}
// SRC/TOPICS/TOPICREGISTRY.TS
class TopicRegistry {
  private topicFactory: TopicFactory;
  // TODO: persist
  private topics = new Map<string, Topic<any>>();
  constructor(
    schemaRegistry: SchemaRegistry,
    private logService?: LogService,
    codec?: ICodec
  ) {
    this.topicFactory = new TopicFactory(schemaRegistry, logService, codec);
  }

  create<Data>(name: string, config: ITopicConfig): Topic<Data> {
    if (this.topics.has(name)) {
      throw new Error("Topic already exists");
    }

    const topic = this.topicFactory.create<Data>(name, config);
    this.topics.set(name, topic);

    this.logService?.globalCollector.log("Topic created", {
      ...config,
      name,
    });

    return topic;
  }

  list() {
    return this.topics.keys();
  }

  get(name: string): Topic<any> | undefined {
    if (!this.topics.has(name)) throw new Error("Topic not found");
    return this.topics.get(name);
  }

  delete(name: string): void {
    this.get(name);
    this.topics.delete(name);

    this.logService?.globalCollector.log("Topic deleted", { name });
  }
}