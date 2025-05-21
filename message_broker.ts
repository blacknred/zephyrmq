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

  create(
    batch: Data[],
    metadataInput: MetadataInput & { topic: string; producerId: number }
  ): Array<{ message: Buffer; meta: MessageMetadata }> {
    const batchId = Date.now();
    return batch.map((data, index) => {
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

      const message = this.codec.encode(data);
      meta.size = message.byteLength;

      this.validators.forEach((v) => v.validate({ data, meta }));
      return { message, meta };
    });
  }
}
//
//
//
//
// PRODUCER ********************************************************
// SRC/PRODUCER/PRODUCER.TS
class Producer<Data> {
  constructor(
    private readonly messageFactory: MessageFactory<Data>,
    private readonly send: (
      message: Buffer,
      meta: MessageMetadata
    ) => Promise<void>,
    private readonly recordActivity: (
      clientId: number,
      activityRecord: Partial<ITopicClientState>
    ) => void,
    private readonly topicName: string,
    private readonly id: number
  ) {}

  async publish(batch: Data[], metadata: MetadataInput = {}) {
    let messagesSent = 0;
    const results: {
      id: number;
      status: "success" | "error";
      ts: number;
      error?: string;
    }[] = [];

    this.recordActivity(this.id, {
      status: "active",
    });

    const messages = this.messageFactory.create(batch, {
      ...metadata,
      topic: this.topicName,
      producerId: this.id,
    });

    for (const { message, meta } of messages) {
      const { id, ts } = meta;

      try {
        await this.send(message, meta);
        results.push({ id, ts, status: "success" });
        messagesSent++;
      } catch (err) {
        const error = err instanceof Error ? err.message : "Unknown error";
        results.push({ id, ts, error, status: "error" });
      }
    }

    this.recordActivity(this.id, {
      messageCount: messagesSent,
      processingTime: Date.now() - messages[0]?.meta.ts,
      status: "idle",
    });

    return results;
  }
}
// SRC/PRODUCER/FACTORY
class ProducerFactory<Data> {
  private messageFactory: MessageFactory<Data>;
  constructor(
    private topicName: string,
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

  create(
    send: (message: Buffer, meta: MessageMetadata) => Promise<void>,
    recordActivity: (
      clientId: number,
      activityRecord: Partial<ITopicClientState>
    ) => void,
    id = uniqueIntGenerator()
  ) {
    return new Producer(
      this.messageFactory,
      send,
      recordActivity,
      this.topicName,
      id
    );
  }
}
//
//
//
//
// CONSUMER ********************************************************
// SRC/CONSUMER/CONSUMER.ts (Facade)
class Consumer<Data> {
  private readonly limit: number;
  private lastConsumptionTs: number;
  constructor(
    private readonly topic: Topic<Data>,
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

  createDlqConsumer() {
    const consumer = this.topic.createDlqConsumer();
    return async () => {
      const messages = [];

      for await (const message of consumer) {
        if (!message) break;
        // @ts-ignore
        messages.push(message);
        if (messages.length == this.limit) break;
      }

      return messages;
    };
  }

  async replayDlq(
    handler: (message: unknown, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ) {
    return this.topic.replayDlq(handler, filter);
  }

  // subscribe(handler: (message: Data) => Promise<void>): void {
  //   this.topic.subscribe(this.id, handler);
  // }

  // unsubscribe(): void {
  //   this.topic.unsubscribe(this.id);
  // }
}
//
//
//
//
// ROUTING_STRATEGY ************************************************
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
interface IRoutingStrategy {
  getCorrelatedEntry(correlationId: string): Generator<number, void, unknown>;
  addEntry(id: number, routingKeys?: string[]): void;
  removeEntry(id: number): void;
}
class KeysHashRoutingStrategy implements IRoutingStrategy {
  private subscriptions = new Map<number, Set<string>>();
  private excludedEntriesCache = new Map<string, Set<number>>();
  constructor(private ring: IHashRing) {}

  getExcludedEntries(routingKey: string): Set<number> {
    // use cache to prevent hot spot
    const cached = this.excludedEntriesCache.get(routingKey);
    if (cached) return cached;

    const excludedEntries = new Set<number>();
    for (const [entry, keys] of this.subscriptions) {
      if (keys.has(routingKey)) excludedEntries.add(entry);
    }

    this.excludedEntriesCache.set(routingKey, excludedEntries);
    return excludedEntries;
  }

  getCorrelatedEntry(correlationId: string): Generator<number, void, unknown> {
    return this.ring.getNode(correlationId);
  }

  addEntry(consumerId: number, routingKeys?: string[]): void {
    this.ring.addNode(consumerId);

    if (routingKeys?.length) {
      this.subscriptions.set(consumerId, new Set(routingKeys));

      // shuffle cache
      for (const key of routingKeys) {
        this.excludedEntriesCache.delete(key);
      }
    }
  }

  removeEntry(consumerId: number): void {
    this.ring.removeNode(consumerId);
    this.subscriptions.delete(consumerId);

    // shuffle cache
    for (const [key, entries] of this.excludedEntriesCache) {
      if (entries.has(consumerId)) {
        this.excludedEntriesCache.delete(key);
      }
    }
  }
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
class ExpirationProcessor implements IMessageProcessor {
  constructor(private dlq: TopicDLQManager) {}
  process(meta: MessageMetadata): boolean {
    if (!meta.ttl) return false;
    const isExpired =
      meta.ts + meta.ttl <= Date.now() || !!(meta.ttd && meta.ttd >= meta.ttl);
    if (isExpired) this.dlq.publish(meta, "expired");
    return isExpired;
  }
}
class AttemptsProcessor implements IMessageProcessor {
  constructor(private dlq: TopicDLQManager, private maxAttempts: number) {}
  process(meta: MessageMetadata): boolean {
    const shouldDeadLetter = meta.attempts > this.maxAttempts;
    if (shouldDeadLetter) {
      this.dlq.publish(meta, "max_attempts");
    }
    return shouldDeadLetter;
  }
}
class DelayProcessor implements IMessageProcessor {
  constructor(private delayedQueue: TopicDelayedQueueManager) {}
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
  clientType: "producer" | "consumer";
  registeredAt: number;
  lastActiveAt: number;
  // Metrics
  status: "active" | "idle" | "lagging";
  messageCount: number;
  processingTime: number;
  avgProcessingTime: number;
  pendingMessages?: number;
}
class TopicClientRegistry {
  private clients = new Map<number, ITopicClientState>();
  private totalConsumers = 0;
  private totalProducers = 0;

  register(type: "producer" | "consumer", id = uniqueIntGenerator()) {
    const now = Date.now();
    this.clients.set(id, {
      registeredAt: now,
      lastActiveAt: now,
      clientType: type,
      messageCount: 0,
      processingTime: 0,
      avgProcessingTime: 0,
      status: "active",
    });

    if (type === "producer") this.totalProducers++;
    else this.totalConsumers++;
    return this.clients.get(id);
  }

  unregister(id: number) {
    const client = this.clients.get(id);
    if (client?.clientType === "producer") this.totalProducers++;
    else this.totalConsumers++;

    this.clients.delete(id);
    return this.clients.size;
  }

  count(type: "producer" | "consumer") {
    if (type == "producer") return this.totalProducers;
    return this.totalConsumers;
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

  getMetadata(clientId?: number) {
    if (clientId) return this.clients.get(clientId);
    return Array.from(this.clients.values());
  }
}
// SRC/TOPIC/MESSAGE_LOG_MANAGER.TS
class TopicMessageLogManager<Data> {
  // separates metadata/messages buffers since we: 1. need read only data/metadata, 2. need update only metadata, 3. need often read metadata (retention timer)
  private messages = new Map<number, Buffer<Data>>();
  private metadatas = new Map<number, Buffer<MessageMetadata>>();
  private flushId?: number;

  constructor(
    private topic: string,
    private codec: ICodec,
    private retentionMs = 86_400_000, // 1day
    private isPersist = true,
    private persistThreshold = 100,
    private chunkSize = 50
  ) {
    // TODO: timeout retention check meta:
    // meta.consumedAt ====> delete
    // meta.ts + meta.ttl < now ====> DLQ
    // (non-expired & non-consumed) && now - meta.ts >= retentionMs ====> DLQ
  }

  async write(message: Buffer, meta: MessageMetadata): Promise<number> {
    const encodedMeta = await this.codec.encodeMetadata(meta);
    this.messages.set(meta.id, message);
    this.metadatas.set(meta.id, encodedMeta);

    this.scheduleFlush();
    return this.messages.size;
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
    this.flushId ??= setImmediate(this.flush, this.persistThreshold);
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
// SRC/TOPIC/QUEUE_MANAGER.TS
class TopicQueueManager {
  private totalQueuedMessages: 0;
  private queues = new Map<number, IPriorityQueue<number>>();
  private pendingMessages = new Map<number, Set<number>>();

  constructor(
    private dlqManager: TopicDLQManager,
    private routingStrategy: KeysHashRoutingStrategy
  ) {}

  addConsumerQueue(consumerId: number, routingKeys?: string[]) {
    this.queues.set(consumerId, new HighCapacityBinaryHeapPriorityQueue());
    this.pendingMessages.set(consumerId, new Set());
    this.routingStrategy.addEntry(consumerId, routingKeys);
  }

  removeConsumerQueue(consumerId: number) {
    this.queues.delete(consumerId);
    this.ack(consumerId);
    this.routingStrategy.removeEntry(consumerId);
  }

  enqueue(meta: MessageMetadata, consumerId?: number) {
    // consumers with routingKey get only messages with the same routingKey
    // consumers without routingKey get all messages

    // put message to consumer (consumer nack case)
    if (consumerId) {
      this.queues.get(consumerId)?.enqueue(meta.id, meta.priority);
      return;
    }

    // no consumers yet
    if (!this.queues.size) {
      this.dlqManager.publish(meta, "no_consumers");
      return 0;
    }

    // consumers binded to other routingKeys
    const excludedQueues = meta.routingKey
      ? this.routingStrategy.getExcludedEntries(meta.routingKey)
      : null;

    // all consumers binded to other routingKeys
    if (excludedQueues?.size == this.queues.size) {
      this.dlqManager.publish(meta, "no_consumers");
      return 0;
    }

    // need nearest correlated consumer scan
    if (meta.correlationId) {
      const correlations = this.routingStrategy.getCorrelatedEntry(
        meta.correlationId
      )!;

      for (const correlatedQueue of correlations) {
        if (!excludedQueues || !excludedQueues.has(correlatedQueue)) {
          this.queues.get(correlatedQueue)?.enqueue(meta.id, meta.priority);
          return 1;
        }
      }
      const fallbackQueue = this.queues.keys().next().value;
      this.queues.get(fallbackQueue)?.enqueue(meta.id, meta.priority);
    }

    // just populate in non-excluded queues
    let needAcks = 0;
    this.queues.forEach((queue, name) => {
      if (excludedQueues?.has(name)) return;
      queue.enqueue(meta.id, meta.priority);
      this.totalQueuedMessages++;
      needAcks++;
    });

    return needAcks;
  }

  dequeue(consumerId: number, autoAck = false) {
    const messageId = this.queues.get(consumerId)?.dequeue();
    if (messageId) {
      this.totalQueuedMessages--;
      if (!autoAck) {
        this.pendingMessages.get(consumerId)?.add(messageId);
      }
    }

    return messageId;
  }

  ack(consumerId: number, messageId?: number) {
    if (messageId) {
      this.pendingMessages.get(consumerId)?.delete(messageId);
      return [messageId];
    }
    const pendingMessages = this.pendingMessages.get(consumerId)!;
    this.pendingMessages.set(consumerId, new Set());
    return Array.from(pendingMessages);
  }

  size() {
    return this.totalQueuedMessages;
  }
}
// SRC/TOPIC/DELAYED_QUEUE_MANAGER.TS
class TopicDelayedQueueManager {
  private queue = new HighCapacityBinaryHeapPriorityQueue<[number, number]>();
  private nextTimeout?: number;
  private isProcessing = false;
  private onReadyHandler?: (messageId: number) => Promise<void>;

  constructor(private logger?: LogCollector) {}

  setOnReadyHandler(handler: (messageId: number) => Promise<void>) {
    this.onReadyHandler = handler;
  }

  size(): number {
    return this.queue.size();
  }

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
        await this.onReadyHandler?.(messageId);
      }
    } finally {
      this.isProcessing = false;
      this.scheduleProcessing();
    }
  };
}
// SRC/TOPIC/DLQ_MANAGER.TS
type DLQReason =
  | "no_consumers"
  | "expired"
  | "max_attempts"
  | "validation"
  | "processing_error";
class TopicDLQManager {
  private messages = new Map<number, DLQReason>();
  public totalMessagesProcessed = 0;

  constructor(
    private topic: string,
    private messageLogManager: TopicMessageLogManager<any>,
    private logger?: LogCollector
  ) {}

  size() {
    return this.messages.size;
  }

  publish(meta: MessageMetadata, reason: DLQReason): void {
    this.messages.set(meta.id, reason);
    this.totalMessagesProcessed++;
    this.logger?.log(`Routed to DLQ. Reason: ${reason}.`, meta, "warn");
  }

  async *createConsumer() {
    for (const [messageId, reason] of this.messages.entries()) {
      const meta = await this.messageLogManager.readMetadata(messageId);
      const message = await this.messageLogManager.readMessage(messageId);
      if (!meta) continue;
      yield { message, meta: { ...meta, reason } };
    }
  }

  async replayMessages(
    handler: (message: unknown, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ) {
    let count = 0;
    const records = this.createConsumer();

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
}
// SRC/TOPIC/TOPIC.TS
interface ITopicConfig {
  schema?: string; // registered schema` name
  persist?: boolean; // true by def
  persistThreshold?: number; // persist flush delay, // 100
  retentionMs?: number; // 86_400_000 1 day
  archivalThreshold?: number; // 100_000
  maxSizeBytes?: number;
  maxDeliveryAttempts?: number;
  maxMessageSize?: number;
  ackTimeoutMs?: number; // e.g., 30_000
  //   partitions?: number;
}
class Topic<Data> {
  constructor(
    public readonly name: string,
    private readonly pipeline: MessagePipeline,
    private readonly messageLogManager: TopicMessageLogManager<Data>,
    private readonly queueManager: TopicQueueManager,
    private readonly dlqManager: TopicDLQManager,
    private readonly delayedQueue: TopicDelayedQueueManager,
    private readonly metrics: TopicMetricsCollector,
    private readonly clientRegistry: TopicClientRegistry,
    private readonly producerFactory: ProducerFactory<Data>,
    private readonly logger?: LogCollector
  ) {
    delayedQueue.setOnReadyHandler(this.delayedQueuePublish);
  }

  private async delayedQueuePublish(messageId: number) {
    const meta = await this.messageLogManager.readMetadata(messageId);
    if (!meta) return;

    this.queueManager.enqueue(meta);
    this.logger?.log(`Delayed message is routed to ${this.name}.`, meta);
  }

  private async publish(message: Buffer, meta: MessageMetadata): Promise<void> {
    await this.messageLogManager.write(message, meta);
    this.metrics.recordEnqueue(meta.size, Date.now() - meta.ts);

    if (this.pipeline.process(meta)) return;

    const needAcks = this.queueManager.enqueue(meta);
    await this.messageLogManager.updateMetadata(meta.id, { needAcks });
    this.logger?.log(`Message is routed to ${this.name}.`, meta);
  }

  async nack(consumerId: number, messageId?: number, requeue = true) {
    const messages = this.queueManager.ack(consumerId, messageId);

    for (const messageId of messages) {
      const meta = await this.messageLogManager.readMetadata(messageId);
      if (!meta) continue;
      await this.messageLogManager.updateMetadata(messageId, {
        attempts: requeue ? meta.attempts + 1 : Infinity,
        consumedAt: undefined,
      });

      if (this.pipeline.process(meta)) continue;

      this.queueManager.enqueue(meta, consumerId);
      this.logger?.log(`Message is nacked to ${this.name}.`, meta);
    }

    return messages;
  }

  async consume(consumerId: number, autoAck = false) {
    const messageId = this.queueManager.dequeue(consumerId, autoAck);
    if (!messageId) return;
    const meta = await this.messageLogManager.readMetadata(messageId);
    if (!meta) return;
    const message = await this.messageLogManager.readMessage(messageId);

    if (autoAck) {
      await this.checkIsConsumed(messageId, meta.needAcks, meta.ts);
    }

    this.logger?.log(`Message is consumed from ${this.name}.`, meta);
    return message;
  }

  async ack(consumerId: number, messageId?: number) {
    const messages = this.queueManager.ack(consumerId, messageId);

    for (const messageId of messages) {
      const meta = await this.messageLogManager.readMetadata(messageId, [
        "needAcks",
        "ts",
      ]);
      if (!meta) continue;
      await this.checkIsConsumed(messageId, meta.needAcks, meta.ts);
    }

    return messages;
  }

  private async checkIsConsumed(
    messageId: number,
    needAcks: number,
    ts: number
  ) {
    const consumedAt = --needAcks < 1 ? Date.now() : undefined;
    await this.messageLogManager.updateMetadata(messageId, {
      needAcks,
      consumedAt,
    });
    if (consumedAt) {
      this.metrics.recordDequeue(consumedAt - ts);
    }
  }

  createDlqConsumer() {
    return this.dlqManager.createConsumer();
  }

  async replayDlq(
    handler: (message: unknown, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ) {
    return this.dlqManager.replayMessages(handler, filter);
  }

  createProducer() {
    const id = uniqueIntGenerator();
    this.clientRegistry.register("producer", id);
    this.logger?.log(`producer_created`, {
      topic: this.name,
      clientId: id,
    });

    return this.producerFactory.create(
      this.publish,
      this.recordClientActivity,
      id
    );
  }

  createConsumer(options: {
    routingKeys?: string[];
    limit?: number;
    autoAck?: boolean;
    pollingInterval?: number;
  }) {
    const { limit, autoAck, routingKeys } = options;
    const id = uniqueIntGenerator();
    this.clientRegistry.register("consumer", id);
    this.queueManager.addConsumerQueue(id, routingKeys);
    this.logger?.log(`consumer_created`, {
      topic: this.name,
      clientId: id,
    });

    return new Consumer(this, id, autoAck, limit);
  }

  deleteClient(id: number) {
    this.clientRegistry.unregister(id);
    this.queueManager.removeConsumerQueue(id);
    this.logger?.log("client_deleted", {
      topic: this.name,
      clientId: id,
    });
    // TODO: reballance etc
  }

  recordClientActivity(
    ...args: Parameters<typeof this.clientRegistry.recordActivity>
  ) {
    this.clientRegistry.recordActivity(...args);
  }

  getMetadata() {
    return {
      ...this.metrics.getMetrics(),
      name: this.name,
      pendingMessages: this.queueManager.size(),
      delayedQueueSize: this.delayedQueue.size(),
      dlqSize: this.dlqManager.size(),
      totalDlqMessages: this.dlqManager.totalMessagesProcessed,
      clients: this.clientRegistry.getMetadata(),
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
    const clientRegistry = new TopicClientRegistry();

    const messageLogManager = new TopicMessageLogManager<Data>(
      name,
      this.codec,
      mergedConfig.retentionMs,
      mergedConfig.persist,
      mergedConfig.persistThreshold
    );

    const dlqManager = new TopicDLQManager(name, messageLogManager, logger);
    const delayedQueueManager = new TopicDelayedQueueManager(logger);
    const queueManager = new TopicQueueManager(
      dlqManager,
      new KeysHashRoutingStrategy(new InMemoryHashRing(new SHA256HashService()))
    );
    const pipeline = new PipelineFactory().create(
      dlqManager,
      delayedQueueManager,
      mergedConfig?.maxDeliveryAttempts
    );

    return new Topic<Data>(
      name,
      pipeline,
      messageLogManager,
      queueManager,
      dlqManager,
      delayedQueueManager,
      metrics,
      clientRegistry,
      new ProducerFactory(
        name,
        this.codec,
        () => metrics.getMetrics().totalBytes,
        validator,
        mergedConfig.maxMessageSize
      ),
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
