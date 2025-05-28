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
  getNodeCount(): number;
  getNode(key: string): Generator<number, void, unknown>;
}
/** Hash ring.
 * The system works regardless of how different the key hashes are because the lookup is always relative to the fixed node positions on the ring.
 * Sorted nodes in a ring: [**100(A)**, _180(user-123 key hash always belong to the B)_, **200(B)**, **300(A)**, **400(B)**, **500(A)**, **600(B)**]
 */
class InMemoryHashRing implements IHashRing {
  private sortedHashes: number[] = [];
  private hashToNodeMap = new Map<number, number>();
  private nodeIds = new Set<number>(); // Tracks unique nodes

  /**
   * Create a new instance of HashRing with the given number of virtual nodes.
   * @param {number} [replicas=3] The number of virtual nodes to create for each node.
   * The more virtual nodes gives you fewer hotspots, more balanced traffic. However, setting
   * this number too high can lead to a large memory footprint and slower lookups.
   */
  constructor(private hashService: IHashService, private replicas = 3) {}

  addNode(id: number): void {
    if (this.nodeIds.has(id)) return; // Avoid duplicate node addition

    for (let i = 0; i < this.replicas; i++) {
      const hash = this.hashService.hash(`${id}-${i}`);
      this.hashToNodeMap.set(hash, id);

      const index = this.findInsertIndex(hash);
      this.sortedHashes.splice(index, 0, hash);
    }

    this.nodeIds.add(id);
  }

  removeNode(id: number): void {
    const hashesToRemove: number[] = [];
    this.hashToNodeMap.forEach((nodeId, hash) => {
      if (nodeId === id) {
        hashesToRemove.push(hash);
      }
    });

    for (const hash of hashesToRemove) {
      this.hashToNodeMap.delete(hash);
      const index = this.sortedHashes.indexOf(hash);
      if (index !== -1) {
        this.sortedHashes.splice(index, 1);
      }
    }

    this.nodeIds.delete(id);
  }

  getNodeCount(): number {
    return this.nodeIds.size;
  }

  *getNode(key: string): Generator<number, void, unknown> {
    if (this.sortedHashes.length === 0) {
      throw new Error("No nodes available in the hash ring");
    }

    const keyHash = this.hashService.hash(key);
    let currentIndex = this.findNodeIndex(keyHash);

    const total = this.sortedHashes.length;
    for (let i = 0; i < total; i++) {
      yield this.hashToNodeMap.get(this.sortedHashes[currentIndex])!;
      currentIndex = (currentIndex + 1) % total;
    }
  }

  // Find node index using binary search
  private findNodeIndex(keyHash: number): number {
    let low = 0;
    let high = this.sortedHashes.length - 1;

    while (low <= high) {
      const mid = (low + high) >>> 1;
      if (this.sortedHashes[mid] < keyHash) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    return low % this.sortedHashes.length;
  }

  // Binary search for insertion index
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
// SRC/PRODUCERS/PRODUCER.TS (Facade)
class Producer<Data> {
  constructor(
    private readonly messageFactory: MessageFactory<Data>,
    private readonly send: (
      producerId: number,
      message: Buffer,
      meta: MessageMetadata
    ) => Promise<void>,
    private readonly topicName: string,
    private readonly id: number
  ) {}

  async publish(batch: Data[], metadata: MetadataInput = {}) {
    const results: {
      id: number;
      status: "success" | "error";
      ts: number;
      error?: string;
    }[] = [];

    const messages = await this.messageFactory.create(batch, {
      ...metadata,
      topic: this.topicName,
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
        await this.send(this.id, message, meta);
        results.push({ id, ts, status: "success" });
      } catch (err) {
        const error = err instanceof Error ? err.message : "Unknown error";
        results.push({ id, ts, error, status: "error" });
      }
    }

    return results;
  }
}
// SRC/PRODUCERS/PRODUCER_FACTORY
class ProducerFactory<Data> {
  private messageFactory: MessageFactory<Data>;
  constructor(
    codec: ICodec,
    private readonly topicName: string,
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
    send: (
      producerId: number,
      message: Buffer,
      meta: MessageMetadata
    ) => Promise<void>,
    id = uniqueIntGenerator()
  ) {
    return new Producer(this.messageFactory, send, this.topicName, id);
  }
}
//
//
//
//
// CONSUMER ********************************************************
// SRC/CONSUMERS/CONSUMER.TS (Facade)
class Consumer<Data> {
  private readonly limit: number;
  constructor(
    private readonly consumable: IConsumable<Data>,
    private readonly id: number,
    private readonly autoAck = false,
    limit?: number
  ) {
    this.limit = Math.max(1, limit!);
  }

  async consume() {
    const messages: Data[] = [];

    for (let i = 0; i < this.limit; i++) {
      const message = await this.consumable.consume(this.id, this.autoAck);
      if (!message) break;
      messages.push(message);
    }

    return messages;
  }

  async ack(messageId?: number) {
    return this.consumable.ack(this.id, messageId);
  }

  async nack(messageId?: number, requeue = true): Promise<number> {
    return this.consumable.nack(this.id, messageId, requeue);
  }

  subscribe(listener: ISubscriptionListener<Data>): void {
    this.consumable.subscribe(this.id, listener, this.autoAck);
  }

  unsubscribe(): void {
    this.consumable.unsubscribe(this.id);
  }
}
// SRC/CONSUMERS/DLQ_CONSUMER.TS (Facade)
class DLQConsumer<Data> {
  private readonly limit: number;
  private reader: AsyncGenerator<DLQEntry<Data>, void, unknown>;
  constructor(
    private readonly topic: IDLQConsumable<Data>,
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
    return this.topic.replayDlq(this.id, handler, filter);
  }

  // subscribe(listener: (message: DLQEntry<Data>) => Promise<void>): void {
  //   this.topic.subscribe<DLQEntry<Data>>(this.id, listener);
  // }

  // unsubscribe(): void {
  //   this.topic.unsubscribe(this.id);
  // }
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
// SRC/DELAY_SCHEDULER.TS
class DelayScheduler<Data> {
  private nextTimeout?: number;
  private isProcessing = false;
  private onReadyCallback: (data: Data) => Promise<void>;

  constructor(private queue: IPriorityQueue<[Data, number]>) {}

  setReadyCallback(onReadyHandler: (data: Data) => Promise<void>) {
    this.onReadyCallback = onReadyHandler;
  }

  schedule(data: Data, readyTs: number) {
    this.queue.enqueue([data, readyTs], readyTs);
    this.setNextTimeout();
  }

  private setNextTimeout(): void {
    if (this.isProcessing || this.queue.isEmpty()) return;

    const record = this.queue.peek();
    if (!record) return;

    const delay = Math.max(0, record[1] - Date.now());
    clearTimeout(this.nextTimeout);
    this.nextTimeout = setTimeout(this.onTimeoutHandler, delay);
  }

  private onTimeoutHandler = () => {
    if (this.isProcessing) return;
    this.isProcessing = true;
    const now = Date.now();

    try {
      while (!this.queue.isEmpty()) {
        const [data, readyTs] = this.queue.peek()!;
        if (readyTs > now) break; // second peak is not ready yet
        this.queue.dequeue();
        this.onReadyCallback(data);
      }
    } finally {
      this.isProcessing = false;
      this.setNextTimeout();
    }
  };

  pendingSize() {
    return this.queue.size;
  }

  cleanup() {
    clearTimeout(this.nextTimeout);
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
class ExpirationProcessor<Data> implements IMessageProcessor {
  constructor(private dlq: TopicDLQManager<Data>) {}
  process(meta: MessageMetadata): boolean {
    if (!meta.ttl) return false;
    const isAlreadyOld = meta.ts + meta.ttl <= Date.now();
    const isWillBeOldWnenDelayIsOver = !!meta.ttd && meta.ttd > meta.ttl;
    const isExpired = isAlreadyOld || isWillBeOldWnenDelayIsOver;
    if (isExpired) this.dlq.enqueue(meta, "expired");
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
      this.dlq.enqueue(meta, "max_attempts");
    }
    return shouldDeadLetter;
  }
}
class DelayProcessor<Data> implements IMessageProcessor {
  constructor(private delayedManager: TopicDelayedManager<Data>) {}
  process(meta: MessageMetadata): boolean {
    if (!meta.ttd) return false;
    const shouldDelay = meta.ts + meta.ttd! > Date.now();
    if (shouldDelay) this.delayedManager.enqueue(meta);
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
    delayedManager: TopicDelayedManager<Data>,
    maxAttempts?: number
  ): MessagePipeline {
    const pipeline = new MessagePipeline();
    pipeline.addProcessor(new ExpirationProcessor(dlqManager));
    pipeline.addProcessor(new DelayProcessor(delayedManager));
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
interface IClientState {
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
}
class TopicClientManager {
  // TODO: persist
  private clients = new Map<number, IClientState>();

  constructor(
    private inactivityThresholdMs = 300_000,
    private processingTimeThresholdMs = 50_000,
    private pendingThresholdMs = 100
  ) {}

  addClient(type: IClientState["clientType"], id = uniqueIntGenerator()) {
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
      id,
    });

    this[`total${type[0].toUpperCase() + type.slice(1)}s`]++;
    return this.clients.get(id);
  }

  removeClient(id: number) {
    const client = this.clients.get(id);
    if (!client) return;
    const type = client.clientType;

    this[`total${type[0].toUpperCase() + type.slice(1)}s`]--;
    this.clients.delete(id);
    return this.clients.size;
  }

  getClients(filter?: (client: IClientState) => boolean) {
    const states = this.clients.values();
    if (!filter) states;
    const results = new Set<IClientState>();
    for (const state of states) {
      if (filter?.(state)) results.add(state);
    }
    return results;
  }

  getClient(clientId: number) {
    return this.clients.get(clientId);
  }

  validateClient(id: number, expectedType?: IClientState["clientType"]) {
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

  recordActivity(clientId: number, activityRecord: Partial<IClientState>) {
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
// SRC/TOPIC/CONSUMER_GROUP.TS
class TopicConsumerGroup {
  private members = new Map<number, Set<string>>();
  constructor(private name: string | undefined, private hashRing: IHashRing) {}

  addMember(id: number, routingKeys?: string[]) {
    const keysSet = new Set(routingKeys);

    // for the group with defined groupId we need to enforce homogeneous routingKeys within the members
    if (this.name && this.members.size > 0) {
      const expectedKeys = this.members.values().next().value as
        | Set<string>
        | undefined;
      const isValid =
        expectedKeys?.size === keysSet.size &&
        [...keysSet].every((k) => expectedKeys.has(k));

      if (!isValid) {
        throw new Error(
          `Member ${id} has incompatible routingKeys for group ${this.name}`
        );
      }
    }

    this.hashRing.addNode(id);
    this.members.set(id, keysSet);
  }

  removeMember(id: number) {
    this.hashRing.removeNode(id);
    this.members.delete(id);
  }

  hasMembers(): boolean {
    return this.hashRing.getNodeCount() > 0;
  }

  getMembers(
    messageId: number,
    correlationId?: string
  ): Iterable<number> | undefined {
    if (this.name || correlationId) {
      return this.hashRing.getNode(correlationId || messageId.toString());
    }
    return this.members.keys();
  }

  getName() {
    return this.name;
  }

  getRoutingKeys(id: number) {
    return this.members.get(id);
  }
}
// SRC/TOPIC/DELIVERY_TRACKER.TS
class TopicDeliveryCounter<Data> {
  // TODO: persist
  private awaitedDeliveries = new Map<number, number>();

  constructor(
    private messageStorage: IMessageStorage<Data>,
    private metrics: TopicMetricsCollector
  ) {}

  setAwaitedDeliveries(messageid: number, awaitedDeliveries: number) {
    this.awaitedDeliveries.set(messageid, awaitedDeliveries);
  }

  async decrementAwaitedDeliveries(messageId: number) {
    let deliveries = this.awaitedDeliveries.get(messageId);
    if (!deliveries) return;
    this.awaitedDeliveries.set(messageId, --deliveries);
    if (deliveries > 0) return;

    const meta = await this.messageStorage.readMetadata(messageId, ["ts"]);
    if (!meta) return;
    const consumedAt = Date.now();
    await this.messageStorage.updateMetadata(messageId, { consumedAt });
    this.metrics.recordDequeue(consumedAt - meta.ts);
  }
}
// SRC/TOPIC/PENDING_MESSAGES.TS
class TopicPendingsMessages {
  // TODO: persist
  private pendingMessages = new Map<number, Map<number, number>>();

  constructor(private clientManager: TopicClientManager) {}

  addPending(consumerId: number, messageId: number): void {
    if (!this.pendingMessages.has(consumerId)) {
      this.pendingMessages.set(consumerId, new Map());
    }
    this.pendingMessages.get(consumerId)?.set(messageId, Date.now());

    this.clientManager.recordActivity(consumerId, {
      pendingMessages: 1,
      status: "active",
    });
  }

  getPendings(consumerId: number) {
    return this.pendingMessages.get(consumerId);
  }

  getAllPendings() {
    return this.pendingMessages;
  }

  removePending(consumerId: number, messageId?: number): void {
    const pendings = this.pendingMessages.get(consumerId);
    if (!pendings) return;
    const now = Date.now();

    if (messageId) {
      if (pendings.has(messageId)) {
        const consumedAt = pendings.get(messageId)!;
        pendings.delete(messageId);

        this.clientManager.recordActivity(consumerId, {
          pendingMessages: -1,
          messageCount: 1,
          processingTime: now - consumedAt,
          status: pendings.size ? "active" : "idle",
        });
      }
    } else if (pendings.size) {
      const firstConsumeAt = pendings.values().next().value as number;
      this.clientManager.recordActivity(consumerId, {
        pendingMessages: -pendings.size,
        messageCount: pendings.size,
        processingTime: Date.now() - firstConsumeAt,
        status: "idle",
      });

      pendings.clear();
    }

    if (!pendings.size) {
      this.pendingMessages.delete(consumerId);
    }
  }
}
// SRC/TOPIC/ACK_MANAGER.TS
class TopicAckManager<Data> {
  constructor(
    private pendingMessages: TopicPendingsMessages,
    private deliveryCounter: TopicDeliveryCounter<Data>,
    private messageStorage: IMessageStorage<any>,
    private pipeline: MessagePipeline,
    private queueManager: TopicQueueManager,
    private logger?: LogCollector
  ) {}

  async ack(consumerId: number, messageId?: number) {
    const pendingMessages: number[] = [];

    if (messageId) {
      pendingMessages.push(messageId);
      this.pendingMessages.removePending(consumerId, messageId);
    } else {
      const pendingMap = this.pendingMessages.getPendings(consumerId);
      if (pendingMap) pendingMessages.push(...pendingMap.keys());
      this.pendingMessages.removePending(consumerId);
    }

    for (const messageId of pendingMessages) {
      await this.deliveryCounter.decrementAwaitedDeliveries(messageId);
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

    return messages.length;
  }
}
// SRC/TOPIC/ACK_MONITOR.TS
class TopicAckMonitor<Data> {
  private timer?: number;

  constructor(
    private pendingMessages: TopicPendingsMessages,
    private ackManager: TopicAckManager<Data>,
    private ackTimeoutMs = 30_000
  ) {
    this.timer = setInterval(
      this.nackTimedOutPendings,
      Math.max(1000, this.ackTimeoutMs / 2)
    );
  }

  stop() {
    clearInterval(this.timer);
  }

  private nackTimedOutPendings = async () => {
    const now = Date.now();
    const pendings = this.pendingMessages.getAllPendings();
    if (!pendings) return;
    for (const [consumerId, messages] of pendings) {
      for (const [messageId, consumedAt] of messages.entries()) {
        if (now - consumedAt > this.ackTimeoutMs) {
          await this.ackManager.nack(consumerId, messageId, true);
        }
      }
    }
  };
}
// SRC/TOPIC/SUBSCRIPTION_MANAGER.TS
type ISubscriptionListener<Data> = (message: Data) => Promise<void>;
class TopicSubscriptionManager<Data> {
  private subscriptions = new Map<
    number,
    [ISubscriptionListener<Data>, boolean]
  >();

  constructor(
    private clientManager: TopicClientManager,
    private queueManager: TopicQueueManager,
    private messageStorage: IMessageStorage<Data>,
    private pendingMessages: TopicPendingsMessages,
    private logger?: LogCollector
  ) {}

  addListener(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    autoAck = false
  ) {
    this.subscriptions.set(consumerId, [listener, autoAck]);
    this.tryDrainQueue(consumerId, listener);
  }

  removeListener(consumerId: number) {
    this.subscriptions.delete(consumerId);
  }

  hasListener(consumerId: number) {
    return this.subscriptions.has(consumerId);
  }

  async pushTo(consumerId: number, meta: MessageMetadata) {
    const subscription = this.subscriptions.get(consumerId);
    if (!subscription) return;
    const message = await this.messageStorage.readMessage(meta.id);
    if (!message) return;

    const [listener, autoAck] = subscription;
    listener(message);

    setImmediate(() => {
      this.logger?.log(`Message is consumed from ${meta.topic}.`, meta);
    });

    if (!autoAck) {
      this.pendingMessages.addPending(consumerId, meta.id);
      return true;
    }

    this.clientManager.recordActivity(consumerId, {
      messageCount: 1,
      pendingMessages: 0,
      processingTime: 0,
      status: "idle",
    });

    return false;
  }

  private async tryDrainQueue(
    consumerId: number,
    listener: ISubscriptionListener<Data>
  ) {
    // stop draining if there are pending messages for this consumer
    const pendings = this.pendingMessages.getPendings(consumerId);
    if (pendings && pendings.size > 0) return;

    // Get the next message from the queue
    const messageId = this.queueManager.dequeue(consumerId);
    if (!messageId) return;
    const [message, meta] = await this.messageStorage.readAll(messageId);
    if (!meta || !message) return;

    const start = Date.now();

    try {
      await listener(message);

      this.clientManager.recordActivity(consumerId, {
        messageCount: 1,
        processingTime: Date.now() - start,
        status: "idle",
      });
      this.logger?.log(`Message is consumed from ${messageId}.`, {
        id: messageId,
      });

      // Acknowledge the message since we're in autoAck mode
      await this.messageStorage.updateMetadata(messageId, {
        consumedAt: Date.now(),
      });

      // Update metrics about message consumption
      const meta = await this.messageStorage.readMetadata(messageId, ["ts"]);
      if (meta?.ts) {
        this.metrics.recordDequeue(Date.now() - meta.ts);
      }

      // Process the next message
      setImmediate(() => this.tryDrainQueue(consumerId, listener));
    } catch (error) {
      // Handle error by requeuing the message or sending to DLQ
      this.logger?.log(`Error processing message: ${messageId}`, {
        id: messageId,
        error: error instanceof Error ? error.message : String(error),
        level: "error",
      });

      // Increment attempt count
      const meta = await this.messageStorage.readMetadata(messageId);
      if (meta) {
        await this.messageStorage.updateMetadata(messageId, {
          attempts: (meta.attempts || 0) + 1,
        });

        // Check if message should be dead-lettered
        if (this.pipeline.process(meta)) {
          // Message was sent to DLQ
          return;
        }
      }

      // Requeue the message for another attempt
      this.queueManager.enqueue(consumerId, meta!);
    }
  }
}
// SRC/TOPIC/ROUTER.ts
class TopicMessageRouter<Data> {
  private consumerGroups = new Map<string | undefined, TopicConsumerGroup>();

  constructor(
    private clientManager: TopicClientManager,
    private queueManager: TopicQueueManager,
    private subscriptionManager: TopicSubscriptionManager<Data>,
    private dlqManager: TopicDLQManager<Data>
  ) {}

  addConsumer(consumerId: number, groupId?: string, routingKeys?: string[]) {
    if (!this.consumerGroups.has(groupId)) {
      const hashRing = new InMemoryHashRing(new SHA256HashService());
      this.consumerGroups.set(
        groupId,
        new TopicConsumerGroup(groupId, hashRing)
      );
    }

    this.consumerGroups.get(groupId)!.addMember(consumerId, routingKeys);
  }

  removeConsumer(consumerId: number) {
    for (const [name, group] of this.consumerGroups.entries()) {
      group.removeMember(consumerId);
      if (!group.hasMembers()) {
        this.consumerGroups.delete(name);
      }
    }
  }

  async route(meta: MessageMetadata): Promise<number> {
    const results = await Promise.all(
      Array.from(this.consumerGroups.values()).map((group) =>
        this.groupRoute(group, meta)
      )
    );

    let processedCount = 0;
    let deliveryCount = 0;

    for (const result of results) {
      if (!result) continue;
      processedCount += result.processedCount;
      deliveryCount += result.deliveryCount;
    }

    // all consumers are non-operable or binded to other routingKeys
    if (!processedCount) {
      this.dlqManager.enqueue(meta, "no_consumers");
    }

    return deliveryCount;
  }

  async routeBatch(metas: MessageMetadata[]): Promise<number[]> {
    return Promise.all(metas.map((msg) => this.route(msg)));
  }

  private async groupRoute(group: TopicConsumerGroup, meta: MessageMetadata) {
    const { routingKey, correlationId, id } = meta;
    const isSingleConsumer = group.getName() || correlationId;
    const candidates = group.getMembers(id, correlationId);
    if (!candidates) return;

    let fallbackCandidateId;
    let processedCount = 0;
    let deliveryCount = 0;
    const now = Date.now();

    for (const candidateId of candidates) {
      // filter candidate
      if (!this.isSuitable(group, candidateId, now, routingKey)) continue;

      // prefer idle consumer for single consumer mode
      if (isSingleConsumer && !this.clientManager.isIdle(candidateId)) {
        fallbackCandidateId ??= candidateId;
        continue;
      }

      // create delivery
      deliveryCount += await this.deliver(candidateId, meta);
      processedCount++;
      fallbackCandidateId = undefined;
      if (isSingleConsumer) break;
    }

    // fallback for single consumer mode
    if (fallbackCandidateId) {
      deliveryCount += await this.deliver(fallbackCandidateId, meta);
      processedCount++;
    }

    return { processedCount, deliveryCount };
  }

  private async deliver(consumerId: number, meta: MessageMetadata) {
    // try to push at first, otherwise enqueue to pull
    if (this.subscriptionManager.hasListener(consumerId)) {
      const needAck = await this.subscriptionManager.pushTo(consumerId, meta);
      return needAck ? 1 : 0;
    }

    this.queueManager.enqueue(consumerId, meta);
    return 1;
  }

  private isSuitable(
    group: TopicConsumerGroup,
    consumerId: number,
    now: number,
    routingKey?: string
  ) {
    // skip non-operable members (backpressure)
    if (!this.clientManager.isOperable(consumerId, now)) return false;

    // filtered-out
    const expectedKeys = group.getRoutingKeys(consumerId);
    return !expectedKeys?.size || expectedKeys.has(routingKey!);
  }
}
// SRC/TOPIC/QUEUE_MANAGER.TS
class TopicQueueManager {
  // TODO: persist
  private queues = new Map<number, IPriorityQueue<number>>();
  private totalQueuedMessages: 0;

  constructor(private queueFactory: new () => IPriorityQueue<number>) {}

  addQueue(id: number) {
    this.queues.set(id, new this.queueFactory());
  }

  removeQueue(id: number) {
    this.queues.delete(id);
  }

  enqueue(id: number, meta: MessageMetadata) {
    this.queues.get(id)?.enqueue(meta.id, meta.priority);
    this.totalQueuedMessages++;
    return this.queues.get(id)?.size();
  }

  dequeue(id: number) {
    const messageId = this.queues.get(id)?.dequeue();
    if (messageId) this.totalQueuedMessages--;
    return messageId;
  }

  getMetadata() {
    return {
      size: this.totalQueuedMessages,
    };
  }
}
// SRC/TOPIC/DELAYED_MANAGER.TS
class TopicDelayedManager<Data> {
  constructor(
    private messageScheduler: DelayScheduler<number>,
    private messageStorage: IMessageStorage<Data>,
    private messageRouter: TopicMessageRouter<Data>,
    private deliveryCounter: TopicDeliveryCounter<Data>,
    private logger?: LogCollector
  ) {
    messageScheduler.setReadyCallback(this.dequeue);
  }

  enqueue(meta: MessageMetadata) {
    if (!meta.ttd) return;
    this.messageScheduler.schedule(meta.id, meta.ts + meta.ttd);
  }

  private dequeue = async (messageId: number) => {
    const meta = await this.messageStorage.readMetadata(messageId);
    if (!meta) return;

    const deliveryCount = await this.messageRouter.route(meta);
    this.deliveryCounter.setAwaitedDeliveries(meta.id, deliveryCount);

    this.logger?.log(`Message is routed to ${meta.topic}.`, meta);
  };

  getMetadata() {
    return {
      size: this.messageScheduler.pendingSize(),
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

  enqueue(meta: MessageMetadata, reason: DLQEntry<Data>["reason"]): void {
    this.messages.set(meta.id, reason);
    this.totalMessagesProcessed++;
    this.logger?.log(
      `Message is routed to DLQ. Reason: ${reason}.`,
      meta,
      "warn"
    );
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
  persist?: boolean; // true by def TODO: dont need
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
interface IConsumable<Data> {
  consume(consumerId: number, autoAck?: boolean): Promise<Data | undefined>;
  ack(consumerId: number, messageId?: number): Promise<number[]>;
  nack(
    consumerId: number,
    messageId?: number,
    requeue?: boolean
  ): Promise<number>;
  subscribe(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    autoAck?: boolean
  ): void;
  unsubscribe(consumerId: number): void;
}
interface IDLQConsumable<Data> {
  createDlqReader(
    consumerId: number
  ): AsyncGenerator<DLQEntry<Data>, void, unknown>;
  replayDlq(
    consumerId: number,
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ): Promise<number>;
}
class Topic<Data> implements IConsumable<Data>, IDLQConsumable<Data> {
  constructor(
    public readonly name: string,
    private readonly pipeline: MessagePipeline,
    private readonly messageStorage: IMessageStorage<Data>,
    private readonly messageRouter: TopicMessageRouter<Data>,
    private readonly queueManager: TopicQueueManager,
    private readonly subscriptionManager: TopicSubscriptionManager<Data>,
    private readonly dlqManager: TopicDLQManager<Data>,
    private readonly delayedManager: TopicDelayedManager<Data>,
    private readonly clientManager: TopicClientManager,
    private readonly producerFactory: ProducerFactory<Data>,
    private readonly deliveryCounter: TopicDeliveryCounter<Data>,
    private readonly pendingMessages: TopicPendingsMessages,
    private readonly ackManager: TopicAckManager<Data>,
    private readonly ackMonitor: TopicAckMonitor<Data>,
    private readonly metrics: TopicMetricsCollector,
    private readonly logger?: LogCollector
  ) {}

  // core

  private async publish(
    producerId: number,
    message: Buffer,
    meta: MessageMetadata
  ): Promise<void> {
    this.clientManager.validateClient(producerId, "producer");
    await this.messageStorage.writeAll(message, meta);

    const processingTime = Date.now() - meta.ts;
    this.metrics.recordEnqueue(meta.size, processingTime);
    this.clientManager.recordActivity(producerId, {
      messageCount: 1,
      processingTime,
      status: "idle",
    });

    if (this.pipeline.process(meta)) return;
    const deliveryCount = await this.messageRouter.route(meta);
    this.deliveryCounter.setAwaitedDeliveries(meta.id, deliveryCount);

    this.logger?.log(`Message is routed to ${this.name}.`, meta);
  }

  async consume(consumerId: number, autoAck = false) {
    this.clientManager.validateClient(consumerId, "consumer");
    const messageId = this.queueManager.dequeue(consumerId);
    if (!messageId) return;

    const [message, meta] = await this.messageStorage.readAll(messageId);
    if (!meta || !message) return;

    if (!autoAck) {
      this.pendingMessages.addPending(consumerId, messageId);
    } else {
      await this.deliveryCounter.decrementAwaitedDeliveries(messageId);
      this.clientManager.recordActivity(consumerId, {
        messageCount: 1,
        pendingMessages: 0,
        processingTime: 0,
        status: "idle",
      });
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

  subscribe(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    autoAck?: boolean
  ) {
    this.clientManager.validateClient(consumerId, "consumer");
    this.subscriptionManager.addListener(consumerId, listener, autoAck);
  }

  unsubscribe(consumerId: number) {
    this.clientManager.validateClient(consumerId, "consumer");
    this.subscriptionManager.removeListener(consumerId);
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

    const start = Date.now();
    this.clientManager.recordActivity(consumerId, {
      status: "active",
    });

    const replayedCount = await this.dlqManager.replayMessages(handler, filter);

    this.clientManager.recordActivity(consumerId, {
      messageCount: replayedCount,
      processingTime: Date.now() - start,
      status: "idle",
    });

    return replayedCount;
  }

  // clients

  createProducer() {
    const id = uniqueIntGenerator();
    this.clientManager.addClient("producer", id);
    this.logger?.log(`producer_created`, {
      topic: this.name,
      clientId: id,
    });

    return this.producerFactory.create(this.publish, id);
  }

  createConsumer(
    options: {
      routingKeys?: string[];
      groupId?: string;
      limit?: number;
      autoAck?: boolean;
    } = {}
  ) {
    const { groupId, routingKeys, autoAck, limit } = options;
    const id = uniqueIntGenerator();
    this.messageRouter.addConsumer(id, groupId, routingKeys);
    this.clientManager.addClient("consumer", id);
    this.queueManager.addQueue(id);

    this.logger?.log(`consumer_created`, {
      topic: this.name,
      clientId: id,
    });

    return new Consumer(this, id, autoAck, limit);
  }

  createDLQConsumer(options?: { limit?: number }) {
    const id = uniqueIntGenerator();
    this.clientManager.addClient("dlq_consumer", id);

    this.logger?.log(`dlq_consumer_created`, {
      topic: this.name,
      clientId: id,
    });

    return new DLQConsumer(this, id, options?.limit);
  }

  deleteClient(id: number) {
    this.messageRouter.removeConsumer(id);
    this.clientManager.removeClient(id);
    this.queueManager.removeQueue(id);

    this.logger?.log("client_deleted", {
      topic: this.name,
      clientId: id,
    });
  }

  // metadata

  getMetadata() {
    return {
      ...this.metrics.getMetrics(),
      name: this.name,
      clients: this.clientManager.getMetadata(),
      pendingMessages: this.queueManager.getMetadata(),
      delayedMessages: this.delayedManager.getMetadata(),
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
    const messageStorage = new LevelDBMessageStorage<Data>(
      name,
      this.codec,
      mergedConfig.retentionMs,
      mergedConfig.persist,
      mergedConfig.persistThresholdMs
    );
    const metrics = new TopicMetricsCollector();
    const clientManager = new TopicClientManager(
      mergedConfig.consumerInactivityThresholdMs,
      mergedConfig.consumerProcessingTimeThresholdMs,
      mergedConfig.consumerPendingThresholdMs
    );
    const queueManager = new TopicQueueManager(
      HighCapacityBinaryHeapPriorityQueue
    );
    const dlqManager = new TopicDLQManager<Data>(name, messageStorage, logger);
    const deliveryCounter = new TopicDeliveryCounter(messageStorage, metrics);
    const pendingMessages = new TopicPendingsMessages(clientManager);
    const subscriptionManager = new TopicSubscriptionManager<Data>(
      clientManager,
      queueManager,
      messageStorage,
      pendingMessages
    );
    const messageRouter = new TopicMessageRouter<Data>(
      clientManager,
      queueManager,
      subscriptionManager,
      dlqManager
    );
    const delayedManager = new TopicDelayedManager<Data>(
      new DelayScheduler<number>(new HighCapacityBinaryHeapPriorityQueue()),
      messageStorage,
      messageRouter,
      deliveryCounter,
      logger
    );
    const pipeline = new PipelineFactory<Data>().create(
      dlqManager,
      delayedManager,
      mergedConfig?.maxDeliveryAttempts
    );
    const ackManager = new TopicAckManager(
      pendingMessages,
      deliveryCounter,
      messageStorage,
      pipeline,
      queueManager,
      logger
    );
    const ackMonitor = new TopicAckMonitor(
      pendingMessages,
      ackManager,
      mergedConfig?.ackTimeoutMs
    );

    return new Topic<Data>(
      name,
      pipeline,
      messageStorage,
      messageRouter,
      queueManager,
      subscriptionManager,
      dlqManager,
      delayedManager,
      clientManager,
      new ProducerFactory(
        this.codec,
        name,
        () => metrics.getMetrics().totalBytes,
        validator,
        mergedConfig.maxMessageSize
      ),
      deliveryCounter,
      pendingMessages,
      ackManager,
      ackMonitor,
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