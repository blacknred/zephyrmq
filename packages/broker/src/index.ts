import msgpack from "@msgpack/msgpack";
import type { Options as AjvOptions, JSONSchemaType } from "ajv";
import Ajv from "ajv";
import crc from "crc-32";
import crypto from "crypto";
import fs from "fs/promises";
import { validateSchema } from "json-schema-compatibility";
import { Level } from "level";
import { clearImmediate, setImmediate } from "node:timers";
import path from "path";
import { BinaryHeapPriorityQueue } from "./binary_heap_priority_queue";
import BinaryCodec, { SchemaCompiler, type ICodec } from "./codec";
import {
  InMemoryHashRing,
  SHA256HashService,
  type IHashRing,
} from "./hashRing/MapStoreHashRing";
import { Mutex } from "./mutex";
import { uniqueIntGenerator } from "./utils";
//
//
//
// PERSISTENT_STRUCTURE
//
//
//
// MESSAGE
interface IMessageValidator<Data> {
  validate(data: { data: Data; size: number; dedupId?: string }): void;
}
class DeduplicationValidator<Data> implements IMessageValidator<Data> {
  constructor(private deduplicationTracker: IDeduplicationTracker) {}

  validate({ dedupId }: { dedupId?: string }) {
    if (!dedupId) return;
    if (this.deduplicationTracker.has(dedupId)) {
      throw new Error("Message was already published");
    }

    this.deduplicationTracker.add(dedupId);
  }
} 
class SchemaValidator<Data> implements IMessageValidator<Data> {
  constructor(
    private schemaRegistry: ISchemaRegistry,
    private schemaId: string
  ) {}

  validate({ data }: { data: Data }): void {
    const validator = this.schemaRegistry.getValidator(this.schemaId);
    if (!!validator && !validator(data)) {
      // @ts-ignore
      throw new Error(validator.errors);
    }
  }
}
class SizeValidator implements IMessageValidator<any> {
  constructor(private maxSize: number) {}

  validate({ size }: { size: number }) {
    if (size > this.maxSize) throw new Error("Message too large");
  }
}
class CapacityValidator implements IMessageValidator<any> {
  constructor(
    private topicMaxCapacity: number,
    private getTopicMetrics: () => Record<ITopicMetricType, number>
  ) {}

  validate({ size }: { size: number }) {
    const { totalBytes } = this.getTopicMetrics();
    if (totalBytes + size > this.topicMaxCapacity) {
      throw new Error(`Exceeds topic max size ${this.topicMaxCapacity}`);
    }
  }
}
// interface MetadataInput
//   extends Pick<
//     MessageMetadata,
//     "priority" | "correlationId" | "ttd" | "ttl" | "dedupId"
//   > {}
// interface IMessageCreationResult {
//   meta: MessageMetadata;
//   message?: Buffer;
//   error?: any;
// }
// interface IMessageFactory<Data> {
//   create(
//     batch: Data[],
//     metadataInput: MetadataInput & {
//       topic: string;
//       producerId: number;
//     }
//   ): Promise<IMessageCreationResult[]>;
// }
// class MessageFactory<Data> implements IMessageFactory<Data> {
//   constructor(
//     private codec: ICodec,
//     private validators: IMessageValidator<Data>[],
//     private schemaId?: string
//   ) {}

//   async create(
//     batch: Data[],
//     metadataInput: MetadataInput & { topic: string; producerId: number }
//   ) {
//     return Promise.all(
//       batch.map(async (data) => {
//         const meta = new MessageMetadata();
//         Object.assign(meta, metadataInput);
//         meta.id = uniqueIntGenerator();

//         try {
//           const message = await this.codec.encode(data, this.schemaId);
//           this.validators.forEach((v) =>
//             v.validate({ data, size: message.length, dedupId: meta.dedupId })
//           );
//           return { meta, message };
//         } catch (error) {
//           return { meta, error };
//         }
//       })
//     );
//   }
// }
//
//
// export interface RangeOptions<K> {
//   gt?: K
//   gte?: K
//   lt?: K
//   lte?: K
//   reverse?: boolean | undefined
//   limit?: number | undefined
// }
// 	interface RangeOptions {
// 		/** Starting key for a range **/
// 		start?: Key
// 		/** Ending key for a range **/
// 		end?: Key
// 		/** Iterate through the entries in reverse order **/
// 		reverse?: boolean
// 		/** Include version numbers in each entry returned **/
// 		versions?: boolean
// 		/** The maximum number of entries to return **/
// 		limit?: number
// 		/** The number of entries to skip **/
// 		offset?: number
// 		/** Use a snapshot of the database from when the iterator started **/
// 		snapshot?: boolean
// 		/** Use the provided transaction for this range query */
// 		transaction?: Transaction
// 	}
//
//
//
// SRC/PUBLISHING_SERVICE.TS
interface IMessageProcessor {
  process(meta: MessageMetadata): boolean;
}
class ExpirationProcessor<Data> implements IMessageProcessor {
  constructor(private dlq: IDLQManager<Data>) {}
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
    private deliveryTracker: IDeliveryTracker,
    private dlq: IDLQManager<Data>
  ) {}
  process(meta: MessageMetadata): boolean {
    const attempts = this.deliveryTracker.decrementDeliveryAttempts(meta.id);
    const shouldDeadLetter = attempts === 0;
    if (shouldDeadLetter) {
      this.dlq.enqueue(meta, "max_attempts");
    }
    return shouldDeadLetter;
  }
}
class DelayProcessor implements IMessageProcessor {
  constructor(private delayedMessageManager: IDelayedMessageManager) {}
  process(meta: MessageMetadata): boolean {
    if (!meta.ttd) return false;
    const shouldDelay = meta.ts + meta.ttd! > Date.now();
    if (shouldDelay) this.delayedMessageManager.enqueue(meta);
    return shouldDelay;
  }
}
interface IMessagePipeline {
  addProcessor(processor: IMessageProcessor): void;
  process(meta: MessageMetadata): boolean;
}
class MessagePipeline implements IMessagePipeline {
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
interface IPipelineFactory<Data> {
  create(
    dlqManager: IDLQManager<Data>,
    deliveryTracker: IDeliveryTracker,
    delayedMessageManager: DelayedMessageManager<Data>
  ): IMessagePipeline;
}
class PipelineFactory<Data> implements IPipelineFactory<Data> {
  create(
    dlqManager: IDLQManager<Data>,
    deliveryTracker: IDeliveryTracker,
    delayedMessageManager: IDelayedMessageManager
  ) {
    const pipeline = new MessagePipeline();
    pipeline.addProcessor(new ExpirationProcessor(dlqManager));
    pipeline.addProcessor(new DelayProcessor(delayedMessageManager));
    pipeline.addProcessor(new AttemptsProcessor(deliveryTracker, dlqManager));

    return pipeline;
  }
}
interface IDeduplicationTracker {
  has(id: string): boolean;
  add(id: string): void;
}
class DeduplicationTracker implements IDeduplicationTracker {
  private seen: IPersistedMap<string, number>; // dedupId:timestamp
  constructor(
    mapFactory: IPersistedMapFactory,
    private deduplicationWindowMs = 300_000
  ) {
    this.seen = mapFactory.create<string, number>("seen");
  }

  has(id: string): boolean {
    const ts = this.seen.get(id);
    if (!ts) return false;
    if (Date.now() - ts > this.deduplicationWindowMs) {
      this.seen.delete(id);
      return false;
    }
    return true;
  }

  add(id: string): void {
    this.seen.set(id, Date.now());
  }
}
interface IDelayMonitor<Data> {
  setReadyCallback(onReadyHandler: (data: Data) => Promise<void>): void;
  schedule(data: Data, readyTs: number): void;
  pendingsCount(): number;
  cleanup(): void;
}
class DelayMonitor<Data> implements IDelayMonitor<Data> {
  private onReadyCallback?: (data: Data) => Promise<void>;
  private nextTimeout?: NodeJS.Timeout;
  private isProcessing = false;

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
        this.onReadyCallback?.(data);
      }
    } finally {
      this.isProcessing = false;
      this.setNextTimeout();
    }
  };

  pendingsCount() {
    return this.queue.size();
  }

  cleanup() {
    clearTimeout(this.nextTimeout);
  }
}
interface IDelayedMessageManager {
  enqueue(meta: MessageMetadata, consumerId?: number): void;
  getMetrics(): {
    count: number;
  };
}
class DelayedMessageManager<Data> implements IDelayedMessageManager {
  constructor(
    private delayMonitor: IDelayMonitor<[number, number | undefined]>,
    private messageStore: IMessageStore<Data>,
    private messagePublisher: IMessagePublisher,
    private queueManager: IQueueManager,
    private logger?: ILogCollector
  ) {
    delayMonitor.setReadyCallback(this.dequeue);
  }

  enqueue(meta: MessageMetadata, consumerId?: number) {
    if (!meta.ttd) return;
    const delay = meta.ts + meta.ttd;
    this.delayMonitor.schedule([meta.id, consumerId], delay);
    this.logger?.log(`Message is delayed until ${delay}.`, meta);
  }

  private dequeue = async (data: [number, number | undefined]) => {
    const [messageId, consumerId] = data;
    const meta = await this.messageStore.readMetadata(messageId);
    if (!meta) return;

    if (consumerId) {
      this.queueManager.enqueue(consumerId, meta);
      this.logger?.log(`Message is requeued to ${consumerId}.`, meta);
      return;
    }

    await this.messagePublisher.publish(meta);
  };

  getMetrics() {
    return {
      count: this.delayMonitor.pendingsCount(),
    };
  }
}
interface IConsumerGroup {
  name: string;
  addMember(id: number, routingKeys?: string[]): void;
  removeMember(id: number): void;
  hasMembers(): boolean;
  getMembers(
    messageId: number,
    correlationId?: string
  ): Iterable<number> | undefined;
  isDefaultGroup(): boolean;
  getMemberRoutingKeys(id: number): Array<string> | undefined;
  getMetrics(): {
    name: string;
    count: number;
  };
}
class ConsumerGroup implements IConsumerGroup {
  static defaultName = "non-grouped";
  private members: IPersistedMap<number, string[]>;

  constructor(
    public name: string = ConsumerGroup.defaultName,
    mapFactory: IPersistedMapFactory,
    private hashRing: IHashRing
  ) {
    this.members = mapFactory.create<number, string[]>(`members!${name}`);
  }

  addMember(id: number, routingKeys?: string[]) {
    // for the group with defined groupId we need to enforce homogeneous routingKeys within the members
    if (this.name && this.members.size > 0) {
      const expectedKeys = this.members.values().next().value as
        | Array<string>
        | undefined;
      const isValid =
        expectedKeys?.length === routingKeys?.length &&
        routingKeys?.every((k) => expectedKeys?.includes(k));

      if (!isValid) {
        throw new Error(
          `Member ${id} has incompatible routingKeys for group ${this.name}`
        );
      }
    }

    this.hashRing.addNode(id);
    this.members.set(id, routingKeys || []);
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

  isDefaultGroup() {
    return this.name === ConsumerGroup.defaultName;
  }

  getMemberRoutingKeys(id: number) {
    return this.members.get(id);
  }

  getMetrics() {
    return {
      name: this.name,
      count: this.members.size,
    };
  }
}
class ConsumerGroupSerializer implements ISerializable {
  constructor(private mapFactory: IPersistedMapFactory) {}
  serialize(group: IConsumerGroup) {
    return group.name;
  }
  deserialize(groupId: string) {
    return new ConsumerGroup(
      groupId,
      this.mapFactory,
      new InMemoryHashRing(new SHA256HashService(), this.mapFactory, groupId)
    );
  }
}
interface IMessageRouter {
  addConsumer(id: number, groupId?: string, routingKeys?: string[]): void;
  removeConsumer(id: number): void;
  route(meta: MessageMetadata, skipDLQ?: boolean): Promise<number>;
  routeBatch(metas: MessageMetadata[]): Promise<number[]>;
  getMetrics(): {
    consumerGroups: {
      name: string;
      count: number;
    }[];
  };
}
class MessageRouter<Data> implements IMessageRouter {
  private consumerGroups: IPersistedMap<string, IConsumerGroup>;

  constructor(
    private mapFactory: IPersistedMapFactory,
    private activityTracker: IClientActivityTracker,
    private queueManager: IQueueManager,
    private subscriptionManager: ISubscriptionManager<Data>,
    private dlqManager: IDLQManager<Data>,
    private processedMessageTracker: IProcessedMessageTracker
  ) {
    this.consumerGroups = mapFactory.create<string, IConsumerGroup>(
      `groups`,
      new ConsumerGroupSerializer(mapFactory)
    );
  }

  getMetrics() {
    return {
      consumerGroups: Array.from(this.consumerGroups.values()).map((group) =>
        group.getMetrics()
      ),
    };
  }

  addConsumer(
    id: number,
    groupId = ConsumerGroup.defaultName,
    routingKeys?: string[]
  ) {
    if (!this.consumerGroups.has(groupId)) {
      const hashRing = new InMemoryHashRing(
        new SHA256HashService(),
        this.mapFactory,
        groupId
      );
      this.consumerGroups.set(
        groupId,
        new ConsumerGroup(groupId, this.mapFactory, hashRing)
      );
    }

    this.consumerGroups.get(groupId)!.addMember(id, routingKeys);
  }

  removeConsumer(id: number) {
    for (const [name, group] of this.consumerGroups.entries()) {
      group.removeMember(id);
      if (!group.hasMembers()) {
        group;
        this.consumerGroups.delete(name);
      }
    }
  }

  async route(meta: MessageMetadata, skipDLQ = false): Promise<number> {
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
    if (!skipDLQ && !processedCount) {
      this.dlqManager.enqueue(meta, "no_consumers");
    }

    return deliveryCount;
  }

  async routeBatch(metas: MessageMetadata[]): Promise<number[]> {
    return Promise.all(metas.map((msg) => this.route(msg)));
  }

  private async groupRoute(group: IConsumerGroup, meta: MessageMetadata) {
    const { routingKey, correlationId, id } = meta;
    const isSingleConsumer = !group.isDefaultGroup() || correlationId;
    const candidates = group.getMembers(id, correlationId);
    if (!candidates) return;

    let fallbackCandidateId;
    let processedCount = 0;
    let deliveryCount = 0;
    const now = Date.now();

    for (const candidateId of candidates) {
      // filter candidate
      if (!this.isSuitable(group, candidateId, meta.id, now, routingKey))
        continue;

      // prefer idle consumer for single consumer mode
      if (isSingleConsumer && !this.activityTracker.isIdle(candidateId)) {
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
    group: IConsumerGroup,
    consumerId: number,
    messageId: number,
    now: number,
    routingKey?: string
  ) {
    // skip those who have processed it (exactly-once)
    if (this.processedMessageTracker.has(consumerId, messageId)) return false;

    // skip non-operable members (backpressure)
    if (!this.activityTracker.isOperable(consumerId, now)) return false;

    // filter by keys
    const expectedKeys = group.getMemberRoutingKeys(consumerId);
    return !expectedKeys?.length || expectedKeys.indexOf(routingKey!) !== -1;
  }
}
interface IMessagePublisher {
  publish(meta: MessageMetadata, skipDLQ?: boolean): Promise<void>;
}
class MessagePublisher implements IMessagePublisher {
  constructor(
    private readonly pipeline: IMessagePipeline,
    private readonly messageRouter: IMessageRouter,
    private readonly deliveryTracker: IDeliveryTracker,
    private readonly logger?: ILogCollector
  ) {}

  async publish(meta: MessageMetadata, skipDLQ = false) {
    if (this.pipeline.process(meta)) return;
    const deliveryCount = await this.messageRouter.route(meta, skipDLQ);
    this.deliveryTracker.setAwaitedDeliveries(meta.id, deliveryCount);

    this.logger?.log(`Message is published in ${meta.topic}.`, meta);
  }
}
// interface IPublishingService {
//   publish(
//     producerId: number,
//     message: Buffer,
//     meta: MessageMetadata
//   ): Promise<void>;
//   getMetrics(): {
//     router: {
//       consumerGroups: {
//         name: string;
//         count: number;
//       }[];
//     };
//     delayedMessages: {
//       count: number;
//     };
//   };
// }
// class PublishingService<Data> implements IPublishingService {
//   constructor(
//     private readonly messageStore: IMessageStore<Data>,
//     private readonly messageRouter: IMessageRouter,
//     private readonly messagePublisher: IMessagePublisher,
//     private readonly activityTracker: IClientActivityTracker,
//     private readonly delayedManager: IDelayedMessageManager,
//     private readonly metrics: IMetricsCollector
//   ) {}

//   async publish(
//     producerId: number,
//     message: Buffer,
//     meta: MessageMetadata
//   ): Promise<void> {
//     await this.messageStore.write(message, meta);

//     const processingTime = Date.now() - meta.ts;
//     this.metrics.recordEnqueue(message.length, processingTime);
//     this.activityTracker.recordActivity(producerId, {
//       messageCount: 1,
//       processingTime,
//       status: "idle",
//     });

//     await this.messagePublisher.publish(meta);
//   }

//   getMetrics() {
//     return {
//       router: this.messageRouter.getMetrics(),
//       delayedMessages: this.delayedManager.getMetrics(),
//       storage: this.messageStore.getMetrics(),
//     };
//   }
// }
//
//
//
// SRC/CONSUMPTION_SERVICE.TS
interface IPriorityQueue<Data = any> {
  enqueue(data: Data, priority?: number): void;
  dequeue(): Data | undefined;
  peek(): Data | undefined;
  isEmpty(): boolean;
  size(): number;
}
interface IQueueManager {
  addQueue(id: number): void;
  removeQueue(id: number): void;
  enqueue(id: number, meta: MessageMetadata): number | undefined;
  dequeue(id: number): number | undefined;
  getMetrics(): {
    size: number;
  };
}
class QueueManager implements IQueueManager {
  private queues: IPersistedMap<number, IPersistedQueue<number>>;
  private totalQueuedMessages = 0;

  constructor(
    mapFactory: IPersistedMapFactory,
    private queueFactory: IPersistedQueueFactory
  ) {
    this.queues = mapFactory.create<number, IPersistedQueue<number>>(
      `queues`,
      new PersistedQueueSerializer(queueFactory, (n: number) => n)
    );
  }

  addQueue(id: number) {
    const queue = this.queueFactory.create(`queue!${id}`, (n: number) => n);
    this.queues.set(id, queue);
  }

  removeQueue(id: number) {
    const queue = this.queues.get(id);
    if (!queue) return;
    queue.clear();
    queue.flush().then(() => {
      this.queues.delete(id);
    });
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

  getMetrics() {
    return {
      size: this.totalQueuedMessages,
    };
  }
}
interface IConsumptionService<Data> {
  consume(consumerId: number, noAck?: boolean): Promise<Data | undefined>;
  getMetrics(): {
    queuedMessages: {
      size: number;
    };
  };
}
class ConsumptionService<Data> implements IConsumptionService<Data> {
  constructor(
    private readonly queueManager: IQueueManager,
    private readonly messageStore: IMessageStore<Data>,
    private readonly pendingAcks: IAckRegistry,
    private readonly deliveryTracker: IDeliveryTracker,
    private readonly processedMessageTracker: IProcessedMessageTracker,
    private readonly activityTracker: IClientActivityTracker,
    private readonly logger?: ILogCollector
  ) {}

  async consume(consumerId: number, noAck = false): Promise<Data | undefined> {
    if (this.pendingAcks.isReachedMaxUnacked(consumerId)) return;

    const messageId = this.queueManager.dequeue(consumerId);
    if (!messageId) return;

    const [message, meta] = await this.messageStore.read(messageId);
    if (!meta || !message) return;

    if (!noAck) {
      this.pendingAcks.addAck(consumerId, messageId);
    } else {
      this.activityTracker.recordActivity(consumerId, {
        messageCount: 1,
        pendingAcks: 0,
        processingTime: 0,
        status: "idle",
      });

      this.processedMessageTracker.add(consumerId, messageId);
      await this.deliveryTracker.decrementAwaitedDeliveries(messageId);
    }

    this.logger?.log(`Message is consumed from ${meta.topic}.`, meta);
    return message;
  }

  getMetrics() {
    return {
      queuedMessages: this.queueManager.getMetrics(),
    };
  }
}
//
//
//
// SRC/ACK_SERVICE.TS
interface IDeliveryTracker {
  setAwaitedDeliveries(messageid: number, deliveries: number): void;
  decrementAwaitedDeliveries(messageId: number): Promise<void>;
  decrementDeliveryAttempts(messageId: number): number;
  getDeliveryRetryBackoff(messageId: number): number;
}
interface IDeliveryEntry {
  awaited: number;
  attempts: number;
}
class DeliveryTracker<Data> implements IDeliveryTracker {
  private deliveries: IPersistedMap<number, IDeliveryEntry>;

  constructor(
    mapFactory: IPersistedMapFactory,
    private messageStore: IMessageStore<Data>,
    private metrics: IMetricsCollector,
    private readonly maxAttempts = 1,
    private readonly initialBackoffMs = 1000,
    private readonly maxBackoffMs = 30_000
  ) {
    this.deliveries = mapFactory.create<number, IDeliveryEntry>("deliveries");
  }

  private getOrCreateEntry(messageid: number) {
    const entry = this.deliveries.get(messageid);
    return entry ?? { awaited: 0, attempts: Math.max(1, this.maxAttempts) };
  }

  setAwaitedDeliveries(messageid: number, deliveries: number) {
    const entry = this.getOrCreateEntry(messageid);
    entry.awaited = deliveries;
    entry.attempts = this.maxAttempts;
    this.deliveries.set(messageid, entry);
  }

  async decrementAwaitedDeliveries(messageId: number) {
    let entry = this.deliveries.get(messageId);
    if (!entry) return;
    if (--entry.awaited > 0) {
      this.deliveries.set(messageId, entry);
      return;
    }

    this.deliveries.delete(messageId);
    // if there is no deliveries needed mark message as consumed
    await this.messageStore.markDeletable(messageId);

    const meta = await this.messageStore.readMetadata(messageId, ["ts"]);
    if (meta) this.metrics.recordDequeue(Date.now() - meta.ts);
  }

  decrementDeliveryAttempts(messageId: number): number {
    const entry = this.getOrCreateEntry(messageId);
    if (--entry.attempts == 0) {
      this.deliveries.delete(messageId);
    } else {
      this.deliveries.set(messageId, entry);
    }

    return entry.attempts;
  }

  getDeliveryRetryBackoff(messageId: number) {
    const attempts = this.deliveries.get(messageId)?.attempts;
    if (!attempts) return 0;
    return Math.min(
      this.maxBackoffMs ?? 30_000,
      this.initialBackoffMs * Math.pow(2, attempts - 1)
    );
  }
}
interface IProcessedMessageRegistry {
  has(consumerId: number, messageId: number): boolean;
  add(consumerId: number, messageId: number): void;
  remove(consumerId: number, messageId: number): void;
}
class ProcessedMessageRegistry implements IProcessedMessageRegistry {
  private processed: IPersistedMap<number, Map<number, number>>; // consumerId:{messageId:ts}

  constructor(
    mapFactory: IPersistedMapFactory,
    private deduplicationWindowMs = 300_000
  ) {
    this.processed = mapFactory.create<number, Map<number, number>>(
      "processed",
      new MapSerializer<number, number>()
    );
  }

  has(consumerId: number, messageId: number): boolean {
    const processed = this.processed.get(consumerId);
    if (!processed) return false;
    const ts = processed.get(messageId);
    if (!ts) return false;
    if (Date.now() - ts > this.deduplicationWindowMs) {
      processed.delete(messageId);
      return false;
    }
    return true;
  }

  add(consumerId: number, messageId: number): void {
    let processed = this.processed.get(consumerId);
    if (!processed) {
      processed = new Map();
      this.processed.set(consumerId, processed);
    }
    processed.set(messageId, Date.now());
  }

  remove(consumerId: number, messageId: number): void {
    this.processed.get(consumerId)?.delete(messageId);
  }
}
interface IAckRegistry {
  addAck(consumerId: number, messageId: number): void;
  getAcks(consumerId: number): Map<number, number> | undefined;
  getAllAcks(): IPersistedMap<number, Map<number, number>>;
  removeAck(consumerId: number, messageId?: number): void;
  isReachedMaxUnacked(consumerId: number): boolean;
  getMetrics(): {
    count: number;
  };
}
class AckRegistry implements IAckRegistry {
  private acks: IPersistedMap<number, Record<number, number>>; // consumerId:{messageId:ts}

  constructor(
    mapFactory: IPersistedMapFactory,
    private activityTracker: IClientActivityTracker,
    private maxUnackedPerConsumer = 10
  ) {
    this.acks = mapFactory.create<number, Record<number, number>>("acks");
  }

  isReachedMaxUnacked(consumerId: number) {
    return this.getAcks(consumerId)?.size === this.maxUnackedPerConsumer;
  }

  getMetrics() {
    let count = 0;
    const consumerAcks = this.acks.values();
    for (const acks of consumerAcks) {
      count += acks.size;
    }
    return { count };
  }

  addAck(consumerId: number, messageId: number): void {
    if (!this.acks.has(consumerId)) {
      this.acks.set(consumerId, new Map());
    }
    this.acks.get(consumerId)?.set(messageId, Date.now());

    this.activityTracker.recordActivity(consumerId, {
      pendingAcks: 1,
      status: "active",
    });
  }

  getAcks(consumerId: number) {
    return this.acks.get(consumerId);
  }

  getAllAcks() {
    return this.acks;
  }

  removeAck(consumerId: number, messageId?: number): void {
    const pendings = this.acks.get(consumerId);
    if (!pendings) return;
    const now = Date.now();

    if (messageId) {
      if (pendings.has(messageId)) {
        const consumedAt = pendings.get(messageId)!;
        pendings.delete(messageId);

        this.activityTracker.recordActivity(consumerId, {
          pendingAcks: -1,
          messageCount: 1,
          processingTime: now - consumedAt,
          status: pendings.size ? "active" : "idle",
        });
      }
    } else if (pendings.size) {
      const firstConsumeAt = pendings.values().next().value as number;
      this.activityTracker.recordActivity(consumerId, {
        pendingAcks: -pendings.size,
        messageCount: pendings.size,
        processingTime: Date.now() - firstConsumeAt,
        status: "idle",
      });

      pendings.clear();
    }

    if (!pendings.size) {
      this.acks.delete(consumerId);
    }
  }
}
interface AckTimeoutHandler {
  (consumerId: number, messageId?: number, requeue?: boolean): Promise<number>;
}
interface IAckMonitor {
  setTimeoutCallback(onTimeoutHandler: AckTimeoutHandler): void;
  stop(): void;
}
class AckMonitor implements IAckMonitor {
  private onTimeoutCallback?: AckTimeoutHandler;
  private timer?: NodeJS.Timeout;

  constructor(
    private pendingAcks: IAckRegistry,
    private ackTimeoutMs = 30_000
  ) {
    this.timer = setInterval(
      this.nackTimedOutPendings,
      Math.max(1000, this.ackTimeoutMs / 2)
    );
  }

  setTimeoutCallback(onTimeoutHandler: AckTimeoutHandler) {
    this.onTimeoutCallback = onTimeoutHandler;
  }

  stop() {
    clearInterval(this.timer);
  }

  private nackTimedOutPendings = async () => {
    const now = Date.now();
    const pendings = this.pendingAcks.getAllAcks();
    if (!pendings) return;
    for (const [consumerId, messages] of pendings.entries()) {
      for (const [messageId, consumedAt] of messages.entries()) {
        if (now - consumedAt > this.ackTimeoutMs) {
          await this.onTimeoutCallback?.(consumerId, messageId, false);
        }
      }
    }
  };
}
interface IAckService {
  ack(consumerId: number, messageId?: number): Promise<number[]>;
  nack: (
    consumerId: number,
    messageId?: number,
    requeue?: boolean
  ) => Promise<number>;
  getMetrics(): {
    pendingAcks: {
      count: number;
    };
  };
}
class AckService<Data> implements IAckService {
  constructor(
    private readonly pendingAcks: IAckRegistry,
    private readonly deliveryTracker: IDeliveryTracker,
    private readonly processedMessageTracker: IProcessedMessageTracker,
    private readonly ackMonitor: IAckMonitor,
    private readonly messageStore: IMessageStore<Data>,
    private readonly messagePublisher: IMessagePublisher,
    private readonly pipeline: IMessagePipeline,
    private readonly subscriptionRegistry: ISubscriptionRegistry<Data>,
    private readonly subscriptionProcessor: ISubscriptionQueueProcessor,
    private readonly delayedMessageManager: IDelayedMessageManager,
    private readonly logger?: ILogCollector
  ) {
    this.ackMonitor.setTimeoutCallback(this.nack);
  }

  async ack(consumerId: number, messageId?: number): Promise<number[]> {
    const pendingAcks: number[] = [];

    if (messageId) {
      pendingAcks.push(messageId);
      this.pendingAcks.removeAck(consumerId, messageId);
    } else {
      const pendingMap = this.pendingAcks.getAcks(consumerId);
      if (pendingMap) pendingAcks.push(...pendingMap.keys());
      this.pendingAcks.removeAck(consumerId);
    }

    for (const messageId of pendingAcks) {
      this.processedMessageTracker.add(consumerId, messageId);
      await this.deliveryTracker.decrementAwaitedDeliveries(messageId);
    }

    // try to drainQueue if push mode is available
    if (this.subscriptionRegistry.hasListener(consumerId)) {
      this.subscriptionProcessor.drainQueue(consumerId);
    }

    return pendingAcks;
  }

  nack = async (consumerId: number, messageId?: number, requeue = true) => {
    const messages = await this.ack(consumerId, messageId);

    for (const messageId of messages) {
      const meta = await this.messageStore.readMetadata(messageId);
      if (!meta) continue;

      this.logger?.log(
        `Message is nacked to ${requeue ? consumerId : meta.topic}.`,
        meta,
        "warn"
      );

      // Consumer nacked with requeue=false (as well as AckMonitor nack call) means he rejected to process the message
      if (!requeue) {
        // mark message as processed by consumer
        this.processedMessageTracker.add(consumerId, messageId);
        //    reroute it with skipDLQ=true to avoid message ends up in dlq due the 'no_consumers' reason.
        // Consumer as well as other consumers which have processed the message will not get it again.
        //    Rerouting helps to redeliver message to another consumer group member.
        await this.messagePublisher.publish(meta, true);
        continue;
      }

      // Consumer nacked with requeue=true: he will process message later so we send it to consumer queue after backoff
      if (this.pipeline.process(meta)) continue;
      const delay = this.deliveryTracker.getDeliveryRetryBackoff(messageId);
      meta.ttd = Date.now() - meta.ts + delay;
      this.delayedMessageManager.enqueue(meta, consumerId);
    }

    return messages.length;
  };

  getMetrics() {
    return {
      pendingAcks: this.pendingAcks.getMetrics(),
    };
  }
}
//
//
//
// SRC/SUBSCRIPTION_SERVICE.TS
type ISubscriptionListener<Data> = (message: Data) => Promise<void>;
interface ISubscriptionRegistry<Data> {
  addListener(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    noAck?: boolean
  ): void;
  removeListener(consumerId: number): void;
  hasListener(consumerId: number): boolean;
  getListener(consumerId: number): ISubscriptionListener<Data> | undefined;
  getAllListeners(): IterableIterator<[number, ISubscriptionListener<Data>]>;
  hasFanout(consumerId: number): boolean;
  size: number;
}
class SubscriptionRegistry<Data> implements ISubscriptionRegistry<Data> {
  private listeners = new Map<number, ISubscriptionListener<Data>>();
  private fanouts = new Set<number>();

  addListener(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    noAck = false
  ) {
    this.listeners.set(consumerId, listener);
    if (noAck) this.fanouts.add(consumerId);
  }

  removeListener(consumerId: number) {
    this.listeners.delete(consumerId);
    this.fanouts.delete(consumerId);
  }

  hasListener(consumerId: number): boolean {
    return this.listeners.has(consumerId);
  }

  getListener(consumerId: number): ISubscriptionListener<Data> | undefined {
    return this.listeners.get(consumerId);
  }

  getAllListeners(): IterableIterator<[number, ISubscriptionListener<Data>]> {
    return this.listeners.entries();
  }

  hasFanout(consumerId: number) {
    return this.fanouts.has(consumerId);
  }

  get size(): number {
    return this.listeners.size;
  }
}
interface ISubscriptionMessageDispatcher {
  pushTo(
    consumerId: number,
    meta: MessageMetadata
  ): Promise<boolean | undefined>;
}
class SubscriptionMessageDispatcher<Data>
  implements ISubscriptionMessageDispatcher
{
  constructor(
    private registry: ISubscriptionRegistry<Data>,
    private messageStore: IMessageStore<Data>,
    private pendingAcks: IAckRegistry,
    private processedMessageTracker: IProcessedMessageTracker,
    private activityTracker: IClientActivityTracker,
    private logger?: ILogCollector
  ) {}

  async pushTo(
    consumerId: number,
    meta: MessageMetadata
  ): Promise<boolean | undefined> {
    const listener = this.registry.getListener(consumerId);
    if (!listener) return;

    if (this.pendingAcks.isReachedMaxUnacked(consumerId)) return;

    const message = await this.messageStore.readMessage(meta.id);
    if (!message) return;

    listener(message).catch((error) => {
      // TODO: record activity
    });

    if (!this.registry.hasFanout(consumerId)) {
      this.pendingAcks.addAck(consumerId, meta.id);
      return true;
    }

    this.processedMessageTracker.add(consumerId, meta.id);

    setImmediate(() => {
      this.logger?.log(`Message is consumed from ${meta.topic}.`, meta);
    });

    this.activityTracker.recordActivity(consumerId, {
      messageCount: 1,
      pendingAcks: 0,
      processingTime: 0,
      status: "idle",
    });

    return false;
  }
}
interface ISubscriptionQueueProcessor {
  drainQueue(consumerId: number): Promise<void>;
}
class SubscriptionQueueProcessor<Data> implements ISubscriptionQueueProcessor {
  constructor(
    private dispatcher: ISubscriptionMessageDispatcher,
    private queueManager: IQueueManager,
    private deliveryTracker: IDeliveryTracker,
    private messageStore: IMessageStore<Data>,
    private pendingAcks: IAckRegistry
  ) {}

  async drainQueue(consumerId: number): Promise<void> {
    if (this.pendingAcks.isReachedMaxUnacked(consumerId)) return;

    const messageId = this.queueManager.dequeue(consumerId);
    if (!messageId) return;

    const meta = await this.messageStore.readMetadata(messageId);
    if (!meta) return;

    const needAck = await this.dispatcher.pushTo(consumerId, meta);

    if (!needAck) {
      await this.deliveryTracker.decrementAwaitedDeliveries(messageId);
    }

    // schedule next drain
    setImmediate(() => this.drainQueue(consumerId));
  }
}
interface ISubscriptionMetricsCollector {
  getMetrics(): {
    count: number;
  };
}
class SubscriptionMetricsCollector<Data>
  implements ISubscriptionMetricsCollector
{
  constructor(private registry: ISubscriptionRegistry<Data>) {}

  getMetrics() {
    return {
      count: this.registry.size,
    };
  }
}
interface ISubscriptionService<Data> {
  subscribe(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    noAck?: boolean
  ): void;
  unsubscribe(consumerId: number): void;
  getMetrics(): {
    count: number;
  };
}
class SubscriptionService<Data> implements ISubscriptionService<Data> {
  constructor(
    private readonly topicName: string,
    private readonly registry: ISubscriptionRegistry<Data>,
    private readonly metrics: ISubscriptionMetricsCollector,
    private readonly logger?: ILogCollector
  ) {}

  subscribe(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    noAck?: boolean
  ): void {
    this.registry.addListener(consumerId, listener, noAck);

    this.logger?.log(`${consumerId} is subscribed to ${this.topicName}.`, {
      consumerId,
      noAck,
    });
  }

  unsubscribe(consumerId: number): void {
    this.registry.removeListener(consumerId);

    this.logger?.log(`${consumerId} is unsubscribed from ${this.topicName}.`, {
      consumerId,
    });
  }

  getMetrics() {
    return this.metrics.getMetrics();
  }
}
//
//
//
// SRC/DLQ_SERVICE.TS
type DLQReason =
  | "no_consumers"
  | "expired"
  | "max_attempts"
  | "validation"
  | "processing_error";
interface IDLQEntry<Data> {
  reason: DLQReason;
  message: Data;
  meta: MessageMetadata;
}
interface IDLQManager<Data> {
  enqueue(meta: MessageMetadata, reason: DLQReason): void;
  createReader(): AsyncGenerator<IDLQEntry<Data>, void, unknown>;
  replayMessages(
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ): Promise<number>;
  getMetrics(): {
    size: number;
  };
}
class DLQManager<Data> implements IDLQManager<Data> {
  private messages: PersistedMap<number, DLQReason>;

  constructor(
    private topic: string,
    mapFactory: IPersistedMapFactory,
    private messageStore: IMessageStore<any>,
    private logger?: ILogCollector
  ) {
    this.messages = mapFactory.create("dlq");
  }

  size() {
    return this.messages.size;
  }

  enqueue(meta: MessageMetadata, reason: DLQReason): void {
    this.messages.set(meta.id, reason);
    this.logger?.log(
      `Message is routed to DLQ. Reason: ${reason}.`,
      meta,
      "warn"
    );
  }

  async *createReader(): AsyncGenerator<IDLQEntry<Data>, void, unknown> {
    for (const [messageId, reason] of this.messages.entries()) {
      const [message, meta] = await this.messageStore.read(messageId);
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

  getMetrics() {
    return {
      size: this.messages.size,
    };
  }
}
interface IDLQService<Data> {
  createDlqReader(): AsyncGenerator<IDLQEntry<Data>, void, unknown>;
  replayDlq(
    consumerId: number,
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ): Promise<number>;
  getMetrics(): {
    size: number;
  };
}
class DLQService<Data> implements IDLQService<Data> {
  constructor(
    private readonly dlqManager: IDLQManager<Data>,
    private readonly clientManager: IClientActivityTracker
  ) {}

  createDlqReader() {
    return this.dlqManager.createReader();
  }

  async replayDlq(
    consumerId: number,
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ) {
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

  getMetrics() {
    return this.dlqManager.getMetrics();
  }
}
//
//
//
// SRC/CLIENT_MANAGEMENT_SERVICE.TS
// type ClientType = "producer" | "consumer" | "dlq_consumer";
// type ClientStatus = "active" | "idle" | "lagging";
// interface IMutableClientState {
//   lastActiveAt: number;
//   // Metrics
//   status: ClientStatus;
//   messageCount: number;
//   processingTime: number;
//   avgProcessingTime: number;
//   pendingAcks: number;
// }
// interface IClientState extends IMutableClientState {
//   id: number;
//   clientType: ClientType;
//   registeredAt: number;
// }
// class ClientState implements IClientState {
//   public registeredAt = Date.now();
//   public lastActiveAt = Date.now();
//   public status: ClientStatus = "active";
//   public messageCount = 0;
//   public processingTime = 0;
//   public avgProcessingTime = 0;
//   public pendingAcks = 0;
//   constructor(
//     public id: number,
//     public clientType: ClientType
//   ) {}
// }
// interface IClientRegistry {
//   addClient(type: ClientType, id: number): ClientState | undefined;
//   removeClient(id: number): number;
//   getClient(id: number): IClientState | undefined;
//   getClients(filter?: (client: IClientState) => boolean): Set<IClientState>;
//   updateClient(id: number, state: IMutableClientState): void;
//   exists(id: number): boolean;
//   throwIfExists(id: number): void;
// }
// class ClientRegistry implements IClientRegistry {
//   private clients: IPersistedMap<number, IClientState>;

//   constructor(mapFactory: IPersistedMapFactory) {
//     this.clients = mapFactory.create<number, IClientState>("clients");
//   }

//   addClient(type: ClientType, id: number) {
//     const client = this.getClient(id);
//     if (client) return;

//     const state = new ClientState(id, type);
//     state.lastActiveAt = Date.now();
//     this.clients.set(id, state);
//     return state;
//   }

//   removeClient(id: number) {
//     const client = this.clients.get(id);
//     if (client) this.clients.delete(id);
//     return this.clients.size;
//   }

//   getClient(id: number): IClientState | undefined {
//     return this.clients.get(id);
//   }

//   getClients(filter?: (client: IClientState) => boolean): Set<IClientState> {
//     const states = this.clients.values();
//     if (!filter) return new Set(states);
//     return new Set(Array.from(states).filter(filter));
//   }

//   updateClient(id: number, state: IMutableClientState) {
//     const client = this.clients.get(id);
//     if (!client) return;
//     this.clients.set(id, Object.assign(client, state));
//   }

//   exists(id: number): boolean {
//     return this.clients.has(id);
//   }

//   throwIfExists(id: number): void {
//     if (!this.exists(id)) return;
//     throw new Error(`Client with ID ${id} already exists`);
//   }
// }
interface IClientValidator {
  validate(id: number, expectedType?: ClientType): void;
}
class ClientValidator implements IClientValidator {
  constructor(private registry: IClientRegistry) {}

  validate(id: number, expectedType?: ClientType): void {
    const metadata = this.registry.getClient(id);
    if (!metadata) {
      throw new Error(`Client with ID ${id} not found`);
    }
    if (expectedType && metadata.clientType !== expectedType) {
      throw new Error(`Client with ID ${id} is not a ${expectedType}`);
    }
  }
}

interface IClientActivityTracker {
  recordActivity(id: number, activityRecord: Partial<IClientState>): void;
  isOperable(id: number, now: number): boolean;
  isIdle(id: number): boolean;
}
class ClientActivityTracker implements IClientActivityTracker {
  constructor(
    private registry: IClientRegistry,
    private inactivityThresholdMs = 300_000,
    private processingTimeThresholdMs = 50_000,
    private pendingThresholdMs = 100
  ) {}

  recordActivity(id: number, activityRecord: IMutableClientState): void {
    const client = this.registry.getClient(id);
    if (!client) return;

    // failures: nack with requeue=false, Consumer crash/connection lossб High average processing time, High average processing time, Schema validation failed

    client.lastActiveAt = Date.now();

    for (const k in activityRecord) {
      const key = k as keyof IMutableClientState;
      const value = activityRecord[key];

      if (typeof client[key] === "number" && typeof value === "number") {
        (client[key] as number) += value;
      } else if (typeof value === "string") {
        (client[key] as string) = value;
      }
    }

    if (client.messageCount > 0) {
      client.avgProcessingTime = client.processingTime / client.messageCount;
    }

    this.registry.updateClient(id, client);
  }

  isOperable(id: number, now: number): boolean {
    const client = this.registry.getClient(id);
    if (!client) return false;
    if (client.status === "lagging") return false;
    if (client.avgProcessingTime > this.processingTimeThresholdMs) return false;
    if (client.pendingAcks > this.pendingThresholdMs) return false;
    return now - client.lastActiveAt < this.inactivityThresholdMs;
  }

  isIdle(id: number): boolean {
    const client = this.registry.getClient(id);
    return !!client && client.status === "idle";
  }
}

interface IClientMetricsCollector {
  getMetrics(): {
    count: number;
    producersCount: number;
    consumersCount: number;
    dlqConsumersCount: number;
    operableCount: number;
    idleCount: number;
    avgProcessingTime: number;
  };
}
class ClientMetricsCollector implements IClientMetricsCollector {
  constructor(
    private registry: IClientRegistry,
    private tracker: IClientActivityTracker
  ) {}

  getMetrics() {
    let avgProcessingTime = 0;
    let producersCount = 0;
    let consumersCount = 0;
    let dlqConsumersCount = 0;
    let operableCount = 0;
    let idleCount = 0;
    const now = Date.now();

    const clients = this.registry.getClients();
    for (const client of clients) {
      avgProcessingTime += client.avgProcessingTime;
      if (this.tracker.isOperable(client.id, now)) operableCount++;
      if (this.tracker.isIdle(client.id)) idleCount++;

      switch (client.clientType) {
        case "producer":
          producersCount++;
          break;
        case "consumer":
          consumersCount++;
          break;
        case "dlq_consumer":
          dlqConsumersCount++;
          break;
      }
    }

    return {
      count: clients.size,
      producersCount,
      consumersCount,
      dlqConsumersCount,
      operableCount,
      idleCount,
      avgProcessingTime: avgProcessingTime / clients.size || 0,
    };
  }
}

// interface IPublishResult {
//   id: number;
//   status: "success" | "error";
//   ts: number;
//   error?: string;
// }
// interface IProducer<Data> {
//   id: number;
//   publish(batch: Data[], metadata?: MetadataInput): Promise<IPublishResult[]>;
// }
// class Producer<Data> implements IProducer<Data> {
//   constructor(
//     private readonly publishingService: IPublishingService,
//     private readonly messageFactory: IMessageFactory<Data>,
//     private readonly topicName: string,
//     public readonly id: number
//   ) {}

//   async publish(batch: Data[], metadata: MetadataInput = {}) {
//     const results: IPublishResult[] = [];

//     const messages = await this.messageFactory.create(batch, {
//       ...metadata,
//       topic: this.topicName,
//       producerId: this.id,
//     });

//     for (const { message, meta, error } of messages) {
//       const { id, ts } = meta;

//       if (error) {
//         const err = error instanceof Error ? error.message : "Unknown error";
//         results.push({ id, ts, error: err, status: "error" });
//         continue;
//       }

//       try {
//         await this.publishingService.publish(this.id, message!, meta);
//         results.push({ id, ts, status: "success" });
//       } catch (err) {
//         const error = err instanceof Error ? err.message : "Unknown error";
//         results.push({ id, ts, error, status: "error" });
//       }
//     }

//     return results;
//   }
// }
interface IProducerFactory<Data> {
  create(id: number): IProducer<Data>;
}
class ProducerFactory<Data> implements IProducerFactory<Data> {
  private messageFactory: IMessageFactory<Data>;
  constructor(
    private readonly topicName: string,
    private readonly publishingService: IPublishingService,
    deduplicationTracker: IDeduplicationTracker,
    schemaRegistry: ISchemaRegistry,
    metricsCollector: IMetricsCollector,
    codec: ICodec,
    schemaId?: string,
    maxMessageSize?: number,
    maxSizeBytes?: number
  ) {
    const validators: IMessageValidator<Data>[] = [];
    validators.push(new DeduplicationValidator(deduplicationTracker));
    if (schemaId) {
      validators.push(new SchemaValidator<Data>(schemaRegistry, schemaId));
    }
    if (maxMessageSize) {
      validators.push(new SizeValidator(maxMessageSize));
    }
    if (maxSizeBytes) {
      validators.push(
        new CapacityValidator(maxSizeBytes, metricsCollector.getMetrics)
      );
    }

    this.messageFactory = new MessageFactory<Data>(
      codec,
      validators,
      schemaRegistry,
      schemaId
    );
  }

  create(id: number) {
    return new Producer(
      this.publishingService,
      this.messageFactory,
      this.topicName,
      id
    );
  }
}
interface IConsumerConfig {
  routingKeys?: string[];
  groupId?: string;
  limit?: number;
  noAck?: boolean;
}
interface IConsumer<Data> {
  id: number;
  consume(): Promise<Data[]>;
  ack(messageId?: number): Promise<number[]>;
  nack(messageId?: number, requeue?: boolean): Promise<number>;
  subscribe(listener: ISubscriptionListener<Data>): void;
  unsubscribe(): void;
}
class Consumer<Data> implements IConsumer<Data> {
  private readonly limit: number;
  constructor(
    private readonly consumptionService: IConsumptionService<Data>,
    private readonly ackService: IAckService,
    private readonly subscriptionService: ISubscriptionService<Data>,
    public readonly id: number,
    private readonly noAck = false,
    limit?: number
  ) {
    this.limit = Math.max(1, limit!);
  }

  async consume() {
    const messages: Data[] = [];

    for (let i = 0; i < this.limit; i++) {
      const message = await this.consumptionService.consume(
        this.id,
        this.noAck
      );
      if (!message) break;
      messages.push(message);
    }

    return messages;
  }

  async ack(messageId?: number) {
    return this.ackService.ack(this.id, messageId);
  }

  async nack(messageId?: number, requeue = true): Promise<number> {
    return this.ackService.nack(this.id, messageId, requeue);
  }

  subscribe(listener: ISubscriptionListener<Data>): void {
    this.subscriptionService.subscribe(this.id, listener, this.noAck);
  }

  unsubscribe(): void {
    this.subscriptionService.unsubscribe(this.id);
  }
}
interface IDLQConsumer<Data> {
  id: number;
  consume(): Promise<IDLQEntry<Data>[]>;
  replayDlq(
    handler: (message: Data, meta: MessageMetadata) => Promise<void>,
    filter?: (meta: MessageMetadata) => boolean
  ): Promise<number>;
}
class DLQConsumer<Data> implements IDLQConsumer<Data> {
  private readonly limit: number;
  private reader: AsyncGenerator<IDLQEntry<Data>, void, unknown>;
  constructor(
    private readonly dlqService: IDLQService<Data>,
    public readonly id: number,
    limit?: number
  ) {
    this.limit = Math.max(1, limit!);
    // singleton reader allows to read only once, waiting for the newest messages to arrive
    this.reader = this.dlqService.createDlqReader();
  }

  async consume() {
    const messages: IDLQEntry<Data>[] = [];

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
    return this.dlqService.replayDlq(this.id, handler, filter);
  }
}
interface IClientManagementService<Data> {
  createProducer(): IProducer<Data>;
  createConsumer(config?: IConsumerConfig, id?: number): IConsumer<Data>;
  createDLQConsumer(limit?: number): DLQConsumer<Data>;
  deleteClient(id: number): void;
  getMetrics(): {
    count: number;
    producersCount: number;
    consumersCount: number;
    dlqConsumersCount: number;
    operableCount: number;
    idleCount: number;
    avgProcessingTime: number;
  };
}
class ClientManagementService<Data> implements IClientManagementService<Data> {
  constructor(
    private readonly producerFactory: IProducerFactory<Data>,
    private readonly clientRegistry: IClientRegistry,
    private readonly metrics: IClientMetricsCollector,
    private readonly messageRouter: IMessageRouter,
    private readonly queueManager: IQueueManager,
    private readonly consumptionService: IConsumptionService<Data>,
    private readonly ackService: IAckService,
    private readonly subscriptionService: ISubscriptionService<Data>,
    private readonly dlqService: IDLQService<Data>,
    private readonly logger?: ILogCollector
  ) {}

  createProducer(id = uniqueIntGenerator()): IProducer<Data> {
    this.clientRegistry.throwIfExists(id);
    this.clientRegistry.addClient("producer", id);
    this.logger?.log(`producer_created`, { id });

    return this.producerFactory.create(id);
  }

  createConsumer(
    config: IConsumerConfig = {},
    id = uniqueIntGenerator()
  ): IConsumer<Data> {
    this.clientRegistry.throwIfExists(id);
    const { groupId, routingKeys, noAck, limit } = config;
    this.messageRouter.addConsumer(id, groupId, routingKeys);
    this.clientRegistry.addClient("consumer", id);

    this.queueManager.addQueue(id);
    this.logger?.log(`consumer_created`, { id });

    return new Consumer(
      this.consumptionService,
      this.ackService,
      this.subscriptionService,
      id,
      noAck,
      limit
    );
  }

  createDLQConsumer(
    id = uniqueIntGenerator(),
    limit?: number
  ): DLQConsumer<Data> {
    this.clientRegistry.throwIfExists(id);
    this.clientRegistry.addClient("dlq_consumer", id);
    this.logger?.log(`dlq_consumer_created`, { id });

    return new DLQConsumer(this.dlqService, id, limit);
  }

  deleteClient(id: number) {
    this.messageRouter.removeConsumer(id);
    this.clientRegistry.removeClient(id);
    this.queueManager.removeQueue(id);

    this.logger?.log("client_deleted", { id });
  }

  getMetrics() {
    return this.metrics.getMetrics();
  }
}
//
//
//
// SRC/TOPIC.TS
class Topic<Data> implements ITopic<Data> {
  constructor(
    private readonly _name: string,
    private readonly _config: ITopicConfig,
    private readonly publishingService: IPublishingService,
    private readonly consumptionService: IConsumptionService<Data>,
    private readonly subscriptionService: ISubscriptionService<Data>,
    private readonly ackService: IAckService,
    private readonly dlqService: IDLQService<Data>,
    private readonly clientService: IClientManagementService<Data>,
    private readonly storageService: IMessageStore<Data>,
    private readonly metrics: IMetricsCollector
  ) {}

  get name() {
    return this._name;
  }

  get config() {
    return this._config;
  }

  async getMetrics() {
    return {
      name: this.name,
      subscriptions: this.subscriptionService.getMetrics(),
      clients: this.clientService.getMetrics(),
      dlq: this.dlqService.getMetrics(),
      storage: await this.storageService.getMetrics(),
      ...this.ackService.getMetrics(),
      ...this.publishingService.getMetrics(),
      ...this.consumptionService.getMetrics(),
      ...this.metrics.getMetrics(),
    };
  }

  createProducer() {
    return this.clientService.createProducer();
  }

  createConsumer(config: IConsumerConfig) {
    return this.clientService.createConsumer(config);
  }

  createDLQConsumer(limit?: number) {
    return this.clientService.createDLQConsumer(limit);
  }

  deleteClient(id: number) {
    return this.clientService.deleteClient(id);
  }

  async dispose() {}
}
class TopicFactory implements ITopicFactory {
  constructor(
    private schemaRegistry: ISchemaRegistry,
    private defaultConfig: ITopicConfig = {
      persistThresholdMs: 1000,
      retentionMs: 86_400_000, // 1 day
      maxDeliveryAttempts: 5,
      consumerPendingThresholdMs: 10,
    },
    private codecFactory: new () => ICodec = BinaryCodec,
    private queueFactory: new () => IPriorityQueue = BinaryHeapPriorityQueue,
    private storageFactory: new (
      ...args
    ) => IMessageStore<unknown> = LevelDBMessageStorage,
    private logService?: LogService
  ) {}

  create<Data>(name: string, config?: Partial<ITopicConfig>): Topic<Data> {
    // TODO: VALIDATE CONFIG AJV (add to registry)
    const mergedConfig = { ...this.defaultConfig, ...config };

    // new WriteAheadLog(path.join(topicDir, "wal.log"));
    // new MessageLog(path.join(topicDir, "segments"));
    // Level(path.join(topicDir, "metadata")); const topicDir = path.join(this.baseDir, this.topic);

    this.validateTopicName(name);
    const codec = new this.codecFactory();
    const logger = this.logService?.for(name);

    const db = new Level("./broker.db", {
      compression: false,
      valueEncoding: {
        type: "custom",
        encode: codec.encodeSync,
        decode: codec.decodeSync,
        buffer: true,
      },
    });

    // Build modules
    const messageStore = new this.storageFactory(
      name,
      codec,
      mergedConfig.retentionMs,
      mergedConfig.persist,
      mergedConfig.persistThresholdMs
    );
    const metrics = new TopicMetricsCollector();
    const clientManager = new ClientManager(
      mergedConfig.consumerInactivityThresholdMs,
      mergedConfig.consumerProcessingTimeThresholdMs,
      mergedConfig.consumerPendingThresholdMs
    );
    const queueManager = new QueueManager(this.queueFactory);
    const dlqManager = new DLQManager<Data>(name, messageStore, logger);
    const deliveryTracker = new DeliveryTracker(messageStore, metrics);

    const pendingAcks = new AckRegistry(
      clientManager,
      mergedConfig.consumerPendingThresholdMs
    );
    const subscriptionManager = new SubscriptionManager<Data>(
      clientManager,
      queueManager,
      messageStore,
      pendingAcks,
      deliveryTracker,
      logger
    );

    const messageRouter = new MessageRouter<Data>(
      clientManager,
      queueManager,
      subscriptionManager,
      dlqManager
    );
    const delayedManager = new DelayedMessageManager<Data>(
      new TimeoutScheduler<number>(new this.queueFactory()),
      messageStore,
      messageRouter,
      deliveryTracker,
      logger
    );
    const pipeline = new PipelineFactory<Data>().create(
      dlqManager,
      delayedManager,
      mergedConfig?.maxDeliveryAttempts
    );
    const ackMonitor = new AckMonitor(pendingAcks, mergedConfig?.ackTimeoutMs);

    // Build services
    const publishingService = new PublishingService(
      messageStore,
      pipeline,
      messageRouter,
      deliveryTracker,
      clientManager,
      delayedManager,
      metrics,
      logger
    );
    const consumptionService = new ConsumptionService(
      queueManager,
      messageStore,
      pendingAcks,
      deliveryTracker,
      clientManager,
      logger
    );
    const ackService = new AckService(
      pendingAcks,
      deliveryTracker,
      ackMonitor,
      messageStore,
      pipeline,
      queueManager,
      subscriptionManager,
      logger
    );
    const subscriptionService = new SubscriptionService<Data>(
      name,
      subscriptionManager,
      logger
    );
    const dlqService = new DLQService(dlqManager, clientManager, logger);
    const producerFactory = new ProducerFactory(
      name,
      publishingService,
      this.schemaRegistry,
      () => metrics.getMetrics().totalBytes,
      codec,
      mergedConfig.schema,
      mergedConfig.maxMessageSize,
      mergedConfig.maxSizeBytes
    );
    const clientManagementService = new ClientManagementService(
      producerFactory,
      clientManager,
      messageRouter,
      queueManager,
      consumptionService,
      ackService,
      subscriptionService,
      dlqService,
      logger
    );

    return new Topic<Data>(
      name,
      publishingService,
      consumptionService,
      subscriptionService,
      ackService,
      dlqService,
      clientManagementService,
      metrics
    );
  }

  private validateTopicName(name: string): void {
    if (!name || !/^[a-zA-Z0-9_-]+$/.test(name)) {
      throw new Error(
        "Invalid topic name. Use alphanumeric, underscore and hyphen characters."
      );
    }
  }
}

interface IMetricsCollector {
  recordEnqueue(byteSize: number, latencyMs: number): void;
  recordDequeue(latencyMs: number): void;
  getMetrics(): Record<ITopicMetricType, number>;
}
class TopicMetricsCollector implements IMetricsCollector {
  private metrics: IPersistedMap<ITopicMetricType, number>;

  constructor(mapFactory: IPersistedMapFactory) {
    this.metrics = mapFactory.create<ITopicMetricType, number>("metrics");
  }

  recordEnqueue(byteSize: number, latencyMs: number): void {
    const totalMessages = this.metrics.get("totalMessagesPublished") ?? 0;
    this.metrics.set("totalMessagesPublished", totalMessages + 1);

    // Message Sampling: track an only subset of messages to avoid overwhelming metrics collector
    if (totalMessages % 100 === 0) {
      const totalBytes = this.metrics.get("totalBytes") ?? 0;
      const depth = this.metrics.get("depth") ?? 0;
      const enqueueRate = this.metrics.get("enqueueRate") ?? 0;

      this.metrics.set("totalBytes", totalBytes + byteSize);
      this.metrics.set("depth", depth + 1);
      this.metrics.set("enqueueRate", enqueueRate + 1);
      this.updateAvgLatency(latencyMs);
    }
  }

  recordDequeue(latencyMs: number): void {
    const totalMessages = this.metrics.get("totalMessagesPublished") ?? 0;
    if (totalMessages % 100 !== 0) return;
    const depth = this.metrics.get("depth") ?? 0;
    const dequeueRate = this.metrics.get("dequeueRate") ?? 0;
    this.metrics.set("depth", depth - 1);
    this.metrics.set("dequeueRate", dequeueRate + 1);
    this.updateAvgLatency(latencyMs);
  }

  private updateAvgLatency(latencyMs: number) {
    const avgLatencyMs = this.metrics.get("avgLatencyMs") ?? 0;
    this.metrics.set("avgLatencyMs", avgLatencyMs * 0.9 + latencyMs * 0.1); // Exponential moving average
  }

  getMetrics() {
    return Object.fromEntries(this.metrics.entries()) as Record<
      ITopicMetricType,
      number
    >;
  }
}
//
//
//
// schema_registry





// interface IValidatorCache {
//   getValidator(
//     schemaId: string,
//     schemaDef: JSONSchemaType<any>
//   ): ISchemaValidator;
//   removeValidator(schemaId: string): void;
//   clear(): void;
// }
class ValidatorCache implements IValidatorCache {
  private validatorCache = new Map<string, ISchemaValidator>();
  private ajv: Ajv;

  constructor(options?: AjvOptions) {
    this.ajv = new Ajv({
      allErrors: true,
      coerceTypes: false,
      useDefaults: true,
      code: { optimize: true, esm: true },
      ...options,
    });
  }

  getValidator(
    schemaId: string,
    schemaDef: JSONSchemaType<any>
  ): ISchemaValidator {
    const cached = this.validatorCache.get(schemaId);
    if (cached) return cached;

    const validator = this.ajv.compile(schemaDef);
    this.validatorCache.set(schemaId, validator);
    return validator;
  }

  removeValidator(schemaId: string): void {
    this.validatorCache.delete(schemaId);
  }

  clear(): void {
    this.validatorCache.clear();
  }
}

// interface ISchemaDefVersionManager {
//   register<T>(
//     name: string,
//     schemaDef: JSONSchemaType<T>,
//     author?: string
//   ): string;
//   findLatestSchemaDefKey(name: string): string | undefined;
// }
// class SchemaDefVersionManager implements ISchemaDefVersionManager {
//   constructor(
//     private store: ISchemaDefStore,
//     private compatibilityMode: "none" | "forward" | "backward" | "full"
//   ) {}

//   register<T>(
//     name: string,
//     schemaDef: JSONSchemaType<T>,
//     author?: string
//   ): string {
//     const latestKey = this.findLatestSchemaDefKey(name);

//     if (latestKey && this.compatibilityMode !== "none") {
//       const latestSchemaDef = this.store.get(latestKey)!;

//       if (!validateSchema(latestSchemaDef, schemaDef, this.compatibilityMode)) {
//         throw new Error(
//           `Schema definition ${name} is not compatible in ${this.compatibilityMode} mode`
//         );
//       }
//     }

//     const version = latestKey ? parseInt(latestKey.split(":")[1]) + 1 : 1;
//     const schemaId = `${name}:${version}`;
//     this.store.set(schemaId, schemaDef, author);
//     return schemaId;
//   }

//   findLatestSchemaDefKey(name: string): string | undefined {
//     const candidates = this.store
//       .list()
//       .reduce<{ key: string; version: number }[]>((acc, key) => {
//         if (!key.startsWith(name + ":")) return acc;
//         return acc.concat({ key, version: parseInt(key.split(":")[1]) });
//       }, [])
//       .sort((a, b) => b.version - a.version);

//     return candidates[0]?.key;
//   }
// }





// In Layered Arch Data Layer is rigid - knows concrete db impl
// Clean Arch uses the idea of Dependency Inversion to solve this.
// It says that the Domain Layer should not depend on the Data Layer.
// Instead, the Data Layer should depend on interfaces from the Domain Layer.
// Business logic no longer depends on how the data is received.
// It does not care which network or database library is used.

// is all about make domain contract-only and use di everywhere
// usecases is a specific domain workflow that prevents domain logic leak into controllers/UI by
// control Validating input data, Converting DTOs → Domain Objects, Handling errors consistently

// Domain objects (core_interfaces, entities)
// Application (usecase workflows(input_validation, dto=>domain_objects))
// Infra ()

// Testability: Mock IUserRepository easily in unit tests

// Swappable Infrastructure: Change databases without touching use cases

// // ✅ 1. Use Case (business rules only)
// @Injectable()
// export class RegisterUserUseCase {
//   constructor(
//     private userRepository: IUserRepository, // Interface
//     private notifier: INotifier             // Interface
//   ) {}

//   async execute(command: RegisterUserCommand) {
//     if (await this.userRepository.exists(command.email)) {
//       throw new DomainException('Email taken'); // Domain exception
//     }
//     const user = User.create(command.email, command.password);
//     await this.userRepository.save(user);
//     await this.notifier.notify('USER_REGISTERED', user);
//   }
// }

// // ✅ 2. Infrastructure Service (implements interface)
// @Injectable()
// export class UserRepository implements IUserRepository {
//   // MongoDB/TypeORM implementation
// }

// // ✅ 3. Adapter (NestJS controller)
// @Controller()
// export class UserController {
//   constructor(private registerUser: RegisterUserUseCase) {}

//   @Post('/register')
//   async register(@Body() body: RegisterUserDto) {
//     await this.registerUser.execute(body);
//   }
// }

// usecases knows how to delete

// The Layers (From Inner to Outer)
// - Entities: Enterprise-wide business objects (most stable)
// - Use Cases: Application-specific business rules
// - Interface Adapters: Convert data between use cases and external agencies
// - Frameworks & Drivers: UI, databases, frameworks (most volatile)

// codec now can throw => dlq "processing_error"
// + mesage sampling, circuet broker, Message Throttling, Message Encryption
// update topic config
// refactor
// Message Indexing / Tagging /Searching

// make all deletable/disposable
// separation of conserns, ioc(inversify_js), visualize components and flows
// project structure (save snapshot before)

// class LagAwareCircuitBreaker extends CircuitBreaker {
//   private lagThresholdMs = 30_000;

//   recordActivity(clientId: number, activity: Partial<IClientState>) {
//     super.recordActivity(clientId, activity);
//     const state = this.getClient(clientId);
//     if (state && Date.now() - state.lastActiveAt > this.lagThresholdMs) {
//       this.open();
//     }
//   }
// }
// class LagMonitor {
//   constructor(private topic: ITopic<any>) {}
//   async checkLag(consumerId: number) {
//     const pending = this.topic.getMetadata().pendingAcks.count;
//     const lastAck = this.topic.getMetadata().lastAckTime;
//     const lag = Date.now() - lastAck;
//     this.logger.log(`Consumer ${consumerId} has ${pending} pending messages`);
//     if (lag > this.lagThresholdMs) {
//       this.circuitBreaker.openCircuit(consumerId);
//     }
//   }
// }
// recordActivity(clientId: number, activityRecord: Partial<IClientState>) {
//   const client = this.clients.get(clientId);
//   if (!client) return;

//   const now = Date.now();
//   const windowMs = 60_000;
//   client.messageCount++;
//   client.activityLog ??= [];
//   client.activityLog.push(now);

//   // Clean up old entries
//   client.activityLog = client.activityLog.filter(
//     (ts) => now - ts < windowMs
//   );

//   if (client.messageCount > this.maxMessagesPerMinute) {
//     throw new Error("Rate limit exceeded");
//   }

//   // Proceed with update
//   for (let key in activityRecord) {
//     if (typeof client[key] === "number") {
//       client[key] += activityRecord[key];
//     } else {
//       client[key] = activityRecord[key];
//     }
//   }
// }

// class CircuitBreaker {
//   constructor(

//     private failureThreshold = 5;
//   private resetTimeoutMs = 30_000;
//   ) {

//   }

//   private failures = new Map<number, number>();
//   private states = new Map<
//     number,
//     "closed" | "open" | "half_open"
//   >();

//   register(consumerId: number) {
//     this.states.set(consumerId, "closed");
//   }

//   markFailure(consumerId: number): void {
//     const count = (this.failures.get(consumerId) ?? 0) + 1;
//     this.failures.set(consumerId, count);

//     if (count >= this.failureThreshold) {
//       this.openCircuit(consumerId);
//     }
//   }

//   markSuccess(consumerId: number): void {
//     this.failures.delete(consumerId);
//     this.halfOpenCircuit(consumerId);
//   }

//   isOpen(consumerId: number): boolean {
//     return this.states.get(consumerId) === "open";
//   }

//   private openCircuit(consumerId: number) {
//     this.states.set(consumerId, "open");
//     setTimeout(() => this.halfOpenCircuit(consumerId), this.resetTimeoutMs);
//   }

//   private halfOpenCircuit(consumerId: number) {
//     this.states.set(consumerId, "half_open");
//     setTimeout(() => this.closeCircuit(consumerId), 10_000);
//   }

//   private closeCircuit(consumerId: number) {
//     this.states.set(consumerId, "closed");
//     this.failures.delete(consumerId);
//   }
// }

// LATER
// 0. Hierarchical Topics (Nested topics like logs.debug, orders.payment, events.user.signup for Fine-grained routing, Subscription wildcards)
// interface ITopicConfig {
//   parent?: string;
// }
// class HierarchicalTopicRouter implements IMessageRouter {
//   private topicTree = new Map<string, Set<string>>();
//   constructor(private baseRouter: IMessageRouter) {}
//   route(meta: MessageMetadata): Promise<number> {
//     const { topic } = meta;
//     const segments = topic.split(".");
//     let matchFound = false;
//     let deliveryCount = 0;

//     for (let i = 0; i <= segments.length; i++) {
//       const subTopic = segments.slice(0, i).join(".");
//       const subscribers = this.topicTree.get(subTopic);
//       if (subscribers?.size) {
//         matchFound = true;
//         for (const subscriber of subscribers) {
//           deliveryCount += await this.baseRouter.route({
//             ...meta,
//             topic: subscriber,
//           });
//         }
//       }
//     }

//     if (!matchFound) {
//       return this.baseRouter.route(meta);
//     }

//     return Promise.resolve(deliveryCount);
//   }
// }
// 1. go to lmdb or redb, use transactional writes(exactly once feature)
// 2. full Exactly-Once: deduplication(dedupId) & Consumer-side idempotent processing are DONE, Transactional Writes(Ensure state changes happen atomically), Broker-side state coordination(Coordinate with external systems)
// 3. switch to Standalone server: you hit >50k msg/sec, cross-service/multi-lang support => need protobuf & lib/sdk per lang; for n-processes use proper-lockfile instead of custom Mutex, Add Admin Tooling CLI, dashboard
// 4. Message Sharding (Split a topic into multiple partitions/shards for Horizontal scaling, High throughput, Parallel processing)
// class ShardedMessageRouter implements IMessageRouter {
//   private shards = new Map<string, IMessageRouter>();
//   constructor(private numShards = 4) {}
//   addShard(name: string, router: IMessageRouter) {
//     this.shards.set(name, router);
//   }
//   async route(meta: MessageMetadata): Promise<number> {
//     const shardKey = meta.correlationId || meta.id.toString();
//     const shardIndex = hash(shardKey) % this.numShards;
//     const shardName = `shard:${shardIndex}`;
//     const shardRouter = this.shards.get(shardName);
//     if (!shardRouter) return 0;
//     return shardRouter.route(meta);
//   }
// }
// 5. message archiving
// class MessageArchiver<Data> {
//   constructor(
//     private messageStorage: IMessageStorage<Data>,
//     private archiveStorage: IMessageStorage<Data>,
//     private archivalThresholdMs: number
//   ) {}

//   async archiveOldMessages() {
//     let count = 0;
//     for await (const [id, meta] of this.messageStorage.entries()) {
//       if (Date.now() - meta.ts > this.archivalThresholdMs) {
//         await this.archiveStorage.writeAll(message, meta);
//         this.messageStorage.remove(id);
//         count++;
//       }
//     }
//     return count;
//   }
// }
// 6. EVENT SOURCING FEATURES
// NO - Message Forwarding (Send a message to multiple topics or brokers)
// MAYBE - Message Transformation (Modify message content before delivery)
// NO - Message Reordering / Stream Processing (Support for time-based ordering and late arrival handling)
// YES(already use wal) - Message Mirroring (Send messages to multiple destinations on publishing for DR (Disaster Recovery), Multi-region replication, Backup purposes)

//

//
// const logger = pino({
//   level: process.env.LOG_LEVEL || "info",
//   transport:
//     process.env.NODE_ENV === "development"
//       ? { target: "pino-pretty" }
//       : undefined,
//   base: { service: "message-broker" },
// });

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

//           - leveldb: 300k (space:78mb, w:18s, r:2.8s, d:ok)
//           - rocksd: 300k (space:100mb, w:15.3s, r:2.9s, d:faster, +ttl, +backups, +mcore, +transaction?, partitions(Column Families allow to group related data together and store it in a separate partition))
//           - Redis(aof) use more disk space, partialy already has broker capabilities(pubsub, streams) which also in ram so i double ram usage, has many other features which i dont need
//             redis list[buffer from protobuf] for queues and redis stream for broadcast(eventlog)

// Job queues and eventLog have distinct arch and seems like this lib should be doing one thing eventually.
// I think i dont want kafka streaming. I just want a scallable message broker which is more like Rabbitmq i guess.
// RabbitMq analogue: log is an exchange, offsets are binary queues(with messageId if log is map or messageIndex if log is list) binded to exchange but ram effective
// message marks will be deleted from the offsets on ack, if nack - also offset manipulation
// but we need both routing and consistency hashing (correlationId) implementation here
// Here is my proposal for universal Topic:
// 1. all messages should be stored in kv log (map/leveldb) and deleted only with retention. Can we skip wal if log persisted?
// 2. we will use virtual offsets per consumer but it will be buffer with messageid(or messageIndex if log is list)
// on message ack only offset will be deleted, on nack also offset manipulation - message already in log

//        1. AJV for per topic json schema validation (2x faster zod due precompilation and faster than protobuf validation)
//        2. Custom binary packing codec based on Buffer, fixed structure(metadata), bitflags for optional fields (no redundant data), precomputed Offsets.
//           Buffer.from is 30% faster TextEncode.encode due the buffer preallocation.
//           Uses worker_threads(makes 2x slower but offload main thread).
//           Codec provides max speed, min size/resource_usage within nodejs specific impl.
//           But for a standalone server its need to be switch to protobuf(encode + compress(indexes vs keys) + schema validation) or message_pack(encode any type + compress)
//           Metrics
//             - json: encode(12k m/sec), decode(9k m/sec), size(200b), gc/cpu(high)
//             - this impl: encode(40k m/sec), decode(50k m/sec), size(60b), gc/cpu(low)
//             - protobuf: encode(30k m/sec), decode(35k m/sec), size(80b), gc/cpu(medium)
//             - message_pack: encode(16k m/sec), decode(15k m/sec), size(100b), gc/cpu(medium)
//        3. leveldb(+snappy)/rocksdb cpp addon
//           - compressor comparision: gzip(zlib wrapper, 4x but slow)/brotli(+20% zlib, slow) => ZSTD(between gzip & snappy) => snappy(only 2x but fast)

// ############################################## INTERNAL PARTITIONS #################################################################
// If you want even more throughput, partition each topic internally like Kafka-style partitions:
// Each partition gets its own queue, DLQ, delayed queue, etc. Consumers can subscribe to one or all partitions.
// You can route messages across partitions using consistent hashing or round-robin.

// ### 1. **Reduce Lock Contention**
// If all messages go into a single queue, every producer and consumer must coordinate access to that shared structure. Even in a single thread, this leads to:

// - Sequential enqueue/dequeue operations
// - Potential bottlenecks in high-throughput scenarios
// - Poor pipelining of async operations

// ### 2. **Granular Consumer Scaling**
// Partitioning allows consumers to scale independently per partition. For example:

// - One slow consumer only affects one partition.
// - Fast consumers can be assigned more partitions.
// - Load balancing becomes easier via consistent hashing or round-robin strategies.

// Even within a single thread, this enables better **pipelined execution** of async tasks like I/O, timers, and network calls.

// ### 3. **Improved Message Ordering Guarantees**
// If your system needs strict ordering **per key** (e.g., per user or per session), partitioning by key ensures:

// - All messages for a given key go to the same partition
// - Consumers process them in order within that partition
// - No global lock required

// Without partitioning, you'd need expensive synchronization mechanisms to preserve order globally.

// ### 4. **Better Utilization of I/O-Bound Workloads**
// Node.js excels at handling **I/O-bound** workloads (network, disk, timers). Partitions allow overlapping:

// - Multiple producers writing to different partitions
// - Consumers reading from different partitions concurrently (via event loop)
// - Internal DLQs, delayed queues, etc., operating independently

// This maximizes throughput under asynchronous I/O pressure.

// ### 5. **Future-Proofing for Multi-Threading**
// Even if you start with a single-threaded model, designing with partitions in mind makes it easy to later:

// - Move each partition to a separate `worker_thread`
// - Distribute partitions across machines
// - Implement sharding patterns similar to Kafka

// ### Step 1: Add Partition Count to Topic Config
// ```ts
// interface ITopicConfig<Data> {
//   schema?: JSONSchemaType<Data>;
//   persist?: boolean;
//   retentionMs?: number;
//   maxDeliveryAttempts?: number;
//   maxMessageSize?: number;
//   partitions?: number; // New field
// }
// ```

// ### Step 2: Create Partitioned Queues in `TopicQueueManager`
// ```ts
// class TopicQueueManager {
//   private partitionQueues: IPriorityQueue[];
//   private routingStrategy: IRoutingStrategy;

//   constructor(
//     routingStrategy: IRoutingStrategy,
//     private queueFactory: () => IPriorityQueue<Buffer>,
//     private partitionCount: number
//   ) {
//     this.partitionQueues = Array.from({ length: partitionCount }, queueFactory);
//     this.routingStrategy = routingStrategy;
//   }

//   getQueue(key?: string): IPriorityQueue {
//     if (!key) return this.partitionQueues[0]; // Fallback

//     const hash = this.routingStrategy.getConsumerId(key); // or custom partitioner
//     const partitionIndex = Math.abs(hash!) % this.partitionCount;
//     return this.partitionQueues[partitionIndex];
//   }
// }
// ```

// ### Step 3: Modify `Topic` Class to Accept Partitions
// ```ts
// class Topic<Data> {
//   private readonly queues: TopicQueueManager;

//   constructor(
//     public name: string,
//     private context: IContext,
//     routingStrategy: IRoutingStrategy,
//     private config?: ITopicConfig<Data>
//   ) {
//     const partitionCount = config?.partitions || 1;
//     this.queues = new TopicQueueManager(routingStrategy, context.queueFactory, partitionCount);
//   }
// }
// ```

// ---

// ## 🧱 Step 2: Add Internal Topic Partitioning

// You already have a `TopicQueueManager`. We'll extend it to support internal partitioning.

// ### 🔧 Update `ITopicConfig`
// ```ts
// // topics/types.ts
// interface ITopicConfig<Data> {
//   schema?: JSONSchemaType<Data>;
//   persist?: boolean;
//   retentionMs?: number;
//   archivalThreshold?: number;
//   maxSizeBytes?: number;
//   maxDeliveryAttempts?: number;
//   maxMessageSize?: number;
//   partitions?: number; // add this
// }
// ```

// ### 🔧 Modify `TopicQueueManager` to Support Partitions
// ```ts
// // topics/queue-manager.ts
// class TopicQueueManager {
//   private sharedQueue: IPriorityQueue;
//   private unicastQueues = new Map<number, IPriorityQueue>();
//   private partitionQueues: IPriorityQueue[];

//   constructor(
//     private routingStrategy: IRoutingStrategy,
//     private queueFactory: () => IPriorityQueue<Buffer>,
//     private partitionCount: number = 1
//   ) {
//     this.sharedQueue = this.queueFactory();
//     this.partitionQueues = Array.from({ length: partitionCount }, queueFactory);
//   }

//   getPartitionIndex(key: string): number {
//     const hash = this.routingStrategy.getConsumerId(key); // or custom hasher
//     return Math.abs(hash!) % this.partitionQueues.length;
//   }

//   getQueue(key?: string): IPriorityQueue {
//     if (key) {
//       const targetConsumer = this.routingStrategy.getConsumerId(key);
//       const unicastQueue = this.unicastQueues.get(targetConsumer);
//       if (unicastQueue) return unicastQueue;

//       const partitionIdx = this.getPartitionIndex(key);
//       return this.partitionQueues[partitionIdx];
//     }
//     return this.sharedQueue;
//   }

//   addConsumerQueue(consumerId: number): void {
//     this.unicastQueues.set(consumerId, this.queueFactory());
//   }

//   removeConsumerQueue(consumerId: number): void {
//     this.unicastQueues.delete(consumerId);
//   }

//   enqueue(record: Buffer, meta: Metadata): void {
//     const queue = this.getQueue(meta.correlationId);
//     queue.enqueue(record, meta.priority);
//   }

//   dequeue(consumerId: number): Buffer | undefined {
//     const queue = this.unicastQueues.get(consumerId);
//     if (queue?.size()) return queue.dequeue();
//     return this.sharedQueue.dequeue();
//   }
// }
// ```

// ### ✅ Update `Topic` Constructor
// ```ts
// // topics/topic.ts
// constructor(
//   public name: string,
//   private context: IContext,
//   routingStrategy: IRoutingStrategy,
//   private config?: ITopicConfig<Data>
// ) {
//   const partitionCount = config?.partitions || 1;
//   this.queues = new TopicQueueManager(routingStrategy, context.queueFactory, partitionCount);
//   ...
// }
// ```

// ## 🚫 So Why Do We Even Have Kafka-Style Partitions?

// Kafka-style partitions exist because:
// - Topics are high-throughput. It supports **millions of messages/sec**
// - Consumers don't care about strict ordering per key
// - Partitions allow horizontal scaling within a single topic
// - Producers write to any partition
// - Consumers read from one or more partitions

// But these features come at the cost of complexity:
// - Rebalancing logic
// - Offset management
// - Partition assignment strategies

// But in your case:
// - You **do** care about ordering per key
// - You’re not trying to maximize throughput at all costs
// - You prefer simplicity over scale-at-all-costs

// So:
// ❌ If you don’t need that level of scale, they’re overkill.

// ############################################## LOG BASED QUEUE #################################################################

// class OffsetTracker {
//   constructor(private storage: IStorage, private topicName: string) {}

//   async getOffset(consumerId: number): Promise<number> {
//     const key = `offset:${this.topicName}:${consumerId}`;
//     const buffer = await this.storage.get(this.topicName, key);
//     return buffer ? buffer.readUInt32BE(0) : 0;
//   }

//   async commitOffset(consumerId: number, offset: number): Promise<void> {
//     const key = `offset:${this.topicName}:${consumerId}`;
//     const buffer = Buffer.alloc(4);
//     buffer.writeUInt32BE(offset, 0);
//     await this.storage.put(this.topicName, key, buffer);
//   }
// }

// class Topic<Data> {
//   private readonly offsetTracker: OffsetTracker;

//   constructor(
//     public name: string,
//     private context: IContext,
//     routingStrategy: IRoutingStrategy,
//     private config?: ITopicConfig<Data>
//   ) {
//     this.offsetTracker = new OffsetTracker(context.storage, name);
//     // ...
//   }

//   async send(record: Buffer, meta: Metadata): Promise<void> {
//     if (await this.pipeline.process(record, meta)) return;

//     // Write once to shared log
//     const offset = await this.getNextOffset();
//     await this.context.storage.put(this.name, `msg:${offset}`, record);

//     this.metrics.recordMessagePublished(meta);
//   }

//   private async getNextOffset(): Promise<number> {
//     const key = `__topic_offset__:${this.name}`;
//     let current = 0;
//     try {
//       const buffer = await this.context.storage.get(this.name, key);
//       current = buffer ? buffer.readUInt32BE(0) : 0;
//     } catch {
//       /* ignore */
//     }
//     const next = current + 1;
//     const buffer = Buffer.alloc(4);
//     buffer.writeUInt32BE(next, 0);
//     await this.context.storage.put(this.name, key, buffer);
//     return next;
//   }

//   async consume(consumerId: number): Promise<[Data, Metadata] | undefined> {
//     const offset = await this.offsetTracker.getOffset(consumerId);
//     const key = `msg:${offset}`;
//     const record = await this.context.storage.get(this.name, key);
//     if (!record) return;

//     const message = this.context.codec.decode<Data>(record);
//     if (await this.pipeline.process(record, message[1])) return;

//     // Advance offset only after successful processing
//     await this.offsetTracker.commitOffset(consumerId, offset + 1);

//     return message;
//   }
// }
// ```

// ### USER
// now it is make sense, but for every consume we need to scan offset-next messages with decoding all including those we will skip. Is it similar to consumer groups? cpu greedy consumption just for not duplicate messages per consumer (ram/disk optimization). Is it worth? Another approach: store log as map and use queues per consumer which store only messageId. Put message in log and fan out id to related consumer queues. Which approach is better for u?
// ### USER
// i like the log-scan approach, its smart and we can make codec decode only one metadata field (tags in this case) due to preposition. but in scale i think its may be not so fast like messageId fanout approach, especially in edge cases. also in second approach we can use buffer as queue because messageId is a number. Why kafka doesnt use this?
// ### USER
// Job queues and eventLog have distinct arch and seems like this lib should be doing one thing eventually.
// I think i dont want kafka streaming. I just want a scallable message broker which is more like Rabbitmq i guess.
// RabbitMq analogue: log is an exchange, offsets are binary queues(with messageId if log is map or messageIndex if log is list) binded to exchange but ram effective
// Message marks will be deleted from the offsets on ack, if nack - also offset manipulation
// But we need both routing and consistency hashing (correlationId) implementation here
// What do u think?

// ############################################## ROAD TO SERVER #################################################################

// ### USER
// If it goes to the standalone nodejs server how it will be scalled?

// ## 🔍 TL;DR: Scaling Strategy for Your Standalone Broker

// | Aspect | Strategy |
// |-------|----------|
// | Horizontal Scale | Use topic partitioning + consumer groups |
// | Fault Tolerance | Add replication & WAL failover |
// | Cross-Language Support | Expose gRPC or HTTP streaming API |
// | Scalable Consumers | Use offset tracking + consumer groups |
// | High Throughput | Offload CPU work (codec) to worker pool |
// | Persistence | Keep LevelDB/WAL + optional Raft/RocksDB later |
// | Cluster Coordination | Optional – use Redis, etcd, or Raft |

// ---

// ## ✅ Step-by-Step: From Embedded Lib to Standalone Broker

// ### 1. 📦 Package Structure

// Split the codebase into microservices-style layers:

// ```
// broker-server/
// ├── core/               # Core logic (same as lib)
// ├── api/                # REST/gRPC interface
// ├── persistence/        # LevelDB/WAL/RocksDB
// ├── cluster/            # Optional coordination layer
// ├── cli/                # CLI tool for management
// └── main.ts             # Entry point
// ```

// ### 3. 🧩 Define External Interface (gRPC Example)

// ```protobuf
// // proto/broker.proto
// syntax = "proto3";

// package broker;

// service TopicService {
//   rpc Produce(stream ProducerRequest) returns (ProducerResponse);
//   rpc Consume(ConsumerRequest) returns (stream ConsumerResponse);
//   rpc Ack(AckRequest) returns (AckResponse);
//   rpc ReplayDlq(DlqReplayRequest) returns (DlqReplayResponse);
// }

// message ProducerRequest {
//   string topic = 1;
//   bytes data = 2;
//   map<string, string> metadata = 3;
// }

// message ProducerResponse {
//   repeated int64 ids = 1;
// }

// message ConsumerRequest {
//   string topic = 1;
//   int32 consumer_id = 2;
//   int32 limit = 3;
//   bool auto_ack = 4;
// }

// message ConsumerResponse {
//   int64 id = 1;
//   bytes data = 2;
//   map<string, string> metadata = 3;
// }

// message AckRequest {
//   string topic = 1;
//   repeated int64 ids = 2;
// }
// ```

// ---

// ### 4. ⚙️ Scaling Internally in Node.js

// Once it’s a server, you can scale using Node.js-native tools:

// #### ✅ A. Multi-Core Parallelism with `cluster.fork()`

// Use Node.js’ `cluster` module to run each topic on a separate process.

// ```ts
// import cluster from 'cluster';
// import os from 'os';

// if (cluster.isPrimary) {
//   const numCPUs = os.cpus().length;
//   for (let i = 0; i < numCPUs; i++) {
//     cluster.fork();
//   }
// } else {
//   // Worker process runs the actual broker
//   startBroker();
// }
// ```

// Each topic can be assigned to a thread via consistent hashing of topic name.

// ### 9. 🪶 Lightweight Architecture Benefits

// Even though it's a single server, you can still scale elegantly:

// | Feature | Strategy |
// |--------|----------|
// | Single-threaded limits | Move to `cluster` or `worker_threads` |
// | Consumer concurrency | Assign N consumers per topic |
// | Message order guarantees | Sticky routing via key hash |
// | Fan-out efficiency | Broadcast log + virtual offsets |
// | Backpressure | Rate limiting + buffer control |
// | Debugging | Metrics + tracing via OpenTelemetry |

// ---

// ### 1. **Isolate High-Traffic Topics into Dedicated Workers**

// You can create a `TopicWorkerPool` that assigns certain topics to dedicated threads:

// ```ts
// class TopicWorkerPool {
//   private workers = new Map<string, Worker>();
//   constructor(private topicNames: string[]) {
//     topicNames.forEach((name) => {
//       this.workers.set(name, new Worker(`./topic-worker.js`, { env: { TOPIC: name } }));
//     });
//   }

//   get(topicName: string): Worker {
//     return this.workers.get(topicName)!;
//   }
// }
// ```

// Each worker handles its own queue, consumer routing, and persistence.
