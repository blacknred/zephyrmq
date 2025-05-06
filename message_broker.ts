import crypto from "node:crypto";
import { LinkedListPriorityQueue } from "../queues";
import { Logger, wait } from "./utils";

// interfaces.ts
interface ILogger {
  log(message: string, level: "INFO" | "WARN" | "DANGER"): void;
}
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
  private static iterator = 1;
  public metadata: IMessageMetadata;
  constructor(
    public data: Data,
    metadata: Omit<IMessageMetadata, "timestamp" | "attempts" | "id">
  ) {
    this.metadata = {
      ...metadata,
      timestamp: Date.now(),
      attempts: 1,
      id: `${Message.iterator++}`,
    };
  }
}
// MessageSerializer.ts
class MessageSerializer {
  static encode<Data>(message: Message<Data>) {
    try {
      return new TextEncoder().encode(JSON.stringify(message));
    } catch (e) {
      throw new Error("Failed to encode message");
    }
  }

  static decode<Data>(bytes: Uint8Array) {
    try {
      const { data, metadata } = JSON.parse(new TextDecoder().decode(bytes));
      return new Message<Data>(data, metadata);
    } catch (e) {
      throw new Error("Failed to decode message");
    }
  }
}
// MessageValidator.ts
class MessageValidator<Data> {
  constructor(private maxMessageSize: number) {}

  validate(message: { record: Uint8Array; data: Data }): void {
    if (message.record.length > this.maxMessageSize) {
      throw new Error(`Message exceeds ${this.maxMessageSize} bytes`);
    }
    // Add schema validation (e.g., using Zod)
  }
}
// MessageFactory.ts
class MessageFactory<Data> {
  create(
    data: Data[],
    metadata: Partial<IMessageMetadata> & { topic: string }
  ): Array<{ message: Message<Data>; record: Uint8Array }> {
    return data.map((item, index) => {
      const message = new Message(item, {
        ...metadata,
        ...(data.length > 1 && {
          batchId: `batch-${Date.now()}`,
          batchIndex: index,
          batchSize: data.length,
        }),
      });
      return {
        message,
        record: MessageSerializer.encode(message),
      };
    });
  }
}
//
//
//
// Topic.ts
class Topic {
  unicastQueues: Map<string, IPriorityQueue<Uint8Array>> = new Map();
  constructor(
    public name: string,
    public broadcastQueue: IPriorityQueue<Uint8Array>,
    public hasher: IConsistencyHasher
  ) {}
}
// TopicRegistry.ts (need sharding)
class TopicRegistry {
  private topics = new Map<string, Topic>();

  constructor(
    private queueFactory: () => IPriorityQueue<Uint8Array>,
    private hasherFactory: () => IConsistencyHasher
  ) {}

  create(name: string): Topic {
    if (this.topics.has(name)) {
      throw new Error("Topic already exists");
    }

    if (!name || !/^[a-zA-Z0-9_-]+$/.test(name)) {
      throw new Error("Invalid topic name");
    }

    this.topics.set(
      name,
      new Topic(name, this.queueFactory(), this.hasherFactory())
    );

    return this.topics.get(name)!;
  }

  list() {
    return this.topics.keys();
  }

  get(name: string): Topic | undefined {
    return this.topics.get(name);
  }

  getOrThrow(name: string) {
    if (!this.topics.has(name)) throw new Error("Topic not found");
    return this.topics.get(name);
  }

  delete(name: string): void {
    if (!this.topics.has(name)) {
      throw new Error("Topic not found");
    }
    this.topics.delete(name);
  }
}
// ClientManager.ts
class ClientManager {
  constructor(
    private topicRegistry: TopicRegistry,
    private queueFactory: () => IPriorityQueue<Uint8Array>
  ) {}

  registerConsumer(topicName: string, consumerId: string): Topic {
    const topic = this.topicRegistry.getOrThrow(topicName);
    topic?.unicastQueues.set(consumerId, this.queueFactory());
    topic?.hasher.addNode(consumerId);
    return topic!;
  }

  unregisterConsumer(topicName: string, consumerId: string): void {
    const topic = this.topicRegistry.getOrThrow(topicName);
    topic?.unicastQueues.delete(consumerId);
    topic?.hasher.removeNode(consumerId);
  }

  registerProducer(topicName: string): Topic {
    return this.topicRegistry.getOrThrow(topicName)!;
  }
}
// DelayedQueueManager.ts
interface IDelayedQueueManager {
  setRouter(router: IMessageRouter): void;
  addMessage(record: Uint8Array, ttd: number): void;
}
class DelayedQueueManager implements IDelayedQueueManager {
  private nextTimeout?: number;
  private router?: IMessageRouter;

  constructor(private delayedQueue: IPriorityQueue<Uint8Array>) {}

  setRouter(router: IMessageRouter): void {
    this.router = router;
  }

  addMessage(record: Uint8Array, ttd: number): void {
    this.delayedQueue.enqueue(record, ttd);
    this.scheduleDelivery();
  }

  private scheduleDelivery(): void {
    if (this.nextTimeout) clearTimeout(this.nextTimeout);
    if (this.delayedQueue.isEmpty()) return;

    const record = this.delayedQueue.peek()!;
    const message = MessageSerializer.decode(record);
    const { ttd, timestamp } = message.metadata;
    const delay = Math.max(0, timestamp + ttd! - Date.now());

    this.nextTimeout = setTimeout(() => {
      this.processQueue();
    }, delay);
  }

  private processQueue(): void {
    if (!this.router) throw new Error("Router not set");

    while (!this.delayedQueue.isEmpty()) {
      const record = this.delayedQueue.dequeue()!;
      const message = MessageSerializer.decode(record);
      this.router.route(record, message.metadata);
      // this.logger?.log(`[Delivered] ${message.metadata.id}`, "INFO");
    }

    this.scheduleDelivery();
  }
}
// MessageRouter.ts
interface IMessageRouter {
  route(record: Uint8Array, metadata: IMessageMetadata): void;
}
class MessageRouter implements IMessageRouter {
  constructor(
    private topicRegistry: TopicRegistry,
    private delayedQueueManager: IDelayedQueueManager,
    private dlQueue: IPriorityQueue<Uint8Array>,
    private maxDeliveryAttempts: number
  ) {}

  route(record: Uint8Array, metadata: IMessageMetadata): string {
    if (this._shouldRouteToDLQ(metadata)) {
      this.dlQueue.enqueue(record);
      return "DLQ";
    }

    if (this._shouldDelay(metadata)) {
      this.delayedQueueManager.addMessage(record, metadata.ttd!);
      return "DELAYED";
    }

    return this._routeToTopic(record, metadata);
  }

  private _shouldRouteToDLQ(metadata: IMessageMetadata): boolean {
    const { attempts, ttl, timestamp } = metadata;
    if (attempts > this.maxDeliveryAttempts) return true;
    if (ttl && timestamp + ttl <= Date.now()) return true;
    return false;
  }

  private _shouldDelay({ ttd }: IMessageMetadata): boolean {
    return !!ttd && ttd > Date.now();
  }

  private _routeToTopic(
    record: Uint8Array,
    metadata: IMessageMetadata
  ): string {
    const { topic: topicName, correlationId, priority } = metadata;
    const topic = this.topicRegistry.get(topicName);
    if (!topic) {
      this.dlQueue.enqueue(record);
      return "DLQ";
    }

    if (correlationId) {
      const consumerId = topic.hasher.getNode(correlationId)!;
      topic.unicastQueues.get(consumerId)?.enqueue(record, priority);
    } else {
      topic.broadcastQueue.enqueue(record, priority);
    }

    return topicName;
  }
}
// TopicBroker.ts (Facade)
interface ITopicBrokerConfig {
  maxDeliveryAttempts: number;
  maxMessageSize: number;
  defaultPollingInterval?: number;
  // replicas: number;
}
class TopicBroker {
  public readonly dlQueue: IPriorityQueue<Uint8Array>;
  public router: MessageRouter;

  constructor(
    public config: ITopicBrokerConfig,
    private queueFactory: () => IPriorityQueue<Uint8Array>,
    private hasherFactory: () => IConsistencyHasher,
    private topicRegistry = new TopicRegistry(
      this.queueFactory,
      this.hasherFactory
    ),
    private clientManager = new ClientManager(
      this.topicRegistry,
      this.queueFactory
    ),
    private delayedQueueManager = new DelayedQueueManager(this.queueFactory()),
    public logger?: Logger
  ) {
    this.dlQueue = this.queueFactory();

    this.router = new MessageRouter(
      this.topicRegistry,
      this.delayedQueueManager,
      this.dlQueue,
      this.config.maxDeliveryAttempts
    );

    this.delayedQueueManager.setRouter(this.router);
  }

  createTopic(name: string): void {
    this.topicRegistry.create(name);
  }

  isTopicExist(name: string) {
    this.topicRegistry.getOrThrow(name);
  }

  deleteTopic(name: string) {
    return this.topicRegistry.delete(name);
  }

  registerConsumer(topicName: string, consumerId: string): Topic {
    return this.clientManager.registerConsumer(topicName, consumerId);
  }

  unregisterConsumer(topicName: string, consumerId: string) {
    return this.clientManager.unregisterConsumer(topicName, consumerId);
  }

  registerProducer(topicName: string, producerId: string) {
    return this.clientManager.registerProducer(topicName);
  }
}
//
//
//
// Producer.ts (Facade)
class Producer<Data> {
  private static iterator = 1;
  public readonly id: string;

  constructor(
    private broker: TopicBroker,
    private topicName: string,
    private messageFactory = new MessageFactory<Data>(),
    private validator = new MessageValidator(broker.config.maxMessageSize)
  ) {
    this.id = `producer-${Producer.iterator++}`;
    this.broker.registerProducer(topicName, this.id);
  }

  send(
    data: Data[],
    metadata: Pick<
      IMessageMetadata,
      "priority" | "correlationId" | "ttd" | "ttl"
    >
  ): number {
    const messages = this.messageFactory.create(data, {
      ...metadata,
      topic: this.topicName,
    });

    messages.forEach(({ message: { data, metadata }, record }) => {
      this.validator.validate({ record, data });
      const target = this.broker.router.route(record, metadata);

      this.broker.logger?.log(
        `[${this.id}] → ${target}: ${JSON.stringify(data)}`,
        target === "DLQ" ? "DANGER" : "INFO"
      );
    });

    return messages.length;
  }
}
//
//
//
// MessageFetcher.ts
class MessageFetcher<Data> {
  constructor(
    private consumerId: string,
    private topic: Topic,
    private limit: number,
    private ackManager: AckManager<Data>,
    private onMessageExpired: (
      record: Uint8Array,
      metadata: IMessageMetadata
    ) => void
  ) {}

  fetchMessages(options?: { signal?: AbortSignal }): Message<Data>[] {
    if (options?.signal?.aborted) return [];

    const messages: Message<Data>[] = [];
    const queues = [
      this.topic.unicastQueues.get(this.consumerId),
      this.topic.broadcastQueue,
    ];

    for (const queue of queues) {
      while (queue && !queue.isEmpty() && messages.length < this.limit) {
        const record = queue.dequeue()!;
        const message = MessageSerializer.decode<Data>(record);
        const { timestamp, ttl } = message.metadata;

        if (ttl && timestamp + ttl <= Date.now()) {
          this.onMessageExpired(record, message.metadata);
        } else {
          messages.push(message);
          this.ackManager.addToPending(message);
        }
      }
    }

    return messages;
  }
}
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

  ack(messageId?: string): void {
    if (messageId) this.pending.delete(messageId);
    else this.pending.clear();
  }
}
// SubscriptionManager.ts
class SubscriptionManager {
  private isActive = false;
  private abortController = new AbortController();

  constructor(
    private messageFetcher: MessageFetcher<any>,
    private interval = 100
  ) {}

  async subscribe(
    handler: (messages: Message[]) => Promise<void>
  ): Promise<void> {
    this.isActive = true;

    while (this.isActive) {
      try {
        const messages = this.messageFetcher.fetchMessages();
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
// Consumer.ts (Facade)
class Consumer<Data> {
  private static iterator = 1;
  public readonly id: string;
  private messageFetcher: MessageFetcher<Data>;
  private subscriptionManager: SubscriptionManager;

  constructor(
    private broker: TopicBroker,
    private ackManager = new AckManager<Data>(),
    private options: { topic: string; limit?: number; autoAck?: boolean }
  ) {
    this.id = `consumer-${Consumer.iterator++}`;
    const topic = this.broker.registerConsumer(options.topic, this.id);

    this.messageFetcher = new MessageFetcher(
      this.id,
      topic,
      options.limit ?? 1,
      this.ackManager,
      this.onMessageExpired
    );

    this.subscriptionManager = new SubscriptionManager(
      this.messageFetcher,
      this.broker.config.defaultPollingInterval
    );
  }

  private onMessageExpired(record: Uint8Array, metadata: IMessageMetadata) {
    const target = this.broker.router.route(record, {
      ...metadata,
      attempts: Infinity,
    });

    this.broker.logger?.log(
      `[${this.id}] → ${target}: ${JSON.stringify(
        MessageSerializer.decode(record)?.data
      )}`,
      target === "DLQ" ? "DANGER" : "INFO"
    );
  }

  consume(): Message<Data>[] {
    const messages = this.messageFetcher.fetchMessages();
    this.broker.logger?.log(
      `[${this.id}] <= ${this.options.topic}: ${JSON.stringify(messages)}`,
      "INFO"
    );
    return messages;
  }

  subscribe(handler: (messages: Message<Data>[]) => Promise<void>): void {
    this.subscriptionManager.subscribe(handler);
  }

  unsubscribe(): void {
    this.subscriptionManager.unsubscribe();
    // this.ackManager.nackAll(true);
  }

  ack(messageId?: string): void {
    this.ackManager.ack(messageId);
  }

  nack(messageId?: string, requeue = true): void {
    const messages = this.ackManager.getMessages(messageId);

    messages.forEach((message) => {
      const record = MessageSerializer.encode(message);
      const target = this.broker.router.route(record, {
        ...message.metadata,
        attempts: requeue ? message.metadata.attempts + 1 : Infinity,
      });

      this.broker.logger?.log(
        `[${this.id}] → ${target}: ${JSON.stringify(message)}`,
        target === "DLQ" ? "DANGER" : "INFO"
      );
    });

    this.ackManager.ack(messageId);
  }
}
