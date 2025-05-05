import crypto from "node:crypto";
import { LinkedListPriorityQueue } from "../queues";
import { Logger, wait } from "./utils";

// interfaces.ts
type ILogger = {
  log(message: string, level: "INFO" | "WARN" | "DANGER"): void;
};
type IPriorityQueue<Data = any> = {
  enqueue(data: Data, priority?: number): void;
  dequeue(): Data | undefined;
  peek(): Data | undefined;
  isEmpty(): boolean;
  size(): number;
};
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
type IConsistencyHasher = {
  addNode(id: string): void;
  removeNode(id: string): void;
  getNode(key: string): string | undefined;
};
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
//
//
// Message.ts
type IMessageMetadata = {
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
};
class Message<Data = any> {
  static iterator = 1;
  metadata: IMessageMetadata;

  constructor(
    public data: Data,
    metadata: Partial<IMessageMetadata> & Pick<IMessageMetadata, "topic">
  ) {
    this.metadata = {
      id: `${Message.iterator++}`,
      timestamp: Date.now(),
      attempts: 1,
      priority: 0,
      ...metadata,
    };
  }

  isExpired() {
    const { ttl, timestamp } = this.metadata;
    return ttl && timestamp + ttl <= Date.now();
  }

  isDelayed() {
    const { ttd, timestamp } = this.metadata;
    return ttd && timestamp + ttd > Date.now();
  }

  getCurrentDelay() {
    const { ttd, timestamp } = this.metadata;
    return !ttd ? 0 : timestamp + ttd - Date.now();
  }

  iterateAttempts() {
    const { attempts = 0, priority = 0 } = this.metadata;
    this.metadata.priority = priority - 1;
    this.metadata.attempts = attempts + 1;
  }
}
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
//
//
// TopicRegistry.ts (need sharding)
class TopicRegistry {
  private topics = new Map<string, Topic>();

  constructor(
    private queueFactory: () => IPriorityQueue<Uint8Array>,
    private hasherFactory: () => IConsistencyHasher
  ) {}

  create(name: string): Topic {
    if (!name || !/^[a-zA-Z0-9_-]+$/.test(name)) {
      throw new Error("Invalid topic name");
    }

    if (this.topics.has(name)) {
      throw new Error("Topic already exists");
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

  delete(name: string): void {
    if (!this.topics.has(name)) {
      throw new Error("Topic not found");
    }
    this.topics.delete(name);
  }
}
//
//
// ClientManager.ts
class ClientManager {
  constructor(
    private topicRegistry: TopicRegistry,
    private queueFactory: () => IPriorityQueue<Uint8Array>
  ) {}

  registerConsumer(topicName: string, consumerId: string): Topic {
    const topic = this.topicRegistry.get(topicName);
    if (!topic) throw new Error("Topic not Found");
    topic.unicastQueues.set(consumerId, this.queueFactory());
    topic.hasher.addNode(consumerId);
    return topic;
  }

  unregisterConsumer(topicName: string, consumerId: string): void {
    const topic = this.topicRegistry.get(topicName);
    if (!topic) throw new Error("Topic not Found");
    topic?.unicastQueues.delete(consumerId);
    topic?.hasher.removeNode(consumerId);
  }

  registerProducer(topicName: string): Topic {
    const topic = this.topicRegistry.get(topicName);
    if (!topic) throw new Error("Topic not Found");
    return topic;
  }
}
//
//
// DeliveryService.ts
class DeliveryService {
  private nextDeliveryTimeout?: number;

  constructor(
    private delayedQueue: IPriorityQueue<Uint8Array>,
    private dlQueue: IPriorityQueue<Uint8Array>,
    private topicRegistry: TopicRegistry,
    private logger?: Logger
  ) {}

  scheduleDelayedDelivery(): void {
    if (this.nextDeliveryTimeout) {
      clearTimeout(this.nextDeliveryTimeout);
    }
    if (this.delayedQueue.isEmpty()) return;

    const record = this.delayedQueue.peek()!;
    const message = MessageSerializer.decode(record);
    const delay = Math.max(0, message.getCurrentDelay());

    this.nextDeliveryTimeout = setTimeout(() => {
      this.processDelayedQueue();
    }, delay);
  }

  private processDelayedQueue(): void {
    while (!this.delayedQueue.isEmpty()) {
      const record = this.delayedQueue.dequeue()!;
      const message = MessageSerializer.decode(record);
      const { topic: topicName, correlationId, priority } = message.metadata;
      const topic = this.topicRegistry.getTopic(topicName);

      if (!topic || message.isExpired()) {
        this.dlQueue.enqueue(record);
        this.logger?.log(
          `[Delayed] ==> (dlq): ${JSON.stringify(message.data)}`,
          "WARN"
        );
        continue;
      }

      if (correlationId) {
        const consumerId = topic.hasher.getNode(correlationId)!;
        topic.unicastQueues.get(consumerId)?.enqueue(record, priority);
      } else {
        topic.broadcastQueue.enqueue(record, priority);
      }

      this.logger?.log(
        `[Delayed] ==> (${topicName}): ${JSON.stringify(message.data)}`,
        "INFO"
      );
    }

    this.scheduleDelayedDelivery();
  }
}
//
//
// TopicBroker.ts (Facade)
type TopicBrokerConfig = {
  maxDeliveryAttempts: number;
  maxMessageSize: number;
  replicas: number;
  defaultPollingInterval?: number;
};
class TopicBroker {
  public readonly dlQueue: IPriorityQueue<Uint8Array>;
  public readonly delayedQueue: IPriorityQueue<Uint8Array>;

  private topicRegistry: TopicRegistry;
  private clientManager: ClientManager;
  private deliveryService: DeliveryService;

  constructor(
    private config: TopicBrokerConfig,
    private queueFactory: () => IPriorityQueue<Uint8Array>,
    private hasherFactory: () => IConsistencyHasher,
    private logger?: Logger
  ) {
    this.topicRegistry = new TopicRegistry(queueFactory, hasherFactory);
    this.clientManager = new ClientManager(this.topicRegistry, queueFactory);
    this.deliveryService = new DeliveryService(
      this.queueFactory(),
      this.queueFactory(),
      this.topicRegistry,
      logger
    );
  }

  createTopic(name: string): void {
    this.topicRegistry.create(name);
  }

  listTopics() {
    return this.topicRegistry.list();
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

  scheduleNextDelayedDelivery() {
    return this.deliveryService.scheduleDelayedDelivery();
  }
}

// class TopicBroker {
//   private queueFactory: new () => IPriorityQueue<Uint8Array>;
//   public hasherFactory: new (hashService: IHashService) => ConsistencyHasher;
//   public topics: Map<string, Topic> = new Map();

//   public dlQueue: IPriorityQueue<Uint8Array>;
//   public delayedQueue: IPriorityQueue<Uint8Array>;
//   private nextDelayedDeliveryTimeout: number;

//   public logger?: ILogger;
//   public maxDeliveryAttempts: number;
//   public maxMessageSize: number;

//   constructor(options: {
//     queueFactory: new () => IPriorityQueue<Uint8Array>;
//     hasherFactory: new () => ConsistencyHasher;
//     logger?: ILogger;
//     maxDeliveryAttempts?: number;
//     maxMessageSize?: number;

//   }) {
//     this.queueFactory = options.queueFactory;
//     this.hasherFactory = options.hasherFactory;
//     this.logger = options.logger;

//     const { maxDeliveryAttempts = 10, maxMessageSize = 1024 * 1024 } = options;
//     if (maxDeliveryAttempts > 0) this.maxDeliveryAttempts = maxDeliveryAttempts;
//     else throw new Error("MaxDeliveryAttempts cannot be negative");
//     if (maxMessageSize > 0) this.maxMessageSize = maxMessageSize;
//     else throw new Error("MaxMessageSize cannot be negative");

//     this.dlQueue = new this.queueFactory();
//     this.delayedQueue = new this.queueFactory();
//   }

// assertTopic(name: string) {
//   if (this.topics.has(name)) return;
//   if (!/^[a-zA-Z0-9_-]+$/.test(name)) {
//     throw new Error("Invalid topic name");
//   }
//   this.topics.set(
//     name,
//     new Topic(
//       name,
//       new this.queueFactory(),
//       new this.hasherFactory(new SHA256HashService())
//     )
//   );
// }

// listTopics() {
//   return this.topics.keys();
// }

// deleteTopic(name: string) {
//   this.getTopicOrThrow(name);
//   this.topics.delete(name);
// }

// private getTopicOrThrow(name: string) {
//   if (!this.topics.has(name)) throw new Error("Topic not found");
//   return this.topics.get(name);
// }

// registerConsumer(topicName: string, consumerId: string) {
//   const topic = this.getTopicOrThrow(topicName)!;
//   topic.unicastQueues.set(consumerId, new this.Queue());
//   topic.hasher.addNode(consumerId);
//   return topic;
// }

// unregisterConsumer(topicName: string, consumerId: string) {
//   const topic = this.getTopicOrThrow(topicName)!;
//   topic.unicastQueues.delete(consumerId);
//   topic.hasher.removeNode(consumerId);
// }

// registerProducer(topicName: string, producerId: string) {
//   return this.getTopicOrThrow(topicName)!;
// }

// scheduleNextDelayedDelivery() {
//   if (this.nextDelayedDeliveryTimeout) {
//     clearTimeout(this.nextDelayedDeliveryTimeout);
//   }
//   if (this.delayedQueue.isEmpty()) return;

//   const record = this.delayedQueue.peek()!;
//   const message = MessageSerializer.decode(record);
//   const delay = Math.max(0, message.getCurrentDelay());
//   this.nextDelayedDeliveryTimeout = setTimeout(
//     this.processDelayedQueue,
//     delay
//   );
// }

// private processDelayedQueue = () => {
//   while (!this.delayedQueue.isEmpty()) {
//     const record = this.delayedQueue.dequeue()!;
//     const { data, metadata } = MessageSerializer.decode(record);
//     const { topic: topicName, correlationId, priority } = metadata;
//     const topic = this.topics.get(topicName);

//     if (!topic) {
//       this.dlQueue.enqueue(record);
//       this.logger?.log(
//         `[Delayed] ==> (dlq): ${JSON.stringify(data)}`,
//         "WARN"
//       );
//       continue;
//     }

//     if (correlationId) {
//       const consumerId = topic.hasher.getNode(correlationId)!;
//       topic.unicastQueues.get(consumerId)?.enqueue(record, priority);
//     } else {
//       topic.broadcastQueue.enqueue(record, priority);
//     }

//     this.logger?.log(
//       `[Delayed] ==> (${metadata.topic}): ${JSON.stringify(data)}`,
//       "INFO"
//     );
//   }

//   this.scheduleNextDelayedDelivery();
// };
// }

class Producer<Data = any> {
  static iterator = 1;
  id: string;
  topic: Topic;
  constructor(private broker: TopicBroker, public topicName: string) {
    this.id = `${Producer.iterator++}`;
    this.topic = this.broker.registerProducer(topicName, this.id);
  }

  send(
    data: Data[],
    metadata?: Partial<
      Pick<IMessageMetadata, "priority" | "correlationId" | "ttd" | "ttl">
    >
  ) {
    const records: { message: Message; record: Uint8Array }[] = [];
    const batchId = `batch-${Date.now()}`;

    for (let i = 0; i < data.length; i++) {
      if (!data[i]) throw new Error("Invalid data");

      const message = new Message(data[i], {
        ...metadata,
        topic: this.topic.name,
        ...(records.length > 1 && {
          batchSize: records.length,
          batchIndex: i,
          batchId,
        }),
      });

      const record = MessageSerializer.encode(message);
      records.push({ message, record });
      if (record.length > this.broker.maxMessageSize) {
        throw new Error(
          `Message size exceeds limit ${this.broker.maxMessageSize}B`
        );
      }
    }

    for (const { message, record } of records) {
      const { correlationId, priority } = message.metadata;
      let target = this.topic.name;

      if (message.isDelayed()) {
        this.broker.delayedQueue.enqueue(record, message.metadata.ttd!);
        target = "Delayed";
      } else if (correlationId) {
        const consumerId = this.topic.hasher.getNode(correlationId)!;
        this.topic.unicastQueues.get(consumerId)?.enqueue(record, priority);
      } else {
        this.topic.broadcastQueue.enqueue(record, priority);
      }

      this.broker.logger?.log(
        `[producer-${this.id}] ==> (${target}): ${JSON.stringify(
          message.data
        )}`,
        "INFO"
      );
    }

    this.broker.scheduleNextDelayedDelivery();
    return records.length;
  }
}

class Consumer<Data = any> {
  static iterator = 0;
  id: string;
  topic: Topic;
  limit: number;
  autoAck: boolean;
  polling = false;
  private pendingAcks = new Map<string, Message>();

  constructor(
    private broker: TopicBroker,
    options: {
      topic: string;
      limit?: number;
      autoAck?: boolean;
    }
  ) {
    const { topic: topicName, limit = 1, autoAck = false } = options;
    if (limit > 0) this.limit = limit;
    else throw new Error("Limit must be positive");
    this.autoAck = autoAck;
    this.id = `${Consumer.iterator++}`;

    this.topic = this.broker.registerConsumer(topicName, this.id);
  }

  consume() {
    const messages: Message<Data>[] = [];
    const queues = [
      this.topic.unicastQueues.get(this.id),
      this.topic.broadcastQueue,
    ];

    for (const queue of queues) {
      while (queue && !queue.isEmpty() && messages.length < this.limit) {
        const record = queue.dequeue()!;
        const message = MessageSerializer.decode<Data>(record);
        if (!this.autoAck) {
          this.pendingAcks.set(message.metadata.id, message);
        }
        if (message.isExpired()) {
          this.nack(message.metadata.id, false);
        } else messages.push(message);
      }
    }

    this.broker.logger?.log(
      `[consumer-${this.id}] <== (${this.topic}): ${JSON.stringify(
        messages.map((r) => r.data)
      )}`,
      "INFO"
    );

    return messages;
  }

  subscribe(handler: (msgs: Message[]) => Promise<void>) {
    this.polling = true;
    this.poll(handler);
  }

  unsubscribe() {
    this.polling = false;
  }

  private async poll(handler: (msgs: Message[]) => Promise<void>) {
    const messages = this.consume();
    try {
      if (!messages.length) await wait(100);
      else {
        messages.forEach((m) => this.pendingAcks.set(m.metadata.id, m));
        await handler(messages);
      }
      if (this.autoAck) this.ack();
    } catch (e) {
      if (this.autoAck) this.nack();
    } finally {
      if (this.polling) this.poll(handler);
    }
  }

  ack(messageId?: string) {
    if (messageId) this.pendingAcks.delete(messageId);
    else this.pendingAcks.clear();
  }

  nack(messageId?: string, requeue = true) {
    const messages = messageId
      ? [this.pendingAcks.get(messageId)!]
      : Array.from(this.pendingAcks.values());

    for (const message of messages) {
      const { correlationId, priority, attempts } = message.metadata;
      let target = this.topic.name;

      if (!requeue || attempts > this.broker.maxDeliveryAttempts) {
        const record = MessageSerializer.encode(message);
        this.broker.dlQueue.enqueue(record);
        target = "dlq";
      } else {
        message.iterateAttempts();
        const record = MessageSerializer.encode(message);
        if (correlationId) {
          const consumerId = this.topic.hasher.getNode(correlationId)!;
          this.topic.unicastQueues.get(consumerId)?.enqueue(record, priority);
        } else {
          this.topic.broadcastQueue.enqueue(record, priority);
        }
      }

      this.broker.logger?.log(
        `[producer-${this.id}] ==> (${target}): ${JSON.stringify(
          message.data
        )}`,
        "WARN"
      );
    }
  }
}

/** Example */

const b = new TopicBroker({
  queueFactory: LinkedListPriorityQueue,
  hasherFactory: ConsistencyHasher,
  logger: new Logger(),
});
b.assertTopic("test");

const prod = new Producer<{ test: number }>(b, "test");
prod.send([{ test: 1 }], { correlationId: "user-123" });

const con = new Consumer<{ test: number }>(b, { topic: "test", limit: 1 });
const messages = con.consume();
con.ack();

// con.subscribe(async (m) => {
//   m[0].data.
// });

// // Send 100K messages with random priorities
// for (let i = 0; i < 100_000; i++) {
//   pub.send({ i }, { priority: Math.random() * 10 });
// }

// const listener = (delay: number) => async (m: any[]) => {
//   await wait(delay);
//   return true; //Math.random() > 0.5;
// };

// type MyData = { n: number };
// const logger = new Logger();
// const mb = new MessageBroker(LinkedListQueue);
// const consumer1 = new Consumer<MyData>(mb, "ttt", 5);
// consumer1.subscribe(listener(1000));
// consumer1.consume();
// consumer1.polling();

// class PersistentQueue<Data> implements IPriorityQueue<Data> {
//   private wal = new fs.createWriteStream("./queue.wal", { flags: "a" });

//   enqueue(data: Data, priority?: number) {
//     const entry = JSON.stringify({ data, priority });
//     this.wal.write(`${entry}\n`);
//     this.inMemoryQueue.enqueue(data, priority); // Delegate to HeapQueue
//   }

//   async recover() {
//     const lines = fs.readFileSync("./queue.wal", "utf-8").split("\n");
//     lines.forEach(line => {
//       if (!line) return;
//       const { data, priority } = JSON.parse(line);
//       this.inMemoryQueue.enqueue(data, priority);
//     });
//   }
// }

// kafka implements queue per consumer instead of per topic since it consumers are services(has different perf) and not workers(has similar perf)
// workers => queue per topic + virtual offsets
// pubsub => queue per consumer

// Different speeds/needs → Queue-per-consumer
// Uniform workers → Queue-per-topic

// Add delay before re-queuing (exponential backoff)
// calculateBackoff(attempt: number) {return Math.min(2 ** attempt * 1000, 30000);
