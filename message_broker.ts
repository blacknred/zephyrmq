import crypto from "node:crypto";
import { LinkedListPriorityQueue } from "../queues";
import { Logger, wait } from "./utils";


// class InMemoryCorrelationRegistry
//   extends Map<string, string>
//   implements ICorrelationRegistry
// {
//   private timestamps = new Map<string, number>();
//   constructor(private ttl = 60_000) {
//     super();
//   }

//   set(correlationId: string, consumerId: string) {
//     super.set(correlationId, consumerId);
//     this.timestamps.set(correlationId, Date.now() + this.ttl);
//     return this;
//   }

//   get(correlationId: string) {
//     if (this.timestamps.get(correlationId)! < Date.now()) {
//       this.delete(correlationId);
//       return undefined;
//     }
//     return super.get(correlationId);
//   }

//   delete(key: string): boolean {
//     this.forEach((consumerIdx, correlationId) => {
//       if (consumerIdx === key) super.delete(correlationId);
//     });
//     return true;
//   }
// }

/** Consistent hashing class.
 * The system works regardless of how different the key hashes are because the lookup is always
 * relative to the fixed node positions on the ring. Sorted nodes in a ring:
 * [**100(A)**, _180(user-123 key hash always belong to the B)_, **200(B)**, **300(A)**, **400(B)**, **500(A)**, **600(B)**]
 */
class ConsistencyHasher {
  private ring = new Map<number, string>();

  /**
   * Create a new instance of ConsistencyHasher with the given number of virtual nodes.
   * @param {number} [replicas=3] The number of virtual nodes to create for each consumer.
   * The more virtual nodes you create, the more evenly distributed the messages will be
   * among your consumers. Which means fewer hotspots, more balanced traffic. However, setting
   * this number too high can lead to a large memory footprint and slower lookups.
   */
  constructor(private replicas = 3) {}

  addNode(id: string) {
    for (let i = 0; i < this.replicas; i++) {
      const hash = this.hash(`${id}-${i}`);
      this.ring.set(hash, id);
    }
  }
  removeNode(id: string) {
    const nodeExists = [...this.ring.values()].includes(id);
    if (!nodeExists) return;
    for (let i = 0; i < this.replicas; i++) {
      const hash = this.hash(`${id}-${i}`);
      this.ring.delete(hash);
    }
  }
  getNode(key: string): string | undefined {
    const hash = this.hash(key);
    const sortedHashes = [...this.ring.keys()].sort((a, b) => a - b);
    const node = sortedHashes.find((h) => h >= hash) || sortedHashes[0];
    return this.ring.get(node);
  }
  private hash(key: string): number {
    return parseInt(
      crypto.createHash("sha256").update(key).digest("hex").slice(0, 8),
      16
    );
  }
}
type ILogger = {
  log(message: string, level: "INFO" | "WARN" | "DANGER"): void;
};
type IQueue<Data = any> = {
  enqueue(data: Data, priority?: number): void;
  dequeue(): Data | undefined;
  peek(): Data | undefined;
  isEmpty(): boolean;
  size(): number;
};
type IMessageMetadata = {
  id: string;
  topic: string;
  attempts: number;
  timestamp: number;
  correlationId?: string;
  priority?: number;
  ttl?: number;
  ttd?: number;
  //
  batchId?: string;
  batchIndex?: number;
  batchSize?: number;
};
class Message<Data = any> {
  static iterator = 1;
  constructor(public data: Data, public metadata: IMessageMetadata) {
    this.metadata.timestamp ||= Date.now();
    this.metadata.attempts ??= 1;
    this.metadata.priority ??= 0;
  }
  static encode<Data>(message: Message<Data>) {
    try {
      return new TextEncoder().encode(JSON.stringify(message));
    } catch (e) {
      throw new Error("Failed to encode message");
    }
  }
  static decode<Data>(record: Uint8Array<ArrayBufferLike>) {
    try {
      const message = JSON.parse(new TextDecoder().decode(record));
      return new Message<Data>(message.data, message.metadata);
    } catch (e) {
      throw new Error("Failed to decode message");
    }
  }
  isExpired() {
    const { ttl, timestamp } = this.metadata;
    if (!ttl) return false;
    return timestamp! + ttl <= Date.now();
  }
  isDelayed() {
    const { ttd, timestamp } = this.metadata;
    if (!ttd) return false;
    return timestamp! + ttd > Date.now();
  }
  getDeliveryDate() {
    const { ttd, timestamp } = this.metadata;
    if (!ttd) return timestamp;
    return timestamp! + ttd - Date.now();
  }
  isExceedAttempts(maxAttempts: number) {
    const { attempts = 0, priority = 0 } = this.metadata;
    if (attempts <= maxAttempts) return false;
    this.metadata.priority = priority - 1;
    this.metadata.attempts = attempts + 1;
    return true;
  }
}
class Topic {
  unicastQueues: Map<string, IQueue<Uint8Array<ArrayBufferLike>>> = new Map();
  constructor(
    public name: string,
    public broadcastQueue: IQueue<Uint8Array<ArrayBufferLike>>,
    public hasher: ConsistencyHasher
  ) {}
}
type ITopicBrokerOptions = {
  Queue: new () => IQueue<Uint8Array<ArrayBufferLike>>;
  Hasher: new () => ConsistencyHasher;
  maxDeliveryAttempts?: number;
  maxMessageSize?: number;
  logger?: ILogger;
};
class TopicBroker {
  private Queue: new () => IQueue<Uint8Array<ArrayBufferLike>>;
  public Hasher: new () => ConsistencyHasher;
  public topics: Map<string, Topic> = new Map();

  public dlqQueue: IQueue<Uint8Array<ArrayBufferLike>>;
  public delayedQueue: IQueue<Uint8Array<ArrayBufferLike>>;
  private nextDelayedDeliveryTimeout: number;

  public logger?: ILogger;
  public maxDeliveryAttempts: number;
  public maxMessageSize: number;

  constructor(options: ITopicBrokerOptions) {
    this.Queue = options.Queue;
    this.Hasher = options.Hasher;
    this.logger = options.logger;

    const { maxDeliveryAttempts = 10, maxMessageSize = 1024 * 1024 } = options;
    if (maxDeliveryAttempts > 0) this.maxDeliveryAttempts = maxDeliveryAttempts;
    else throw new Error("MaxDeliveryAttempts cannot be negative");
    if (maxMessageSize > 0) this.maxMessageSize = maxMessageSize;
    else throw new Error("MaxMessageSize cannot be negative");

    this.dlqQueue = new this.Queue();
    this.delayedQueue = new this.Queue();
  }

  assertTopic(name: string) {
    if (this.topics.has(name)) return;
    if (!/^[a-zA-Z0-9_-]+$/.test(name)) {
      throw new Error("Invalid topic name");
    }
    this.topics.set(name, new Topic(name, new this.Queue(), new this.Hasher()));
  }

  listTopics() {
    return this.topics.keys();
  }

  deleteTopic(name: string) {
    if (!this.topics.has(name)) throw new Error("Topic not found");
    this.topics.delete(name);
  }

  registerConsumer(topicName: string, id: string) {
    const topic = this.topics.get(topicName)!;
    if (!topic) throw new Error("Topic is not exist");

    topic.unicastQueues.set(id, new this.Queue());
    topic.hasher.addNode(id);
    return topic;
  }

  unregisterConsumer(topicName: string, id: string) {
    const topic = this.topics.get(topicName)!;
    if (!topic) throw new Error("Topic is not exist");

    topic.hasher.removeNode(id);
    topic.unicastQueues.delete(id);
    if (!topic.unicastQueues.size) {
      this.deleteTopic(topicName);
    }
  }

  registerProducer(topicName: string, id: string) {
    const topic = this.topics.get(topicName)!;
    if (!topic) throw new Error("Topic is not exist");

    if (topicName == this.dlqTopic) throw new Error("Invalid topic");
    return topic;
  }

  // misc

  private processDelayedQueue() {
    const now = Date.now();
    while (!this.delayedQueue.isEmpty()) {
      const record = this.delayedQueue.peek()!;
      const { data, metadata } = Message.decode(record);
      if (metadata.timestamp! > now) break;

      this.delayedQueue.dequeue();
      const { topic: topicName, correlationId, priority } = metadata;
      const topic = this.topics.get(topicName!);
      if (!topic) {
        this.logger?.log(
          `[Delayed] ==> (NULL): ${JSON.stringify(data)}`,
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
        `[Delayed] ==> (${metadata.topic}): ${JSON.stringify(data)}`,
        "INFO"
      );
    }

    this.scheduleNextDeliveryCheck();
  }

  scheduleNextDeliveryCheck() {
    if (this.nextDelayedDeliveryTimeout) {
      clearTimeout(this.nextDelayedDeliveryTimeout);
    }

    if (this.delayedQueue.isEmpty()) return;
    const record = this.delayedQueue.peek()!;
    const { metadata } = Message.decode(record);

    const delay = Math.max(0, metadata.timestamp! - Date.now());
    this.nextDelayedDeliveryTimeout = setTimeout(
      this.processDelayedQueue.bind(this),
      delay
    );
  }
}
class Producer<Data = any> {
  static iterator = 1;
  id: string;
  topic: Topic;
  constructor(private broker: TopicBroker, public topicName: string) {
    this.id = `${Producer.iterator++}`;
    this.topic = this.broker.registerProducer(topicName, this.id);
  }

  send(
    data: Data,
    metadata: Pick<
      Partial<IMessageMetadata>,
      "topic" | "priority" | "correlationId" | "ttd" | "ttl"
    > = {}
  ) {
    if (data == null) throw new Error("Invalid data");

    metadata.id = `${Message.iterator++}`;
    const message = new Message(data, metadata);
    message.metadata.topic = this.topic.name;
    const record = Message.encode(message);
    const { correlationId, priority } = message.metadata;

    if (record.length > this.broker.maxMessageSize) {
      throw new Error(
        `Message size exceeds limit ${this.broker.maxMessageSize}B`
      );
    }

    if (message.isDelayed()) {
      this.broker.delayedQueue.enqueue(record, message.getDeliveryDate());
      this.broker.logger?.log(
        `[producer-${this.id}] ==> (Delayed): ${JSON.stringify(data)}`,
        "INFO"
      );
      return this.broker.scheduleNextDeliveryCheck();
    }

    if (correlationId) {
      const consumerId = this.topic.hasher.getNode(correlationId)!;
      this.topic.unicastQueues.get(consumerId)?.enqueue(record, priority);
    } else {
      this.topic.broadcastQueue.enqueue(record, priority);
    }

    this.broker.logger?.log(
      `[producer-${this.id}] ==> (${this.topic}): ${JSON.stringify(data)}`,
      "INFO"
    );
  }

  sendBatch(items: Array<{ data: Data; metadata?: IMessageMetadata }>) {
    const batchId = `batch-${Date.now()}`;
    items.forEach((item, batchIndex) => {
      this.send(item.data, {
        ...item.metadata,
        batchId,
        batchIndex,
        batchSize: items.length,
      });
    });
  }
}

type IConsumerOptions = {
  topic: string;
  limit?: number;
  autoAck?: boolean;
};
class Consumer<Data = any> {
  static iterator = 0;
  id: string;
  topic: Topic;
  limit: number;
  autoAck: boolean;
  polling = false;
  private pendingAcks = new Map<string, Message>();
  constructor(private broker: TopicBroker, options: IConsumerOptions) {
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
        const message = Message.decode<Data>(record);
        if (!this.autoAck) this.pendingAcks.set(message.metadata.id, message);
        if (message.isExpired()) this.nack(message.metadata.id, false);
        else messages.push(message);
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
    if (!messages.length) await wait(100);
    else await handler(messages);
    if (this.polling) this.poll(handler);
  }

  ack(messageId?: string) {
    if (messageId) this.pendingAcks.delete(messageId);
    else this.pendingAcks.clear();
  }

  nack(messageId?: string, requeue = true) {
    const message = this.pendingAcks.get(messageId);
    if (!message) return;
    const { correlationId, priority } = message.metadata;
    const record = Message.encode(message);
    let topicName = this.topic.name;

    if (!requeue || message.isExceedAttempts(this.broker.maxDeliveryAttempts)) {
      topicName = this.broker.dlqTopic;
      const topic = this.broker.topics.get(topicName);
      topic?.broadcastQueue.enqueue(record);
    } else if (correlationId) {
      const consumerId = this.topic.hasher.getNode(correlationId)!;
      this.topic.unicastQueues.get(consumerId)?.enqueue(record, priority);
    } else {
      this.topic.broadcastQueue.enqueue(record, priority);
    }

    this.broker.logger?.log(
      `[producer-${this.id}] ==> (${topicName}): ${JSON.stringify(
        message.data
      )}`,
      "INFO"
    );

    // // Add delay before re-queuing (exponential backoff)
    // const delay = this.broker.calculateBackoff(message.metadata.attempts!);
    // await wait(delay);
    //   calculateBackoff(attempt: number) {
    // return Math.min(2 ** attempt * 1000, 30000);
  }
}

/** Example */

const b = new TopicBroker({
  Queue: LinkedListPriorityQueue,
  Hasher: ConsistencyHasher,
  logger: new Logger(),
});

const pub = new Producer(b, "test");
pub.send({ test: 1 });

const sub = new Consumer(b, { topic: "test", limit: 1 });
const messages = sub.consume();
sub.ack();

// b.unsubscribe(sub);
// const brokerWithHeap = new Broker(HeapQueue);
// const pub = new Producer(brokerWithHeap, "test");

// // Send 100K messages with random priorities
// for (let i = 0; i < 100_000; i++) {
//   pub.send({ i }, { priority: Math.random() * 10 });
// }

// // Consume all
// const sub = new Consumer(brokerWithHeap, "test");
// while (!brokerWithHeap.queues["test"].isEmpty()) {
//   sub.consume(); // ~3x faster than LinkedListQueue
// }

// type ILogger = {
//   log(message: string, level: "INFO" | "WARN" | "DANGER"): void;
// };
// type IMessageResolver<Data = any> = (records: Message<Data>[]) => void;
// class Consumer<Data> {
//   private resolver?: IMessageResolver<Data>;
//   constructor(
//     private broker: MessageBroker,
//     private topic: string,
//     private limit = 1,
//     private logger?: ILogger
//   ) {
//     // this.broker.register(this)
//     this.broker.assertTopic(topic);
//   }
//   private async polling() {
//     while (this.resolver !== undefined) {
//       const records = this.consume();
//       if (!records.length) await wait(0);
//       else {
//         const data = JSON.stringify(records.map((r) => r.data));
//         this.logger?.log(
//           `[consumer-${1}] <== (${this.topic}): ${data}`,
//           "INFO"
//         );
//         try {
//           await this.resolver(records);
//         } catch (e) {
//           this.nack(records);
//         }
//       }
//     }
//   }

//   consume() {
//     return this.broker.read<Data>(this.topic, this.limit);
//   }
//   subscribe(resolver: IMessageResolver<Data>) {
//     this.resolver = resolver;
//     this.polling();
//   }
//   unsubscribe() {
//     this.resolver = undefined;
//   }
// }

// /** Examples */

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

// const producer1 = new Producer<MyData>(mb, logger);
// producer1.send("ttt", { n: 1 }, { ttl: Date.now() + 1000 });

// const unsubscribe1 = mb.subscribe(listener(600), 2);
// const unsubscribe2 = mb.subscribeTo("", listener(300), 4);
// let i = 0;
// const interval = setInterval(
//   () => mb.sendTo("", { n: ++i }, { priority: 1 }),
//   10
// );
// // for (let i = 1; i <= 100; i++) myQueue.send(`${i}`)
// wait(1500).then(() => {
//   clearInterval(interval);
//   unsubscribe1();
//   unsubscribe2();
//   console.log(mb.consumersCount);
// });

// // Consumer.consume.subscribe.unsubscribe.pause
// // Producer.send

// class PersistentQueue<Data> implements IQueue<Data> {
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

// await Promise.all(
//       messages.map(async msg => {
//         try {
//           await Promise.race([
//             handler(msg),
//             new Promise((_, reject) =>
//               setTimeout(reject, this.processingTimeout, 'Processing timeout')
//             )
//           ]);
//           // Implicit ACK on success
//         } catch (error) {
//           super.nack(topic, msg.metadata.correlationId!, false); // Send to DLQ
//         }
//       })
//     );
