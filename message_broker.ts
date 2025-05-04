import crypto from "node:crypto";
import { LinkedListPriorityQueue } from "../queues/linked_list_proirity_queue";
import { Logger, wait } from "./utils";

/** Consistent hashing class.
 * The system works regardless of how different the key hashes are because the lookup is always
 * relative to the fixed node positions on the ring. Sorted nodes in a ring:
 * [**100(A)**, _180(user-123 key hash always belong to the B)_, **200(B)**, **300(A)**, **400(B)**, **500(A)**, **600(B)**]
 */
class Hasher {
  private ring = new Map<number, string>(); // hash:consumerId

  /**
   * Create a new instance of Hasher with the given number of virtual nodes.
   * @param {number} [virtualNodes=3] The number of virtual nodes to create for each consumer.
   * The more virtual nodes you create, the more evenly distributed the messages will be
   * among your consumers. Which means fewer hotspots, more balanced traffic. However, setting
   * this number too high can lead to a large memory footprint and slower lookups.
   */
  constructor(private virtualNodes = 3) {}

  addConsumer(consumerId: string) {
    for (let i = 0; i < this.virtualNodes; i++) {
      const hash = this.hash(`${consumerId}-${i}`);
      this.ring.set(hash, consumerId);
    }
  }
  removeConsumer(consumerId: string) {
    const consumerExists = [...this.ring.values()].includes(consumerId);
    if (!consumerExists) return;
    for (let i = 0; i < this.virtualNodes; i++) {
      const hash = this.hash(`${consumerId}-${i}`);
      this.ring.delete(hash);
    }
  }
  getConsumer(key: string): string | undefined {
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

type IQueue<Data = any> = {
  enqueue(data: Data, priority?: number): void;
  dequeue(): Data | undefined;
  peek(): Data | undefined;
  isEmpty(): boolean;
  size(): number;
};

// class DelayedQueue {
//   private heap: Array<{ timestamp: number; message: Uint8Array }> = [];

//   enqueue(message: Uint8Array, delayMs: number) {
//     const deliveryTime = Date.now() + delayMs;
//     this.heap.push({ timestamp: deliveryTime, message });
//     this.heap.sort((a, b) => a.timestamp - b.timestamp); // Min-heap
//   }

//   peekNextDeliveryTime(): number | null {
//     return this.heap.length > 0 ? this.heap[0].timestamp : null;
//   }

//   dequeueDueMessages(): Uint8Array[] {
//     const now = Date.now();
//     const dueMessages: Uint8Array[] = [];

//     while (this.heap.length > 0 && this.heap[0].timestamp <= now) {
//       dueMessages.push(this.heap.shift()!.message);
//     }

//     return dueMessages;
//   }
// }

// type ICorrelationRegistry = {
//   has(correlationId: string): boolean;
//   get(correlationId: string): string | undefined;
//   set(correlationId: string, consumerId: string): ICorrelationRegistry;
//   delete(consumerId: string): boolean;
// };
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

type ILogger = {
  log(message: string, level: "INFO" | "WARN" | "DANGER"): void;
};
type IBrokerOptions = {
  Queue: new () => IQueue<Uint8Array<ArrayBufferLike>>;
  Hasher: new () => Hasher;
  logger?: ILogger;
  maxDeliveryAttempts?: number;
  maxMessageSize?: number;
};
class Broker {
  private Queue: new () => IQueue<Uint8Array<ArrayBufferLike>>;
  public Hasher: new () => Hasher;
  // public queues: Record<string, IQueue<Uint8Array<ArrayBufferLike>>> = {};
  private topics: Map<
    string,
    {
      physicalQueues: Map<string, IQueue<Uint8Array<ArrayBufferLike>>>; // per consumer
      virtualQueue: IQueue<Uint8Array<ArrayBufferLike>>; // broadcasting
      hasher: Hasher;
    }
  > = new Map();

  public delayedQueue: IQueue<Uint8Array<ArrayBufferLike>>;
  public dlqTopic = "dlq";
  private nextDeliveryTimeout: number;

  public logger?: ILogger;
  public maxDeliveryAttempts: number;
  public maxMessageSize: number;

  // private producers: Record<string, Set<string>> = {};
  // private consumers: Record<string, Set<string>> = {};

  constructor(options: IBrokerOptions) {
    this.Queue = options.Queue;
    this.Hasher = options.Hasher;
    this.logger = options.logger;

    const { maxDeliveryAttempts = 10, maxMessageSize = 1024 * 1024 } = options;
    if (maxDeliveryAttempts > 0) this.maxDeliveryAttempts = maxDeliveryAttempts;
    else throw new Error("MaxDeliveryAttempts cannot be negative");
    if (maxMessageSize > 0) this.maxMessageSize = maxMessageSize;
    else throw new Error("MaxMessageSize cannot be negative");

    this.assertTopic(this.dlqTopic);
    this.delayedQueue = new this.Queue();
  }

  assertTopic(name: string) {
    if (this.topics.has(name)) return;
    if (!/^[a-zA-Z0-9_-]+$/.test(name)) {
      throw new Error("Invalid topic name");
    }
    this.topics.set(name, {
      physicalQueues: new Map(),
      virtualQueue: new this.Queue(),
      hasher: new this.Hasher(),
    });
  }

  listTopics() {
    return this.topics.keys();
  }

  purgeTopic(name: string) {
    if (!this.topics.has[name]) throw new Error("Topic not found");
    this.topics.delete(name);
    // delete this.producers[name];
    // delete this.consumers[name];
  }

  register(agent: Producer | Consumer) {
    const { topic, id } = agent;
    this.assertTopic(topic);
    if (agent instanceof Producer) {
      if (topic == this.dlqTopic) {
        throw new Error("Invalid topic");
      }
      // this.producers[topic].add(id);
    } else {
      // this.consumers[topic].add(id);
      this.topics.set(topic)
    }
  }

  unregister(agent: Producer | Consumer) {
    const { topic, id } = agent;
    if (agent instanceof Producer) {
      this.producers[topic]?.delete(id);
    } else {
      this.consumers[topic]?.delete(id);
      this.correlations?.delete(id);
    }
    if (!this.producers[topic].size && !this.consumers[topic].size) {
      this.purgeTopic(topic);
    }
  }

  // misc

  // private processDelayedQueue() {
  //   const now = Date.now();
  //   while (!this.delayedQueue.isEmpty()) {
  //     const record = this.delayedQueue.peek()!;
  //     const { metadata } = Message.decode(record);
  //     if (metadata.timestamp! > now) break;
  //     this.queues[metadata.topic!].enqueue(record, metadata.priority);
  //   }

  //   this.scheduleNextDeliveryCheck();
  // }

  // scheduleNextDeliveryCheck() {
  //   if (this.nextDeliveryTimeout) {
  //     clearTimeout(this.nextDeliveryTimeout);
  //   }

  //   if (this.delayedQueue.isEmpty()) return;
  //   const record = this.delayedQueue.peek()!;
  //   const { metadata } = Message.decode(record);

  //   const delay = Math.max(0, metadata.timestamp! - Date.now());
  //   this.nextDeliveryTimeout = setTimeout(
  //     this.processDelayedQueue.bind(this),
  //     delay
  //   );
  // }

  calculateBackoff(attempt: number) {
    return Math.min(2 ** attempt * 1000, 30000);
  }

  // isNotCorrelate(correlationId: string, agentId: string) {
  //   // Consistent hashing
  //   if (!this.correlations) return false;
  //   const correlatedAgentId = this.correlations.get(correlationId);
  //   if (correlatedAgentId && correlatedAgentId !== agentId) return true;
  //   this.correlations.set(correlationId, agentId);
  //   return false;
  // }
}

type IMessageMetadata = Partial<{
  topic: string;
  correlationId: string;
  priority: number;
  ttl: number;
  ttd: number;
  attempts: number;
  timestamp: number;
  //
  batchId: string;
  batchIndex: number;
  batchSize: number;
}>;
class Message<Data = any> {
  constructor(public data: Data, public metadata: IMessageMetadata = {}) {
    this.metadata.timestamp ||= Date.now();
    this.metadata.attempts ??= 0;
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

class Producer<Data = any> {
  static iterator = 0;
  id: string;
  constructor(private broker: Broker, public topic: string) {
    if (!topic) throw new Error("Invalid topic");
    this.id = `${Producer.iterator++}`;
    this.broker.register(this);
  }

  send(data: Data, metadata?: IMessageMetadata) {
    if (data == null) throw new Error("Invalid data");

    const message = new Message(data, metadata);
    message.metadata.topic ||= this.topic;

    try {
      const record = Message.encode(message);
      const { maxMessageSize } = this.broker;

      if (record.length > maxMessageSize) {
        throw new Error(`Message size exceeds limit ${maxMessageSize}B`);
      }

      if (message.isDelayed()) {
        this.broker.delayedQueue.enqueue(record, message.getDeliveryDate());
        this.broker.scheduleNextDeliveryCheck();
      } else {
        this.broker.queues[this.topic].enqueue(
          record,
          message.metadata.priority
        );
      }

      this.broker.logger?.log(
        `[producer-${this.id}] ==> (${this.topic}): ${JSON.stringify(data)}`,
        "INFO"
      );
    } catch (e) {
      throw new Error(`Message rejected: ${e.message}`);
    }
  }

  sendBatch(items: Array<{ data: Data; metadata?: IMessageMetadata }>) {
    const batchId = `batch-${Date.now()}`;
    items.forEach((item, i) => {
      this.send(item.data, {
        ...item.metadata,
        batchId,
        batchIndex: i,
        batchSize: items.length,
      });
    });
  }
}

class Consumer<Data = any> {
  static iterator = 0;
  id: string;
  constructor(
    private broker: Broker,
    public topic: string,
    public prefetch = 1
  ) {
    if (!topic) throw new Error("Invalid topic");
    this.id = `${Consumer.iterator++}`;
    this.broker.register(this);
  }

  consume() {
    const messages: Message<Data>[] = [];
    let batchSize = 0;

    while (
      batchSize < this.prefetch &&
      !this.broker.queues[this.topic].isEmpty()
    ) {
      const record = this.broker.queues[this.topic].peek()!;
      const message = Message.decode<Data>(record);
      const { correlationId } = message.metadata;
      if (message.isExpired()) {
        this.broker.queues[this.topic].dequeue();
        message.metadata.attempts = this.broker.maxDeliveryAttempts; /////
        this.nack([message]);
        continue;
      }
      if (correlationId && this.broker.isNotCorrelate(correlationId, this.id)) {
        continue;
      }

      this.broker.queues[this.topic].dequeue()!;
      messages.push(message);
      batchSize++;
    }

    const data = JSON.stringify(messages.map((r) => r.data));
    this.broker.logger?.log(
      `[consumer-${this.id}] <== (${this.topic}): ${data}`,
      "INFO"
    );

    return messages;
  }

  async nack(messages: Message[]) {
    for (const message of messages) {
      let topic = this.topic;

      if (message.isExceedAttempts(this.broker.maxDeliveryAttempts)) {
        topic = this.broker.dlqTopic;
      }

      // Add delay before re-queuing (exponential backoff)
      const delay = this.broker.calculateBackoff(message.metadata.attempts!);
      await wait(delay);
      const record = Message.encode(message);
      this.broker.queues[topic].enqueue(record, message.metadata.priority);

      const data = JSON.stringify(messages.map((r) => r.data));
      this.broker.logger?.log(
        `[consumer-${this.id}] ==> (${this.topic}): ${data}`,
        "INFO"
      );
    }
  }
}

/** Example */
// HeapQueue is 3-4x faster at scale due to O(log n) vs. O(n) inserts
// LinkedListQueue struggles with priorities (sorts on every insert).
// Memory usage is similar (~10MB for 1M messages).

const b = new Broker({
  Queue: LinkedListPriorityQueue,
  correlations: new InMemoryCorrelationRegistry(),
  logger: new Logger(),
});

const pub = new Producer(b, "test");
pub.send({ test: 1 });

const sub = new Consumer(b, "test", 1);
const messages = sub.consume();
sub.nack(messages);
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
//     private prefetch = 1,
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
//     return this.broker.read<Data>(this.topic, this.prefetch);
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
