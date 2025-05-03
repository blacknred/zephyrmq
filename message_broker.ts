import { LinkedListQueue } from "../queue";
import { Logger, wait } from "./utils";

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
class InMemoryQueue extends LinkedListQueue implements IQueue {
  // HeapQueue is 3-4x faster at scale due to O(log n) vs. O(n) inserts
  // LinkedListQueue struggles with priorities (sorts on every insert).
  // Memory usage is similar (~10MB for 1M messages).
}

type ICorrelationRegistry = {
  has(correlationId: string): boolean;
  get(correlationId: string): string | undefined;
  set(correlationId: string, consumerId: string): ICorrelationRegistry;
  delete(consumerId: string): boolean;
};
class InMemoryCorrelationRegistry
  extends Map<string, string>
  implements ICorrelationRegistry
{
  private timestamps = new Map<string, number>();
  constructor(private ttl = 60_000) {
    super();
  }

  set(correlationId: string, consumerId: string) {
    super.set(correlationId, consumerId);
    this.timestamps.set(correlationId, Date.now() + this.ttl);
    return this;
  }

  get(correlationId: string) {
    if (this.timestamps.get(correlationId)! < Date.now()) {
      this.delete(correlationId);
      return undefined;
    }
    return super.get(correlationId);
  }

  delete(key: string): boolean {
    this.forEach((consumerIdx, correlationId) => {
      if (consumerIdx === key) super.delete(correlationId);
    });
    return true;
  }
}

class Broker {
  public queues: Record<string, IQueue<Uint8Array<ArrayBufferLike>>> = {};
  private producers: Record<string, Set<string>> = {};
  private consumers: Record<string, Set<string>> = {};
  public deadLetterTopic = "dead-letter";

  constructor(
    private Queue: new () => IQueue<Uint8Array<ArrayBufferLike>>,
    public correlations?: ICorrelationRegistry,
    public logger?: ILogger,
    public maxDeliveryAttempts = 10
  ) {
    this.assertTopic(this.deadLetterTopic);
  }

  assertTopic(name: string) {
    if (this.queues[name]) return;
    this.queues[name] = new this.Queue();
  }

  listTopics() {
    return Object.keys(this.queues);
  }

  purgeTopic(name: string) {
    if (!this.queues[name]) throw new Error("Topic not found");
    delete this.queues[name];
    delete this.producers[name];
    delete this.consumers[name];
  }

  // Consistent hashing
  isNotCorrelate(correlationId: string, agentId: string) {
    if (!this.correlations) return false;
    const correlatedAgentId = this.correlations.get(correlationId);
    if (correlatedAgentId && correlatedAgentId !== agentId) return true;
    this.correlations.set(correlationId, agentId);
    return false;
  }

  register(agent: Producer | Consumer) {
    const { topic, id } = agent;
    this.assertTopic(topic);
    if (agent instanceof Producer) {
      if (topic == this.deadLetterTopic) {
        throw new Error("Invalid topic");
      }
      this.producers[topic].add(id);
    } else {
      this.consumers[topic].add(id);
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
}

type IMessageMetadata = Partial<{
  correlationId: string;
  priority: number;
  ttl: number;
  ttd: number;
  attempts: number;
  timestamp: number;
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
    const message = new Message(data, metadata);
    try {
      const record = Message.encode(message);
      this.broker.queues[this.topic].enqueue(record, message.metadata.priority);

      this.broker.logger?.log(
        `[producer-${this.id}] ==> (${this.topic}): ${JSON.stringify(data)}`,
        "INFO"
      );
    } catch (e) {
      throw new Error("Failed to enqueue message");
    }
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
      if (message.isDelayed()) continue;
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
        topic = this.broker.deadLetterTopic;
      }

      // Add delay before re-queuing (exponential backoff)
      await wait(Math.min(2 ** message.metadata.attempts! * 1000, 30000));
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

const b = new Broker(
  InMemoryQueue,
  new InMemoryCorrelationRegistry(),
  new Logger()
);

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
