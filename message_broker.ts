import { LinkedListQueue } from "../queue";
import { Logger, wait } from "../utils";

type IQueue<Data = any> = {
  enqueue(data: Data, priority?: number): void;
  dequeue(): Data | undefined;
  peak(): Data | undefined;
  isEmpty(): boolean;
  size(): number;
};
class InMemoryQueue extends LinkedListQueue implements IQueue {}

type ICorrelationRegistry = {
  has(correlationId: string): boolean;
  get(correlationId: string): string | undefined;
  set(correlationId: string, consumerId: string): void;
  delete(consumerId: string): boolean;
};
class InMemoryCorrelationRegistry
  extends Map<string, string>
  implements ICorrelationRegistry
{
  delete(key: string): boolean {
    this.forEach((consumerIdx, correlationId) => {
      if (consumerIdx === key) super.delete(correlationId);
    });
    return true;
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
  constructor(public data: Data, public metadata: IMessageMetadata) {}
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
}

class Broker {
  public queues: Record<string, IQueue<Uint8Array<ArrayBufferLike>>> = {};
  private producers: Record<string, Set<string>> = {};
  private consumers: Record<string, Set<string>> = {};
  public deadLetterTopic = "dead-letter";

  constructor(private Queue: new () => IQueue<Uint8Array<ArrayBufferLike>>) {
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

  addProducer(producer: Producer) {
    const { topic, id } = producer;
    this.assertTopic(topic);
    this.producers[topic].add(id);
  }

  removeProducer(producer: Producer) {
    const { topic, id } = producer;
    this.producers[topic].delete(id);
    if (!this.producers[topic].size && !this.consumers[topic].size) {
      this.purgeTopic(topic);
    }
  }

  addConsumer(consumer: Consumer) {
    const { topic, id } = consumer;
    this.assertTopic(topic);
    this.consumers[topic].add(id);
  }

  removeConsumer(consumer: Consumer) {
    const { topic, id } = consumer;
    this.consumers[topic].delete(id);
    if (!this.producers[topic].size && !this.consumers[topic].size) {
      this.purgeTopic(topic);
    }
  }

  // unsubscribe(subscription: Subscription) {
  //   const { topic, id } = subscription;
  //   if (!this.queues[topic]) throw new Error("Topic not found");

  
  //   if (this.subscriptions[topic]) {
  //     this.subscriptions[topic].delete(id);
  //   }
  // }
}

class Producer<Data = any> {
  static iterator = 0;
  id: string;
  constructor(
    private broker: Broker,
    public topic: string,
    private maxDeliveryAttempts = 10,
    private timeout = 5000
  ) {
    if (topic == this.broker.deadLetterTopic) {
      throw new Error("Cannot send to topic");
    }
    this.id = `${Producer.iterator++}`;
    this.broker.addProducer(this);
  }
  send(data: Data, metadata?: IMessageMetadata) {
    let actualTopic = this.topic;
    const message = new Message(data, metadata || {});

    const { priority, attempts, timestamp } = message.metadata;
    if (!timestamp) message.metadata.timestamp = Date.now();
    if (attempts === 0) actualTopic = this.broker.deadLetterTopic;
    else if (attempts) message.metadata.attempts = attempts - 1;
    else message.metadata.attempts = this.maxDeliveryAttempts;

    try {
      const record = Message.encode(message);
      this.broker.queues[actualTopic].enqueue(record, priority);
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
    public prefetch = 1,
    private correlations?: ICorrelationRegistry
  ) {
    this.id = `${Consumer.iterator++}`;
    this.broker.addConsumer(this);
  }
  consume() {
    const messages: Message<Data>[] = [];
    const now = Date.now();
    // Batching
    try {
      while (
        messages.length < this.prefetch &&
        !this.broker.queues[this.topic].isEmpty()
      ) {
        const record = this.broker.queues[this.topic].peak()!;
        const message = Message.decode<Data>(record);
        const { correlationId, ttl, ttd } = message.metadata;
        // Time-to-deliver
        if (ttd && ttd > now) continue;
        // Time-to-live
        if (ttl && ttl > now) {
          this.broker.queues[this.topic].dequeue();
          continue;
        }
        // Consistent hashing
        if (this.correlations && correlationId) {
          const correlatedConsumerId = this.correlations.get(correlationId);
          if (correlatedConsumerId && correlatedConsumerId !== this.id) {
            continue;
          }
          this.correlations.set(correlationId, this.id);
        }

        this.broker.queues[this.topic].dequeue()!;
        messages.push(message);
      }
    } catch (e) {
    } finally {
      return messages;
    }
  }

  nack(messages: Message[]) {
    for (const message of messages) {
      const { attempts = 0 } = message.metadata;
      message.metadata.priority = -1;
      message.metadata.attempts = attempts - 1;
      this.sendTo<any>(topic, message.data, message.metadata);
    }
  }
}

/** Example */

const correlations = new InMemoryCorrelationRegistry();
const b = new Broker(InMemoryQueue);

const pub = new Producer(b, "test");
pub.send({ test: 1 });

const sub = new Consumer(b, "test", 1, correlations);
const messages = sub.consume();
sub.nack(messages);
// b.unsubscribe(sub);

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

// this.logger?.log(
//   `[producer-${1}] ==> (${topic}): ${JSON.stringify(data)}`,
//   "INFO"
// );
// return this.broker.send(topic, message);
// // 2021-08-27 13:09:50.349  INFO [producer-1] ===> (Purchases): key = htanaka value = t-shirts
