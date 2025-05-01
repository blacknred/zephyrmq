import { LinkedListQueue } from "./queues";
import { Logger, wait } from "./utils";

type IQueue<Data = any> = {
  enqueue(data: Data, priority?: number): void;
  dequeue(): Data | undefined;
  peak(): Data | undefined;
  isEmpty(): boolean;
  size(): number;
};

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

// can have millions of keys (orderId as correlationId e.g.), use redis in prod
class CorrelationRegistry extends Map<string, number> {}

class Broker {
  private queues: Record<string, IQueue<Uint8Array<ArrayBufferLike>>> = {};
  private deadLetterTopic = "dead-letter";
  private consumerIterator = 0;

  constructor(
    private Queue: new () => IQueue<Uint8Array<ArrayBufferLike>>,
    private correlations?: CorrelationRegistry
  ) {
    this.assertTopic(this.deadLetterTopic);
  }

  assertTopic(name: string) {
    if (this.queues[name]) return;
    this.queues[name] = new this.Queue();
  }

  purgeTopic(name: string) {
    delete this.queues[name];
  }

  listTopics() {
    return Object.keys(this.queues);
  }

  sendTo<Data>(topic: string, data: Data, metadata?: IMessageMetadata) {
    if (!this.queues[topic]) throw new Error("Topic not found");
    if (topic == this.deadLetterTopic) throw new Error("Cannot send to topic");

    const message = new Message(data, metadata || {});
    message.metadata.timestamp = Date.now();
    const record = Message.encode(message);

    try {
      const { priority, attempts } = message.metadata;
      if (attempts && attempts > 10) {
        this.queues[this.deadLetterTopic].enqueue(record, priority);
      } else {
        this.queues[topic].enqueue(record, priority);
      }
    } catch (e) {
      throw new Error("Failed to enqueue message");
    }
  }

  nack(topic: string, messages: Message[]) {
    for (const message of messages) {
      const { attempts = 0 } = message.metadata;
      message.metadata.priority = -1;
      message.metadata.attempts = attempts + 1;
      this.sendTo<any>(topic, message);
    }
  }

  readFrom<Data>(topic: string, consumerId: number, prefetch: number) {
    const messages: Message<Data>[] = [];
    const now = Date.now();
    // Batching
    try {
      while (messages.length < prefetch) {
        if (this.queues[topic].isEmpty()) break;
        const record = this.queues[topic].peak()!;
        const message = Message.decode<Data>(record);
        const { correlationId, ttl, ttd } = message.metadata;
        // Time-to-deliver
        if (ttd && ttd > now) continue;
        // Time-to-live
        if (ttl && ttl > now) {
          this.queues[topic].dequeue();
          continue;
        }
        // Consistent hashing
        if (this.correlations && correlationId) {
          const correlatedConsumerId = this.correlations.get(correlationId);
          if (correlatedConsumerId && correlatedConsumerId !== consumerId) {
            continue;
          }
          this.correlations.set(correlationId, consumerId);
        }

        this.queues[topic].dequeue()!;
        messages.push(message);
      }
    } catch (e) {
    } finally {
      return messages;
    }
  }

  subscribe(subcription: Subscription) {
    if (!this.queues[subcription.topic]) throw new Error("Topic not found");
    const id = this.consumerIterator++;
    // add subscription
    return id;
  }

  unsubscribe(subcription: Subscription) {
    if (!this.queues[subcription.topic]) throw new Error("Topic not found");
    // remove subscription
    this.correlations?.forEach((corellatedConsumerIdx, correlationId) => {
      if (corellatedConsumerIdx === subcription.id) {
        this.correlations?.delete(correlationId);
      }
    });
  }
}

class Subscription<Data = any> {
  id: number;
  constructor(
    private broker: Broker,
    public topic: string,
    public prefetch = 1
  ) {
    this.id = this.broker.subscribe(this);
  }
  read() {
    return this.broker.readFrom<Data>(this.topic, this.id, this.prefetch);
  }
  poll() {}
  nack(messages: Message[]) {
    this.broker.nack(this.topic, messages);
  }
}

/** Example */

const b = new Broker(LinkedListQueue, new CorrelationRegistry());
b.assertTopic("test");
b.sendTo("test", { data: "test" });

const sub = new Subscription(b, "test");
const messages = sub.read();
sub.nack(messages);
b.unsubscribe(sub);

const sub2 = new Subscription(b, "test");
const messages2 = sub2.read();
b.unsubscribe(sub2);

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

// class Producer<Data> {
//   constructor(private broker: MessageBroker, private logger?: ILogger) {}
//   send(topic: string, data: Data, metadata: IMessageMetadata = {}) {
//     this.broker.assertTopic(topic);
//     const message = new Message<Data>(data, metadata);
//     this.logger?.log(
//       `[producer-${1}] ==> (${topic}): ${JSON.stringify(data)}`,
//       "INFO"
//     );
//     return this.broker.send(topic, message);
//     // 2021-08-27 13:09:50.349  INFO [producer-1] ===> (Purchases): key = htanaka value = t-shirts
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
