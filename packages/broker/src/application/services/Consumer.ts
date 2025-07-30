import type {
  IConsumer,
  ISubscriptionListener,
} from "@app/interfaces/IConsumer";
import type { AckMessages } from "@app/usecases/message/AckMessages";
import type { ConsumeMessages } from "@app/usecases/message/ConsumeMessages";
import type { NackMessages } from "@app/usecases/message/NackMessages";
import type { SubscribeToMessages } from "@app/usecases/message/SubscribeToMessages";
import type { UnsubscribeToMessages } from "@app/usecases/message/UnsubscribeToMessages";

export class Consumer<Data> implements IConsumer<Data> {
  static MAX_LIMIT = 1000;
  constructor(
    private consumeMessages: ConsumeMessages,
    private ackMessages: AckMessages,
    private nackMessages: NackMessages,
    private subscribeToMessages: SubscribeToMessages,
    private unsubscribeToMessages: UnsubscribeToMessages,
    public readonly id: number,
    private readonly noAck = false,
    private readonly limit = 1
  ) {
    this.limit = Math.min(Consumer.MAX_LIMIT, limit);
  }

  async consume() {
    return this.consumeMessages.execute<Data>(this.id, this.limit, this.noAck);
  }

  async ack(messageId?: number) {
    return this.ackMessages.execute(this.id, messageId);
  }

  async nack(messageId?: number, requeue = true): Promise<number> {
    return this.nackMessages.execute(this.id, messageId, requeue);
  }

  async subscribe(listener: ISubscriptionListener<Data>) {
    await this.subscribeToMessages.execute<Data>(this.id, listener, this.noAck);
  }

  async unsubscribe() {
    await this.unsubscribeToMessages.execute(this.id);
  }
}
