import type { ISubscriptionListener } from "@app/interfaces/IConsumer";
import type { ILogService } from "@app/interfaces/ILogService";
import type { ISubscriptionCreator } from "@domain/interfaces/message/subscription/ISubscriptionCreator";

export class SubscribeToMessages {
  constructor(
    private subscriptionCreator: ISubscriptionCreator,
    private logService?: ILogService
  ) {}

  async execute<Data>(
    consumerId: number,
    listener: ISubscriptionListener<Data>,
    noAck?: boolean
  ) {
    await this.subscriptionCreator.create<Data>(consumerId, listener, noAck);

    this.logService?.log(`${consumerId} is subscribed.`, {
      consumerId,
      noAck,
    });
  }
}
