import type { ILogService } from "@app/interfaces/ILogService";
import type { ISubscriptionDeleter } from "@domain/interfaces/message/subscription/ISubscriptionDeleter";

export class UnsubscribeToMessages {
  constructor(
    private subscriptionDeleter: ISubscriptionDeleter,
    private logService?: ILogService
  ) {}

  async execute(consumerId: number) {
    await this.subscriptionDeleter.delete(consumerId);

    this.logService?.log(`${consumerId} is unsubscribed.`, {
      consumerId,
    });
  }
}
