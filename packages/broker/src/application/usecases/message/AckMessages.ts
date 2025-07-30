import type { IAckDeleter } from "@domain/interfaces/message/ack/IAckDeleter";
import type { IAcksGetter } from "@domain/interfaces/message/ack/IAcksGetter";
import type { IDedupCreator } from "@domain/interfaces/message/dedup/IDedupCreator";
import type { IDeliveryConfirmer } from "@domain/interfaces/message/delivery/IDeliveryConfirmer";
import type { ISubscriptionChecker } from "@domain/interfaces/message/subscription/ISubscriptionChecker";

export class AckMessages {
  constructor(
    private ackDeleter: IAckDeleter,
    private deliveryConfirmer: IDeliveryConfirmer,
    private dedupCreator: IDedupCreator,
    private acksGetter: IAcksGetter,
    private subscriptionChecker: ISubscriptionChecker
  ) {}

  async execute(consumerId: number, messageId?: number) {
    const pendingAcks: number[] = [];

    if (messageId) {
      pendingAcks.push(messageId);
      await this.ackDeleter.delete(consumerId, messageId);
    } else {
      const pendingMap = await this.acksGetter.get(consumerId);
      if (pendingMap) pendingAcks.push(...Object.keys(pendingMap).map(Number));
      await this.ackDeleter.delete(consumerId);
    }

    for (const messageId of pendingAcks) {
      await this.dedupCreator.create(consumerId, messageId);
      await this.deliveryConfirmer.confirm(messageId);
    }

    // try to drainQueue if push mode is available
    if (await this.subscriptionChecker.isSubscribed(consumerId)) {
      this.subscriptionProcessor.drainQueue(consumerId);
    }

    return pendingAcks;
  }
}

