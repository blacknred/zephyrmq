import type { IPublishingService } from "@app/interfaces/IPublishingService";
import type { PublishMessage } from "@app/usecases/message/PublishMessage";
import type { MessageMetadata } from "@domain/entities/MessageMetadata";

export class PublishingService<Data> implements IPublishingService {
  constructor(
    private readonly publishMessage: PublishMessage,
    // private readonly messageStore: IMessageStore<Data>,
    // private readonly messageRouter: IMessageRouter,
    // private readonly messagePublisher: IMessagePublisher,
    // private readonly activityTracker: IClientActivityTracker,
    // private readonly delayedManager: IDelayedMessageManager,
    // private readonly metrics: IMetricsCollector
  ) {}

  async publish(
    producerId: number,
    message: Buffer,
    meta: MessageMetadata
  ): Promise<void> {
    this.publishMessage.execute(producerId, message, meta);
  }

  // getMetrics() {
  //   return {
  //     router: this.messageRouter.getMetrics(),
  //     delayedMessages: this.delayedManager.getMetrics(),
  //     storage: this.messageStore.getMetrics(),
  //   };
  // }
}
