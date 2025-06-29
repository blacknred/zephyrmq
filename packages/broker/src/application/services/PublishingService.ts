import type { MessageMetadata } from "@domain/entities/MessageMetadata";

export interface IPublishingService {
  publish(
    producerId: number,
    message: Buffer,
    meta: MessageMetadata
  ): Promise<void>;
  getMetrics(): {
    router: {
      consumerGroups: {
        name: string;
        count: number;
      }[];
    };
    delayedMessages: {
      count: number;
    };
  };
}

export class PublishingService<Data> implements IPublishingService {
  constructor(
    private readonly messageStore: IMessageStore<Data>,
    private readonly messageRouter: IMessageRouter,
    private readonly messagePublisher: IMessagePublisher,
    private readonly activityTracker: IClientActivityTracker,
    private readonly delayedManager: IDelayedMessageManager,
    private readonly metrics: IMetricsCollector
  ) {}

  async publish(
    producerId: number,
    message: Buffer,
    meta: MessageMetadata
  ): Promise<void> {
    await this.messageStore.write(message, meta);

    const processingTime = Date.now() - meta.ts;
    this.metrics.recordEnqueue(message.length, processingTime);
    this.activityTracker.recordActivity(producerId, {
      messageCount: 1,
      processingTime,
      status: "idle",
    });

    await this.messagePublisher.publish(meta);
  }

  getMetrics() {
    return {
      router: this.messageRouter.getMetrics(),
      delayedMessages: this.delayedManager.getMetrics(),
      storage: this.messageStore.getMetrics(),
    };
  }
}
