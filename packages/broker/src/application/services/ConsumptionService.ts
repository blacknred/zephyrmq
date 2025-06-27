export interface IConsumptionService<Data> {
  consume(consumerId: number, noAck?: boolean): Promise<Data | undefined>;
  getMetrics(): {
    queuedMessages: {
      size: number;
    };
  };
}

export class ConsumptionService<Data> implements IConsumptionService<Data> {
  constructor(
    private readonly queueManager: IQueueManager,
    private readonly messageStore: IMessageStore<Data>,
    private readonly pendingAcks: IAckRegistry,
    private readonly deliveryTracker: IDeliveryTracker,
    private readonly processedMessageTracker: IProcessedMessageTracker,
    private readonly activityTracker: IClientActivityTracker,
    private readonly logger?: ILogCollector
  ) {}

  async consume(consumerId: number, noAck = false): Promise<Data | undefined> {
    if (this.pendingAcks.isReachedMaxUnacked(consumerId)) return;

    const messageId = this.queueManager.dequeue(consumerId);
    if (!messageId) return;

    const [message, meta] = await this.messageStore.read(messageId);
    if (!meta || !message) return;

    if (!noAck) {
      this.pendingAcks.addAck(consumerId, messageId);
    } else {
      this.activityTracker.recordActivity(consumerId, {
        messageCount: 1,
        pendingAcks: 0,
        processingTime: 0,
        status: "idle",
      });

      this.processedMessageTracker.add(consumerId, messageId);
      await this.deliveryTracker.decrementAwaitedDeliveries(messageId);
    }

    this.logger?.log(`Message is consumed from ${meta.topic}.`, meta);
    return message;
  }

  getMetrics() {
    return {
      queuedMessages: this.queueManager.getMetrics(),
    };
  }
}
