import type { MessageMetadata } from "@domain/entities/MessageMetadata";

export class RouteMessage {
  constructor(
    private writer: IMessageWriter,
    private metrics: IMetricsCollector,
    private activityTracker: IClientActivityTracker,
    private logService?: ILogService
  ) {}

  async execute(meta: MessageMetadata, _skipDLQ = false) {
    await this.messagePublisher.publish(meta);
  }
}
