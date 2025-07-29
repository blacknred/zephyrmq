import type { IMetadataInput, IPublishResult } from "@app/interfaces/IProducer";
import type { IAppender } from "../../domain/interfaces/IAppender";
import type { ILogService } from "@app/interfaces/ILogService";
import type { IMessageWriter } from "@domain/interfaces/message/IMessageWriter";
import type { MessageMetadata } from "@domain/entities/MessageMetadata";

export class PublishMessage {
  constructor(
    private writer: IMessageWriter,
    private metrics: IMetricsCollector,
    private activityTracker: IClientActivityTracker,
    private logService?: ILogService
  ) {}

  async execute(
    producerId: number,
    message: Buffer,
    meta: MessageMetadata
  ) {
    // 1. valid
    // 2.
    await this.writer.write(message, meta);

    const processingTime = Date.now() - meta.ts;
    this.metrics.recordEnqueue(message.length, processingTime);
    this.activityTracker.recordActivity(producerId, {
      messageCount: 1,
      processingTime,
      status: "idle",
    });

    this.logService?.log(`Message is published in ${meta.topic}.`, meta);
  }
}

// ConsumerGroupRouter

// ConsumerGroupCreator
// ConsumerGroupDeleter
// ConsumerGroupLister

// ConsumerGroupMemberCreator
// ConsumerGroupMemberDeleter
// ConsumerGroupMemberLister




// constructor(
//     private readonly publishingService: IPublishingService,
//     private readonly messageFactory: IMessageFactory<Data>,
//     private readonly topicName: string,
//     public readonly id: number
//   ) {}

  // async publish(batch: Data[], metadata: IMetadataInput = {}) {
  //   const results: IPublishResult[] = [];

  //   const messages = await this.messageFactory.create(batch, {
  //     ...metadata,
  //     topic: this.topicName,
  //     producerId: this.id,
  //   });

  //   for (const { message, meta, error } of messages) {
  //     const { id, ts } = meta;

  //     if (error) {
  //       const err = error instanceof Error ? error.message : "Unknown error";
  //       results.push({ id, ts, error: err, status: "error" });
  //       continue;
  //     }

  //     try {
  //       await this.publishingService.publish(this.id, message!, meta);
  //       results.push({ id, ts, status: "success" });
  //     } catch (err) {
  //       const error = err instanceof Error ? err.message : "Unknown error";
  //       results.push({ id, ts, error, status: "error" });
  //     }
  //   }

  //   return results;
  // }