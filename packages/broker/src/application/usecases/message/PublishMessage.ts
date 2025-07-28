import type { IMetadataInput, IPublishResult } from "@app/interfaces/IProducer";
import type { IAppender } from "../../domain/interfaces/IAppender";
import type { ILogService } from "@app/interfaces/ILogService";

export class PublishMessage {
  constructor(
    private creator: IClientCreator,
    private logService?: ILogService
  ) {}

  async execute<Data>(batch: Data[], metadata: IMetadataInput = {}) {
    // const results: IPublishResult[] = [];

    // // 1
    // const messages = await this.messageFactory.create(batch, {
    //   ...metadata,
    //   topic: this.topicName,
    //   producerId: this.id,
    // });

    // // 2
    // for (const { message, meta, error } of messages) {
    //   const { id, ts } = meta;

    //   if (error) {
    //     const err = error instanceof Error ? error.message : "Unknown error";
    //     results.push({ id, ts, error: err, status: "error" });
    //     continue;
    //   }

    //   try {
    //     await this.publishingService.publish(this.id, message!, meta);
    //     results.push({ id, ts, status: "success" });
    //   } catch (err) {
    //     const error = err instanceof Error ? err.message : "Unknown error";
    //     results.push({ id, ts, error, status: "error" });
    //   }
    // }

    // return results;

// 3

    await this.messageStore.write(message, meta);
    // 4

    const processingTime = Date.now() - meta.ts;
    this.metrics.recordEnqueue(message.length, processingTime);
    this.activityTracker.recordActivity(producerId, {
      messageCount: 1,
      processingTime,
      status: "idle",
    });

    // 5

    await this.messagePublisher.publish(meta);
  }
}



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