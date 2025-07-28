import type { MessageMetadata } from "@domain/entities/MessageMetadata";

export interface IPublishingService {
  publish(
    producerId: number,
    message: Buffer,
    meta: MessageMetadata
  ): Promise<void>;
  // getMetrics(): {
  //   router: {
  //     consumerGroups: {
  //       name: string;
  //       count: number;
  //     }[];
  //   };
  //   delayedMessages: {
  //     count: number;
  //   };
  // };
}
