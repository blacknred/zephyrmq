import type { MessageMetadata } from "@domain/entities/MessageMetadata";

export interface IQueueEnqueuer {
  enqueue(id: number, meta: MessageMetadata): Promise<number | undefined>;
}