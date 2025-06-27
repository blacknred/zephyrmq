import type { MessageMetadata } from "../models/MessageMetadata";

export interface IQueueManager {
  addQueue(id: number): void;
  removeQueue(id: number): void;
  enqueue(id: number, meta: MessageMetadata): number | undefined;
  dequeue(id: number): number | undefined;
  getMetrics(): {
    size: number;
  };
}
