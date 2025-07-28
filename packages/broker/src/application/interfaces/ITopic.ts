import type { ITopicConfig } from "./ITopicConfig";

export interface ITopic<Data> {
  name: string;
  config: ITopicConfig;
  createProducer(): IProducer<Data>;
  createConsumer(config: IConsumerConfig): IConsumer<Data>;
  createDLQConsumer(limit?: number): DLQConsumer<Data>;
  deleteClient(id: number): void;
  getMetrics(): Promise<{
    name: string;
    ts: number;
    totalMessagesPublished: number;
    totalBytes: number;
    depth: number;
    enqueueRate: number;
    dequeueRate: number;
    avgLatencyMs: number;
    queuedMessages: {
      size: number;
    };
    subscriptions: {
      count: number;
    };
    pendingAcks: {
      count: number;
    };
    dlq: {
      size: number;
    };
    storage: {
      wal: {
        fileSize: number | undefined;
        batchSize: number;
        batchCount: number;
        isFlushing: boolean;
      };
      log: {
        totalSize: number;
        messageCount: number;
        currentSegmentId: number | undefined;
        segmentCount: number;
      };
      db: {};
      ram: NodeJS.MemoryUsage;
    };
    router: {
      consumerGroups: {
        name: string;
        count: number;
      }[];
    };
    delayedMessages: {
      count: number;
    };
    clients: {
      count: number;
      producersCount: number;
      consumersCount: number;
      dlqConsumersCount: number;
      operableCount: number;
      avgProcessingTime: number;
    };
  }>;
}
