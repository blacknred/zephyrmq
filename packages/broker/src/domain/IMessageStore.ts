import type { MessageMetadata } from "./entities/MessageMetadata";

export interface IMessageStore<Data> {
  write(message: Buffer, meta: MessageMetadata): Promise<number | undefined>;
  read(
    id: number
  ): Promise<
    [
      Awaited<Data> | undefined,
      Pick<MessageMetadata, keyof MessageMetadata> | undefined,
    ]
  >;
  readMessage(id: number): Promise<Data | undefined>;
  readMetadata<K extends keyof MessageMetadata>(
    id: number,
    keys?: K[]
  ): Promise<Pick<MessageMetadata, K> | undefined>;
  markDeletable(id: number): Promise<void>;
  unmarkDeletable(id: number): Promise<void>;
  close(): Promise<void>;
  getMetrics(): Promise<{
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
  }>;
}
