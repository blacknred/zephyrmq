export interface IWriteAheadLog {
  append(data: Buffer): Promise<number | void>;
  read(offset: number, length: number): Promise<Buffer | void>;
  truncate(upToOffset: number): Promise<void>;
  close(): Promise<void>;
  getMetrics(): Promise<{
    size: number | undefined;
    batchSize: number;
    batchCount: number;
    isFlushing: boolean;
  }>;
}
