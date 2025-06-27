export interface IAppender {
  append(data: Buffer): Promise<number | void>;
  flush(): Promise<void>;
  isFlushing: boolean;
  batch: Buffer[];
  batchSize: number;
}
