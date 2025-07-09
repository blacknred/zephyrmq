export interface IThreadingPolicy {
  shouldEncodeInThread<T>(data: T, schemaRef?: string): boolean;
  shouldDecodeInThread(buffer: Buffer): boolean;
}
