export interface IMessageStoreCloser {
  close(): Promise<void>;
}
