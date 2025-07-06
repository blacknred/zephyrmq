export interface IDBCloser {
  close(): Promise<void>;
}
