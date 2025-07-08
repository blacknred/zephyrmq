export interface IDBFlusher<K, V> {
  flush(): Promise<void>;
  commit(key: K, value?: V): void;
}
