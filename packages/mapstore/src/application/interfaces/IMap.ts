import type { AsyncMapIterator } from "@domain/interfaces/AsyncMapIterator";

export interface IMap<K, V> {
  size: number;
  clear(): void;
  has(key: K): boolean;
  set(key: K, value: V): void;
  delete(key: K): boolean;
  keys(): AsyncMapIterator<K>;
  values(): AsyncMapIterator<V>;
  entries(): AsyncMapIterator<[K, V]>;
  //
  get(key: K): Promise<V | undefined>;
  flush(): Promise<void>;
}
