import type { AsyncMapIterator } from "@domain/interfaces/AsyncMapIterator";

export interface IMap<K, V> {
  size: number;
  clear(): void;
  set(key: K, value: V): Map<K, V>;
  delete(key: K): boolean;
  keys(): AsyncMapIterator<K>;
  values(): AsyncMapIterator<V>;
  entries(): AsyncMapIterator<[K, V]>;
  //
  has(key: K): Promise<boolean>;
  get(key: K): Promise<V | undefined>;
  flush(): Promise<void>;
}
