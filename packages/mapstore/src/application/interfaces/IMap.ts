export interface IMap<K, V> {
  size: number;
  flush(): Promise<void>,
  clear(): void;
  has(key: K): boolean;
  get(key: K): V | undefined;
  set(key: K, value: V): Map<K, V>;
  delete(key: K): boolean;
  keys(): MapIterator<K>;
  values(): MapIterator<V>;
  entries(): MapIterator<[K, V]>;
}
