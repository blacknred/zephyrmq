import type { AsyncMapIterator } from "./AsyncMapIterator";

export interface IEntriesReader<K, V> {
  entries(): AsyncMapIterator<[K, V]>;
}
