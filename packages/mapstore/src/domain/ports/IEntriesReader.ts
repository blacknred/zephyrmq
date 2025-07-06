export interface IEntriesReader<K, V> {
  entries(): MapIterator<[K, V]>;
}
