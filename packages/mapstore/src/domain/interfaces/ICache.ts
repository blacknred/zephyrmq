export interface ICache<K, V> extends Map<K, V> {
  isFull(): boolean;
}
