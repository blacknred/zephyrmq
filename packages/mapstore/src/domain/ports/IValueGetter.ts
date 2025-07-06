export interface IValueGetter<K, V> {
  get(key: K): V | undefined;
}
