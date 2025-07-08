export interface IValueGetter<K, V> {
  get(key: K): Promise<V | undefined>;
}
