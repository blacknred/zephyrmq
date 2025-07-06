export interface IRecordSetter<K, V> {
  set(key: K, value: V): Map<K, V>;
}
