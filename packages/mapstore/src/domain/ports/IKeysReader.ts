export interface IKeysReader<K> {
  keys(): MapIterator<K>;
}
