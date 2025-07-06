export interface IValuesReader<V> {
  values(): MapIterator<V>;
}
