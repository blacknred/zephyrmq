export interface IRecordDeleter<K> {
  delete(key: K): boolean
}