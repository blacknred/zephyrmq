export interface AsyncMapIterator<T>
  extends AsyncIteratorObject<T, BuiltinIteratorReturn, unknown> {
  [Symbol.asyncIterator](): AsyncMapIterator<T>;
}