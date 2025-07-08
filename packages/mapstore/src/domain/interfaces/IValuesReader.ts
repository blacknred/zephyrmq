import type { AsyncMapIterator } from "./AsyncMapIterator";

export interface IValuesReader<V> {
  values(): AsyncMapIterator<V>;
}
