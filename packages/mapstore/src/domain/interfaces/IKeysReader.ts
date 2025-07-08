import type { AsyncMapIterator } from "./AsyncMapIterator";

export interface IKeysReader<K> {
  keys(): AsyncMapIterator<K>;
}
