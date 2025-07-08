import type { AsyncMapIterator } from "@domain/interfaces/AsyncMapIterator";
import type { ICache } from "@domain/interfaces/ICache";
import type { IKeysReader } from "@domain/interfaces/IKeysReader";

export class FifoCacheKeysReader<K, V> implements IKeysReader<K> {
  constructor(private cache: ICache<K, V>) {}

  keys(): AsyncMapIterator<K> {
    const iterator = this.cache.keys();

    return {
      async next() {
        return iterator.next();
      },

      [Symbol.asyncIterator]() {
        return this;
      },
    };
  }
}
