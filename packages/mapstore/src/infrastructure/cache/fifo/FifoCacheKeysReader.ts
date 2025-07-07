import type { AsyncMapIterator } from "@domain/ports/AsyncMapIterator";
import type { ICache } from "@domain/ports/ICache";
import type { IKeysReader } from "@domain/ports/IKeysReader";

export class FifiCacheKeysReader<K, V> implements IKeysReader<K> {
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
