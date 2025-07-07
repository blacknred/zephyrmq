import type { AsyncMapIterator } from "@domain/ports/AsyncMapIterator";
import type { ICache } from "@domain/ports/ICache";
import type { IValuesReader } from "@domain/ports/IValuesReader";

export class FifiCacheValuesReader<K, V> implements IValuesReader<V> {
  constructor(private cache: ICache<K, V>) {}

  values(): AsyncMapIterator<V> {
    const iterator = this.cache.values();

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
