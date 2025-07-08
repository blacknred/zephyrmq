import type { AsyncMapIterator } from "@domain/interfaces/AsyncMapIterator";
import type { ICache } from "@domain/interfaces/ICache";
import type { IValuesReader } from "@domain/interfaces/IValuesReader";

export class FifoCacheValuesReader<K, V> implements IValuesReader<V> {
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
