import type { AsyncMapIterator } from "@domain/ports/AsyncMapIterator";
import type { ICache } from "@domain/ports/ICache";
import type { IEntriesReader } from "@domain/ports/IEntriesReader";

export class FifiCacheEntriesReader<K, V> implements IEntriesReader<K, V> {
  constructor(private cache: ICache<K, V>) {}

  entries(): AsyncMapIterator<[K, V]> {
    const iterator = this.cache.entries();

    return {
      async next(): Promise<IteratorResult<[K, V], undefined>> {
        return iterator.next();
      },

      [Symbol.asyncIterator]() {
        return this;
      },
    };
  }
}
