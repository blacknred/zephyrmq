import type { AsyncMapIterator } from "@domain/interfaces/AsyncMapIterator";
import type { ICache } from "@domain/interfaces/ICache";
import type { IEntriesReader } from "@domain/interfaces/IEntriesReader";

export class FifoCacheEntriesReader<K, V> implements IEntriesReader<K, V> {
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
