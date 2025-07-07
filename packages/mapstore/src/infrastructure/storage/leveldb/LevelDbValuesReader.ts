import type { AsyncMapIterator } from "@domain/ports/AsyncMapIterator";
import type { IEntriesReader } from "@domain/ports/IEntriesReader";
import type { IValuesReader } from "@domain/ports/IValuesReader";

export class LevelDbValuesReader<K, V> implements IValuesReader<V> {
  constructor(private entriesReader: IEntriesReader<K, V>) {}

  values(): AsyncMapIterator<V> {
    const iterator = this.entriesReader.entries();

    return {
      async next() {
        const entry = await iterator.next();
        const value = entry?.value?.[1];
        if (value) return { value, done: false };
        return { value: undefined, done: true };
      },

      [Symbol.asyncIterator]() {
        return this;
      },
    };
  }
}
