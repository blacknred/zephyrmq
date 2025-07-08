import type { AsyncMapIterator } from "../../../domain/interfaces/AsyncMapIterator";
import type { IEntriesReader } from "../../../domain/interfaces/IEntriesReader";
import type { IValuesReader } from "../../../domain/interfaces/IValuesReader";

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
