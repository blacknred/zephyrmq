import type { AsyncMapIterator } from "@domain/ports/AsyncMapIterator";
import type { IEntriesReader } from "@domain/ports/IEntriesReader";
import type { IKeysReader } from "@domain/ports/IKeysReader";

export class LevelDbKeysReader<K, V> implements IKeysReader<K> {
  constructor(private entriesReader: IEntriesReader<K, V>) {}

  keys(): AsyncMapIterator<K> {
    const iterator = this.entriesReader.entries();

    return {
      async next() {
        const entry = await iterator.next();
        const value = entry?.value?.[0];
        if (value) return { value, done: false };
        return { value: undefined, done: true };
      },

      [Symbol.asyncIterator]() {
        return this;
      },
    };
  }
}
