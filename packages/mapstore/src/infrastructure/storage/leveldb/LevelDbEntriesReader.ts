import type { IEntriesReader } from "../../../domain/interfaces/IEntriesReader";
import type { ISerializable } from "../../../domain/interfaces/ISerializable";
import type { Level } from "level";

export class LevelDbEntriesReader<K, V> implements IEntriesReader<K, V> {
  constructor(
    private db: Level<string, unknown>,
    private name: string,
    private serializer?: ISerializable<V>
  ) {}

  entries(): AsyncIterableIterator<[K, V]> {
    const prefix = `${this.name}!`;
    const suffix = `${this.name}~`;
    const { serializer } = this;
    const iterator = this.db.iterator({ gt: prefix, lt: suffix });

    return {
      async next(): Promise<IteratorResult<[K, V], undefined>> {
        const entry = await iterator.next();
        if (!entry) {
          return { value: undefined, done: true };
        }

        const [rawKey, rawValue] = entry;
        const key = rawKey.slice(prefix.length + 1) as K;
        const value = serializer?.deserialize(rawValue) ?? (rawValue as V);

        return { value: [key, value], done: false };
      },

      [Symbol.asyncIterator]() {
        return this;
      },
    };
  }
}
