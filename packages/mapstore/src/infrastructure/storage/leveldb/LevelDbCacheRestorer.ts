import type { ICache } from "@domain/ports/ICache";
import type { ICacheRestorer } from "@domain/ports/ICacheRestorer";
import type { ISerializable } from "@domain/ports/ISerializable";
import type { Level } from "level";

export class LevelDbCacheRestorer<K, V> implements ICacheRestorer {
  constructor(
    private cache: ICache<K, V>,
    private db: Level<string, unknown>,
    private name: string,
    private serializer?: ISerializable<V>
  ) {}

  async restore() {
    const prefix = `${this.name}!`;
    const suffix = `${this.name}~`;

    try {
      for await (const [key, value] of this.db.iterator({
        gt: prefix,
        lt: suffix,
      })) {
        if (this.cache.isFull()) break;
        const validKey = key.slice(this.name.length + 1);
        const deserialisedValue = this.serializer?.deserialize(value) ?? value;
        this.cache.set(validKey as K, deserialisedValue as V);
      }
    } catch (cause) {
      throw new Error(`Failed to restore ${this.name}`, { cause });
    }
  }
}
