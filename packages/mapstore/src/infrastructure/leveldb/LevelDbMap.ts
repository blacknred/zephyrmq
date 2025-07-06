import type { ISerializable } from "@domain/ports/ISerializable";
import type { Level } from "level";

export class LevelDbMap<K, V> extends Map<K, V> {
  constructor(
    private db: Level<string, unknown>,
    private name: string,
    private serializer?: ISerializable<V>
  ) {
    super();
    this.init();
  }

  private async init() {
    try {
      for await (const [key, value] of this.db.iterator({
        gt: `${this.name}!`,
        lt: `${this.name}~`,
      })) {
        const validKey = key.slice(this.name.length + 1);
        const deserialisedValue = this.serializer?.deserialize(value) ?? value;
        this.set(validKey as K, deserialisedValue as V);
      }
    } catch (cause) {
      throw new Error(`Failed to restore ${this.name}`, { cause });
    }
  }
}
