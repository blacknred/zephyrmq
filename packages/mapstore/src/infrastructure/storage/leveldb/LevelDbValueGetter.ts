import type { ISerializable } from "@domain/interfaces/ISerializable";
import type { IValueGetter } from "@domain/interfaces/IValueGetter";
import type { Level } from "level";

export class LevelDbValueGetter<K, V> implements IValueGetter<K, V> {
  constructor(
    private db: Level<string, unknown>,
    private name: string,
    private serializer?: ISerializable<V>
  ) {}

  async get(key: K) {
    const rawValue = await this.db.get(`${this.name}!${key}`);
    if (rawValue != undefined && this.serializer) {
      return this.serializer?.deserialize(rawValue);
    }
    return rawValue as V | undefined;
  }
}
