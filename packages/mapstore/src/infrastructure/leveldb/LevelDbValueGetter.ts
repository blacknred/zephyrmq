import type { ISerializable } from "@domain/ports/ISerializable";
import type { IValueGetter } from "@domain/ports/IValueGetter";
import type { Level } from "level";
import type { LevelDbMap } from "./LevelDbMap";

export class LevelDbValueGetter<K, V> implements IValueGetter<K, V> {
  constructor(
    private map: LevelDbMap<K, V>,
    private db: Level<string, unknown>,
    private name: string,
    private serializer?: ISerializable<V>
  ) {}

  get(key: K) {
    const value = this.map.get(key);
    if (value != undefined) return value;
    const rawValue = this.db.getSync(`${this.name}!${key}`) as V;
    if (rawValue != undefined && this.serializer) {
      return this.serializer?.deserialize(rawValue);
    }
    return rawValue;
  }
}
