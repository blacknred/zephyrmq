import type { IKeysReader } from "@domain/ports/IKeysReader";
import type { ISerializable } from "@domain/ports/ISerializable";
import type { Level } from "level";

export class LevelDbKeysReader<K, V> implements IKeysReader<K> {
  constructor(
    private map: Map<K, V>,
    private db: Level<string, unknown>,
    private name: string,
    private serializer?: ISerializable<V>
  ) {}

  keys() {
    return this.map.keys();
    // TODO: keys from db?
  }
}
