import type { ISerializable } from "@domain/ports/ISerializable";
import type { IValuesReader } from "@domain/ports/IValuesReader";
import type { Level } from "level";

export class LevelDbValuesReader<K, V> implements IValuesReader<V> {
  constructor(
    private map: Map<K, V>,
    private db: Level<string, unknown>,
    private name: string,
    private serializer?: ISerializable<V>
  ) {}

  values() {
    return this.map.values();
    // TODO: values from db?
  }
}
