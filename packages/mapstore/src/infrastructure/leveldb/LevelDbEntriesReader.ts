import type { IEntriesReader } from "@domain/ports/IEntriesReader";
import type { ISerializable } from "@domain/ports/ISerializable";
import type { Level } from "level";
import type { LevelDbMap } from "./LevelDbMap";

export class LevelDbEntriesReader<K, V> implements IEntriesReader<K, V> {
  constructor(
    private map: LevelDbMap<K, V>,
    private db: Level<string, unknown>,
    private name: string,
    private serializer?: ISerializable<V>
  ) {}

  entries() {
    return this.map.entries();
    // TODO: entries from db?
  }
}
