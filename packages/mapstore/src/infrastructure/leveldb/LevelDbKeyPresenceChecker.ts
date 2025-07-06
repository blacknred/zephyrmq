import type { IKeyPresenceChecker } from "@domain/ports/IKeyPresenceChecker";
import type { Level } from "level";
import type { LevelDbMap } from "./LevelDbMap";

export class LevelDbKeyPresenceChecker<K, V> implements IKeyPresenceChecker<K> {
  constructor(
    private map: LevelDbMap<K, V>,
    private db: Level<string, unknown>,
    private name: string
  ) {}

  has(key: K) {
    const exists = this.map.has(key);
    if (!exists) {
      return this.db.getSync(`${this.name}!${key}`) != undefined;
    }
    return exists;
  }
}
