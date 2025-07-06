import type { IFlushManager } from "@domain/ports/IFlushManager";
import type { IMapFlusher } from "@domain/ports/IMapFlusher";
import type { ISerializable } from "@domain/ports/ISerializable";
import { Mutex } from "@infra/util/Mutex";
import type { Level } from "level";
import type { LevelDbMap } from "./LevelDbMap";
import type { LevelDbMapSizer } from "./LevelDbMapSizer";

export class LevelDbMapFlusher<K, V> implements IMapFlusher {
  private mutex = new Mutex();

  constructor(
    private db: Level<string, unknown>,
    private flushManager: IFlushManager,
    private name: string,
    private map: LevelDbMap<K, V>,
    private dirtyKeys: Set<K>,
    private sizer: LevelDbMapSizer,
    private serializer?: ISerializable<V>
  ) {
    this.flushManager.register(this.flush);
  }

  flush = async () => {
    if (this.dirtyKeys.size === 0) return;
    await this.mutex.acquire();

    try {
      if (!this.sizer.size) {
        await this.db.clear({
          gt: `${this.name}!`,
          lt: `${this.name}~`,
        });

        this.dirtyKeys.clear();
        return;
      }

      const batch = this.db.batch();

      for (const key of this.dirtyKeys) {
        const fullKey = `${this.name}!${key}`;
        const value = this.map.get(key);
        if (value == undefined) {
          batch.del(fullKey);
        } else {
          const serialisedValue = this.serializer?.serialize(value) ?? value;
          batch.put(fullKey, serialisedValue as V);
        }
      }

      await batch.write();
      this.dirtyKeys.clear();
    } catch (cause) {
      throw new Error(`Failed to flush ${this.name}`, { cause });
    } finally {
      this.mutex.release();
    }
  };
}
