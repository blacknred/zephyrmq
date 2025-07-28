import type { IDBFlushManager } from "@domain/interfaces/IDBFlushManager";
import type { IDBFlusher } from "@domain/interfaces/IDBFlusher";
import type { ISerializable } from "@domain/interfaces/ISerializable";
import { Mutex } from "@infra/util/Mutex";
import type { Level } from "level";

export class LevelDbMapFlusher<K, V> implements IDBFlusher<K, V> {
  private mutex = new Mutex();
  private dirtyKeys = new Map<K, V | undefined>();

  constructor(
    private db: Level<string, unknown>,
    private flushManager: IDBFlushManager,
    private name: string,
    private serializer?: ISerializable<V>
  ) {
    this.flushManager.register(this.flush);
  }

  commit(key: K, value?: V) {
    this.dirtyKeys.set(key, value);
    this.flushManager.commit();
  }

  flush = async () => {
    if (this.dirtyKeys.size === 0) return;
    await this.mutex.acquire();

    try {
      const batch = this.db.batch();

      for (const [key, value] of this.dirtyKeys) {
        const fullKey = `${this.name}!${key}`;
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
