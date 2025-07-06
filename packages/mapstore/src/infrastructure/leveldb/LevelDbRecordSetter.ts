import type { IFlushManager } from "@domain/ports/IFlushManager";
import type { IRecordSetter } from "@domain/ports/IRecordSetter";
import type { LevelDbMap } from "./LevelDbMap";
import type { LevelDbMapSizer } from "./LevelDbMapSizer";

export class LevelDbRecordSetter<K, V> implements IRecordSetter<K, V> {
  constructor(
    private flushManager: IFlushManager,
    private map: LevelDbMap<K, V>,
    private dirtyKeys: Set<K>,
    private sizer: LevelDbMapSizer,
    private maxSize = Infinity
  ) {}

  set(key: K, value: V) {
    if (this.map.get(key) !== value) {
      this.sizer.size++;
      this.dirtyKeys.add(key);
      this.flushManager.commit();
    }

    this.map.set(key, value);
    this.evictIfFull();
    return this.map;
  }

  private evictIfFull(): void {
    // TODO: LRU-style eviction (simplified)
    if (this.map.size >= this.maxSize) {
      const firstKey = this.map.keys().next().value!;
      this.map.delete(firstKey);
    }
  }
}
