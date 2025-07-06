import type { IFlushManager } from "@domain/ports/IFlushManager";
import type { IRecordDeleter } from "@domain/ports/IRecordDeleter";
import type { LevelDbMap } from "./LevelDbMap";
import type { LevelDbMapSizer } from "./LevelDbMapSizer";

export class LevelDbRecordDeleter<K, V> implements IRecordDeleter<K> {
  constructor(
    private flushManager: IFlushManager,
    private map: LevelDbMap<K, V>,
    private dirtyKeys: Set<K>,
    private sizer: LevelDbMapSizer
  ) {}

  delete(key: K) {
    this.dirtyKeys.add(key);
    this.sizer.size--;
    this.flushManager.commit();
    return this.map.delete(key);
  }
}
