import type { IFlushManager } from "@domain/ports/IFlushManager";
import type { IMapCleaner } from "@domain/ports/IMapCleaner";
import type { LevelDbMap } from "./LevelDbMap";
import type { LevelDbMapSizer } from "./LevelDbMapSizer";

export class LevelDbMapCleaner<K, V> implements IMapCleaner {
  constructor(
    private flushManager: IFlushManager,
    private map: LevelDbMap<K, V>,
    private sizer: LevelDbMapSizer
  ) {}

  clean() {
    this.map.clear();
    this.sizer.size = 0;
    this.flushManager.commit();
  }
}
