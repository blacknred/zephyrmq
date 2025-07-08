import type { IMapStore } from "@app/interfaces/IMapStore";
import { MapStore } from "@app/services/MapStore";
import { CloseDB } from "@app/usecases/CloseDB";
import { CreateMap } from "@app/usecases/CreateMap";
import { MemoryCapacityService } from "@infra/ram/MemoryCapacityService";
import { MemoryPressureChecker } from "@infra/ram/MemoryPressureChecker";
import { FlushManager } from "@infra/storage/FlushManager";
import { LevelDbCloser } from "@infra/storage/leveldb/LevelDbCloser";
import { Level, type DatabaseOptions } from "level";
import { FifoCacheFactory } from "./FifoCacheFactory";
import { LevelDbMapFactory } from "./LevelDbMapFactory";
import { CacheCapacityCalculator } from "@infra/cache/CacheCapacityCalculator";

interface IDBFlushManagerConfig {
  persistThresholdMs?: number;
  maxPendingFlushes?: number;
  memoryUsageThresholdMB?: number;
}

export interface IMapStoreConfig
  extends DatabaseOptions<string, unknown>,
    IDBFlushManagerConfig {}

export class LevelDbMapStoreFactory {
  create(
    location: string,
    options: IMapStoreConfig | undefined = {}
  ): IMapStore {
    const {
      persistThresholdMs,
      maxPendingFlushes,
      memoryUsageThresholdMB,
      ...dbOptions
    } = options;
    const db = new Level(location, dbOptions);

    const flushManager = new FlushManager(
      new MemoryPressureChecker(memoryUsageThresholdMB),
      new Set(),
      persistThresholdMs,
      maxPendingFlushes
    );

    const cacheFactory = new FifoCacheFactory(
      new CacheCapacityCalculator(new MemoryCapacityService())
    );

    const dbCloser = new LevelDbCloser(db, flushManager);
    const mapCreator = new LevelDbMapFactory(db, flushManager, cacheFactory);

    return new MapStore(new CreateMap(mapCreator), new CloseDB(dbCloser));
  }
}
