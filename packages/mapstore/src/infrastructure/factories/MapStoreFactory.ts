import { LevelDbMapFactory } from "@app/factories/LevelDbMapFactory";
import { MapStore } from "@app/MapStore";
import { CloseDB } from "@app/usecases/CloseDB";
import { CreateMap } from "@app/usecases/CreateMap";
import { MemoryCapacityService } from "@infra/ram/MemoryCapacityService";
import { MemoryPressureChecker } from "@infra/ram/MemoryPressureChecker";
import { FlushManager } from "@infra/storage/FlushManager";
import { LevelDbCloser } from "@infra/storage/leveldb/LevelDbCloser";
import { Level, type DatabaseOptions } from "level";
import { FifoCacheFactory } from "./FifoCacheFactory";

interface IDBFlushManagerConfig {
  persistThresholdMs?: number;
  maxPendingFlushes?: number;
  memoryUsageThresholdMB?: number;
}

export interface IMapStoreConfig
  extends DatabaseOptions<string, unknown>,
    IDBFlushManagerConfig {}

export class MapStoreFactory {
  create(location: string, options: IMapStoreConfig | undefined = {}) {
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

    const cacheFactory = new FifoCacheFactory(new MemoryCapacityService());

    const dbCloser = new LevelDbCloser(db, flushManager);
    const mapCreator = new LevelDbMapFactory(db, flushManager, cacheFactory);

    return new MapStore(new CreateMap(mapCreator), new CloseDB(dbCloser));
  }
}
