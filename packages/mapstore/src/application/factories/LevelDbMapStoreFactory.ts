import { LevelDbMapFactory } from "@app/factories/LevelDbMapFactory";
import { MapStore } from "@app/MapStore";
import { CloseDB } from "@app/usecases/CloseDB";
import { CreateMap } from "@app/usecases/CreateMap";
import type { IDBFlushManagerConfig } from "@domain/ports/IDBFlushManager";
import { LevelDbCloser } from "@infra/storage/leveldb/LevelDbCloser";
import { FlushManager } from "@infra/util/FlushManager";
import { Level, type DatabaseOptions } from "level";

interface IMapStoreConfig
  extends DatabaseOptions<string, unknown>,
    IDBFlushManagerConfig {}

export class LevelDbMapStoreFactory {
  create<K extends string | number, V>(
    location: string,
    options: IMapStoreConfig | undefined = {}
  ) {
    const {
      persistThresholdMs,
      maxPendingFlushes,
      memoryUsageThresholdMB,
      ...dbOptions
    } = options;
    const db = new Level(location, dbOptions);

    const flushManager = new FlushManager({
      persistThresholdMs,
      maxPendingFlushes,
      memoryUsageThresholdMB,
    });

    const dbCloser = new LevelDbCloser(db, flushManager);
    const mapCreator = new LevelDbMapFactory(db, flushManager);

    return new MapStore(new CreateMap(mapCreator), new CloseDB(dbCloser));
  }
}
