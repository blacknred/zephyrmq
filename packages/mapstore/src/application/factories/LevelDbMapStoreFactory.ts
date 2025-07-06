import { LevelDbMapFactory } from "@app/factories/LevelDbMapFactory";
import { MapStore } from "@app/MapStore";
import { CloseDB } from "@app/usecases/CloseDB";
import { CreateMap } from "@app/usecases/CreateMap";
import { LevelDbCloser } from "@infra/leveldb/LevelDbCloser";
import { FlushManager } from "@infra/util/FlushManager";
import { Level, type DatabaseOptions } from "level";

export class LevelDbMapStoreFactory {
  create<K extends string | number, V>(
    location: string,
    options?: DatabaseOptions<string, unknown> | undefined
  ) {
    const db = new Level(location, options);
    const flushManager = new FlushManager();

    const dbCloser = new LevelDbCloser(db, flushManager);
    const mapCreator = new LevelDbMapFactory(db, flushManager);

    return new MapStore(new CreateMap(mapCreator), new CloseDB(dbCloser));
  }
}
