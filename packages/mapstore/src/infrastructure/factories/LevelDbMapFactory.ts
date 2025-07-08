import type { ICacheFactory } from "@infra/factories/ICacheFactory";
import type { IMapFactory, IMapOptions } from "@app/interfaces/IMapFactory";
import { Map } from "@app/Map";
import { CheckKeyPresence } from "@app/usecases/CheckKeyPresence";
import { CleanMap } from "@app/usecases/CleanMap";
import { DeleteRecord } from "@app/usecases/DeleteRecord";
import { FlushMap } from "@app/usecases/FlushMap";
import { GetMapSize } from "@app/usecases/GetMapSize";
import { GetValue } from "@app/usecases/GetValue";
import { ReadEntries } from "@app/usecases/ReadEntries";
import { ReadKeys } from "@app/usecases/ReadKeys";
import { ReadValues } from "@app/usecases/ReadValues";
import { SetRecord } from "@app/usecases/SetRecord";
import type { IDBFlushManager } from "../../domain/interfaces/IDBFlushManager";
import { CuckooFilterKeyTracker } from "@infra/keytracker/cuckoo-filter/CuckooFilterKeyTracker";
import { LevelDbCacheRestorer } from "@infra/storage/leveldb/LevelDbCacheRestorer";
import { LevelDbEntriesReader } from "@infra/storage/leveldb/LevelDbEntriesReader";
import { LevelDbKeysReader } from "@infra/storage/leveldb/LevelDbKeysReader";
import { LevelDbMapCleaner } from "@infra/storage/leveldb/LevelDbMapCleaner";
import { LevelDbMapFlusher } from "@infra/storage/leveldb/LevelDbMapFlusher";
import { LevelDbValueGetter } from "@infra/storage/leveldb/LevelDbValueGetter";
import { LevelDbValuesReader } from "@infra/storage/leveldb/LevelDbValuesReader";
import type { Level } from "level";

export class LevelDbMapFactory implements IMapFactory {
  static CACHE_MEMORY_RATIO = 1 / 3;

  constructor(
    private db: Level<string, unknown>,
    private flushManager: IDBFlushManager,
    private cacheFactory: ICacheFactory
  ) {}

  create<K extends string | number, V>(
    name: string,
    { serializer }: IMapOptions<V> = {}
  ) {
    const { cache, cacheEnriesReader, cacheKeysReader, cacheValuesReader } =
      this.cacheFactory.create();
    new LevelDbCacheRestorer(cache, this.db, name, serializer).restore();

    const dbValueGetter = new LevelDbValueGetter<K, V>(
      this.db,
      name,
      serializer
    );
    const dbEntriesReader = new LevelDbEntriesReader<K, V>(
      this.db,
      name,
      serializer
    );
    const dbKeysReader = new LevelDbKeysReader<K, V>(dbEntriesReader);
    const dbValuesReader = new LevelDbValuesReader<K, V>(dbEntriesReader);
    const dbCleaner = new LevelDbMapCleaner(this.db, name);
    const dbFlusher = new LevelDbMapFlusher<K, V>(
      this.db,
      this.flushManager,
      name,
      serializer
    );

    const keyRegistry = new CuckooFilterKeyTracker<K>();

    return new Map<K, V>(
      new GetMapSize(keyRegistry),
      new CheckKeyPresence<K, V>(cache, keyRegistry),
      new GetValue(cache, dbValueGetter, keyRegistry),
      new SetRecord(cache, dbFlusher, keyRegistry),
      new DeleteRecord(cache, dbFlusher, keyRegistry),
      new ReadKeys(cache, cacheKeysReader, dbKeysReader, keyRegistry),
      new ReadValues(cache, cacheValuesReader, dbValuesReader, keyRegistry),
      new ReadEntries(cache, cacheEnriesReader, dbEntriesReader, keyRegistry),
      new CleanMap(cache, dbCleaner, keyRegistry),
      new FlushMap(dbFlusher)
    );
  }
}
