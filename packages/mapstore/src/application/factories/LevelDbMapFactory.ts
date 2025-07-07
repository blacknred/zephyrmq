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
import type { IDBFlushManager } from "@domain/ports/IDBFlushManager";
import { FifoCache } from "@infra/cache/fifo/FifoCache";
import { FifiCacheEntriesReader } from "@infra/cache/fifo/FifoCacheEntriesReader";
import { FifiCacheKeysReader } from "@infra/cache/fifo/FifoCacheKeysReader";
import { FifiCacheValuesReader } from "@infra/cache/fifo/FifoCacheValuesReader";
import { LevelDbCacheRestorer } from "@infra/storage/leveldb/LevelDbCacheRestorer";
import { LevelDbEntriesReader } from "@infra/storage/leveldb/LevelDbEntriesReader";
import { LevelDbKeysReader } from "@infra/storage/leveldb/LevelDbKeysReader";
import { LevelDbMapFlusher } from "@infra/storage/leveldb/LevelDbMapFlusher";
import { LevelDbMapSizer } from "@infra/storage/leveldb/LevelDbMapSizer";
import { LevelDbValueGetter } from "@infra/storage/leveldb/LevelDbValueGetter";
import { LevelDbValuesReader } from "@infra/storage/leveldb/LevelDbValuesReader";
import type { Level } from "level";

export class LevelDbMapFactory implements IMapFactory {
  private static instanceCount = 0;

  constructor(
    private db: Level<string, unknown>,
    private flushManager: IDBFlushManager
  ) {}

  create<K extends string | number, V>(
    name: string,
    { serializer }: IMapOptions<V> = {}
  ) {
    const mapSizer = new LevelDbMapSizer();

    const cache = new FifoCache<K, V>(LevelDbMapFactory.instanceCount++);
    new LevelDbCacheRestorer(cache, this.db, name, serializer).restore();
    const cacheEnriesReader = new FifiCacheEntriesReader(cache);
    const cacheKeysReader = new FifiCacheKeysReader(cache);
    const cacheValuesReader = new FifiCacheValuesReader(cache);

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
    const dbFlusher = new LevelDbMapFlusher<K, V>(
      this.db,
      this.flushManager,
      name,
      mapSizer,
      serializer
    );

    return new Map<K, V>(
      new GetMapSize(mapSizer),
      new CheckKeyPresence(cache, dbValueGetter),
      new GetValue(cache, dbValueGetter),
      new SetRecord(cache, dbFlusher),
      new DeleteRecord(cache, dbFlusher, mapSizer),
      new ReadKeys(cache, cacheKeysReader, dbKeysReader, mapSizer),
      new ReadValues(cache, cacheValuesReader, dbValuesReader, mapSizer),
      new ReadEntries(cache, cacheEnriesReader, dbEntriesReader, mapSizer),
      new CleanMap(cache, dbFlusher, mapSizer),
      new FlushMap(dbFlusher)
    );
  }
}
