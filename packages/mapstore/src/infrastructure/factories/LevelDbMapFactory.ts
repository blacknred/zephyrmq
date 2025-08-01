import type { IMap } from "@app/interfaces/IMap";
import { Map } from "@app/services/Map";
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
import type { ISerializable } from "@domain/interfaces/ISerializable";
import { CuckooFilterKeyTracker } from "@infra/keytracker/cuckoo-filter/CuckooFilterKeyTracker";
import { LevelDbCacheRestorer } from "@infra/db/leveldb/LevelDbCacheRestorer";
import { LevelDbEntriesReader } from "@infra/db/leveldb/LevelDbEntriesReader";
import { LevelDbKeysReader } from "@infra/db/leveldb/LevelDbKeysReader";
import { LevelDbMapCleaner } from "@infra/db/leveldb/LevelDbMapCleaner";
import { LevelDbMapFlusher } from "@infra/db/leveldb/LevelDbMapFlusher";
import { LevelDbValueGetter } from "@infra/db/leveldb/LevelDbValueGetter";
import { LevelDbValuesReader } from "@infra/db/leveldb/LevelDbValuesReader";
import type { Level } from "level";
import type { IDBFlushManager } from "@domain/interfaces/IDBFlushManager";
import type { ICacheFactory } from "./ICacheFactory";

interface IMapOptions<T> {
  serializer?: ISerializable<T>;
}

export class LevelDbMapFactory {
  constructor(
    private db: Level<string, unknown>,
    private flushManager: IDBFlushManager,
    private cacheFactory: ICacheFactory
  ) {}

  create<K extends string | number, V>(
    name: string,
    { serializer }: IMapOptions<V> = {}
  ): IMap<K, V> {
    const keyRegistry = new CuckooFilterKeyTracker<K>();

    const { cache, cacheEnriesReader, cacheKeysReader, cacheValuesReader } =
      this.cacheFactory.create<K, V>();

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
