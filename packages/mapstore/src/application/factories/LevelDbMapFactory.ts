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
import type { IFlushManager } from "@domain/ports/IFlushManager";
import { LevelDbEntriesReader } from "@infra/leveldb/LevelDbEntriesReader";
import { LevelDbKeyPresenceChecker } from "@infra/leveldb/LevelDbKeyPresenceChecker";
import { LevelDbKeysReader } from "@infra/leveldb/LevelDbKeysReader";
import { LevelDbMap } from "@infra/leveldb/LevelDbMap";
import { LevelDbMapCleaner } from "@infra/leveldb/LevelDbMapCleaner";
import { LevelDbMapFlusher } from "@infra/leveldb/LevelDbMapFlusher";
import { LevelDbMapSizer } from "@infra/leveldb/LevelDbMapSizer";
import { LevelDbRecordDeleter } from "@infra/leveldb/LevelDbRecordDeleter";
import { LevelDbRecordSetter } from "@infra/leveldb/LevelDbRecordSetter";
import { LevelDbValueGetter } from "@infra/leveldb/LevelDbValueGetter";
import { LevelDbValuesReader } from "@infra/leveldb/LevelDbValuesReader";
import type { Level } from "level";

export class LevelDbMapFactory implements IMapFactory {
  constructor(
    private db: Level<string, unknown>,
    private flushManager: IFlushManager
  ) {}

  create<K extends string | number, V>(
    name: string,
    { maxSize, serializer }: IMapOptions<V> = {}
  ) {
    const dirtyKeys = new Set<K>();
    const map = new LevelDbMap<K, V>(this.db, name, serializer);
    const mapSizer = new LevelDbMapSizer();
    const keyChecker = new LevelDbKeyPresenceChecker(map, this.db, name);
    const valueGetter = new LevelDbValueGetter(map, this.db, name);
    const keysReader = new LevelDbKeysReader(map, this.db, name);
    const valuesReader = new LevelDbValuesReader(map, this.db, name);
    const entriesReader = new LevelDbEntriesReader(map, this.db, name);

    const recordSetter = new LevelDbRecordSetter(
      this.flushManager,
      map,
      dirtyKeys,
      mapSizer,
      maxSize
    );
    const recordDeleter = new LevelDbRecordDeleter(
      this.flushManager,
      map,
      dirtyKeys,
      mapSizer
    );
    const mapCleaner = new LevelDbMapCleaner(this.flushManager, map, mapSizer);
    const mapFlusher = new LevelDbMapFlusher(
      this.db,
      this.flushManager,
      name,
      map,
      dirtyKeys,
      mapSizer,
      serializer
    );

    return new Map<K, V>(
      new GetMapSize(mapSizer),
      new CheckKeyPresence(keyChecker),
      new GetValue(valueGetter),
      new SetRecord(recordSetter),
      new DeleteRecord(recordDeleter),
      new ReadKeys(keysReader),
      new ReadValues(valuesReader),
      new ReadEntries(entriesReader),
      new CleanMap(mapCleaner),
      new FlushMap(mapFlusher)
    );
  }
}
