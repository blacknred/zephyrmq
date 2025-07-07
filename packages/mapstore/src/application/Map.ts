import type { IMap } from "@app/interfaces/IMap";
import type { CheckKeyPresence } from "./usecases/CheckKeyPresence";
import type { CleanMap } from "./usecases/CleanMap";
import type { DeleteRecord } from "./usecases/DeleteRecord";
import type { FlushMap } from "./usecases/FlushMap";
import type { GetMapSize } from "./usecases/GetMapSize";
import type { GetValue } from "./usecases/GetValue";
import type { ReadEntries } from "./usecases/ReadEntries";
import type { ReadKeys } from "./usecases/ReadKeys";
import type { ReadValues } from "./usecases/ReadValues";
import type { SetRecord } from "./usecases/SetRecord";

export class Map<K extends string | number, V> implements IMap<K, V> {
  constructor(
    private getMapSize: GetMapSize,
    private checkKeyPresence: CheckKeyPresence<K, V>,
    private getValue: GetValue<K, V>,
    private setRecord: SetRecord<K, V>,
    private deleteRecord: DeleteRecord<K, V>,
    private readKeys: ReadKeys<K, V>,
    private readValues: ReadValues<K, V>,
    private readEntries: ReadEntries<K, V>,
    private cleanMap: CleanMap<K, V>,
    private flushMap: FlushMap<K, V>
  ) {}

  get size(): number {
    return this.getMapSize.execute();
  }

  flush(): Promise<void> {
    return this.flushMap.execute();
  }

  has(key: K) {
    return this.checkKeyPresence.execute(key);
  }

  get(key: K) {
    return this.getValue.execute(key);
  }

  set(key: K, value: V) {
    return this.setRecord.execute(key, value);
  }

  delete(key: K): boolean {
    return this.deleteRecord.execute(key);
  }

  clear(): void {
    this.cleanMap.execute();
  }

  keys() {
    return this.readKeys.execute();
  }

  values() {
    return this.readValues.execute();
  }

  entries() {
    return this.readEntries.execute();
  }
}
