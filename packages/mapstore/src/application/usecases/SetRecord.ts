import type { IRecordSetter } from "@domain/ports/IRecordSetter";

export class SetRecord<K, V> {
  constructor(private readonly recordSetter: IRecordSetter<K, V>) {}

  execute(key: K, value: V) {
    return this.recordSetter.set(key, value);
  }
}
