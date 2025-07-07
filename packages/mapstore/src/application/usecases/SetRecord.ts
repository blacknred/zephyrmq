import type { ICache } from "@domain/ports/ICache";
import type { IDBFlusher } from "@domain/ports/IDBFlusher";

export class SetRecord<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly dbFlusher: IDBFlusher<K, V>
  ) {}

  execute(key: K, value: V) {
    if (this.cache.has(key) !== value) {
      this.sizer.size++;
      this.dbFlusher.commit(key, value);
    }

    return this.cache.set(key, value);
  }
}
