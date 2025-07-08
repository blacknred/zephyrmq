import type { ICache } from "@domain/interfaces/ICache";
import type { IDBFlusher } from "@domain/interfaces/IDBFlusher";
import type { IKeyTracker } from "@domain/interfaces/IKeyTracker";

export class SetRecord<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly dbFlusher: IDBFlusher<K, V>,
    private readonly keyTracker: IKeyTracker<K>
  ) {}

  execute(key: K, value: V) {
    const prevValue = this.cache.get(key);

    if (!prevValue) {
      this.keyTracker.add(key);
    }

    if (prevValue !== value) {
      this.dbFlusher.commit(key, value);
    }

    return this.cache.set(key, value);
  }
}
