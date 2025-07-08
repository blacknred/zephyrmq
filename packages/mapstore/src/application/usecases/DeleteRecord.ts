import type { ICache } from "../../domain/interfaces/ICache";
import type { IDBFlusher } from "../../domain/interfaces/IDBFlusher";
import type { IKeyTracker } from "../../domain/interfaces/IKeyTracker";

export class DeleteRecord<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly dbFlusher: IDBFlusher<K, V>,
    private readonly keyTracker: IKeyTracker<K>
  ) {}

  execute(key: K) {
    this.dbFlusher.commit(key);
    this.keyTracker.remove(key);
    return this.cache.delete(key);
  }
}
