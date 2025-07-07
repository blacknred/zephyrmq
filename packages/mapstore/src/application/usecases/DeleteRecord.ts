import type { ICache } from "@domain/ports/ICache";
import type { IDBFlusher } from "@domain/ports/IDBFlusher";
import type { IMapSizer } from "@domain/ports/IMapSizer";

export class DeleteRecord<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly dbFlusher: IDBFlusher<K, V>,
    private readonly sizer: IMapSizer
  ) {}

  execute(key: K) {
    this.dbFlusher.commit(key);
    this.sizer.size--;
    return this.cache.delete(key);
  }
}
