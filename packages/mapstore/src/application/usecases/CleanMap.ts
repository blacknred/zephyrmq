import type { ICache } from "@domain/ports/ICache";
import type { IDBFlusher } from "@domain/ports/IDBFlusher";
import type { IMapSizer } from "@domain/ports/IMapSizer";

export class CleanMap<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly dbFlusher: IDBFlusher<K, V>,
    private readonly sizer: IMapSizer
  ) {}

  execute() {
    this.cache.clear();
    this.sizer.size = 0;
    this.dbFlusher.flush();
  }
}
