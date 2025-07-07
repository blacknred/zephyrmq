import type { ICache } from "@domain/ports/ICache";
import type { IEntriesReader } from "@domain/ports/IEntriesReader";
import type { IMapSizer } from "@domain/ports/IMapSizer";

export class ReadEntries<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly cacheEntriesReader: IEntriesReader<K, V>,
    private readonly storageEntriesReader: IEntriesReader<K, V>,
    private readonly sizer: IMapSizer
  ) {}

  execute() {
    if (this.cache.size == this.sizer.size) {
      return this.cacheEntriesReader.entries();
    }

    return this.storageEntriesReader.entries();
  }
}
