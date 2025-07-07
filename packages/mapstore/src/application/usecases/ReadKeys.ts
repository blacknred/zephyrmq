import type { ICache } from "@domain/ports/ICache";
import type { IKeysReader } from "@domain/ports/IKeysReader";
import type { IMapSizer } from "@domain/ports/IMapSizer";

export class ReadKeys<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly cacheEntriesReader: IKeysReader<K>,
    private readonly storageEntriesReader: IKeysReader<K>,
    private readonly sizer: IMapSizer
  ) {}

  execute() {
    if (this.cache.size == this.sizer.size) {
      return this.cacheEntriesReader.keys();
    }

    return this.storageEntriesReader.keys();
  }
}
