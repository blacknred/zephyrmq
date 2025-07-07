import type { ICache } from "@domain/ports/ICache";
import type { IMapSizer } from "@domain/ports/IMapSizer";
import type { IValuesReader } from "@domain/ports/IValuesReader";

export class ReadValues<K, V> {
  constructor(
    private readonly cache: ICache<K, V>,
    private readonly cacheEntriesReader: IValuesReader<V>,
    private readonly storageEntriesReader: IValuesReader<V>,
    private readonly sizer: IMapSizer
  ) {}

  execute() {
    if (this.cache.size == this.sizer.size) {
      return this.cacheEntriesReader.values();
    }

    return this.storageEntriesReader.values();
  }
}
